defmodule DurableServer.HeartbeatWatchdog do
  @moduledoc false

  use GenServer
  require Logger

  alias DurableServer.HeartbeatMetrics

  defstruct supervisor_name: nil,
            owner: nil,
            owner_monitor: nil,
            deadline_ms: nil,
            last_heartbeat_at: nil,
            heartbeat_task_pid: nil,
            heartbeat_metrics: nil,
            timer_ref: nil,
            timer_token: nil

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    supervisor_name = Keyword.fetch!(opts, :supervisor_name)

    GenServer.start_link(__MODULE__, supervisor_name, name: name)
  end

  def arm(name, owner, last_heartbeat_at, deadline_ms)
      when is_pid(owner) and is_integer(last_heartbeat_at) and is_integer(deadline_ms) and
             deadline_ms > 0 do
    arm(name, owner, last_heartbeat_at, deadline_ms, nil)
  end

  def arm(name, owner, last_heartbeat_at, deadline_ms, heartbeat_metrics)
      when is_pid(owner) and is_integer(last_heartbeat_at) and is_integer(deadline_ms) and
             deadline_ms > 0 do
    GenServer.call(
      name,
      {:arm, owner, last_heartbeat_at, deadline_ms, heartbeat_metrics}
    )
  end

  def renew(name, owner, heartbeat_at)
      when is_pid(owner) and is_integer(heartbeat_at) do
    GenServer.cast(name, {:renew, owner, heartbeat_at})
  end

  def track_heartbeat_task(name, owner, task_pid)
      when is_pid(owner) and is_pid(task_pid) do
    GenServer.call(name, {:track_heartbeat_task, owner, task_pid})
  end

  def disarm(name, owner) when is_pid(owner) do
    GenServer.cast(name, {:disarm, owner})
  end

  @impl true
  def init(supervisor_name) when is_atom(supervisor_name) do
    {:ok, %__MODULE__{supervisor_name: supervisor_name}}
  end

  @impl true
  def handle_call(
        {:arm, owner, last_heartbeat_at, deadline_ms, heartbeat_metrics},
        _from,
        %__MODULE__{} = state
      ) do
    state =
      state
      |> clear_owner_monitor()
      |> cancel_timer()

    owner_monitor = Process.monitor(owner)

    state =
      %{
        state
        | owner: owner,
          owner_monitor: owner_monitor,
          deadline_ms: deadline_ms,
          last_heartbeat_at: last_heartbeat_at,
          heartbeat_task_pid: nil,
          heartbeat_metrics: heartbeat_metrics
      }
      |> schedule_deadline()

    {:reply, :ok, state}
  end

  def handle_call(
        {:track_heartbeat_task, owner, task_pid},
        _from,
        %__MODULE__{owner: owner} = state
      ) do
    {:reply, :ok, %{state | heartbeat_task_pid: task_pid}}
  end

  def handle_call({:track_heartbeat_task, _owner, _task_pid}, _from, %__MODULE__{} = state) do
    {:reply, :ignored, state}
  end

  @impl true
  def handle_cast({:renew, owner, heartbeat_at}, %__MODULE__{owner: owner} = state) do
    state =
      state
      |> cancel_timer()
      |> Map.merge(%{
        last_heartbeat_at: max(state.last_heartbeat_at, heartbeat_at),
        heartbeat_task_pid: nil
      })
      |> schedule_deadline()

    {:noreply, state}
  end

  def handle_cast({:renew, _owner, _heartbeat_at}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  def handle_cast({:disarm, owner}, %__MODULE__{owner: owner} = state) do
    {:noreply, disarm_state(state)}
  end

  def handle_cast({:disarm, _owner}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(
        {:DOWN, owner_monitor, :process, owner, _reason},
        %__MODULE__{owner: owner, owner_monitor: owner_monitor} = state
      ) do
    {:noreply, disarm_state(state)}
  end

  def handle_info(
        {:heartbeat_deadline, timer_token},
        %__MODULE__{timer_token: timer_token} = state
      ) do
    elapsed_since_last = System.monotonic_time(:millisecond) - state.last_heartbeat_at
    {children_fenced, child_count_status} = count_fenced_children(state.supervisor_name)
    metrics = heartbeat_snapshot(state, elapsed_since_last)

    :ok =
      HeartbeatMetrics.watchdog_termination(
        state.supervisor_name,
        %{
          children_fenced: children_fenced,
          consecutive_failures: metrics.consecutive_failures,
          last_success_age_ms: metrics.last_success_age_ms,
          remaining_watchdog_budget_ms: 0,
          cache_degraded_duration_ms: metrics.cache_degraded_duration_ms,
          watchdog_terminations: 1
        },
        %{
          child_count_status: child_count_status,
          cache_degraded: metrics.cache_degraded?
        }
      )

    Logger.error(fn ->
      "#{inspect(state.supervisor_name)}: heartbeat watchdog deadline exceeded " <>
        "(#{elapsed_since_last}ms since last watchdog renewal, " <>
        "#{metrics.last_success_age_ms}ms since last successful write, " <>
        "deadline #{state.deadline_ms}ms, remaining budget 0ms, " <>
        "consecutive failures #{metrics.consecutive_failures}, " <>
        "cache degraded for #{metrics.cache_degraded_duration_ms}ms), " <>
        "terminating lifecycle manager to prevent orphan conflicts; " <>
        "children_fenced=#{format_child_count(children_fenced, child_count_status)}"
    end)

    if is_pid(state.heartbeat_task_pid) do
      Process.exit(state.heartbeat_task_pid, :kill)
    end

    if is_pid(state.owner) do
      Process.exit(state.owner, :kill)
    end

    {:noreply, disarm_state(state)}
  end

  def handle_info({:heartbeat_deadline, _timer_token}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  defp schedule_deadline(%__MODULE__{last_heartbeat_at: nil} = state), do: state

  defp schedule_deadline(%__MODULE__{} = state) do
    deadline_at = state.last_heartbeat_at + state.deadline_ms
    delay_ms = max(deadline_at - System.monotonic_time(:millisecond), 0)
    timer_token = make_ref()
    timer_ref = Process.send_after(self(), {:heartbeat_deadline, timer_token}, delay_ms)

    %{state | timer_ref: timer_ref, timer_token: timer_token}
  end

  defp disarm_state(%__MODULE__{} = state) do
    state
    |> cancel_timer()
    |> clear_owner_monitor()
    |> Map.merge(%{
      owner: nil,
      owner_monitor: nil,
      deadline_ms: nil,
      last_heartbeat_at: nil,
      heartbeat_task_pid: nil,
      heartbeat_metrics: nil,
      timer_ref: nil,
      timer_token: nil
    })
  end

  defp cancel_timer(%__MODULE__{timer_ref: timer_ref} = state) when is_reference(timer_ref) do
    Process.cancel_timer(timer_ref)
    %{state | timer_ref: nil, timer_token: nil}
  end

  defp cancel_timer(%__MODULE__{} = state), do: state

  defp clear_owner_monitor(%__MODULE__{owner_monitor: owner_monitor} = state)
       when is_reference(owner_monitor) do
    Process.demonitor(owner_monitor, [:flush])
    %{state | owner_monitor: nil}
  end

  defp clear_owner_monitor(%__MODULE__{} = state), do: state

  defp heartbeat_snapshot(
         %__MODULE__{heartbeat_metrics: %HeartbeatMetrics{} = metrics, deadline_ms: deadline_ms},
         elapsed_since_last
       ) do
    snapshot = HeartbeatMetrics.snapshot(metrics, deadline_ms)

    %{
      consecutive_failures: snapshot.consecutive_failures,
      last_success_age_ms: snapshot.last_success_age_ms || elapsed_since_last,
      cache_degraded?: snapshot.cache_degraded?,
      cache_degraded_duration_ms: snapshot.cache_degraded_duration_ms
    }
  end

  defp heartbeat_snapshot(%__MODULE__{}, elapsed_since_last) do
    %{
      consecutive_failures: 0,
      last_success_age_ms: elapsed_since_last,
      cache_degraded?: false,
      cache_degraded_duration_ms: 0
    }
  end

  # Querying DynamicSupervisor would be a blocking GenServer call on the process
  # responsible for enforcing the hard deadline. Its direct links are the
  # supervised children plus its parent, so this gives us a non-blocking count.
  defp count_fenced_children(supervisor_name) do
    dynamic_supervisor = DurableServer.Supervisor.get_dynamic_supervisor(supervisor_name)

    with pid when is_pid(pid) <- Process.whereis(dynamic_supervisor),
         [links: links, dictionary: dictionary] <-
           Process.info(pid, [:links, :dictionary]) do
      ancestors =
        dictionary
        |> Keyword.get(:"$ancestors", [])
        |> Enum.filter(&is_pid/1)
        |> MapSet.new()

      children_fenced =
        Enum.count(links, fn linked_pid ->
          not MapSet.member?(ancestors, linked_pid)
        end)

      {children_fenced, :known}
    else
      _ -> {0, :unavailable}
    end
  end

  defp format_child_count(children_fenced, :known), do: children_fenced
  defp format_child_count(_children_fenced, :unavailable), do: "unknown"
end
