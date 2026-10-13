defmodule DurableServer.LifecycleHelpersTest do
  use ExUnit.Case, async: true
  import DurableServer.LifecycleHelpers

  @moduletag :capture_log

  defmodule Manager do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    def init(opts) do
      {:ok, Map.merge(Map.new(opts), %{current_discovery_task: nil})}
    end

    def handle_info(:discover_and_restart, %{ignore?: true} = state), do: {:noreply, state}

    def handle_info(:discover_and_restart, state) do
      owner = state.owner

      task =
        Task.Supervisor.async_nolink(state.task_supervisor, fn ->
          send(owner, {:discovery_started, self()})

          receive do
            :finish -> {:discover, :ok}
            :crash -> exit(:injected_failure)
          end
        end)

      {:noreply, %{state | current_discovery_task: task}}
    end

    def handle_info({ref, {:discover, :ok}}, %{current_discovery_task: %Task{ref: ref}} = state) do
      Process.demonitor(ref, [:flush])
      {:noreply, %{state | current_discovery_task: nil}}
    end

    def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
      %Task{ref: ^ref} = state.current_discovery_task
      {:noreply, %{state | current_discovery_task: nil}}
    end
  end

  setup do
    task_supervisor = start_supervised!(Task.Supervisor)

    manager =
      start_supervised!(
        {Manager, owner: self(), task_supervisor: task_supervisor, ignore?: false}
      )

    %{manager: manager}
  end

  test "waits until the started discovery task completes and its result is processed", context do
    waiter = Task.async(fn -> discover_and_wait(context.manager) end)
    assert_receive {:discovery_started, task_pid}
    assert Task.yield(waiter, 0) == nil

    send(task_pid, :finish)
    assert Task.await(waiter) == :ok
    assert :sys.get_state(context.manager).current_discovery_task == nil
  end

  test "an idle manager is not mistaken for a completed discovery", context do
    :sys.replace_state(context.manager, &%{&1 | ignore?: true})

    assert_raise ExUnit.AssertionError, ~r/task_not_started/, fn ->
      discover_and_wait(context.manager)
    end
  end

  test "a failed discovery task is not mistaken for a completed discovery", context do
    waiter =
      Task.async(fn ->
        assert_raise ExUnit.AssertionError, ~r/task_exit.*injected_failure/, fn ->
          discover_and_wait(context.manager)
        end
      end)

    assert_receive {:discovery_started, task_pid}
    send(task_pid, :crash)
    Task.await(waiter)
  end

  test "a timed out wait does not leave a debug hook sending late results", context do
    assert_raise ExUnit.AssertionError, ~r/did not complete/, fn ->
      discover_and_wait(context.manager, 20)
    end

    assert_receive {:discovery_started, task_pid}
    ref = Process.monitor(task_pid)
    send(task_pid, :finish)
    assert_receive {:DOWN, ^ref, :process, ^task_pid, :normal}
    :sys.get_state(context.manager)
    refute_receive {:discovery, _ref, _result}, 0
  end
end
