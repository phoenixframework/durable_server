defmodule DurableServer.Terminator do
  @moduledoc """
  Terminator GenServer that coordinates graceful shutdown of DurableServer processes.

  The Terminator is placed at the bottom of the DurableServer.Supervisor supervision
  tree and traps exits. When the supervisor is shutting down, the Terminator's 
  terminate/2 callback is called, which:

  1. Sends sync_and_stop messages to all DurableServer children
  2. Monitors each child process for DOWN messages  
  3. Waits up to a configurable timeout for all children to sync and terminate
  4. Returns to continue the shutdown process

  This ensures that DurableServer processes have an opportunity to persist their
  state before the supervisor tree is torn down, while preventing indefinite
  hangs during shutdown.

  ## Configuration

  The Terminator uses the same configuration as its parent DurableServer.Supervisor:
  - `:graceful_shutdown_timeout_ms` - Maximum time to wait for graceful shutdown 
    (default: 30_000ms)

  ## Graceful Shutdown Protocol

  1. Supervisor begins shutdown process
  2. Terminator's terminate/2 is called with reason and state
  3. Terminator sends {:durable, {:sync_and_stop, reason}} to all DurableServer children
  4. Each DurableServer calls sync_state/1 then stops normally
  5. Terminator monitors for DOWN messages from each child
  6. After all children stop or timeout is reached, terminate/2 returns
  7. Supervisor continues shutdown process

  The graceful shutdown only applies to normal shutdown scenarios (e.g., application
  stop, supervisor shutdown). For abnormal termination (crashes, kills), the normal
  supervision tree behavior applies.
  """

  use GenServer
  require Logger

  @graceful_shutdown_timeout_ms 30_000

  def start_link(opts) do
    supervisor_name = Keyword.fetch!(opts, :supervisor_name)
    config = Keyword.fetch!(opts, :config)
    GenServer.start_link(__MODULE__, {supervisor_name, config})
  end

  def init({supervisor_name, config}) do
    # trap exits so we can coordinate graceful shutdown
    Process.flag(:trap_exit, true)

    state = %{
      supervisor_name: supervisor_name,
      config: config,
      graceful_shutdown_timeout_ms:
        Map.get(config, :graceful_shutdown_timeout_ms, @graceful_shutdown_timeout_ms)
    }

    {:ok, state}
  end

  def terminate(reason, state) do
    Logger.info(
      "Terminator initiating graceful shutdown for #{state.supervisor_name}: #{inspect(reason)}"
    )

    perform_graceful_shutdown(state)
  end

  defp perform_graceful_shutdown(state) do
    case get_durable_server_children(state.supervisor_name) do
      [_ | _] = children ->
        Logger.info(
          "Coordinating graceful shutdown of #{length(children)} DurableServer processes"
        )

        # send sync_and_stop message to all children and monitor them
        monitored_children =
          children
          |> Enum.map(fn {_id, pid, _type, _modules} ->
            ref = Process.monitor(pid)
            send(pid, {:durable, {:sync_and_stop, :shutdown}})
            {ref, pid}
          end)
          |> Map.new()

        # wait for all children to terminate or timeout
        wait_for_children(monitored_children, state.graceful_shutdown_timeout_ms)

      [] ->
        Logger.debug("No DurableServer children to shutdown gracefully")
        :ok
    end
  end

  defp get_durable_server_children(supervisor_name) do
    try do
      # Get the DynamicSupervisor child name
      dynamic_sup_name = :"#{supervisor_name}_dynamic"

      dynamic_sup_name
      |> DynamicSupervisor.which_children()
      |> Enum.filter(fn
        # Filter out non-DurableServer children (LifecycleManager, Terminator, TaskSupervisor)
        {_id, pid, _type, [DurableServer]} when is_pid(pid) ->
          true

        _ ->
          false
      end)
    catch
      :exit, {:noproc, _} ->
        # supervisor already gone
        []
    end
  end

  defp wait_for_children(monitored_children, _timeout_ms)
       when map_size(monitored_children) == 0 do
    Logger.debug("All DurableServer children have terminated gracefully")
    :ok
  end

  defp wait_for_children(monitored_children, timeout_ms)
       when map_size(monitored_children) > 0 and timeout_ms <= 0 do
    Logger.warning(
      "Graceful shutdown timeout reached, #{map_size(monitored_children)} children may not have synced"
    )

    :ok
  end

  defp wait_for_children(monitored_children, timeout_ms) do
    start_time = System.monotonic_time(:millisecond)

    receive do
      {:DOWN, ref, :process, _pid, _reason} ->
        # child terminated, remove from monitoring map - O(1) operation
        remaining_children = Map.delete(monitored_children, ref)

        elapsed_time = System.monotonic_time(:millisecond) - start_time
        remaining_timeout = max(0, timeout_ms - elapsed_time)

        wait_for_children(remaining_children, remaining_timeout)
    after
      timeout_ms ->
        Logger.warning(
          "Graceful shutdown timeout reached, #{map_size(monitored_children)} children may not have synced"
        )

        :ok
    end
  end
end
