defmodule DurableServer.Terminator do
  @moduledoc """
  Terminator GenServer that coordinates graceful shutdown of DurableServer processes.

  The Terminator is placed at the bottom of the DurableServer.Supervisor supervision
  tree and traps exits. When the supervisor is shutting down, the Terminator's
  terminate/2 callback is called, which:

  1. Sends sync_and_stop messages to DurableServer children (with limited concurrency)
  2. Monitors each child process for DOWN messages
  3. Waits up to a configurable timeout for each child to sync and terminate
  4. Returns to continue the shutdown process

  This ensures that DurableServer processes have an opportunity to persist their
  state before the supervisor tree is torn down, while preventing indefinite
  hangs during shutdown.

  ## Configuration

  The Terminator uses the same configuration as its parent DurableServer.Supervisor:
  - `:graceful_shutdown_timeout_ms` - Maximum time to wait for each child to shutdown
    (default: 30_000ms)
  - `:graceful_shutdown_concurrency` - Maximum concurrent shutdown operations
    (default: 50, should match Finch pool size to avoid connection exhaustion)

  ## Graceful Shutdown Protocol

  1. Supervisor begins shutdown process
  2. Terminator's terminate/2 is called with reason and state
  3. Terminator uses Task.async_stream with limited concurrency to:
     a. Send {:durable, {:sync_and_stop, reason}} to each DurableServer
     b. Wait for each child to terminate (up to timeout)
  4. Each DurableServer calls sync_state/1 then stops normally
  5. After all children stop or timeout is reached, terminate/2 returns
  6. Supervisor continues shutdown process

  The concurrency limit prevents overwhelming the Finch connection pool when many
  DurableServers try to persist their state simultaneously during shutdown.

  The graceful shutdown only applies to normal shutdown scenarios (e.g., application
  stop, supervisor shutdown). For abnormal termination (crashes, kills), the normal
  supervision tree behavior applies.
  """

  use GenServer
  require Logger

  @graceful_shutdown_timeout_ms 30_000
  @graceful_shutdown_concurrency 50

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
        Map.get(config, :graceful_shutdown_timeout_ms, @graceful_shutdown_timeout_ms),
      graceful_shutdown_concurrency:
        Map.get(config, :graceful_shutdown_concurrency, @graceful_shutdown_concurrency)
    }

    {:ok, state}
  end

  def terminate(reason, state) do
    Logger.info(
      "Terminator initiating graceful shutdown for #{state.supervisor_name}: #{inspect(reason)}"
    )

    try do
      DurableServer.LifecycleManager.stop_discovery(state.supervisor_name)
    catch
      :exit, _ -> :ok
    end

    perform_graceful_shutdown(state)
  end

  defp perform_graceful_shutdown(state) do
    case get_durable_server_children(state.supervisor_name) do
      [_ | _] = children ->
        child_count = length(children)

        Logger.info(
          "Coordinating graceful shutdown of #{child_count} DurableServer processes " <>
            "(concurrency: #{state.graceful_shutdown_concurrency})"
        )

        # Use Task.async_stream to limit concurrent shutdown operations.
        # This prevents overwhelming the Finch connection pool when many
        # DurableServers try to persist their state simultaneously.
        per_child_timeout = state.graceful_shutdown_timeout_ms
        start_time = System.monotonic_time(:millisecond)

        killed_count =
          children
          |> Task.async_stream(
            fn {_id, pid, _type, _modules} ->
              shutdown_child(pid, per_child_timeout)
            end,
            max_concurrency: state.graceful_shutdown_concurrency,
            timeout: :infinity,
            ordered: false
          )
          |> Enum.reduce(0, fn {:ok, result}, acc ->
            if result == :killed, do: acc + 1, else: acc
          end)

        elapsed_ms = System.monotonic_time(:millisecond) - start_time

        Logger.info(
          "Graceful shutdown completed in #{elapsed_ms}ms " <>
            "(#{child_count} children, #{killed_count} killed due to timeout)"
        )

        :ok

      [] ->
        Logger.debug("No DurableServer children to shutdown gracefully")
        :ok
    end
  end

  defp shutdown_child(pid, timeout) do
    ref = Process.monitor(pid)
    send(pid, {:durable, {:sync_and_stop, :shutdown}})

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} ->
        :ok
    after
      timeout ->
        # Didn't finish in time - kill to avoid blocking DynamicSupervisor shutdown
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
        after
          1000 -> Process.demonitor(ref, [:flush])
        end

        Logger.warning("Child #{inspect(pid)} did not terminate within #{timeout}ms, killed")
        :killed
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
end
