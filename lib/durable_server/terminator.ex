defmodule DurableServer.Terminator do
  @moduledoc """
  Terminator GenServer that coordinates graceful shutdown of DurableServer processes.

  The Terminator is placed at the bottom of the DurableServer.Supervisor supervision
  tree and traps exits. When the supervisor is shutting down, the Terminator's
  terminate/2 callback is called, which:

  1. Sends sync_and_stop messages to DurableServer children (with limited concurrency)
  2. Monitors each child process for DOWN messages
  3. Enforces both per-child and overall deadlines for children to sync and terminate
  4. Returns to continue the shutdown process

  This ensures that DurableServer processes have an opportunity to persist their
  state before the supervisor tree is torn down, while preventing indefinite
  hangs during shutdown.

  ## Configuration

  The Terminator uses the same configuration as its parent DurableServer.Supervisor:
  - `:graceful_shutdown_timeout_ms` - Maximum time for each child to persist and stop
    after its shutdown starts (default: 30_000ms)
  - `:graceful_shutdown_total_timeout_ms` - Maximum time for discovery shutdown and all
    child shutdown batches together (default: 55_000ms)
  - `:graceful_shutdown_concurrency` - Maximum concurrent shutdown operations
    (default: 50, should match Finch pool size to avoid connection exhaustion)

  ## Graceful Shutdown Protocol

  1. Supervisor begins shutdown process
  2. Terminator's terminate/2 is called with reason and state
  3. Terminator uses Task.async_stream with limited concurrency to:
     a. Send {:durable, {:sync_and_stop, reason}} to each DurableServer
     b. Wait for each child up to its own timeout and within the overall shutdown deadline
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
  @graceful_shutdown_total_timeout_ms 55_000
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
      graceful_shutdown_total_timeout_ms:
        Map.get(
          config,
          :graceful_shutdown_total_timeout_ms,
          @graceful_shutdown_total_timeout_ms
        ),
      graceful_shutdown_concurrency:
        Map.get(config, :graceful_shutdown_concurrency, @graceful_shutdown_concurrency)
    }

    {:ok, state}
  end

  def terminate(reason, state) do
    Logger.info(
      "Terminator initiating graceful shutdown for #{state.supervisor_name}: #{inspect(reason)}"
    )

    deadline =
      System.monotonic_time(:millisecond) + state.graceful_shutdown_total_timeout_ms

    case remaining_timeout(deadline) do
      0 ->
        :ok

      timeout ->
        try do
          DurableServer.LifecycleManager.stop_discovery(state.supervisor_name, timeout)
        catch
          :exit, _ -> :ok
        end
    end

    perform_graceful_shutdown(state, deadline)
  end

  defp perform_graceful_shutdown(state, deadline) do
    case get_durable_server_children(state.supervisor_name) do
      [_ | _] = children ->
        child_count = length(children)

        Logger.info(
          "Coordinating graceful shutdown of #{child_count} DurableServer processes " <>
            "(concurrency: #{state.graceful_shutdown_concurrency})"
        )

        # Use Task.async_stream to limit concurrent shutdown operations. Each child
        # gets its configured persistence window, capped by the overall deadline so
        # later batches cannot make shutdown unbounded.
        start_time = System.monotonic_time(:millisecond)

        diagnostics_before =
          DurableServer.LifecycleManager.get_discovery_diagnostics(state.supervisor_name)

        killed_count =
          children
          |> Task.async_stream(
            fn {_id, pid, _type, _modules} ->
              shutdown_child(pid, state.graceful_shutdown_timeout_ms, deadline)
            end,
            max_concurrency: state.graceful_shutdown_concurrency,
            timeout: :infinity,
            ordered: false
          )
          |> Enum.reduce(0, fn
            {:ok, :killed}, acc -> acc + 1
            {:ok, :ok}, acc -> acc
            {:exit, _reason}, acc -> acc + 1
          end)

        diagnostics_after =
          DurableServer.LifecycleManager.get_discovery_diagnostics(state.supervisor_name)

        sync_error_count =
          Map.get(diagnostics_after, :sync_and_stop_error, 0) -
            Map.get(diagnostics_before, :sync_and_stop_error, 0)

        elapsed_ms = System.monotonic_time(:millisecond) - start_time

        Logger.info(
          "Graceful shutdown completed in #{elapsed_ms}ms " <>
            "(#{child_count} children, #{killed_count} killed due to timeout)"
        )

        if sync_error_count > 0 do
          Logger.warning(
            "#{sync_error_count} DurableServer children failed final persistence during shutdown"
          )
        end

        :ok

      [] ->
        Logger.debug("No DurableServer children to shutdown gracefully")
        :ok
    end
  end

  defp shutdown_child(pid, per_child_timeout, overall_deadline) do
    ref = Process.monitor(pid)
    send(pid, {:durable, {:sync_and_stop, :shutdown}})

    child_deadline =
      min(
        System.monotonic_time(:millisecond) + per_child_timeout,
        overall_deadline
      )

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} ->
        :ok
    after
      remaining_timeout(child_deadline) ->
        Process.exit(pid, :kill)
        Process.demonitor(ref, [:flush])

        Logger.warning(
          "Child #{inspect(pid)} did not terminate before its graceful shutdown deadline, killed"
        )

        :killed
    end
  end

  defp remaining_timeout(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end

  defp get_durable_server_children(supervisor_name) do
    try do
      # Get the DynamicSupervisor child name
      dynamic_sup_name = :"#{supervisor_name}_dynamic"

      dynamic_sup_name
      |> DynamicSupervisor.which_children()
      |> Enum.filter(fn
        # Filter out non-DurableServer children (LifecycleManager, Terminator, TaskSupervisor)
        {_id, pid, _type, modules} when is_pid(pid) and is_list(modules) ->
          DurableServer in modules

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
