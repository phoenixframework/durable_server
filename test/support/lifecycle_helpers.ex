defmodule DurableServer.LifecycleHelpers do
  @moduledoc """
  Synchronization helpers for LifecycleManager tests.
  """
  import ExUnit.Assertions

  @doc """
  Triggers discovery and waits for that task's successful result to be processed.

  The GenServer debug hook observes the real message loop without adding a
  production test API. An idle manager, a failed task, and a completed cycle are
  distinct outcomes. The hook and monitor are removed even when an assertion fails.

  Fixtures asserting individual cycles should disable startup discovery bursts
  and use long automatic discovery intervals so those sweeps do not race the test.
  """
  def discover_and_wait(manager_pid, timeout \\ 5_000) do
    owner = self()
    ref = Process.monitor(manager_pid)
    handler = fn stage, event, _ -> observe_discovery(stage, event, owner, ref) end

    try do
      :ok = :sys.install(manager_pid, {ref, handler, :awaiting_request})
      send(manager_pid, :discover_and_restart)

      receive do
        {:discovery, ^ref, :ok} ->
          :ok

        {:discovery, ^ref, {:error, reason}} ->
          flunk("Discovery failed: #{inspect(reason)}")

        {:DOWN, ^ref, :process, ^manager_pid, reason} ->
          flunk("LifecycleManager crashed: #{inspect(reason)}")
      after
        timeout ->
          flunk("Discovery did not complete within #{timeout}ms")
      end
    after
      try do
        :sys.remove(manager_pid, ref)
      catch
        :exit, _ -> :ok
      end

      Process.demonitor(ref, [:flush])

      receive do
        {:discovery, ^ref, _result} -> :ok
      after
        0 -> :ok
      end
    end
  end

  defp observe_discovery(:awaiting_request, {:in, :discover_and_restart}, _owner, _ref),
    do: :starting

  defp observe_discovery(
         :starting,
         {:noreply, %{current_discovery_task: %Task{ref: task_ref}}},
         _owner,
         _ref
       ),
       do: {:running, task_ref}

  defp observe_discovery(:starting, {:noreply, _state}, owner, ref) do
    send(owner, {:discovery, ref, {:error, :task_not_started}})
    :done
  end

  defp observe_discovery({:running, task_ref}, {:in, {task_ref, {:discover, :ok}}}, _owner, _ref),
    do: :finishing

  defp observe_discovery(
         :finishing,
         {:noreply, %{current_discovery_task: nil}},
         owner,
         ref
       ) do
    send(owner, {:discovery, ref, :ok})
    :done
  end

  defp observe_discovery(
         {:running, task_ref},
         {:in, {:DOWN, task_ref, :process, _pid, reason}},
         owner,
         ref
       ) do
    send(owner, {:discovery, ref, {:error, {:task_exit, reason}}})
    :done
  end

  defp observe_discovery(stage, _event, _owner, _ref), do: stage
end
