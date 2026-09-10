defmodule DurableServer.HeartbeatWatchdogTest do
  use ExUnit.Case, async: true

  alias DurableServer.{HeartbeatMetrics, HeartbeatWatchdog}

  @moduletag :capture_log

  test "termination telemetry reports the children that the supervisor tree will fence" do
    supervisor_name = unique_name("supervisor")
    dynamic_supervisor = DurableServer.Supervisor.get_dynamic_supervisor(supervisor_name)
    watchdog_name = unique_name("watchdog")

    start_supervised!({DynamicSupervisor, name: dynamic_supervisor, strategy: :one_for_one})

    Enum.each(1..2, fn id ->
      child_spec =
        Supervisor.child_spec(
          {Task, fn -> Process.sleep(:infinity) end},
          id: {__MODULE__, id}
        )

      assert {:ok, _pid} = DynamicSupervisor.start_child(dynamic_supervisor, child_spec)
    end)

    start_supervised!(
      Supervisor.child_spec(
        {HeartbeatWatchdog, name: watchdog_name, supervisor_name: supervisor_name},
        id: watchdog_name
      )
    )

    metrics = HeartbeatMetrics.new(supervisor_name)
    now = System.monotonic_time(:millisecond)
    HeartbeatMetrics.mark_heartbeat_success(metrics, now)
    HeartbeatMetrics.mark_watchdog_renewal(metrics, now)
    HeartbeatMetrics.record_cache_refresh(metrics, false, 1, 2)

    HeartbeatMetrics.record_attempt(
      metrics,
      %Req.TransportError{reason: :enetunreach},
      true,
      now + 5_000
    )

    attach_telemetry(HeartbeatMetrics.events().watchdog_termination, supervisor_name)

    owner = spawn(fn -> Process.sleep(:infinity) end)
    owner_ref = Process.monitor(owner)

    assert :ok = HeartbeatWatchdog.arm(watchdog_name, owner, now, 50, metrics)
    assert_receive {:DOWN, ^owner_ref, :process, ^owner, :killed}, 1_000

    assert_receive {:telemetry,
                    %{
                      count: 1,
                      watchdog_terminations: 1,
                      children_fenced: 2,
                      consecutive_failures: 1,
                      remaining_watchdog_budget_ms: 0,
                      cache_degraded_duration_ms: degraded_duration_ms
                    },
                    %{
                      child_count_status: :known,
                      cache_degraded: true
                    }},
                   1_000

    assert degraded_duration_ms >= 0
  end

  defp attach_telemetry(event, supervisor_name) do
    handler_id = {__MODULE__, self(), make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        event,
        fn _event, measurements, metadata, {test_pid, expected_supervisor} ->
          if metadata.supervisor == expected_supervisor do
            send(test_pid, {:telemetry, measurements, metadata})
          end
        end,
        {self(), supervisor_name}
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp unique_name(label) do
    :"heartbeat_watchdog_test_#{label}_#{System.unique_integer([:positive])}"
  end
end
