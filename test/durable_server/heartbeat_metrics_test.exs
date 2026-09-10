defmodule DurableServer.HeartbeatMetricsTest do
  use ExUnit.Case, async: true

  alias DurableServer.HeartbeatMetrics

  @moduletag :capture_log

  test "records attempts with bounded HTTP and transport classifications" do
    supervisor_name = unique_name()
    metrics = HeartbeatMetrics.new(supervisor_name)
    deadline_at = System.monotonic_time(:millisecond) + 5_000
    attach_telemetry(HeartbeatMetrics.events().attempt, supervisor_name)

    HeartbeatMetrics.mark_heartbeat_success(metrics, System.monotonic_time(:millisecond))
    HeartbeatMetrics.mark_watchdog_renewal(metrics, System.monotonic_time(:millisecond))

    HeartbeatMetrics.record_attempt(metrics, %Req.Response{status: 503}, true, deadline_at)

    HeartbeatMetrics.record_attempt(
      metrics,
      %Req.TransportError{reason: :enetunreach},
      true,
      deadline_at
    )

    HeartbeatMetrics.record_attempt(
      metrics,
      %Req.TransportError{reason: {:future_transport_reason, make_ref()}},
      true,
      deadline_at
    )

    HeartbeatMetrics.record_attempt(metrics, %Req.Response{status: 200}, false, deadline_at)

    snapshot = HeartbeatMetrics.snapshot(metrics, 5_000)

    assert snapshot.attempts == %{
             total: 4,
             by_http_status: %{200 => 1, 503 => 1},
             by_transport_class: %{network: 1, other: 1},
             by_result: %{http_error: 1, success: 1, transport_error: 2}
           }

    assert snapshot.consecutive_failures == 0
    assert is_integer(snapshot.last_success_age_ms)
    assert snapshot.remaining_watchdog_budget_ms in 0..5_000

    assert_receive {:telemetry, measurements,
                    %{
                      result: :http_error,
                      http_status: 503,
                      transport_class: :none,
                      retryable: true
                    }}

    assert measurements.consecutive_failures == 1

    assert_receive {:telemetry, _measurements,
                    %{result: :transport_error, transport_class: :network}}

    assert_receive {:telemetry, _measurements,
                    %{result: :transport_error, transport_class: :other} = metadata}

    refute Map.has_key?(metadata, :reason)
    refute Map.has_key?(metadata, :key)

    assert_receive {:telemetry, %{recovered_after_failures: 3},
                    %{result: :success, http_status: 200}}
  end

  test "classifies permanent authentication and configuration failures" do
    assert HeartbeatMetrics.classify_attempt(%Req.Response{status: 403}) == %{
             result: :http_error,
             http_status: 403,
             transport_class: :none,
             error_class: :authentication
           }

    assert HeartbeatMetrics.classify_attempt(%Req.Response{status: 400}) == %{
             result: :http_error,
             http_status: 400,
             transport_class: :none,
             error_class: :configuration
           }
  end

  test "tracks cache degradation until a complete refresh recovers" do
    supervisor_name = unique_name()
    metrics = HeartbeatMetrics.new(supervisor_name)
    attach_telemetry(HeartbeatMetrics.events().cache, supervisor_name)

    assert %{status: :degraded, transition: :entered, degraded_duration_ms: 0} =
             HeartbeatMetrics.record_cache_refresh(metrics, false, 2, 10)

    Process.sleep(5)

    snapshot = HeartbeatMetrics.snapshot(metrics, 5_000)
    assert snapshot.cache_degraded?
    assert snapshot.cache_degraded_duration_ms >= 5

    assert %{status: :healthy, transition: :recovered, degraded_duration_ms: duration_ms} =
             HeartbeatMetrics.record_cache_refresh(metrics, true, 0, 4)

    assert duration_ms >= 5

    refute HeartbeatMetrics.snapshot(metrics, 5_000).cache_degraded?

    assert_receive {:telemetry, %{error_count: 2}, %{status: :degraded, transition: :entered}}

    assert_receive {:telemetry, %{degraded_duration_ms: recovered_duration},
                    %{status: :healthy, transition: :recovered}}

    assert recovered_duration >= 5
  end

  defp attach_telemetry(event, supervisor_name) do
    handler_id = {__MODULE__, self(), make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        event,
        fn _event, measurements, metadata, test_pid ->
          if metadata.supervisor == supervisor_name do
            send(test_pid, {:telemetry, measurements, metadata})
          end
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp unique_name do
    :"heartbeat_metrics_test_#{System.unique_integer([:positive])}"
  end
end
