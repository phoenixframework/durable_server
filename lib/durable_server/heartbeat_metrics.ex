defmodule DurableServer.HeartbeatMetrics do
  @moduledoc """
  Heartbeat observability with bounded telemetry dimensions.

  The lifecycle manager emits the following events:

    * `[:durable_server, :heartbeat, :attempt]` for every backend/HTTP attempt
    * `[:durable_server, :heartbeat, :cache]` after every heartbeat-cache refresh
    * `[:durable_server, :heartbeat, :watchdog, :termination]` when the watchdog
      fences a supervisor tree

  Attempt metadata deliberately contains classifications rather than raw errors,
  URLs, or keys. This keeps metric series bounded and avoids leaking request data.
  """

  require Logger

  @attempt_event [:durable_server, :heartbeat, :attempt]
  @cache_event [:durable_server, :heartbeat, :cache]
  @watchdog_event [:durable_server, :heartbeat, :watchdog, :termination]

  @enforce_keys [:supervisor_name, :table, :started_at]
  defstruct [:supervisor_name, :table, :started_at]

  @type t :: %__MODULE__{
          supervisor_name: atom(),
          table: :ets.tid(),
          started_at: integer()
        }

  @type attempt_classification :: %{
          result: :success | :http_error | :transport_error | :protocol_error | :backend_error,
          http_status: 100..599 | :none | :other,
          transport_class:
            :none
            | :timeout
            | :closed
            | :connection_refused
            | :dns
            | :tls
            | :network
            | :socket
            | :other,
          error_class:
            :none
            | :http
            | :authentication
            | :configuration
            | :transport
            | :protocol
            | :conflict
            | :unavailable
            | :timeout
            | :other
        }

  @doc """
  Returns the telemetry event names emitted for heartbeat operations.
  """
  def events do
    %{
      attempt: @attempt_event,
      cache: @cache_event,
      watchdog_termination: @watchdog_event
    }
  end

  @doc false
  @spec new(atom()) :: t()
  def new(supervisor_name) when is_atom(supervisor_name) do
    table =
      :ets.new(__MODULE__, [
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])

    :ets.insert(table, [
      {:attempt_count, 0},
      {:consecutive_failures, 0}
    ])

    %__MODULE__{
      supervisor_name: supervisor_name,
      table: table,
      started_at: System.monotonic_time(:millisecond)
    }
  end

  @doc false
  @spec record_attempt(t(), term(), boolean(), integer()) :: attempt_classification() | :ignored
  def record_attempt(
        %__MODULE__{} = metrics,
        response_or_error,
        retryable?,
        deadline_at
      )
      when is_boolean(retryable?) and is_integer(deadline_at) do
    classification = classify_attempt(response_or_error)
    total_attempts = increment(metrics, :attempt_count)
    increment(metrics, {:attempt_result, classification.result})

    if classification.http_status != :none do
      increment(metrics, {:attempt_http_status, classification.http_status})
    end

    if classification.transport_class != :none do
      increment(metrics, {:attempt_transport_class, classification.transport_class})
    end

    previous_failures = counter(metrics, :consecutive_failures)

    {consecutive_failures, recovered_after_failures} =
      if classification.result == :success do
        :ets.insert(metrics.table, {:consecutive_failures, 0})
        {0, previous_failures}
      else
        {increment(metrics, :consecutive_failures), 0}
      end

    now = System.monotonic_time(:millisecond)
    {cache_degraded?, cache_degraded_duration_ms} = cache_degraded(metrics, now)
    last_success_age_ms = last_success_age(metrics, now)

    measurements = %{
      count: 1,
      total_attempts: total_attempts,
      consecutive_failures: consecutive_failures,
      recovered_after_failures: recovered_after_failures,
      last_success_age_ms: last_success_age_ms || 0,
      remaining_watchdog_budget_ms: max(deadline_at - now, 0),
      cache_degraded_duration_ms: cache_degraded_duration_ms
    }

    metadata =
      Map.merge(classification, %{
        supervisor: metrics.supervisor_name,
        retryable: retryable?,
        cache_degraded: cache_degraded?,
        has_last_success: is_integer(last_success_age_ms)
      })

    :telemetry.execute(@attempt_event, measurements, metadata)
    maybe_log_attempt(metrics, response_or_error, measurements, metadata)

    classification
  rescue
    ArgumentError ->
      # The owning lifecycle manager may disappear while its heartbeat task is
      # being fenced. Observability must never turn that race into a new failure.
      :ignored
  end

  @doc false
  @spec successful_attempt?(term()) :: boolean()
  def successful_attempt?(response_or_error) do
    classify_attempt(response_or_error).result == :success
  end

  @doc false
  @spec mark_heartbeat_success(t(), integer()) :: :ok
  def mark_heartbeat_success(%__MODULE__{} = metrics, monotonic_at)
      when is_integer(monotonic_at) do
    :ets.insert(metrics.table, {:last_success_tick, tick(metrics, monotonic_at)})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc false
  @spec mark_watchdog_renewal(t(), integer()) :: :ok
  def mark_watchdog_renewal(%__MODULE__{} = metrics, monotonic_at)
      when is_integer(monotonic_at) do
    :ets.insert(metrics.table, {:watchdog_renewal_tick, tick(metrics, monotonic_at)})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc false
  @spec record_cache_refresh(t(), boolean(), non_neg_integer(), non_neg_integer()) :: map()
  def record_cache_refresh(
        %__MODULE__{} = metrics,
        complete?,
        error_count,
        refresh_duration_ms
      )
      when is_boolean(complete?) and is_integer(error_count) and error_count >= 0 and
             is_integer(refresh_duration_ms) and refresh_duration_ms >= 0 do
    now = System.monotonic_time(:millisecond)
    now_tick = tick(metrics, now)

    {status, transition, degraded_duration_ms} =
      if complete? do
        case :ets.take(metrics.table, :cache_degraded_since_tick) do
          [{:cache_degraded_since_tick, degraded_since}] ->
            {:healthy, :recovered, max(now_tick - degraded_since, 0)}

          [] ->
            {:healthy, :unchanged, 0}
        end
      else
        entered? =
          :ets.insert_new(
            metrics.table,
            {:cache_degraded_since_tick, now_tick}
          )

        degraded_since =
          if entered? do
            now_tick
          else
            value(metrics, :cache_degraded_since_tick)
          end

        transition = if entered?, do: :entered, else: :continued
        {:degraded, transition, max(now_tick - degraded_since, 0)}
      end

    measurements = %{
      count: 1,
      error_count: error_count,
      refresh_duration_ms: refresh_duration_ms,
      degraded_duration_ms: degraded_duration_ms
    }

    metadata = %{
      supervisor: metrics.supervisor_name,
      status: status,
      transition: transition
    }

    :telemetry.execute(@cache_event, measurements, metadata)

    Map.merge(measurements, metadata)
  rescue
    ArgumentError ->
      %{status: :unknown, transition: :unchanged, degraded_duration_ms: 0}
  end

  @doc false
  @spec watchdog_termination(atom(), map(), map()) :: :ok
  def watchdog_termination(supervisor_name, measurements, metadata)
      when is_atom(supervisor_name) and is_map(measurements) and is_map(metadata) do
    :telemetry.execute(
      @watchdog_event,
      Map.put(measurements, :count, 1),
      Map.put(metadata, :supervisor, supervisor_name)
    )

    :ok
  end

  @doc false
  @spec snapshot(t(), pos_integer()) :: map()
  def snapshot(%__MODULE__{} = metrics, watchdog_deadline_ms)
      when is_integer(watchdog_deadline_ms) and watchdog_deadline_ms > 0 do
    now = System.monotonic_time(:millisecond)
    entries = :ets.tab2list(metrics.table)
    {cache_degraded?, cache_degraded_duration_ms} = cache_degraded(metrics, now)
    last_success_age_ms = last_success_age(metrics, now)
    watchdog_age_ms = elapsed_since(metrics, :watchdog_renewal_tick, now)

    %{
      attempts: %{
        total: entry_value(entries, :attempt_count, 0),
        by_http_status: grouped_attempts(entries, :attempt_http_status),
        by_transport_class: grouped_attempts(entries, :attempt_transport_class),
        by_result: grouped_attempts(entries, :attempt_result)
      },
      consecutive_failures: entry_value(entries, :consecutive_failures, 0),
      last_success_age_ms: last_success_age_ms,
      remaining_watchdog_budget_ms:
        if(is_integer(watchdog_age_ms),
          do: max(watchdog_deadline_ms - watchdog_age_ms, 0),
          else: nil
        ),
      cache_degraded?: cache_degraded?,
      cache_degraded_duration_ms: cache_degraded_duration_ms
    }
  rescue
    ArgumentError ->
      %{
        attempts: %{total: 0, by_http_status: %{}, by_transport_class: %{}, by_result: %{}},
        consecutive_failures: 0,
        last_success_age_ms: nil,
        remaining_watchdog_budget_ms: nil,
        cache_degraded?: false,
        cache_degraded_duration_ms: 0
      }
  end

  @doc false
  @spec classify_attempt(term()) :: attempt_classification()
  def classify_attempt({:ok, _result}), do: success_classification()
  def classify_attempt(:ok), do: success_classification()
  def classify_attempt({:error, reason}), do: classify_attempt(reason)
  def classify_attempt({:mirror_failed, reason}), do: classify_attempt(reason)

  def classify_attempt(%Req.Response{status: status}) when status in 200..299 do
    %{
      result: :success,
      http_status: normalize_http_status(status),
      transport_class: :none,
      error_class: :none
    }
  end

  def classify_attempt(%Req.Response{status: status}) do
    %{
      result: :http_error,
      http_status: normalize_http_status(status),
      transport_class: :none,
      error_class: http_error_class(status)
    }
  end

  def classify_attempt(%Req.TransportError{reason: reason}) do
    %{
      result: :transport_error,
      http_status: :none,
      transport_class: transport_class(reason),
      error_class: :transport
    }
  end

  def classify_attempt(%Req.HTTPError{}) do
    %{
      result: :protocol_error,
      http_status: :none,
      transport_class: :none,
      error_class: :protocol
    }
  end

  def classify_attempt(%ArgumentError{}) do
    backend_error_classification(:configuration)
  end

  def classify_attempt({:raised, :error, %ArgumentError{}}),
    do: backend_error_classification(:configuration)

  def classify_attempt({:raised, kind, _reason}) when kind in [:error, :exit, :throw],
    do: backend_error_classification(:other)

  def classify_attempt(reason)
      when reason in [
             :invalid_credentials,
             :unauthorized,
             :forbidden,
             :invalid_configuration,
             :unsupported
           ] do
    backend_error_classification(:configuration)
  end

  def classify_attempt(reason) when reason in [:conflict, :already_exists] do
    backend_error_classification(:conflict)
  end

  def classify_attempt(reason)
      when reason in [
             :no_quorum,
             :quorum_timeout,
             :unavailable,
             :temporarily_unavailable,
             :cluster_overflow,
             :cluster_not_ready
           ] do
    backend_error_classification(:unavailable)
  end

  def classify_attempt(:timeout), do: backend_error_classification(:timeout)
  def classify_attempt(_reason), do: backend_error_classification(:other)

  defp success_classification do
    %{
      result: :success,
      http_status: :none,
      transport_class: :none,
      error_class: :none
    }
  end

  defp backend_error_classification(error_class) do
    %{
      result: :backend_error,
      http_status: :none,
      transport_class: :none,
      error_class: error_class
    }
  end

  defp normalize_http_status(status) when status in 100..599, do: status
  defp normalize_http_status(_status), do: :other

  defp http_error_class(status) when status in [401, 403], do: :authentication
  defp http_error_class(status) when status in [400, 404], do: :configuration
  defp http_error_class(_status), do: :http

  defp transport_class(:timeout), do: :timeout
  defp transport_class(:closed), do: :closed
  defp transport_class(:econnrefused), do: :connection_refused

  defp transport_class(reason)
       when reason in [:nxdomain, :eai_again, :eai_fail, :eai_noname, :host_not_found],
       do: :dns

  defp transport_class(reason)
       when reason in [
              :enetdown,
              :enetreset,
              :enetunreach,
              :econnaborted,
              :econnreset,
              :ehostdown,
              :ehostunreach
            ],
       do: :network

  defp transport_class(:protocol_not_negotiated), do: :tls
  defp transport_class({:bad_alpn_protocol, _protocol}), do: :tls
  defp transport_class({:tls_alert, _alert}), do: :tls
  defp transport_class({:bad_cert, _reason}), do: :tls
  defp transport_class(reason) when is_atom(reason), do: :socket
  defp transport_class(_reason), do: :other

  defp increment(%__MODULE__{table: table}, key) do
    :ets.update_counter(table, key, {2, 1}, {key, 0})
  end

  defp counter(%__MODULE__{} = metrics, key), do: value(metrics, key, 0)

  defp value(%__MODULE__{table: table}, key, default \\ nil) do
    case :ets.lookup(table, key) do
      [{^key, value}] -> value
      [] -> default
    end
  end

  defp tick(%__MODULE__{started_at: started_at}, monotonic_at) do
    max(monotonic_at - started_at + 1, 1)
  end

  defp elapsed_since(%__MODULE__{} = metrics, key, now) do
    case value(metrics, key) do
      stored_tick when is_integer(stored_tick) -> max(tick(metrics, now) - stored_tick, 0)
      nil -> nil
    end
  end

  defp last_success_age(%__MODULE__{} = metrics, now) do
    elapsed_since(metrics, :last_success_tick, now)
  end

  defp cache_degraded(%__MODULE__{} = metrics, now) do
    case value(metrics, :cache_degraded_since_tick) do
      degraded_since when is_integer(degraded_since) ->
        {true, max(tick(metrics, now) - degraded_since, 0)}

      nil ->
        {false, 0}
    end
  end

  defp entry_value(entries, key, default) do
    case List.keyfind(entries, key, 0) do
      {^key, value} -> value
      nil -> default
    end
  end

  defp grouped_attempts(entries, group) do
    for {{^group, classification}, count} <- entries, into: %{} do
      {classification, count}
    end
  end

  defp maybe_log_attempt(
         %__MODULE__{supervisor_name: supervisor_name},
         _response_or_error,
         measurements,
         %{result: :success}
       )
       when measurements.recovered_after_failures > 0 do
    Logger.info(
      "#{inspect(supervisor_name)}: heartbeat request path recovered after " <>
        "#{measurements.recovered_after_failures} consecutive failed attempt(s); " <>
        "last_success_age_ms=#{measurements.last_success_age_ms} " <>
        "remaining_watchdog_budget_ms=#{measurements.remaining_watchdog_budget_ms}",
      durable_server_supervisor: supervisor_name,
      heartbeat_result: :success
    )
  end

  defp maybe_log_attempt(
         %__MODULE__{supervisor_name: supervisor_name},
         response_or_error,
         measurements,
         %{result: result} = metadata
       )
       when result != :success do
    if not metadata.retryable or log_failure_count?(measurements.consecutive_failures) do
      Logger.warning(
        "#{inspect(supervisor_name)}: heartbeat attempt failed; " <>
          "result=#{metadata.result} http_status=#{metadata.http_status} " <>
          "transport_class=#{metadata.transport_class} error_class=#{metadata.error_class} " <>
          attempt_detail(response_or_error) <>
          "retryable=#{metadata.retryable} " <>
          "has_last_success=#{metadata.has_last_success} " <>
          "consecutive_failures=#{measurements.consecutive_failures} " <>
          "last_success_age_ms=#{measurements.last_success_age_ms} " <>
          "remaining_watchdog_budget_ms=#{measurements.remaining_watchdog_budget_ms}",
        durable_server_supervisor: supervisor_name,
        heartbeat_result: metadata.result,
        heartbeat_http_status: metadata.http_status,
        heartbeat_transport_class: metadata.transport_class,
        heartbeat_retryable: metadata.retryable
      )
    end
  end

  defp maybe_log_attempt(_metrics, _response_or_error, _measurements, _metadata), do: :ok

  defp attempt_detail(%Req.TransportError{reason: reason}) do
    "transport_reason=#{inspect(reason, limit: 20, printable_limit: 200)} "
  end

  defp attempt_detail({:error, reason}), do: attempt_detail(reason)
  defp attempt_detail({:mirror_failed, reason}), do: attempt_detail(reason)
  defp attempt_detail(_response_or_error), do: ""

  defp log_failure_count?(count) when count <= 3, do: true
  defp log_failure_count?(count) when count > 0, do: Bitwise.band(count, count - 1) == 0
end
