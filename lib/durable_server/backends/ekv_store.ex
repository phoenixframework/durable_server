defmodule DurableServer.Backends.EKVStore do
  @moduledoc false

  @behaviour DurableServer.StorageBackend

  alias DurableServer.StoredState

  @default_timeout 10_000
  @default_backoff {10, 60}
  @default_cas_retries 5
  @subscribe_ready_timeout_ms 5_000

  @valid_state_opts [
    :name,
    :consistent_reads,
    :cas_retries,
    :backoff,
    :timeout,
    :task_supervisor,
    :ekv_mod,
    :ekv_supervisor_mod
  ]

  @type state :: %{
          required(:name) => atom(),
          required(:consistent_reads) => boolean(),
          required(:cas_retries) => non_neg_integer(),
          required(:backoff) => {non_neg_integer(), non_neg_integer()},
          required(:timeout) => pos_integer() | :infinity,
          required(:task_supervisor) => atom(),
          required(:ekv_mod) => module(),
          required(:ekv_supervisor_mod) => module()
        }

  def normalize_opts(opts) when is_list(opts) do
    opts = Keyword.validate!(opts, @valid_state_opts)
    name = Keyword.fetch!(opts, :name)

    unless is_atom(name) do
      raise ArgumentError, "EKV backend :name must be an atom, got: #{inspect(name)}"
    end

    cas_retries = Keyword.get(opts, :cas_retries, @default_cas_retries)

    unless is_integer(cas_retries) and cas_retries >= 0 do
      raise ArgumentError, "EKV backend :cas_retries must be >= 0, got: #{inspect(cas_retries)}"
    end

    backoff = Keyword.get(opts, :backoff, @default_backoff)

    unless match?(
             {min, max} when is_integer(min) and is_integer(max) and min >= 0 and max >= min,
             backoff
           ) do
      raise ArgumentError,
            "EKV backend :backoff must be {min_ms, max_ms} with min <= max, got: #{inspect(backoff)}"
    end

    timeout = Keyword.get(opts, :timeout, @default_timeout)

    unless timeout == :infinity or (is_integer(timeout) and timeout > 0) do
      raise ArgumentError,
            "EKV backend :timeout must be a positive integer or :infinity, got: #{inspect(timeout)}"
    end

    consistent_reads = Keyword.get(opts, :consistent_reads, true)

    unless is_boolean(consistent_reads) do
      raise ArgumentError,
            "EKV backend :consistent_reads must be boolean, got: #{inspect(consistent_reads)}"
    end

    %{
      name: name,
      consistent_reads: consistent_reads,
      cas_retries: cas_retries,
      backoff: backoff,
      timeout: timeout,
      task_supervisor: Keyword.get(opts, :task_supervisor, DurableServer.TaskSupervisor),
      ekv_mod: Keyword.get(opts, :ekv_mod, :"Elixir.EKV"),
      ekv_supervisor_mod: Keyword.get(opts, :ekv_supervisor_mod, :"Elixir.EKV.Supervisor")
    }
  end

  @impl true
  def init_backend(opts) when is_map(opts), do: opts |> Map.to_list() |> init_backend()

  def init_backend(opts) when is_list(opts) do
    {:ok,
     %{
       state: normalize_opts(opts),
       defaults: %{
         heartbeat_tracking_mode: :subscribe,
         discovery_interval_ms: 3_000,
         heartbeat_interval_ms: 10_000,
         heartbeat_reconcile_interval_ms: 30_000
       },
       features: %{
         heartbeat_subscribe?: true,
         list_includes_body?: true
       }
     }}
  end

  @impl true
  def ensure_ready(%{} = state) do
    with {:ok, config} <- fetch_config(state, state.name),
         :ok <- ensure_cas_config(config) do
      :ok
    end
  end

  @impl true
  def subscribe(%{} = state, subscriber, prefix, opts)
      when is_pid(subscriber) and is_binary(prefix) and is_list(opts) do
    _opts = Keyword.validate!(opts, [])

    with_ekv(state, fn ->
      parent = self()

      {relay_pid, monitor_ref} =
        spawn_monitor(fn ->
          subscription_relay(parent, subscriber, state, prefix)
        end)

      receive do
        {:durable_server_storage_subscribed, ^relay_pid, :ok} ->
          Process.demonitor(monitor_ref, [:flush])
          {:ok, relay_pid}

        {:durable_server_storage_subscribed, ^relay_pid, {:error, reason}} ->
          Process.demonitor(monitor_ref, [:flush])
          {:error, reason}

        {:DOWN, ^monitor_ref, :process, ^relay_pid, reason} ->
          {:error, {:subscription_exit, reason}}
      after
        @subscribe_ready_timeout_ms ->
          Process.exit(relay_pid, :kill)
          {:error, :subscribe_timeout}
      end
    end)
  end

  @impl true
  def unsubscribe(%{} = _state, subscription_ref) when is_pid(subscription_ref) do
    if Process.alive?(subscription_ref) do
      send(subscription_ref, {:durable_server_storage_unsubscribe, self()})

      receive do
        {:durable_server_storage_unsubscribed, ^subscription_ref} ->
          :ok
      after
        @subscribe_ready_timeout_ms ->
          :ok
      end
    else
      :ok
    end
  end

  def unsubscribe(%{} = _state, _subscription_ref), do: :ok

  @impl true
  def get_object(%{} = state, key, opts) when is_binary(key) do
    opts = Keyword.validate!(opts, [:consistent])
    consistent = Keyword.get(opts, :consistent, state.consistent_reads)

    with_ekv(state, fn ->
      case current_value_and_vsn(state, key, consistent: consistent) do
        {:ok, {_value = nil, _vsn = nil}} ->
          {:error, :not_found}

        {:ok, {value, vsn}} ->
          with {:ok, body} <- decode_body(value) do
            {:ok, %{body: body, etag: encode_vsn(vsn)}}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end)
  end

  @impl true
  def list_all_objects_stream(%{} = state, prefix, opts) when is_binary(prefix) do
    {error_handler, stream_opts} =
      Keyword.pop(opts, :error_handler, fn reason -> raise inspect(reason) end)

    stream_opts = Keyword.validate!(stream_opts, [:consistent, :include_objects])
    include_objects = Keyword.get(stream_opts, :include_objects, false)

    case ensure_ready(state) do
      :ok ->
        case if(include_objects, do: ekv_scan(state, prefix), else: ekv_keys(state, prefix)) do
          {:ok, entries} ->
            if include_objects do
              Stream.transform(entries, :ok, fn
                {key, value, vsn}, :ok ->
                  case decode_body(value) do
                    {:ok, body} ->
                      {[%{key: key, etag: encode_vsn(vsn), body: body}], :ok}

                    {:error, reason} ->
                      case error_handler.({:decode_failed, key, reason}) do
                        :halt -> {:halt, :ok}
                        _ -> {[], :ok}
                      end
                  end
              end)
            else
              Stream.map(entries, fn {key, vsn} -> %{key: key, etag: encode_vsn(vsn)} end)
            end

          {:error, reason} ->
            case error_handler.(reason) do
              :continue -> Stream.map([], & &1)
              :halt -> Stream.map([], & &1)
              _ -> Stream.map([], & &1)
            end
        end

      {:error, reason} ->
        case error_handler.(reason) do
          :continue -> Stream.map([], & &1)
          :halt -> Stream.map([], & &1)
          _ -> Stream.map([], & &1)
        end
    end
  end

  @impl true
  def put_object(%{} = state, key, data, opts) when is_binary(key) do
    opts = normalize_put_opts!(opts)
    timeout = Keyword.get(opts, :timeout, state.timeout)

    with_ekv(state, fn ->
      with {:ok, encoded_data} <- encode_body(data) do
        case Keyword.fetch(opts, :etag) do
          {:ok, etag} ->
            case decode_vsn(etag) do
              {:ok, expected_vsn} ->
                do_put_with_expected_vsn(state, key, encoded_data, data, expected_vsn,
                  retries: Keyword.get(opts, :max_retries, 0),
                  timeout: timeout
                )

              :error ->
                {:error, :conflict}
            end

          :error ->
            do_put_latest(state, key, encoded_data, data,
              retries: Keyword.get(opts, :max_retries, state.cas_retries),
              timeout: timeout
            )
        end
      end
    end)
  end

  @impl true
  def delete_object(%{} = state, key) when is_binary(key) do
    with_ekv(state, fn ->
      do_delete(state, key, 0, state.cas_retries, timeout_deadline(state.timeout))
    end)
  end

  @impl true
  def try_claim(%{} = state, key, body) when is_binary(key) do
    with_ekv(state, fn ->
      with {:ok, encoded_body} <- encode_body(body) do
        case ekv_put(state, key, encoded_body,
               if_vsn: nil,
               timeout: state.timeout,
               resolve_unconfirmed: true
             ) do
          {:ok, vsn} ->
            {:ok, {:claimed, encode_vsn(vsn)}}

          {:error, :conflict} ->
            {:error, :already_claimed}

          {:error, reason} ->
            {:error, reason}
        end
      end
    end)
  end

  @impl true
  def update_object(%{} = state, key, update_fn, opts)
      when is_binary(key) and is_function(update_fn, 1) do
    opts =
      Keyword.validate!(opts, [
        :timeout,
        :max_retries,
        :consistent,
        :content_type,
        :task_supervisor,
        :etag,
        :headers,
        :backoff_fun
      ])

    timeout = Keyword.get(opts, :timeout, :infinity)
    max_retries = Keyword.get(opts, :max_retries, 5)
    task_sup = Keyword.get(opts, :task_supervisor, state.task_supervisor)

    with_ekv(state, fn ->
      if timeout == :infinity or timeout == nil do
        do_update(state, key, update_fn, max_retries, 0)
      else
        task =
          Task.Supervisor.async(task_sup, fn ->
            do_update(state, key, update_fn, max_retries, 0)
          end)

        case Task.yield(task, timeout) || Task.shutdown(task) do
          {:ok, result} -> result
          nil -> {:error, :timeout}
        end
      end
    end)
  end

  @impl true
  def encode(%{} = _state, data), do: encode_body(data)

  @impl true
  def decode(%{} = _state, data), do: decode_body(data)

  defp do_update(state, key, update_fn, max_retries, attempt) do
    if attempt > max_retries do
      {:error, :max_retries_exceeded}
    else
      case get_object(state, key, consistent: true) do
        {:ok, %{body: body, etag: etag}} ->
          case update_fn.(%{body: body, etag: etag}) do
            {:ok, new_data} ->
              case put_object(state, key, new_data, etag: etag, max_retries: 0) do
                {:ok, result} ->
                  {:ok, result}

                {:error, :conflict} ->
                  Process.sleep(backoff_for_attempt(state.backoff, attempt))
                  do_update(state, key, update_fn, max_retries, attempt + 1)

                {:error, reason} ->
                  {:error, reason}
              end

            {:error, reason} ->
              {:error, reason}
          end

        {:error, :not_found} ->
          {:error, :not_found}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp do_delete(state, key, attempt, max_retries, deadline_at) do
    case current_vsn(state, key) do
      {:ok, nil} ->
        {:error, :not_found}

      {:ok, vsn} ->
        case remaining_timeout(deadline_at) do
          timeout when is_integer(timeout) and timeout <= 0 ->
            {:error, :timeout}

          timeout ->
            case ekv_delete(state, key,
                   if_vsn: vsn,
                   timeout: timeout,
                   resolve_unconfirmed: true
                 ) do
              {:ok, _new_vsn} ->
                :ok

              {:error, :conflict} when attempt < max_retries ->
                sleep_with_deadline(state.backoff, deadline_at, attempt)
                do_delete(state, key, attempt + 1, max_retries, deadline_at)

              {:error, :conflict} ->
                case current_vsn(state, key) do
                  {:ok, nil} -> {:error, :not_found}
                  {:ok, _} -> {:error, :conflict}
                  {:error, reason} -> {:error, reason}
                end

              {:error, reason} when attempt < max_retries ->
                if retryable_error?(reason) do
                  sleep_with_deadline(state.backoff, deadline_at, attempt)
                  do_delete(state, key, attempt + 1, max_retries, deadline_at)
                else
                  {:error, reason}
                end

              {:error, reason} ->
                {:error, reason}
            end
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_put_with_expected_vsn(state, key, encoded_data, data, expected_vsn, opts) do
    retries = Keyword.fetch!(opts, :retries)
    deadline_at = timeout_deadline(Keyword.fetch!(opts, :timeout))

    do_put_with_expected_vsn(
      state,
      key,
      encoded_data,
      data,
      expected_vsn,
      retries,
      deadline_at,
      0
    )
  end

  defp do_put_with_expected_vsn(
         _state,
         _key,
         _encoded_data,
         _data,
         _expected_vsn,
         retries,
         _deadline_at,
         attempt
       )
       when attempt > retries do
    {:error, :conflict}
  end

  defp do_put_with_expected_vsn(
         state,
         key,
         encoded_data,
         data,
         expected_vsn,
         retries,
         deadline_at,
         attempt
       ) do
    case remaining_timeout(deadline_at) do
      timeout when is_integer(timeout) and timeout <= 0 ->
        {:error, :timeout}

      timeout ->
        case ekv_put(state, key, encoded_data,
               if_vsn: expected_vsn,
               timeout: timeout,
               resolve_unconfirmed: true
             ) do
          {:ok, vsn} ->
            {:ok, %{etag: encode_vsn(vsn), body: data}}

          {:error, :conflict} ->
            {:error, :conflict}

          {:error, reason} when attempt < retries ->
            if retryable_error?(reason) do
              sleep_with_deadline(state.backoff, deadline_at, attempt)

              do_put_with_expected_vsn(
                state,
                key,
                encoded_data,
                data,
                expected_vsn,
                retries,
                deadline_at,
                attempt + 1
              )
            else
              {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp do_put_latest(state, key, encoded_data, data, opts) do
    retries = Keyword.fetch!(opts, :retries)
    deadline_at = timeout_deadline(Keyword.fetch!(opts, :timeout))
    do_put_latest(state, key, encoded_data, data, retries, deadline_at, 0)
  end

  defp do_put_latest(_state, _key, _encoded_data, _data, retries, _deadline_at, attempt)
       when attempt > retries do
    {:error, :conflict}
  end

  defp do_put_latest(state, key, encoded_data, data, retries, deadline_at, attempt) do
    case remaining_timeout(deadline_at) do
      timeout when is_integer(timeout) and timeout <= 0 ->
        {:error, :timeout}

      timeout ->
        case ekv_update(state, key, {__MODULE__, :put_latest_update, [encoded_data]},
               timeout: timeout,
               retries: 0,
               resolve_unconfirmed: true
             ) do
          {:ok, _new_value, vsn} ->
            {:ok, %{etag: encode_vsn(vsn), body: data}}

          {:error, :conflict} when attempt < retries ->
            sleep_with_deadline(state.backoff, deadline_at, attempt)
            do_put_latest(state, key, encoded_data, data, retries, deadline_at, attempt + 1)

          {:error, :conflict} ->
            {:error, :conflict}

          {:error, reason} when attempt < retries ->
            if retryable_error?(reason) do
              sleep_with_deadline(state.backoff, deadline_at, attempt)
              do_put_latest(state, key, encoded_data, data, retries, deadline_at, attempt + 1)
            else
              {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @doc false
  def put_latest_update(_current_value, encoded_data), do: encoded_data

  defp current_vsn(state, key) do
    case ekv_lookup(state, key) do
      {:ok, nil} -> {:ok, nil}
      {:ok, {_value, vsn}} -> {:ok, vsn}
      {:error, reason} -> {:error, reason}
    end
  end

  defp current_value_and_vsn(state, key, opts) do
    consistent = Keyword.get(opts, :consistent, false)

    if consistent do
      case consistent_read_barrier(state, key) do
        :ok ->
          :ok

        {:error, reason} ->
          return_error({:consistent_read_failed, reason})
      end
    end

    case ekv_lookup(state, key) do
      {:ok, nil} -> {:ok, {nil, nil}}
      {:ok, {value, vsn}} -> {:ok, {value, vsn}}
      {:error, reason} -> {:error, reason}
    end
  catch
    {:return_error, reason} -> {:error, reason}
  end

  defp return_error(reason), do: throw({:return_error, reason})

  defp consistent_read_barrier(state, key) do
    do_consistent_read_barrier(state, key, state.cas_retries, timeout_deadline(state.timeout), 0)
  end

  defp do_consistent_read_barrier(_state, _key, retries, _deadline_at, attempt)
       when attempt > retries do
    {:error, :timeout}
  end

  defp do_consistent_read_barrier(state, key, retries, deadline_at, attempt) do
    case remaining_timeout(deadline_at) do
      timeout when is_integer(timeout) and timeout <= 0 ->
        {:error, :timeout}

      timeout ->
        case ekv_get(state, key, consistent: true, timeout: timeout) do
          {:ok, _value} ->
            :ok

          {:error, reason} ->
            if attempt < retries and retryable_error?(reason) do
              sleep_with_deadline(state.backoff, deadline_at, attempt)
              do_consistent_read_barrier(state, key, retries, deadline_at, attempt + 1)
            else
              {:error, reason}
            end
        end
    end
  end

  defp normalize_put_opts!(opts) do
    Keyword.validate!(opts, [
      :content_type,
      :consistent,
      :headers,
      :backoff_fun,
      :timeout,
      :task_supervisor,
      :max_retries,
      :max_results,
      :continuation_token,
      :prefix,
      :etag
    ])
  end

  defp encode_vsn(vsn) do
    vsn
    |> :erlang.term_to_binary()
    |> Base.url_encode64(padding: false)
  end

  defp decode_vsn(etag) when is_binary(etag) do
    with {:ok, bin} <- Base.url_decode64(etag, padding: false),
         {ts, origin} <- :erlang.binary_to_term(bin),
         true <- is_integer(ts) do
      {:ok, {ts, origin}}
    else
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp retryable_error?(reason) do
    reason in [
      :timeout,
      :no_quorum,
      :quorum_timeout,
      :unavailable,
      :cluster_overflow,
      :cluster_not_ready
    ]
  end

  defp with_ekv(state, fun) when is_function(fun, 0) do
    case ensure_ready(state) do
      :ok -> fun.()
      {:error, _} = error -> error
    end
  end

  defp fetch_config(state, name) when is_atom(name) do
    try do
      {:ok, ekv_get_config(state, name)}
    rescue
      _ -> {:error, {:ekv_not_started, name}}
    catch
      _, _ -> {:error, {:ekv_not_started, name}}
    end
  end

  defp ensure_cas_config(%{mode: :client}) do
    :ok
  end

  defp ensure_cas_config(%{cluster_size: nil}) do
    {:error, :ekv_cas_not_configured}
  end

  defp ensure_cas_config(_), do: :ok

  defp backoff_for_attempt({min_ms, max_ms}, _attempt) when min_ms == max_ms, do: min_ms

  defp backoff_for_attempt({min_ms, max_ms}, _attempt) do
    :rand.uniform(max_ms - min_ms + 1) + min_ms - 1
  end

  defp sleep_with_deadline(backoff, deadline_at, attempt) do
    sleep_ms =
      case remaining_timeout(deadline_at) do
        :infinity -> backoff_for_attempt(backoff, attempt)
        remaining_ms -> min(backoff_for_attempt(backoff, attempt), remaining_ms)
      end

    if sleep_ms > 0 do
      Process.sleep(sleep_ms)
    end
  end

  defp timeout_deadline(:infinity), do: :infinity

  defp timeout_deadline(timeout) when is_integer(timeout) and timeout > 0 do
    System.monotonic_time(:millisecond) + timeout
  end

  defp remaining_timeout(:infinity), do: :infinity

  defp remaining_timeout(deadline_at) when is_integer(deadline_at) do
    max(deadline_at - System.monotonic_time(:millisecond), 0)
  end

  defp subscription_relay(parent, subscriber, state, prefix) do
    monitor_ref = Process.monitor(subscriber)

    case ekv_subscribe(state, prefix) do
      {:ok, :ok} ->
        send(parent, {:durable_server_storage_subscribed, self(), :ok})
        subscription_relay_loop(subscriber, state, prefix, monitor_ref)

      {:error, reason} ->
        send(parent, {:durable_server_storage_subscribed, self(), {:error, reason}})
    end
  end

  defp subscription_relay_loop(subscriber, state, prefix, monitor_ref) do
    receive do
      {:durable_server_storage_unsubscribe, from} ->
        _ = ekv_unsubscribe(state, prefix)
        send(from, {:durable_server_storage_unsubscribed, self()})
        :ok

      {:DOWN, ^monitor_ref, :process, ^subscriber, _reason} ->
        _ = ekv_unsubscribe(state, prefix)
        :ok

      {:ekv, events, %{name: name}} when is_list(events) and name == state.name ->
        normalized_events =
          events
          |> Enum.flat_map(&normalize_ekv_event/1)

        if normalized_events != [] do
          send(subscriber, {:durable_server_storage_events, normalized_events})
        end

        subscription_relay_loop(subscriber, state, prefix, monitor_ref)

      _other ->
        subscription_relay_loop(subscriber, state, prefix, monitor_ref)
    end
  end

  defp normalize_ekv_event(%{type: :put, key: key, value: value}) when is_binary(key) do
    case decode_body(value) do
      {:ok, decoded_value} -> [%{type: :put, key: key, value: decoded_value}]
      {:error, _reason} -> []
    end
  end

  defp normalize_ekv_event(%{type: :delete, key: key, value: value}) when is_binary(key) do
    case decode_body(value) do
      {:ok, decoded_value} -> [%{type: :delete, key: key, value: decoded_value}]
      {:error, _reason} -> []
    end
  end

  defp normalize_ekv_event(_event), do: []

  defp encode_body(%StoredState{} = stored_state) do
    {:ok, StoredState.to_storage_term(stored_state)}
  rescue
    error in [ArgumentError, RuntimeError] -> {:error, error}
  end

  defp encode_body(data), do: {:ok, data}

  defp decode_body(value) do
    case StoredState.from_storage_term(value) do
      {:ok, stored_state} -> {:ok, stored_state}
      :not_stored_state -> {:ok, value}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ekv_get_config(%{ekv_supervisor_mod: mod}, name), do: apply(mod, :get_config, [name])

  defp ekv_keys(%{ekv_mod: mod, name: name}, prefix),
    do: ekv_raw_call(fn -> apply(mod, :keys, [name, prefix]) end)

  defp ekv_scan(%{ekv_mod: mod, name: name}, prefix),
    do: ekv_raw_call(fn -> apply(mod, :scan, [name, prefix]) end)

  defp ekv_lookup(%{ekv_mod: mod, name: name}, key),
    do: ekv_raw_call(fn -> apply(mod, :lookup, [name, key]) end)

  defp ekv_put(%{ekv_mod: mod, name: name}, key, value, opts),
    do: ekv_result_call(fn -> apply(mod, :put, [name, key, value, opts]) end)

  defp ekv_update(%{ekv_mod: mod, name: name}, key, fun, opts),
    do: ekv_result_call(fn -> apply(mod, :update, [name, key, fun, opts]) end)

  defp ekv_get(%{ekv_mod: mod, name: name}, key, opts),
    do: ekv_result_call(fn -> apply(mod, :get, [name, key, opts]) end)

  defp ekv_delete(%{ekv_mod: mod, name: name}, key, opts),
    do: ekv_result_call(fn -> apply(mod, :delete, [name, key, opts]) end)

  defp ekv_subscribe(%{ekv_mod: mod, name: name}, prefix),
    do: ekv_raw_call(fn -> apply(mod, :subscribe, [name, prefix]) end)

  defp ekv_unsubscribe(%{ekv_mod: mod, name: name}, prefix),
    do: ekv_raw_call(fn -> apply(mod, :unsubscribe, [name, prefix]) end)

  defp ekv_raw_call(fun) when is_function(fun, 0) do
    {:ok, fun.()}
  rescue
    error ->
      {:error, normalize_runtime_error(error)}
  catch
    :exit, reason ->
      {:error, normalize_exit_reason(reason)}
  end

  defp ekv_result_call(fun) when is_function(fun, 0) do
    case ekv_raw_call(fun) do
      {:ok, {:ok, _} = ok} -> ok
      {:ok, {:ok, _, _} = ok} -> ok
      {:ok, {:error, _} = error} -> error
      {:ok, other} -> {:ok, other}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_runtime_error(%RuntimeError{message: "EKV: consistent read failed: " <> reason}) do
    normalize_consistent_read_reason(String.trim(reason))
  end

  defp normalize_runtime_error(%RuntimeError{message: message}), do: {:ekv_runtime_error, message}
  defp normalize_runtime_error(error), do: {:ekv_runtime_error, Exception.message(error)}

  defp normalize_consistent_read_reason(":timeout"), do: :timeout
  defp normalize_consistent_read_reason(":no_quorum"), do: :no_quorum
  defp normalize_consistent_read_reason(":quorum_timeout"), do: :quorum_timeout
  defp normalize_consistent_read_reason(":unavailable"), do: :unavailable
  defp normalize_consistent_read_reason(":cluster_overflow"), do: :cluster_overflow
  defp normalize_consistent_read_reason(":cluster_not_ready"), do: :cluster_not_ready
  defp normalize_consistent_read_reason(other), do: {:consistent_read_failed, other}

  defp normalize_exit_reason(:timeout), do: :timeout
  defp normalize_exit_reason({:timeout, _}), do: :timeout
  defp normalize_exit_reason({:shutdown, reason}), do: normalize_exit_reason(reason)
  defp normalize_exit_reason({:noproc, _}), do: :unavailable
  defp normalize_exit_reason({:nodedown, _}), do: :unavailable
  defp normalize_exit_reason(reason), do: {:ekv_exit, reason}
end
