defmodule DurableServer.StorageBackend.EKV do
  @moduledoc false

  @behaviour DurableServer.StorageBackend

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
    :task_supervisor
  ]

  @type state :: %{
          required(:name) => atom(),
          required(:consistent_reads) => boolean(),
          required(:cas_retries) => non_neg_integer(),
          required(:backoff) => {non_neg_integer(), non_neg_integer()},
          required(:timeout) => pos_integer() | :infinity,
          required(:task_supervisor) => atom()
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
      task_supervisor: Keyword.get(opts, :task_supervisor, DurableServer.TaskSupervisor)
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
         discovery_interval_ms: 5_000,
         heartbeat_interval_ms: 5_000,
         heartbeat_reconcile_interval_ms: 30_000
       },
       features: %{
         heartbeat_subscribe?: true
       }
     }}
  end

  @impl true
  def ensure_ready(%{name: name} = _state) do
    with {:ok, config} <- fetch_config(name),
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
          subscription_relay(parent, subscriber, state.name, prefix)
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

        {:ok, {value, vsn}} when is_binary(value) ->
          {:ok, %{body: value, etag: encode_vsn(vsn)}}

        {:ok, {value, _vsn}} ->
          {:error, {:unexpected_value_type, value}}

        {:error, reason} ->
          {:error, reason}
      end
    end)
  end

  @impl true
  def list_all_objects_stream(%{} = state, prefix, opts) when is_binary(prefix) do
    {error_handler, stream_opts} =
      Keyword.pop(opts, :error_handler, fn reason -> raise inspect(reason) end)

    _stream_opts = Keyword.validate!(stream_opts, [:consistent])

    case ensure_ready(state) do
      :ok ->
        state.name
        |> ekv_keys(prefix)
        |> Stream.map(fn {key, vsn} -> %{key: key, etag: encode_vsn(vsn)} end)

      {:error, reason} ->
        case error_handler.(reason) do
          :continue -> Stream.map([], & &1)
          :halt -> Stream.map([], & &1)
          _ -> Stream.map([], & &1)
        end
    end
  end

  @impl true
  def put_object(%{} = state, key, data, opts) when is_binary(key) and is_binary(data) do
    opts = normalize_put_opts!(opts)
    timeout = Keyword.get(opts, :timeout, state.timeout)

    with_ekv(state, fn ->
      case Keyword.fetch(opts, :etag) do
        {:ok, etag} ->
          case decode_vsn(etag) do
            {:ok, expected_vsn} ->
              do_put_with_expected_vsn(state, key, data, expected_vsn,
                retries: Keyword.get(opts, :max_retries, 0),
                timeout: timeout
              )

            :error ->
              {:error, :conflict}
          end

        :error ->
          do_put_latest(state, key, data,
            retries: Keyword.get(opts, :max_retries, state.cas_retries),
            timeout: timeout
          )
      end
    end)
  end

  @impl true
  def delete_object(%{} = state, key) when is_binary(key) do
    with_ekv(state, fn ->
      do_delete(state, key, 0, state.cas_retries)
    end)
  end

  @impl true
  def try_claim(%{} = state, key, body) when is_binary(key) and is_binary(body) do
    with_ekv(state, fn ->
      case ekv_put(state.name, key, body,
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

  defp do_delete(state, key, attempt, max_retries) do
    case current_vsn(state, key) do
      nil ->
        {:error, :not_found}

      vsn ->
        case ekv_delete(state.name, key,
               if_vsn: vsn,
               timeout: state.timeout,
               resolve_unconfirmed: true
             ) do
          {:ok, _new_vsn} ->
            :ok

          {:error, :conflict} when attempt < max_retries ->
            Process.sleep(backoff_for_attempt(state.backoff, attempt))
            do_delete(state, key, attempt + 1, max_retries)

          {:error, :conflict} ->
            case current_vsn(state, key) do
              nil -> {:error, :not_found}
              _ -> {:error, :conflict}
            end

          {:error, reason} when attempt < max_retries ->
            if retryable_error?(reason) do
              Process.sleep(backoff_for_attempt(state.backoff, attempt))
              do_delete(state, key, attempt + 1, max_retries)
            else
              {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp do_put_with_expected_vsn(state, key, data, expected_vsn, opts) do
    retries = Keyword.fetch!(opts, :retries)
    timeout = Keyword.fetch!(opts, :timeout)

    do_put_with_expected_vsn(state, key, data, expected_vsn, retries, timeout, 0)
  end

  defp do_put_with_expected_vsn(_state, _key, _data, _expected_vsn, retries, _timeout, attempt)
       when attempt > retries do
    {:error, :conflict}
  end

  defp do_put_with_expected_vsn(state, key, data, expected_vsn, retries, timeout, attempt) do
    case ekv_put(state.name, key, data,
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
          Process.sleep(backoff_for_attempt(state.backoff, attempt))
          do_put_with_expected_vsn(state, key, data, expected_vsn, retries, timeout, attempt + 1)
        else
          {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_put_latest(state, key, data, opts) do
    retries = Keyword.fetch!(opts, :retries)
    timeout = Keyword.fetch!(opts, :timeout)
    do_put_latest(state, key, data, retries, timeout, 0)
  end

  defp do_put_latest(_state, _key, _data, retries, _timeout, attempt) when attempt > retries do
    {:error, :conflict}
  end

  defp do_put_latest(state, key, data, retries, timeout, attempt) do
    expected_vsn = current_vsn(state, key)

    case ekv_put(state.name, key, data,
           if_vsn: expected_vsn,
           timeout: timeout,
           resolve_unconfirmed: true
         ) do
      {:ok, vsn} ->
        {:ok, %{etag: encode_vsn(vsn), body: data}}

      {:error, :conflict} when attempt < retries ->
        Process.sleep(backoff_for_attempt(state.backoff, attempt))
        do_put_latest(state, key, data, retries, timeout, attempt + 1)

      {:error, :conflict} ->
        {:error, :conflict}

      {:error, reason} when attempt < retries ->
        if retryable_error?(reason) do
          Process.sleep(backoff_for_attempt(state.backoff, attempt))
          do_put_latest(state, key, data, retries, timeout, attempt + 1)
        else
          {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp current_vsn(state, key) do
    case ekv_lookup(state.name, key) do
      nil -> nil
      {_value, vsn} -> vsn
    end
  end

  defp current_value_and_vsn(state, key, opts) do
    consistent = Keyword.get(opts, :consistent, false)

    if consistent do
      try do
        _ = ekv_get(state.name, key, consistent: true, timeout: state.timeout)
      rescue
        error in RuntimeError ->
          return_error({:consistent_read_failed, error.message})
      end
    end

    case ekv_lookup(state.name, key) do
      nil -> {:ok, {nil, nil}}
      {value, vsn} -> {:ok, {value, vsn}}
    end
  catch
    {:return_error, reason} -> {:error, reason}
  end

  defp return_error(reason), do: throw({:return_error, reason})

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
         true <- is_integer(ts) and is_atom(origin) do
      {:ok, {ts, origin}}
    else
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp retryable_error?(reason) do
    reason in [
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

  defp fetch_config(name) do
    try do
      {:ok, ekv_get_config(name)}
    rescue
      _ -> {:error, {:ekv_not_started, name}}
    catch
      _, _ -> {:error, {:ekv_not_started, name}}
    end
  end

  defp ensure_cas_config(%{cluster_size: nil}) do
    {:error, :ekv_cas_not_configured}
  end

  defp ensure_cas_config(_), do: :ok

  defp backoff_for_attempt({min_ms, max_ms}, _attempt) when min_ms == max_ms, do: min_ms

  defp backoff_for_attempt({min_ms, max_ms}, _attempt) do
    :rand.uniform(max_ms - min_ms + 1) + min_ms - 1
  end

  defp subscription_relay(parent, subscriber, name, prefix) do
    monitor_ref = Process.monitor(subscriber)

    case ekv_subscribe(name, prefix) do
      :ok ->
        send(parent, {:durable_server_storage_subscribed, self(), :ok})
        subscription_relay_loop(subscriber, name, prefix, monitor_ref)

      {:error, reason} ->
        send(parent, {:durable_server_storage_subscribed, self(), {:error, reason}})
    end
  end

  defp subscription_relay_loop(subscriber, name, prefix, monitor_ref) do
    receive do
      {:durable_server_storage_unsubscribe, from} ->
        _ = ekv_unsubscribe(name, prefix)
        send(from, {:durable_server_storage_unsubscribed, self()})
        :ok

      {:DOWN, ^monitor_ref, :process, ^subscriber, _reason} ->
        _ = ekv_unsubscribe(name, prefix)
        :ok

      {:ekv, events, %{name: ^name}} when is_list(events) ->
        normalized_events =
          events
          |> Enum.flat_map(&normalize_ekv_event/1)

        if normalized_events != [] do
          send(subscriber, {:durable_server_storage_events, normalized_events})
        end

        subscription_relay_loop(subscriber, name, prefix, monitor_ref)

      _other ->
        subscription_relay_loop(subscriber, name, prefix, monitor_ref)
    end
  end

  defp normalize_ekv_event(%{type: :put, key: key, value: value}) when is_binary(key) do
    [%{type: :put, key: key, value: value}]
  end

  defp normalize_ekv_event(%{type: :delete, key: key, value: value}) when is_binary(key) do
    [%{type: :delete, key: key, value: value}]
  end

  defp normalize_ekv_event(_event), do: []

  defp ekv_mod, do: :"Elixir.EKV"

  defp ekv_get_config(name), do: apply(ekv_mod(), :get_config, [name])
  defp ekv_keys(name, prefix), do: apply(ekv_mod(), :keys, [name, prefix])
  defp ekv_lookup(name, key), do: apply(ekv_mod(), :lookup, [name, key])
  defp ekv_put(name, key, value, opts), do: apply(ekv_mod(), :put, [name, key, value, opts])
  defp ekv_get(name, key, opts), do: apply(ekv_mod(), :get, [name, key, opts])
  defp ekv_delete(name, key, opts), do: apply(ekv_mod(), :delete, [name, key, opts])
  defp ekv_subscribe(name, prefix), do: apply(ekv_mod(), :subscribe, [name, prefix])
  defp ekv_unsubscribe(name, prefix), do: apply(ekv_mod(), :unsubscribe, [name, prefix])
end
