defmodule DurableServer.Backends.ObjectStore do
  @moduledoc false

  @behaviour DurableServer.StorageBackend

  alias DurableServer.{Meta, StoredState}
  alias DurableServer.ObjectStore

  @impl true
  def init_backend(%ObjectStore{} = store) do
    {:ok,
     %{
       state: store,
       defaults: %{
         heartbeat_tracking_mode: :poll,
         discovery_interval_ms: 60_000,
         heartbeat_interval_ms: 10_000,
         heartbeat_reconcile_interval_ms: 10_000
       },
       features: %{
         heartbeat_subscribe?: false,
         conditional_delete?: conditional_delete_supported?(store)
       }
     }}
  end

  def init_backend(opts) when is_list(opts), do: init_backend(ObjectStore.new(opts))
  def init_backend(opts) when is_map(opts), do: opts |> Map.to_list() |> init_backend()

  defp conditional_delete_supported?(%ObjectStore{s3_endpoint: endpoint})
       when is_binary(endpoint) do
    case URI.parse(endpoint) do
      %URI{host: host, port: 4566} when host in ["localhost", "127.0.0.1"] -> false
      _ -> true
    end
  end

  defp conditional_delete_supported?(%ObjectStore{}), do: true

  @impl true
  def ensure_ready(%ObjectStore{} = store) do
    ObjectStore.ensure_bucket_exists(store)
  end

  @impl true
  def get_object(%ObjectStore{} = store, key, opts) do
    case ObjectStore.get_object(store, key, opts) do
      {:ok, %{body: encoded, etag: etag}} ->
        case decode_body(encoded) do
          {:ok, body} -> {:ok, %{body: body, etag: etag}}
          {:error, reason} -> {:error, reason}
        end

      other ->
        other
    end
  end

  @impl true
  def list_all_objects_stream(%ObjectStore{} = store, prefix, opts) do
    {_include_objects, opts} = Keyword.pop(opts, :include_objects, false)
    ObjectStore.list_all_objects_stream(store, prefix, opts)
  end

  @impl true
  def put_object(%ObjectStore{} = store, key, data, opts) do
    with {:ok, encoded} <- encode_body(data) do
      case ObjectStore.put_object(store, key, encoded, opts) do
        {:ok, %{etag: etag}} ->
          {:ok, %{body: data, etag: etag}}

        {:error, :conflict} ->
          resolve_ambiguous_conditional_put(store, key, data, encoded, opts)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  # A conditional PUT can commit while its response is lost. A retry with the
  # old ETag then reports a conflict even though this exact boot wrote the
  # desired state. Only adopt storage when both ownership and bytes are exact.
  defp resolve_ambiguous_conditional_put(
         %ObjectStore{} = store,
         key,
         data,
         encoded,
         opts
       ) do
    with true <- Keyword.has_key?(opts, :etag),
         {:ok, etag} <- read_matching_owned_state(store, key, data, encoded) do
      {:ok, %{body: data, etag: etag}}
    else
      _other -> {:error, :conflict}
    end
  end

  defp read_matching_owned_state(
         store,
         key,
         %StoredState{meta: %Meta{} = attempted_meta},
         encoded
       ) do
    with {:ok, %{body: ^encoded, etag: etag}} <-
           ObjectStore.get_object(store, key, consistent: true),
         {:ok, %StoredState{meta: %Meta{} = persisted_meta}} <- decode_body(encoded),
         true <- same_boot_owner?(attempted_meta, persisted_meta) do
      {:ok, etag}
    else
      _other -> :error
    end
  end

  defp read_matching_owned_state(_store, _key, _data, _encoded), do: :error

  defp same_boot_owner?(
         %Meta{pid: pid, node_ref: node_ref, node_str: node_str},
         %Meta{pid: pid, node_ref: node_ref, node_str: node_str}
       )
       when is_pid(pid) and not is_nil(node_ref) and is_binary(node_str),
       do: true

  defp same_boot_owner?(%Meta{}, %Meta{}), do: false

  @impl true
  def delete_object(%ObjectStore{} = store, key) do
    ObjectStore.delete_object(store, key)
  end

  @impl true
  def delete_object(%ObjectStore{} = store, key, opts) when is_list(opts) do
    ObjectStore.delete_object(store, key, opts)
  end

  @impl true
  def try_claim(%ObjectStore{} = store, key, body) do
    with {:ok, encoded} <- encode_body(body) do
      case ObjectStore.try_claim(store, key, encoded) do
        {:error, :already_claimed} ->
          resolve_ambiguous_claim(store, key, body, encoded, :already_claimed)

        {:error, %Req.TransportError{} = reason} ->
          resolve_ambiguous_claim(store, key, body, encoded, reason)

        result ->
          result
      end
    end
  end

  # A lost claim response is recoverable only for this boot's exact stored state.
  defp resolve_ambiguous_claim(store, key, body, encoded, reason) do
    case read_matching_owned_state(store, key, body, encoded) do
      {:ok, etag} -> {:ok, {:claimed, etag}}
      :error -> {:error, reason}
    end
  end

  @impl true
  def update_object(%ObjectStore{} = store, key, update_fn, opts) do
    case ObjectStore.update_object(
           store,
           key,
           fn %{body: encoded, etag: etag} ->
             with {:ok, body} <- decode_body(encoded),
                  {:ok, new_body} <- update_fn.(%{body: body, etag: etag}),
                  {:ok, new_encoded} <- encode_body(new_body) do
               {:ok, new_encoded}
             end
           end,
           opts
         ) do
      {:ok, %{body: encoded, etag: etag}} ->
        case decode_body(encoded) do
          {:ok, body} -> {:ok, %{body: body, etag: etag}}
          {:error, reason} -> {:error, reason}
        end

      other ->
        other
    end
  end

  @impl true
  def encode(%ObjectStore{} = _store, data), do: encode_body(data)

  @impl true
  def decode(%ObjectStore{} = _store, data), do: decode_body(data)

  defp encode_body(%StoredState{meta: %Meta{}} = data) do
    {:ok, JSON.encode!(StoredState.to_object_store_term(data))}
  rescue
    error in [ArgumentError, RuntimeError, Protocol.UndefinedError] -> {:error, error}
  end

  defp encode_body(data) do
    {:ok, JSON.encode!(data)}
  rescue
    error in [ArgumentError, RuntimeError, Protocol.UndefinedError] -> {:error, error}
  end

  defp decode_body(encoded) when is_binary(encoded) do
    data = JSON.decode!(encoded)

    case StoredState.from_object_store_term(data) do
      {:ok, body} ->
        {:ok, body}

      :not_stored_state ->
        {:ok, data}

      {:error, reason} ->
        {:error, reason}
    end
  catch
    kind, reason ->
      {:error, {kind, reason, encoded}}
  end

  defp decode_body(other), do: {:error, {:error, {:unexpected_encoded_value, other}, other}}
end
