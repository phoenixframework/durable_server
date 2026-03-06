defmodule DurableServer.StorageBackend.ObjectStore do
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
         heartbeat_subscribe?: false
       }
     }}
  end

  def init_backend(opts) when is_list(opts), do: init_backend(ObjectStore.new(opts))
  def init_backend(opts) when is_map(opts), do: opts |> Map.to_list() |> init_backend()

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
    ObjectStore.list_all_objects_stream(store, prefix, opts)
  end

  @impl true
  def put_object(%ObjectStore{} = store, key, data, opts) do
    with {:ok, encoded} <- encode_body(data),
         {:ok, %{etag: etag}} <- ObjectStore.put_object(store, key, encoded, opts) do
      {:ok, %{body: data, etag: etag}}
    end
  end

  @impl true
  def delete_object(%ObjectStore{} = store, key) do
    ObjectStore.delete_object(store, key)
  end

  @impl true
  def try_claim(%ObjectStore{} = store, key, body) do
    with {:ok, encoded} <- encode_body(body) do
      ObjectStore.try_claim(store, key, encoded)
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

  defp encode_body(%StoredState{meta: %Meta{} = meta} = data) do
    encoded_meta = Meta.encode_to_binary(meta)
    stored_state = %StoredState{data | meta: encoded_meta}
    {:ok, JSON.encode!(stored_state)}
  rescue
    error in [ArgumentError, RuntimeError] -> {:error, error}
  end

  defp encode_body(data) do
    {:ok, JSON.encode!(data)}
  rescue
    error in [ArgumentError, RuntimeError] -> {:error, error}
  end

  defp decode_body(encoded) when is_binary(encoded) do
    data = JSON.decode!(encoded)

    case data do
      %{"vsn" => vsn, "state" => state, "meta" => meta_str} when is_binary(meta_str) ->
        {:ok,
         %StoredState{
           vsn: vsn,
           state: state,
           meta: Meta.decode_from_binary(meta_str, %{key: nil, prefix: nil})
         }}

      other ->
        {:ok, other}
    end
  catch
    kind, reason ->
      {:error, {kind, reason, encoded}}
  end

  defp decode_body(other), do: {:error, {:error, {:unexpected_encoded_value, other}, other}}
end
