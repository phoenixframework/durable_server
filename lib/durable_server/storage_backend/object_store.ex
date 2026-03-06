defmodule DurableServer.StorageBackend.ObjectStore do
  @moduledoc false

  @behaviour DurableServer.StorageBackend

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
    ObjectStore.get_object(store, key, opts)
  end

  @impl true
  def list_all_objects_stream(%ObjectStore{} = store, prefix, opts) do
    ObjectStore.list_all_objects_stream(store, prefix, opts)
  end

  @impl true
  def put_object(%ObjectStore{} = store, key, data, opts) do
    ObjectStore.put_object(store, key, data, opts)
  end

  @impl true
  def delete_object(%ObjectStore{} = store, key) do
    ObjectStore.delete_object(store, key)
  end

  @impl true
  def try_claim(%ObjectStore{} = store, key, body) do
    ObjectStore.try_claim(store, key, body)
  end

  @impl true
  def update_object(%ObjectStore{} = store, key, update_fn, opts) do
    ObjectStore.update_object(store, key, update_fn, opts)
  end
end
