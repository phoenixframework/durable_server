defmodule DurableServer.SupervisorBackendSpecTest do
  use ExUnit.Case, async: false

  alias DurableServer.Backends.MirrorStore
  alias DurableServer.StorageBackend

  defmodule InMemoryBackend do
    @behaviour StorageBackend

    @impl true
    def init_backend(raw_opts) do
      opts =
        case raw_opts do
          %{} = map -> map
          opts when is_list(opts) -> Map.new(opts)
          other -> %{raw_opts: other}
        end

      {:ok,
       %{
         state: %{
           table: :ets.new(__MODULE__, [:set, :public]),
           name: Map.get(opts, :name)
         },
         defaults: %{
           heartbeat_tracking_mode: :poll,
           discovery_interval_ms: 60_000,
           heartbeat_interval_ms: 10_000,
           heartbeat_reconcile_interval_ms: 10_000
         }
       }}
    end

    @impl true
    def ensure_ready(_state), do: :ok

    @impl true
    def get_object(%{table: table}, key, _opts) do
      case :ets.lookup(table, key) do
        [{^key, %{body: body, etag: etag}}] -> {:ok, %{body: body, etag: etag}}
        [] -> {:error, :not_found}
      end
    end

    @impl true
    def list_all_objects_stream(%{table: table}, prefix, _opts) do
      table
      |> :ets.tab2list()
      |> Stream.filter(fn {key, _value} -> String.starts_with?(key, prefix) end)
      |> Stream.map(fn {key, %{etag: etag}} -> %{key: key, etag: etag} end)
    end

    @impl true
    def put_object(%{table: table}, key, data, _opts) do
      etag = next_etag()
      :ets.insert(table, {key, %{body: data, etag: etag}})
      {:ok, %{body: data, etag: etag}}
    end

    @impl true
    def delete_object(%{table: table}, key) do
      case :ets.lookup(table, key) do
        [{^key, _value}] ->
          :ets.delete(table, key)
          :ok

        [] ->
          {:error, :not_found}
      end
    end

    @impl true
    def try_claim(%{table: table}, key, body) do
      case :ets.lookup(table, key) do
        [] ->
          etag = next_etag()
          :ets.insert(table, {key, %{body: body, etag: etag}})
          {:ok, {:claimed, etag}}

        [_existing] ->
          {:error, :taken}
      end
    end

    @impl true
    def update_object(%{table: table} = state, key, update_fn, _opts) do
      with {:ok, %{body: body, etag: etag}} <- get_object(state, key, []),
           {:ok, new_body} <- update_fn.(%{body: body, etag: etag}) do
        put_object(%{table: table}, key, new_body, [])
      end
    end

    @impl true
    def encode(_state, data), do: {:ok, data}

    @impl true
    def decode(_state, data), do: {:ok, data}

    defp next_etag do
      System.unique_integer([:positive, :monotonic])
      |> Integer.to_string()
    end
  end

  test "accepts backend module spec directly" do
    supervisor_name = unique_supervisor_name("custom")
    prefix = unique_prefix("custom")

    start_supervised!(
      {DurableServer.Supervisor,
       [
         name: supervisor_name,
         prefix: prefix,
         backend: {InMemoryBackend, name: :custom},
         graceful_shutdown_timeout_ms: 500
       ]}
    )

    %{storage_backend: storage_backend, object_store: object_store} =
      DurableServer.Supervisor.__get_config__(supervisor_name)

    assert storage_backend.adapter == InMemoryBackend
    assert storage_backend.state.name == :custom
    assert object_store == nil
  end

  test "accepts nested backend module specs in migration store" do
    supervisor_name = unique_supervisor_name("migration")
    prefix = unique_prefix("migration")

    start_supervised!(
      {DurableServer.Supervisor,
       [
         name: supervisor_name,
         prefix: prefix,
         backend:
           {MirrorStore,
            [
              primary: {InMemoryBackend, name: :primary},
              secondary: {InMemoryBackend, name: :secondary},
              read_preference: :primary,
              write_target: :primary,
              mirror_writes: true,
              mirror_mode: :required,
              secondary_required: true
            ]},
         graceful_shutdown_timeout_ms: 500
       ]}
    )

    %{storage_backend: storage_backend, object_store: object_store} =
      DurableServer.Supervisor.__get_config__(supervisor_name)

    assert storage_backend.adapter == MirrorStore
    assert storage_backend.state.primary.adapter == InMemoryBackend
    assert storage_backend.state.secondary.adapter == InMemoryBackend
    assert storage_backend.state.primary.state.name == :primary
    assert storage_backend.state.secondary.state.name == :secondary
    assert object_store == nil
  end

  defp unique_supervisor_name(label) do
    :"durable_backend_spec_#{label}_#{System.unique_integer([:positive, :monotonic])}"
  end

  defp unique_prefix(label) do
    "backend_spec/#{label}/#{System.unique_integer([:positive, :monotonic])}/"
  end
end
