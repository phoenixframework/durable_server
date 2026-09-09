defmodule DurableServer.PropertyFixture do
  @moduledoc false
  import ExUnit.Assertions
  import ExUnit.Callbacks

  alias DurableServer.Backends.EKVStore
  alias DurableServer.StorageBackend

  # These names are deliberately reused: properties are synchronous, and allocating
  # atoms for every generated input/shrink would leak atoms in long campaigns.
  @store :durable_property_ekv
  @supervisor :durable_property_supervisor

  def with_sample(root, kind, fun) do
    dir = Path.join(root, Integer.to_string(System.unique_integer([:positive, :monotonic])))

    children =
      [{EKV, name: @store, data_dir: dir, cluster_size: 1, node_id: 1, log: false}] ++
        durable_children(kind)

    try do
      start_supervised!(%{
        id: __MODULE__,
        start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]},
        type: :supervisor,
        shutdown: :infinity
      })

      {:ok, backend} = StorageBackend.init_backend(EKVStore, name: @store)
      fun.(%{backend: backend, supervisor: @supervisor})
    after
      # Reverse child shutdown order keeps EKV alive for final durable writes.
      # This runs for every input AND shrink, including assertion failures.
      stopped = stop_supervised(__MODULE__)
      assert stopped in [:ok, {:error, :not_found}]
      File.rm_rf!(dir)
    end
  end

  defp durable_children(:storage), do: []

  defp durable_children(:durable) do
    [
      {DurableServer.Supervisor,
       name: @supervisor,
       prefix: "property/",
       backend: {EKVStore, name: @store, start: false},
       initial_discovery_delay_ms: 60_000,
       discovery_interval_ms: 60_000,
       discovery_burst_count: 0,
       graceful_shutdown_timeout_ms: 500}
    ]
  end
end
