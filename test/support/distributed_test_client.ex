defmodule DurableServer.DistributedTestClient do
  @moduledoc false

  alias DurableServer.{LifecycleManager, StorageBackend}
  alias DurableServer.Backends.EKVStore

  defmodule Counter do
    use DurableServer, vsn: 1

    def dump_state(state), do: state
    def load_state(_vsn, state), do: state
    def init(state), do: {:ok, state, permanent: true}

    def handle_call(:get_count, _from, state), do: {:reply, state.count, state}

    def handle_call(:increment_and_sync, _from, state) do
      state = %{state | count: state.count + 1}
      {:reply, state.count, state, :sync}
    end
  end

  def start_store(name, data_dir) do
    Supervisor.start_child(EKV.AppSupervisor, {
      EKV,
      name: name, data_dir: data_dir, cluster_size: 3, node_id: to_string(node()), log: false
    })
  end

  def start_durable(name, store) do
    Supervisor.start_child(DurableServer.AppSupervisor, {
      DurableServer.Supervisor,
      name: name,
      prefix: "ownership/",
      backend: {EKVStore, name: store, start: false},
      initial_discovery_delay_ms: 60_000,
      discovery_interval_ms: 200,
      discovery_burst_count: 0,
      heartbeat_interval_ms: 250,
      heartbeat_staleness_threshold_ms: 4_000,
      graceful_shutdown_timeout_ms: 500
    })
  end

  def ensure_counter(supervisor, key) do
    DurableServer.Supervisor.ensure_started_child(
      supervisor,
      {Counter, key: key, initial_state: %{count: 0}},
      local_only: true,
      timeout: 10_000
    )
  end

  def invoke(pid, request) do
    {:ok, GenServer.call(pid, request, 10_000)}
  catch
    # A missing acknowledgement is not evidence that a write did not commit.
    :exit, reason -> {:indeterminate, reason}
  end

  def stored(supervisor, key) do
    %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor)
    StorageBackend.get_object(backend, "ownership/" <> key, consistent: true)
  end

  def stale_write(supervisor, key, body, etag) do
    %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor)
    StorageBackend.put_object(backend, "ownership/" <> key, body, etag: etag, max_retries: 0)
  end

  def discover(supervisor) do
    send(LifecycleManager.name(supervisor), :discover_and_restart)
    :ok
  end

  def lookup(supervisor, key), do: DurableServer.Supervisor.lookup(supervisor, key)

  def connected_store_members(store) do
    EKV.info(store).connected_members |> Enum.map(& &1.node) |> Enum.sort()
  end
end
