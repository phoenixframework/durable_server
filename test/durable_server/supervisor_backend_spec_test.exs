defmodule DurableServer.SupervisorBackendSpecTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias DurableServer.LifecycleManager
  alias DurableServer.Backends.EKVStore
  alias DurableServer.Backends.MirrorStore
  alias DurableServer.StorageBackend

  def throw_not_ready do
    throw({:error, :not_ready})
  end

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

  test "child_spec uses configured supervisor shutdown timeout" do
    shutdown_timeout = 12_345
    supervisor_name = unique_supervisor_name("child_spec")
    prefix = unique_prefix("child_spec")

    opts = [
      name: supervisor_name,
      prefix: prefix,
      backend: {InMemoryBackend, name: :child_spec},
      supervisor_shutdown_timeout_ms: shutdown_timeout
    ]

    child_spec = DurableServer.Supervisor.child_spec(opts)

    assert child_spec.id == supervisor_name
    assert child_spec.start == {DurableServer.Supervisor, :start_link, [opts]}
    assert child_spec.type == :supervisor
    assert child_spec.restart == :permanent
    assert child_spec.shutdown == shutdown_timeout
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

  test "EKV start: false rejects managed startup opts" do
    assert_raise RuntimeError, ~r/start: false cannot include managed EKV startup opts/, fn ->
      start_supervised!(
        {DurableServer.Supervisor,
         [
           name: unique_supervisor_name("ekv_external_invalid"),
           prefix: unique_prefix("ekv_external_invalid"),
           backend:
             {EKVStore,
              [
                name: :external_ekv_invalid,
                start: false,
                data_dir: "/tmp/should_not_start"
              ]}
         ]}
      )
    end
  end

  test "EKV backend with only a name remains external by default" do
    assert_raise RuntimeError, ~r/could not start child|failed to start child/i, fn ->
      start_supervised!(
        {DurableServer.Supervisor,
         [
           name: unique_supervisor_name("ekv_external_default"),
           prefix: unique_prefix("ekv_external_default"),
           backend: {EKVStore, [name: :external_by_default]}
         ]}
      )
    end
  end

  test "ready? requires lifecycle manager, not just the supervisor pid" do
    supervisor_name = unique_supervisor_name("not_ready")
    pid = spawn(fn -> Process.sleep(:infinity) end)
    true = Process.register(pid, supervisor_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :kill)
      end
    end)

    refute DurableServer.Supervisor.ready?(supervisor_name)
  end

  test "ready? returns true for a started supervisor" do
    supervisor_name = unique_supervisor_name("ready")
    prefix = unique_prefix("ready")

    start_supervised!(
      {DurableServer.Supervisor,
       [
         name: supervisor_name,
         prefix: prefix,
         backend: {InMemoryBackend, name: :ready},
         graceful_shutdown_timeout_ms: 500
       ]}
    )

    assert DurableServer.Supervisor.ready?(supervisor_name)
  end

  test "discovery tuning options are propagated to lifecycle manager state" do
    supervisor_name = unique_supervisor_name("discovery_tuning")
    prefix = unique_prefix("discovery_tuning")

    start_supervised!(
      {DurableServer.Supervisor,
       [
         name: supervisor_name,
         prefix: prefix,
         backend: {InMemoryBackend, name: :discovery_tuning},
         initial_discovery_delay_ms: {10, 20},
         discovery_shuffle_batch_size: 123,
         parallel_restart_batch_size: 7,
         restart_start_timeout_ms: 12_000,
         heartbeat_staleness_threshold_ms: 30_000,
         restart_claim_preferred_fanout: 3,
         restart_claim_expanded_fanout: 5,
         restart_claim_gate_expand_after_ms: 5_000,
         restart_claim_gate_disable_after_ms: 45_000,
         graceful_shutdown_timeout_ms: 500
       ]}
    )

    %{
      initial_discovery_delay_ms: initial_discovery_delay_ms,
      discovery_shuffle_batch_size: discovery_shuffle_batch_size,
      parallel_restart_batch_size: parallel_restart_batch_size,
      restart_start_timeout_ms: restart_start_timeout_ms,
      heartbeat_staleness_threshold_ms: heartbeat_staleness_threshold_ms,
      restart_claim_preferred_fanout: restart_claim_preferred_fanout,
      restart_claim_expanded_fanout: restart_claim_expanded_fanout,
      restart_claim_gate_expand_after_ms: restart_claim_gate_expand_after_ms,
      restart_claim_gate_disable_after_ms: restart_claim_gate_disable_after_ms
    } = DurableServer.Supervisor.__get_config__(supervisor_name)

    assert initial_discovery_delay_ms == {10, 20}
    assert discovery_shuffle_batch_size == 123
    assert parallel_restart_batch_size == 7
    assert restart_start_timeout_ms == 12_000
    assert heartbeat_staleness_threshold_ms == 30_000
    assert restart_claim_preferred_fanout == 3
    assert restart_claim_expanded_fanout == 5
    assert restart_claim_gate_expand_after_ms == 5_000
    assert restart_claim_gate_disable_after_ms == 45_000

    state = :sys.get_state(LifecycleManager.name(supervisor_name))

    assert state.initial_discovery_delay_ms == {10, 20}
    assert state.discovery_shuffle_batch_size == 123
    assert state.parallel_restart_batch_size == 7
    assert state.restart_start_timeout_ms == 12_000
    assert state.config.heartbeat_staleness_threshold_ms == 30_000
    assert state.restart_claim_preferred_fanout == 3
    assert state.restart_claim_expanded_fanout == 5
    assert state.restart_claim_gate_expand_after_ms == 5_000
    assert state.restart_claim_gate_disable_after_ms == 45_000
  end

  test "init builds lifecycle manager and terminator child specs with configured shutdown timeout" do
    supervisor_name = unique_supervisor_name("shutdown_specs")
    prefix = unique_prefix("shutdown_specs")
    shutdown_timeout = 12_345

    assert {:ok, {_flags, child_specs}} =
             DurableServer.Supervisor.init(
               name: supervisor_name,
               prefix: prefix,
               backend: {InMemoryBackend, name: :shutdown_specs},
               supervisor_shutdown_timeout_ms: shutdown_timeout
             )

    lifecycle_manager_spec = Enum.find(child_specs, &(&1.id == LifecycleManager))
    terminator_spec = Enum.find(child_specs, &(&1.id == DurableServer.Terminator))

    assert lifecycle_manager_spec.shutdown == shutdown_timeout
    assert terminator_spec.shutdown == shutdown_timeout
  end

  test "warns when supervisor shutdown timeout is shorter than child shutdown requirements" do
    supervisor_name = unique_supervisor_name("shutdown_warning")
    prefix = unique_prefix("shutdown_warning")
    unique_id = System.unique_integer([:positive, :monotonic])
    ekv_name = :"durable_shutdown_warning_ekv_#{unique_id}"
    data_dir = Path.join(System.tmp_dir!(), "durable_shutdown_warning_#{unique_id}")

    log =
      capture_log(fn ->
        assert {:ok, {_flags, _child_specs}} =
                 DurableServer.Supervisor.init(
                   name: supervisor_name,
                   prefix: prefix,
                   backend:
                     {EKVStore,
                      [
                        name: ekv_name,
                        data_dir: data_dir,
                        cluster_size: 1,
                        node_id: 1,
                        log: false,
                        shutdown_barrier: 120_000
                      ]},
                   graceful_shutdown_timeout_ms: 90_000,
                   supervisor_shutdown_timeout_ms: 60_000
                 )
      end)

    assert log =~
             "supervisor_shutdown_timeout_ms (60000) is less than graceful_shutdown_timeout_ms (90000)"

    assert log =~
             "supervisor_shutdown_timeout_ms (60000) is less than managed EKV shutdown requirement"

    assert log =~ inspect(ekv_name)
  end

  test "invalid discovery tuning options raise" do
    supervisor_name = unique_supervisor_name("invalid_discovery_tuning")
    prefix = unique_prefix("invalid_discovery_tuning")

    assert_raise RuntimeError, ~r/initial_discovery_delay_ms/, fn ->
      start_supervised!(
        {DurableServer.Supervisor,
         [
           name: supervisor_name,
           prefix: prefix,
           backend: {InMemoryBackend, name: :invalid_discovery_tuning},
           initial_discovery_delay_ms: {20, 10}
         ]}
      )
    end

    assert_raise RuntimeError, ~r/discovery_shuffle_batch_size/, fn ->
      start_supervised!(
        {DurableServer.Supervisor,
         [
           name: unique_supervisor_name("invalid_shuffle"),
           prefix: unique_prefix("invalid_shuffle"),
           backend: {InMemoryBackend, name: :invalid_shuffle},
           discovery_shuffle_batch_size: 0
         ]}
      )
    end

    assert_raise RuntimeError, ~r/parallel_restart_batch_size/, fn ->
      start_supervised!(
        {DurableServer.Supervisor,
         [
           name: unique_supervisor_name("invalid_parallel"),
           prefix: unique_prefix("invalid_parallel"),
           backend: {InMemoryBackend, name: :invalid_parallel},
           parallel_restart_batch_size: 0
         ]}
      )
    end

    assert_raise RuntimeError, ~r/restart_start_timeout_ms/, fn ->
      start_supervised!(
        {DurableServer.Supervisor,
         [
           name: unique_supervisor_name("invalid_restart_timeout"),
           prefix: unique_prefix("invalid_restart_timeout"),
           backend: {InMemoryBackend, name: :invalid_restart_timeout},
           restart_start_timeout_ms: 0
         ]}
      )
    end

    assert_raise RuntimeError, ~r/heartbeat_staleness_threshold_ms/, fn ->
      start_supervised!(
        {DurableServer.Supervisor,
         [
           name: unique_supervisor_name("invalid_heartbeat_staleness"),
           prefix: unique_prefix("invalid_heartbeat_staleness"),
           backend: {InMemoryBackend, name: :invalid_heartbeat_staleness},
           heartbeat_staleness_threshold_ms: 2_000
         ]}
      )
    end

    assert_raise RuntimeError, ~r/heartbeat_interval_ms/, fn ->
      start_supervised!(
        {DurableServer.Supervisor,
         [
           name: unique_supervisor_name("invalid_heartbeat_interval"),
           prefix: unique_prefix("invalid_heartbeat_interval"),
           backend: {InMemoryBackend, name: :invalid_heartbeat_interval},
           heartbeat_interval_ms: 10_000,
           heartbeat_staleness_threshold_ms: 15_000
         ]}
      )
    end

    assert_raise RuntimeError, ~r/restart_claim_preferred_fanout/, fn ->
      start_supervised!(
        {DurableServer.Supervisor,
         [
           name: unique_supervisor_name("invalid_restart_preferred"),
           prefix: unique_prefix("invalid_restart_preferred"),
           backend: {InMemoryBackend, name: :invalid_restart_preferred},
           restart_claim_preferred_fanout: 0
         ]}
      )
    end

    assert_raise RuntimeError,
                 ~r/restart_claim_expanded_fanout must be >= restart_claim_preferred_fanout/,
                 fn ->
                   start_supervised!(
                     {DurableServer.Supervisor,
                      [
                        name: unique_supervisor_name("invalid_restart_expanded"),
                        prefix: unique_prefix("invalid_restart_expanded"),
                        backend: {InMemoryBackend, name: :invalid_restart_expanded},
                        restart_claim_preferred_fanout: 3,
                        restart_claim_expanded_fanout: 2
                      ]}
                   )
                 end

    assert_raise RuntimeError,
                 ~r/restart_claim_gate_disable_after_ms must be >= restart_claim_gate_expand_after_ms/,
                 fn ->
                   start_supervised!(
                     {DurableServer.Supervisor,
                      [
                        name: unique_supervisor_name("invalid_restart_disable"),
                        prefix: unique_prefix("invalid_restart_disable"),
                        backend: {InMemoryBackend, name: :invalid_restart_disable},
                        restart_claim_gate_expand_after_ms: 5_000,
                        restart_claim_gate_disable_after_ms: 4_999
                      ]}
                   )
                 end
  end

  test "safe_erpc_call rethrows remote not_ready as a local throw" do
    assert catch_throw(
             DurableServer.Supervisor.safe_erpc_call(
               Node.self(),
               __MODULE__,
               :throw_not_ready,
               [],
               1_000
             )
           ) == {:error, :not_ready}
  end

  defp unique_supervisor_name(label) do
    :"durable_backend_spec_#{label}_#{System.unique_integer([:positive, :monotonic])}"
  end

  defp unique_prefix(label) do
    "backend_spec/#{label}/#{System.unique_integer([:positive, :monotonic])}/"
  end
end
