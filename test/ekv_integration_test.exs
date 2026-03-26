defmodule DurableServer.EKVIntegrationTest do
  use ExUnit.Case, async: false

  alias DurableServer.Backends.EKVStore
  alias DurableServer.{LifecycleManager, Meta, StoredState}
  alias DurableServer.StorageBackend
  alias DurableServer.TestCounterServer, as: CounterServer
  alias DurableServer.TestTemporalServer

  @moduletag :integration
  @moduletag :capture_log

  setup do
    unique_id = System.unique_integer([:positive, :monotonic])

    ekv_name = :"durable_ekv_integration_#{unique_id}"
    supervisor_name = :"durable_ekv_supervisor_#{unique_id}"
    prefix = "ekv_integration/#{unique_id}/"
    data_dir = Path.join(System.tmp_dir!(), "durable_server_ekv_integration_#{unique_id}")

    File.rm_rf(data_dir)

    start_supervised!(
      {ekv_mod(),
       [
         name: ekv_name,
         data_dir: data_dir,
         cluster_size: 1,
         node_id: 1,
         log: false
       ]}
    )

    start_supervised!(
      {DurableServer.Supervisor,
       [
         name: supervisor_name,
         prefix: prefix,
         backend: {EKVStore, [name: ekv_name, start: false]},
         graceful_shutdown_timeout_ms: 500
       ]}
    )

    on_exit(fn ->
      File.rm_rf(data_dir)
    end)

    {:ok,
     supervisor_name: supervisor_name, prefix: prefix, ekv_name: ekv_name, data_dir: data_dir}
  end

  test "uses EKV backend defaults for heartbeat tracking and intervals", %{
    supervisor_name: supervisor_name
  } do
    config = DurableServer.Supervisor.__get_config__(supervisor_name)

    assert config.heartbeat_tracking_mode == :subscribe
    assert config.discovery_interval_ms == 3_000
    assert config.heartbeat_interval_ms == 10_000
    assert config.heartbeat_reconcile_interval_ms == 30_000
  end

  test "explicit interval and tracking options override backend defaults", %{ekv_name: ekv_name} do
    unique_id = System.unique_integer([:positive, :monotonic])
    supervisor_name = :"durable_ekv_override_supervisor_#{unique_id}"
    prefix = "ekv_integration_override/#{unique_id}/"

    start_supervised!(%{
      id: {DurableServer.Supervisor, supervisor_name},
      start:
        {DurableServer.Supervisor, :start_link,
         [
           [
             name: supervisor_name,
             prefix: prefix,
             backend: {EKVStore, [name: ekv_name, start: false]},
             discovery_interval_ms: 11_000,
             heartbeat_interval_ms: 7_000,
             heartbeat_tracking_mode: :poll,
             heartbeat_reconcile_interval_ms: 21_000
           ]
         ]}
    })

    config = DurableServer.Supervisor.__get_config__(supervisor_name)

    assert config.discovery_interval_ms == 11_000
    assert config.heartbeat_interval_ms == 7_000
    assert config.heartbeat_tracking_mode == :poll
    assert config.heartbeat_reconcile_interval_ms == 21_000
  end

  test "managed EKV backend auto-starts a separate heartbeat EKV", %{
    data_dir: data_dir
  } do
    unique_id = System.unique_integer([:positive, :monotonic])
    ekv_name = :"durable_managed_ekv_#{unique_id}"
    supervisor_name = :"durable_managed_ekv_supervisor_#{unique_id}"
    prefix = "ekv_managed/#{unique_id}/"
    managed_dir = Path.join(data_dir, "managed_#{unique_id}")

    start_supervised!(%{
      id: {DurableServer.Supervisor, supervisor_name},
      start:
        {DurableServer.Supervisor, :start_link,
         [
           [
             name: supervisor_name,
             prefix: prefix,
             backend:
               {EKVStore,
                [
                  name: ekv_name,
                  data_dir: managed_dir,
                  cluster_size: 1,
                  node_id: 1,
                  log: false
                ]},
             graceful_shutdown_timeout_ms: 500
           ]
         ]}
    })

    config = DurableServer.Supervisor.__get_config__(supervisor_name)
    heartbeat_name = :"#{ekv_name}_heartbeats"

    assert config.storage_backend.state.name == ekv_name
    assert config.heartbeat_backend.state.name == heartbeat_name
    assert Process.whereis(:"#{ekv_name}_ekv_sup") != nil
    assert Process.whereis(:"#{heartbeat_name}_ekv_sup") != nil

    assert_eventually(fn ->
      Enum.empty?(EKV.keys(ekv_name, "#{prefix}__nodes/") |> Enum.to_list())
    end)

    assert_eventually(fn ->
      heartbeat_keys = EKV.keys(heartbeat_name, "#{prefix}__nodes/") |> Enum.to_list()
      heartbeat_keys != []
    end)

    assert File.dir?(Path.join(managed_dir, "heartbeats"))
  end

  test "EKV backend accepts client-mode EKV instances" do
    unique_id = System.unique_integer([:positive, :monotonic])
    client_name = :"durable_ekv_client_only_#{unique_id}"

    start_supervised!(
      {ekv_mod(),
       [
         name: client_name,
         mode: :client,
         region: "ord",
         region_routing: ["ord"],
         log: false
       ]}
    )

    {:ok, backend} = StorageBackend.init_backend(EKVStore, name: client_name)

    assert :ok = StorageBackend.ensure_ready(backend)
  end

  test "EKV.update accepts MFA tuples", %{ekv_name: ekv_name} do
    key = "mfa-update"

    assert {:ok, "v1", _vsn} =
             ekv_mod().update(
               ekv_name,
               key,
               {__MODULE__, :replace_value, ["v1"]},
               resolve_unconfirmed: true
             )

    assert {:ok, "v2", _vsn} =
             ekv_mod().update(
               ekv_name,
               key,
               {__MODULE__, :replace_value, ["v2"]},
               resolve_unconfirmed: true
             )
  end

  test "EKVStore client backend can update an existing remote key without etag" do
    ensure_distributed_node!()

    unique_id = System.unique_integer([:positive, :monotonic])
    peer_name = :"durable_ekv_client_peer_#{unique_id}"
    ekv_name = :"durable_ekv_client_cluster_#{unique_id}"

    remote_data_dir =
      Path.join(System.tmp_dir!(), "durable_server_ekv_client_remote_#{unique_id}")

    key = "client-existing-key"

    File.rm_rf(remote_data_dir)

    {:ok, peer, peer_node} = :peer.start_link(%{name: peer_name})

    on_exit(fn ->
      try do
        :peer.stop(peer)
      catch
        :exit, _ -> :ok
      end

      File.rm_rf(remote_data_dir)
    end)

    assert Node.connect(peer_node)
    :ok = bootstrap_remote_peer(peer_node)

    assert {:ok, _} =
             :erpc.call(
               peer_node,
               Supervisor,
               :start_child,
               [
                 EKV.AppSupervisor,
                 {ekv_mod(),
                  [
                    name: ekv_name,
                    region: "fra",
                    data_dir: remote_data_dir,
                    cluster_size: 1,
                    node_id: 1,
                    log: false
                  ]}
               ]
             )

    start_supervised!(
      {ekv_mod(),
       [
         name: ekv_name,
         mode: :client,
         region: "ams",
         region_routing: ["fra"],
         wait_for_route: 5_000,
         wait_for_quorum: 5_000,
         log: false
       ]}
    )

    assert_eventually(fn ->
      case EKV.ClientRouter.backend(ekv_name) do
        {:ok, ^peer_node} -> true
        _ -> false
      end
    end)

    assert {:ok, _vsn} =
             :erpc.call(peer_node, ekv_mod(), :put, [
               ekv_name,
               key,
               "v1",
               [if_vsn: nil, resolve_unconfirmed: true]
             ])

    {:ok, backend} = StorageBackend.init_backend(EKVStore, name: ekv_name)

    assert {:ok, %{body: "v1"}} = StorageBackend.get_object(backend, key)
    assert {:ok, %{body: "v2"}} = StorageBackend.put_object(backend, key, "v2", max_retries: 0)
    assert {:ok, %{body: "v3"}} = StorageBackend.put_object(backend, key, "v3", max_retries: 0)
  end

  test "subscribe heartbeat tracking updates cache from EKV events", %{
    supervisor_name: supervisor_name,
    prefix: prefix
  } do
    %{storage_backend: storage_backend} = DurableServer.Supervisor.__get_config__(supervisor_name)

    remote_node = "remote-heartbeat@ekv"
    heartbeat_key = "#{prefix}__nodes/#{remote_node}"

    heartbeat_body = %{
      "node" => remote_node,
      "node_ref" => "remote-ref",
      "last_heartbeat_at" => System.system_time(:millisecond),
      "heartbeat_meta" => %{"region" => "iad", "placement_region" => "iad"}
    }

    assert {:ok, _} = StorageBackend.put_object(storage_backend, heartbeat_key, heartbeat_body)

    assert_eventually(fn ->
      case LifecycleManager.get_cluster_nodes(supervisor_name) do
        %{^remote_node => %{heartbeat_meta: %{"region" => "iad", "placement_region" => "iad"}}} ->
          true

        _ ->
          false
      end
    end)

    assert :ok = StorageBackend.delete_object(storage_backend, heartbeat_key)

    assert_eventually(fn ->
      LifecycleManager.get_cluster_nodes(supervisor_name)
      |> Map.has_key?(remote_node)
      |> Kernel.not()
    end)
  end

  test "EKV first boot passes native terms to load_state/2", %{supervisor_name: supervisor_name} do
    key = "temporal-native-first-boot"
    occurred_at = ~U[2026-03-06 19:26:44.533821Z]

    {:ok, {pid, _meta}} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {TestTemporalServer,
         %{
           key: key,
           occurred_at: occurred_at,
           nested: %{occurred_at: occurred_at}
         }}
      )

    assert :native_term == GenServer.call(pid, :get_loaded_shape)

    snapshot = GenServer.call(pid, :get_snapshot)
    assert %DateTime{} = snapshot.occurred_at
    assert %DateTime{} = snapshot.nested.occurred_at
  end

  test "persists and reloads state with existing: true", %{supervisor_name: supervisor_name} do
    key = "counter-restart"

    {:ok, {pid, _meta}} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {CounterServer, %{key: key, count: 0}}
      )

    assert 1 = GenServer.call(pid, :increment_and_sync)

    monitor_ref = Process.monitor(pid)
    assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, pid)
    assert_receive {:DOWN, ^monitor_ref, :process, ^pid, _reason}, 5_000

    assert nil == DurableServer.Supervisor.lookup(supervisor_name, key)

    {:ok, {restarted_pid, _meta}} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {CounterServer, %{key: key}},
        existing: true
      )

    assert 1 == GenServer.call(restarted_pid, :get_count)
  end

  test "EKV restart passes native persisted terms to load_state/2", %{
    supervisor_name: supervisor_name
  } do
    key = "temporal-native"
    occurred_at = ~U[2026-03-06 19:26:44.533821Z]

    {:ok, {pid, _meta}} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {TestTemporalServer,
         %{
           key: key,
           occurred_at: occurred_at,
           nested: %{occurred_at: occurred_at}
         }}
      )

    assert :ok = GenServer.call(pid, :sync_now)

    monitor_ref = Process.monitor(pid)
    assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, pid)
    assert_receive {:DOWN, ^monitor_ref, :process, ^pid, _reason}, 5_000

    {:ok, {restarted_pid, _meta}} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {TestTemporalServer, %{key: key}},
        existing: true
      )

    assert :native_term == GenServer.call(restarted_pid, :get_loaded_shape)

    snapshot = GenServer.call(restarted_pid, :get_snapshot)
    assert %DateTime{} = snapshot.occurred_at
    assert %DateTime{} = snapshot.nested.occurred_at
  end

  test "persists a minimal explicit stored-state envelope in EKV", %{
    supervisor_name: supervisor_name,
    prefix: prefix,
    ekv_name: ekv_name
  } do
    key = "counter-envelope"

    {:ok, {pid, _meta}} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {CounterServer, %{key: key, count: 4}}
      )

    assert 5 == GenServer.call(pid, :increment_and_sync)

    assert_eventually(fn ->
      case ekv_mod().lookup(ekv_name, "#{prefix}#{key}") do
        {%{vsn: 1, state: %{count: 5}, meta: meta}, _vsn}
        when is_map(meta) and not is_struct(meta) ->
          Map.keys(meta) |> Enum.member?(:status)

        _ ->
          false
      end
    end)

    {%{} = raw_body, _vsn} = ekv_mod().lookup(ekv_name, "#{prefix}#{key}")

    assert Enum.sort(Map.keys(raw_body)) == [:meta, :state, :vsn]
    refute is_struct(raw_body)
    refute is_struct(raw_body.meta)
    refute Map.has_key?(raw_body.meta, :key)
    refute Map.has_key?(raw_body.meta, :prefix)
  end

  test "concurrent starts for the same key resolve to a single owner", %{
    supervisor_name: supervisor_name
  } do
    key = "counter-concurrent"

    results =
      1..16
      |> Task.async_stream(
        fn _ ->
          DurableServer.Supervisor.start_child(
            supervisor_name,
            {CounterServer, %{key: key, count: 0}}
          )
        end,
        max_concurrency: 16,
        ordered: false,
        timeout: :timer.seconds(10)
      )
      |> Enum.map(fn {:ok, result} -> result end)

    successes =
      Enum.filter(results, fn
        {:ok, {pid, _meta}} when is_pid(pid) -> true
        _ -> false
      end)

    assert length(successes) == 1

    assert Enum.all?(results, fn
             {:ok, {pid, _meta}} when is_pid(pid) ->
               true

             {:error, {:already_started, {pid, _meta}}} when is_pid(pid) ->
               true

             {:error, {:already_started, pid}} when is_pid(pid) ->
               true

             _ ->
               false
           end)

    assert match?(
             {pid, _meta} when is_pid(pid),
             DurableServer.Supervisor.lookup(supervisor_name, key)
           )
  end

  test "streams persisted keys through EKV backend", %{
    supervisor_name: supervisor_name,
    prefix: prefix
  } do
    {:ok, _} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {CounterServer, %{key: "a", count: 1}}
      )

    {:ok, _} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {CounterServer, %{key: "b", count: 2}}
      )

    %{storage_backend: storage_backend} = DurableServer.Supervisor.__get_config__(supervisor_name)

    listed_keys =
      StorageBackend.list_all_objects_stream(storage_backend, prefix, consistent: false)
      |> Enum.map(& &1.key)

    assert "#{prefix}a" in listed_keys
    assert "#{prefix}b" in listed_keys

    listed_objects =
      StorageBackend.list_all_objects_stream(storage_backend, prefix,
        consistent: false,
        include_objects: true
      )
      |> Enum.filter(&(&1.key in ["#{prefix}a", "#{prefix}b"]))
      |> Enum.map(fn %{key: key, body: %StoredState{} = body} -> {key, body.state.count} end)

    assert {"#{prefix}a", 1} in listed_objects
    assert {"#{prefix}b", 2} in listed_objects
  end

  test "lifecycle manager discovers and restarts a seeded permanent object via shared EKV" do
    ensure_distributed_node!()

    unique_id = System.unique_integer([:positive, :monotonic])
    peer_name = :"durable_ekv_peer_#{unique_id}"
    ekv_name = :"durable_ekv_cluster_#{unique_id}"
    supervisor_name = :"durable_ekv_cluster_sup_#{unique_id}"
    prefix = "ekv_cluster/#{unique_id}/"
    local_data_dir = Path.join(System.tmp_dir!(), "durable_server_ekv_local_#{unique_id}")
    remote_data_dir = Path.join(System.tmp_dir!(), "durable_server_ekv_remote_#{unique_id}")
    key = "seeded-restart"

    File.rm_rf(local_data_dir)
    File.rm_rf(remote_data_dir)

    {:ok, peer, peer_node} = :peer.start_link(%{name: peer_name})

    on_exit(fn ->
      try do
        :peer.stop(peer)
      catch
        :exit, _ -> :ok
      end

      File.rm_rf(local_data_dir)
      File.rm_rf(remote_data_dir)
    end)

    assert Node.connect(peer_node)
    :ok = bootstrap_remote_peer(peer_node)

    start_supervised!(%{
      id: {ekv_mod(), ekv_name},
      start:
        {ekv_mod(), :start_link,
         [
           [
             name: ekv_name,
             data_dir: local_data_dir,
             cluster_size: 2,
             node_id: 1,
             log: false
           ]
         ]}
    })

    assert {:ok, _} =
             :erpc.call(
               peer_node,
               Supervisor,
               :start_child,
               [
                 EKV.AppSupervisor,
                 {ekv_mod(),
                  [
                    name: ekv_name,
                    data_dir: remote_data_dir,
                    cluster_size: 2,
                    node_id: 2,
                    log: false
                  ]}
               ]
             )

    start_supervised!(%{
      id: {DurableServer.Supervisor, supervisor_name},
      start:
        {DurableServer.Supervisor, :start_link,
         [
           [
             name: supervisor_name,
             prefix: prefix,
             backend: {EKVStore, [name: ekv_name, start: false]},
             discovery_interval_ms: 200,
             heartbeat_interval_ms: 250,
             heartbeat_reconcile_interval_ms: 10_000,
             graceful_shutdown_timeout_ms: 500,
             dead_node_threshold_ms: 5_000
           ]
         ]}
    })

    assert {:ok, _} =
             :erpc.call(
               peer_node,
               Supervisor,
               :start_child,
               [
                 DurableServer.AppSupervisor,
                 {DurableServer.Supervisor,
                  [
                    name: supervisor_name,
                    prefix: prefix,
                    backend: {EKVStore, [name: ekv_name, start: false]},
                    discovery_interval_ms: 200,
                    heartbeat_interval_ms: 250,
                    heartbeat_reconcile_interval_ms: 10_000,
                    graceful_shutdown_timeout_ms: 500,
                    dead_node_threshold_ms: 5_000
                  ]}
               ]
             )

    assert_eventually(
      fn ->
        peer_node in connected_ekv_peer_nodes(ekv_name) and
          Node.self() in remote_connected_ekv_peer_nodes(peer_node, ekv_name)
      end,
      10_000
    )

    assert_eventually(
      fn ->
        LifecycleManager.get_cluster_nodes(supervisor_name)
        |> Map.has_key?(to_string(peer_node))
      end,
      5_000
    )

    peer_node_string = to_string(peer_node)

    %{storage_backend: storage_backend} = DurableServer.Supervisor.__get_config__(supervisor_name)

    remote_node_ref =
      case LifecycleManager.lookup_node_health(%{
             supervisor: supervisor_name,
             node_str: peer_node_string
           }) do
        {:healthy, %{node_ref: node_ref}} when is_integer(node_ref) -> node_ref
      end

    seeded_meta = %Meta{
      module: CounterServer,
      permanent: true,
      pid: self(),
      status: :stopped_graceful,
      key: key,
      prefix: prefix,
      supervisor: supervisor_name,
      task_supervisor: DurableServer.TaskSupervisor,
      node_ref: remote_node_ref,
      node_str: peer_node_string,
      last_heartbeat_at: System.system_time(:millisecond)
    }

    seeded_state = %StoredState{
      vsn: 1,
      state: %{count: 7},
      meta: seeded_meta
    }

    assert {:ok, _} =
             StorageBackend.put_object(
               storage_backend,
               "#{prefix}#{key}",
               seeded_state
             )

    send(LifecycleManager.name(supervisor_name), :discover_and_restart)

    assert_eventually(
      fn ->
        case DurableServer.Supervisor.lookup(supervisor_name, key) do
          {pid, _meta} when is_pid(pid) ->
            node(pid) == Node.self()

          nil ->
            false
        end
      end,
      10_000
    )

    {restarted_pid, _meta} = DurableServer.Supervisor.lookup(supervisor_name, key)
    assert 7 == GenServer.call(restarted_pid, :get_count)
  end

  defp ekv_mod, do: :"Elixir.EKV"

  def replace_value(_current_value, new_value), do: new_value

  defp bootstrap_remote_peer(peer_node) do
    code_paths = :code.get_path()

    assert :ok = :erpc.call(peer_node, :code, :add_paths, [code_paths])
    assert {:ok, _} = :erpc.call(peer_node, Application, :ensure_all_started, [:ekv])
    assert {:ok, _} = :erpc.call(peer_node, Application, :ensure_all_started, [:durable_server])
    :ok
  end

  defp connected_ekv_peer_nodes(ekv_name) do
    ekv_mod().info(ekv_name).connected_members |> Enum.map(& &1.node)
  end

  defp remote_connected_ekv_peer_nodes(peer_node, ekv_name) do
    :erpc.call(peer_node, ekv_mod(), :info, [ekv_name]).connected_members |> Enum.map(& &1.node)
  end

  defp ensure_distributed_node! do
    if Node.alive?() do
      :ok
    else
      name = :"durable_server_test_#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = Node.start(name, :shortnames)
      :ok
    end
  end

  defp assert_eventually(fun, timeout \\ 2_000, interval \\ 25)
       when is_function(fun, 0) and is_integer(timeout) and timeout > 0 do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(fun, deadline, interval)
  end

  defp do_assert_eventually(fun, deadline, interval) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("eventual assertion timed out")
      else
        Process.sleep(interval)
        do_assert_eventually(fun, deadline, interval)
      end
    end
  end
end
