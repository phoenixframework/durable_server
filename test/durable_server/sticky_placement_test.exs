defmodule DurableServer.StickyPlacementTest do
  use ExUnit.Case, async: false
  import DurableServer.TestHelper
  alias DurableServer

  defmodule StickyPlacementTestServer do
    use DurableServer, vsn: 1

    @impl true
    def init(state) do
      {:ok, state}
    end

    @impl true
    def handle_call(:get_state, _from, state) do
      {:reply, state, state}
    end

    @impl true
    def handle_call(:crash, _from, _state) do
      raise "Intentional crash"
    end

    @impl true
    def dump_state(state), do: state

    @impl true
    def load_state(_vsn, state), do: state
  end

  setup do
    supervisor_name = :"test_supervisor_#{:erlang.unique_integer([:positive])}"
    prefix = "sticky_placement_test_#{:erlang.unique_integer([:positive])}/"

    {:ok, supervisor_name: supervisor_name, prefix: prefix}
  end

  describe "sticky_placement configuration" do
    test "accepts valid sticky_placement config with keyword list format", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      # Should not raise
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         sticky_placement: %{
           StickyPlacementTestServer => [
             FLY_MACHINE_ID: 10_000,
             FLY_REGION: 20_000,
             any: 30_000
           ]
         }}
      )
    end

    test "accepts default_sticky_placement config", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      # Should not raise
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         default_sticky_placement: [
           FLY_REGION: 10_000,
           any: 20_000
         ]}
      )
    end

    test "raises on invalid env var (non-atom key)", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      assert_raise RuntimeError, ~r/must be a keyword list/, fn ->
        start_supervised!(
          {DurableServer.Supervisor,
           name: supervisor_name,
           prefix: prefix,
           object_store: test_object_store_opts(),
           sticky_placement: %{
             StickyPlacementTestServer => %{
               "FLY_REGION" => 10_000
             }
           }}
        )
      end
    end

    test "raises on invalid delay (non-integer)", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      assert_raise RuntimeError, ~r/must be non-negative integers/, fn ->
        start_supervised!(
          {DurableServer.Supervisor,
           name: supervisor_name,
           prefix: prefix,
           object_store: test_object_store_opts(),
           sticky_placement: %{
             StickyPlacementTestServer => [
               FLY_REGION: "10000"
             ]
           }}
        )
      end
    end
  end

  describe "__get_sticky_placement_for_module__/2" do
    test "returns config for module", %{supervisor_name: supervisor_name, prefix: prefix} do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         sticky_placement: %{
           StickyPlacementTestServer => [
             FLY_MACHINE_ID: 5_000,
             FLY_REGION: 15_000
           ]
         }}
      )

      config =
        DurableServer.Supervisor.__get_sticky_placement_for_module__(
          supervisor_name,
          StickyPlacementTestServer
        )

      assert config == [FLY_MACHINE_ID: 5_000, FLY_REGION: 15_000]
    end

    test "returns default when module not configured", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         default_sticky_placement: [
           FLY_REGION: 10_000
         ]}
      )

      config =
        DurableServer.Supervisor.__get_sticky_placement_for_module__(
          supervisor_name,
          StickyPlacementTestServer
        )

      assert config == [FLY_REGION: 10_000]
    end

    test "returns nil when no config", %{supervisor_name: supervisor_name, prefix: prefix} do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name, prefix: prefix, object_store: test_object_store_opts()}
      )

      config =
        DurableServer.Supervisor.__get_sticky_placement_for_module__(
          supervisor_name,
          StickyPlacementTestServer
        )

      assert is_nil(config)
    end
  end

  describe "collect_sticky_placement_env_vars/1" do
    test "collects all unique env vars from all modules", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         sticky_placement: %{
           StickyPlacementTestServer => [
             FLY_MACHINE_ID: 5_000,
             FLY_REGION: 10_000,
             any: 15_000
           ],
           __MODULE__ => [
             FLY_REGION: 10_000,
             FLY_APP_NAME: 20_000
           ]
         }}
      )

      env_vars = DurableServer.Supervisor.collect_sticky_placement_env_vars(supervisor_name)

      assert MapSet.new(env_vars) ==
               MapSet.new(["FLY_MACHINE_ID", "FLY_REGION", "FLY_APP_NAME"])
    end

    test "returns empty list when no sticky placement config", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name, prefix: prefix, object_store: test_object_store_opts()}
      )

      env_vars = DurableServer.Supervisor.collect_sticky_placement_env_vars(supervisor_name)

      assert env_vars == []
    end
  end

  describe "sticky_placement in Meta" do
    test "builds sticky_placement with keyword list format", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      # Set env vars
      System.put_env("FLY_MACHINE_ID", "test-machine-456")
      System.put_env("FLY_REGION", "ord")

      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         sticky_placement: %{
           StickyPlacementTestServer => [
             FLY_MACHINE_ID: 10_000,
             FLY_REGION: 20_000,
             any: 30_000
           ]
         }}
      )

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {StickyPlacementTestServer, %{key: "test-key-kw"}}
        )

      assert is_pid(pid)

      # Fetch meta from storage to see sticky_placement
      %{object_store: store} = DurableServer.Supervisor.__get_config__(supervisor_name)

      {:ok, stored_state} =
        DurableServer.fetch_stored_state(store, %{key: "test-key-kw", prefix: prefix})

      assert stored_state.meta.sticky_placement == [
               %{env_var: "FLY_MACHINE_ID", value: "test-machine-456"},
               %{env_var: "FLY_REGION", value: "ord"},
               %{env_var: :any, value: :any}
             ]

      # Cleanup
      System.delete_env("FLY_MACHINE_ID")
      System.delete_env("FLY_REGION")
    end

    test "builds sticky_placement when starting a child", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      # Set env vars
      System.put_env("FLY_MACHINE_ID", "test-machine-123")
      System.put_env("FLY_REGION", "sjc")

      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         sticky_placement: %{
           StickyPlacementTestServer => [
             FLY_MACHINE_ID: 10_000,
             FLY_REGION: 20_000,
             any: 30_000
           ]
         }}
      )

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {StickyPlacementTestServer, %{key: "test-key"}}
        )

      assert is_pid(pid)

      # Fetch meta from storage to see sticky_placement
      %{object_store: store} = DurableServer.Supervisor.__get_config__(supervisor_name)

      {:ok, stored_state} =
        DurableServer.fetch_stored_state(store, %{key: "test-key", prefix: prefix})

      assert stored_state.meta.sticky_placement == [
               %{env_var: "FLY_MACHINE_ID", value: "test-machine-123"},
               %{env_var: "FLY_REGION", value: "sjc"},
               %{env_var: :any, value: :any}
             ]

      # Cleanup
      System.delete_env("FLY_MACHINE_ID")
      System.delete_env("FLY_REGION")
    end

    test "handles nil env var values", %{supervisor_name: supervisor_name, prefix: prefix} do
      # Don't set FLY_MACHINE_ID
      System.delete_env("FLY_MACHINE_ID")

      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         sticky_placement: %{
           StickyPlacementTestServer => [
             FLY_MACHINE_ID: 10_000
           ]
         }}
      )

      {:ok, {_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {StickyPlacementTestServer, %{key: "test-key"}}
        )

      %{object_store: store} = DurableServer.Supervisor.__get_config__(supervisor_name)

      {:ok, stored_state} =
        DurableServer.fetch_stored_state(store, %{key: "test-key", prefix: prefix})

      assert stored_state.meta.sticky_placement == [
               %{env_var: "FLY_MACHINE_ID", value: nil}
             ]
    end
  end

  describe "heartbeat env_vars" do
    test "includes env_vars in heartbeat", %{supervisor_name: supervisor_name, prefix: prefix} do
      System.put_env("FLY_MACHINE_ID", "machine-456")
      System.put_env("FLY_REGION", "ord")

      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         sticky_placement: %{
           StickyPlacementTestServer => [
             FLY_MACHINE_ID: 10_000,
             FLY_REGION: 20_000
           ]
         }}
      )

      # Wait for heartbeat to be written
      Process.sleep(100)

      # Check heartbeat table
      table_name = :"durable_server_heartbeats_#{supervisor_name}"
      node_str = Atom.to_string(Node.self())

      case :ets.lookup(table_name, node_str) do
        [{^node_str, _node_ref, _timestamp, _capacity, _resources, env_vars, _labels}] ->
          assert env_vars == %{
                   "FLY_MACHINE_ID" => "machine-456",
                   "FLY_REGION" => "ord"
                 }

        [] ->
          flunk("Expected heartbeat entry to exist")
      end

      # Cleanup
      System.delete_env("FLY_MACHINE_ID")
      System.delete_env("FLY_REGION")
    end

    test "env_vars is empty map when no sticky placement", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name, prefix: prefix, object_store: test_object_store_opts()}
      )

      # Wait for heartbeat
      Process.sleep(100)

      table_name = :"durable_server_heartbeats_#{supervisor_name}"
      node_str = Atom.to_string(Node.self())

      case :ets.lookup(table_name, node_str) do
        [{^node_str, _node_ref, _timestamp, _capacity, _resources, env_vars, _labels}] ->
          assert env_vars == %{}

        [] ->
          flunk("Expected heartbeat entry to exist")
      end
    end
  end

  describe "heartbeat_meta configuration" do
    test "accepts heartbeat_meta as a static map", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         heartbeat_meta: %{"region" => "ord", "app" => "test"}}
      )

      # Wait for heartbeat
      Process.sleep(100)

      # Check via get_cluster_nodes
      nodes = DurableServer.LifecycleManager.get_cluster_nodes(supervisor_name)
      node_str = Atom.to_string(Node.self())

      assert Map.has_key?(nodes, node_str)
      heartbeat_meta = nodes[node_str].heartbeat_meta
      assert heartbeat_meta["region"] == "ord"
      assert heartbeat_meta["app"] == "test"
    end

    test "accepts heartbeat_meta as a zero-arity function", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         heartbeat_meta: fn -> %{"region" => "sjc", "dynamic" => true} end}
      )

      # Wait for heartbeat
      Process.sleep(100)

      nodes = DurableServer.LifecycleManager.get_cluster_nodes(supervisor_name)
      node_str = Atom.to_string(Node.self())

      assert Map.has_key?(nodes, node_str)
      heartbeat_meta = nodes[node_str].heartbeat_meta
      assert heartbeat_meta["region"] == "sjc"
      assert heartbeat_meta["dynamic"] == true
    end

    test "heartbeat_meta is nil when not configured", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name, prefix: prefix, object_store: test_object_store_opts()}
      )

      # Wait for heartbeat
      Process.sleep(100)

      nodes = DurableServer.LifecycleManager.get_cluster_nodes(supervisor_name)
      node_str = Atom.to_string(Node.self())

      assert Map.has_key?(nodes, node_str)
      assert nodes[node_str].heartbeat_meta == nil
    end

    test "raises when heartbeat_meta function returns non-map", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      Process.flag(:trap_exit, true)

      result =
        DurableServer.Supervisor.start_link(
          name: supervisor_name,
          prefix: prefix,
          object_store: test_object_store_opts(),
          heartbeat_meta: fn -> "not a map" end
        )

      assert {:error, {%ArgumentError{message: message}, _stacktrace}} = result
      assert message =~ "heartbeat_meta function must return a map"
    end

    test "raises when heartbeat_meta is not a map or function", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      Process.flag(:trap_exit, true)

      result =
        DurableServer.Supervisor.start_link(
          name: supervisor_name,
          prefix: prefix,
          object_store: test_object_store_opts(),
          heartbeat_meta: "not valid"
        )

      assert {:error, {%ArgumentError{message: message}, _stacktrace}} = result
      assert message =~ "heartbeat_meta must be a map or a zero-arity function"
    end
  end
end
