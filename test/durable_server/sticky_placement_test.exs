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

    test "heartbeat_meta defaults to empty map when not configured", %{
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
      assert nodes[node_str].heartbeat_meta == %{}
    end

    test "stop_discovery writes draining heartbeat immediately", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name, prefix: prefix, object_store: test_object_store_opts()}
      )

      Process.sleep(100)

      :ok = DurableServer.LifecycleManager.stop_discovery(supervisor_name)

      nodes = DurableServer.LifecycleManager.get_cluster_nodes(supervisor_name)
      node_str = Atom.to_string(Node.self())

      assert Map.has_key?(nodes, node_str)
      assert nodes[node_str].heartbeat_meta["draining"] == true
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

  describe "find_my_matching_level behavior" do
    # These tests verify the sticky placement level matching logic:
    # - nil/empty config -> level 0 (any node matches)
    # - config with :any -> all nodes match at some level (never nil)
    # - config WITHOUT :any -> non-matching nodes get nil (can never claim)

    test "no sticky config returns level 0 for any node", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      # No sticky_placement configured at all
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name, prefix: prefix, object_store: test_object_store_opts()}
      )

      # Start a server to create storage state
      System.put_env("FLY_REGION", "ord")

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {StickyPlacementTestServer, %{key: "no-sticky-test"}}
        )

      assert is_pid(pid)

      # Get the augmented sticky placement - should be nil (no config)
      augmented =
        DurableServer.Supervisor.__get_augmented_sticky_placement__(
          supervisor_name,
          StickyPlacementTestServer,
          "no-sticky-test"
        )

      # With no sticky config, augmented should be nil
      assert augmented == nil

      System.delete_env("FLY_REGION")
    end

    test "sticky config with :any allows all nodes to match", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      System.put_env("FLY_REGION", "ord")

      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         sticky_placement: %{
           StickyPlacementTestServer => [
             FLY_REGION: 20_000,
             any: 50_000
           ]
         }}
      )

      {:ok, {_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {StickyPlacementTestServer, %{key: "with-any-test"}}
        )

      # Get the augmented sticky placement
      augmented =
        DurableServer.Supervisor.__get_augmented_sticky_placement__(
          supervisor_name,
          StickyPlacementTestServer,
          "with-any-test"
        )

      # Should have both FLY_REGION entry and :any entry
      assert length(augmented) == 2
      assert Enum.any?(augmented, fn p -> p.env_var == "FLY_REGION" and p.value == "ord" end)
      assert Enum.any?(augmented, fn p -> p.env_var == :any and p.value == :any end)

      System.delete_env("FLY_REGION")
    end

    test "sticky config WITHOUT :any does not include :any fallback", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      System.put_env("FLY_REGION", "ord")

      start_supervised!({DurableServer.Supervisor,
       name: supervisor_name,
       prefix: prefix,
       object_store: test_object_store_opts(),
       sticky_placement: %{
         # Note: NO :any here - only FLY_REGION
         StickyPlacementTestServer => [
           FLY_REGION: 20_000
         ]
       }})

      {:ok, {_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {StickyPlacementTestServer, %{key: "without-any-test"}}
        )

      # Get the augmented sticky placement
      augmented =
        DurableServer.Supervisor.__get_augmented_sticky_placement__(
          supervisor_name,
          StickyPlacementTestServer,
          "without-any-test"
        )

      # Should ONLY have FLY_REGION entry, NO :any
      assert length(augmented) == 1
      assert Enum.any?(augmented, fn p -> p.env_var == "FLY_REGION" and p.value == "ord" end)
      refute Enum.any?(augmented, fn p -> p.env_var == :any end)

      System.delete_env("FLY_REGION")
    end

    test "non-matching node returns nil level when :any not configured", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      # Server was started in region "ord"
      System.put_env("FLY_REGION", "ord")

      start_supervised!({DurableServer.Supervisor,
       name: supervisor_name,
       prefix: prefix,
       object_store: test_object_store_opts(),
       sticky_placement: %{
         # NO :any - only matching region can claim
         StickyPlacementTestServer => [
           FLY_REGION: 20_000
         ]
       }})

      {:ok, {_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {StickyPlacementTestServer, %{key: "non-matching-test"}}
        )

      # Get the stored sticky_placement (persisted with "ord")
      augmented =
        DurableServer.Supervisor.__get_augmented_sticky_placement__(
          supervisor_name,
          StickyPlacementTestServer,
          "non-matching-test"
        )

      # The persisted sticky_placement has FLY_REGION: "ord"
      assert augmented == [%{env_var: "FLY_REGION", value: "ord"}]

      # Now simulate a different node (FLY_REGION = "sjc")
      # This node should NOT be able to claim because:
      # 1. It doesn't match FLY_REGION: "ord"
      # 2. There's no :any fallback
      System.put_env("FLY_REGION", "sjc")

      my_env_vars = %{"FLY_REGION" => "sjc"}

      # Manually test find_my_matching_level logic:
      # With augmented = [%{env_var: "FLY_REGION", value: "ord"}]
      # and my_env_vars = %{"FLY_REGION" => "sjc"}
      # The node does NOT match any level, so should return nil
      matching_level =
        Enum.find_index(augmented, fn preference ->
          case preference do
            %{env_var: :any, value: :any} ->
              true

            %{env_var: env_var, value: expected_value} ->
              Map.get(my_env_vars, env_var) == expected_value

            _ ->
              false
          end
        end)

      # Non-matching node with no :any should get nil
      assert matching_level == nil

      System.delete_env("FLY_REGION")
    end

    test "matching node returns level 0 for sticky placement", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      System.put_env("FLY_REGION", "ord")

      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         sticky_placement: %{
           StickyPlacementTestServer => [
             FLY_REGION: 20_000
           ]
         }}
      )

      {:ok, {_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {StickyPlacementTestServer, %{key: "matching-test"}}
        )

      augmented =
        DurableServer.Supervisor.__get_augmented_sticky_placement__(
          supervisor_name,
          StickyPlacementTestServer,
          "matching-test"
        )

      # Same region - should match level 0
      my_env_vars = %{"FLY_REGION" => "ord"}

      matching_level =
        Enum.find_index(augmented, fn preference ->
          case preference do
            %{env_var: :any, value: :any} ->
              true

            %{env_var: env_var, value: expected_value} ->
              Map.get(my_env_vars, env_var) == expected_value

            _ ->
              false
          end
        end)

      # Matching node should get level 0
      assert matching_level == 0

      System.delete_env("FLY_REGION")
    end
  end
end
