defmodule DurableServer.RemotePlacementTest do
  use ExUnit.Case, async: false
  import DurableServer.TestHelper
  alias DurableServer

  @moduletag :capture_log

  defmodule RemotePlacementTestServer do
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
    def dump_state(state), do: state

    @impl true
    def load_state(_vsn, state), do: state
  end

  defmodule EnsureSingleflightTestServer do
    use DurableServer, vsn: 1

    @impl true
    def init(%{singleflight_delay_ms: delay_ms} = state)
        when is_integer(delay_ms) and delay_ms > 0 do
      Process.sleep(delay_ms)
      {:ok, Map.delete(state, :singleflight_delay_ms)}
    end

    def init(state) do
      {:ok, state}
    end

    @impl true
    def dump_state(state), do: state

    @impl true
    def load_state(_vsn, state), do: state
  end

  setup do
    supervisor_name = :"test_supervisor_#{:erlang.unique_integer([:positive])}"
    prefix = "remote_placement_test_#{:erlang.unique_integer([:positive])}/"

    {:ok, supervisor_name: supervisor_name, prefix: prefix}
  end

  describe "find_eligible_nodes/3" do
    test "returns empty list when no nodes are eligible", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      # Start supervisor with capacity limit of 1
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{:total => 1}}
      )

      # Start one child to fill capacity
      {:ok, {_pid, _}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {RemotePlacementTestServer, %{key: "key1"}},
          max_placement_retries: 0
        )

      # Should return empty list since local node is at capacity and always excluded
      assert [] =
               DurableServer.LifecycleManager.find_eligible_nodes(
                 supervisor_name,
                 RemotePlacementTestServer
               )
    end

    test "always excludes local node", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      # Start supervisor with capacity for 2
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{:total => 2}}
      )

      # Local node should never be included (function is only called after local placement fails)
      nodes =
        DurableServer.LifecycleManager.find_eligible_nodes(
          supervisor_name,
          RemotePlacementTestServer
        )

      refute Node.self() in nodes
    end

    test "respects limit option", %{supervisor_name: supervisor_name, prefix: prefix} do
      # Start supervisor
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{:total => 100}}
      )

      # Should respect limit even if more nodes available
      nodes =
        DurableServer.LifecycleManager.find_eligible_nodes(
          supervisor_name,
          RemotePlacementTestServer,
          limit: 1
        )

      # Empty since no remote nodes available in test
      assert length(nodes) <= 1
    end
  end

  describe "start_child with max_placement_retries" do
    test "succeeds locally when capacity available", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{:total => 10}}
      )

      # Should start locally
      assert {:ok, {pid, _meta}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {RemotePlacementTestServer, %{key: "key1"}},
                 max_placement_retries: 3
               )

      assert is_pid(pid)
      assert node(pid) == Node.self()
    end

    test "returns capacity error when local at capacity and max_placement_retries is 0", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{:total => 1}}
      )

      # Fill local capacity
      {:ok, {_pid1, _}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {RemotePlacementTestServer, %{key: "key1"}},
          max_placement_retries: 0
        )

      # Should fail with capacity error when max_placement_retries: 0
      assert {:error, {:capacity_limit, :max_children_total}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {RemotePlacementTestServer, %{key: "key2"}},
                 max_placement_retries: 0
               )
    end

    test "returns no_available_nodes when local at capacity and no remote nodes", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{:total => 1}}
      )

      # Fill local capacity
      {:ok, {_pid1, _}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {RemotePlacementTestServer, %{key: "key1"}},
          max_placement_retries: 0
        )

      # Should fail with no_available_nodes when trying remote placement
      assert {:error, {:capacity_limit, :no_available_nodes}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {RemotePlacementTestServer, %{key: "key2"}},
                 max_placement_retries: 3,
                 placement_timeout: 0
               )
    end
  end

  describe "ensure_started_child with max_placement_retries" do
    test "passes max_placement_retries to start_child", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{:total => 10}}
      )

      # Should start locally
      assert {:ok, {pid, _meta}} =
               DurableServer.Supervisor.ensure_started_child(
                 supervisor_name,
                 {RemotePlacementTestServer, %{key: "key1"}},
                 max_placement_retries: 3
               )

      assert is_pid(pid)
      assert node(pid) == Node.self()
    end

    test "returns existing process if already started", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_singleflight_waiters_per_key_module: nil}
      )

      # Start first time
      {:ok, {pid1, _}} =
        DurableServer.Supervisor.ensure_started_child(
          supervisor_name,
          {RemotePlacementTestServer, %{key: "key1"}},
          max_placement_retries: 3
        )

      # Second call should return same pid
      {:ok, {pid2, _}} =
        DurableServer.Supervisor.ensure_started_child(
          supervisor_name,
          {RemotePlacementTestServer, %{key: "key1"}},
          max_placement_retries: 3
        )

      assert pid1 == pid2
    end

    test "coalesces concurrent ensure_started_child calls by key+module", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_singleflight_waiters_per_key_module: nil}
      )

      key = "singleflight-#{:erlang.unique_integer([:positive])}"

      child_spec =
        {EnsureSingleflightTestServer, %{key: key, singleflight_delay_ms: 120}}

      results =
        1..24
        |> Task.async_stream(
          fn _ ->
            DurableServer.Supervisor.ensure_started_child(supervisor_name, child_spec)
          end,
          max_concurrency: 24,
          ordered: false,
          timeout: :timer.seconds(10)
        )
        |> Enum.map(fn {:ok, result} -> result end)

      pids =
        Enum.map(results, fn
          {:ok, {pid, _meta}} when is_pid(pid) -> pid
          other -> flunk("Unexpected ensure_started_child result: #{inspect(other)}")
        end)

      assert length(Enum.uniq(pids)) == 1

      diagnostics = DurableServer.LifecycleManager.get_discovery_diagnostics(supervisor_name)
      assert Map.get(diagnostics, :ensure_started_singleflight_leader, 0) >= 1
      assert Map.get(diagnostics, :ensure_started_singleflight_waiter, 0) >= 1
    end

    test "fails fast when singleflight waiter cap is exceeded", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_singleflight_waiters_per_key_module: 1}
      )

      key = "singleflight-cap-#{:erlang.unique_integer([:positive])}"
      child_spec = {EnsureSingleflightTestServer, %{key: key, singleflight_delay_ms: 300}}

      results =
        1..24
        |> Task.async_stream(
          fn _ ->
            DurableServer.Supervisor.ensure_started_child(supervisor_name, child_spec)
          end,
          max_concurrency: 24,
          ordered: false,
          timeout: :timer.seconds(10)
        )
        |> Enum.map(fn {:ok, result} -> result end)

      success_count =
        Enum.count(results, fn
          {:ok, {pid, _meta}} when is_pid(pid) -> true
          _ -> false
        end)

      overloaded_count =
        Enum.count(results, fn
          {:error, :singleflight_overloaded} -> true
          _ -> false
        end)

      assert success_count >= 2
      assert overloaded_count >= 1
    end
  end

  describe "can_node_accept_module?/2" do
    test "returns true for healthy node with capacity" do
      health =
        {:healthy,
         %{
           node_ref: "test-ref",
           capacity: %{:total => %{current: 5, limit: 10}},
           resources: %{cpu: 50, max_cpu: 80}
         }}

      assert DurableServer.LifecycleManager.can_node_accept_module?(
               health,
               RemotePlacementTestServer
             )
    end

    test "returns false when at global capacity" do
      health =
        {:healthy,
         %{
           node_ref: "test-ref",
           capacity: %{:total => %{current: 10, limit: 10}},
           resources: %{cpu: 50, max_cpu: 80}
         }}

      refute DurableServer.LifecycleManager.can_node_accept_module?(
               health,
               RemotePlacementTestServer
             )
    end

    test "returns false when at module capacity" do
      health =
        {:healthy,
         %{
           node_ref: "test-ref",
           capacity: %{
             :total => %{current: 5, limit: 10},
             RemotePlacementTestServer => %{current: 3, limit: 3}
           },
           resources: %{cpu: 50, max_cpu: 80}
         }}

      refute DurableServer.LifecycleManager.can_node_accept_module?(
               health,
               RemotePlacementTestServer
             )
    end

    test "returns false when CPU at capacity" do
      health =
        {:healthy,
         %{
           node_ref: "test-ref",
           capacity: %{:total => %{current: 5, limit: 10}},
           resources: %{cpu: 85, max_cpu: 80}
         }}

      refute DurableServer.LifecycleManager.can_node_accept_module?(
               health,
               RemotePlacementTestServer
             )
    end

    test "returns false when memory at capacity" do
      health =
        {:healthy,
         %{
           node_ref: "test-ref",
           capacity: %{:total => %{current: 5, limit: 10}},
           resources: %{memory: 90, max_memory: 85}
         }}

      refute DurableServer.LifecycleManager.can_node_accept_module?(
               health,
               RemotePlacementTestServer
             )
    end

    test "returns true when no capacity info (backwards compat)" do
      health =
        {:healthy,
         %{
           node_ref: "test-ref",
           capacity: nil,
           resources: nil
         }}

      assert DurableServer.LifecycleManager.can_node_accept_module?(
               health,
               RemotePlacementTestServer
             )
    end

    test "returns false when node heartbeat marks node as draining" do
      health =
        {:healthy,
         %{
           node_ref: "test-ref",
           capacity: %{:total => %{current: 1, limit: 10}},
           resources: %{cpu: 10, max_cpu: 80},
           heartbeat_meta: %{"draining" => true}
         }}

      refute DurableServer.LifecycleManager.can_node_accept_module?(
               health,
               RemotePlacementTestServer
             )
    end

    test "returns false for stale nodes" do
      refute DurableServer.LifecycleManager.can_node_accept_module?(
               :stale,
               RemotePlacementTestServer
             )
    end

    test "returns false for unknown nodes" do
      refute DurableServer.LifecycleManager.can_node_accept_module?(
               :unknown,
               RemotePlacementTestServer
             )
    end

    test "returns false when disk at capacity" do
      health =
        {:healthy,
         %{
           node_ref: "test-ref",
           capacity: %{:total => %{current: 5, limit: 10}},
           resources: %{disk: 95, max_disk: 90}
         }}

      refute DurableServer.LifecycleManager.can_node_accept_module?(
               health,
               RemotePlacementTestServer
             )
    end

    test "returns true when disk below capacity" do
      health =
        {:healthy,
         %{
           node_ref: "test-ref",
           capacity: %{:total => %{current: 5, limit: 10}},
           resources: %{disk: 80, max_disk: 90}
         }}

      assert DurableServer.LifecycleManager.can_node_accept_module?(
               health,
               RemotePlacementTestServer
             )
    end

    test "bypasses disk check when matching_level is 0 (sticky local)" do
      health =
        {:healthy,
         %{
           node_ref: "test-ref",
           capacity: %{:total => %{current: 5, limit: 10}},
           resources: %{disk: 95, max_disk: 90}
         }}

      # Without matching_level: 0, should fail disk check
      refute DurableServer.LifecycleManager.can_node_accept_module?(
               health,
               RemotePlacementTestServer
             )

      # With matching_level: 0, should bypass disk check
      assert DurableServer.LifecycleManager.can_node_accept_module?(
               health,
               RemotePlacementTestServer,
               matching_level: 0
             )
    end

    test "does not bypass CPU check when matching_level is 0" do
      health =
        {:healthy,
         %{
           node_ref: "test-ref",
           capacity: %{:total => %{current: 5, limit: 10}},
           resources: %{cpu: 85, max_cpu: 80}
         }}

      # Even with matching_level: 0, CPU check should still fail
      refute DurableServer.LifecycleManager.can_node_accept_module?(
               health,
               RemotePlacementTestServer,
               matching_level: 0
             )
    end

    test "does not bypass memory check when matching_level is 0" do
      health =
        {:healthy,
         %{
           node_ref: "test-ref",
           capacity: %{:total => %{current: 5, limit: 10}},
           resources: %{memory: 90, max_memory: 85}
         }}

      # Even with matching_level: 0, memory check should still fail
      refute DurableServer.LifecycleManager.can_node_accept_module?(
               health,
               RemotePlacementTestServer,
               matching_level: 0
             )
    end
  end

  describe "start_child with local_only: true" do
    test "succeeds locally when capacity available", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{:total => 10}}
      )

      assert {:ok, {pid, _meta}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {RemotePlacementTestServer, %{key: "local_only_ok"}},
                 local_only: true
               )

      assert is_pid(pid)
      assert node(pid) == Node.self()
    end

    test "returns capacity error instead of trying remote placement", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{:total => 1}}
      )

      # Fill local capacity
      {:ok, {_pid, _}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {RemotePlacementTestServer, %{key: "local_fill"}},
          local_only: true
        )

      # Should fail with capacity error — NOT try remote placement
      assert {:error, {:capacity_limit, :max_children_total}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {RemotePlacementTestServer, %{key: "local_only_fail"}},
                 local_only: true
               )
    end
  end

  describe "ensure_started_child with local_only: true" do
    test "starts locally when capacity available", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{:total => 10}}
      )

      assert {:ok, {pid, _meta}} =
               DurableServer.Supervisor.ensure_started_child(
                 supervisor_name,
                 {RemotePlacementTestServer, %{key: "ensure_local_ok"}},
                 local_only: true
               )

      assert is_pid(pid)
      assert node(pid) == Node.self()
    end

    test "returns existing process if already started", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name, prefix: prefix, object_store: test_object_store_opts()}
      )

      {:ok, {pid1, _}} =
        DurableServer.Supervisor.ensure_started_child(
          supervisor_name,
          {RemotePlacementTestServer, %{key: "ensure_local_existing"}},
          local_only: true
        )

      {:ok, {pid2, _}} =
        DurableServer.Supervisor.ensure_started_child(
          supervisor_name,
          {RemotePlacementTestServer, %{key: "ensure_local_existing"}},
          local_only: true
        )

      assert pid1 == pid2
    end

    test "returns capacity error instead of trying remote placement", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{:total => 1}}
      )

      # Fill local capacity
      {:ok, {_pid, _}} =
        DurableServer.Supervisor.ensure_started_child(
          supervisor_name,
          {RemotePlacementTestServer, %{key: "ensure_fill"}},
          local_only: true
        )

      # Should fail with capacity error — NOT try remote placement
      assert {:error, {:capacity_limit, _reason}} =
               DurableServer.Supervisor.ensure_started_child(
                 supervisor_name,
                 {RemotePlacementTestServer, %{key: "ensure_local_fail"}},
                 local_only: true
               )
    end
  end
end
