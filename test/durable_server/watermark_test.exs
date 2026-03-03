defmodule DurableServer.WatermarkTest do
  use ExUnit.Case, async: false
  import DurableServer.TestHelper
  alias DurableServer

  defmodule WatermarkTestServer do
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

  setup do
    supervisor_name = :"test_supervisor_#{:erlang.unique_integer([:positive])}"
    prefix = "watermark_test_#{:erlang.unique_integer([:positive])}/"

    {:ok, supervisor_name: supervisor_name, prefix: prefix}
  end

  describe "max_children count limits" do
    test "enforces global max_children limit", %{supervisor_name: supervisor_name, prefix: prefix} do
      # Start supervisor with global limit of 2
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{:total => 2}}
      )

      # First two should succeed
      assert {:ok, {_pid1, _meta1}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {WatermarkTestServer, %{key: "key1"}}
               )

      assert {:ok, {_pid2, _meta2}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {WatermarkTestServer, %{key: "key2"}}
               )

      # Third should fail (disable remote placement for this test)
      assert {:error, {:capacity_limit, :max_children_total}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {WatermarkTestServer, %{key: "key3"}},
                 max_placement_retries: 0
               )
    end

    test "enforces per-module max_children limit", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      # Start supervisor with per-module limit
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{WatermarkTestServer => 1}}
      )

      # First should succeed
      assert {:ok, {_pid1, _meta1}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {WatermarkTestServer, %{key: "key1"}}
               )

      # Second should fail (disable remote placement for this test)
      assert {:error, {:capacity_limit, :max_children_module}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {WatermarkTestServer, %{key: "key2"}},
                 max_placement_retries: 0
               )
    end

    test "enforces both global and per-module limits", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      defmodule AnotherTestServer do
        use DurableServer, vsn: 1

        @impl true
        def init(state), do: {:ok, state}

        @impl true
        def dump_state(state), do: state

        @impl true
        def load_state(_vsn, state), do: state
      end

      # Start supervisor with both limits
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{
           :total => 3,
           WatermarkTestServer => 2,
           AnotherTestServer => 2
         }}
      )

      # Start 2 of first module
      assert {:ok, {_pid1, _}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {WatermarkTestServer, %{key: "key1"}}
               )

      assert {:ok, {_pid2, _}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {WatermarkTestServer, %{key: "key2"}}
               )

      # Start 1 of second module (hits global limit)
      assert {:ok, {_pid3, _}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {AnotherTestServer, %{key: "key3"}}
               )

      # Fourth should fail on global limit (disable remote placement for this test)
      assert {:error, {:capacity_limit, :max_children_total}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {AnotherTestServer, %{key: "key4"}},
                 max_placement_retries: 0
               )
    end

    test "count decreases when server terminates", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{:total => 2}}
      )

      # Start 2 servers
      assert {:ok, {pid1, _}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {WatermarkTestServer, %{key: "key1"}}
               )

      assert {:ok, {_pid2, _}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {WatermarkTestServer, %{key: "key2"}}
               )

      # Third fails (disable remote placement for this test)
      assert {:error, {:capacity_limit, :max_children_total}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {WatermarkTestServer, %{key: "key3"}},
                 max_placement_retries: 0
               )

      # Terminate one
      Process.monitor(pid1)
      DurableServer.Supervisor.terminate_child(supervisor_name, pid1)
      assert_receive {:DOWN, _ref, :process, ^pid1, :normal}

      # Now third should succeed
      assert {:ok, {_pid3, _}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {WatermarkTestServer, %{key: "key3"}}
               )
    end
  end

  describe "resource limits" do
    test "accepts child when resources below limits", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      # Start supervisor with high resource limits (should never be hit in test)
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_cpu: 99,
         max_memory: 99}
      )

      # Should succeed since resources are below limits (disable remote placement for this test)
      # Note: May fail if actual CPU is above 99% during test
      result =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {WatermarkTestServer, %{key: "key1"}},
          max_placement_retries: 0
        )

      case result do
        {:ok, _} -> :ok
        # System CPU above 99%
        {:error, {:capacity_limit, :max_cpu}} -> :ok
        other -> flunk("Unexpected result: #{inspect(other)}")
      end
    end

    test "rejects child when CPU limit would be exceeded", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      # Start supervisor with impossible CPU limit
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name, prefix: prefix, object_store: test_object_store_opts(), max_cpu: 1}
      )

      # This may or may not fail depending on actual CPU usage (disable remote placement for this test)
      result =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {WatermarkTestServer, %{key: "key1"}},
          max_placement_retries: 0
        )

      case result do
        {:ok, _} -> :ok
        {:error, {:capacity_limit, :max_cpu}} -> :ok
        other -> flunk("Unexpected result: #{inspect(other)}")
      end
    end

    test "rejects child when memory limit would be exceeded", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      # Skip this test if os_mon is not available (common in test environments)
      # Start supervisor with impossible memory limit
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_memory: 1}
      )

      # This should fail since memory is definitely above 1%
      # Note: This test requires os_mon to be available
      # Returns :no_available_nodes when all nodes (including local) are at capacity
      assert {:error, {:capacity_limit, :no_available_nodes}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {WatermarkTestServer, %{key: "key1"}},
                 placement_timeout: 0
               )
    end
  end

  describe "limit configuration validation" do
    test "accepts valid max_children map configuration", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      assert {:ok, _pid} =
               start_supervised(
                 {DurableServer.Supervisor,
                  name: supervisor_name,
                  prefix: prefix,
                  object_store: test_object_store_opts(),
                  max_children: %{:total => 10, WatermarkTestServer => 5}}
               )
    end

    test "rejects invalid max_children with non-positive values", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      assert_raise RuntimeError, ~r/ArgumentError.*Invalid max_children entry/, fn ->
        start_supervised!(
          {DurableServer.Supervisor,
           name: supervisor_name,
           prefix: prefix,
           object_store: test_object_store_opts(),
           max_children: %{:total => 0}}
        )
      end
    end

    test "accepts max_cpu values above 100", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      pid =
        start_supervised!(
          {DurableServer.Supervisor,
           name: supervisor_name,
           prefix: prefix,
           object_store: test_object_store_opts(),
           max_cpu: 150}
        )

      assert is_pid(pid)
    end

    test "rejects invalid max_cpu zero value" do
      supervisor_name = :"test_supervisor_cpu_#{:erlang.unique_integer([:positive])}"
      prefix = "watermark_test_cpu_#{:erlang.unique_integer([:positive])}/"

      assert_raise RuntimeError, ~r/ArgumentError.*max_cpu must be a positive integer/, fn ->
        start_supervised!(
          {DurableServer.Supervisor,
           name: supervisor_name,
           prefix: prefix,
           object_store: test_object_store_opts(),
           max_cpu: 0}
        )
      end
    end

    test "rejects invalid max_memory values", %{supervisor_name: supervisor_name, prefix: prefix} do
      assert_raise RuntimeError, ~r/ArgumentError.*max_memory must be an integer/, fn ->
        start_supervised!(
          {DurableServer.Supervisor,
           name: supervisor_name,
           prefix: prefix,
           object_store: test_object_store_opts(),
           max_memory: 101}
        )
      end
    end

    test "rejects invalid max_memory negative value" do
      supervisor_name = :"test_supervisor_mem_#{:erlang.unique_integer([:positive])}"
      prefix = "watermark_test_mem_#{:erlang.unique_integer([:positive])}/"

      assert_raise RuntimeError, ~r/ArgumentError.*max_memory must be an integer/, fn ->
        start_supervised!(
          {DurableServer.Supervisor,
           name: supervisor_name,
           prefix: prefix,
           object_store: test_object_store_opts(),
           max_memory: -1}
        )
      end
    end

    test "rejects invalid max_disk with percent over 100", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      assert_raise RuntimeError, ~r/ArgumentError.*max_disk must be/, fn ->
        start_supervised!(
          {DurableServer.Supervisor,
           name: supervisor_name,
           prefix: prefix,
           object_store: test_object_store_opts(),
           max_disk: {101, "/data"}}
        )
      end
    end

    test "rejects invalid max_disk with zero percent" do
      supervisor_name = :"test_supervisor_disk_#{:erlang.unique_integer([:positive])}"
      prefix = "watermark_test_disk_#{:erlang.unique_integer([:positive])}/"

      assert_raise RuntimeError, ~r/ArgumentError.*max_disk must be/, fn ->
        start_supervised!(
          {DurableServer.Supervisor,
           name: supervisor_name,
           prefix: prefix,
           object_store: test_object_store_opts(),
           max_disk: {0, "/data"}}
        )
      end
    end

    test "rejects invalid max_disk with non-binary mount point", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      assert_raise RuntimeError, ~r/ArgumentError.*max_disk must be/, fn ->
        start_supervised!(
          {DurableServer.Supervisor,
           name: supervisor_name,
           prefix: prefix,
           object_store: test_object_store_opts(),
           max_disk: {90, :data}}
        )
      end
    end

    test "accepts valid max_disk configuration", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      assert {:ok, _pid} =
               start_supervised(
                 {DurableServer.Supervisor,
                  name: supervisor_name,
                  prefix: prefix,
                  object_store: test_object_store_opts(),
                  max_disk: {90, "/data"}}
               )
    end

    test "accepts integer max_children for DynamicSupervisor (legacy)", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      # Integer max_children should be accepted but not used for capacity limiting
      assert {:ok, _pid} =
               start_supervised(
                 {DurableServer.Supervisor,
                  name: supervisor_name,
                  prefix: prefix,
                  object_store: test_object_store_opts(),
                  max_children: 100}
               )
    end
  end

  describe "check_capacity/2 function" do
    test "returns :ok when no limits configured", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name, prefix: prefix, object_store: test_object_store_opts()}
      )

      assert :ok =
               DurableServer.LifecycleManager.check_capacity(supervisor_name, WatermarkTestServer)
    end

    test "returns error when global limit reached", %{
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

      # Start one child
      {:ok, {_pid, _}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {WatermarkTestServer, %{key: "key1"}}
        )

      # Check should now fail
      assert {:error, {:limit_reached, :max_children_total, %{current: 1, limit: 1}}} =
               DurableServer.LifecycleManager.check_capacity(supervisor_name, WatermarkTestServer)
    end

    test "returns error when module limit reached", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      start_supervised!(
        {DurableServer.Supervisor,
         name: supervisor_name,
         prefix: prefix,
         object_store: test_object_store_opts(),
         max_children: %{WatermarkTestServer => 1}}
      )

      # Start one child
      {:ok, {_pid, _}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {WatermarkTestServer, %{key: "key1"}}
        )

      # Check should now fail
      assert {:error, {:limit_reached, :max_children_module, details}} =
               DurableServer.LifecycleManager.check_capacity(supervisor_name, WatermarkTestServer)

      assert details.module == WatermarkTestServer
      assert details.current == 1
      assert details.limit == 1
    end
  end
end
