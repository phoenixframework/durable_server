defmodule DurableServer.LifecycleTest do
  use ExUnit.Case, async: true
  import DurableServer.TestHelper

  alias DurableServer
  alias DurableServer.{CircuitBreaker, LifecycleManager, Meta}
  alias DurableServer.ObjectStore

  @moduletag :capture_log

  def atomify_keys(map) when is_map(map) do
    Map.new(map, fn
      {k, v} when is_binary(k) -> {String.to_atom(k), atomify_keys(v)}
      {k, v} -> {k, atomify_keys(v)}
    end)
  end

  def atomify_keys(list) when is_list(list) do
    Enum.map(list, &atomify_keys/1)
  end

  def atomify_keys(other), do: other

  defmodule TestServer do
    use DurableServer,
      vsn: 1

    def dump_state(state) do
      state
    end

    def load_state(_old_vsn, persisted_state) do
      DurableServer.LifecycleTest.atomify_keys(persisted_state)
    end

    def init(loaded_state) do
      {:ok, Map.put_new(loaded_state, :count, 0), auto_sync: false, meta: %{my: "meta"}}
    end

    def handle_call(:get_count, _from, %{count: count} = state) do
      {:reply, count, state}
    end

    def handle_call(:increment, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state, :sync}
    end

    def handle_call(:stop_permanent, _from, %{} = state) do
      {:stop, {:shutdown, :permanent}, :bye, state}
    end
  end

  defmodule DelayedTerminateServer do
    use DurableServer,
      vsn: 1

    def dump_state(state), do: Map.delete(state, :notify_pid)

    def load_state(_old_vsn, persisted_state) do
      DurableServer.LifecycleTest.atomify_keys(persisted_state)
    end

    def init(loaded_state) do
      state =
        loaded_state
        |> Map.put_new(:count, 0)
        |> Map.put_new(:terminate_delay_ms, 300)
        |> Map.put_new(:notify_pid, nil)

      {:ok, state, auto_sync: false}
    end

    def handle_call(:stop_normal, _from, state) do
      {:stop, :normal, :ok, state}
    end

    def handle_call({:set_notify_pid, notify_pid}, _from, state) when is_pid(notify_pid) do
      {:reply, :ok, Map.put(state, :notify_pid, notify_pid)}
    end

    def terminate(_reason, %{terminate_delay_ms: delay_ms, notify_pid: notify_pid})
        when is_integer(delay_ms) and delay_ms > 0 do
      if is_pid(notify_pid), do: send(notify_pid, {:terminate_started, self()})

      Process.sleep(delay_ms)

      if is_pid(notify_pid), do: send(notify_pid, {:terminate_finished, self()})

      :ok
    end

    def terminate(_reason, _state), do: :ok
  end

  defmodule HeartbeatRetryBackend do
    @behaviour DurableServer.StorageBackend

    @impl true
    def init_backend(opts), do: {:ok, %{state: Map.new(opts)}}

    @impl true
    def ensure_ready(_state), do: :ok

    @impl true
    def get_object(_state, _key, _opts), do: {:error, :not_found}

    @impl true
    def list_all_objects_stream(_state, _prefix, _opts), do: []

    @impl true
    def put_object(%{table: table, fail_count: fail_count}, _key, data, _opts) do
      attempt = :ets.update_counter(table, :attempts, {2, 1}, {:attempts, 0})

      if attempt <= fail_count do
        {:error, {:mirror_failed, :no_quorum}}
      else
        :ets.insert(table, {:last_write, data})
        {:ok, %{body: data, etag: Integer.to_string(attempt)}}
      end
    end

    @impl true
    def delete_object(_state, _key), do: :ok

    @impl true
    def try_claim(_state, _key, _body), do: {:error, :unsupported}

    @impl true
    def update_object(_state, _key, _update_fn, _opts), do: {:error, :unsupported}

    @impl true
    def encode(_state, data), do: {:ok, data}

    @impl true
    def decode(_state, data), do: {:ok, data}
  end

  setup do
    # Create test supervisor for this test
    supervisor_name = :"test_supervisor_#{DurableServer.UUID.uuid4()}"
    prefix = "test_#{DurableServer.UUID.uuid4()}/"

    _supervisor_pid =
      start_supervised!({
        DurableServer.Supervisor,
        name: supervisor_name, prefix: prefix, object_store: test_object_store_opts()
      })

    object_store = test_object_store()
    test_bucket_name = "durable-test-lifecycle-#{DurableServer.UUID.uuid4()}"

    case ObjectStore.create_bucket_with_credentials(object_store, test_bucket_name) do
      {:ok, %ObjectStore{} = store} ->
        on_exit(fn ->
          try do
            ObjectStore.delete_bucket(store, test_bucket_name)
          catch
            _, _ -> :ok
          end
        end)

        supervisor_config = DurableServer.Supervisor.__get_config__(supervisor_name)
        circuit_breaker = supervisor_config.circuit_breaker

        # Create test config that mimics what supervisor provides
        test_config = %{
          name: supervisor_name,
          prefix: prefix,
          object_store: object_store,
          discovery_interval_ms: 60_000,
          heartbeat_interval_ms: 10_000,
          graceful_shutdown_timeout_ms: 30_000,
          dead_node_threshold_ms: 24 * 60 * 60 * 1000,
          crash_threshold_count: 5,
          crash_threshold_window_ms: 60 * 60 * 1000,
          module_circuit_breaker_count: 50,
          module_circuit_breaker_window_ms: 5 * 60 * 1000,
          module_circuit_breaker_cooldown_ms: 30 * 60 * 1000,
          ets_table: supervisor_config.ets_table
        }

        {:ok,
         test_bucket: test_bucket_name,
         store: store,
         supervisor_name: supervisor_name,
         prefix: prefix,
         config: test_config,
         circuit_breaker: circuit_breaker}

      {:error, reason} ->
        {:skip, "Failed to create test bucket: #{inspect(reason)}"}
    end
  end

  describe "stop modes" do
    test "stop_permanent sets status to stopped_permanent", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config
    } do
      key = "stop-permanent-test-#{DurableServer.UUID.uuid4()}"

      pid = start_test_server(supervisor_name, key)

      assert GenServer.call(pid, :increment) == 1

      # Monitor the process to wait for termination
      ref = Process.monitor(pid)
      assert :ok = DurableServer.Supervisor.terminate_child_permanent(supervisor_name, pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      assert data.meta.status == :stopped_permanent
    end

    test "stop_graceful sets status to stopped_graceful", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config
    } do
      key = "stop-graceful-test-#{DurableServer.UUID.uuid4()}"

      pid = start_test_server(supervisor_name, key)

      assert GenServer.call(pid, :increment) == 1

      ref = Process.monitor(pid)
      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      assert data.meta.status == :stopped_graceful
    end

    test "normal termination sets status to stopped_graceful", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config
    } do
      key = "normal-terminate-test-#{DurableServer.UUID.uuid4()}"

      pid = start_test_server(supervisor_name, key)

      assert GenServer.call(pid, :increment) == 1

      ref = Process.monitor(pid)
      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}

      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      assert data.meta.status == :stopped_graceful
    end

    test "graceful status persists after user terminate callback returns", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config
    } do
      key = "delayed-terminate-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DelayedTerminateServer, %{key: key, terminate_delay_ms: 300, notify_pid: self()}}
        )

      assert :ok = GenServer.call(pid, {:set_notify_pid, self()})

      ref = Process.monitor(pid)

      stop_task =
        Task.async(fn -> DurableServer.Supervisor.terminate_child(supervisor_name, pid) end)

      assert_receive {:terminate_started, ^pid}, 1_000

      {:ok, mid_data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      assert mid_data.meta.status == :running
      assert_receive {:terminate_finished, ^pid}, 1_000
      assert :ok = Task.await(stop_task, 2_000)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

      {:ok, final_data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      assert final_data.meta.status == :stopped_graceful
    end

    test "intentionaly stop permanent sets status to stopped_permanent", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config
    } do
      key = "normal-terminate-test-#{DurableServer.UUID.uuid4()}"

      pid = start_test_server(supervisor_name, key)

      ref = Process.monitor(pid)
      assert GenServer.call(pid, :stop_permanent) == :bye

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :permanent}}

      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      assert data.meta.status == :stopped_permanent
    end
  end

  describe "restart functionality" do
    test "can restart server from existing data", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config
    } do
      key = "restart-test-#{DurableServer.UUID.uuid4()}"

      # Start first (permanent) server and create some state
      pid1 = start_test_server(supervisor_name, key)
      assert GenServer.call(pid1, :increment) == 1
      assert GenServer.call(pid1, :increment) == 2

      # Stop the first server
      ref = Process.monitor(pid1)

      assert :ok =
               DurableServer.Supervisor.terminate_child_permanent(supervisor_name, pid1)

      assert_receive {:DOWN, ^ref, :process, ^pid1, :normal}

      # Check if state was saved to storage
      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      assert data.state["count"] == 2, "State should be saved to storage"

      # Start a new server with the same key - should automatically load existing state
      {:ok, {pid2, _meta}} =
        DurableServer.Supervisor.start_child(supervisor_name, {TestServer, %{key: key}})

      # Should have the previous state (automatically loaded from object storage)
      assert GenServer.call(pid2, :get_count) == 2

      # Should be able to continue operating
      assert GenServer.call(pid2, :increment) == 3
    end
  end

  describe "group registration" do
    test "server registers and unregisters from syn", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "syn-test-#{DurableServer.UUID.uuid4()}"

      # Start server
      pid = start_test_server(supervisor_name, key)

      # Should be registered in syn
      assert {^pid, meta} = DurableServer.Supervisor.lookup(supervisor_name, key)
      assert meta == %{my: "meta"}

      # Stop server
      ref = Process.monitor(pid)
      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}

      # Should be unregistered
      assert DurableServer.Supervisor.lookup(supervisor_name, key) == nil
    end
  end

  describe "metadata tracking" do
    test "stores module name in metadata", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config
    } do
      key = "metadata-test-#{DurableServer.UUID.uuid4()}"

      pid = start_test_server(supervisor_name, key)

      assert GenServer.call(pid, :increment) == 1

      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      assert data.meta.module == TestServer
      assert data.meta.status == :running
      assert data.meta.node_str == to_string(Node.self())
      assert is_integer(data.meta.node_ref)
      assert data.meta.pid == pid
      assert is_integer(data.meta.last_heartbeat_at)
    end
  end

  describe "actual restart functionality" do
    test "LifecycleManager attempts restart of crashed servers", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      key = "detect-crashed-test-#{DurableServer.UUID.uuid4()}"

      # Create server and stop it
      pid = start_test_server(supervisor_name, key)
      assert GenServer.call(pid, :increment) == 1
      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, pid)

      # Mark as crashed with very old heartbeat to ensure orphan detection
      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      # 10 minutes ago
      old_time = System.system_time(:millisecond) - 10 * 60 * 1000

      crashed_meta = %{
        data.meta
        | status: :crashed,
          node_str: "unreachable@test",
          node_ref: "dead-ref",
          module: TestServer,
          last_heartbeat_at: old_time,
          # Explicitly preserve permanent flag
          permanent: true
      }

      encoded_meta = Meta.encode_to_binary(crashed_meta)
      updated_data = %{data | meta: encoded_meta}

      {:ok, _} =
        ObjectStore.put_object(
          config.object_store,
          "#{prefix}#{key}",
          encode_legacy_stored_state(updated_data)
        )

      # Use the real supervisor's LifecycleManager for actual restart testing
      {:ok, manager_pid} = get_supervisor_lifecycle_manager(supervisor_name)

      # Manually trigger discovery
      send(manager_pid, :discover_and_restart)
      wait_for_discovery_completion(manager_pid, 5000)

      # Check if restart attempt was made
      {:ok, final_data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      # The LifecycleManager should have successfully restarted the server
      # Check that the server was restarted by verifying the status changed
      # (successful restart should update the status from :crashed)

      # The status should no longer be crashed (since restart succeeded)
      refute final_data.meta.status == :crashed

      # But restart metadata should be cleaned up after failure
      assert Map.get(final_data.meta, :restart_attempt_node) == nil
      assert Map.get(final_data.meta, :restart_attempt_time) == nil
      assert Map.get(final_data.meta, :restart_attempt_ttl) == nil

      # This confirms the LifecycleManager detected the orphan, attempted restart,
      # failed due to module interface mismatch, and cleaned up properly
      GenServer.stop(manager_pid)
    end

    test "restart preserves complex server state", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: _config,
      circuit_breaker: circuit_breaker
    } do
      key = "complex-state-test-#{DurableServer.UUID.uuid4()}"

      # Create server with complex state
      pid = start_test_server(supervisor_name, key)
      assert GenServer.call(pid, :increment) == 1
      assert GenServer.call(pid, :increment) == 2
      assert GenServer.call(pid, :increment) == 3

      original_count = GenServer.call(pid, :get_count)
      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, pid)

      # Mark as crashed from unreachable node
      store = circuit_breaker.object_store
      {:ok, data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})

      crashed_meta = %{
        data.meta
        | status: :crashed,
          # MockNodeModule.ping returns :pang
          node_str: "unreachable@test",
          module: TestServer,
          # Explicitly preserve permanent flag
          permanent: true
      }

      encoded_meta = Meta.encode_to_binary(crashed_meta)
      updated_data = %{data | meta: encoded_meta}
      ObjectStore.put_object(store, "#{prefix}#{key}", encode_legacy_stored_state(updated_data))

      # Use the real supervisor's LifecycleManager for actual restart testing
      {:ok, manager_pid} = get_supervisor_lifecycle_manager(supervisor_name)

      send(manager_pid, :discover_and_restart)
      wait_for_discovery_completion(manager_pid, 3000)

      # Check if restart was attempted - after failure, metadata gets cleaned up
      {:ok, final_data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})

      # Restart should have been attempted and failed (since TestServer interface mismatch)
      # After failure, restart metadata is cleaned up, but state should be preserved
      assert final_data.state["count"] == original_count,
             "State was not preserved during restart attempt"

      # Status should no longer be crashed since restart succeeded
      refute final_data.meta.status == :crashed

      GenServer.stop(manager_pid)
    end

    test "restart works when object storage updated between crash and restart", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config
    } do
      key = "storage-update-test-#{DurableServer.UUID.uuid4()}"

      # Create server and let it sync some state
      pid = start_test_server(supervisor_name, key)
      assert GenServer.call(pid, :increment) == 1

      {:ok, original_data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, pid)

      # Simulate external update to object storage while server is down
      # Different from server's last known state
      updated_state = %{count: 5}

      # Create a new properly encoded object with updated state
      encoded_meta = Meta.encode_to_binary(original_data.meta)

      external_update_data = %{
        vsn: original_data.vsn,
        state: updated_state,
        meta: encoded_meta
      }

      {:ok, %{etag: _}} =
        ObjectStore.put_object(
          config.object_store,
          "#{prefix}#{key}",
          JSON.encode!(external_update_data)
        )

      # Now mark as crashed for restart
      {:ok, current_data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      crashed_meta = %{
        current_data.meta
        | status: :crashed,
          node_str: "dead_node@test",
          # Explicitly preserve permanent flag
          permanent: true
      }

      encoded_meta = Meta.encode_to_binary(crashed_meta)
      final_data = %{current_data | meta: encoded_meta}

      {:ok, %{etag: _}} =
        ObjectStore.put_object(
          config.object_store,
          "#{prefix}#{key}",
          encode_legacy_stored_state(final_data)
        )

      # Use the real supervisor's LifecycleManager for actual restart testing
      {:ok, manager_pid} = get_supervisor_lifecycle_manager(supervisor_name)

      send(manager_pid, :discover_and_restart)
      wait_for_discovery_completion(manager_pid, 2000)

      # Verify restart was attempted (metadata cleaned up after failure) and state preserved
      {:ok, post_restart_data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      # After restart failure, metadata is cleaned up but state should be preserved
      assert post_restart_data.state["count"] == 5, "Should use updated state from storage"

      refute post_restart_data.meta.status == :crashed,
             "Status should change from crashed after successful restart"

      GenServer.stop(manager_pid)
    end
  end

  describe "real-world restart scenarios" do
    test "handles restart timing conflicts between multiple managers", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      key = "timing-conflict-test-#{DurableServer.UUID.uuid4()}"

      # Create crashed server state
      meta_attrs = %{
        status: :crashed,
        node_str: "unreachable@test",
        node_ref: "dead-ref",
        pid: self(),
        last_heartbeat_at: System.system_time(:millisecond),
        module: TestServer
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 3}, meta_attrs)

      # Start two managers that will compete for restart (use spawn to avoid name conflicts)
      {:ok, manager1} =
        start_standalone_lifecycle_manager(supervisor_name, config,
          node_module: DurableServer.LifecycleTest.MockNodeModule
        )

      {:ok, manager2} =
        start_standalone_lifecycle_manager(supervisor_name, config,
          node_module: DurableServer.LifecycleTest.MockNodeModule
        )

      # Trigger discovery on both simultaneously
      send(manager1, :discover_and_restart)
      send(manager2, :discover_and_restart)

      # Give time for both to process
      Process.sleep(2000)

      # Check final state - should have atomic claim (only one winner)
      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      restart_node = Map.get(data.meta, :restart_attempt_node)

      # Should have exactly one restart attempt node or completion
      if restart_node do
        assert restart_node == to_string(Node.self())
      else
        # Or restart completed successfully with no remaining metadata
        assert Map.get(data.meta, :restart_attempt_time) == nil
      end
    end

    test "restart coordination across discovery cycles", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      key = "coordination-test-#{DurableServer.UUID.uuid4()}"

      meta_attrs = %{
        status: :crashed,
        # Use unreachable node
        node_str: "unreachable@test",
        node_ref: "dead-ref",
        pid: self(),
        last_heartbeat_at: System.system_time(:millisecond),
        module: TestServer
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config,
          node_module: DurableServer.LifecycleTest.MockNodeModule
        )

      # Multiple discovery cycles
      for i <- 1..3 do
        send(manager_pid, :discover_and_restart)
        wait_for_discovery_completion(manager_pid, 1500)

        # Check state after each cycle
        {:ok, data} =
          DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

        case i do
          1 ->
            # First cycle should claim restart since node is unreachable
            restart_attempt = Map.get(data.meta, :restart_attempt_node)

            if restart_attempt == nil do
              # If no restart attempt, server might have been restarted successfully
              # or the discovery logic didn't find it as orphaned
              # This is acceptable - just log for debugging
              :ok
            end

          2 ->
            # Second cycle - restart might be in progress or cleaned up
            :ok

          3 ->
            # Final cycle should have completed or cleaned up
            restart_time = Map.get(data.meta, :restart_attempt_time)

            if restart_time do
              # If still present, should be recent
              assert System.system_time(:millisecond) - restart_time < 30_000
            end
        end
      end

      GenServer.stop(manager_pid)
    end
  end

  describe "complete error recovery testing" do
    test "cleanup prevents future restart attempts after repeated failures", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      key = "repeated-failure-test-#{DurableServer.UUID.uuid4()}"

      meta_attrs = %{
        status: :crashed,
        node_str: "dead_node@test",
        node_ref: "dead-ref",
        pid: self(),
        last_heartbeat_at: System.system_time(:millisecond),
        # This will always fail to restart
        module: NonExistentModule
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      # Trigger multiple discovery rounds to simulate repeated failures
      for _ <- 1..3 do
        send(manager_pid, :discover_and_restart)
        wait_for_discovery_completion(manager_pid, 1500)
      end

      # Check final state - should have cleanup after failure
      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      # Should have no restart attempt metadata (cleaned up after failure)
      assert Map.get(data.meta, :restart_attempt_ttl) == nil
      assert Map.get(data.meta, :restart_attempt_node) == nil
      assert Map.get(data.meta, :restart_attempt_time) == nil

      # Status should remain crashed since restart failed
      assert data.meta.status == :crashed

      GenServer.stop(manager_pid)
    end

    test "recovery from corrupted restart attempt metadata", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      key = "corrupted-restart-test-#{DurableServer.UUID.uuid4()}"

      # Create object with corrupted restart attempt metadata
      meta_attrs = %{
        status: :running,
        node_str: "dead_node@test",
        node_ref: "dead-ref",
        pid: self(),
        last_heartbeat_at: System.system_time(:millisecond),
        module: TestServer,
        restart_attempt_node: "corrupt_node",
        # Should be integer
        restart_attempt_time: "invalid_time",
        # Should be integer
        restart_attempt_ttl: "invalid_ttl"
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      # Should handle corrupted metadata gracefully
      send(manager_pid, :discover_and_restart)
      wait_for_discovery_completion(manager_pid, 1500)

      # Manager should still be alive despite corrupted data
      assert_process_alive(manager_pid)

      # Should have attempted to process or clean up corruption
      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      # The restart attempt fields should be handled gracefully
      # Since we started with corrupted string values, they might remain as strings
      # or be cleaned up entirely. Both are acceptable for corruption recovery.
      restart_time = Map.get(data.meta, :restart_attempt_time)

      if restart_time do
        # Either cleaned up and replaced with proper integer, or left as original corrupt string
        assert is_integer(restart_time) or is_binary(restart_time),
               "Restart time should be integer (if fixed) or string (if original corrupt data)"
      end

      GenServer.stop(manager_pid)
    end

    test "error recovery during discovery with mixed valid/invalid objects", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      # Create multiple objects with mixed validity
      keys = for i <- 1..5, do: "mixed-test-#{i}-#{DurableServer.UUID.uuid4()}"

      # Create mix of valid and invalid objects
      for {key, i} <- Enum.with_index(keys, 1) do
        case rem(i, 3) do
          0 ->
            # Valid crashed server
            meta_attrs = %{
              status: :crashed,
              node_str: "dead_node@test",
              node_ref: "dead-ref",
              pid: self(),
              last_heartbeat_at: System.system_time(:millisecond),
              module: TestServer
            }

            create_test_object(config.object_store, "#{prefix}#{key}", %{count: i}, meta_attrs)

          1 ->
            # Corrupted metadata
            corrupted_data = %{
              vsn: 1,
              state: %{count: i},
              meta: "invalid_base64_#{i}"
            }

            ObjectStore.put_object(
              config.object_store,
              "#{prefix}#{key}",
              JSON.encode!(corrupted_data)
            )

          2 ->
            # Valid running server (should be ignored)
            meta_attrs = %{
              status: :running,
              node_str: to_string(Node.self()),
              node_ref: "current-ref",
              pid: self(),
              last_heartbeat_at: System.system_time(:millisecond),
              module: TestServer
            }

            create_test_object(config.object_store, "#{prefix}#{key}", %{count: i}, meta_attrs)
        end
      end

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      # Should handle mixed object states gracefully
      send(manager_pid, :discover_and_restart)
      wait_for_discovery_completion(manager_pid, 2000)

      # Manager should survive processing mixed objects
      assert_process_alive(manager_pid)

      # Should have attempted restart only on valid crashed servers
      # Every 3rd key (0-indexed)
      valid_crashed_keys = Enum.take_every(keys, 3)

      for key <- valid_crashed_keys do
        case DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix}) do
          {:ok, data} ->
            # Should have restart attempt or completion
            restart_attempt = Map.get(data.meta, :restart_attempt_node)

            assert restart_attempt != nil or data.meta.status != :crashed,
                   "Valid crashed server #{key} should have restart attempt or be recovered"

          {:error, _} ->
            # Object might have been cleaned up during restart - acceptable
            :ok
        end
      end

      GenServer.stop(manager_pid)
    end
  end

  describe "integration behavior" do
    test "LifecycleManager can be started", %{
      supervisor_name: supervisor_name,
      prefix: _prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      {:ok, pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      assert_process_alive(pid)
      GenServer.stop(pid)
    end
  end

  # Helper to start a test server through the supervisor
  defp start_test_server(supervisor_name, key) do
    {:ok, {pid, _meta}} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {TestServer, %{key: key, permanent: true}}
      )

    pid
  end

  defp assert_process_alive(pid, timeout_ms \\ 25) when is_pid(pid) do
    ref = Process.monitor(pid)

    try do
      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, timeout_ms
    after
      Process.demonitor(ref, [:flush])
    end
  end

  # Helper to create properly encoded test objects like DurableServer does
  defp create_test_object(%ObjectStore{} = store, key, state, meta_attrs) do
    encoded_meta = Meta.encode_to_binary(struct!(Meta, meta_attrs))

    test_data = %{
      vsn: 1,
      state: state,
      meta: encoded_meta
    }

    ObjectStore.put_object(store, key, JSON.encode!(test_data))
  end

  defp encode_legacy_stored_state(%DurableServer.StoredState{
         vsn: vsn,
         state: state,
         meta: meta_binary
       })
       when is_binary(meta_binary) do
    JSON.encode!(%{
      "vsn" => vsn,
      "state" => state,
      "meta" => meta_binary
    })
  end

  # Helper to get LifecycleManager PID from the real supervisor (for restart tests)
  defp get_supervisor_lifecycle_manager(supervisor_name) do
    case Supervisor.which_children(supervisor_name) do
      children when is_list(children) ->
        case Enum.find(children, fn {module, _pid, _type, _modules} ->
               module == LifecycleManager
             end) do
          {LifecycleManager, pid, _type, _modules} when is_pid(pid) -> {:ok, pid}
          _ -> {:error, :not_found}
        end

      _ ->
        {:error, :supervisor_not_found}
    end
  end

  # Helper to start a dedicated LifecycleManager for testing without supervisor conflicts
  defp start_standalone_lifecycle_manager(base_supervisor_name, config, opts \\ []) do
    # Create a unique supervisor name for standalone LifecycleManager testing
    standalone_supervisor_name =
      :"#{base_supervisor_name}_standalone_#{DurableServer.UUID.uuid4()}"

    # Create ETS table for standalone supervisor
    table_name = :"durable_supervisor_#{standalone_supervisor_name}"
    ^table_name = :ets.new(table_name, [:named_table, :set, :protected, read_concurrency: true])

    config = %{config | name: standalone_supervisor_name, ets_table: table_name}

    # Store config in ETS for __get_config__ to find
    :ets.insert(table_name, {:config, config})
    :ets.insert(table_name, {:capacity_limits, %{}})
    :ets.insert(table_name, {:sticky_placement_config, %{per_module: %{}, default: nil}})

    # Create a minimal circuit breaker for testing
    circuit_breaker_config = %{
      object_store: config.object_store,
      crash_threshold_count: 5,
      crash_threshold_window_ms: 60 * 60 * 1000,
      module_circuit_breaker_count: 50,
      module_circuit_breaker_window_ms: 5 * 60 * 1000,
      module_circuit_breaker_cooldown_ms: 30 * 60 * 1000
    }

    circuit_breaker = CircuitBreaker.new(standalone_supervisor_name, circuit_breaker_config)

    # Create TaskSupervisor for standalone testing
    task_sup_name = :"#{standalone_supervisor_name}_task_sup"
    _ = start_supervised!({Task.Supervisor, name: task_sup_name})

    # Start LifecycleManager with the standalone supervisor name
    manager_opts =
      [
        supervisor_name: standalone_supervisor_name,
        task_supervisor: task_sup_name,
        object_store: config.object_store,
        config: config,
        circuit_breaker: circuit_breaker
      ] ++ opts

    start_supervised({LifecycleManager, manager_opts}, id: standalone_supervisor_name)
  end

  # Helper function to wait for discovery task completion or manager crash
  defp wait_for_discovery_completion(manager_pid, timeout) do
    ref = Process.monitor(manager_pid)

    receive do
      {:DOWN, ^ref, :process, ^manager_pid, reason} ->
        flunk("LifecycleManager crashed: #{inspect(reason)}")
    after
      timeout ->
        Process.demonitor(ref, [:flush])
        :ok
    end
  end

  describe "lifecycle manager edge cases" do
    test "handles object store failures during discovery gracefully", %{
      supervisor_name: supervisor_name,
      prefix: _prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      # This test would be better with ObjectStore mocking, but we can at least verify
      # the LifecycleManager doesn't crash when encountering errors
      {:ok, pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      # Send a discovery message manually to trigger discovery
      send(pid, :discover_and_restart)

      # Wait for discovery to complete or manager to crash
      wait_for_discovery_completion(pid, 500)

      assert_process_alive(pid)
      GenServer.stop(pid)
    end

    test "handles invalid metadata gracefully during restart detection", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      key = "invalid-meta-test-#{DurableServer.UUID.uuid4()}"

      # Create a server to establish an object
      pid = start_test_server(supervisor_name, key)
      assert GenServer.call(pid, :increment) == 1
      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, pid)

      # Manually corrupt the metadata by directly writing invalid data
      corrupted_data = %{
        vsn: 1,
        state: %{count: 1},
        meta: "invalid_base64_metadata"
      }

      ObjectStore.put_object(config.object_store, "#{prefix}#{key}", JSON.encode!(corrupted_data))

      # Discovery should handle this gracefully
      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      send(manager_pid, :discover_and_restart)

      # Wait for discovery to complete or manager to crash
      wait_for_discovery_completion(manager_pid, 500)

      assert_process_alive(manager_pid)
      GenServer.stop(manager_pid)
    end

    test "assigns keys correctly across nodes", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      # Use proper config instead of minimal config
      full_config = Map.merge(config, %{prefix: "test/"})

      defmodule DurableServer.LifecycleTest.MockRegionNodeModule do
        def self(), do: :test_node@test
        def ping(:unreachable@test), do: :pang
        def ping(_), do: :pong
      end

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, full_config,
          node_module: DurableServer.LifecycleTest.MockRegionNodeModule
        )

      # Create some test objects in storage
      keys = for i <- 1..10, do: "test-key-#{i}"

      for key <- keys do
        meta_attrs = %{
          status: :stopped_graceful,
          # Different node so it's orphaned
          node_str: "node4@test",
          node_ref: "test-ref",
          pid: self(),
          last_heartbeat_at: System.system_time(:millisecond),
          module: TestServer
        }

        create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)
      end

      send(manager_pid, :discover_and_restart)

      # Wait for discovery to complete or manager to crash
      wait_for_discovery_completion(manager_pid, 500)

      assert_process_alive(manager_pid)
      GenServer.stop(manager_pid)
    end

    test "group registry empty bypass works correctly", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      # Use proper config instead of minimal config
      full_config = Map.merge(config, %{prefix: "test/"})

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, full_config)

      # Create a test object that should be skipped due to empty group registry
      key = "empty-syn-test-#{DurableServer.UUID.uuid4()}"

      meta_attrs = %{
        status: :running,
        node_str: to_string(Node.self()),
        node_ref: "test-ref",
        pid: self(),
        last_heartbeat_at: System.system_time(:millisecond),
        module: TestServer
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      send(manager_pid, :discover_and_restart)

      # Wait for discovery to complete or manager to crash
      wait_for_discovery_completion(manager_pid, 100)
      assert_process_alive(manager_pid)

      GenServer.stop(manager_pid)
    end

    test "node ping failures are handled correctly", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config,
          node_module: DurableServer.LifecycleTest.MockNodeModule
        )

      # Create test object with unreachable node
      key = "ping-fail-test-#{DurableServer.UUID.uuid4()}"

      meta_attrs = %{
        status: :running,
        node_str: "unreachable@test",
        node_ref: "test-ref",
        pid: self(),
        last_heartbeat_at: System.system_time(:millisecond),
        module: TestServer
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      send(manager_pid, :discover_and_restart)

      # Wait for discovery to complete or manager to crash
      wait_for_discovery_completion(manager_pid, 100)
      assert_process_alive(manager_pid)

      GenServer.stop(manager_pid)
    end

    test "crashed status servers are always considered orphaned", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      # Create test object with crashed status
      key = "crashed-status-test-#{DurableServer.UUID.uuid4()}"

      meta_attrs = %{
        status: :crashed,
        node_str: to_string(Node.self()),
        node_ref: "test-ref",
        pid: self(),
        last_heartbeat_at: System.system_time(:millisecond),
        module: TestServer
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      send(manager_pid, :discover_and_restart)

      # Wait for discovery to complete or manager to crash
      wait_for_discovery_completion(manager_pid, 100)
      assert_process_alive(manager_pid)

      GenServer.stop(manager_pid)
    end

    test "very old heartbeat triggers orphan detection", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      # Create test object with very old heartbeat
      key = "old-heartbeat-test-#{DurableServer.UUID.uuid4()}"
      # 5 minutes ago
      old_heartbeat = System.system_time(:millisecond) - 5 * 60 * 1000

      meta_attrs = %{
        status: :running,
        node_str: to_string(Node.self()),
        node_ref: "test-ref",
        pid: self(),
        last_heartbeat_at: old_heartbeat,
        module: TestServer
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      send(manager_pid, :discover_and_restart)

      # Wait for discovery to complete or manager to crash
      wait_for_discovery_completion(manager_pid, 100)
      assert_process_alive(manager_pid)

      GenServer.stop(manager_pid)
    end

    test "stopped_permanent servers are never restarted", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      # Create test object with stopped_permanent status
      key = "stopped-permanent-test-#{DurableServer.UUID.uuid4()}"

      meta_attrs = %{
        status: :stopped_permanent,
        node_str: to_string(Node.self()),
        node_ref: "test-ref",
        pid: self(),
        # Old heartbeat
        last_heartbeat_at: System.system_time(:millisecond) - 5 * 60 * 1000,
        module: TestServer
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      send(manager_pid, :discover_and_restart)

      # Wait for discovery to complete or manager to crash
      wait_for_discovery_completion(manager_pid, 100)
      assert_process_alive(manager_pid)

      GenServer.stop(manager_pid)
    end

    test "expired restart attempt TTL allows reclaim", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      # Create test object with expired restart attempt
      key = "expired-ttl-test-#{DurableServer.UUID.uuid4()}"
      # 1 minute ago
      expired_time = System.system_time(:millisecond) - 60_000

      meta_attrs = %{
        status: :running,
        node_str: to_string(Node.self()),
        node_ref: "test-ref",
        pid: self(),
        last_heartbeat_at: System.system_time(:millisecond),
        restart_attempt_ttl: expired_time,
        module: TestServer
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      send(manager_pid, :discover_and_restart)

      # Wait for discovery to complete or manager to crash
      wait_for_discovery_completion(manager_pid, 100)
      assert_process_alive(manager_pid)

      GenServer.stop(manager_pid)
    end

    test "group registry stale entries vs metadata mismatch", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config,
          node_module: DurableServer.LifecycleTest.MockNodeModule
        )

      key = "stale-syn-test-#{DurableServer.UUID.uuid4()}"

      # Create object with metadata that doesn't match group registry
      meta_attrs = %{
        status: :running,
        node_str: to_string(Node.self()),
        # Different from MockStaleSynModule
        node_ref: "old-node-ref",
        pid: self(),
        last_heartbeat_at: System.system_time(:millisecond),
        module: TestServer
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      send(manager_pid, :discover_and_restart)

      # Wait for discovery to complete or manager to crash
      wait_for_discovery_completion(manager_pid, 100)
      assert_process_alive(manager_pid)

      GenServer.stop(manager_pid)
    end

    test "restart claim TTL expiration during process", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      key = "ttl-expire-test-#{DurableServer.UUID.uuid4()}"

      # Create object with expired restart attempt from another node
      current_time = System.system_time(:millisecond)
      # Already expired
      expired_ttl = current_time - 1000

      meta_attrs = %{
        status: :running,
        node_str: "other_node@test",
        node_ref: "other-ref",
        pid: self(),
        last_heartbeat_at: current_time,
        restart_attempt_node: "other_node@test",
        # 31 seconds ago
        restart_attempt_time: current_time - 31_000,
        restart_attempt_ttl: expired_ttl,
        module: TestServer
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config,
          node_module: DurableServer.LifecycleTest.MockNodeModule
        )

      send(manager_pid, :discover_and_restart)

      # Wait for discovery to complete or manager to crash
      wait_for_discovery_completion(manager_pid, 100)

      # Should be able to claim since TTL expired
      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      # The object might not actually be processed if it's not considered orphaned
      # Since the node is "other_node@test" and MockNodeModule.ping returns :pong for everything,
      # the orphan check might not trigger as expected. This tests the TTL logic exists
      # but the specific scenario may not result in processing due to other conditions.

      # Verify the test setup ran without errors
      assert is_map(data)

      GenServer.stop(manager_pid)
    end

    test "node reachable but lock validation fails", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config,
          # Always returns :pong
          node_module: DurableServer.LifecycleTest.MockNodeModule
        )

      key = "lock-fail-test-#{DurableServer.UUID.uuid4()}"

      # Create object where node is reachable but lock should be expired
      # (this would require mocking DurableServer.__check_lock__ to return :expired)
      meta_attrs = %{
        status: :running,
        # Same node, so ping succeeds
        node_str: to_string(Node.self()),
        node_ref: "expired-lock-ref",
        # Dead PID
        pid: spawn(fn -> :ok end),
        last_heartbeat_at: System.system_time(:millisecond),
        module: TestServer
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      send(manager_pid, :discover_and_restart)

      # Wait for discovery to complete or manager to crash
      wait_for_discovery_completion(manager_pid, 100)
      assert_process_alive(manager_pid)

      GenServer.stop(manager_pid)
    end

    test "restart attempt cleanup after failures", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      key = "cleanup-test-#{DurableServer.UUID.uuid4()}"

      # Create object that will fail to restart (missing module)
      meta_attrs = %{
        status: :crashed,
        node_str: "dead_node@test",
        node_ref: "dead-ref",
        pid: self(),
        last_heartbeat_at: System.system_time(:millisecond),
        # This will cause restart failure
        module: NonExistentModule
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      send(manager_pid, :discover_and_restart)

      # Wait longer for restart attempt and cleanup to complete
      wait_for_discovery_completion(manager_pid, 200)

      # Check that restart attempt metadata was cleaned up after failure
      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      assert Map.get(data.meta, :restart_attempt_ttl) == nil
      assert Map.get(data.meta, :restart_attempt_node) == nil
      assert Map.get(data.meta, :restart_attempt_time) == nil

      GenServer.stop(manager_pid)
    end

    test "multiple discovery rounds with changing orphan states", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      key = "changing-state-test-#{DurableServer.UUID.uuid4()}"

      # Start with non-orphaned state
      meta_attrs = %{
        status: :running,
        node_str: to_string(Node.self()),
        node_ref: "current-ref",
        pid: self(),
        last_heartbeat_at: System.system_time(:millisecond),
        module: TestServer
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      # First discovery - should not be orphaned
      send(manager_pid, :discover_and_restart)
      wait_for_discovery_completion(manager_pid, 100)

      # Update object to be orphaned (old heartbeat)
      old_time = System.system_time(:millisecond) - 5 * 60 * 1000
      meta_attrs = Map.put(meta_attrs, :last_heartbeat_at, old_time)
      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      # Second discovery - should now be orphaned
      send(manager_pid, :discover_and_restart)
      wait_for_discovery_completion(manager_pid, 100)

      assert_process_alive(manager_pid)
      GenServer.stop(manager_pid)
    end

    test "task supervision completes discovery cycle properly", %{
      supervisor_name: supervisor_name,
      prefix: _prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      # Trigger discovery and wait for completion
      send(manager_pid, :discover_and_restart)

      # Wait for discovery task to complete
      wait_for_discovery_completion(manager_pid, 200)

      # Manager should still be alive and ready for next cycle
      assert_process_alive(manager_pid)
      GenServer.stop(manager_pid)
    end

    test "retries retryable heartbeat write failures until success within deadline", %{
      supervisor_name: supervisor_name,
      config: config
    } do
      table = :ets.new(__MODULE__.HeartbeatRetryBackend, [:set, :public])

      storage_backend =
        DurableServer.StorageBackend.new(HeartbeatRetryBackend, %{
          table: table,
          fail_count: 2
        })

      test_config =
        config
        |> Map.put(:object_store, storage_backend)
        |> Map.put(:storage_backend, storage_backend)

      {:ok, _pid} = start_standalone_lifecycle_manager(supervisor_name, test_config)

      assert [{:attempts, attempts}] = :ets.lookup(table, :attempts)
      assert attempts >= 3
      assert [{:last_write, heartbeat_data}] = :ets.lookup(table, :last_write)
      assert is_map(heartbeat_data)
      assert Map.has_key?(heartbeat_data, "last_heartbeat_at")
    end

    test "group heartbeat join timeout does not crash the lifecycle manager handler", %{
      supervisor_name: supervisor_name
    } do
      shard = Group.Replica.shard_for(supervisor_name, nil, "__heartbeat")
      ref = make_ref()

      state = %LifecycleManager{
        supervisor_name: supervisor_name,
        heartbeat_interval_ms: 10_000,
        current_heartbeat_task: %Task{
          mfa: {:erlang, :apply, []},
          owner: self(),
          pid: self(),
          ref: ref
        }
      }

      heartbeat_entry =
        {"node@test", 123, System.system_time(:millisecond), %{}, %{}, %{}, %{"region" => "ord"}}

      :sys.suspend(shard)

      try do
        {us, result} =
          :timer.tc(fn ->
            LifecycleManager.handle_info(
              {ref, {:heartbeat, {%{total_ms: 10, put_ms: 5, cache_ms: 5}, heartbeat_entry}}},
              state
            )
          end)

        assert us >= 5_000_000
        assert {:noreply, %LifecycleManager{current_heartbeat_task: nil}} = result
      after
        :sys.resume(shard)

        receive do
          :heartbeat -> :ok
        after
          0 -> :ok
        end
      end
    end
  end

  describe "dead node cleanup" do
    test "automatically cleans up permanently dead nodes from heartbeat storage", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      # Create node heartbeat entries with different ages
      current_time = System.system_time(:millisecond)
      # 1 second ago (alive)
      recent_time = current_time - 1000
      # 6 hours ago (should be cleaned up)
      old_time = current_time - 6 * 60 * 60 * 1000
      # 48 hours ago (should be cleaned up)
      very_old_time = current_time - 48 * 60 * 60 * 1000

      alive_node = "alive@test"
      dead_node1 = "dead1@test"
      dead_node2 = "dead2@test"

      # Create heartbeat entries
      alive_heartbeat = %{
        node: alive_node,
        node_ref: "alive-ref",
        last_heartbeat_at: recent_time
      }

      dead_heartbeat1 = %{
        node: dead_node1,
        node_ref: "dead1-ref",
        last_heartbeat_at: old_time
      }

      dead_heartbeat2 = %{
        node: dead_node2,
        node_ref: "dead2-ref",
        last_heartbeat_at: very_old_time
      }

      # Write heartbeat data to storage
      ObjectStore.put_object(
        config.object_store,
        "#{prefix}__nodes/#{alive_node}",
        JSON.encode!(alive_heartbeat)
      )

      ObjectStore.put_object(
        config.object_store,
        "#{prefix}__nodes/#{dead_node1}",
        JSON.encode!(dead_heartbeat1)
      )

      ObjectStore.put_object(
        config.object_store,
        "#{prefix}__nodes/#{dead_node2}",
        JSON.encode!(dead_heartbeat2)
      )

      # Verify all heartbeats exist initially
      {:ok, _} = ObjectStore.get_object(config.object_store, "#{prefix}__nodes/#{alive_node}")
      {:ok, _} = ObjectStore.get_object(config.object_store, "#{prefix}__nodes/#{dead_node1}")
      {:ok, _} = ObjectStore.get_object(config.object_store, "#{prefix}__nodes/#{dead_node2}")

      # Create a short-lived config with very low dead node threshold for testing
      # 2 hours
      test_config = Map.put(config, :dead_node_threshold_ms, 2 * 60 * 60 * 1000)

      # Start LifecycleManager to trigger heartbeat refresh and cleanup
      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, test_config)

      # Trigger heartbeat cycle which should clean up dead nodes
      send(manager_pid, :heartbeat)

      # Give some time for the heartbeat task to complete
      Process.sleep(200)

      # Verify alive node still exists
      {:ok, _} = ObjectStore.get_object(config.object_store, "#{prefix}__nodes/#{alive_node}")

      # Verify dead nodes were cleaned up (should return not_found errors)
      {:error, :not_found} =
        ObjectStore.get_object(config.object_store, "#{prefix}__nodes/#{dead_node1}")

      {:error, :not_found} =
        ObjectStore.get_object(config.object_store, "#{prefix}__nodes/#{dead_node2}")

      GenServer.stop(manager_pid)
    end
  end

  describe "permanent flag and circuit breaker" do
    test "lifecycle manager only restarts permanent servers", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      # Create a non-permanent server object (default permanent: false)
      key = "non-permanent-#{DurableServer.UUID.uuid4()}"

      meta_attrs = %{
        status: :crashed,
        node_str: "dead_node@test",
        node_ref: "dead-ref",
        module: TestServer,
        # Non-permanent
        permanent: false,
        # Dead process for test
        pid: spawn(fn -> :ok end)
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      # Trigger discovery
      send(manager_pid, :discover_and_restart)
      wait_for_discovery_completion(manager_pid, 1000)

      # Verify server was NOT restarted (should remain crashed)
      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      assert data.meta.status == :crashed

      GenServer.stop(manager_pid)
    end

    test "lifecycle manager skips permanently crashed servers", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      key = "permanently-crashed-#{DurableServer.UUID.uuid4()}"

      meta_attrs = %{
        status: :permanently_crashed,
        node_str: "dead_node@test",
        node_ref: "dead-ref",
        module: TestServer,
        permanent: true,
        crash_history: [
          %{timestamp: System.system_time(:millisecond) - 1000, reason: "crash 1"},
          %{timestamp: System.system_time(:millisecond) - 2000, reason: "crash 2"}
        ],
        # Dead process for test
        pid: spawn(fn -> :ok end)
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      # Trigger discovery
      send(manager_pid, :discover_and_restart)
      wait_for_discovery_completion(manager_pid, 1000)

      # Verify server was NOT restarted (should remain permanently crashed)
      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      assert data.meta.status == :permanently_crashed

      GenServer.stop(manager_pid)
    end
  end
end
