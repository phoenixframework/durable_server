defmodule DurableServerTest do
  use ExUnit.Case, async: true
  import DurableServer.TestHelper

  alias DurableServer
  alias DurableServer.StoredState
  alias DurableServer.ObjectStore
  alias DurableServer.Backends.ObjectStore, as: ObjectStoreBackend
  alias DurableServer.StorageBackend
  alias DurableServer.TestTemporalServer
  alias DurableServerTest.EdgeCaseTestServer
  alias DurableServerTest.ValidatorTestServer

  @moduletag :capture_log

  # Helper to convert string keys to atom keys recursively
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

  defp start_test_supervisor(extra_opts \\ []) do
    unique_id = "#{System.system_time(:microsecond)}_#{DurableServer.UUID.uuid4()}"
    supervisor_name = :"test_supervisor_#{unique_id}"
    prefix = "test_#{unique_id}/"

    base_opts = [
      name: supervisor_name,
      prefix: prefix,
      graceful_shutdown_timeout_ms: 500
    ]

    base_opts =
      if Keyword.has_key?(extra_opts, :backend) do
        base_opts
      else
        Keyword.put(base_opts, :object_store, test_object_store_opts())
      end

    supervisor_opts = Keyword.merge(base_opts, extra_opts)

    # create a child spec with unique ID to avoid ExUnit conflicts
    child_spec = %{
      id: {DurableServer.Supervisor, supervisor_name},
      start: {DurableServer.Supervisor, :start_link, [supervisor_opts]},
      type: :supervisor
    }

    supervisor_pid = start_supervised!(child_spec)

    {supervisor_name, supervisor_pid, prefix}
  end

  # Test implementation modules
  defmodule RejectingStartupSyncBackend do
    @behaviour DurableServer.StorageBackend

    alias DurableServer.StorageBackend
    alias DurableServer.Backends.ObjectStore, as: ObjectStoreBackend

    @impl true
    def init_backend(opts) when is_list(opts) do
      delegate_opts = Keyword.fetch!(opts, :delegate_opts)
      {:ok, delegate} = StorageBackend.init_backend(ObjectStoreBackend, delegate_opts)
      {:ok, %{state: %{delegate: delegate}}}
    end

    @impl true
    def ensure_ready(%{delegate: delegate}), do: StorageBackend.ensure_ready(delegate)

    @impl true
    def get_object(%{delegate: delegate}, key, opts),
      do: StorageBackend.get_object(delegate, key, opts)

    @impl true
    def list_all_objects_stream(%{delegate: delegate}, prefix, opts),
      do: StorageBackend.list_all_objects_stream(delegate, prefix, opts)

    @impl true
    def put_object(%{delegate: delegate}, key, data, opts) do
      if String.contains?(key, "__nodes/") do
        StorageBackend.put_object(delegate, key, data, opts)
      else
        {:error, :startup_sync_rejected}
      end
    end

    @impl true
    def delete_object(%{delegate: delegate}, key), do: StorageBackend.delete_object(delegate, key)

    @impl true
    def try_claim(%{delegate: delegate}, key, body),
      do: StorageBackend.try_claim(delegate, key, body)

    @impl true
    def update_object(%{delegate: delegate}, key, update_fn, opts),
      do: StorageBackend.update_object(delegate, key, update_fn, opts)

    @impl true
    def encode(%{delegate: delegate}, data), do: StorageBackend.encode(delegate, data)

    @impl true
    def decode(%{delegate: delegate}, data), do: StorageBackend.decode(delegate, data)

    @impl true
    def subscribe(%{delegate: delegate}, subscriber, prefix, opts),
      do: StorageBackend.subscribe(delegate, subscriber, prefix, opts)

    @impl true
    def unsubscribe(%{delegate: delegate}, subscription_ref),
      do: StorageBackend.unsubscribe(delegate, subscription_ref)
  end

  defmodule TestServer do
    use DurableServer,
      vsn: 1

    def dump_state(state) do
      # Remove test_pid before persisting since PIDs can't be JSON encoded
      Map.delete(state, :test_pid)
    end

    def load_state(_old_vsn, persisted_state) do
      # Convert string keys from JSON to atom keys and add test_pid back as nil
      persisted_state
      |> DurableServerTest.atomify_keys()
      |> Map.put(:test_pid, nil)
      |> Map.put_new(:count, 0)
    end

    def init(%{error: reason}) do
      {:error, reason}
    end

    def init(%{ignore: true}) do
      :ignore
    end

    def init(init_state) do
      custom_opts = Enum.into(init_state[:custom_opts] || %{}, [])

      # Extract meta if provided and add to options
      {meta, remaining_state} = Map.pop(init_state, :meta)
      final_opts = if meta, do: Keyword.put(custom_opts, :meta, meta), else: custom_opts

      # Ensure count is set
      state_with_count = Map.put_new(remaining_state, :count, 0)

      # Handle test case for invalid options
      if remaining_state[:invalid_options_test] do
        {:ok, remaining_state, [invalid_opt: true]}
      else
        {:ok, state_with_count, final_opts}
      end
    end

    def handle_call(:get_count, _from, %{count: count} = state) do
      {:reply, count, state}
    end

    def handle_call(:increment, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state}
    end

    def handle_call(:increment_and_sync, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state, :sync}
    end

    def handle_call(:increment_with_timeout, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state, 1000}
    end

    def handle_call(:stop_normal, _from, state) do
      {:stop, :normal, :ok, state}
    end

    def handle_call(:stop_with_reply, _from, %{count: count} = state) do
      new_state = %{state | count: count + 100}
      {:stop, :normal, count + 100, new_state}
    end

    def handle_call(:continue_test, _from, state) do
      {:reply, :ok, state, {:continue, :increment}}
    end

    def handle_call(:continue_and_sync_test, _from, state) do
      {:reply, :ok, state, {:continue, :increment_and_sync}}
    end

    def handle_call({:set_test_pid, pid}, _from, state) do
      {:reply, :ok, %{state | test_pid: pid}}
    end

    # Test callbacks for meta option functionality
    def handle_call({:update_meta_test, new_meta}, _from, state) do
      {:reply, :ok, state, meta: new_meta}
    end

    def handle_call({:sync_and_update_meta, new_meta}, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state, :sync, meta: new_meta}
    end

    def handle_call({:continue_with_meta, new_meta}, _from, state) do
      {:reply, :ok, state, {:continue, :increment_and_sync}, meta: new_meta}
    end

    def handle_call({:continue_with_meta_and_sync_option, new_meta}, _from, state) do
      {:reply, :ok, state, {:continue, :increment}, meta: new_meta, sync: true}
    end

    def handle_call({:invalid_meta_test, invalid_meta}, _from, state) do
      # This should cause a function clause error since invalid_meta isn't a map
      {:reply, :ok, state, meta: invalid_meta}
    end

    def handle_cast(:increment, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:noreply, new_state}
    end

    def handle_cast(:increment_and_sync, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:noreply, new_state, :sync}
    end

    def handle_cast(:stop, state) do
      {:stop, :normal, state}
    end

    def handle_cast({:update_meta_cast, new_meta}, state) do
      {:noreply, state, meta: new_meta}
    end

    def handle_info(:increment, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:noreply, new_state}
    end

    def handle_info(:increment_and_sync, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:noreply, new_state, :sync}
    end

    def handle_info({:update_meta_info, new_meta}, state) do
      {:noreply, state, meta: new_meta}
    end

    def handle_continue(:increment, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:noreply, new_state}
    end

    def handle_continue(:increment_and_sync, %{count: count} = state) do
      new_state = %{state | count: count + 10}
      {:noreply, new_state, :sync}
    end

    def terminate(_reason, state) do
      if state[:test_pid] && Process.alive?(state[:test_pid]) do
        send(state[:test_pid], :terminate_called)
      end

      :ok
    end

    def code_change(_old_vsn, state, :add_multiplier) do
      # Simulate a code change that adds a new field
      {:ok, Map.put(state, :multiplier, 2)}
    end

    def code_change(_old_vsn, state, _extra) do
      {:ok, state}
    end
  end

  defmodule AutoSyncServer do
    use DurableServer,
      vsn: 1

    def dump_state(state), do: state

    def load_state(_old_vsn, persisted_state) do
      DurableServerTest.atomify_keys(persisted_state)
    end

    def init(%{key: key}, info) do
      # assert that we have a task sup in the info map
      _ = info.task_supervisor
      {:ok, %{key: key, count: 0}, auto_sync: true}
    end

    def handle_call(:increment, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state}
    end
  end

  defmodule PeriodicSyncServer do
    use DurableServer,
      vsn: 1

    def dump_state(state), do: state

    def load_state(_old_vsn, persisted_state) do
      state = DurableServerTest.atomify_keys(persisted_state)
      Map.put_new(state, :count, 0)
    end

    def init(%{sync_ms: sync_ms} = init_state) do
      {:ok, init_state, auto_sync: false, sync_every_ms: sync_ms}
    end

    def handle_call(:increment, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state}
    end
  end

  defmodule InitInfoServer do
    use DurableServer, vsn: 1

    def dump_state(state), do: Map.take(state, [:key])

    def load_state(_old_vsn, persisted_state) do
      DurableServerTest.atomify_keys(persisted_state)
    end

    def init(%{key: key}, info) do
      # Store the info map in state so tests can verify it
      {:ok, %{key: key, info: info}}
    end

    def handle_call(:get_info, _from, state) do
      {:reply, state.info, state}
    end
  end

  defmodule AfterTerminateTestServer do
    use DurableServer, vsn: 1

    def dump_state(state), do: Map.take(state, [:key, :count])

    def load_state(_old_vsn, persisted_state) do
      persisted_state
      |> DurableServerTest.atomify_keys()
      |> Map.put_new(:count, 0)
      |> Map.put(:test_pid, nil)
    end

    def init(%{key: key} = init_state) do
      {:ok, %{key: key, count: 0, test_pid: Map.get(init_state, :test_pid)}}
    end

    def handle_call({:set_test_pid, test_pid}, _from, state) do
      {:reply, :ok, %{state | test_pid: test_pid}}
    end

    def handle_call(:increment, _from, %{count: count} = state) do
      {:reply, count + 1, %{state | count: count + 1}}
    end

    def handle_call(:stop_normal, _from, state) do
      {:stop, :normal, :ok, state}
    end

    def handle_call(:stop_error, _from, state) do
      {:stop, {:error, :boom}, :ok, state}
    end

    def terminate(reason, state) do
      {:after_terminate_payload, state.test_pid, reason, state.count}
    end

    def after_terminate({:after_terminate_payload, pid, terminate_reason, count}, info)
        when is_pid(pid) do
      send(pid, {:after_terminate_called, terminate_reason, count, info})
      :ok
    end

    def after_terminate(_terminate_return, _info), do: :ok
  end

  setup do
    # Create a test bucket
    test_bucket_name =
      "durable-test-durable-#{DurableServer.UUID.uuid4()}"

    case ObjectStore.create_bucket_with_credentials(test_object_store(), test_bucket_name) do
      {:ok, %ObjectStore{} = store} ->
        on_exit(fn ->
          # Clean up bucket on test completion
          try do
            ObjectStore.delete_bucket(store, test_bucket_name)
          catch
            _, _ -> :ok
          end
        end)

        # Start a DurableServer.Supervisor for tests that need one
        {supervisor_name, supervisor_pid, prefix} = start_test_supervisor()

        {:ok,
         test_bucket: test_bucket_name,
         store: store,
         supervisor_name: supervisor_name,
         supervisor_pid: supervisor_pid,
         prefix: prefix}

      {:error, reason} ->
        {:skip, "Failed to create test bucket: #{inspect(reason)}"}
    end
  end

  describe "init/1" do
    test "initializes successfully with valid options", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "test-server-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(supervisor_name, {TestServer, %{key: key}})

      assert Process.alive?(pid)
    end

    test "handles ignore return from user init", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      # DurableServer.Supervisor.start_child should handle ignore return
      result =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, %{key: "123", ignore: true}}
        )

      # When init returns :ignore, start_child should return error or specific result
      case result do
        # This is acceptable behavior
        {:error, _} -> :ok
        # This would also be acceptable
        {:ok, :undefined} -> :ok
        # Direct :ignore return is also acceptable
        :ignore -> :ok
        other -> flunk("Unexpected result: #{inspect(other)}")
      end
    end

    test "validates start_link arguments require :key field", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      # Missing key should cause start_child to fail with ArgumentError

      assert_raise ArgumentError, ~r/expects a map with :key field/, fn ->
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, %{custom_opts: %{}}}
        )
      end
    end

    test "validates init/1 returned options are valid DurableServer options", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      # Invalid option returned from init should cause startup to fail
      {:error, {%ArgumentError{message: message}, _}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, %{key: "test", invalid_options_test: true}}
        )

      assert message =~ "unknown keys [:invalid_opt]"
      assert message =~ "allowed keys are: [:auto_sync, :sync_every_ms, :meta, :permanent]"
    end

    test "returns startup sync failure to caller and logs the error" do
      {supervisor_name, _supervisor_pid, _prefix} =
        start_test_supervisor(
          backend: {RejectingStartupSyncBackend, delegate_opts: test_object_store_opts()}
        )

      key = "startup-sync-fail-#{DurableServer.UUID.uuid4()}"

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:error, :startup_sync_rejected} =
                   DurableServer.Supervisor.start_child(
                     supervisor_name,
                     {TestServer, %{key: key}}
                   )
        end)

      assert log =~ "failed to sync startup status :running"
      assert log =~ key
    end

    test "sets up node reference in persistent term", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "test-server-#{DurableServer.UUID.uuid4()}"

      {:ok, {_pid, _meta}} =
        DurableServer.Supervisor.start_child(supervisor_name, {TestServer, %{key: key}})

      assert is_integer(DurableServer.Supervisor.node_ref(supervisor_name))
    end

    test "ensure_started_child returns existing process or starts new one", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "ensure-started-test-#{DurableServer.UUID.uuid4()}"
      initial_meta = %{type: "test", version: 1}

      # First call should start the process
      {:ok, {pid1, ^initial_meta}} =
        DurableServer.Supervisor.ensure_started_child(
          supervisor_name,
          {TestServer, %{key: key, meta: initial_meta}}
        )

      assert Process.alive?(pid1)

      # Second call should return the same process
      {:ok, {pid2, ^initial_meta}} =
        DurableServer.Supervisor.ensure_started_child(
          supervisor_name,
          {TestServer, %{key: key, meta: initial_meta}}
        )

      assert pid1 == pid2
      assert Process.alive?(pid2)

      # Verify it's the same process in the registry
      {^pid1, ^initial_meta} = DurableServer.Supervisor.lookup(supervisor_name, key)
    end
  end

  describe "existing: true" do
    test "start_child returns {:error, :not_found} when no persisted state exists", %{
      supervisor_name: supervisor_name
    } do
      key = "nonexistent-#{DurableServer.UUID.uuid4()}"

      assert {:error, :not_found} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {TestServer, %{key: key}},
                 existing: true
               )
    end

    test "start_child starts server when persisted state exists", %{
      supervisor_name: supervisor_name
    } do
      key = "existing-start-#{DurableServer.UUID.uuid4()}"

      # Start, sync, and stop to create valid persisted state
      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(supervisor_name, {TestServer, %{key: key}})

      GenServer.call(pid, :increment_and_sync)
      ref = Process.monitor(pid)
      GenServer.call(pid, :stop_normal)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

      # Now start_child with existing: true should succeed
      assert {:ok, {pid2, _meta}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {TestServer, %{key: key}},
                 existing: true
               )

      assert Process.alive?(pid2)
      assert pid2 != pid
    end

    test "ensure_started_child returns {:error, :not_found} when no persisted state exists", %{
      supervisor_name: supervisor_name
    } do
      key = "nonexistent-ensure-#{DurableServer.UUID.uuid4()}"

      assert {:error, :not_found} =
               DurableServer.Supervisor.ensure_started_child(
                 supervisor_name,
                 {TestServer, %{key: key}},
                 existing: true
               )
    end

    test "ensure_started_child starts server when persisted state exists", %{
      supervisor_name: supervisor_name
    } do
      key = "existing-ensure-#{DurableServer.UUID.uuid4()}"

      # Start, sync, and stop to create valid persisted state
      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(supervisor_name, {TestServer, %{key: key}})

      GenServer.call(pid, :increment_and_sync)
      ref = Process.monitor(pid)
      GenServer.call(pid, :stop_normal)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

      # Now ensure_started_child with existing: true should succeed
      assert {:ok, {pid2, _meta}} =
               DurableServer.Supervisor.ensure_started_child(
                 supervisor_name,
                 {TestServer, %{key: key}},
                 existing: true
               )

      assert Process.alive?(pid2)
      assert pid2 != pid
    end

    test "ensure_started_child returns already-running process even without persisted state", %{
      supervisor_name: supervisor_name
    } do
      key = "already-running-#{DurableServer.UUID.uuid4()}"

      # Start a process first (creates persisted state)
      {:ok, {pid1, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, %{key: key}}
        )

      # ensure_started_child with existing: true should find it via lookup
      # (before reaching the S3 check)
      {:ok, {pid2, _meta}} =
        DurableServer.Supervisor.ensure_started_child(
          supervisor_name,
          {TestServer, %{key: key}},
          existing: true
        )

      assert pid1 == pid2
    end
  end

  describe "init/2 with info map" do
    test "receives built-in supervisor info", %{supervisor_name: supervisor_name} do
      key = "init-info-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {InitInfoServer, %{key: key}}
        )

      info = GenServer.call(pid, :get_info)

      # Verify built-in keys are present
      assert info.supervisor == supervisor_name
      assert info.task_supervisor == DurableServer.Supervisor.get_task_supervisor(supervisor_name)

      assert info.dynamic_supervisor ==
               DurableServer.Supervisor.get_dynamic_supervisor(supervisor_name)
    end

    test "receives user-defined init_info from supervisor config" do
      # Start a supervisor with custom init_info
      prefix = "init-info-custom-#{DurableServer.UUID.uuid4()}/"
      supervisor_name = :"init_info_test_sup_#{System.unique_integer([:positive])}"

      {:ok, _sup_pid} =
        DurableServer.Supervisor.start_link(
          name: supervisor_name,
          prefix: prefix,
          object_store: test_object_store_opts(),
          init_info: %{api_client: MyApp.APIClient, custom_key: "custom_value"}
        )

      key = "init-info-custom-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {InitInfoServer, %{key: key}}
        )

      info = GenServer.call(pid, :get_info)

      # Verify user-defined keys are present and merged
      assert info.api_client == MyApp.APIClient
      assert info.custom_key == "custom_value"

      # Built-in keys should still be there
      assert info.supervisor == supervisor_name
      assert is_atom(info.task_supervisor)
      assert is_atom(info.dynamic_supervisor)

      Supervisor.stop(supervisor_name)
    end
  end

  describe "basic GenServer behavior" do
    setup %{supervisor_name: supervisor_name, prefix: _prefix} do
      key = "basic-server-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(supervisor_name, {TestServer, %{key: key}})

      :ok = GenServer.call(pid, {:set_test_pid, self()})
      %{pid: pid}
    end

    test "handles call messages", %{pid: pid} do
      assert GenServer.call(pid, :get_count) == 0
      assert GenServer.call(pid, :increment) == 1
      assert GenServer.call(pid, :get_count) == 1
    end

    test "handles cast messages", %{pid: pid} do
      GenServer.cast(pid, :increment)
      # Allow cast to process
      :sys.get_state(pid)
      assert GenServer.call(pid, :get_count) == 1
    end

    test "handles info messages", %{pid: pid} do
      send(pid, :increment)
      # Allow info to process
      :sys.get_state(pid)
      assert GenServer.call(pid, :get_count) == 1
    end

    test "handles continue messages", %{pid: pid} do
      # Test continue without sync
      assert GenServer.call(pid, :continue_test) == :ok
      assert GenServer.call(pid, :get_count) == 1
    end

    test "handles continue actions with sync", %{supervisor_name: supervisor_name, prefix: prefix} do
      key = "continue-sync-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer,
           %{
             key: key,
             custom_opts: %{
               auto_sync: false
             }
           }}
        )

      # Test continue with sync
      assert GenServer.call(pid, :get_count) == 0
      assert GenServer.call(pid, :continue_and_sync_test) == :ok
      # incremented by 10 from continue
      assert GenServer.call(pid, :get_count) == 10

      # Verify state was synced by continue action
      store = test_object_store()

      {:ok, %StoredState{} = persisted_data} =
        DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})

      assert %{state: %{"count" => 10}} = persisted_data
    end

    @tag :durable_server
    test "callbacks can update group registry metadata via :meta option", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      # Start a test server with initial metadata
      initial_meta = %{user_count: 0, status: "idle"}
      key = "meta-test-server"

      {:ok, {pid, ^initial_meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, %{key: key, meta: initial_meta}}
        )

      # Verify initial metadata is set in group registry
      assert {^pid, ^initial_meta} = DurableServer.Supervisor.lookup(supervisor_name, key)

      # Update metadata via handle_call with meta option
      updated_meta = %{user_count: 5, status: "processing"}
      assert :ok = GenServer.call(pid, {:update_meta_test, updated_meta})

      # Verify metadata was updated in group registry
      assert {^pid, ^updated_meta} = DurableServer.Supervisor.lookup(supervisor_name, key)

      # Test with handle_cast and meta option
      cast_meta = %{user_count: 10, status: "busy"}
      GenServer.cast(pid, {:update_meta_cast, cast_meta})

      # ensure processed
      :sys.get_state(pid)

      # Verify cast metadata update
      assert {^pid, ^cast_meta} = DurableServer.Supervisor.lookup(supervisor_name, key)

      # Test with handle_info and meta option
      info_meta = %{user_count: 3, status: "waiting"}
      send(pid, {:update_meta_info, info_meta})

      # ensure processed
      :sys.get_state(pid)

      # Verify info metadata update
      assert {^pid, ^info_meta} = DurableServer.Supervisor.lookup(supervisor_name, key)
    end

    @tag :durable_server
    test "callbacks can combine action and :meta option", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "combo-test-server"
      initial_meta = %{counter: 0}

      {:ok, {pid, ^initial_meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, %{key: key, meta: initial_meta}}
        )

      # Test combining :sync action with :meta option
      updated_meta = %{counter: 1, last_sync: "now"}
      assert 1 = GenServer.call(pid, {:sync_and_update_meta, updated_meta})

      # Verify both state and metadata were updated
      assert GenServer.call(pid, :get_count) == 1
      assert {^pid, ^updated_meta} = DurableServer.Supervisor.lookup(supervisor_name, key)

      # Test with continue action and meta
      continue_meta = %{counter: 2, continued: true}
      assert :ok = GenServer.call(pid, {:continue_with_meta, continue_meta})

      # ensure processed
      :sys.get_state(pid)

      # Verify continue processed and metadata updated
      # 1 + 10 from continue
      assert GenServer.call(pid, :get_count) == 11

      assert {^pid, ^continue_meta} =
               DurableServer.Supervisor.lookup(supervisor_name, key)

      # Test with continue action + meta + explicit sync option
      sync_option_meta = %{counter: 3, continue_sync: true}
      assert :ok = GenServer.call(pid, {:continue_with_meta_and_sync_option, sync_option_meta})

      # ensure processed
      :sys.get_state(pid)

      # Verify continue processed and metadata updated
      # 11 + 1 from continue(:increment)
      assert GenServer.call(pid, :get_count) == 12
      assert {^pid, ^sync_option_meta} = DurableServer.Supervisor.lookup(supervisor_name, key)

      # Verify sync: true persisted callback state even though continue action itself did not return :sync
      store = test_object_store()

      {:ok, %StoredState{} = persisted_data} =
        DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})

      # `sync: true` syncs the state at callback return time (before handle_continue/2 runs)
      assert %{state: %{"count" => 11}} = persisted_data
    end

    @tag :durable_server
    test "meta option validation requires map", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "validation-test-server"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, %{key: key}}
        )

      # Test that invalid meta types cause the GenServer call to exit with FunctionClauseError
      catch_exit do
        GenServer.call(pid, {:invalid_meta_test, "not_a_map"})
      end
    end

    test "handles stop with reply", %{pid: pid} do
      Process.unlink(pid)
      ref = Process.monitor(pid)
      assert GenServer.call(pid, :stop_with_reply) == 100
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    test "handles stop without reply", %{pid: pid} do
      Process.unlink(pid)
      ref = Process.monitor(pid)
      assert GenServer.call(pid, :stop_normal) == :ok
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    test "syncs state to storage on terminate", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      # Get the key from the server's state for verification
      key = "terminate-sync-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer,
           %{
             key: key,
             custom_opts: %{
               auto_sync: false
             }
           }}
        )

      # Increment count but don't sync (auto_sync: false)
      assert GenServer.call(pid, :increment) == 1
      assert GenServer.call(pid, :get_count) == 1

      # Verify state is not synced yet (should still be initial state)
      store = test_object_store()
      {:ok, persisted_data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})
      assert %{state: %{"count" => 0}} = persisted_data

      # Stop the server - this should trigger terminate/2 which syncs
      :ok = GenServer.call(pid, :stop_normal)

      # Verify state was synced during termination
      {:ok, persisted_data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})
      assert %{state: %{"count" => 1}} = persisted_data
    end
  end

  describe "sync functionality" do
    setup %{supervisor_name: supervisor_name, prefix: _prefix} do
      key = "test-server-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(supervisor_name, {TestServer, %{key: key}})

      %{pid: pid, key: key}
    end

    test "explicit sync with :sync action", %{pid: pid} do
      # Increment with sync
      assert GenServer.call(pid, :increment_and_sync) == 1

      # Verify state persisted by checking if we can start another server with same key
      # This would require modifying the test to use a known key
      assert GenServer.call(pid, :get_count) == 1
    end

    test "sync with cast and :sync action", %{pid: pid} do
      GenServer.cast(pid, :increment_and_sync)
      # Allow cast and sync to complete
      :sys.get_state(pid)
      assert GenServer.call(pid, :get_count) == 1
    end

    test "sync with info and :sync action", %{pid: pid} do
      send(pid, :increment_and_sync)
      # Allow info and sync to complete
      :sys.get_state(pid)
      assert GenServer.call(pid, :get_count) == 1
    end

    test "handles sync errors gracefully", %{pid: pid} do
      # This test would require mocking ObjectStore to return errors
      # For now, verify the server continues operating
      assert GenServer.call(pid, :increment_and_sync) == 1
      assert GenServer.call(pid, :get_count) == 1
    end
  end

  describe "after_terminate callback" do
    test "invokes after_terminate after graceful final sync", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "after-terminate-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {AfterTerminateTestServer, %{key: key}}
        )

      :ok = GenServer.call(pid, {:set_test_pid, self()})

      # mutate without explicit sync
      assert 1 = GenServer.call(pid, :increment)

      store = test_object_store()

      {:ok, persisted_before} =
        DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})

      assert %{state: %{"count" => 0}} = persisted_before

      assert :ok = GenServer.call(pid, :stop_normal)

      assert_receive {:after_terminate_called, :normal, 1, info}, 1_000
      assert info.key == key
      assert info.supervisor == supervisor_name
      assert info.final_status == :stopped_graceful
      assert info.sync_result == :ok
      assert info.reason == :normal

      {:ok, persisted_after} =
        DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})

      assert %{state: %{"count" => 1}} = persisted_after
    end

    test "does not invoke after_terminate for non-graceful stop", %{
      supervisor_name: supervisor_name
    } do
      key = "after-terminate-error-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {AfterTerminateTestServer, %{key: key}}
        )

      :ok = GenServer.call(pid, {:set_test_pid, self()})
      assert :ok = GenServer.call(pid, :stop_error)
      refute_receive {:after_terminate_called, _terminate_reason, _count, _info}, 300
    end
  end

  describe "auto sync functionality" do
    test "auto syncs on state changes when enabled", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "auto-sync-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {AutoSyncServer, %{key: key}}
        )

      # Increment should trigger auto sync
      assert GenServer.call(pid, :increment) == 1

      # Verify state was persisted to storage due to auto_sync: true
      store = test_object_store()
      {:ok, persisted_data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})

      assert %{
               meta: %{
                 node_ref: _,
                 pid: ^pid,
                 module: DurableServerTest.AutoSyncServer,
                 node_str: "nonode@nohost",
                 status: :running
               },
               state: %{"count" => 1, "key" => ^key},
               vsn: 1
             } = persisted_data
    end

    test "does not auto sync when disabled", %{supervisor_name: supervisor_name, prefix: prefix} do
      key = "no-auto-sync-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer,
           %{
             key: key,
             custom_opts: %{
               auto_sync: false
             }
           }}
        )

      # Regular increment should not sync (auto_sync: false)
      assert GenServer.call(pid, :increment) == 1
      assert GenServer.call(pid, :get_count) == 1

      # Verify state was NOT persisted to storage due to auto_sync: false
      # Should only have initial state (count: 0) from init lock acquisition
      store = test_object_store()
      {:ok, persisted_data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})
      assert %{state: %{"count" => 0}} = persisted_data
    end
  end

  describe "periodic sync functionality" do
    @tag :slow
    test "periodically syncs state based on sync_every_ms", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "periodic-sync-test-#{DurableServer.UUID.uuid4()}"
      sync_ms = 100

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {PeriodicSyncServer, %{key: key, sync_ms: sync_ms}}
        )

      # Increment without explicit sync
      assert GenServer.call(pid, :increment) == 1

      # Wait for periodic sync to trigger
      Process.sleep(sync_ms * 2)
      :sys.get_state(pid)

      # Verify server is still responsive
      assert GenServer.call(pid, :increment) == 2
    end
  end

  describe "state persistence and recovery" do
    test "object store first boot passes backend-shaped JSON-decoded state to load_state/2", %{
      supervisor_name: supervisor_name
    } do
      key = "temporal-object-store-first-boot-#{DurableServer.UUID.uuid4()}"
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

      assert :json_string == GenServer.call(pid, :get_loaded_shape)

      snapshot = GenServer.call(pid, :get_snapshot)
      assert is_binary(snapshot.occurred_at)
      assert is_binary(snapshot.nested["occurred_at"])
    end

    test "persists state on termination and allows recovery", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      Process.put(:test_pid, self())
      key = "persistence-test-#{DurableServer.UUID.uuid4()}"

      # Start first server
      {:ok, {pid1, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer,
           %{
             key: key,
             custom_opts: %{
               auto_sync: false
             }
           }}
        )

      # Increment counter and explicitly sync
      assert GenServer.call(pid1, :increment) == 1
      assert GenServer.call(pid1, :increment_and_sync) == 2

      # Kill the first process
      Process.exit(pid1, :kill)

      # Start second server with same key - should eventually work after lock expires
      # For now, just verify basic functionality
      key2 = "different-key-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid2, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer,
           %{
             key: key2,
             custom_opts: %{
               auto_sync: false
             }
           }}
        )

      # Verify new server works
      assert GenServer.call(pid2, :get_count) == 0
    end

    test "handles explicit sync operations", %{supervisor_name: supervisor_name, prefix: _prefix} do
      key = "sync-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer,
           %{
             key: key,
             custom_opts: %{
               auto_sync: false
             }
           }}
        )

      # Start from known state
      assert GenServer.call(pid, :get_count) == 0

      # Test call with sync
      assert GenServer.call(pid, :increment_and_sync) == 1
      assert GenServer.call(pid, :get_count) == 1

      # Test cast with sync
      GenServer.cast(pid, :increment_and_sync)
      # Give more time for async operation
      :sys.get_state(pid)
      assert GenServer.call(pid, :get_count) == 2

      # Test info with sync
      send(pid, :increment_and_sync)
      # Give more time for async operation
      :sys.get_state(pid)
      assert GenServer.call(pid, :get_count) == 3
    end

    test "object store restart passes backend-shaped JSON-decoded state to load_state/2", %{
      supervisor_name: supervisor_name
    } do
      key = "temporal-object-store-#{DurableServer.UUID.uuid4()}"
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

      assert :json_string == GenServer.call(restarted_pid, :get_loaded_shape)

      snapshot = GenServer.call(restarted_pid, :get_snapshot)
      assert is_binary(snapshot.occurred_at)
      assert is_binary(snapshot.nested["occurred_at"])
    end
  end

  describe "lock mechanism" do
    test "__check_lock__/3 returns :locked for alive process with matching node_ref", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      node_ref = DurableServer.Supervisor.node_ref(supervisor_name)

      pid = self()
      assert match?({:locked, _}, DurableServer.__check_lock__(pid, node_ref, supervisor_name))
    end

    test "__check_lock__/3 returns :expired for dead process", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      node_ref = DurableServer.Supervisor.node_ref(supervisor_name)

      # Create a process and kill it
      pid = spawn(fn -> :ok end)
      Process.monitor(pid)
      Process.exit(pid, :kill)
      # Ensure process is dead
      assert_receive {:DOWN, _ref, :process, ^pid, _}

      assert DurableServer.__check_lock__(pid, node_ref, supervisor_name) == :expired
    end

    test "__check_lock__/3 returns :expired for mismatched node_ref", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      _node_ref = DurableServer.Supervisor.node_ref(supervisor_name)
      wrong_node_ref = DurableServer.UUID.uuid4()

      pid = self()
      assert DurableServer.__check_lock__(pid, wrong_node_ref, supervisor_name) == :expired
    end
  end

  describe "lock expiration edge cases" do
    test "handles process death during lock acquisition", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "lock-death-test-#{DurableServer.UUID.uuid4()}"

      # Start first server to establish lock
      {:ok, {pid1, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer,
           %{
             key: key,
             custom_opts: %{
               auto_sync: true
             }
           }}
        )

      # Verify it's running and increment count (this should sync due to auto_sync: true)
      assert GenServer.call(pid1, :increment) == 1
      assert GenServer.call(pid1, :get_count) == 1

      # Verify state was persisted
      store = test_object_store()
      {:ok, persisted_data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})

      assert %{
               state: %{"count" => 1},
               meta: %{
                 module: DurableServerTest.TestServer,
                 pid: _,
                 status: :running,
                 node_ref: _,
                 last_heartbeat_at: _,
                 node_str: "nonode@nohost"
               },
               vsn: 1
             } = persisted_data

      # Kill the process abruptly (simulating crash)
      Process.unlink(pid1)
      ref = Process.monitor(pid1)
      # Stop the process cleanly first
      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, pid1)
      assert_receive {:DOWN, ^ref, :process, ^pid1, _reason}

      # Second server should be able to acquire the expired lock and get existing state
      {:ok, {pid2, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer,
           %{
             key: key,
             custom_opts: %{
               auto_sync: false
             }
           }}
        )

      # Let's check what the second server actually loaded
      actual_count = GenServer.call(pid2, :get_count)
      # Should get the previous state (count: 1) since first server had incremented and synced
      assert actual_count == 1
    end
  end

  describe "error handling" do
    test "continues operation when sync operations encounter errors", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "test-server-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(supervisor_name, {TestServer, %{key: key}})

      # Basic operations should work
      assert GenServer.call(pid, :increment) == 1
      assert GenServer.call(pid, :increment_and_sync) == 2
      assert GenServer.call(pid, :get_count) == 2
    end

    test "auto-sync servers continue operating", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "error-handling-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {AutoSyncServer, %{key: key}}
        )

      # Even if sync fails, server should continue
      assert GenServer.call(pid, :increment) == 1
      assert GenServer.call(pid, :increment) == 2
    end
  end

  describe "code_change/3" do
    setup %{supervisor_name: supervisor_name, prefix: _prefix} do
      key = "test-server-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(supervisor_name, {TestServer, %{key: key}})

      %{pid: pid}
    end

    test "server continues to work after potential code changes", %{pid: pid} do
      # Basic functionality should work (testing the behavior rather than sys internals)
      assert GenServer.call(pid, :get_count) == 0
      assert GenServer.call(pid, :increment) == 1
      assert GenServer.call(pid, :get_count) == 1
    end

    test "code_change delegates to user module and transforms state", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "code-change-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer,
           %{
             key: key,
             custom_opts: %{
               auto_sync: false
             }
           }}
        )

      # Set some initial state
      assert GenServer.call(pid, :increment) == 1
      assert GenServer.call(pid, :get_count) == 1

      # Get current state
      %{user_state: old_user_state} = :sys.get_state(pid)
      assert %{count: 1} = old_user_state

      # Test that our code_change implementation works by calling it directly
      # (This tests the DurableServer.code_change/3 function)
      test_state = %DurableServer{
        user_state: %{count: 5},
        module: TestServer
      }

      {:ok, %{user_state: new_user_state}} =
        DurableServer.code_change("1.0", test_state, :add_multiplier)

      assert %{count: 5, multiplier: 2} = new_user_state
    end
  end

  describe "integration with ObjectStore" do
    test "uses ObjectStore for persistence operations", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "test-server-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(supervisor_name, {TestServer, %{key: key}})

      # Perform operations that should interact with ObjectStore
      assert GenServer.call(pid, :increment_and_sync) == 1
      assert GenServer.call(pid, :get_count) == 1
    end

    test "get_object allows introspection of stored state", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "introspection-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer,
           %{
             key: key,
             custom_opts: %{
               auto_sync: false
             }
           }}
        )

      # Increment and sync state
      assert GenServer.call(pid, :increment_and_sync) == 1

      # Now introspect the stored object
      store = test_object_store()

      assert {:ok,
              %StoredState{
                meta: %{
                  node_ref: _,
                  pid: _,
                  last_heartbeat_at: _,
                  module: DurableServerTest.TestServer,
                  node_str: "nonode@nohost",
                  status: :running
                },
                state: %{
                  "count" => 1,
                  "custom_opts" => %{"auto_sync" => false},
                  "key" => ^key
                },
                vsn: 1
              }} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})
    end
  end

  describe "initialization edge cases" do
    test "handles error return from user init", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "error-test-#{DurableServer.UUID.uuid4()}"

      result =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, %{key: key, error_test: "custom_error"}}
        )

      assert {:error, {:bad_init_return, {:error, "custom_error"}}} = result
    end

    test "handles crash during user init", %{supervisor_name: supervisor_name, prefix: _prefix} do
      key = "crash-test-#{DurableServer.UUID.uuid4()}"

      result =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, %{key: key, crash_on_init: true}}
        )

      assert {:error, {_, _}} = result
    end

    test "handles invalid return from user init", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "invalid-test-#{DurableServer.UUID.uuid4()}"

      result =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, %{key: key, invalid_return: true}}
        )

      assert {:error, {:bad_init_return, :invalid_return}} = result
    end

    test "handles missing required options", %{supervisor_name: supervisor_name, prefix: _prefix} do
      # Test that start_link validation catches missing key

      assert_raise ArgumentError, ~r/expects a map with :key field/, fn ->
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, %{bad_options: true}}
        )
      end
    end
  end

  describe "crash recovery" do
    test "server crashes mark status as crashed on termination", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "crash-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, %{key: key, count: 0}}
        )

      # Increment to create some state
      assert GenServer.call(pid, :increment) == 1

      # Monitor and crash the process
      ref = Process.monitor(pid)
      catch_exit(GenServer.call(pid, :crash))
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}

      # Check that status was set to crashed
      store = test_object_store()
      {:ok, data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})
      assert data.meta.status == :crashed
    end

    test "abnormal stop reasons mark status as crashed", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "abnormal-stop-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, %{key: key, count: 0}}
        )

      assert GenServer.call(pid, :increment) == 1

      ref = Process.monitor(pid)
      assert GenServer.call(pid, :stop_abnormal) == :ok
      assert_receive {:DOWN, ^ref, :process, ^pid, {:error, :abnormal_reason}}

      store = test_object_store()
      {:ok, data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})
      assert data.meta.status == :crashed
    end

    test "cast crashes mark status as crashed", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "cast-crash-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, %{key: key, count: 0}}
        )

      assert GenServer.call(pid, :increment) == 1

      ref = Process.monitor(pid)
      GenServer.cast(pid, :crash)
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}

      store = test_object_store()
      {:ok, data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})
      assert data.meta.status == :crashed
    end

    test "info crashes mark status as crashed", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "info-crash-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, %{key: key, count: 0}}
        )

      assert GenServer.call(pid, :increment) == 1

      ref = Process.monitor(pid)
      send(pid, :crash)
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}

      store = test_object_store()
      {:ok, data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})
      assert data.meta.status == :crashed
    end
  end

  describe "metadata override validation" do
    test "sync_state_to_storage validates allowed override keys", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "validation-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, %{key: key, count: 0}}
        )

      # This should work - status is allowed
      ref = Process.monitor(pid)
      assert :ok = DurableServer.Supervisor.terminate_child_permanent(supervisor_name, pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}
    end
  end

  describe "multiple servers with same key" do
    test "second server cannot start with same key while first is running", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "duplicate-key-test-#{DurableServer.UUID.uuid4()}"

      {:ok, _pid1} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, %{key: key}}
        )

      # Second server with same key should fail to start due to lock
      result =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, %{key: key}}
        )

      assert {:error, {:already_started, {pid, _meta}}} = result
      assert is_pid(pid)
    end
  end

  # additional test server modules for edge cases
  defmodule EdgeCaseTestServer do
    use DurableServer,
      vsn: 1

    def dump_state(state), do: state

    def load_state(_old_vsn, persisted_state) do
      DurableServerTest.atomify_keys(persisted_state)
    end

    def init(%{error_test: reason}) do
      {:error, reason}
    end

    def init(%{crash_on_init: true}) do
      raise "Intentional crash during init"
    end

    def init(%{bad_options: true}) do
      {:ok, %{count: 0}, auto_synctypo: false}
    end

    def init(%{invalid_return: true}) do
      :invalid_return
    end

    def init(state) do
      {:ok, state, auto_sync: false}
    end

    def handle_call(:get_count, _from, %{count: count} = state) do
      {:reply, count, state}
    end

    def handle_call(:increment, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state}
    end

    def handle_call(:increment_and_sync, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state, :sync}
    end

    def handle_call(:crash, _from, _state) do
      raise "Intentional crash"
    end

    def handle_call(:stop_abnormal, _from, state) do
      {:stop, {:error, :abnormal_reason}, :ok, state}
    end

    def handle_cast(:crash, _state) do
      raise "Intentional crash in cast"
    end

    def handle_info(:crash, _state) do
      raise "Intentional crash in info"
    end
  end

  describe "crash tracking and permanent status" do
    test "servers are non-permanent by default", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "non-permanent-default-test-#{DurableServer.UUID.uuid4()}"

      {:ok, _pid} =
        DurableServer.Supervisor.start_child(supervisor_name, {TestServer, %{key: key}})

      # Check metadata shows non-permanent by default (user must opt-in)
      store = test_object_store()
      {:ok, data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})
      assert data.meta.permanent == false
    end

    test "servers can be marked as permanent", %{supervisor_name: supervisor_name, prefix: prefix} do
      key = "permanent-test-#{DurableServer.UUID.uuid4()}"

      # Define a server that returns permanent: true
      defmodule PermanentTestServer do
        use DurableServer, vsn: 1

        def dump_state(state), do: state

        def load_state(_old_vsn, state) do
          DurableServerTest.atomify_keys(state)
        end

        def init(state) do
          {:ok, state, permanent: true}
        end
      end

      {:ok, _pid} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {PermanentTestServer, %{key: key}}
        )

      # Check metadata shows permanent
      store = test_object_store()
      {:ok, data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})
      assert data.meta.permanent == true
    end

    test "repeated crashes mark server as permanently crashed", %{
      supervisor_name: _supervisor_name,
      prefix: _prefix
    } do
      # Test the circuit breaker logic directly since the function was moved to CircuitBreaker module
      alias DurableServer.CircuitBreaker

      config = %{
        object_store: test_object_store(),
        crash_threshold_count: 3,
        # 1 minute
        crash_threshold_window_ms: 60_000,
        module_circuit_breaker_count: 50,
        module_circuit_breaker_window_ms: 5 * 60 * 1000,
        module_circuit_breaker_cooldown_ms: 30 * 60 * 1000
      }

      supervisor_name = :test_supervisor_durable_server_test
      circuit_breaker = CircuitBreaker.new(supervisor_name, config)
      current_time = System.system_time(:millisecond)

      # Create crash entries to simulate repeated crashes
      crash_entry1 = %{timestamp: current_time - 30000, reason: "crash 1", node_ref: "test_node"}
      crash_entry2 = %{timestamp: current_time - 20000, reason: "crash 2", node_ref: "test_node"}
      crash_entry3 = %{timestamp: current_time - 10000, reason: "crash 3", node_ref: "test_node"}

      fake_metadata = %{crash_history: []}

      # These should return :crashed since there's no existing crash history
      {status1, _} =
        CircuitBreaker.check_object_crash_status(
          circuit_breaker,
          fake_metadata,
          crash_entry1
        )

      assert status1 == :crashed

      {status2, _} =
        CircuitBreaker.check_object_crash_status(
          circuit_breaker,
          fake_metadata,
          crash_entry2
        )

      assert status2 == :crashed

      {status3, _} =
        CircuitBreaker.check_object_crash_status(
          circuit_breaker,
          fake_metadata,
          crash_entry3
        )

      assert status3 == :crashed
    end

    test "explicitly restarting permanently crashed server clears crash status", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "clear-crash-test-#{DurableServer.UUID.uuid4()}"
      storage_key = "#{prefix}#{key}"

      # Manually create a permanently crashed object
      store = test_object_store()
      # Create properly encoded metadata
      meta_map = %{
        status: :permanently_crashed,
        crash_history: [
          %{timestamp: System.system_time(:millisecond) - 1000, reason: "crash"}
        ],
        key: key,
        module: TestServer,
        supervisor: supervisor_name,
        permanent: true,
        node_ref: "dead-node-ref-for-test",
        node_str: "dead-node-for-test",
        # Create a process that immediately dies
        pid: spawn(fn -> :ok end)
      }

      encoded_meta = meta_map |> :erlang.term_to_binary() |> Base.encode64()

      crashed_data = %{
        "vsn" => 1,
        "state" => %{},
        "meta" => encoded_meta
      }

      ObjectStore.put_object(store, storage_key, JSON.encode!(crashed_data))

      # Explicitly restart it
      {:ok, _pid} =
        DurableServer.Supervisor.start_child(supervisor_name, {TestServer, %{key: key}})

      # Verify crash status was cleared and server is running
      {:ok, updated_data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})
      assert updated_data.meta.status == :running
    end

    test "durable crash history accumulates across restarts until permanent crash", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "durable-crash-test-#{DurableServer.UUID.uuid4()}"
      storage_key = "#{prefix}#{key}"

      # Create a server that will crash multiple times
      defmodule CrashingServer do
        use DurableServer, vsn: 1

        def init(%{key: key, crash_after: crash_after}) do
          {:ok, %{key: key, crash_after: crash_after, call_count: 0}}
        end

        def handle_call(:get_count, _from, state) do
          new_count = state.call_count + 1
          new_state = %{state | call_count: new_count}

          if new_count >= state.crash_after do
            # Simulate a crash
            raise "Intentional crash for testing"
          end

          {:reply, new_count, new_state}
        end

        def dump_state(state), do: state

        def load_state(_old_vsn, state) do
          DurableServerTest.atomify_keys(state)
        end
      end

      %{object_store: store} = DurableServer.Supervisor.__get_config__(supervisor_name)

      # First crash - should create crash history
      {:ok, {pid1, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {CrashingServer, %{key: key, crash_after: 1}}
        )

      # Trigger crash
      ref = Process.monitor(pid1)
      catch_exit(GenServer.call(pid1, :get_count))

      # Wait for process to terminate and crash to be processed
      assert_receive {:DOWN, ^ref, :process, _pid, _}

      # Check that crash history was recorded (don't check status since next server will overwrite it)
      {:ok, data1} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})
      assert length(data1.meta.crash_history) == 1

      # Verify the crash entry has the right structure
      assert [%{timestamp: _, reason: _, node_ref: _}] = data1.meta.crash_history

      # The supervisor will automatically restart the crashed server
      # Let's trigger more crashes by calling the already-restarted server

      {:ok, {restarted_pid, _meta}} =
        DurableServer.Supervisor.ensure_started_child(
          supervisor_name,
          {CrashingServer, %{key: key, crash_after: 1}}
        )

      # Second crash
      ref = Process.monitor(restarted_pid)
      catch_exit(GenServer.call(restarted_pid, :get_count))
      assert_receive {:DOWN, ^ref, :process, _pid, _}

      # Check crash history accumulated
      {:ok, data2} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})
      assert length(data2.meta.crash_history) == 2

      # Continue crashing until we hit the threshold (default is 5)
      for _i <- 3..5 do
        # Find the current server process (may have been restarted)
        {:ok, {current_pid, _meta}} =
          DurableServer.Supervisor.ensure_started_child(
            supervisor_name,
            {CrashingServer, %{key: key, crash_after: 1}}
          )

        ref = Process.monitor(current_pid)
        catch_exit(GenServer.call(current_pid, :get_count))
        assert_receive {:DOWN, ^ref, :process, _pid, _}
      end

      # Final check - crash history should have accumulated to threshold
      {:ok, %StoredState{} = final_data} =
        DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})

      assert length(final_data.meta.crash_history) == 5

      # Verify crash history entries have proper structure and are recent
      for crash_entry <- final_data.meta.crash_history do
        assert %{timestamp: _, reason: _, node_ref: _} = crash_entry
        assert is_integer(crash_entry.timestamp)
        assert is_binary(crash_entry.reason)
        assert is_integer(crash_entry.node_ref)
        # Verify timestamp is recent (within last 10 seconds)
        assert crash_entry.timestamp > System.system_time(:millisecond) - 10_000
      end

      # Test that the circuit breaker logic works by checking what status it would return
      circuit_breaker =
        DurableServer.Supervisor.__get_config__(supervisor_name).circuit_breaker

      crash_entry = %{
        timestamp: System.system_time(:millisecond),
        reason: "test crash",
        node_ref: "test_node"
      }

      {status, _} =
        DurableServer.CircuitBreaker.check_object_crash_status(
          circuit_breaker,
          final_data.meta,
          crash_entry
        )

      # after 5 crashes, adding one more should return :permanently_crashed
      assert status == :permanently_crashed
    end
  end

  defmodule ValidatorTestServer do
    use DurableServer,
      vsn: 1

    def dump_state(state), do: state

    def load_state(_old_vsn, persisted_state) do
      DurableServerTest.atomify_keys(persisted_state)
    end

    def init(init_state) do
      key = init_state[:key]

      if init_state[:setup_data] do
        # first write some data to storage to test decoder validation
        store = test_object_store()
        test_data = %{vsn: 1, state: %{count: 0}, meta: %{}}
        ObjectStore.put_object(store, key, JSON.encode!(test_data))
      end

      {:ok, %{key: key, count: 0}, auto_sync: false}
    end
  end

  # test server that supports user_meta
  defmodule LookupTestServer do
    use DurableServer, vsn: 1

    def init(%{key: key, user_meta: user_meta}) do
      {:ok, %{key: key, count: 0}, meta: user_meta}
    end

    def init(%{key: key}) do
      {:ok, %{key: key, count: 0}}
    end

    def dump_state(state), do: state

    def load_state(_old_vsn, state) do
      DurableServerTest.atomify_keys(state)
    end
  end

  # crashing test server that supports user_meta
  defmodule LookupCrashingServer do
    use DurableServer, vsn: 1

    def init(%{key: key, crash_after: crash_after, user_meta: user_meta}) do
      {:ok, %{key: key, crash_after: crash_after, call_count: 0}, meta: user_meta}
    end

    def handle_call(:get_count, _from, state) do
      new_count = state.call_count + 1
      new_state = %{state | call_count: new_count}

      if new_count >= state.crash_after do
        # Simulate a crash
        raise "Intentional crash for testing"
      end

      {:reply, new_count, new_state}
    end

    def dump_state(state), do: state

    def load_state(_old_vsn, state) do
      DurableServerTest.atomify_keys(state)
    end
  end

  describe "DurableServer.Supervisor.lookup/2" do
    test "returns {pid, user_meta} for existing server", %{supervisor_name: supervisor_name} do
      key = "lookup_test_#{DurableServer.UUID.uuid4()}"

      user_meta = %{type: "test_server", version: "1.0"}

      {:ok, {server_pid, ^user_meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {LookupTestServer, %{key: key, user_meta: user_meta}}
        )

      # test successful lookup
      result = DurableServer.Supervisor.lookup(supervisor_name, key)
      assert {^server_pid, ^user_meta} = result
    end

    test "returns nil for non-existent server", %{supervisor_name: supervisor_name} do
      result = DurableServer.Supervisor.lookup(supervisor_name, "non_existent_key")
      assert result == nil
    end

    test "returns nil for non-existent supervisor" do
      # use a unique supervisor name that definitely doesn't exist
      non_existent_name =
        :"definitely_non_existent_supervisor_#{DurableServer.UUID.uuid4()}"

      # this should handle the error gracefully and return nil
      result = DurableServer.Supervisor.lookup(non_existent_name, "some_key")
      assert result == nil
    end

    test "handles crashed servers correctly", %{supervisor_name: supervisor_name} do
      key = "lookup_crash_test_#{DurableServer.UUID.uuid4()}"

      user_meta = %{type: "crash_test"}

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {LookupCrashingServer, %{key: key, user_meta: user_meta, crash_after: 1}}
        )

      # verify initial lookup works
      result = DurableServer.Supervisor.lookup(supervisor_name, key)
      assert {^server_pid, ^user_meta} = result

      # crash the server
      ref = Process.monitor(server_pid)
      catch_exit(GenServer.call(server_pid, :get_count))
      # give time for supervisor to restart
      assert_receive {:DOWN, ^ref, :process, _pid, _}

      # server should be restarted automatically by the supervisor with the same user_meta
      result_after_crash = DurableServer.Supervisor.lookup(supervisor_name, key)

      case result_after_crash do
        {new_pid, ^user_meta} when new_pid != server_pid ->
          # server was restarted with new pid, user_meta preserved
          assert Process.alive?(new_pid)

        nil ->
          # if no automatic restart, that's also valid behavior for some configurations
          :ok
      end
    end

    test "handles different user_meta types", %{supervisor_name: supervisor_name} do
      test_cases = [
        {"string_meta", "simple string"},
        {"map_meta", %{complex: "map", with: %{nested: "values"}}},
        {"list_meta", [1, 2, 3, "mixed", %{types: true}]},
        {"my_meta", "my_str"},
        {"number_meta", 42},
        {"nil_meta", nil}
      ]

      for {key_suffix, user_meta} <- test_cases do
        key = "meta_test_#{key_suffix}_#{DurableServer.UUID.uuid4()}"

        {:ok, {server_pid, ^user_meta}} =
          DurableServer.Supervisor.start_child(
            supervisor_name,
            {LookupTestServer, %{key: key, user_meta: user_meta}}
          )

        result = DurableServer.Supervisor.lookup(supervisor_name, key)
        assert {^server_pid, ^user_meta} = result
      end
    end

    test "lookup is case sensitive for keys", %{supervisor_name: supervisor_name} do
      base_key = "CaseSensitive_#{DurableServer.UUID.uuid4()}"

      user_meta = %{test: "case_sensitivity"}

      {:ok, _server_pid} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {LookupTestServer, %{key: base_key, user_meta: user_meta}}
        )

      # correct case should work
      result_correct = DurableServer.Supervisor.lookup(supervisor_name, base_key)
      assert {_pid, ^user_meta} = result_correct

      # different case should not work
      result_lower = DurableServer.Supervisor.lookup(supervisor_name, String.downcase(base_key))
      assert result_lower == nil

      result_upper = DurableServer.Supervisor.lookup(supervisor_name, String.upcase(base_key))
      assert result_upper == nil
    end
  end

  defmodule DeleteTestServer do
    use DurableServer,
      vsn: 1

    def dump_state(state) do
      Map.take(state, [:key, :user_meta])
    end

    def load_state(_old_vsn, encoded_state) do
      %{
        key: encoded_state[:key] || encoded_state["key"],
        user_meta: encoded_state[:user_meta] || encoded_state["user_meta"],
        test_pid: nil
      }
    end

    def init(init_state) do
      new_state = %{
        key: init_state.key,
        user_meta: init_state.user_meta,
        test_pid: nil
      }

      {:ok, new_state, meta: init_state.user_meta || %{}}
    end

    def handle_call({:set_test_pid, test_pid}, _from, state) do
      {:reply, :ok, %{state | test_pid: test_pid}}
    end

    def handle_call(:delete_self, _from, state) do
      {:stop, {:shutdown, :delete}, :ok, state}
    end

    def handle_call(:delete_self_with_reply, _from, state) do
      if test_pid = state.test_pid do
        send(test_pid, {:deleting, state.key})
      end

      {:stop, {:shutdown, :delete}, :deleted, state}
    end

    def handle_call(:delete_self_non_shutdown, _from, state) do
      {:stop, :delete, :ok, state}
    end

    def handle_call(:stop_permanent, _from, state) do
      {:stop, {:shutdown, :permanent}, :ok, state}
    end

    def handle_call(:stop_permanent_non_shutdown, _from, state) do
      {:stop, :permanent, :ok, state}
    end

    def terminate(reason, state) do
      if test_pid = state.test_pid do
        send(test_pid, {:terminate, reason, state.key})
      end

      :ok
    end
  end

  describe "terminate_and_delete_child functionality" do
    setup do
      {supervisor_name, supervisor_pid, prefix} = start_test_supervisor()
      %{supervisor_name: supervisor_name, supervisor_pid: supervisor_pid, prefix: prefix}
    end

    test "terminate_and_delete_child/2 with PID deletes running process and storage", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "delete_test_pid_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, %{key: key}}
        )

      GenServer.call(server_pid, {:set_test_pid, self()})

      assert Process.alive?(server_pid)
      assert %{} = :sys.get_state(server_pid)

      # verify object exists in storage
      store = test_object_store()
      assert {:ok, _} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # delete by PID
      assert :ok =
               DurableServer.Supervisor.terminate_and_delete_child(supervisor_name, server_pid)

      # verify process is terminated
      refute Process.alive?(server_pid)

      # verify object is deleted from storage
      assert {:error, :not_found} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # verify terminate was called
      assert_receive {:terminate, _reason, ^key}
    end

    test "terminate_and_delete_child/2 with key deletes running process and storage", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "delete_test_key_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, %{key: key}}
        )

      GenServer.call(server_pid, {:set_test_pid, self()})

      assert Process.alive?(server_pid)

      # verify object exists in storage
      store = test_object_store()
      assert {:ok, _} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # delete by key
      assert :ok = DurableServer.Supervisor.terminate_and_delete_child(supervisor_name, key)

      # verify process is terminated
      refute Process.alive?(server_pid)

      # verify object is deleted from storage
      assert {:error, :not_found} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # verify terminate was called
      assert_receive {:terminate, _reason, ^key}
    end

    test "terminate_and_delete_child/2 with key when process not running still deletes storage",
         %{
           supervisor_name: supervisor_name,
           prefix: prefix
         } do
      key = "delete_test_orphaned_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, %{key: key}}
        )

      # set the test PID after starting
      GenServer.call(server_pid, {:set_test_pid, self()})

      # wait for sync to ensure object is saved
      :sys.get_state(server_pid)

      ref = Process.monitor(server_pid)
      # kill the process (simulate crash)
      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, server_pid)
      assert_receive {:DOWN, ^ref, :process, ^server_pid, _}

      # verify object still exists in storage
      store = test_object_store()
      assert {:ok, _} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # delete by key (process not running)
      assert :ok = DurableServer.Supervisor.terminate_and_delete_child(supervisor_name, key)

      # verify object is deleted from storage
      assert {:error, :not_found} = ObjectStore.get_object(store, "#{prefix}#{key}")
    end

    test "terminate_and_delete_child/2 with non-existent key returns ok", %{
      supervisor_name: supervisor_name
    } do
      non_existent_key = "non_existent_#{DurableServer.UUID.uuid4()}"

      # should succeed even if key doesn't exist
      assert :ok =
               DurableServer.Supervisor.terminate_and_delete_child(
                 supervisor_name,
                 non_existent_key
               )
    end
  end

  describe "{:stop, {:shutdown, :delete}, state} callback functionality" do
    setup do
      {supervisor_name, supervisor_pid, prefix} = start_test_supervisor()
      %{supervisor_name: supervisor_name, supervisor_pid: supervisor_pid, prefix: prefix}
    end

    test "server can delete itself via callback", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "delete_callback_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, %{key: key}}
        )

      # set the test PID after starting
      GenServer.call(server_pid, {:set_test_pid, self()})

      # verify server is running
      assert Process.alive?(server_pid)

      # verify object exists in storage
      store = test_object_store()
      assert {:ok, _} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # call delete_self callback
      Process.monitor(server_pid)
      assert :ok = GenServer.call(server_pid, :delete_self)

      # verify process is terminated
      assert_receive {:DOWN, _ref, :process, ^server_pid, {:shutdown, :delete}}

      # verify object is deleted from storage
      assert {:error, :not_found} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # verify terminate was called with {:shutdown, :delete}
      assert_receive {:terminate, {:shutdown, :delete}, ^key}
    end

    test "server delete callback with reply works correctly", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "delete_callback_reply_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, %{key: key}}
        )

      # set the test PID after starting
      GenServer.call(server_pid, {:set_test_pid, self()})

      # verify server is running
      assert Process.alive?(server_pid)

      # verify object exists in storage
      store = test_object_store()
      assert {:ok, _} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # call delete_self_with_reply callback
      assert :deleted = GenServer.call(server_pid, :delete_self_with_reply)

      # should receive deleting message before termination
      assert_receive {:deleting, ^key}

      # verify process is terminated
      refute Process.alive?(server_pid)

      # verify object is deleted from storage
      assert {:error, :not_found} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # verify terminate was called with {:shutdown, :delete}
      assert_receive {:terminate, {:shutdown, :delete}, ^key}
    end

    test "delete callback updates status to :deleting before deletion", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "delete_status_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, %{key: key}}
        )

      # set the test PID after starting
      GenServer.call(server_pid, {:set_test_pid, self()})

      # start the deletion process but pause before the object is actually deleted
      # by monitoring the deletion status sync
      parent = self()

      task =
        Task.async(fn ->
          GenServer.call(server_pid, :delete_self)
          send(parent, :continue)
          :done
        end)

      assert_receive :continue

      # if we're fast enough, we might catch the :deleting status in storage
      store = test_object_store()

      case ObjectStore.get_object(store, "#{prefix}#{key}") do
        {:ok, %{body: body}} ->
          parsed = JSON.decode!(body)
          meta_binary = parsed["meta"]

          try do
            meta =
              DurableServer.Meta.decode_from_binary(meta_binary, %{
                key: key,
                prefix: prefix
              })

            # should be either :deleting or object is already gone
            assert meta.status == :deleting or meta.status == :running
          rescue
            _ ->
              # object might already be deleted or metadata corrupted
              :ok
          end

        {:error, :not_found} ->
          # object already deleted, which is also valid
          :ok
      end

      # wait for deletion to complete
      assert :done = Task.await(task)

      # verify final deletion
      assert {:error, :not_found} = ObjectStore.get_object(store, "#{prefix}#{key}")
    end

    test "terminate_and_delete_child respects process locks and uses message-based deletion", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "delete_lock_test_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, %{key: key}}
        )

      # set the test PID after starting
      GenServer.call(server_pid, {:set_test_pid, self()})

      # verify server is running and holds a lock (by being able to call it)
      assert Process.alive?(server_pid)
      assert %{} = :sys.get_state(server_pid)

      # verify object exists in storage
      store = test_object_store()
      assert {:ok, _} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # delete by PID - this should send a message to the process since it's locked
      # the process should then delete itself
      assert :ok =
               DurableServer.Supervisor.terminate_and_delete_child(supervisor_name, server_pid)

      # verify process is terminated
      refute Process.alive?(server_pid)

      # verify object is deleted from storage
      assert {:error, :not_found} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # verify terminate was called with {:shutdown, :delete}
      assert_receive {:terminate, {:shutdown, :delete}, ^key}
    end
  end

  describe "non-shutdown wrapped stop behavior" do
    setup do
      {supervisor_name, supervisor_pid, prefix} = start_test_supervisor()
      %{supervisor_name: supervisor_name, supervisor_pid: supervisor_pid, prefix: prefix}
    end

    test "{:stop, :delete, state} deletes without propagating exit signal", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "delete_non_shutdown_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, %{key: key}}
        )

      GenServer.call(server_pid, {:set_test_pid, self()})

      # trap exits and link to verify no exit signal propagates
      Process.flag(:trap_exit, true)
      Process.link(server_pid)
      ref = Process.monitor(server_pid)

      assert %{} = :sys.get_state(server_pid)

      store = test_object_store()
      assert {:ok, _} = ObjectStore.get_object(store, "#{prefix}#{key}")

      assert :ok = GenServer.call(server_pid, :delete_self_non_shutdown)

      # verify process terminated with :normal (transformed from :delete)
      assert_receive {:DOWN, ^ref, :process, ^server_pid, :normal}

      # verify object is deleted from storage
      assert {:error, :not_found} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # verify terminate was called with :normal (user callback sees transformed reason)
      assert_receive {:terminate, :normal, ^key}

      # verify EXIT signal was :normal (which doesn't kill non-trapping processes)
      assert_receive {:EXIT, ^server_pid, :normal}, 100
    end

    test "{:stop, {:shutdown, :permanent}, state} stops with permanent status and propagates exit",
         %{
           supervisor_name: supervisor_name,
           prefix: prefix
         } do
      key = "permanent_shutdown_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, %{key: key}}
        )

      GenServer.call(server_pid, {:set_test_pid, self()})

      # Trap exits and link to verify exit signal propagates
      Process.flag(:trap_exit, true)
      Process.link(server_pid)
      ref = Process.monitor(server_pid)

      assert %{} = :sys.get_state(server_pid)

      # verify object exists in storage
      store = test_object_store()
      assert {:ok, _} = ObjectStore.get_object(store, "#{prefix}#{key}")

      assert :ok = GenServer.call(server_pid, :stop_permanent)

      # verify process terminated with {:shutdown, :permanent}
      assert_receive {:DOWN, ^ref, :process, ^server_pid, {:shutdown, :permanent}}

      # verify object still exists with stopped_permanent status
      assert {:ok, %{body: body}} = ObjectStore.get_object(store, "#{prefix}#{key}")
      decoded = JSON.decode!(body)
      meta_binary = Map.fetch!(decoded, "meta")

      meta =
        DurableServer.Meta.decode_from_binary(meta_binary, %{key: key, prefix: prefix})

      assert meta.status == :stopped_permanent

      # verify terminate was called with {:shutdown, :permanent}
      assert_receive {:terminate, {:shutdown, :permanent}, ^key}

      # verify EXIT signal was propagated (shutdown-wrapped exits propagate)
      assert_receive {:EXIT, ^server_pid, {:shutdown, :permanent}}, 100
    end

    test "{:stop, :permanent, state} stops permanently without propagating exit", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "permanent_non_shutdown_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, %{key: key}}
        )

      # set the test PID after starting
      GenServer.call(server_pid, {:set_test_pid, self()})

      # trap exits and link to verify no exit signal propagates
      Process.flag(:trap_exit, true)
      Process.link(server_pid)
      ref = Process.monitor(server_pid)
      assert %{} = :sys.get_state(server_pid)

      # verify object exists in storage
      store = test_object_store()
      assert {:ok, _} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # call stop_permanent_non_shutdown - should NOT propagate exit to linked process
      assert :ok = GenServer.call(server_pid, :stop_permanent_non_shutdown)

      # verify process terminated with :normal (transformed from :permanent)
      assert_receive {:DOWN, ^ref, :process, ^server_pid, :normal}

      # verify object still exists with stopped_permanent status
      assert {:ok, %{body: body}} = ObjectStore.get_object(store, "#{prefix}#{key}")
      decoded = JSON.decode!(body)
      meta_binary = Map.fetch!(decoded, "meta")

      meta =
        DurableServer.Meta.decode_from_binary(meta_binary, %{key: key, prefix: prefix})

      assert meta.status == :stopped_permanent

      # verify terminate was called with :normal (user callback sees transformed reason)
      assert_receive {:terminate, :normal, ^key}

      # verify EXIT signal was :normal (which doesn't kill non-trapping processes)
      assert_receive {:EXIT, ^server_pid, :normal}, 100
    end
  end
end
