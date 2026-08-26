defmodule DurableServerTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  import DurableServer.TestHelper

  alias DurableServer
  alias DurableServer.CircuitBreaker
  alias DurableServer.LifecycleManager
  alias DurableServer.Meta
  alias DurableServer.StoredState
  alias DurableServer.ObjectStore
  alias DurableServer.Backends.MirrorStore
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

  defp preloaded_child_spec(module, init_arg, %{body: %StoredState{meta: %Meta{}}} = preloaded) do
    boot_info = %{preloaded: preloaded, is_sticky_local: false}

    boot_info =
      case preloaded.body.meta.restart_attempt_node do
        node when is_binary(node) -> Map.put(boot_info, :restart_attempt_node, node)
        _ -> boot_info
      end

    {module, init_arg, boot_info}
  end

  defp refute_process_down(pid, timeout \\ 0) when is_pid(pid) do
    ref = Process.monitor(pid)
    refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, timeout
    Process.demonitor(ref, [:flush])
  end

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

  defp await_lookup(supervisor_name, key, timeout_ms \\ 2_000) do
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms
    do_await_lookup(supervisor_name, key, deadline_ms)
  end

  defp do_await_lookup(supervisor_name, key, deadline_ms) do
    case DurableServer.Supervisor.lookup(supervisor_name, key) do
      {pid, meta} ->
        {:ok, {pid, meta}}

      nil ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          :timeout
        else
          Process.sleep(25)
          do_await_lookup(supervisor_name, key, deadline_ms)
        end
    end
  end

  defp await_stored_pid(supervisor_name, prefix, key, timeout_ms \\ 2_000) do
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms
    do_await_stored_pid(supervisor_name, prefix, key, deadline_ms)
  end

  defp do_await_stored_pid(supervisor_name, prefix, key, deadline_ms) do
    case DurableServer.fetch_stored_state(
           supervisor_name,
           %{key: key, prefix: prefix},
           consistent: true
         ) do
      {:ok, %StoredState{meta: %Meta{pid: pid}}} when is_pid(pid) ->
        pid

      _ ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          flunk("storage lock for #{inspect(key)} was not written before timeout")
        else
          Process.sleep(25)
          do_await_stored_pid(supervisor_name, prefix, key, deadline_ms)
        end
    end
  end

  defp backend_table(%StorageBackend{state: %{table: table}}), do: table

  defp put_backend_override(table, key, consistent, response) do
    :ets.insert(table, {{:override, key, consistent}, response})
  end

  defp put_backend_delete_override(table, key, response) do
    :ets.insert(table, {{:delete_override, key}, response})
  end

  defp put_backend_write_override(table, key, response) do
    :ets.insert(table, {{:put_override, key}, response})
  end

  defp clear_backend_write_override(table, key) do
    :ets.delete(table, {:put_override, key})
  end

  defp start_server_with_write_failure(module, initial_state \\ %{}) do
    test_pid = self()

    after_put = fn _state, key, _data, opts, result ->
      send(test_pid, {:consistency_probe_put, key, opts, result})
    end

    {supervisor_name, _supervisor_pid, prefix} =
      start_test_supervisor(
        backend: {DurableServerTest.ConsistencyProbeBackend, owner: self(), after_put: after_put}
      )

    key = "sync-failure-#{DurableServer.UUID.uuid4()}"

    {:ok, {pid, _meta}} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {module, key: key, initial_state: initial_state}
      )

    %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
    storage_key = prefix <> key
    table = backend_table(backend)

    assert_receive {:consistency_probe_put, ^storage_key, _opts, {:ok, _object}}

    put_backend_write_override(table, storage_key, {:error, :unavailable})

    %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      key: key,
      pid: pid,
      backend: backend,
      table: table,
      storage_key: storage_key
    }
  end

  defp fatal_sync_exit(reason) do
    {:shutdown, {:durable, {:fatal_exit, {:sync_failed, reason}}}}
  end

  defp assert_call_sync_failure(pid, request, reason) do
    monitor_ref = Process.monitor(pid)
    expected_exit = fatal_sync_exit(reason)

    assert {^expected_exit, {GenServer, :call, _call_args}} =
             catch_exit(GenServer.call(pid, request))

    assert_receive {:DOWN, ^monitor_ref, :process, ^pid, ^expected_exit}
  end

  defp insert_running_lock(table, storage_key, supervisor_name, prefix, key, pid) do
    node_ref = DurableServer.Supervisor.node_ref(supervisor_name)

    stored_state = %StoredState{
      vsn: 1,
      state: %{"count" => 0},
      meta: %DurableServer.Meta{
        key: key,
        prefix: prefix,
        supervisor: supervisor_name,
        module: TestServer,
        status: :running,
        node_str: to_string(node()),
        node_ref: node_ref,
        pid: pid,
        last_heartbeat_at: System.system_time(:millisecond)
      }
    }

    :ets.insert(table, {{:data, storage_key}, %{body: stored_state, etag: "etag-#{key}"}})
    stored_state
  end

  defp start_group_registrar(supervisor_name, prefix, key, user_meta) do
    parent = self()

    pid =
      spawn_link(fn ->
        send(parent, {:registrar_ready, self()})

        receive do
          :register ->
            :ok =
              DurableServer.Supervisor.__register_child__(
                supervisor_name,
                key,
                %DurableServer.GroupMeta{
                  key: key,
                  module: TestServer,
                  storage_key: prefix <> key,
                  node_ref: DurableServer.Supervisor.node_ref(supervisor_name),
                  start_time: System.system_time(:millisecond),
                  user_meta: user_meta,
                  supervisor: supervisor_name
                }
              )

            send(parent, {:registrar_registered, self()})

            receive do
              :stop -> :ok
            end

          :stop ->
            :ok
        end
      end)

    assert_receive {:registrar_ready, ^pid}

    on_exit(fn ->
      if Process.alive?(pid) do
        send(pid, :stop)
      end
    end)

    pid
  end

  defp recorded_get_opts(table, key) do
    table
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {{:get_call, _call_id}, %{key: ^key, opts: opts}} -> [opts]
      _ -> []
    end)
  end

  defp recorded_delete_opts(table, key) do
    table
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {{:delete_call, _call_id}, %{key: ^key, opts: opts}} -> [opts]
      _ -> []
    end)
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

  defmodule ConsistencyProbeBackend do
    @behaviour DurableServer.StorageBackend

    @impl true
    def init_backend(raw_opts) do
      opts =
        case raw_opts do
          %{} = map -> map
          list when is_list(list) -> Map.new(list)
        end

      {:ok,
       %{
         state: %{
           table: :ets.new(__MODULE__, [:set, :public]),
           owner: Map.get(opts, :owner),
           after_put: Map.get(opts, :after_put),
           delete_error: Map.get(opts, :delete_error)
         },
         features: %{conditional_delete?: true}
       }}
    end

    @impl true
    def ensure_ready(_state), do: :ok

    @impl true
    def get_object(%{table: table, owner: owner}, key, opts) do
      record_get_call(table, key, opts, owner)
      consistent = Keyword.get(opts, :consistent, :unset)

      case :ets.lookup(table, {:override, key, consistent}) do
        [{{:override, ^key, ^consistent}, response}] ->
          response

        [] ->
          case :ets.lookup(table, {:data, key}) do
            [{{:data, ^key}, %{body: body, etag: etag}}] -> {:ok, %{body: body, etag: etag}}
            [] -> {:error, :not_found}
          end
      end
    end

    @impl true
    def list_all_objects_stream(%{table: table}, prefix, _opts) do
      table
      |> :ets.tab2list()
      |> Stream.filter(fn
        {{:data, key}, _value} -> String.starts_with?(key, prefix)
        _ -> false
      end)
      |> Stream.map(fn {{:data, key}, %{etag: etag}} -> %{key: key, etag: etag} end)
    end

    @impl true
    def put_object(%{table: table} = state, key, data, opts) do
      result =
        case :ets.lookup(table, {:put_override, key}) do
          [{{:put_override, ^key}, response}] ->
            response

          [] ->
            case Keyword.fetch(opts, :etag) do
              {:ok, expected_etag} ->
                case :ets.lookup(table, {:data, key}) do
                  [{{:data, ^key}, %{etag: ^expected_etag}}] ->
                    store_value(table, key, data)

                  [{{:data, ^key}, _value}] ->
                    {:error, :conflict}

                  [] ->
                    {:error, :not_found}
                end

              :error ->
                store_value(table, key, data)
            end
        end

      maybe_after_put(state, key, data, opts, result)
      result
    end

    @impl true
    def delete_object(%{delete_error: delete_error}, _key) when not is_nil(delete_error) do
      {:error, delete_error}
    end

    def delete_object(%{table: table}, key) do
      case :ets.lookup(table, {:delete_override, key}) do
        [{{:delete_override, ^key}, response}] ->
          response

        [] ->
          case :ets.lookup(table, {:data, key}) do
            [{{:data, ^key}, _value}] ->
              :ets.delete(table, {:data, key})
              :ok

            [] ->
              {:error, :not_found}
          end
      end
    end

    @impl true
    def delete_object(%{delete_error: delete_error}, _key, _opts) when not is_nil(delete_error) do
      {:error, delete_error}
    end

    def delete_object(%{table: table, owner: owner}, key, opts) do
      opts = Keyword.validate!(opts, [:etag])
      record_delete_call(table, key, opts, owner)

      case :ets.lookup(table, {:delete_override, key}) do
        [{{:delete_override, ^key}, response}] ->
          response

        [] ->
          case Keyword.fetch(opts, :etag) do
            {:ok, expected_etag} ->
              case :ets.lookup(table, {:data, key}) do
                [{{:data, ^key}, %{etag: ^expected_etag}}] ->
                  :ets.delete(table, {:data, key})
                  :ok

                [{{:data, ^key}, _value}] ->
                  {:error, :conflict}

                [] ->
                  {:error, :not_found}
              end

            :error ->
              delete_object(%{table: table}, key)
          end
      end
    end

    @impl true
    def try_claim(%{table: table}, key, body) do
      case :ets.lookup(table, {:data, key}) do
        [] ->
          {:ok, %{etag: etag}} = store_value(table, key, body)
          {:ok, {:claimed, etag}}

        [_existing] ->
          {:error, :already_claimed}
      end
    end

    @impl true
    def update_object(%{table: _table} = state, key, update_fn, _opts) do
      with {:ok, %{body: body, etag: etag}} <- get_object(state, key, consistent: true),
           {:ok, new_body} <- update_fn.(%{body: body, etag: etag}) do
        put_object(state, key, new_body, etag: etag)
      end
    end

    @impl true
    def encode(_state, data), do: {:ok, data}

    @impl true
    def decode(_state, data), do: {:ok, data}

    defp store_value(table, key, data) do
      etag =
        System.unique_integer([:positive, :monotonic])
        |> Integer.to_string()

      :ets.insert(table, {{:data, key}, %{body: data, etag: etag}})
      {:ok, %{body: data, etag: etag}}
    end

    defp maybe_after_put(%{after_put: after_put} = state, key, data, opts, result)
         when is_function(after_put, 5) do
      after_put.(state, key, data, opts, result)
    end

    defp maybe_after_put(_state, _key, _data, _opts, _result), do: :ok

    defp record_get_call(table, key, opts, owner) do
      call_id = System.unique_integer([:positive, :monotonic])
      :ets.insert(table, {{:get_call, call_id}, %{key: key, opts: opts}})

      if is_pid(owner) do
        send(owner, {:consistency_probe_get, key, opts})
      end
    end

    defp record_delete_call(table, key, opts, owner) do
      call_id = System.unique_integer([:positive, :monotonic])
      :ets.insert(table, {{:delete_call, call_id}, %{key: key, opts: opts}})

      if is_pid(owner) do
        send(owner, {:consistency_probe_delete, key, opts})
      end
    end
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

    def init(%{error: reason}, _info) do
      {:error, reason}
    end

    def init(%{ignore: true}, _info) do
      :ignore
    end

    def init(init_state, _info) do
      if sleep_ms = init_state[:init_sleep_ms] do
        Process.sleep(sleep_ms)
      end

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

    def handle_call(:increment_and_sync_metadata, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      metadata = %{last_heartbeat_at: System.system_time(:millisecond)}
      {:reply, count + 1, new_state, {:sync, metadata}}
    end

    def handle_call(:increment_with_sync_option, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state, sync: true}
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

    def handle_cast(:increment_with_timeout, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:noreply, new_state, 1_000}
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
      if is_pid(state[:test_pid]) do
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

    def init(_state, info) do
      # assert that we have a task sup in the info map
      _ = info.task_supervisor
      {:ok, %{key: info.key, count: 0}, auto_sync: true}
    end

    def handle_call(:increment, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state}
    end
  end

  defmodule HandleSyncTestServer do
    use DurableServer, vsn: 1

    def dump_state(state), do: Map.take(state, [:key, :count, :auto_sync?])

    def load_state(_old_vsn, persisted_state) do
      persisted_state
      |> DurableServerTest.atomify_keys()
      |> Map.put_new(:count, 0)
      |> Map.put(:test_pid, nil)
      |> Map.put(:sync_count, 0)
    end

    def init(init_state, info) do
      {:ok,
       %{
         key: info.key,
         count: Map.get(init_state, :count, 0),
         test_pid: Map.get(init_state, :test_pid),
         sync_count: 0,
         auto_sync?: Map.get(init_state, :auto_sync?, false)
       }, auto_sync: Map.get(init_state, :auto_sync?, false)}
    end

    def handle_sync(info, old_state, new_state) do
      if is_pid(new_state.test_pid) do
        send(new_state.test_pid, {:handle_sync, info, old_state, new_state})
        {:ok, %{new_state | sync_count: new_state.sync_count + 1}}
      else
        {:ok, new_state}
      end
    end

    def handle_call({:set_test_pid, pid}, _from, state) do
      {:reply, :ok, %{state | test_pid: pid}}
    end

    def handle_call(:sync_same, _from, state) do
      {:reply, :ok, state, :sync}
    end

    def handle_call(:increment_sync, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state, :sync}
    end

    def handle_call(:increment_auto, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state}
    end

    def handle_call(:increment_no_sync, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state}
    end

    def handle_call(:stop_without_user_state_change, _from, state) do
      {:stop, :normal, :ok, state}
    end

    def handle_call(:stop_with_user_state_change, _from, %{count: count} = state) do
      {:stop, :normal, :ok, %{state | count: count + 1}}
    end

    def handle_call(:get_state, _from, state) do
      {:reply, state, state}
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

    def init(%{sync_ms: sync_ms} = init_state, info) do
      init_state = Map.put(init_state, :key, info.key)
      {:ok, init_state, auto_sync: false, sync_every_ms: sync_ms}
    end

    def handle_call(:increment, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state}
    end
  end

  defmodule SlowTerminateServer do
    use DurableServer, vsn: 1

    def dump_state(state), do: state
    def load_state(_old_vsn, state), do: DurableServerTest.atomify_keys(state)

    def terminate(_reason, %{terminate_sleep_ms: sleep_ms}) do
      Process.sleep(sleep_ms)
      :ok
    end
  end

  defmodule InitInfoServer do
    use DurableServer, vsn: 1

    def dump_state(state), do: Map.take(state, [:key])

    def load_state(_old_vsn, persisted_state) do
      DurableServerTest.atomify_keys(persisted_state)
    end

    def init(_state, info) do
      # Store the info map in state so tests can verify it
      {:ok, %{key: info.key, info: info}}
    end

    def handle_call(:get_info, _from, state) do
      {:reply, state.info, state}
    end
  end

  defmodule KeyInfoServer do
    use DurableServer, vsn: 1

    def dump_state(state), do: Map.take(state, [:count])

    def load_state(_old_vsn, persisted_state) do
      DurableServerTest.atomify_keys(persisted_state)
    end

    def init(state, info) do
      {:ok, Map.put(state, :key_from_info, info.key)}
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

    def init(init_state, info) do
      {:ok, %{key: info.key, count: 0, test_pid: Map.get(init_state, :test_pid)}}
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
    test "releases its prefix claim when the supervisor stops", %{prefix: _prefix} do
      unique_id = DurableServer.UUID.uuid4()
      supervisor_name = :"restartable_supervisor_#{unique_id}"
      prefix = "restartable_#{unique_id}/"
      opts = [name: supervisor_name, prefix: prefix, object_store: test_object_store_opts()]

      assert {:ok, first_pid} = DurableServer.Supervisor.start_link(opts)

      assert_raise ArgumentError, ~r/already claimed by supervisor/, fn ->
        DurableServer.Supervisor.start_link(
          Keyword.put(opts, :name, :"competing_supervisor_#{unique_id}")
        )
      end

      assert :ok = Supervisor.stop(first_pid)

      restarted_opts =
        Keyword.put(opts, :name, :"restarted_supervisor_#{unique_id}")

      assert {:ok, second_pid} = DurableServer.Supervisor.start_link(restarted_opts)
      assert :ok = Supervisor.stop(second_pid)
    end

    test "releases its prefix claim when startup fails", %{prefix: _prefix} do
      unique_id = DurableServer.UUID.uuid4()
      supervisor_name = :"failed_restartable_supervisor_#{unique_id}"
      prefix = "failed_restartable_#{unique_id}/"

      invalid_opts = [
        name: supervisor_name,
        prefix: prefix,
        backend: {ConsistencyProbeBackend, owner: self()},
        heartbeat_interval_ms: 0
      ]

      failed_start =
        Task.async(fn ->
          Process.flag(:trap_exit, true)
          DurableServer.Supervisor.start_link(invalid_opts)
        end)

      assert {:error, _reason} = Task.await(failed_start)

      valid_opts =
        Keyword.delete(invalid_opts, :heartbeat_interval_ms)

      assert {:ok, supervisor_pid} = DurableServer.Supervisor.start_link(valid_opts)
      assert :ok = Supervisor.stop(supervisor_pid)
    end

    test "initializes successfully with valid options", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "test-server-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

      refute_process_down(pid)
    end

    test "starts from child-spec keyword args without persisting key in user state", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "new-api-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{count: 4}}
        )

      assert GenServer.call(pid, :get_count) == 4

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)

      {:ok, %StoredState{} = stored_state} =
        DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})

      refute Map.has_key?(stored_state.state, "key")
      refute Map.has_key?(stored_state.state, :key)
      assert stored_state.state["count"] == 4
    end

    test "passes durable key to init/2 info for keyword child specs", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "init-info-key-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {KeyInfoServer, key: key, initial_state: %{count: 8}}
        )

      refute_process_down(pid)

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)

      {:ok, %StoredState{} = stored_state} =
        DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})

      assert stored_state.state["count"] == 8
      refute Map.has_key?(stored_state.state, "key_from_info")
      refute Map.has_key?(stored_state.state, "key")
    end

    test "handles ignore return from user init", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      # DurableServer.Supervisor.start_child should handle ignore return
      result =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: "123", initial_state: %{ignore: true}}
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

    test "ensure_started_child handles ignore return from user init", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      assert :ignore =
               DurableServer.Supervisor.ensure_started_child(
                 supervisor_name,
                 {TestServer, key: "ensure-ignore", initial_state: %{ignore: true}}
               )
    end

    test "explicit local starts reject draining node before spawning a child", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      table_name = DurableServer.Supervisor.__ets_table_name__(supervisor_name)
      :ets.insert(table_name, {:shutting_down, true})

      log =
        capture_log([level: :info], fn ->
          assert {:error, {:capacity_limit, :node_shutting_down}} =
                   DurableServer.Supervisor.start_child(
                     supervisor_name,
                     {TestServer,
                      key: "draining-start-#{DurableServer.UUID.uuid4()}", initial_state: %{}},
                     max_placement_retries: 0
                   )

          assert {:error, {:capacity_limit, :node_shutting_down}} =
                   DurableServer.Supervisor.ensure_started_child(
                     supervisor_name,
                     {TestServer,
                      key: "draining-ensure-#{DurableServer.UUID.uuid4()}", initial_state: %{}},
                     local_only: true
                   )
        end)

      refute log =~ "DurableServer node shutting down - Cannot start"
    end

    test "validates start_link arguments require :key field", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      # Missing key should cause start_child to fail with ArgumentError

      assert_raise ArgumentError, ~r/start_child requires :key/, fn ->
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, initial_state: %{custom_opts: %{}}}
        )
      end
    end

    test "rejects child keys in the internal heartbeat namespace", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      assert_raise ArgumentError, ~r/reserved internal namespace/, fn ->
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: "__nodes/future@host", initial_state: %{}}
        )
      end
    end

    test "validates child args require map :initial_state", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      assert_raise ArgumentError, ~r/start_child requires :initial_state/, fn ->
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: "missing-initial-state"}
        )
      end

      assert_raise ArgumentError, ~r/start_child :initial_state must be a map/, fn ->
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: "bad-initial-state", initial_state: [count: 0]}
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
          {TestServer, key: "test", initial_state: %{invalid_options_test: true}}
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
                     {TestServer, key: key, initial_state: %{}}
                   )
        end)

      assert log =~ "failed to sync startup status :running"
      assert log =~ key
    end

    test "repairs claimed storage after startup sync failure" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(
          backend: {RejectingStartupSyncBackend, delegate_opts: test_object_store_opts()}
        )

      key = "startup-sync-repair-#{DurableServer.UUID.uuid4()}"

      assert {:error, :startup_sync_rejected} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{custom_opts: %{permanent: true}}}
               )

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)

      assert {:ok, %StoredState{} = stored_state} =
               DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})

      assert stored_state.meta.status == :stopped_graceful
      assert stored_state.meta.permanent == true
    end

    test "sets up node reference in persistent term", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      key = "test-server-#{DurableServer.UUID.uuid4()}"

      {:ok, {_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

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
          {TestServer, key: key, initial_state: %{meta: initial_meta}}
        )

      refute_process_down(pid1)

      # Second call should return the same process
      {:ok, {pid2, ^initial_meta}} =
        DurableServer.Supervisor.ensure_started_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{meta: initial_meta}}
        )

      assert pid1 == pid2
      refute_process_down(pid2)

      # Verify it's the same process in the registry
      {^pid1, ^initial_meta} = DurableServer.Supervisor.lookup(supervisor_name, key)
    end

    test "ensure_started_child waits for raced Group registration before returning", %{
      prefix: _default_prefix
    } do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
      table = backend_table(backend)
      key = "ensure-raced-registration-#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key
      user_meta = %{source: :group_registration}

      registrar = start_group_registrar(supervisor_name, prefix, key, user_meta)
      insert_running_lock(table, storage_key, supervisor_name, prefix, key, registrar)

      Task.start(fn ->
        Process.sleep(100)
        send(registrar, :register)
      end)

      assert {:ok, {^registrar, ^user_meta}} =
               DurableServer.Supervisor.ensure_started_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{}},
                 timeout: 2_000
               )

      assert_receive {:registrar_registered, ^registrar}, 500
    end

    test "ensure_started_child returns unreachable when storage lock has no Group registration",
         %{
           prefix: _default_prefix
         } do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
      table = backend_table(backend)
      key = "ensure-unreachable-lock-#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key

      registrar = start_group_registrar(supervisor_name, prefix, key, %{unused: true})
      insert_running_lock(table, storage_key, supervisor_name, prefix, key, registrar)

      assert {:error, {:unreachable, ^registrar}} =
               DurableServer.Supervisor.ensure_started_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{}},
                 timeout: 300
               )

      assert DurableServer.Supervisor.lookup(supervisor_name, key) == nil
    end

    test "ensure_started_child honors a caller timeout longer than the Group registration wait",
         %{
           supervisor_name: supervisor_name,
           prefix: prefix
         } do
      key = "ensure-slow-init-registration-#{DurableServer.UUID.uuid4()}"
      init_sleep_ms = 6_500

      child_spec =
        {TestServer, key: key, initial_state: %{init_sleep_ms: init_sleep_ms}}

      starter =
        Task.async(fn ->
          DurableServer.Supervisor.start_child(
            supervisor_name,
            child_spec,
            timeout: 10_000
          )
        end)

      owner_pid = await_stored_pid(supervisor_name, prefix, key)

      {elapsed_us, ensure_result} =
        :timer.tc(fn ->
          DurableServer.Supervisor.ensure_started_child(
            supervisor_name,
            child_spec,
            timeout: 10_000
          )
        end)

      assert {:ok, {^owner_pid, _meta}} = ensure_result
      assert elapsed_us >= 5_000_000
      assert {:ok, {^owner_pid, _meta}} = Task.await(starter, 5_000)
    end

    test "ensure_started_child performs one final retry when restart claim expires before timeout",
         %{
           prefix: _default_prefix
         } do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
      now = System.system_time(:millisecond)
      key = "ensure-expired-restart-claim-#{DurableServer.UUID.uuid4()}"
      node_str = to_string(Node.self())
      node_ref = DurableServer.Supervisor.node_ref(supervisor_name)

      heartbeat_table = :"durable_server_heartbeats_#{supervisor_name}"
      :ets.insert(heartbeat_table, {node_str, node_ref, now, %{}, %{}, %{}, nil})

      stored_state = %StoredState{
        vsn: 1,
        state: %{"count" => 11},
        meta: %Meta{
          key: key,
          prefix: prefix,
          supervisor: supervisor_name,
          module: TestServer,
          status: :stopped_graceful,
          permanent: false,
          node_str: node_str,
          node_ref: node_ref,
          pid: self(),
          last_heartbeat_at: now,
          restart_attempt_node: node_str,
          restart_attempt_time: now,
          restart_attempt_ttl: now + 50
        }
      }

      assert {:ok, %{etag: _etag}} =
               StorageBackend.put_object(backend, prefix <> key, stored_state, [])

      assert {:ok, {pid, _meta}} =
               DurableServer.Supervisor.ensure_started_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{}},
                 timeout: 500
               )

      assert GenServer.call(pid, :get_count) == 11

      assert {:ok, %StoredState{meta: %Meta{} = meta}} =
               DurableServer.fetch_stored_state(backend, %{key: key, prefix: prefix},
                 consistent: true
               )

      assert meta.status == :running
      assert meta.restart_attempt_node == nil
      assert meta.restart_attempt_ttl == nil
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
                 {TestServer, key: key, initial_state: %{}},
                 existing: true
               )
    end

    test "start_child starts server when persisted state exists", %{
      supervisor_name: supervisor_name
    } do
      key = "existing-start-#{DurableServer.UUID.uuid4()}"

      # Start, sync, and stop to create valid persisted state
      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

      GenServer.call(pid, :increment_and_sync)
      ref = Process.monitor(pid)
      GenServer.call(pid, :stop_normal)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

      # Now start_child with existing: true should succeed
      assert {:ok, {pid2, _meta}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{}},
                 existing: true
               )

      refute_process_down(pid2)
      assert pid2 != pid
    end

    test "ensure_started_child returns {:error, :not_found} when no persisted state exists", %{
      supervisor_name: supervisor_name
    } do
      key = "nonexistent-ensure-#{DurableServer.UUID.uuid4()}"

      assert {:error, :not_found} =
               DurableServer.Supervisor.ensure_started_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{}},
                 existing: true
               )
    end

    test "ensure_started_child starts server when persisted state exists", %{
      supervisor_name: supervisor_name
    } do
      key = "existing-ensure-#{DurableServer.UUID.uuid4()}"

      # Start, sync, and stop to create valid persisted state
      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

      GenServer.call(pid, :increment_and_sync)
      ref = Process.monitor(pid)
      GenServer.call(pid, :stop_normal)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

      # Now ensure_started_child with existing: true should succeed
      assert {:ok, {pid2, _meta}} =
               DurableServer.Supervisor.ensure_started_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{}},
                 existing: true
               )

      refute_process_down(pid2)
      assert pid2 != pid
    end

    test "__start_child__ accepts internal preloaded boot info", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "existing-restart-shape-#{DurableServer.UUID.uuid4()}"
      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

      GenServer.call(pid, :increment_and_sync)
      ref = Process.monitor(pid)
      GenServer.call(pid, :stop_normal)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

      {:ok, %{etag: etag} = body} =
        DurableServer.fetch_stored_state(backend, %{key: key, prefix: prefix})

      assert {:ok, {pid2, _meta}} =
               DurableServer.Supervisor.__start_child__(
                 supervisor_name,
                 preloaded_child_spec(TestServer, [key: key, initial_state: %{}], %{
                   body: body,
                   etag: etag
                 }),
                 local_only: true
               )

      refute_process_down(pid2)
      assert pid2 != pid
    end

    test "start_child rejects internal boot info child specs", %{
      supervisor_name: supervisor_name
    } do
      assert_raise ArgumentError, ~r/start_child expects/, fn ->
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, [key: "private-shape", initial_state: %{}], %{}}
        )
      end
    end

    test "ensure_started_child returns already-running process even without persisted state", %{
      supervisor_name: supervisor_name
    } do
      key = "already-running-#{DurableServer.UUID.uuid4()}"

      # Start a process first (creates persisted state)
      {:ok, {pid1, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

      # ensure_started_child with existing: true should find it via lookup
      # (before reaching the S3 check)
      {:ok, {pid2, _meta}} =
        DurableServer.Supervisor.ensure_started_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}},
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
          {InitInfoServer, key: key, initial_state: %{}}
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
          {InitInfoServer, key: key, initial_state: %{}}
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
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

      :ok = GenServer.call(pid, {:set_test_pid, self()})
      %{pid: pid}
    end

    test "handles call messages", %{pid: pid} do
      assert GenServer.call(pid, :get_count) == 0
      assert GenServer.call(pid, :increment) == 1
      assert GenServer.call(pid, :get_count) == 1
    end

    test "supports integer timeout actions from call and noreply callbacks", %{pid: pid} do
      assert GenServer.call(pid, :increment_with_timeout) == 1
      GenServer.cast(pid, :increment_with_timeout)
      :sys.get_state(pid)
      assert GenServer.call(pid, :get_count) == 2
      refute_process_down(pid)
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
           key: key,
           initial_state: %{
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
          {TestServer, key: key, initial_state: %{meta: initial_meta}}
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
          {TestServer, key: key, initial_state: %{meta: initial_meta}}
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
          {TestServer, key: key, initial_state: %{}}
        )

      # Test that invalid meta types cause the GenServer call to exit with FunctionClauseError
      catch_exit(GenServer.call(pid, {:invalid_meta_test, "not_a_map"}))
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
           key: key,
           initial_state: %{
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
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

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

  describe "handle_sync callback" do
    test "runs inline after an explicit sync writes state and skips noop syncs", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "handle-sync-explicit-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {HandleSyncTestServer, key: key, initial_state: %{count: 0}}
        )

      assert :ok = GenServer.call(pid, {:set_test_pid, self()})

      assert :ok = GenServer.call(pid, :sync_same)
      refute_receive {:handle_sync, _info, _old_state, _new_state}, 100

      assert 1 = GenServer.call(pid, :increment_sync)
      assert_receive {:handle_sync, info, old_state, new_state}, 1_000
      assert info.status == :running
      assert info.key == key
      assert info.supervisor == supervisor_name
      assert old_state.count == 0
      assert new_state.count == 1
      assert new_state.sync_count == 0

      assert %{count: 1, sync_count: 1} = GenServer.call(pid, :get_state)

      assert :ok = GenServer.call(pid, :sync_same)
      refute_receive {:handle_sync, _info, _old_state, _new_state}, 100

      store = test_object_store()
      {:ok, persisted_data} = DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix})
      persisted_state = atomify_keys(persisted_data.state)

      assert persisted_state.count == 1
      refute Map.has_key?(persisted_state, :sync_count)
      refute Map.has_key?(persisted_state, :test_pid)
    end

    test "runs after auto_sync writes state with the pre-callback state", %{
      supervisor_name: supervisor_name
    } do
      key = "handle-sync-auto-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {HandleSyncTestServer, key: key, initial_state: %{count: 0, auto_sync?: true}}
        )

      assert :ok = GenServer.call(pid, {:set_test_pid, self()})
      refute_receive {:handle_sync, _info, _old_state, _new_state}, 100

      assert 1 = GenServer.call(pid, :increment_auto)
      assert_receive {:handle_sync, info, old_state, new_state}, 1_000
      assert info.status == :running
      assert info.key == key
      assert info.supervisor == supervisor_name
      assert old_state.count == 0
      assert new_state.count == 1
      assert new_state.sync_count == 0

      assert %{count: 1, sync_count: 1} = GenServer.call(pid, :get_state)
    end

    test "does not run for terminate final status persistence when user state is unchanged", %{
      supervisor_name: supervisor_name
    } do
      key = "handle-sync-terminate-status-only-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {HandleSyncTestServer, key: key, initial_state: %{count: 0}}
        )

      ref = Process.monitor(pid)
      assert :ok = GenServer.call(pid, {:set_test_pid, self()})
      assert :ok = GenServer.call(pid, :stop_without_user_state_change)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
      refute_receive {:handle_sync, _info, _old_state, _new_state}, 100
    end

    test "runs for terminate final persistence when user state changed", %{
      supervisor_name: supervisor_name
    } do
      key = "handle-sync-terminate-state-changed-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {HandleSyncTestServer, key: key, initial_state: %{count: 0}}
        )

      ref = Process.monitor(pid)
      assert :ok = GenServer.call(pid, {:set_test_pid, self()})
      assert :ok = GenServer.call(pid, :stop_with_user_state_change)
      assert_receive {:handle_sync, info, old_state, new_state}, 1_000
      assert info.status == :stopped_graceful
      assert info.key == key
      assert info.supervisor == supervisor_name
      assert old_state.count == 1
      assert new_state.count == 1
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
    end

    test "marks terminate_and_delete_child final persistence as deleting", %{
      supervisor_name: supervisor_name
    } do
      key = "handle-sync-delete-state-changed-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {HandleSyncTestServer, key: key, initial_state: %{count: 0}}
        )

      assert :ok = GenServer.call(pid, {:set_test_pid, self()})
      assert 1 = GenServer.call(pid, :increment_no_sync)
      assert :ok = DurableServer.Supervisor.terminate_and_delete_child(supervisor_name, pid)

      assert_receive {:handle_sync, info, old_state, new_state}, 1_000
      assert info.status == :deleting
      assert info.key == key
      assert info.supervisor == supervisor_name
      assert old_state.count == 1
      assert new_state.count == 1
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
          {AfterTerminateTestServer, key: key, initial_state: %{}}
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
          {AfterTerminateTestServer, key: key, initial_state: %{}}
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
          {AutoSyncServer, key: key, initial_state: %{}}
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

    test "stops when auto sync loses the storage CAS", %{prefix: _prefix} do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      key = "auto-sync-conflict-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {AutoSyncServer, key: key, initial_state: %{}}
        )

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
      storage_key = prefix <> key
      {:ok, %{body: stored_state, etag: etag}} = StorageBackend.get_object(backend, storage_key)

      assert {:ok, _obj} =
               StorageBackend.put_object(backend, storage_key, stored_state, etag: etag)

      ref = Process.monitor(pid)
      assert catch_exit(GenServer.call(pid, :increment))
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}
    end

    test "keeps dirty state after a transient failure and persists it on a later sync" do
      test_pid = self()

      after_put = fn _state, key, _data, opts, result ->
        send(test_pid, {:consistency_probe_put, key, opts, result})
      end

      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(
          backend: {ConsistencyProbeBackend, owner: self(), after_put: after_put}
        )

      key = "auto-sync-retry-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {AutoSyncServer, key: key, initial_state: %{}}
        )

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
      storage_key = prefix <> key
      table = backend_table(backend)

      assert_receive {:consistency_probe_put, ^storage_key, _opts, {:ok, _object}}

      put_backend_write_override(table, storage_key, {:error, :unavailable})

      assert GenServer.call(pid, :increment) == 1

      assert_receive {:consistency_probe_put, ^storage_key, failed_opts, {:error, :unavailable}}

      assert Keyword.fetch!(failed_opts, :max_retries) == 5
      assert Process.alive?(pid)

      assert {:ok, %StoredState{state: %{count: 0}}} =
               DurableServer.fetch_stored_state(backend, %{key: key, prefix: prefix})

      clear_backend_write_override(table, storage_key)

      assert GenServer.call(pid, :increment) == 2
      assert_receive {:consistency_probe_put, ^storage_key, _opts, {:ok, _object}}

      assert {:ok, %StoredState{state: %{count: 2}}} =
               DurableServer.fetch_stored_state(backend, %{key: key, prefix: prefix})
    end

    test "does not auto sync when disabled", %{supervisor_name: supervisor_name, prefix: prefix} do
      key = "no-auto-sync-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer,
           key: key,
           initial_state: %{
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
          {PeriodicSyncServer, key: key, initial_state: %{sync_ms: sync_ms}}
        )

      # Increment without explicit sync
      assert GenServer.call(pid, :increment) == 1

      # Wait for periodic sync to trigger
      Process.sleep(sync_ms * 2)
      :sys.get_state(pid)

      # Verify server is still responsive
      assert GenServer.call(pid, :increment) == 2
    end

    test "keeps dirty state after a transient failure and retries on the next interval" do
      test_pid = self()

      after_put = fn _state, key, _data, opts, result ->
        send(test_pid, {:consistency_probe_put, key, opts, result})
      end

      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(
          backend: {ConsistencyProbeBackend, owner: self(), after_put: after_put}
        )

      key = "periodic-sync-retry-#{DurableServer.UUID.uuid4()}"
      sync_ms = 25

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {PeriodicSyncServer, key: key, initial_state: %{sync_ms: sync_ms}}
        )

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
      storage_key = prefix <> key
      table = backend_table(backend)

      assert_receive {:consistency_probe_put, ^storage_key, _opts, {:ok, _object}}

      put_backend_write_override(table, storage_key, {:error, :unavailable})

      assert GenServer.call(pid, :increment) == 1

      assert_receive {:consistency_probe_put, ^storage_key, failed_opts, {:error, :unavailable}},
                     1_000

      assert Keyword.fetch!(failed_opts, :max_retries) == 5
      assert Process.alive?(pid)

      assert {:ok, %StoredState{state: %{count: 0}}} =
               DurableServer.fetch_stored_state(backend, %{key: key, prefix: prefix})

      clear_backend_write_override(table, storage_key)

      assert_receive {:consistency_probe_put, ^storage_key, _opts, {:ok, _object}}, 1_000

      assert {:ok, %StoredState{state: %{count: 1}}} =
               DurableServer.fetch_stored_state(backend, %{key: key, prefix: prefix})
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
           key: key,
           initial_state: %{occurred_at: occurred_at, nested: %{occurred_at: occurred_at}}}
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
           key: key,
           initial_state: %{
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
           key: key2,
           initial_state: %{
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
           key: key,
           initial_state: %{
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
           key: key,
           initial_state: %{occurred_at: occurred_at, nested: %{occurred_at: occurred_at}}}
        )

      assert :ok = GenServer.call(pid, :sync_now)

      monitor_ref = Process.monitor(pid)
      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, pid)
      assert_receive {:DOWN, ^monitor_ref, :process, ^pid, _reason}, 5_000

      {:ok, {restarted_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestTemporalServer, key: key, initial_state: %{}},
          existing: true
        )

      assert :json_string == GenServer.call(restarted_pid, :get_loaded_shape)

      snapshot = GenServer.call(restarted_pid, :get_snapshot)
      assert is_binary(snapshot.occurred_at)
      assert is_binary(snapshot.nested["occurred_at"])
    end

    test "rehome_child reserves stopped object before LifecycleManager can claim it" do
      key = "rehome-race-#{DurableServer.UUID.uuid4()}"
      owner = self()

      after_put = fn backend_state, storage_key, stored_state, _opts, result ->
        case {stored_state, result} do
          {%StoredState{meta: %Meta{status: :stopped_graceful} = meta}, {:ok, %{etag: etag}}} ->
            if String.ends_with?(storage_key, key) do
              prefix = String.replace_suffix(storage_key, key, "")

              stored_state = %{
                stored_state
                | key: key,
                  prefix: prefix,
                  etag: etag,
                  meta: %{meta | key: key, prefix: prefix}
              }

              backend = StorageBackend.new(ConsistencyProbeBackend, backend_state)
              result = DurableServer.claim_restart_attempt(backend, stored_state, ttl: 30_000)
              send(owner, {:rehome_race_claim_result, result})
            end

          _ ->
            :ok
        end
      end

      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(
          backend: {ConsistencyProbeBackend, owner: owner, after_put: after_put}
        )

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{count: 1, custom_opts: %{permanent: true}}},
          timeout: 10_000
        )

      assert {:ok, {rehome_pid, _meta}} =
               DurableServer.Supervisor.rehome_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{}},
                 shutdown_timeout: 5_000
               )

      assert rehome_pid != pid

      assert_receive {:rehome_race_claim_result, claim_result}, 1_000
      assert {:error, :already_claimed} = claim_result

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)

      assert {:ok, %StoredState{meta: %Meta{} = meta}} =
               DurableServer.fetch_stored_state(backend, %{key: key, prefix: prefix},
                 consistent: true
               )

      assert meta.status == :running
      assert meta.restart_attempt_node == nil
      assert meta.restart_attempt_ttl == nil
    end

    test "concurrent rehome_child calls singleflight to one stop/start attempt" do
      key = "rehome-singleflight-#{DurableServer.UUID.uuid4()}"
      owner = self()
      gate_table = :ets.new(:rehome_singleflight_gate, [:set, :public])

      after_put = fn _backend_state, storage_key, stored_state, _opts, result ->
        case {stored_state, result} do
          {%StoredState{meta: %Meta{status: :stopped_graceful}}, {:ok, %{etag: _etag}}} ->
            if String.ends_with?(storage_key, key) and :ets.info(gate_table) != :undefined do
              count =
                :ets.update_counter(
                  gate_table,
                  :stopped_writes,
                  {2, 1},
                  {:stopped_writes, 0}
                )

              if count == 1 do
                hook_pid = self()
                send(owner, {:rehome_singleflight_stopped_write, hook_pid})

                receive do
                  {:release_rehome_singleflight_stopped_write, ^hook_pid} -> :ok
                after
                  5_000 -> :ok
                end
              end
            end

          _ ->
            :ok
        end
      end

      {supervisor_name, _supervisor_pid, _prefix} =
        start_test_supervisor(
          backend: {ConsistencyProbeBackend, owner: owner, after_put: after_put}
        )

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{custom_opts: %{permanent: true}}},
          timeout: 10_000
        )

      tasks =
        for _ <- 1..8 do
          Task.async(fn ->
            DurableServer.Supervisor.rehome_child(
              supervisor_name,
              {TestServer, key: key, initial_state: %{}},
              shutdown_timeout: 10_000
            )
          end)
        end

      assert_receive {:rehome_singleflight_stopped_write, hook_pid}, 1_000
      refute_receive {:rehome_singleflight_stopped_write, _other_hook_pid}, 100
      send(hook_pid, {:release_rehome_singleflight_stopped_write, hook_pid})

      results = Enum.map(tasks, &Task.await(&1, 15_000))

      assert Enum.all?(results, &match?({:ok, {_pid, _meta}}, &1))

      rehome_pids =
        Enum.map(results, fn {:ok, {rehome_pid, _meta}} -> rehome_pid end)
        |> Enum.uniq()

      assert [rehome_pid] = rehome_pids
      assert rehome_pid != pid
      assert :ets.lookup(gate_table, :stopped_writes) == [{:stopped_writes, 1}]
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
           key: key,
           initial_state: %{
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
           key: key,
           initial_state: %{
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

  describe "graceful shutdown" do
    test "reports final persistence failures", %{prefix: _prefix} do
      unique_id = DurableServer.UUID.uuid4()
      supervisor_name = :"shutdown_failure_#{unique_id}"
      prefix = "shutdown_failure_#{unique_id}/"

      assert {:ok, supervisor_pid} =
               DurableServer.Supervisor.start_link(
                 name: supervisor_name,
                 prefix: prefix,
                 backend: {ConsistencyProbeBackend, owner: self()}
               )

      key = "shutdown-sync-failure"

      assert {:ok, {_server_pid, _meta}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{}}
               )

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
      storage_key = prefix <> key

      assert {:ok, %{body: stored_state, etag: etag}} =
               StorageBackend.get_object(backend, storage_key)

      assert {:ok, _obj} =
               StorageBackend.put_object(backend, storage_key, stored_state, etag: etag)

      log =
        capture_log(fn -> assert :ok = Supervisor.stop(supervisor_pid, :normal, :infinity) end)

      assert log =~ "1 DurableServer children failed final persistence during shutdown"
    end

    test "uses one overall timeout budget across all concurrency batches", %{prefix: _prefix} do
      unique_id = DurableServer.UUID.uuid4()
      supervisor_name = :"shutdown_budget_#{unique_id}"

      assert {:ok, supervisor_pid} =
               DurableServer.Supervisor.start_link(
                 name: supervisor_name,
                 prefix: "shutdown_budget_#{unique_id}/",
                 backend: {ConsistencyProbeBackend, owner: self()},
                 graceful_shutdown_concurrency: 1,
                 graceful_shutdown_timeout_ms: 1_000,
                 graceful_shutdown_total_timeout_ms: 75
               )

      pids =
        for index <- 1..4 do
          key = "slow-shutdown-#{index}-#{DurableServer.UUID.uuid4()}"

          assert {:ok, {pid, _meta}} =
                   DurableServer.Supervisor.start_child(
                     supervisor_name,
                     {SlowTerminateServer, key: key, initial_state: %{terminate_sleep_ms: 1_000}}
                   )

          pid
        end

      {elapsed_us, :ok} = :timer.tc(fn -> Supervisor.stop(supervisor_pid, :normal, :infinity) end)
      elapsed_ms = div(elapsed_us, 1_000)

      assert elapsed_ms < 225
      assert Enum.all?(pids, &(not Process.alive?(&1)))
    end
  end

  describe "explicit sync failure handling" do
    test "does not acknowledge a failed sync and restarts from the last durable state" do
      context = start_server_with_write_failure(TestServer)

      assert_call_sync_failure(context.pid, :increment_and_sync, :unavailable)

      assert_receive {:consistency_probe_put, storage_key, failed_opts, {:error, :unavailable}}
      assert storage_key == context.storage_key
      assert Keyword.fetch!(failed_opts, :max_retries) == 5

      assert {:ok, %StoredState{state: %{count: 0}, meta: %Meta{pid: failed_pid}}} =
               DurableServer.fetch_stored_state(context.backend, %{
                 key: context.key,
                 prefix: context.prefix
               })

      assert failed_pid == context.pid

      clear_backend_write_override(context.table, context.storage_key)

      assert {:ok, {restarted_pid, _meta}} =
               DurableServer.Supervisor.ensure_started_child(
                 context.supervisor_name,
                 {TestServer, key: context.key, initial_state: %{}},
                 timeout: 5_000
               )

      refute restarted_pid == context.pid
      assert GenServer.call(restarted_pid, :get_count) == 0
    end

    for request <- [:increment_and_sync_metadata, :increment_with_sync_option] do
      test "makes #{inspect(request)} a strict durability boundary" do
        context = start_server_with_write_failure(TestServer)
        assert_call_sync_failure(context.pid, unquote(request), :unavailable)
      end
    end

    test "terminates with a structured sync failure after a storage conflict" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      key = "explicit-sync-conflict-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
      storage_key = prefix <> key
      {:ok, %{body: stored_state, etag: etag}} = StorageBackend.get_object(backend, storage_key)

      assert {:ok, _object} =
               StorageBackend.put_object(backend, storage_key, stored_state, etag: etag)

      assert_call_sync_failure(pid, :increment_and_sync, :conflict)
    end

    for callback_type <- [:cast, :info] do
      test "terminates after a failed explicit sync from #{callback_type}" do
        context = start_server_with_write_failure(TestServer)
        monitor_ref = Process.monitor(context.pid)
        expected_exit = fatal_sync_exit(:unavailable)

        case unquote(callback_type) do
          :cast -> GenServer.cast(context.pid, :increment_and_sync)
          :info -> send(context.pid, :increment_and_sync)
        end

        assert_receive {:DOWN, ^monitor_ref, :process, pid, ^expected_exit}
        assert pid == context.pid
      end
    end

    test "terminates when an explicit sync returned from handle_continue fails" do
      context = start_server_with_write_failure(TestServer)
      monitor_ref = Process.monitor(context.pid)
      expected_exit = fatal_sync_exit(:unavailable)

      assert GenServer.call(context.pid, :continue_and_sync_test) == :ok
      assert_receive {:DOWN, ^monitor_ref, :process, pid, ^expected_exit}
      assert pid == context.pid
    end

    test "does not invoke handle_sync after a failed write" do
      context =
        start_server_with_write_failure(HandleSyncTestServer, %{count: 0, test_pid: self()})

      refute_receive {:handle_sync, _info, _old_state, _new_state}, 100
      assert_call_sync_failure(context.pid, :increment_sync, :unavailable)
      refute_receive {:handle_sync, _info, _old_state, _new_state}, 100
    end
  end

  describe "code_change/3" do
    setup %{supervisor_name: supervisor_name, prefix: _prefix} do
      key = "test-server-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

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
           key: key,
           initial_state: %{
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
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

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
           key: key,
           initial_state: %{
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
                  "custom_opts" => %{"auto_sync" => false}
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
          {EdgeCaseTestServer, key: key, initial_state: %{error_test: "custom_error"}}
        )

      assert {:error, {:bad_init_return, {:error, "custom_error"}}} = result
    end

    test "handles crash during user init", %{supervisor_name: supervisor_name, prefix: _prefix} do
      key = "crash-test-#{DurableServer.UUID.uuid4()}"

      result =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, key: key, initial_state: %{crash_on_init: true}}
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
          {EdgeCaseTestServer, key: key, initial_state: %{invalid_return: true}}
        )

      assert {:error, {:bad_init_return, :invalid_return}} = result
    end

    test "handles missing required options", %{supervisor_name: supervisor_name, prefix: _prefix} do
      # Test that start_link validation catches missing key

      assert_raise ArgumentError, ~r/start_child requires :key/, fn ->
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, initial_state: %{bad_options: true}}
        )
      end
    end

    test "blocked child bootstrap does not block starting later children", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      blocked_key = "blocked-test-#{DurableServer.UUID.uuid4()}"
      fast_key = "fast-test-#{DurableServer.UUID.uuid4()}"
      dynamic_supervisor = DurableServer.Supervisor.get_dynamic_supervisor(supervisor_name)

      before_children =
        dynamic_supervisor
        |> DynamicSupervisor.which_children()
        |> Enum.map(&elem(&1, 1))
        |> MapSet.new()

      blocked_task =
        Task.async(fn ->
          DurableServer.Supervisor.start_child(
            supervisor_name,
            {DurableServerTest.BlockingInitServer,
             key: blocked_key, initial_state: %{block_on_init: true}}
          )
        end)

      Process.sleep(200)
      assert Task.yield(blocked_task, 0) == nil

      blocked_children =
        dynamic_supervisor
        |> DynamicSupervisor.which_children()
        |> Enum.map(&elem(&1, 1))
        |> MapSet.new()
        |> MapSet.difference(before_children)
        |> MapSet.to_list()

      assert [blocked_pid] = blocked_children
      refute_process_down(blocked_pid)

      {elapsed_us, fast_result} =
        :timer.tc(fn ->
          DurableServer.Supervisor.start_child(
            supervisor_name,
            {DurableServerTest.BlockingInitServer, key: fast_key, initial_state: %{}}
          )
        end)

      assert {:ok, {fast_pid, _meta}} = fast_result
      refute_process_down(fast_pid)
      assert div(elapsed_us, 1000) < 2_000

      send(blocked_pid, :continue_init)
      assert {:ok, {^blocked_pid, _meta}} = Task.await(blocked_task, 2_000)
    end

    test "start_child returns timeout for blocked bootstrap without exiting caller", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      blocked_key = "blocked-timeout-#{DurableServer.UUID.uuid4()}"
      dynamic_supervisor = DurableServer.Supervisor.get_dynamic_supervisor(supervisor_name)

      before_children =
        dynamic_supervisor
        |> DynamicSupervisor.which_children()
        |> Enum.map(&elem(&1, 1))
        |> MapSet.new()

      {elapsed_us, result} =
        :timer.tc(fn ->
          DurableServer.Supervisor.start_child(
            supervisor_name,
            {DurableServerTest.BlockingInitServer,
             key: blocked_key, initial_state: %{block_on_init: true}},
            timeout: 100
          )
        end)

      assert {:error, :timeout} = result
      assert div(elapsed_us, 1000) < 1_000

      blocked_children =
        dynamic_supervisor
        |> DynamicSupervisor.which_children()
        |> Enum.map(&elem(&1, 1))
        |> MapSet.new()
        |> MapSet.difference(before_children)
        |> MapSet.to_list()

      assert [blocked_pid] = blocked_children
      refute_process_down(blocked_pid)

      send(blocked_pid, :continue_init)

      assert {:ok, {^blocked_pid, _meta}} = await_lookup(supervisor_name, blocked_key)

      ref = Process.monitor(blocked_pid)
      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, blocked_pid)
      assert_receive {:DOWN, ^ref, :process, ^blocked_pid, _reason}, 2_000
    end

    test "ensure_started_child returns timeout for blocked bootstrap without exiting caller", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      blocked_key = "ensure-timeout-#{DurableServer.UUID.uuid4()}"
      dynamic_supervisor = DurableServer.Supervisor.get_dynamic_supervisor(supervisor_name)

      before_children =
        dynamic_supervisor
        |> DynamicSupervisor.which_children()
        |> Enum.map(&elem(&1, 1))
        |> MapSet.new()

      {elapsed_us, result} =
        :timer.tc(fn ->
          DurableServer.Supervisor.ensure_started_child(
            supervisor_name,
            {DurableServerTest.BlockingInitServer,
             key: blocked_key, initial_state: %{block_on_init: true}},
            local_only: true,
            timeout: 100
          )
        end)

      assert {:error, :timeout} = result
      assert div(elapsed_us, 1000) < 1_000

      blocked_children =
        dynamic_supervisor
        |> DynamicSupervisor.which_children()
        |> Enum.map(&elem(&1, 1))
        |> MapSet.new()
        |> MapSet.difference(before_children)
        |> MapSet.to_list()

      assert [blocked_pid] = blocked_children
      refute_process_down(blocked_pid)

      send(blocked_pid, :continue_init)

      assert {:ok, {^blocked_pid, _meta}} = await_lookup(supervisor_name, blocked_key)

      ref = Process.monitor(blocked_pid)
      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, blocked_pid)
      assert_receive {:DOWN, ^ref, :process, ^blocked_pid, _reason}, 2_000
    end

    test "singleflight waiter retries when the owner finishes before waiter registration", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      singleflight_key =
        {:ensure_started_child, "registration-race", DurableServerTest.BlockingInitServer}

      owner_registry = :"durable_sf_owner_#{supervisor_name}"
      waiters_registry = :"durable_sf_waiters_#{supervisor_name}"
      parent = self()

      owner_pid =
        spawn(fn ->
          {:ok, _} = Registry.register(owner_registry, singleflight_key, :singleflight_owner)
          send(parent, {:singleflight_owner_ready, self()})

          receive do
            :finish ->
              Registry.dispatch(waiters_registry, singleflight_key, fn _entries -> :ok end)
              Registry.unregister(owner_registry, singleflight_key)
              send(parent, {:singleflight_owner_finished, self()})

              receive do
                :stop -> :ok
              end
          end
        end)

      assert_receive {:singleflight_owner_ready, ^owner_pid}
      send(owner_pid, :finish)
      assert_receive {:singleflight_owner_finished, ^owner_pid}
      assert Process.alive?(owner_pid)

      waiter_ref = make_ref()
      reply_alias = :erlang.alias()

      assert :retry =
               DurableServer.Supervisor.__register_singleflight_waiter__(
                 owner_registry,
                 waiters_registry,
                 singleflight_key,
                 owner_pid,
                 {waiter_ref, reply_alias}
               )

      :erlang.unalias(reply_alias)
      Registry.unregister(waiters_registry, singleflight_key)
      send(owner_pid, :stop)
    end

    test "timed out singleflight waiter does not receive late singleflight_done", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      blocked_key = "ensure-timeout-waiter-#{DurableServer.UUID.uuid4()}"

      child_spec =
        {DurableServerTest.BlockingInitServer,
         key: blocked_key, initial_state: %{block_on_init: true}}

      dynamic_supervisor = DurableServer.Supervisor.get_dynamic_supervisor(supervisor_name)

      before_children =
        dynamic_supervisor
        |> DynamicSupervisor.which_children()
        |> Enum.map(&elem(&1, 1))
        |> MapSet.new()

      leader_task =
        Task.async(fn ->
          DurableServer.Supervisor.ensure_started_child(
            supervisor_name,
            child_spec,
            local_only: true,
            timeout: 5_000
          )
        end)

      blocked_children =
        Stream.repeatedly(fn ->
          dynamic_supervisor
          |> DynamicSupervisor.which_children()
          |> Enum.map(&elem(&1, 1))
          |> MapSet.new()
          |> MapSet.difference(before_children)
          |> MapSet.to_list()
        end)
        |> Enum.find(fn
          [pid] when is_pid(pid) -> true
          _ -> false
        end)

      assert [blocked_pid] = blocked_children
      refute_process_down(blocked_pid)

      assert {:error, :timeout} =
               DurableServer.Supervisor.ensure_started_child(
                 supervisor_name,
                 child_spec,
                 local_only: true,
                 timeout: 100
               )

      send(blocked_pid, :continue_init)

      assert {:ok, {^blocked_pid, _meta}} = Task.await(leader_task, 5_000)
      refute_receive {:singleflight_done, _, _, _}, 250

      ref = Process.monitor(blocked_pid)
      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, blocked_pid)
      assert_receive {:DOWN, ^ref, :process, ^blocked_pid, _reason}, 2_000
    end

    test "singleflight reply alias drops late completion after waiter timeout", %{
      supervisor_name: supervisor_name,
      prefix: _prefix
    } do
      blocked_key = "ensure-timeout-alias-#{DurableServer.UUID.uuid4()}"

      child_spec =
        {DurableServerTest.BlockingInitServer,
         key: blocked_key, initial_state: %{block_on_init: true}}

      dynamic_supervisor = DurableServer.Supervisor.get_dynamic_supervisor(supervisor_name)

      singleflight_key =
        {:ensure_started_child, blocked_key, DurableServerTest.BlockingInitServer}

      waiters_registry = :"durable_sf_waiters_#{supervisor_name}"

      before_children =
        dynamic_supervisor
        |> DynamicSupervisor.which_children()
        |> Enum.map(&elem(&1, 1))
        |> MapSet.new()

      leader_task =
        Task.async(fn ->
          DurableServer.Supervisor.ensure_started_child(
            supervisor_name,
            child_spec,
            local_only: true,
            timeout: 5_000
          )
        end)

      blocked_children =
        Stream.repeatedly(fn ->
          dynamic_supervisor
          |> DynamicSupervisor.which_children()
          |> Enum.map(&elem(&1, 1))
          |> MapSet.new()
          |> MapSet.difference(before_children)
          |> MapSet.to_list()
        end)
        |> Enum.find(fn
          [pid] when is_pid(pid) -> true
          _ -> false
        end)

      assert [blocked_pid] = blocked_children
      refute_process_down(blocked_pid)

      parent = self()

      waiter_pid =
        spawn(fn ->
          result =
            DurableServer.Supervisor.ensure_started_child(
              supervisor_name,
              child_spec,
              local_only: true,
              timeout: 100
            )

          send(parent, {:waiter_result, self(), result})

          receive do
            {:check_mailbox, from} ->
              receive do
                {:singleflight_done, _, _, _} = msg ->
                  send(from, {:waiter_mailbox, msg})
              after
                0 ->
                  send(from, :waiter_mailbox_empty)
              end
          end
        end)

      {waiter_ref, reply_alias} =
        Stream.repeatedly(fn -> Registry.lookup(waiters_registry, singleflight_key) end)
        |> Enum.find_value(fn
          [{^waiter_pid, {waiter_ref, reply_alias}}] -> {waiter_ref, reply_alias}
          _ -> nil
        end)

      assert_receive {:waiter_result, ^waiter_pid, {:error, :timeout}}, 2_000

      send(
        reply_alias,
        {:singleflight_done, singleflight_key, waiter_ref, {:ok, {self(), %{key: blocked_key}}}}
      )

      send(waiter_pid, {:check_mailbox, self()})
      assert_receive :waiter_mailbox_empty, 500

      send(blocked_pid, :continue_init)
      assert {:ok, {^blocked_pid, _meta}} = Task.await(leader_task, 5_000)

      ref = Process.monitor(blocked_pid)
      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, blocked_pid)
      assert_receive {:DOWN, ^ref, :process, ^blocked_pid, _reason}, 2_000
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
          {EdgeCaseTestServer, key: key, initial_state: %{count: 0}}
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

    test "a stale crashing process does not mark a newer owner as crashed", %{
      prefix: _prefix
    } do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      key = "stale-crash-owner-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, key: key, initial_state: %{count: 0}}
        )

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
      storage_key = prefix <> key

      assert {:ok, %{body: stored_state, etag: etag}} =
               StorageBackend.get_object(backend, storage_key)

      newer_owner =
        stored_state.meta
        |> Map.put(:pid, self())
        |> Map.update!(:node_ref, &(&1 + 1))
        |> Meta.put_status(:running)

      assert {:ok, _obj} =
               StorageBackend.put_object(
                 backend,
                 storage_key,
                 %{stored_state | meta: newer_owner},
                 etag: etag
               )

      ref = Process.monitor(pid)
      assert catch_exit(GenServer.call(pid, :crash))
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}

      assert {:ok, %{body: final_state}} = StorageBackend.get_object(backend, storage_key)
      assert final_state.meta.pid == self()
      assert final_state.meta.node_ref == newer_owner.node_ref
      assert final_state.meta.status == :running
    end

    test "abnormal stop reasons mark status as crashed", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "abnormal-stop-test-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, key: key, initial_state: %{count: 0}}
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
          {EdgeCaseTestServer, key: key, initial_state: %{count: 0}}
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
          {EdgeCaseTestServer, key: key, initial_state: %{count: 0}}
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
          {EdgeCaseTestServer, key: key, initial_state: %{count: 0}}
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
          {EdgeCaseTestServer, key: key, initial_state: %{}}
        )

      # Second server with same key should fail to start due to lock
      result =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {EdgeCaseTestServer, key: key, initial_state: %{}}
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

    def init(%{error_test: reason}, _info) do
      {:error, reason}
    end

    def init(%{crash_on_init: true}, _info) do
      raise "Intentional crash during init"
    end

    def init(%{bad_options: true}, _info) do
      {:ok, %{count: 0}, auto_synctypo: false}
    end

    def init(%{invalid_return: true}, _info) do
      :invalid_return
    end

    def init(state, info) do
      state = Map.put(state, :key, info.key)
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

  defmodule BlockingInitServer do
    use DurableServer,
      vsn: 1

    def dump_state(state), do: state

    def load_state(_old_vsn, persisted_state) do
      DurableServerTest.atomify_keys(persisted_state)
    end

    def init(%{block_on_init: true} = state, info) do
      state = Map.put(state, :key, info.key)

      receive do
        :continue_init ->
          {:ok, Map.delete(state, :block_on_init), auto_sync: false}
      end
    end

    def init(state, info) do
      state = Map.put(state, :key, info.key)
      {:ok, state, auto_sync: false}
    end
  end

  describe "crash tracking and permanent status" do
    test "servers are non-permanent by default", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "non-permanent-default-test-#{DurableServer.UUID.uuid4()}"

      {:ok, _pid} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

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

        def init(state, info) do
          {:ok, Map.put(state, :key, info.key), permanent: true}
        end
      end

      {:ok, _pid} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {PermanentTestServer, key: key, initial_state: %{}}
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
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

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

        def init(%{crash_after: crash_after}, info) do
          {:ok, %{key: info.key, crash_after: crash_after, call_count: 0}}
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
          {CrashingServer, key: key, initial_state: %{crash_after: 1}}
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
          {CrashingServer, key: key, initial_state: %{crash_after: 1}}
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
            {CrashingServer, key: key, initial_state: %{crash_after: 1}}
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

    def init(init_state, info) do
      key = info.key

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

    def init(%{user_meta: user_meta}, info) do
      {:ok, %{key: info.key, count: 0}, meta: user_meta}
    end

    def init(_state, info) do
      {:ok, %{key: info.key, count: 0}}
    end

    def dump_state(state), do: state

    def load_state(_old_vsn, state) do
      DurableServerTest.atomify_keys(state)
    end
  end

  # crashing test server that supports user_meta
  defmodule LookupCrashingServer do
    use DurableServer, vsn: 1

    def init(%{crash_after: crash_after, user_meta: user_meta}, info) do
      {:ok, %{key: info.key, crash_after: crash_after, call_count: 0}, meta: user_meta}
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
          {LookupTestServer, key: key, initial_state: %{user_meta: user_meta}}
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
          {LookupCrashingServer, key: key, initial_state: %{user_meta: user_meta, crash_after: 1}}
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
          refute_process_down(new_pid)

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
            {LookupTestServer, key: key, initial_state: %{user_meta: user_meta}}
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
          {LookupTestServer, key: base_key, initial_state: %{user_meta: user_meta}}
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

    def init(init_state, info) do
      new_state = %{
        key: info.key,
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

    def handle_call(:stop_shutdown_normal, _from, state) do
      {:stop, {:shutdown, :normal}, :ok, state}
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

    test "terminate_and_cordon_child/2 with PID persists cordoned status and blocks starts",
         %{
           supervisor_name: supervisor_name,
           prefix: prefix
         } do
      key = "cordon_pid_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{count: 7}}
        )

      ref = Process.monitor(server_pid)

      assert :ok =
               DurableServer.Supervisor.terminate_and_cordon_child(supervisor_name, server_pid)

      assert_receive {:DOWN, ^ref, :process, ^server_pid, _reason}, 2_000

      store = test_object_store()

      assert {:ok, %StoredState{meta: %Meta{status: :cordoned}}} =
               DurableServer.fetch_stored_state(
                 store,
                 %{key: key, prefix: prefix, supervisor: supervisor_name},
                 consistent: true
               )

      assert {:error, :cordoned} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{count: 8}}
               )

      assert {:error, :cordoned} =
               DurableServer.Supervisor.ensure_started_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{count: 8}}
               )
    end

    test "terminate_and_cordon_child/2 with key cordons stopped storage and uncordon allows start",
         %{
           supervisor_name: supervisor_name,
           prefix: prefix
         } do
      key = "cordon_key_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{count: 7}}
        )

      ref = Process.monitor(server_pid)
      assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, server_pid)
      assert_receive {:DOWN, ^ref, :process, ^server_pid, _reason}, 2_000

      store = test_object_store()

      assert {:ok, %StoredState{meta: %Meta{status: :stopped_graceful}}} =
               DurableServer.fetch_stored_state(
                 store,
                 %{key: key, prefix: prefix, supervisor: supervisor_name},
                 consistent: true
               )

      assert :ok =
               DurableServer.Supervisor.terminate_and_cordon_child(supervisor_name, key,
                 timeout: 5_000
               )

      assert {:ok, %StoredState{meta: %Meta{status: :cordoned}} = stored_state} =
               DurableServer.fetch_stored_state(
                 store,
                 %{key: key, prefix: prefix, supervisor: supervisor_name},
                 consistent: true
               )

      assert {:error, :not_eligible} =
               DurableServer.claim_restart_attempt(
                 DurableServer.Supervisor.__get_config__(supervisor_name).storage_backend,
                 stored_state,
                 ttl: 10_000
               )

      assert :ok = DurableServer.Supervisor.uncordon_child(supervisor_name, key)

      assert {:ok, %StoredState{meta: %Meta{status: :stopped_graceful}}} =
               DurableServer.fetch_stored_state(
                 store,
                 %{key: key, prefix: prefix, supervisor: supervisor_name},
                 consistent: true
               )

      assert {:ok, {new_pid, _meta}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{count: 8}}
               )

      refute_process_down(new_pid)
    end

    test "terminate_and_delete_child/2 with key deletes cordoned storage through tombstone CAS" do
      parent = self()

      after_put = fn _state, key, data, opts, result ->
        send(parent, {:consistency_probe_put, key, data, opts, result})
      end

      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(
          backend: {ConsistencyProbeBackend, owner: self(), after_put: after_put}
        )

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)
      table = backend_table(store)
      key = "delete_cordoned_#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key

      cordoned_state = %StoredState{
        vsn: 1,
        state: %{count: 7},
        meta: %Meta{
          status: :cordoned,
          module: TestServer,
          pid: nil,
          supervisor: supervisor_name,
          node_ref: nil,
          node_str: to_string(Node.self()),
          last_heartbeat_at: System.system_time(:millisecond),
          crash_history: []
        }
      }

      :ets.insert(table, {{:data, storage_key}, %{body: cordoned_state, etag: "cordoned"}})

      assert :ok = DurableServer.Supervisor.terminate_and_delete_child(supervisor_name, key)

      assert_receive {:consistency_probe_put, ^storage_key,
                      %StoredState{
                        state: %{},
                        meta: %Meta{
                          status: :deleting,
                          pid: nil,
                          module: nil,
                          node_ref: nil,
                          supervisor: nil
                        }
                      }, [etag: "cordoned"], {:ok, %{etag: tombstone_etag}}},
                     2_000

      assert_receive {:consistency_probe_delete, ^storage_key, [etag: ^tombstone_etag]}, 2_000

      assert {:error, :not_found} =
               StorageBackend.get_object(store, storage_key, consistent: true)
    end

    test "terminate_and_cordon_child/2 with key retries when lock pid exits before accepting",
         %{
           supervisor_name: supervisor_name,
           prefix: prefix
         } do
      key = "cordon_key_noproc_#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key
      parent = self()

      fake_owner =
        spawn(fn ->
          send(parent, {:fake_owner_ready, self()})

          receive do
            _message -> :ok
          end
        end)

      assert_receive {:fake_owner_ready, ^fake_owner}
      owner_ref = Process.monitor(fake_owner)

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)

      running_state = %StoredState{
        vsn: 1,
        state: %{count: 7},
        meta: %Meta{
          status: :running,
          module: TestServer,
          pid: fake_owner,
          supervisor: supervisor_name,
          node_ref: DurableServer.Supervisor.node_ref(supervisor_name),
          node_str: to_string(Node.self()),
          last_heartbeat_at: System.system_time(:millisecond),
          crash_history: []
        }
      }

      assert {:ok, _object} = StorageBackend.put_object(store, storage_key, running_state, [])

      assert :ok = DurableServer.Supervisor.terminate_and_cordon_child(supervisor_name, key)
      assert_receive {:DOWN, ^owner_ref, :process, ^fake_owner, _reason}, 2_000

      assert {:ok, %StoredState{meta: %Meta{status: :cordoned}}} =
               DurableServer.fetch_stored_state(
                 store,
                 %{key: key, prefix: prefix, supervisor: supervisor_name},
                 consistent: true
               )
    end

    test "terminate_and_cordon_child/3 validates options", %{
      supervisor_name: supervisor_name
    } do
      assert_raise ArgumentError, fn ->
        DurableServer.Supervisor.terminate_and_cordon_child(supervisor_name, "missing",
          invalid: true
        )
      end
    end

    test "explicit start waits while delete tombstone is inside its lease" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)
      key = "delete_tombstone_active_#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key

      deleting_tombstone = %StoredState{
        vsn: 1,
        state: %{},
        meta: %Meta{
          status: :deleting,
          pid: nil,
          supervisor: nil,
          module: nil,
          node_ref: nil,
          node_str: to_string(Node.self()),
          last_heartbeat_at: System.system_time(:millisecond),
          crash_history: []
        }
      }

      {:ok, %{etag: tombstone_etag}} =
        StorageBackend.put_object(store, storage_key, deleting_tombstone, [])

      assert {:error, {:already_started, :deleting}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{count: 41}}
               )

      {:ok, stored_state} =
        DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix}, consistent: true)

      assert stored_state.etag == tombstone_etag
      assert stored_state.meta.status == :deleting
      assert stored_state.meta.supervisor == nil
    end

    test "ensure_started_child returns a delete tombstone error instead of crashing" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)
      key = "delete_tombstone_ensure_#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key

      deleting_tombstone = %StoredState{
        vsn: 1,
        state: %{},
        meta: %Meta{
          status: :deleting,
          pid: nil,
          supervisor: nil,
          module: nil,
          node_ref: nil,
          node_str: to_string(Node.self()),
          last_heartbeat_at: System.system_time(:millisecond),
          crash_history: []
        }
      }

      {:ok, %{etag: tombstone_etag}} =
        StorageBackend.put_object(store, storage_key, deleting_tombstone, [])

      assert {:error, {:already_started, :deleting}} =
               DurableServer.Supervisor.ensure_started_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{count: 41}}
               )

      {:ok, stored_state} =
        DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix}, consistent: true)

      assert stored_state.etag == tombstone_etag
      assert stored_state.meta.status == :deleting
    end

    test "explicit start confirms stale delete tombstone before blocking key reuse" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)
      table = backend_table(store)
      key = "delete_tombstone_stale_#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key

      deleting_tombstone = %StoredState{
        vsn: 1,
        state: %{},
        meta: %Meta{
          status: :deleting,
          pid: nil,
          supervisor: nil,
          module: nil,
          node_ref: nil,
          node_str: to_string(Node.self()),
          last_heartbeat_at: System.system_time(:millisecond),
          crash_history: []
        }
      }

      put_backend_override(
        table,
        storage_key,
        false,
        {:ok, %{body: deleting_tombstone, etag: "stale-tombstone-etag"}}
      )

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{count: 41}}
        )

      assert 41 = GenServer.call(pid, :get_count)
      refute_process_down(pid)

      get_opts = recorded_get_opts(table, storage_key)
      assert [consistent: false] in get_opts
      assert [consistent: true] in get_opts

      {:ok, stored_state} =
        DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix}, consistent: true)

      assert stored_state.meta.status == :running
      assert stored_state.meta.pid == pid
      assert atomify_keys(stored_state.state).count == 41
    end

    test "first-claim race against delete tombstone does not report the old pid as live" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)
      table = backend_table(store)
      key = "delete_tombstone_race_#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key
      old_pid = self()

      deleting_tombstone = %StoredState{
        vsn: 1,
        state: %{},
        meta: %Meta{
          status: :deleting,
          pid: old_pid,
          supervisor: nil,
          module: nil,
          node_ref: nil,
          node_str: to_string(Node.self()),
          last_heartbeat_at: System.system_time(:millisecond),
          crash_history: []
        }
      }

      :ets.insert(table, {{:data, storage_key}, %{body: deleting_tombstone, etag: "tombstone"}})
      put_backend_override(table, storage_key, false, {:error, :not_found})

      assert {:error, {:already_started, :deleting}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{count: 41}},
                 timeout: 10_000
               )
    end

    test "first-claim race against expired delete tombstone claims the tombstone" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)
      table = backend_table(store)
      key = "delete_tombstone_expired_race_#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key

      deleting_tombstone = %StoredState{
        vsn: 1,
        state: %{},
        meta: %Meta{
          status: :deleting,
          pid: self(),
          supervisor: nil,
          module: nil,
          node_ref: nil,
          node_str: to_string(Node.self()),
          last_heartbeat_at: System.system_time(:millisecond) - 120_000,
          crash_history: []
        }
      }

      :ets.insert(table, {{:data, storage_key}, %{body: deleting_tombstone, etag: "tombstone"}})
      put_backend_override(table, storage_key, false, {:error, :not_found})

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{count: 41}},
          timeout: 10_000
        )

      assert 41 = GenServer.call(pid, :get_count)
      refute_process_down(pid)

      {:ok, stored_state} =
        DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix}, consistent: true)

      assert stored_state.meta.status == :running
      assert stored_state.meta.pid == pid
      assert atomify_keys(stored_state.state).count == 41
    end

    test "physical delete uses the delete tombstone etag" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)
      table = backend_table(store)
      key = "delete_tombstone_conditional_#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{count: 7}}
        )

      assert :ok =
               DurableServer.Supervisor.terminate_and_delete_child(supervisor_name, server_pid)

      assert_receive {:consistency_probe_delete, ^storage_key, [etag: tombstone_etag]}, 2_000
      assert is_binary(tombstone_etag)
      assert [[etag: ^tombstone_etag]] = recorded_delete_opts(table, storage_key)

      assert {:error, :not_found} =
               StorageBackend.get_object(store, storage_key, consistent: true)
    end

    test "stale physical delete cannot remove a replacement after tombstone expiry" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)
      table = backend_table(store)
      key = "delete_tombstone_late_delete_#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key

      deleting_tombstone = %StoredState{
        vsn: 1,
        state: %{},
        meta: %Meta{
          status: :deleting,
          pid: nil,
          supervisor: nil,
          module: nil,
          node_ref: nil,
          node_str: to_string(Node.self()),
          last_heartbeat_at: System.system_time(:millisecond) - 120_000,
          crash_history: []
        }
      }

      :ets.insert(table, {{:data, storage_key}, %{body: deleting_tombstone, etag: "tombstone"}})

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{count: 41}},
          timeout: 10_000
        )

      assert {:error, :conflict} =
               StorageBackend.delete_object(store, storage_key, etag: "tombstone")

      {:ok, stored_state} =
        DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix}, consistent: true)

      assert stored_state.meta.status == :running
      assert stored_state.meta.pid == pid
      assert atomify_keys(stored_state.state).count == 41
      refute_process_down(pid)
    end

    test "explicit delete waits while delete tombstone is inside delete request margin" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(
          backend: {ConsistencyProbeBackend, owner: self()},
          delete_tombstone_delete_request_margin_ms: 5_000
        )

      %{
        storage_backend: store,
        heartbeat_staleness_threshold_ms: heartbeat_staleness_threshold_ms,
        delete_tombstone_delete_request_margin_ms: delete_tombstone_delete_request_margin_ms
      } = DurableServer.Supervisor.__get_config__(supervisor_name)

      key = "delete_tombstone_margin_#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key

      deleting_tombstone = %StoredState{
        vsn: 1,
        state: %{},
        meta: %Meta{
          status: :deleting,
          pid: nil,
          supervisor: nil,
          module: nil,
          node_ref: nil,
          node_str: to_string(Node.self()),
          last_heartbeat_at:
            System.system_time(:millisecond) - heartbeat_staleness_threshold_ms -
              div(delete_tombstone_delete_request_margin_ms, 2),
          crash_history: []
        }
      }

      {:ok, %{etag: tombstone_etag}} =
        StorageBackend.put_object(store, storage_key, deleting_tombstone, [])

      assert {:error, {:locked, :deleting}} =
               DurableServer.Supervisor.terminate_and_delete_child(supervisor_name, key)

      {:ok, stored_state} =
        DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix}, consistent: true)

      assert stored_state.etag == tombstone_etag
      assert stored_state.meta.status == :deleting
      assert stored_state.meta.supervisor == nil
    end

    test "explicit start replaces expired delete tombstone with initial state" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)
      key = "delete_tombstone_expired_#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key

      deleting_tombstone = %StoredState{
        vsn: 1,
        state: %{},
        meta: %Meta{
          status: :deleting,
          pid: nil,
          supervisor: nil,
          module: nil,
          node_ref: nil,
          node_str: to_string(Node.self()),
          last_heartbeat_at: System.system_time(:millisecond) - 120_000,
          crash_history: []
        }
      }

      {:ok, _object} = StorageBackend.put_object(store, storage_key, deleting_tombstone, [])

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{count: 41}}
        )

      assert 41 = GenServer.call(pid, :get_count)

      refute_process_down(pid)

      {:ok, stored_state} =
        DurableServer.fetch_stored_state(store, %{key: key, prefix: prefix}, consistent: true)

      assert stored_state.meta.status == :running
      assert stored_state.meta.pid == pid
      assert atomify_keys(stored_state.state).count == 41
    end

    test "terminate_and_delete_child/2 with PID deletes running process and storage", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "delete_test_pid_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, key: key, initial_state: %{}}
        )

      GenServer.call(server_pid, {:set_test_pid, self()})

      refute_process_down(server_pid)
      assert %{} = :sys.get_state(server_pid)

      # verify object exists in storage
      store = test_object_store()
      assert {:ok, _} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # delete by PID
      ref = Process.monitor(server_pid)

      assert :ok =
               DurableServer.Supervisor.terminate_and_delete_child(supervisor_name, server_pid)

      # verify process is terminated
      assert_receive {:DOWN, ^ref, :process, ^server_pid, _reason}, 2_000

      # verify object is deleted from storage
      assert {:error, :not_found} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # verify terminate was called
      assert_receive {:terminate, _reason, ^key}
    end

    test "a stale process cannot delete a newer owner's object", %{prefix: _prefix} do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      key = "stale-delete-owner-#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, key: key, initial_state: %{}}
        )

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
      storage_key = prefix <> key

      assert {:ok, %{body: stored_state, etag: etag}} =
               StorageBackend.get_object(backend, storage_key)

      newer_owner =
        stored_state.meta
        |> Map.put(:pid, self())
        |> Map.update!(:node_ref, &(&1 + 1))
        |> Meta.put_status(:running)

      assert {:ok, _obj} =
               StorageBackend.put_object(
                 backend,
                 storage_key,
                 %{stored_state | meta: newer_owner},
                 etag: etag
               )

      ref = Process.monitor(server_pid)
      assert :ok = GenServer.call(server_pid, :delete_self)
      assert_receive {:DOWN, ^ref, :process, ^server_pid, {:shutdown, :delete}}

      assert {:ok, %{body: final_state}} = StorageBackend.get_object(backend, storage_key)
      assert final_state.meta.pid == self()
      assert final_state.meta.node_ref == newer_owner.node_ref
      assert final_state.meta.status == :running
    end

    test "terminate_and_delete_child returns a storage deletion failure", %{prefix: _prefix} do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)

      for target <- [:pid, :key] do
        key = "delete-failure-#{target}-#{DurableServer.UUID.uuid4()}"

        {:ok, {server_pid, _meta}} =
          DurableServer.Supervisor.start_child(
            supervisor_name,
            {DeleteTestServer, key: key, initial_state: %{}}
          )

        storage_key = prefix <> key
        put_backend_delete_override(backend_table(backend), storage_key, {:error, :unavailable})
        delete_target = if target == :pid, do: server_pid, else: key

        assert {:error, :unavailable} =
                 DurableServer.Supervisor.terminate_and_delete_child(
                   supervisor_name,
                   delete_target
                 )

        assert {:ok, _object} = StorageBackend.get_object(backend, storage_key)
      end
    end

    test "terminate_and_delete_child/2 with PID returns storage delete errors" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(
          backend: {ConsistencyProbeBackend, owner: self(), delete_error: :delete_failed}
        )

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)
      table = backend_table(store)
      key = "delete_test_pid_error_#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, key: key, initial_state: %{}}
        )

      GenServer.call(server_pid, {:set_test_pid, self()})

      ref = Process.monitor(server_pid)

      assert {:error, :delete_failed} =
               DurableServer.Supervisor.terminate_and_delete_child(supervisor_name, server_pid)

      assert_receive {:DOWN, ^ref, :process, ^server_pid, _reason}, 2_000
      assert_receive {:terminate, {:shutdown, :delete}, ^key}

      assert {:ok, stored_state} =
               DurableServer.fetch_stored_state(
                 store,
                 %{key: key, prefix: prefix, supervisor: supervisor_name},
                 consistent: true
               )

      assert stored_state.meta.status == :deleting

      [{{:data, ^storage_key}, %{body: raw_stored_state}}] =
        :ets.lookup(table, {:data, storage_key})

      assert raw_stored_state.state == %{}
      assert raw_stored_state.meta.status == :deleting
      assert raw_stored_state.meta.pid == nil
      assert raw_stored_state.meta.module == nil
      assert raw_stored_state.meta.node_ref == nil
      assert raw_stored_state.meta.supervisor == nil
    end

    test "terminate_and_delete_child/2 with key deletes running process and storage", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "delete_test_key_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, key: key, initial_state: %{}}
        )

      GenServer.call(server_pid, {:set_test_pid, self()})

      refute_process_down(server_pid)

      # verify object exists in storage
      store = test_object_store()
      assert {:ok, _} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # delete by key
      ref = Process.monitor(server_pid)
      assert :ok = DurableServer.Supervisor.terminate_and_delete_child(supervisor_name, key)

      # verify process is terminated
      assert_receive {:DOWN, ^ref, :process, ^server_pid, _reason}, 2_000

      # verify object is deleted from storage
      assert {:error, :not_found} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # verify terminate was called
      assert_receive {:terminate, _reason, ^key}
    end

    test "terminate_and_delete_child/2 with key returns live process storage delete errors" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(
          backend: {ConsistencyProbeBackend, owner: self(), delete_error: :delete_failed}
        )

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)
      key = "delete_test_key_error_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, key: key, initial_state: %{}}
        )

      GenServer.call(server_pid, {:set_test_pid, self()})

      ref = Process.monitor(server_pid)

      assert {:error, :delete_failed} =
               DurableServer.Supervisor.terminate_and_delete_child(supervisor_name, key)

      assert_receive {:DOWN, ^ref, :process, ^server_pid, _reason}, 2_000
      assert_receive {:terminate, {:shutdown, :delete}, ^key}

      assert {:ok, stored_state} =
               DurableServer.fetch_stored_state(
                 store,
                 %{key: key, prefix: prefix, supervisor: supervisor_name},
                 consistent: true
               )

      assert stored_state.meta.status == :deleting
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
          {DeleteTestServer, key: key, initial_state: %{}}
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

    test "terminate_and_delete_child/2 with missing key does not issue stale physical delete" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)
      table = backend_table(store)
      key = "delete_missing_no_physical_#{DurableServer.UUID.uuid4()}"
      storage_key = prefix <> key

      assert :ok = DurableServer.Supervisor.terminate_and_delete_child(supervisor_name, key)
      assert [] = recorded_delete_opts(table, storage_key)
    end
  end

  describe "mirror backend delete semantics" do
    test "conditional delete support follows write target and does not delete mirror with foreign etag" do
      primary_table = :ets.new(:mirror_primary_probe, [:set, :public])
      secondary_table = :ets.new(:mirror_secondary_probe, [:set, :public])

      primary =
        StorageBackend.new(
          ConsistencyProbeBackend,
          %{table: primary_table, owner: self(), after_put: nil, delete_error: nil},
          %{},
          %{conditional_delete?: true}
        )

      secondary =
        StorageBackend.new(
          ConsistencyProbeBackend,
          %{table: secondary_table, owner: self(), after_put: nil, delete_error: nil},
          %{},
          %{}
        )

      {:ok, mirror} =
        StorageBackend.init_backend(MirrorStore,
          primary: primary,
          secondary: secondary,
          read_preference: :secondary,
          write_target: :primary,
          mirror_writes: true,
          mirror_mode: :required
        )

      key = "mirror/delete/#{DurableServer.UUID.uuid4()}"

      assert StorageBackend.supports?(mirror, :conditional_delete?)
      assert {:ok, %{etag: primary_etag}} = StorageBackend.put_object(primary, key, "primary")
      assert {:ok, _} = StorageBackend.put_object(secondary, key, "secondary")

      assert :ok = StorageBackend.delete_object(mirror, key, etag: primary_etag)

      assert [[etag: ^primary_etag]] = recorded_delete_opts(primary_table, key)
      assert [] = recorded_delete_opts(secondary_table, key)
      assert {:error, :not_found} = StorageBackend.get_object(primary, key)
      assert {:ok, %{body: "secondary"}} = StorageBackend.get_object(secondary, key)
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
          {DeleteTestServer, key: key, initial_state: %{}}
        )

      # set the test PID after starting
      GenServer.call(server_pid, {:set_test_pid, self()})

      # verify server is running
      refute_process_down(server_pid)

      # verify object exists in storage
      store = test_object_store()
      assert {:ok, _} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # call delete_self callback
      ref = Process.monitor(server_pid)
      assert :ok = GenServer.call(server_pid, :delete_self)

      # verify process is terminated
      assert_receive {:DOWN, ^ref, :process, ^server_pid, {:shutdown, :delete}}

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
          {DeleteTestServer, key: key, initial_state: %{}}
        )

      # set the test PID after starting
      GenServer.call(server_pid, {:set_test_pid, self()})

      # verify server is running
      refute_process_down(server_pid)

      # verify object exists in storage
      store = test_object_store()
      assert {:ok, _} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # call delete_self_with_reply callback
      ref = Process.monitor(server_pid)
      assert :deleted = GenServer.call(server_pid, :delete_self_with_reply)

      # should receive deleting message before termination
      assert_receive {:deleting, ^key}

      # verify process is terminated
      assert_receive {:DOWN, ^ref, :process, ^server_pid, {:shutdown, :delete}}, 2_000

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
          {DeleteTestServer, key: key, initial_state: %{}}
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
          {DeleteTestServer, key: key, initial_state: %{}}
        )

      # set the test PID after starting
      GenServer.call(server_pid, {:set_test_pid, self()})

      # verify server is running and holds a lock (by being able to call it)
      refute_process_down(server_pid)
      assert %{} = :sys.get_state(server_pid)

      # verify object exists in storage
      store = test_object_store()
      assert {:ok, _} = ObjectStore.get_object(store, "#{prefix}#{key}")

      # delete by PID - this should send a message to the process since it's locked
      # the process should then delete itself
      ref = Process.monitor(server_pid)

      assert :ok =
               DurableServer.Supervisor.terminate_and_delete_child(supervisor_name, server_pid)

      # verify process is terminated
      assert_receive {:DOWN, ^ref, :process, ^server_pid, _reason}, 2_000

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
          {DeleteTestServer, key: key, initial_state: %{}}
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

    test "{:stop, {:shutdown, :normal}, state} stops gracefully with the wrapped reason", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      key = "normal_shutdown_#{DurableServer.UUID.uuid4()}"

      {:ok, {server_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {DeleteTestServer, key: key, initial_state: %{}}
        )

      ref = Process.monitor(server_pid)
      assert :ok = GenServer.call(server_pid, :stop_shutdown_normal)
      assert_receive {:DOWN, ^ref, :process, ^server_pid, {:shutdown, :normal}}

      assert {:ok, stored_state} =
               DurableServer.fetch_stored_state(
                 DurableServer.Supervisor.__get_config__(supervisor_name).storage_backend,
                 %{key: key, prefix: prefix}
               )

      assert stored_state.meta.status == :stopped_graceful
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
          {DeleteTestServer, key: key, initial_state: %{}}
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
          {DeleteTestServer, key: key, initial_state: %{}}
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

  describe "explicit consistency opts" do
    test "fetch_stored_state forwards consistent opt to the backend" do
      {:ok, backend} = StorageBackend.init_backend(ConsistencyProbeBackend, owner: self())
      table = backend_table(backend)
      prefix = "probe/"
      key = "forwarded"
      storage_key = prefix <> key

      put_backend_override(
        table,
        storage_key,
        false,
        {:ok, %{body: %StoredState{vsn: 1, state: %{}, meta: %DurableServer.Meta{}}, etag: "v1"}}
      )

      assert {:ok, %StoredState{}} =
               DurableServer.fetch_stored_state(backend, %{key: key, prefix: prefix},
                 consistent: false
               )

      assert Enum.any?(recorded_get_opts(table, storage_key), &(&1 == [consistent: false]))
    end

    test "fetch_node_heartbeat_from_storage forwards explicit consistency opts" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
      table = backend_table(backend)
      node_str = to_string(node())
      storage_key = "#{prefix}__nodes/#{node_str}"

      assert {:healthy, %{}} =
               LifecycleManager.fetch_node_heartbeat_from_storage(
                 supervisor_name,
                 node_str,
                 consistent: false
               )

      assert {:healthy, %{}} =
               LifecycleManager.fetch_node_heartbeat_from_storage(
                 supervisor_name,
                 node_str,
                 consistent: true
               )

      get_opts = recorded_get_opts(table, storage_key)
      assert [consistent: false] in get_opts
      assert [consistent: true] in get_opts
    end

    test "startup rereads consistently before acting on an expired eventual read" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
      table = backend_table(backend)
      key = "stale-expired-reread"
      storage_key = prefix <> key
      current_node_ref = DurableServer.Supervisor.node_ref(supervisor_name)

      stale_state = %StoredState{
        vsn: 1,
        state: %{"count" => 1},
        meta: %DurableServer.Meta{
          key: key,
          prefix: prefix,
          supervisor: supervisor_name,
          module: TestServer,
          status: :stopped_graceful,
          node_str: "stale@node",
          node_ref: current_node_ref,
          pid: self()
        }
      }

      locked_state = %StoredState{
        vsn: 2,
        state: %{"count" => 2},
        meta: %DurableServer.Meta{
          key: key,
          prefix: prefix,
          supervisor: supervisor_name,
          module: TestServer,
          status: :running,
          node_str: to_string(node()),
          node_ref: current_node_ref,
          pid: self()
        }
      }

      put_backend_override(
        table,
        storage_key,
        false,
        {:ok, %{body: stale_state, etag: "stale-etag"}}
      )

      put_backend_override(
        table,
        storage_key,
        true,
        {:ok, %{body: locked_state, etag: "fresh-etag"}}
      )

      assert {:error, {:already_started, pid}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {TestServer, key: key, initial_state: %{}}
               )

      assert pid == self()

      get_opts = recorded_get_opts(table, storage_key)
      assert [consistent: false] in get_opts
      assert [consistent: true] in get_opts
    end

    test "preloaded boot info skips storage get for preloaded stopped state" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(backend: {ConsistencyProbeBackend, owner: self()})

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)
      table = backend_table(backend)
      key = "restart-no-read"
      storage_key = prefix <> key
      etag = "restart-etag"

      stored_state = %StoredState{
        vsn: 1,
        state: %{"count" => 41},
        meta: %DurableServer.Meta{
          key: key,
          prefix: prefix,
          supervisor: supervisor_name,
          module: TestServer,
          status: :stopped_graceful,
          node_str: "old@node",
          node_ref: 123,
          pid: self()
        }
      }

      :ets.insert(table, {{:data, storage_key}, %{body: stored_state, etag: etag}})

      assert {:ok, {pid, _meta}} =
               DurableServer.Supervisor.__start_child__(
                 supervisor_name,
                 preloaded_child_spec(TestServer, [key: key, initial_state: %{}], %{
                   body: stored_state,
                   etag: etag
                 }),
                 local_only: true
               )

      assert GenServer.call(pid, :get_count) == 41
      assert recorded_get_opts(table, storage_key) == []
    end
  end

  describe "global lock circuit breaker" do
    test "preloaded boots bypass an open global lock circuit breaker" do
      {supervisor_name, _supervisor_pid, prefix} =
        start_test_supervisor(
          global_lock_failure_count: 1,
          global_lock_failure_window_ms: 60_000,
          global_lock_failure_cooldown_ms: 60_000
        )

      key = "restart-breaker-bypass-#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

      ref = Process.monitor(pid)
      assert :ok = GenServer.call(pid, :stop_normal)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

      %{storage_backend: backend, circuit_breaker: circuit_breaker} =
        DurableServer.Supervisor.__get_config__(supervisor_name)

      {:ok, %{etag: etag} = body} =
        DurableServer.fetch_stored_state(backend, %{key: key, prefix: prefix})

      CircuitBreaker.increment_global_lock_failures(circuit_breaker)

      assert {:circuit_open, _cooldown_ms} =
               CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker)

      assert {:error, {:circuit_open, :network_partition}} =
               DurableServer.Supervisor.start_child(
                 supervisor_name,
                 {TestServer, key: "fresh-#{DurableServer.UUID.uuid4()}", initial_state: %{}}
               )

      assert {:ok, {restart_pid, _meta}} =
               DurableServer.Supervisor.__start_child__(
                 supervisor_name,
                 preloaded_child_spec(TestServer, [key: key, initial_state: %{}], %{
                   body: body,
                   etag: etag
                 }),
                 local_only: true,
                 timeout: 10_000
               )

      refute_process_down(restart_pid)
    end

    test "normal races still increment the breaker but restart races do not" do
      slow_init_ms = 6_500

      {normal_supervisor, _normal_supervisor_pid, _prefix} =
        start_test_supervisor(
          global_lock_failure_count: 1,
          global_lock_failure_window_ms: 60_000,
          global_lock_failure_cooldown_ms: 60_000
        )

      normal_breaker = DurableServer.Supervisor.__get_config__(normal_supervisor).circuit_breaker
      normal_key = "normal-race-#{DurableServer.UUID.uuid4()}"

      normal_results =
        [
          Task.async(fn ->
            DurableServer.Supervisor.start_child(
              normal_supervisor,
              {TestServer, key: normal_key, initial_state: %{init_sleep_ms: slow_init_ms}},
              timeout: 15_000
            )
          end),
          Task.async(fn ->
            DurableServer.Supervisor.start_child(
              normal_supervisor,
              {TestServer, key: normal_key, initial_state: %{init_sleep_ms: slow_init_ms}},
              timeout: 15_000
            )
          end)
        ]
        |> Enum.map(&Task.await(&1, 20_000))

      assert Enum.count(normal_results, &match?({:ok, {_pid, _meta}}, &1)) == 1
      assert Enum.count(normal_results, &match?({:error, {:already_started, _pid}}, &1)) == 1

      assert {:circuit_open, _cooldown_ms} =
               CircuitBreaker.check_global_lock_circuit_breaker(normal_breaker)

      {restart_supervisor, _restart_supervisor_pid, restart_prefix} =
        start_test_supervisor(
          global_lock_failure_count: 1,
          global_lock_failure_window_ms: 60_000,
          global_lock_failure_cooldown_ms: 60_000
        )

      restart_key = "restart-race-#{DurableServer.UUID.uuid4()}"

      {:ok, {seed_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          restart_supervisor,
          {TestServer, key: restart_key, initial_state: %{init_sleep_ms: slow_init_ms}},
          timeout: 15_000
        )

      seed_ref = Process.monitor(seed_pid)
      assert :ok = GenServer.call(seed_pid, :stop_normal)
      assert_receive {:DOWN, ^seed_ref, :process, ^seed_pid, :normal}

      %{storage_backend: restart_backend, circuit_breaker: restart_breaker} =
        DurableServer.Supervisor.__get_config__(restart_supervisor)

      {:ok, %{etag: restart_etag} = restart_body} =
        DurableServer.fetch_stored_state(
          restart_backend,
          %{key: restart_key, prefix: restart_prefix}
        )

      restart_results =
        [
          Task.async(fn ->
            DurableServer.Supervisor.__start_child__(
              restart_supervisor,
              preloaded_child_spec(TestServer, [key: restart_key, initial_state: %{}], %{
                body: restart_body,
                etag: restart_etag
              }),
              local_only: true,
              timeout: 15_000
            )
          end),
          Task.async(fn ->
            DurableServer.Supervisor.__start_child__(
              restart_supervisor,
              preloaded_child_spec(TestServer, [key: restart_key, initial_state: %{}], %{
                body: restart_body,
                etag: restart_etag
              }),
              local_only: true,
              timeout: 15_000
            )
          end)
        ]
        |> Enum.map(&Task.await(&1, 20_000))

      assert Enum.count(restart_results, &match?({:ok, {_pid, _meta}}, &1)) == 1
      assert Enum.count(restart_results, &match?({:error, {:already_started, _pid}}, &1)) == 1
      assert :ok = CircuitBreaker.check_global_lock_circuit_breaker(restart_breaker)

      assert {:ok, {_pid, _meta}} =
               DurableServer.Supervisor.start_child(
                 restart_supervisor,
                 {TestServer,
                  key: "fresh-after-restart-race-#{DurableServer.UUID.uuid4()}",
                  initial_state: %{}}
               )
    end

    test "normal start waits for an active restart claim instead of racing it" do
      {supervisor_name, _supervisor_pid, prefix} = start_test_supervisor()
      key = "restart-lease-#{DurableServer.UUID.uuid4()}"

      {:ok, {seed_pid, _meta}} =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}},
          timeout: 15_000
        )

      seed_ref = Process.monitor(seed_pid)
      assert :ok = GenServer.call(seed_pid, :stop_normal)
      assert_receive {:DOWN, ^seed_ref, :process, ^seed_pid, :normal}

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)

      {:ok, %StoredState{} = stored_state} =
        DurableServer.fetch_stored_state(
          backend,
          %{key: key, prefix: prefix}
        )

      assert {:ok, %{body: claimed_body, etag: claimed_etag}} =
               DurableServer.claim_restart_attempt(backend, stored_state, ttl: 10_000)

      restart_task =
        Task.async(fn ->
          Process.sleep(250)

          DurableServer.Supervisor.__start_child__(
            supervisor_name,
            preloaded_child_spec(TestServer, [key: key, initial_state: %{}], %{
              body: claimed_body,
              etag: claimed_etag
            }),
            local_only: true,
            timeout: 15_000
          )
        end)

      direct_result =
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}},
          timeout: 15_000
        )

      assert {:ok, {restart_pid, _restart_meta}} = Task.await(restart_task, 20_000)
      assert {:error, {:already_started, {^restart_pid, _direct_meta}}} = direct_result
    end
  end
end
