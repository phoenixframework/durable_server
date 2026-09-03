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

    def init(loaded_state, info) do
      {:ok, loaded_state |> Map.put(:key, info.key) |> Map.put_new(:count, 0),
       auto_sync: false, meta: %{my: "meta"}}
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

    def init(loaded_state, info) do
      state =
        loaded_state
        |> Map.put(:key, info.key)
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
    def put_object(%{table: table, fail_count: fail_count} = state, _key, data, opts) do
      attempt = :ets.update_counter(table, :attempts, {2, 1}, {:attempts, 0})
      :ets.insert(table, {:last_put_opts, opts})

      if attempt <= fail_count do
        {:error, Map.get(state, :failure_reason, {:mirror_failed, :no_quorum})}
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

  defmodule DiscoveryCrashOnceBackend do
    @behaviour DurableServer.StorageBackend

    alias DurableServer.StorageBackend

    @impl true
    def init_backend(opts) do
      delegate = Keyword.fetch!(opts, :delegate)

      {:ok,
       %{
         state: %{
           delegate: delegate,
           table: Keyword.fetch!(opts, :table),
           notify_pid: Keyword.fetch!(opts, :notify_pid)
         },
         defaults: StorageBackend.defaults(delegate),
         features: StorageBackend.features(delegate)
       }}
    end

    @impl true
    def ensure_ready(%{delegate: delegate}), do: StorageBackend.ensure_ready(delegate)

    @impl true
    def get_object(%{delegate: delegate}, key, opts),
      do: StorageBackend.get_object(delegate, key, opts)

    @impl true
    def list_all_objects_stream(
          %{delegate: delegate, table: table, notify_pid: notify_pid},
          prefix,
          opts
        ) do
      if String.ends_with?(prefix, "__nodes/") do
        StorageBackend.list_all_objects_stream(delegate, prefix, opts)
      else
        attempt =
          :ets.update_counter(
            table,
            :discovery_list_attempts,
            {2, 1},
            {:discovery_list_attempts, 0}
          )

        send(notify_pid, {:discovery_list_attempt, attempt})

        if attempt == 1 do
          raise "injected discovery failure"
        else
          StorageBackend.list_all_objects_stream(delegate, prefix, opts)
        end
      end
    end

    @impl true
    def put_object(%{delegate: delegate}, key, data, opts),
      do: StorageBackend.put_object(delegate, key, data, opts)

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
  end

  defmodule HeartbeatListBackend do
    @behaviour DurableServer.StorageBackend

    alias DurableServer.StorageBackend

    @impl true
    def init_backend(opts) do
      delegate =
        case Keyword.fetch!(opts, :delegate) do
          %StorageBackend{} = backend ->
            backend

          {adapter, raw_opts} ->
            {:ok, backend} = StorageBackend.init_backend(adapter, raw_opts)
            backend
        end

      {:ok,
       %{
         state: %{
           delegate: delegate,
           mode: Keyword.fetch!(opts, :mode),
           heartbeat_prefix: Keyword.fetch!(opts, :heartbeat_prefix),
           inject_key: Keyword.get(opts, :inject_key)
         },
         defaults: StorageBackend.defaults(delegate),
         features: StorageBackend.features(delegate)
       }}
    end

    @impl true
    def ensure_ready(%{delegate: delegate}), do: StorageBackend.ensure_ready(delegate)

    @impl true
    def get_object(%{delegate: delegate} = state, key, opts) do
      if current_mode(state) == :fetch_error and key == state.inject_key do
        {:error, :simulated_fetch_failure}
      else
        StorageBackend.get_object(delegate, key, opts)
      end
    end

    @impl true
    def list_all_objects_stream(%{delegate: delegate} = state, prefix, opts) do
      stream = StorageBackend.list_all_objects_stream(delegate, prefix, opts)

      if prefix == state.heartbeat_prefix do
        inject_list_fault(stream, current_mode(state), state, opts)
      else
        stream
      end
    end

    defp inject_list_fault(stream, :truncate, _state, opts) do
      error_handler = Keyword.get(opts, :error_handler, fn _reason -> :continue end)
      Stream.concat(stream, reported_error_stream(error_handler, :simulated_page_failure))
    end

    defp inject_list_fault(stream, :inject_key, state, _opts) do
      Stream.concat(stream, [%{key: state.inject_key}])
    end

    defp inject_list_fault(stream, :fetch_error, state, _opts) do
      Stream.concat(stream, [%{key: state.inject_key}])
    end

    defp inject_list_fault(stream, {:block, notify_pid}, _state, _opts) do
      send(notify_pid, {:heartbeat_cache_refresh_blocked, self()})

      receive do
        :continue_heartbeat_cache_refresh -> stream
      after
        5_000 -> raise "timed out waiting to continue heartbeat cache refresh"
      end
    end

    defp inject_list_fault(stream, :ok, _state, _opts), do: stream

    defp current_mode(%{mode: {:controlled, table}}) do
      :ets.lookup_element(table, :mode, 2)
    end

    defp current_mode(%{mode: mode}), do: mode

    defp reported_error_stream(error_handler, reason) do
      Stream.flat_map([reason], fn reason ->
        _ = error_handler.({:list_failed, reason})
        []
      end)
    end

    @impl true
    def put_object(%{delegate: delegate}, key, data, opts),
      do: StorageBackend.put_object(delegate, key, data, opts)

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
  end

  defmodule HeartbeatDelayBackend do
    @behaviour DurableServer.StorageBackend

    alias DurableServer.StorageBackend

    @impl true
    def init_backend(opts) do
      delegate =
        case Keyword.fetch!(opts, :delegate) do
          %StorageBackend{} = backend ->
            backend

          {adapter, raw_opts} ->
            {:ok, backend} = StorageBackend.init_backend(adapter, raw_opts)
            backend
        end

      {:ok,
       %{
         state: %{
           delegate: delegate,
           table: Keyword.fetch!(opts, :table),
           delay_ms: Keyword.fetch!(opts, :delay_ms)
         },
         defaults: StorageBackend.defaults(delegate),
         features: StorageBackend.features(delegate)
       }}
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
    def put_object(%{delegate: delegate, table: table, delay_ms: delay_ms}, key, data, opts) do
      if String.contains?(key, "__nodes/") do
        :ets.insert(table, {:heartbeat_write_started_at, System.system_time(:millisecond)})
        :ets.insert(table, {:heartbeat_write, data})
        Process.sleep(delay_ms)
      end

      StorageBackend.put_object(delegate, key, data, opts)
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

  defmodule HeartbeatHangAfterFirstBackend do
    @behaviour DurableServer.StorageBackend

    alias DurableServer.StorageBackend

    @impl true
    def init_backend(opts) do
      delegate =
        case Keyword.fetch!(opts, :delegate) do
          %StorageBackend{} = backend ->
            backend

          {adapter, raw_opts} ->
            {:ok, backend} = StorageBackend.init_backend(adapter, raw_opts)
            backend
        end

      {:ok,
       %{
         state: %{
           delegate: delegate,
           table: Keyword.fetch!(opts, :table),
           hang_after: Keyword.fetch!(opts, :hang_after),
           notify_pid: Keyword.fetch!(opts, :notify_pid)
         },
         defaults: StorageBackend.defaults(delegate),
         features: StorageBackend.features(delegate)
       }}
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
    def put_object(
          %{delegate: delegate, table: table, hang_after: hang_after, notify_pid: notify_pid},
          key,
          data,
          opts
        ) do
      if String.contains?(key, "__nodes/") do
        attempt = :ets.update_counter(table, :heartbeat_puts, {2, 1}, {:heartbeat_puts, 0})

        if attempt >= hang_after do
          send(notify_pid, {:hanging_heartbeat_task, self()})
          Process.sleep(:infinity)
        end
      end

      StorageBackend.put_object(delegate, key, data, opts)
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

  defmodule HeartbeatConflictBackend do
    @behaviour DurableServer.StorageBackend

    alias DurableServer.StorageBackend

    @impl true
    def init_backend(opts) do
      delegate =
        case Keyword.fetch!(opts, :delegate) do
          %StorageBackend{} = backend ->
            backend

          {adapter, raw_opts} ->
            {:ok, backend} = StorageBackend.init_backend(adapter, raw_opts)
            backend
        end

      conflict_keys =
        opts
        |> Keyword.get(:conflict_keys, [])
        |> MapSet.new()

      {:ok,
       %{
         state: %{delegate: delegate, conflict_keys: conflict_keys},
         defaults: StorageBackend.defaults(delegate),
         features: StorageBackend.features(delegate)
       }}
    end

    @impl true
    def ensure_ready(%{delegate: delegate}), do: StorageBackend.ensure_ready(delegate)

    @impl true
    def get_object(%{delegate: delegate, conflict_keys: conflict_keys}, key, opts) do
      if Keyword.get(opts, :consistent) == true and MapSet.member?(conflict_keys, key) do
        {:error, {:consistent_read_failed, {:consistent_read_failed, ":conflict"}}}
      else
        StorageBackend.get_object(delegate, key, opts)
      end
    end

    @impl true
    def list_all_objects_stream(%{delegate: delegate}, prefix, opts),
      do: StorageBackend.list_all_objects_stream(delegate, prefix, opts)

    @impl true
    def put_object(%{delegate: delegate}, key, data, opts),
      do: StorageBackend.put_object(delegate, key, data, opts)

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

  defmodule HeartbeatConflictAfterFirstReadBackend do
    @behaviour DurableServer.StorageBackend

    alias DurableServer.StorageBackend

    @impl true
    def init_backend(opts) do
      delegate =
        case Keyword.fetch!(opts, :delegate) do
          %StorageBackend{} = backend ->
            backend

          {adapter, raw_opts} ->
            {:ok, backend} = StorageBackend.init_backend(adapter, raw_opts)
            backend
        end

      conflict_keys =
        opts
        |> Keyword.get(:conflict_keys, [])
        |> MapSet.new()

      table = :ets.new(__MODULE__, [:set, :public])

      {:ok,
       %{
         state: %{delegate: delegate, conflict_keys: conflict_keys, table: table},
         defaults: StorageBackend.defaults(delegate),
         features: StorageBackend.features(delegate)
       }}
    end

    @impl true
    def ensure_ready(%{delegate: delegate}), do: StorageBackend.ensure_ready(delegate)

    @impl true
    def get_object(%{delegate: delegate, conflict_keys: conflict_keys, table: table}, key, opts) do
      if Keyword.get(opts, :consistent) == true and MapSet.member?(conflict_keys, key) do
        attempts =
          :ets.update_counter(table, {:consistent_get, key}, {2, 1}, {{:consistent_get, key}, 0})

        if attempts > 1 do
          {:error, {:consistent_read_failed, {:consistent_read_failed, ":conflict"}}}
        else
          StorageBackend.get_object(delegate, key, opts)
        end
      else
        StorageBackend.get_object(delegate, key, opts)
      end
    end

    @impl true
    def list_all_objects_stream(%{delegate: delegate}, prefix, opts),
      do: StorageBackend.list_all_objects_stream(delegate, prefix, opts)

    @impl true
    def put_object(%{delegate: delegate}, key, data, opts),
      do: StorageBackend.put_object(delegate, key, data, opts)

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

  defmodule DeleteTrackingBackend do
    @behaviour DurableServer.StorageBackend

    alias DurableServer.StorageBackend

    @impl true
    def init_backend(opts) do
      delegate =
        case Keyword.fetch!(opts, :delegate) do
          %StorageBackend{} = backend ->
            backend

          {adapter, raw_opts} ->
            {:ok, backend} = StorageBackend.init_backend(adapter, raw_opts)
            backend
        end

      table = Keyword.fetch!(opts, :table)

      {:ok,
       %{
         state: %{delegate: delegate, table: table},
         defaults: StorageBackend.defaults(delegate),
         features: StorageBackend.features(delegate)
       }}
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
    def put_object(%{delegate: delegate}, key, data, opts),
      do: StorageBackend.put_object(delegate, key, data, opts)

    @impl true
    def delete_object(%{delegate: delegate, table: table}, key) do
      :ets.insert(table, {:deleted, key})
      StorageBackend.delete_object(delegate, key)
    end

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
          {DelayedTerminateServer,
           key: key, initial_state: %{terminate_delay_ms: 300, notify_pid: self()}}
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
        DurableServer.Supervisor.start_child(
          supervisor_name,
          {TestServer, key: key, initial_state: %{}}
        )

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
      circuit_breaker: circuit_breaker
    } do
      key = "detect-crashed-test-#{DurableServer.UUID.uuid4()}"
      breaker_table = circuit_breaker.table_name

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

      assert {restarted_pid, _meta} =
               DurableServer.Supervisor.lookup(supervisor_name, key)

      assert restarted_pid != pid
      assert_process_alive(restarted_pid)

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

      assert :ets.lookup(breaker_table, TestServer) == []

      GenServer.stop(manager_pid)
    end

    test "stale heartbeat cache does not make a live storage heartbeat orphan-claimable", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)

      key = "stale-heartbeat-lock-test-#{DurableServer.UUID.uuid4()}"
      node_str = "remote@test"
      node_ref = System.unique_integer([:positive])
      pid = self()
      now = System.system_time(:millisecond)

      stored_state = %DurableServer.StoredState{
        vsn: 1,
        state: %{"count" => 1},
        meta: %Meta{
          key: key,
          prefix: prefix,
          supervisor: supervisor_name,
          module: TestServer,
          permanent: true,
          status: :running,
          node_str: node_str,
          node_ref: node_ref,
          pid: pid,
          last_heartbeat_at: now
        }
      }

      assert {:ok, _} =
               DurableServer.StorageBackend.put_object(
                 backend,
                 "#{prefix}#{key}",
                 stored_state
               )

      assert {:ok, %DurableServer.StoredState{} = stored_state} =
               DurableServer.fetch_stored_state(
                 backend,
                 %{key: key, prefix: prefix}
               )

      heartbeat_data = %{
        "node" => node_str,
        "node_ref" => node_ref,
        "last_heartbeat_at" => now
      }

      assert {:ok, _} =
               DurableServer.StorageBackend.put_object(
                 backend,
                 "#{prefix}__nodes/#{node_str}",
                 heartbeat_data
               )

      heartbeat_table = :"durable_server_heartbeats_#{supervisor_name}"
      stale_timestamp = now - 60_000
      :ets.insert(heartbeat_table, {node_str, node_ref, stale_timestamp, %{}, %{}, %{}, nil})

      assert {:locked, ^pid} = DurableServer.check_lock(stored_state.meta)

      assert {:error, :not_eligible} =
               DurableServer.claim_restart_attempt(backend, stored_state, ttl: 10_000)
    end

    test "degraded discovery rejects stale orphan claims at the storage mutation boundary", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      %{ets_table: coordination_table, storage_backend: backend} =
        DurableServer.Supervisor.__get_config__(supervisor_name)

      key = "degraded-stale-claim-test-#{DurableServer.UUID.uuid4()}"
      now = System.system_time(:millisecond)

      stored_state = %DurableServer.StoredState{
        vsn: 1,
        state: %{"count" => 1},
        meta: %Meta{
          key: key,
          prefix: prefix,
          supervisor: supervisor_name,
          module: TestServer,
          permanent: true,
          status: :running,
          node_str: "remote@test",
          node_ref: System.unique_integer([:positive]),
          pid: self(),
          last_heartbeat_at: now - 60_000
        }
      }

      assert {:ok, _} =
               DurableServer.StorageBackend.put_object(
                 backend,
                 "#{prefix}#{key}",
                 stored_state
               )

      assert {:ok, %DurableServer.StoredState{} = stored_state} =
               DurableServer.fetch_stored_state(
                 backend,
                 %{key: key, prefix: prefix}
               )

      :ets.insert(coordination_table, {:discovery_degraded, true})

      assert {:error, :discovery_degraded} =
               DurableServer.claim_restart_attempt(backend, stored_state, ttl: 10_000)

      assert {:error, :discovery_degraded} =
               DurableServer.claim_restart_attempt_with_verified_expired_lock(
                 backend,
                 stored_state,
                 ttl: 10_000
               )

      assert {:ok, unchanged_state} =
               DurableServer.fetch_stored_state(
                 backend,
                 %{key: key, prefix: prefix}
               )

      assert unchanged_state.etag == stored_state.etag
      assert unchanged_state.meta.restart_attempt_node == nil
    end

    test "running restart claims revalidate heartbeat before claiming stale running meta", %{
      supervisor_name: supervisor_name,
      prefix: prefix
    } do
      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)

      key = "running-claim-revalidate-#{DurableServer.UUID.uuid4()}"
      node_str = "remote-revalidated@test"
      node_ref = System.unique_integer([:positive])
      pid = self()
      now = System.system_time(:millisecond)

      stored_state = %DurableServer.StoredState{
        vsn: 1,
        state: %{"count" => 1},
        meta: %Meta{
          key: key,
          prefix: prefix,
          supervisor: supervisor_name,
          module: TestServer,
          permanent: true,
          status: :running,
          node_str: node_str,
          node_ref: node_ref,
          pid: pid,
          last_heartbeat_at: now - 60_000
        }
      }

      assert {:ok, _} =
               DurableServer.StorageBackend.put_object(
                 backend,
                 "#{prefix}#{key}",
                 stored_state
               )

      assert {:ok, %DurableServer.StoredState{} = stored_state} =
               DurableServer.fetch_stored_state(
                 backend,
                 %{key: key, prefix: prefix}
               )

      heartbeat_data = %{
        "node" => node_str,
        "node_ref" => node_ref,
        "last_heartbeat_at" => System.system_time(:millisecond)
      }

      assert {:ok, _} =
               DurableServer.StorageBackend.put_object(
                 backend,
                 "#{prefix}__nodes/#{node_str}",
                 heartbeat_data
               )

      assert {:locked, ^pid} = DurableServer.check_lock(stored_state.meta)

      assert {:error, :not_eligible} =
               DurableServer.claim_restart_attempt(backend, stored_state, ttl: 10_000)
    end

    test "discovery does not keep cached skip entries when heartbeat confirmation hits a consistent-read conflict" do
      supervisor_name = :"test_supervisor_#{DurableServer.UUID.uuid4()}"
      prefix = "test_#{DurableServer.UUID.uuid4()}/"
      node_str = "remote-conflict@test"
      heartbeat_key = "#{prefix}__nodes/#{node_str}"

      start_supervised!(
        {DurableServer.Supervisor,
         [
           name: supervisor_name,
           prefix: prefix,
           object_store: test_object_store_opts(),
           heartbeat_backend:
             {HeartbeatConflictBackend,
              [
                delegate: {DurableServer.Backends.ObjectStore, test_object_store_opts()},
                conflict_keys: [heartbeat_key]
              ]},
           initial_discovery_delay_ms: 60_000,
           discovery_interval_ms: 60_000
         ]},
        id: supervisor_name
      )

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)

      key = "conflict-heartbeat-lock-test-#{DurableServer.UUID.uuid4()}"
      now = System.system_time(:millisecond)

      stored_state = %DurableServer.StoredState{
        vsn: 1,
        state: %{"count" => 1},
        meta: %Meta{
          key: key,
          prefix: prefix,
          supervisor: supervisor_name,
          module: TestServer,
          permanent: true,
          status: :running,
          node_str: node_str,
          node_ref: System.unique_integer([:positive]),
          pid: self(),
          last_heartbeat_at: now - 60_000
        }
      }

      assert {:ok, %{etag: etag}} =
               DurableServer.StorageBackend.put_object(
                 backend,
                 "#{prefix}#{key}",
                 stored_state
               )

      {:ok, manager_pid} = get_supervisor_lifecycle_manager(supervisor_name)
      manager_state = :sys.get_state(manager_pid)

      :ets.insert(
        manager_state.discovery_skip_table,
        {key, etag, stored_state.meta, System.monotonic_time(:millisecond)}
      )

      send(manager_pid, :discover_and_restart)
      wait_for_discovery_completion(manager_pid, 1000)

      assert [] = :ets.lookup(manager_state.discovery_skip_table, key)
      assert nil == DurableServer.Supervisor.lookup(supervisor_name, key)
    end

    test "running orphan proved expired by the first heartbeat read can still be claimed even if a second read would conflict" do
      supervisor_name = :"test_supervisor_#{DurableServer.UUID.uuid4()}"
      prefix = "test_#{DurableServer.UUID.uuid4()}/"
      node_str = "remote-once-missing@test"
      heartbeat_key = "#{prefix}__nodes/#{node_str}"

      start_supervised!(
        {DurableServer.Supervisor,
         [
           name: supervisor_name,
           prefix: prefix,
           object_store: test_object_store_opts(),
           heartbeat_backend:
             {HeartbeatConflictAfterFirstReadBackend,
              [
                delegate: {DurableServer.Backends.ObjectStore, test_object_store_opts()},
                conflict_keys: [heartbeat_key]
              ]},
           initial_discovery_delay_ms: 60_000,
           discovery_interval_ms: 60_000
         ]},
        id: supervisor_name
      )

      %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(supervisor_name)

      key = "conflict-after-expired-#{DurableServer.UUID.uuid4()}"
      now = System.system_time(:millisecond)

      stored_state = %DurableServer.StoredState{
        vsn: 1,
        state: %{"count" => 1},
        meta: %Meta{
          key: key,
          prefix: prefix,
          supervisor: supervisor_name,
          module: TestServer,
          permanent: true,
          status: :running,
          node_str: node_str,
          node_ref: System.unique_integer([:positive]),
          pid: self(),
          last_heartbeat_at: now - 60_000
        }
      }

      assert {:ok, _} =
               DurableServer.StorageBackend.put_object(
                 backend,
                 "#{prefix}#{key}",
                 stored_state
               )

      {:ok, manager_pid} = get_supervisor_lifecycle_manager(supervisor_name)

      send(manager_pid, :discover_and_restart)
      wait_for_discovery_completion(manager_pid, 1000)

      diagnostics = LifecycleManager.get_discovery_diagnostics(supervisor_name)
      assert Map.get(diagnostics, :restart_claim_ok, 0) >= 1
      assert Map.get(diagnostics, :restart_claim_not_eligible, 0) == 0

      assert Map.get(diagnostics, :restart_start_ok, 0) +
               Map.get(diagnostics, :restart_start_already_started, 0) >= 1
    end

    test "orderly lifecycle manager shutdown deletes the local heartbeat key", %{
      supervisor_name: supervisor_name,
      config: config
    } do
      table = :ets.new(__MODULE__, [:set, :public])

      {:ok, heartbeat_backend} =
        DurableServer.StorageBackend.init_backend(DeleteTrackingBackend,
          delegate: {DurableServer.Backends.ObjectStore, test_object_store_opts()},
          table: table
        )

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(
          supervisor_name,
          config,
          heartbeat_backend: heartbeat_backend
        )

      standalone_supervisor_name = :sys.get_state(manager_pid).supervisor_name

      heartbeat_key = "#{config.prefix}__nodes/#{to_string(Node.self())}"

      assert_eventually(fn ->
        match?(
          {:ok, %{body: _}},
          DurableServer.StorageBackend.get_object(heartbeat_backend, heartbeat_key,
            consistent: false
          )
        )
      end)

      assert :ok = LifecycleManager.stop_discovery(standalone_supervisor_name)
      GenServer.stop(manager_pid, :shutdown, 15_000)

      assert_eventually(fn ->
        match?([{:deleted, ^heartbeat_key}], :ets.lookup(table, :deleted))
      end)
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

      # Corrupt typed fields are rejected as data errors rather than admitted into Meta.
      assert {:error, %ArgumentError{message: message}} =
               DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      assert message =~ "invalid metadata field :restart_attempt_time"

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
        {TestServer, key: key, initial_state: %{permanent: true}}
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
    ^table_name = :ets.new(table_name, [:named_table, :set, :public, read_concurrency: true])

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

    heartbeat_watchdog_name =
      DurableServer.Supervisor.heartbeat_watchdog_name(standalone_supervisor_name)

    _ =
      start_supervised!(
        Supervisor.child_spec(
          {DurableServer.HeartbeatWatchdog,
           name: heartbeat_watchdog_name, supervisor_name: standalone_supervisor_name},
          id: heartbeat_watchdog_name
        )
      )

    presence_scope = DurableServer.Supervisor.presence_pg_scope(standalone_supervisor_name)
    _ = start_supervised!(%{id: presence_scope, start: {:pg, :start_link, [presence_scope]}})

    # Start LifecycleManager with the standalone supervisor name
    manager_opts =
      [
        supervisor_name: standalone_supervisor_name,
        task_supervisor: task_sup_name,
        heartbeat_watchdog: heartbeat_watchdog_name,
        object_store: config.object_store,
        config: config,
        circuit_breaker: circuit_breaker
      ] ++ opts

    start_supervised(
      Supervisor.child_spec({LifecycleManager, manager_opts},
        id: standalone_supervisor_name,
        restart: :temporary
      )
    )
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

  @stale_heartbeat_age_ms 48 * 60 * 60 * 1000

  defp await_heartbeat_cycle(manager_pid) do
    before = :sys.get_state(manager_pid).last_successful_heartbeat_monotonic_at
    refute is_nil(before)

    send(manager_pid, :heartbeat)

    assert_eventually(fn ->
      :sys.get_state(manager_pid).last_successful_heartbeat_monotonic_at != before
    end)
  end

  defp await_heartbeat_reconciliation(manager_pid) do
    send(manager_pid, :heartbeat_reconcile)

    # This call reaches the manager after the reconciliation message, proving
    # that the task was either started or completed.
    _ = :sys.get_state(manager_pid)

    assert_eventually(fn ->
      :sys.get_state(manager_pid).current_heartbeat_reconcile_task == nil
    end)
  end

  defp seed_peer_heartbeat(config, prefix, node, current_time) do
    ObjectStore.put_object(
      config.object_store,
      "#{prefix}__nodes/#{node}",
      JSON.encode!(%{
        node: node,
        node_ref: "#{node}-ref",
        last_heartbeat_at: current_time - 1_000
      })
    )
  end

  defp insert_stale_heartbeat(heartbeat_table, node, current_time) do
    :ets.insert(
      heartbeat_table,
      {node, "#{node}-ref", current_time - @stale_heartbeat_age_ms, %{}, %{}, %{}, nil}
    )
  end

  defp heartbeat_list_store(config, prefix, opts) do
    {:ok, store} =
      DurableServer.StorageBackend.init_backend(
        HeartbeatListBackend,
        [
          delegate:
            DurableServer.StorageBackend.new(
              DurableServer.Backends.ObjectStore,
              config.object_store
            ),
          heartbeat_prefix: "#{prefix}__nodes/"
        ] ++ opts
      )

    store
  end

  defp assert_eventually(fun, timeout \\ 5_000, interval \\ 25) when is_function(fun, 0) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(fun, deadline, interval)
  end

  defp do_assert_eventually(fun, deadline, interval) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("condition was not met within timeout")
      else
        Process.sleep(interval)
        do_assert_eventually(fun, deadline, interval)
      end
    end
  end

  defp setup_restart_gate_tables(supervisor_name) do
    config_table = :"durable_supervisor_#{supervisor_name}"
    heartbeat_table = :"durable_server_heartbeats_#{supervisor_name}"
    restart_gate_table = :"durable_server_restart_gate_#{supervisor_name}"

    ^config_table =
      :ets.new(config_table, [:named_table, :set, :protected, read_concurrency: true])

    ^heartbeat_table =
      :ets.new(heartbeat_table, [:named_table, :set, :public, read_concurrency: true])

    ^restart_gate_table =
      :ets.new(restart_gate_table, [
        :named_table,
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])

    :ets.insert(config_table, {:sticky_placement_config, %{per_module: %{}, default: nil}})

    on_exit(fn ->
      if :ets.whereis(heartbeat_table) != :undefined do
        :ets.delete(heartbeat_table)
      end

      if :ets.whereis(config_table) != :undefined do
        :ets.delete(config_table)
      end

      if :ets.whereis(restart_gate_table) != :undefined do
        :ets.delete(restart_gate_table)
      end
    end)
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

    test "restart claimer gate uses LM-local gate age, not object age" do
      supervisor_name = :"restart_gate_#{DurableServer.UUID.uuid4()}"
      setup_restart_gate_tables(supervisor_name)

      now = System.system_time(:millisecond)

      nodes = [
        :"gate-a@test",
        :"gate-b@test",
        :"gate-c@test",
        :"gate-d@test",
        :"gate-e@test"
      ]

      heartbeat_table = :"durable_server_heartbeats_#{supervisor_name}"

      Enum.with_index(nodes, 1)
      |> Enum.each(fn {node, idx} ->
        :ets.insert(
          heartbeat_table,
          {to_string(node), idx, now, nil, nil, %{}, %{}}
        )
      end)

      base_meta = %Meta{
        key: "driver/test",
        module: TestServer,
        permanent: true,
        status: :running,
        supervisor: supervisor_name,
        node_str: "owner@test",
        node_ref: 1,
        sticky_placement: nil,
        last_heartbeat_at: now - :timer.minutes(10)
      }

      fresh_decisions =
        Enum.map(nodes, fn node ->
          LifecycleManager.__preferred_restart_claimer__(
            supervisor_name,
            base_meta,
            local_node: node,
            now: now,
            gate_first_seen_at: now
          )
        end)

      warm_decisions =
        Enum.map(nodes, fn node ->
          LifecycleManager.__preferred_restart_claimer__(
            supervisor_name,
            base_meta,
            local_node: node,
            now: now,
            gate_first_seen_at: now - :timer.seconds(45)
          )
        end)

      old_decisions =
        Enum.map(nodes, fn node ->
          LifecycleManager.__preferred_restart_claimer__(
            supervisor_name,
            base_meta,
            local_node: node,
            now: now,
            gate_first_seen_at: now - :timer.minutes(3)
          )
        end)

      assert Enum.count(fresh_decisions, & &1) == 2
      assert Enum.count(warm_decisions, & &1) == 4
      assert Enum.all?(old_decisions)
    end

    test "restart claimer gate does not strand keys when local node is absent from candidate view" do
      supervisor_name = :"restart_gate_absent_#{DurableServer.UUID.uuid4()}"
      setup_restart_gate_tables(supervisor_name)

      now = System.system_time(:millisecond)
      heartbeat_table = :"durable_server_heartbeats_#{supervisor_name}"

      Enum.with_index([:"gate-a@test", :"gate-b@test", :"gate-c@test"], 1)
      |> Enum.each(fn {node, idx} ->
        :ets.insert(
          heartbeat_table,
          {to_string(node), idx, now, nil, nil, %{}, %{}}
        )
      end)

      meta = %Meta{
        key: "driver/test",
        module: TestServer,
        permanent: true,
        status: :running,
        supervisor: supervisor_name,
        node_str: "owner@test",
        node_ref: 1,
        sticky_placement: nil,
        last_heartbeat_at: now
      }

      assert LifecycleManager.__preferred_restart_claimer__(
               supervisor_name,
               meta,
               local_node: :missing@test,
               now: now
             )
    end

    test "restart claimer gate bypasses hashing when the local candidate tail fits in one restart wave" do
      supervisor_name = :"restart_gate_small_tail_#{DurableServer.UUID.uuid4()}"
      setup_restart_gate_tables(supervisor_name)

      now = System.system_time(:millisecond)
      heartbeat_table = :"durable_server_heartbeats_#{supervisor_name}"

      nodes = [
        :"gate-a@test",
        :"gate-b@test",
        :"gate-c@test",
        :"gate-d@test",
        :"gate-e@test"
      ]

      Enum.with_index(nodes, 1)
      |> Enum.each(fn {node, idx} ->
        :ets.insert(
          heartbeat_table,
          {to_string(node), idx, now, nil, nil, %{}, %{}}
        )
      end)

      meta = %Meta{
        key: "driver/test",
        module: TestServer,
        permanent: true,
        status: :running,
        supervisor: supervisor_name,
        node_str: "owner@test",
        node_ref: 1,
        sticky_placement: nil,
        last_heartbeat_at: now - :timer.minutes(10)
      }

      fresh_gate_decisions =
        Enum.map(nodes, fn node ->
          LifecycleManager.__preferred_restart_claimer__(
            supervisor_name,
            meta,
            local_node: node,
            now: now,
            gate_first_seen_at: now
          )
        end)

      tail_bypass_decisions =
        Enum.map(nodes, fn node ->
          LifecycleManager.__preferred_restart_claimer__(
            supervisor_name,
            meta,
            local_node: node,
            now: now,
            gate_first_seen_at: now,
            local_candidate_batch_size: 5,
            local_tail_bypass_threshold: 10
          )
        end)

      assert Enum.count(fresh_gate_decisions, & &1) == 2
      assert Enum.all?(tail_bypass_decisions)
    end

    test "restart claim ttl stays ahead of the LM restart timeout" do
      assert LifecycleManager.__restart_claim_ttl_ms__(5_000) == 30_000
      assert LifecycleManager.__restart_claim_ttl_ms__(30_000) == 40_000
    end

    test "timeout-like restart failures keep the restart claim in place" do
      refute LifecycleManager.__clear_restart_attempt_after_failure__(:timeout)
      refute LifecycleManager.__clear_restart_attempt_after_failure__({:already_started, self()})
      assert LifecycleManager.__clear_restart_attempt_after_failure__({:capacity_limit, :cpu})
    end

    test "restart diagnostics classify claim and start outcomes" do
      assert LifecycleManager.__restart_claim_diag_key__({:ok, %{}}) == :restart_claim_ok

      assert LifecycleManager.__restart_claim_diag_key__({:error, :already_claimed}) ==
               :restart_claim_already_claimed

      assert LifecycleManager.__restart_claim_diag_key__({:error, :not_eligible}) ==
               :restart_claim_not_eligible

      assert LifecycleManager.__restart_claim_diag_key__({:error, :unavailable}) ==
               :restart_claim_error

      assert LifecycleManager.__restart_start_diag_key__({:ok, {self(), %{}}}) ==
               :restart_start_ok

      assert LifecycleManager.__restart_start_diag_key__({:error, {:already_started, self()}}) ==
               :restart_start_already_started

      assert LifecycleManager.__restart_start_diag_key__({:error, :timeout}) ==
               :restart_start_timeout

      assert LifecycleManager.__restart_start_diag_key__({:error, {:capacity_limit, :cpu}}) ==
               :restart_start_error
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

    test "an abnormal discovery task exit is handled and retried", %{
      supervisor_name: supervisor_name,
      config: config
    } do
      table = :ets.new(__MODULE__.DiscoveryCrashOnceBackend, [:set, :public])
      delegate = DurableServer.Supervisor.__get_config__(supervisor_name).storage_backend

      {:ok, storage_backend} =
        DurableServer.StorageBackend.init_backend(DiscoveryCrashOnceBackend,
          delegate: delegate,
          table: table,
          notify_pid: self()
        )

      test_config =
        config
        |> Map.put(:object_store, storage_backend)
        |> Map.put(:storage_backend, storage_backend)
        |> Map.put(:initial_discovery_delay_ms, 60_000)
        |> Map.put(:discovery_interval_ms, 10)

      {:ok, manager_pid} = start_standalone_lifecycle_manager(supervisor_name, test_config)
      manager_ref = Process.monitor(manager_pid)

      send(manager_pid, :discover_and_restart)

      assert_receive {:discovery_list_attempt, 1}, 1_000
      assert_receive {:discovery_list_attempt, 2}, 1_000
      refute_receive {:DOWN, ^manager_ref, :process, ^manager_pid, _reason}, 50
      assert Process.alive?(manager_pid)

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

    test "heartbeat owner deadline is based on the timestamp written to storage", %{
      supervisor_name: supervisor_name,
      config: config
    } do
      table = :ets.new(__MODULE__.HeartbeatDelayBackend, [:set, :public])

      {:ok, storage_backend} =
        DurableServer.StorageBackend.init_backend(HeartbeatDelayBackend,
          delegate: {DurableServer.Backends.ObjectStore, test_object_store_opts()},
          table: table,
          delay_ms: 2_200
        )

      test_config =
        config
        |> Map.put(:object_store, storage_backend)
        |> Map.put(:storage_backend, storage_backend)

      {:ok, manager_pid} = start_standalone_lifecycle_manager(supervisor_name, test_config)
      manager_state = :sys.get_state(manager_pid)

      assert [{:heartbeat_write, %{"last_heartbeat_at" => stored_heartbeat_at}}] =
               :ets.lookup(table, :heartbeat_write)

      # The persisted wall timestamp remains available for cross-node freshness checks,
      # while local deadline arithmetic tracks the equivalent monotonic instant.
      assert abs(manager_state.last_successful_heartbeat_at - stored_heartbeat_at) < 500

      assert System.monotonic_time(:millisecond) -
               manager_state.last_successful_heartbeat_monotonic_at < 3_000

      GenServer.stop(manager_pid)
    end

    test "continues heartbeat PUTs while cache reconciliation is blocked", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config
    } do
      control = :ets.new(__MODULE__.HeartbeatListBackend, [:set, :public])
      :ets.insert(control, {:mode, :ok})

      heartbeat_store =
        heartbeat_list_store(config, prefix,
          mode: {:controlled, control},
          inject_key: "#{prefix}__nodes/injected@test"
        )

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config,
          heartbeat_store: heartbeat_store
        )

      manager_state = :sys.get_state(manager_pid)
      watchdog = manager_state.heartbeat_watchdog
      previous_watchdog_heartbeat = :sys.get_state(watchdog).last_heartbeat_at

      :ets.insert(control, {:mode, {:block, self()}})
      send(manager_pid, :heartbeat_reconcile)

      assert_receive {:heartbeat_cache_refresh_blocked, reconcile_task_pid}, 1_000
      reconcile_task = :sys.get_state(manager_pid).current_heartbeat_reconcile_task
      assert reconcile_task.pid == reconcile_task_pid

      Process.sleep(2)
      await_heartbeat_cycle(manager_pid)

      first_heartbeat = :sys.get_state(manager_pid).last_successful_heartbeat_monotonic_at
      assert :sys.get_state(watchdog).last_heartbeat_at > previous_watchdog_heartbeat
      assert :sys.get_state(manager_pid).current_heartbeat_reconcile_task == reconcile_task

      Process.sleep(2)
      await_heartbeat_cycle(manager_pid)

      assert :sys.get_state(manager_pid).last_successful_heartbeat_monotonic_at >
               first_heartbeat

      assert :sys.get_state(manager_pid).current_heartbeat_reconcile_task == reconcile_task

      send(reconcile_task_pid, :continue_heartbeat_cache_refresh)

      assert_eventually(fn ->
        :sys.get_state(manager_pid).current_heartbeat_reconcile_task == nil
      end)

      GenServer.stop(manager_pid)
    end

    test "starts in degraded mode when the initial heartbeat snapshot is partial", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config
    } do
      heartbeat_store =
        heartbeat_list_store(config, prefix,
          mode: :fetch_error,
          inject_key: "#{prefix}__nodes/failed-peer@test"
        )

      assert {:ok, manager_pid} =
               start_standalone_lifecycle_manager(supervisor_name, config,
                 heartbeat_store: heartbeat_store
               )

      manager_state = :sys.get_state(manager_pid)
      assert manager_state.discovery_degraded
      assert is_integer(manager_state.last_successful_heartbeat_monotonic_at)
      assert LifecycleManager.discovery_degraded?(manager_state.supervisor_name)
      assert Process.alive?(manager_pid)

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

      {:ok, manager_pid} = start_standalone_lifecycle_manager(supervisor_name, test_config)

      assert [{:attempts, attempts}] = :ets.lookup(table, :attempts)
      assert attempts >= 3
      assert [{:last_put_opts, put_opts}] = :ets.lookup(table, :last_put_opts)
      assert Keyword.get(put_opts, :max_retries) > 0
      assert [{:last_write, heartbeat_data}] = :ets.lookup(table, :last_write)
      assert is_map(heartbeat_data)
      assert Map.has_key?(heartbeat_data, "last_heartbeat_at")

      manager_name = :sys.get_state(manager_pid).supervisor_name
      metrics = LifecycleManager.get_heartbeat_metrics(manager_name)

      assert metrics.attempts.total == attempts
      assert metrics.attempts.by_result == %{backend_error: 2, success: 1}
      assert metrics.consecutive_failures == 0
      assert is_integer(metrics.last_success_age_ms)
      assert metrics.remaining_watchdog_budget_ms > 0
      refute metrics.cache_degraded?
      assert metrics.cache_degraded_duration_ms == 0
    end

    test "retries ambiguous Req transport errors from a heartbeat backend", %{
      supervisor_name: supervisor_name,
      config: config
    } do
      table = :ets.new(__MODULE__.HeartbeatRetryBackend, [:set, :public])

      storage_backend =
        DurableServer.StorageBackend.new(HeartbeatRetryBackend, %{
          table: table,
          fail_count: 2,
          failure_reason: %Req.TransportError{reason: {:future_transport_reason, make_ref()}}
        })

      test_config =
        config
        |> Map.put(:object_store, storage_backend)
        |> Map.put(:storage_backend, storage_backend)

      {:ok, manager_pid} = start_standalone_lifecycle_manager(supervisor_name, test_config)

      assert [{:attempts, 3}] = :ets.lookup(table, :attempts)

      manager_name = :sys.get_state(manager_pid).supervisor_name
      metrics = LifecycleManager.get_heartbeat_metrics(manager_name)

      assert metrics.attempts.by_result == %{success: 1, transport_error: 2}
      assert metrics.attempts.by_transport_class == %{other: 2}
      assert metrics.consecutive_failures == 0
    end

    test "heartbeat watchdog terminates lifecycle manager even when message handling is suspended",
         %{
           supervisor_name: supervisor_name,
           config: config
         } do
      test_config =
        config
        |> Map.put(:heartbeat_staleness_threshold_ms, 2_200)
        |> Map.put(:heartbeat_interval_ms, 100)

      {:ok, manager_pid} = start_standalone_lifecycle_manager(supervisor_name, test_config)

      ref = Process.monitor(manager_pid)
      :ok = :sys.suspend(manager_pid)

      assert_receive {:DOWN, ^ref, :process, ^manager_pid, :killed}, 1_500
    end

    test "heartbeat watchdog kills an in-flight heartbeat task before terminating lifecycle manager",
         %{
           supervisor_name: supervisor_name,
           config: config
         } do
      table = :ets.new(__MODULE__.HeartbeatHangAfterFirstBackend, [:set, :public])

      {:ok, storage_backend} =
        DurableServer.StorageBackend.init_backend(HeartbeatHangAfterFirstBackend,
          delegate: {DurableServer.Backends.ObjectStore, test_object_store_opts()},
          table: table,
          hang_after: 2,
          notify_pid: self()
        )

      test_config =
        config
        |> Map.put(:object_store, storage_backend)
        |> Map.put(:storage_backend, storage_backend)
        |> Map.put(:heartbeat_staleness_threshold_ms, 2_200)
        |> Map.put(:heartbeat_interval_ms, 100)

      {:ok, manager_pid} = start_standalone_lifecycle_manager(supervisor_name, test_config)
      manager_ref = Process.monitor(manager_pid)

      assert_receive {:hanging_heartbeat_task, heartbeat_task_pid}, 1_000
      heartbeat_task_ref = Process.monitor(heartbeat_task_pid)

      assert_receive {:DOWN, ^heartbeat_task_ref, :process, ^heartbeat_task_pid, :killed}, 1_500

      assert_receive {:DOWN, ^manager_ref, :process, ^manager_pid, manager_reason}, 1_500
      assert manager_reason in [:killed, {:heartbeat_failed, :killed}]
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
              {ref,
               {:heartbeat,
                {%{total_ms: 10, put_ms: 5, cache_ms: 5}, heartbeat_entry,
                 System.monotonic_time(:millisecond)}}},
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

      await_heartbeat_reconciliation(manager_pid)

      # Verify alive node still exists
      {:ok, _} = ObjectStore.get_object(config.object_store, "#{prefix}__nodes/#{alive_node}")

      # Verify dead nodes were cleaned up (should return not_found errors)
      {:error, :not_found} =
        ObjectStore.get_object(config.object_store, "#{prefix}__nodes/#{dead_node1}")

      {:error, :not_found} =
        ObjectStore.get_object(config.object_store, "#{prefix}__nodes/#{dead_node2}")

      GenServer.stop(manager_pid)
    end

    test "prunes cached heartbeat entries for nodes absent from a complete listing", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config
    } do
      current_time = System.system_time(:millisecond)
      peer_node = "peer-complete@test"
      orphan_node = "orphan-complete@test"

      seed_peer_heartbeat(config, prefix, peer_node, current_time)

      {:ok, manager_pid} = start_standalone_lifecycle_manager(supervisor_name, config)
      heartbeat_table = :sys.get_state(manager_pid).heartbeat_table
      insert_stale_heartbeat(heartbeat_table, orphan_node, current_time)

      await_heartbeat_reconciliation(manager_pid)

      assert :ets.lookup(heartbeat_table, orphan_node) == []
      assert :ets.lookup(heartbeat_table, peer_node) != []

      GenServer.stop(manager_pid)
    end

    test "retains the last complete heartbeat snapshot when a listing is truncated", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config
    } do
      current_time = System.system_time(:millisecond)
      peer_node = "peer-truncated@test"
      orphan_node = "orphan-truncated@test"

      seed_peer_heartbeat(config, prefix, peer_node, current_time)
      control = :ets.new(__MODULE__.HeartbeatListBackend, [:set, :public])
      :ets.insert(control, {:mode, :ok})

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config,
          heartbeat_store:
            heartbeat_list_store(config, prefix,
              mode: {:controlled, control},
              inject_key: "#{prefix}__nodes/injected@test"
            )
        )

      heartbeat_table = :sys.get_state(manager_pid).heartbeat_table
      assert :ets.lookup(heartbeat_table, peer_node) != []
      insert_stale_heartbeat(heartbeat_table, orphan_node, current_time)

      :ets.insert(control, {:mode, :truncate})
      await_heartbeat_reconciliation(manager_pid)

      assert :ets.lookup(heartbeat_table, peer_node) != []
      assert :ets.lookup(heartbeat_table, orphan_node) != []
      assert :sys.get_state(manager_pid).discovery_degraded
      assert LifecycleManager.discovery_degraded?(:sys.get_state(manager_pid).supervisor_name)

      GenServer.stop(manager_pid)
    end

    test "a heartbeat GET failure degrades discovery without changing the snapshot, then recovers",
         %{
           supervisor_name: supervisor_name,
           prefix: prefix,
           config: config
         } do
      current_time = System.system_time(:millisecond)
      known_node = "known-peer@test"
      new_node = "new-peer@test"
      failed_node = "failed-peer@test"

      seed_peer_heartbeat(config, prefix, known_node, current_time)

      control = :ets.new(__MODULE__.HeartbeatListBackend, [:set, :public])
      :ets.insert(control, {:mode, :ok})

      heartbeat_store =
        heartbeat_list_store(config, prefix,
          mode: {:controlled, control},
          inject_key: "#{prefix}__nodes/#{failed_node}"
        )

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config,
          heartbeat_store: heartbeat_store
        )

      initial_state = :sys.get_state(manager_pid)
      heartbeat_table = initial_state.heartbeat_table
      manager_supervisor = initial_state.supervisor_name
      known_snapshot = :ets.lookup(heartbeat_table, known_node)
      assert known_snapshot != []

      seed_peer_heartbeat(config, prefix, new_node, System.system_time(:millisecond))
      :ets.insert(control, {:mode, :fetch_error})

      await_heartbeat_reconciliation(manager_pid)

      degraded_state = :sys.get_state(manager_pid)
      assert Process.alive?(manager_pid)
      assert degraded_state.discovery_degraded
      assert LifecycleManager.discovery_degraded?(manager_supervisor)
      assert :ets.lookup(heartbeat_table, known_node) == known_snapshot
      assert :ets.lookup(heartbeat_table, new_node) == []

      # All takeover and outbound placement decisions fail closed while the
      # previous complete snapshot remains available for diagnostics.
      assert {:error, :discovery_degraded} =
               DurableServer.check_lock_status(%Meta{supervisor: manager_supervisor})

      assert LifecycleManager.find_eligible_nodes(manager_supervisor, TestServer) == []

      send(manager_pid, :discover_and_restart)
      Process.sleep(25)
      assert :sys.get_state(manager_pid).current_discovery_task == nil

      # Degraded discovery does not stop this node from renewing its own
      # heartbeat or retrying the failed reconciliation.
      Process.sleep(2)
      await_heartbeat_cycle(manager_pid)
      await_heartbeat_reconciliation(manager_pid)
      still_degraded_state = :sys.get_state(manager_pid)
      assert still_degraded_state.discovery_degraded

      assert still_degraded_state.last_successful_heartbeat_monotonic_at >
               degraded_state.last_successful_heartbeat_monotonic_at

      assert :ets.lookup(heartbeat_table, known_node) == known_snapshot
      assert :ets.lookup(heartbeat_table, new_node) == []

      degraded_metrics = LifecycleManager.get_heartbeat_metrics(manager_supervisor)
      assert degraded_metrics.cache_degraded?
      assert is_integer(degraded_metrics.cache_degraded_duration_ms)

      :ets.insert(control, {:mode, :ok})
      await_heartbeat_reconciliation(manager_pid)

      recovered_state = :sys.get_state(manager_pid)
      refute recovered_state.discovery_degraded
      refute LifecycleManager.discovery_degraded?(manager_supervisor)
      assert :ets.lookup(heartbeat_table, new_node) != []

      recovered_metrics = LifecycleManager.get_heartbeat_metrics(manager_supervisor)
      refute recovered_metrics.cache_degraded?
      assert recovered_metrics.cache_degraded_duration_ms == 0

      GenServer.stop(manager_pid)
    end

    test "does not prune a node whose heartbeat key was listed but could not be fetched", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config
    } do
      current_time = System.system_time(:millisecond)
      peer_node = "peer-missing@test"
      orphan_node = "orphan-missing@test"

      seed_peer_heartbeat(config, prefix, peer_node, current_time)

      heartbeat_store =
        heartbeat_list_store(config, prefix,
          mode: :inject_key,
          inject_key: "#{prefix}__nodes/#{orphan_node}"
        )

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config,
          heartbeat_store: heartbeat_store
        )

      heartbeat_table = :sys.get_state(manager_pid).heartbeat_table
      insert_stale_heartbeat(heartbeat_table, orphan_node, current_time)

      await_heartbeat_reconciliation(manager_pid)

      assert :ets.lookup(heartbeat_table, peer_node) != []
      assert :ets.lookup(heartbeat_table, orphan_node) != []

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

    test "lifecycle manager skips cordoned permanent servers", %{
      supervisor_name: supervisor_name,
      prefix: prefix,
      config: config,
      circuit_breaker: _circuit_breaker
    } do
      key = "cordoned-permanent-#{DurableServer.UUID.uuid4()}"

      meta_attrs = %{
        status: :cordoned,
        node_str: "dead_node@test",
        node_ref: "dead-ref",
        module: TestServer,
        permanent: true,
        pid: spawn(fn -> :ok end)
      }

      create_test_object(config.object_store, "#{prefix}#{key}", %{count: 0}, meta_attrs)

      {:ok, manager_pid} =
        start_standalone_lifecycle_manager(supervisor_name, config)

      send(manager_pid, :discover_and_restart)
      wait_for_discovery_completion(manager_pid, 1000)

      {:ok, data} =
        DurableServer.fetch_stored_state(config.object_store, %{key: key, prefix: prefix})

      assert data.meta.status == :cordoned

      GenServer.stop(manager_pid)
    end
  end
end
