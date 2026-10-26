defmodule DurableServer.ExistingReadErrorTest do
  use ExUnit.Case, async: false

  alias DurableServer.StorageBackend
  alias DurableServer.TestCounterServer

  # An in-memory backend whose reads can be made to fail: `fail_reads/3` makes
  # the next `count` reads of a key return `{:error, reason}`, and every read
  # of a key is counted.
  defmodule FlakyReadBackend do
    @behaviour StorageBackend

    @impl true
    def init_backend(opts) do
      opts = Map.new(opts)

      {:ok,
       %{
         state: %{table: Map.fetch!(opts, :table)},
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
    def get_object(%{table: table}, key, opts) do
      :ets.update_counter(table, {:reads, key}, 1, {{:reads, key}, 0})
      :ets.insert(table, {{:read_opts, key}, opts})

      case :ets.lookup(table, {:fail, key}) do
        [{_, reason, count}] when count > 0 ->
          :ets.insert(table, {{:fail, key}, reason, count - 1})
          {:error, reason}

        _ ->
          case :ets.lookup(table, {:object, key}) do
            [{_, object}] -> {:ok, object}
            [] -> {:error, :not_found}
          end
      end
    end

    @impl true
    def list_all_objects_stream(%{table: table}, prefix, _opts) do
      table
      |> :ets.match({{:object, :"$1"}, %{etag: :"$2"}})
      |> Stream.filter(fn [key, _etag] -> String.starts_with?(key, prefix) end)
      |> Stream.map(fn [key, etag] -> %{key: key, etag: etag} end)
    end

    @impl true
    def put_object(%{table: table}, key, data, _opts) do
      object = %{body: data, etag: next_etag()}
      :ets.insert(table, {{:object, key}, object})
      {:ok, object}
    end

    @impl true
    def delete_object(%{table: table}, key) do
      case :ets.take(table, {:object, key}) do
        [_object] -> :ok
        [] -> {:error, :not_found}
      end
    end

    @impl true
    def try_claim(%{table: table}, key, body) do
      etag = next_etag()

      if :ets.insert_new(table, {{:object, key}, %{body: body, etag: etag}}),
        do: {:ok, {:claimed, etag}},
        else: {:error, :taken}
    end

    @impl true
    def update_object(%{table: table} = state, key, update_fn, _opts) do
      case :ets.lookup(table, {:object, key}) do
        [{_, object}] ->
          with {:ok, new_body} <- update_fn.(object), do: put_object(state, key, new_body, [])

        [] ->
          {:error, :not_found}
      end
    end

    @impl true
    def encode(_state, data), do: {:ok, data}

    @impl true
    def decode(_state, data), do: {:ok, data}

    def fail_reads(table, key, reason, count),
      do: :ets.insert(table, {{:fail, key}, reason, count})

    def reads(table, key) do
      case :ets.lookup(table, {:reads, key}) do
        [{_, count}] -> count
        [] -> 0
      end
    end

    def read_opts(table, key) do
      case :ets.lookup(table, {:read_opts, key}) do
        [{_, opts}] -> opts
        [] -> nil
      end
    end

    defp next_etag,
      do: Integer.to_string(System.unique_integer([:positive, :monotonic]))
  end

  setup do
    # Owned by a holder that stops after the supervisor, so the backend's
    # table outlives the supervisor's shutdown writes.
    holder = start_supervised!({Agent, fn -> :ets.new(__MODULE__, [:set, :public]) end})
    table = Agent.get(holder, & &1)
    name = :"existing_read_error_#{System.unique_integer([:positive, :monotonic])}"
    prefix = "existing_read_error/#{System.unique_integer([:positive, :monotonic])}/"

    start_supervised!(
      {DurableServer.Supervisor,
       [
         name: name,
         prefix: prefix,
         backend: {FlakyReadBackend, table: table},
         graceful_shutdown_timeout_ms: 500
       ]}
    )

    %{supervisor: name, prefix: prefix, table: table}
  end

  # Starts, syncs, and stops a server for `key`, so its state is stored.
  defp store(supervisor, key) do
    {:ok, {pid, _meta}} =
      DurableServer.Supervisor.start_child(
        supervisor,
        {TestCounterServer, key: key, initial_state: %{count: 0}}
      )

    1 = GenServer.call(pid, :increment_and_sync)
    ref = Process.monitor(pid)
    :ok = DurableServer.Supervisor.terminate_child(supervisor, key)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 5_000
  end

  defp child(key), do: {TestCounterServer, key: key, initial_state: %{count: 0}}

  for {fun, label} <- [ensure_started_child: "ensure_started_child", start_child: "start_child"] do
    describe "#{label} with existing: true" do
      test "retries a retryable read error and starts the stored server", ctx do
        key = "retry-#{unquote(fun)}"
        store(ctx.supervisor, key)
        storage_key = ctx.prefix <> key
        reads = FlakyReadBackend.reads(ctx.table, storage_key)
        FlakyReadBackend.fail_reads(ctx.table, storage_key, :bad_gateway, 1)

        assert {:ok, {pid, _meta}} =
                 apply(DurableServer.Supervisor, unquote(fun), [
                   ctx.supervisor,
                   child(key),
                   [existing: true]
                 ])

        assert GenServer.call(pid, :get_count) == 1
        assert FlakyReadBackend.reads(ctx.table, storage_key) - reads >= 2
      end

      test "a read that keeps failing is storage_unavailable, not not_found", ctx do
        key = "unavailable-#{unquote(fun)}"
        store(ctx.supervisor, key)
        storage_key = ctx.prefix <> key
        reads = FlakyReadBackend.reads(ctx.table, storage_key)
        reason = %Req.TransportError{reason: :timeout}
        FlakyReadBackend.fail_reads(ctx.table, storage_key, reason, 1_000)

        assert {:error, {:storage_unavailable, ^reason}} =
                 apply(DurableServer.Supervisor, unquote(fun), [
                   ctx.supervisor,
                   child(key),
                   [existing: true]
                 ])

        # Retried, and nothing was started for the key.
        assert FlakyReadBackend.reads(ctx.table, storage_key) - reads >= 2
        assert DurableServer.Supervisor.lookup(ctx.supervisor, key) == nil
      end

      test "an error that isn't retryable is storage_unavailable at once", ctx do
        key = "corrupt-#{unquote(fun)}"
        store(ctx.supervisor, key)
        storage_key = ctx.prefix <> key
        reads = FlakyReadBackend.reads(ctx.table, storage_key)
        reason = {:invalid_stored_state, :bad_meta}
        FlakyReadBackend.fail_reads(ctx.table, storage_key, reason, 1_000)

        assert {:error, {:storage_unavailable, ^reason}} =
                 apply(DurableServer.Supervisor, unquote(fun), [
                   ctx.supervisor,
                   child(key),
                   [existing: true]
                 ])

        assert FlakyReadBackend.reads(ctx.table, storage_key) - reads == 1
      end

      test "a definite miss is not_found, read once", ctx do
        key = "missing-#{unquote(fun)}"
        storage_key = ctx.prefix <> key

        assert {:error, :not_found} =
                 apply(DurableServer.Supervisor, unquote(fun), [
                   ctx.supervisor,
                   child(key),
                   [existing: true]
                 ])

        assert FlakyReadBackend.reads(ctx.table, storage_key) == 1
      end
    end
  end

  describe "get_server_info" do
    test "retries a retryable read error", ctx do
      key = "info-retry"
      store(ctx.supervisor, key)
      storage_key = ctx.prefix <> key
      reads = FlakyReadBackend.reads(ctx.table, storage_key)
      FlakyReadBackend.fail_reads(ctx.table, storage_key, :bad_gateway, 1)

      assert {:ok, %{key: ^key, user_state: user_state}} =
               DurableServer.Supervisor.get_server_info(ctx.supervisor, key)

      assert user_state in [%{count: 1}, %{"count" => 1}]
      assert FlakyReadBackend.reads(ctx.table, storage_key) - reads == 2
    end

    test "a read that keeps failing is storage_unavailable, not not_found", ctx do
      key = "info-unavailable"
      store(ctx.supervisor, key)
      storage_key = ctx.prefix <> key
      reason = %Req.TransportError{reason: :closed}
      FlakyReadBackend.fail_reads(ctx.table, storage_key, reason, 1_000)

      assert {:error, {:storage_unavailable, ^reason}} =
               DurableServer.Supervisor.get_server_info(ctx.supervisor, key)
    end

    test "a definite miss is not_found, read once", ctx do
      storage_key = ctx.prefix <> "info-missing"

      assert {:error, :not_found} =
               DurableServer.Supervisor.get_server_info(ctx.supervisor, "info-missing")

      assert FlakyReadBackend.reads(ctx.table, storage_key) == 1
    end

    test "reads eventually consistent by default, or consistent on request", ctx do
      key = "info-consistent"
      store(ctx.supervisor, key)
      storage_key = ctx.prefix <> key

      assert {:ok, _info} = DurableServer.Supervisor.get_server_info(ctx.supervisor, key)
      assert FlakyReadBackend.read_opts(ctx.table, storage_key)[:consistent] == false

      assert {:ok, _info} =
               DurableServer.Supervisor.get_server_info(ctx.supervisor, key, consistent: true)

      assert FlakyReadBackend.read_opts(ctx.table, storage_key)[:consistent] == true
    end
  end
end
