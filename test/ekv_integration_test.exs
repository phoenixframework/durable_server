defmodule DurableServer.EKVIntegrationTest do
  use ExUnit.Case, async: false

  alias DurableServer.StorageBackend

  @moduletag :integration
  @moduletag :capture_log

  defmodule CounterServer do
    use DurableServer, vsn: 1

    def dump_state(state), do: %{count: state.count}

    def load_state(_old_vsn, persisted_state) when is_map(persisted_state) do
      count = Map.get(persisted_state, :count, Map.get(persisted_state, "count", 0))
      %{count: count}
    end

    def init(%{count: count}) when is_integer(count), do: {:ok, %{count: count}}
    def init(_state), do: {:ok, %{count: 0}}

    def handle_call(:get_count, _from, %{count: count} = state) do
      {:reply, count, state}
    end

    def handle_call(:increment_and_sync, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, new_state.count, new_state, :sync}
    end
  end

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
         backend: {:ekv, [name: ekv_name]},
         graceful_shutdown_timeout_ms: 500
       ]}
    )

    on_exit(fn ->
      File.rm_rf(data_dir)
    end)

    {:ok, supervisor_name: supervisor_name, prefix: prefix}
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
  end

  defp ekv_mod, do: :"Elixir.EKV"
end
