defmodule DurableServer.MirrorBackendE2ETest do
  use ExUnit.Case, async: false

  import DurableServer.TestHelper

  alias DurableServer.Backends.{EKVStore, MirrorStore, ObjectStore}
  alias DurableServer.StoredState
  alias DurableServer.StorageBackend
  alias DurableServer.TestCounterServer, as: CounterServer

  @moduletag :integration
  @moduletag :capture_log

  setup do
    unique_id = System.unique_integer([:positive, :monotonic])
    prefix = "mirror_e2e/#{unique_id}/"
    ekv_name = :"durable_mirror_e2e_#{unique_id}"
    data_dir = Path.join(System.tmp_dir!(), "durable_server_mirror_e2e_#{unique_id}")

    File.rm_rf(data_dir)

    object_store = test_object_store()

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

    {:ok, primary_backend} = StorageBackend.init_backend(ObjectStore, object_store)
    {:ok, secondary_backend} = StorageBackend.init_backend(EKVStore, name: ekv_name)
    :ok = StorageBackend.ensure_ready(primary_backend)
    :ok = StorageBackend.ensure_ready(secondary_backend)

    on_exit(fn ->
      File.rm_rf(data_dir)
    end)

    {:ok,
     prefix: prefix,
     object_store: object_store,
     ekv_name: ekv_name,
     primary_backend: primary_backend,
     secondary_backend: secondary_backend}
  end

  test "phase 1 shadow mode mirrors authoritative object store writes into EKV", context do
    supervisor_name = unique_supervisor_name("shadow")
    key = "shadow-counter"
    storage_key = storage_key(context.prefix, key)

    _supervisor =
      start_durable_supervisor!(
        {:shadow_phase, supervisor_name},
        supervisor_name,
        context.prefix,
        shadow_backend_spec(context)
      )

    {:ok, {pid, _meta}} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {CounterServer, %{key: key, count: 2}}
      )

    assert 3 == GenServer.call(pid, :increment_and_sync)

    assert_eventually(fn ->
      with true <-
             stored_object_has_count?(
               StorageBackend.get_object(context.primary_backend, storage_key),
               3
             ),
           true <-
             stored_object_has_count?(
               StorageBackend.get_object(context.secondary_backend, storage_key),
               3
             ) do
        true
      else
        _ -> false
      end
    end)
  end

  test "strict phase 1 boot fails when EKV is unavailable", context do
    supervisor_name = unique_supervisor_name("shadow_boot_fail")
    missing_ekv_name = :"missing_ekv_#{System.unique_integer([:positive, :monotonic])}"

    assert_raise RuntimeError, ~r/failed to start child with the spec/, fn ->
      start_durable_supervisor!(
        {:shadow_boot_fail, supervisor_name},
        supervisor_name,
        context.prefix,
        strict_shadow_backend_spec(context.object_store, missing_ekv_name)
      )
    end
  end

  test "strict phase 1 write fails when mirrored EKV write fails", context do
    unique_id = System.unique_integer([:positive, :monotonic])
    ekv_name = :"durable_mirror_write_fail_#{unique_id}"
    supervisor_name = unique_supervisor_name("shadow_write_fail")
    prefix = "mirror_e2e/write_fail/#{unique_id}/"
    key = "shadow-write-fail"
    data_dir = Path.join(System.tmp_dir!(), "durable_server_mirror_write_fail_#{unique_id}")

    File.rm_rf(data_dir)

    ekv_pid =
      start_supervised!(%{
        id: {:write_fail_ekv, ekv_name},
        restart: :temporary,
        start:
          {ekv_mod(), :start_link,
           [
             [
               name: ekv_name,
               data_dir: data_dir,
               cluster_size: 1,
               node_id: 1,
               log: false
             ]
           ]}
      })

    on_exit(fn ->
      File.rm_rf(data_dir)
    end)

    _supervisor =
      start_durable_supervisor!(
        {:shadow_write_fail, supervisor_name},
        supervisor_name,
        prefix,
        strict_shadow_backend_spec(context.object_store, ekv_name)
      )

    monitor_ref = Process.monitor(ekv_pid)
    :ok = GenServer.stop(ekv_pid)
    assert_receive {:DOWN, ^monitor_ref, :process, ^ekv_pid, _reason}, 5_000

    assert {:error, reason} =
             DurableServer.Supervisor.start_child(
               supervisor_name,
               {CounterServer, %{key: key, count: 0}}
             )

    assert match?({:mirror_failed, _}, reason) or match?({:noproc, _}, reason)
  end

  test "phase 2 backfill plus phase 3 read cutover uses EKV reads and keeps primary writes",
       context do
    supervisor_name = unique_supervisor_name("read_cutover")
    key = "backfilled-counter"
    new_key = "phase3-new-counter"
    storage_key = storage_key(context.prefix, key)

    phase0_supervisor =
      start_durable_supervisor!(
        {:phase0, supervisor_name},
        supervisor_name,
        context.prefix,
        {ObjectStore, context.object_store}
      )

    {:ok, {pid, _meta}} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {CounterServer, %{key: key, count: 11}}
      )

    assert 12 == GenServer.call(pid, :increment_and_sync)

    monitor_ref = Process.monitor(pid)
    assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, pid)
    assert_receive {:DOWN, ^monitor_ref, :process, ^pid, _reason}, 5_000

    assert_eventually(fn ->
      match?(
        {:ok,
         %{
           body: %StoredState{
             state: %{"count" => 12},
             meta: %{status: :stopped_graceful}
           }
         }},
        StorageBackend.get_object(context.primary_backend, storage_key)
      )
    end)

    assert {:error, :not_found} =
             StorageBackend.get_object(context.secondary_backend, storage_key)

    stop_supervisor!(phase0_supervisor, supervisor_name, context.prefix)

    backfill_prefix!(context.primary_backend, context.secondary_backend, context.prefix)

    assert stored_object_has_count?(
             StorageBackend.get_object(context.secondary_backend, storage_key),
             12
           )

    {:ok, %{body: %StoredState{} = primary_state, etag: primary_etag}} =
      StorageBackend.get_object(context.primary_backend, storage_key)

    stale_primary_state = %{primary_state | state: %{"count" => 1}}

    assert {:ok, _} =
             StorageBackend.put_object(
               context.primary_backend,
               storage_key,
               stale_primary_state,
               etag: primary_etag
             )

    _phase3_supervisor =
      start_durable_supervisor!(
        {:phase3, supervisor_name},
        supervisor_name,
        context.prefix,
        read_cutover_backend_spec(context)
      )

    config = DurableServer.Supervisor.__get_config__(supervisor_name)
    assert config.heartbeat_tracking_mode == :subscribe

    assert {:ok, %StoredState{state: %{"count" => 12}}} =
             DurableServer.fetch_stored_state(supervisor_name, %{key: key, prefix: context.prefix})

    {:ok, {pid, _meta}} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {CounterServer, %{key: new_key, count: 30}}
      )

    assert 31 == GenServer.call(pid, :increment_and_sync)

    new_storage_key = storage_key(context.prefix, new_key)

    assert {:ok, %{body: %StoredState{state: %{"count" => 1}}}} =
             StorageBackend.get_object(context.primary_backend, storage_key)

    assert stored_object_has_count?(
             StorageBackend.get_object(context.secondary_backend, storage_key),
             12
           )

    assert {:ok, %{body: %StoredState{state: %{"count" => 31}}}} =
             StorageBackend.get_object(context.primary_backend, new_storage_key)

    assert stored_object_has_count?(
             StorageBackend.get_object(context.secondary_backend, new_storage_key),
             31
           )
  end

  test "phase 4 write cutover keeps EKV authoritative and mirrors updates back to object store",
       context do
    supervisor_name = unique_supervisor_name("write_cutover")
    key = "write-cutover-counter"
    storage_key = storage_key(context.prefix, key)

    phase0_supervisor =
      start_durable_supervisor!(
        {:phase0, supervisor_name},
        supervisor_name,
        context.prefix,
        {ObjectStore, context.object_store}
      )

    {:ok, {pid, _meta}} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {CounterServer, %{key: key, count: 20}}
      )

    assert 21 == GenServer.call(pid, :increment_and_sync)

    monitor_ref = Process.monitor(pid)
    assert :ok = DurableServer.Supervisor.terminate_child(supervisor_name, pid)
    assert_receive {:DOWN, ^monitor_ref, :process, ^pid, _reason}, 5_000

    assert_eventually(fn ->
      match?(
        {:ok,
         %{
           body: %StoredState{
             state: %{"count" => 21},
             meta: %{status: :stopped_graceful}
           }
         }},
        StorageBackend.get_object(context.primary_backend, storage_key)
      )
    end)

    stop_supervisor!(phase0_supervisor, supervisor_name, context.prefix)

    backfill_prefix!(context.primary_backend, context.secondary_backend, context.prefix)

    _phase4_supervisor =
      start_durable_supervisor!(
        {:phase4, supervisor_name},
        supervisor_name,
        context.prefix,
        write_cutover_backend_spec(context)
      )

    {:ok, {restarted_pid, _meta}} =
      DurableServer.Supervisor.start_child(
        supervisor_name,
        {CounterServer, %{key: key}},
        existing: true
      )

    assert 21 == GenServer.call(restarted_pid, :get_count)

    {:ok, %{body: %StoredState{} = primary_state, etag: primary_etag}} =
      StorageBackend.get_object(context.primary_backend, storage_key)

    stale_primary_state = %{primary_state | state: %{"count" => 0}}

    assert {:ok, _} =
             StorageBackend.put_object(
               context.primary_backend,
               storage_key,
               stale_primary_state,
               etag: primary_etag
             )

    assert 22 == GenServer.call(restarted_pid, :increment_and_sync)

    assert_eventually(fn ->
      with true <-
             stored_object_has_count?(
               StorageBackend.get_object(context.secondary_backend, storage_key),
               22
             ),
           true <-
             stored_object_has_count?(
               StorageBackend.get_object(context.primary_backend, storage_key),
               22
             ) do
        true
      else
        _ -> false
      end
    end)
  end

  defp start_durable_supervisor!(id, supervisor_name, prefix, backend_spec) do
    start_supervised!(%{
      id: id,
      restart: :temporary,
      start:
        {DurableServer.Supervisor, :start_link,
         [
           [
             name: supervisor_name,
             prefix: prefix,
             backend: backend_spec,
             graceful_shutdown_timeout_ms: 500
           ]
         ]}
    })
  end

  defp stop_supervisor!(pid, supervisor_name, prefix)
       when is_pid(pid) and is_atom(supervisor_name) and is_binary(prefix) do
    monitor_ref = Process.monitor(pid)
    :ok = Supervisor.stop(pid)
    assert_receive {:DOWN, ^monitor_ref, :process, ^pid, _reason}, 5_000
    assert_eventually(fn -> Process.whereis(supervisor_name) == nil end)
    :persistent_term.erase({DurableServer.Supervisor, :prefix, prefix})
    :ok
  end

  defp backfill_prefix!(primary_backend, secondary_backend, prefix) do
    primary_backend
    |> StorageBackend.list_all_objects_stream(prefix)
    |> Enum.reject(&String.starts_with?(&1.key, "#{prefix}__nodes/"))
    |> Enum.each(fn %{key: key} ->
      {:ok, %{body: body}} = StorageBackend.get_object(primary_backend, key)
      {:ok, _} = StorageBackend.put_object(secondary_backend, key, body)
    end)
  end

  defp stored_object_has_count?({:ok, %{body: %StoredState{state: state}}}, expected_count)
       when is_map(state) do
    Map.get(state, "count") == expected_count or Map.get(state, :count) == expected_count
  end

  defp stored_object_has_count?(_, _expected_count), do: false

  defp shadow_backend_spec(context) do
    strict_shadow_backend_spec(context.object_store, context.ekv_name)
  end

  defp strict_shadow_backend_spec(object_store, ekv_name) do
    {MirrorStore,
     [
       primary: {ObjectStore, object_store},
       secondary: {EKVStore, [name: ekv_name]},
       read_preference: :primary,
       write_target: :primary,
       fallback_reads: true,
       promote_on_fallback: true,
       mirror_writes: true,
       mirror_mode: :required,
       secondary_required: true
     ]}
  end

  defp read_cutover_backend_spec(context) do
    {MirrorStore,
     [
       primary: {ObjectStore, context.object_store},
       secondary: {EKVStore, [name: context.ekv_name]},
       read_preference: :secondary,
       write_target: :primary,
       fallback_reads: true,
       promote_on_fallback: true,
       mirror_writes: true,
       mirror_mode: :required,
       secondary_required: true
     ]}
  end

  defp write_cutover_backend_spec(context) do
    {MirrorStore,
     [
       primary: {ObjectStore, context.object_store},
       secondary: {EKVStore, [name: context.ekv_name]},
       read_preference: :secondary,
       write_target: :secondary,
       fallback_reads: true,
       promote_on_fallback: true,
       mirror_writes: true,
       mirror_mode: :required,
       secondary_required: true
     ]}
  end

  defp storage_key(prefix, key), do: prefix <> key

  defp unique_supervisor_name(label) do
    :"durable_mirror_#{label}_#{System.unique_integer([:positive, :monotonic])}"
  end

  defp ekv_mod, do: :"Elixir.EKV"

  defp assert_eventually(fun, timeout \\ 5_000, interval \\ 25)
       when is_function(fun, 0) and is_integer(timeout) and timeout > 0 do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(fun, deadline, interval)
  end

  defp do_assert_eventually(fun, deadline, interval) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("eventual assertion timed out")
      else
        Process.sleep(interval)
        do_assert_eventually(fun, deadline, interval)
      end
    end
  end
end
