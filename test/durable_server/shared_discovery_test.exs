defmodule DurableServer.SharedDiscoveryTest do
  # Starts distributed Erlang and attaches request observers on the peers.
  use ExUnit.Case, async: false
  alias DurableServer.{Backends, LifecycleManager, Meta, StoredState, TestCounterServer}
  import DurableServer.TestHelper

  @moduletag :capture_log

  for {mode, requests} <- [shared: 2, independent: 4, mixed: 3] do
    @tag mode: mode, requests: requests
    test "#{mode} discovery sees edits and recovers on the only eligible node after a scanner leaves",
         context do
      exercise_cluster(context)
    end
  end

  defp exercise_cluster(%{mode: mode, requests: requests}) do
    id = System.unique_integer([:positive])
    unless Node.alive?(), do: Node.start(:"discovery_test_#{id}", :shortnames)

    peers =
      for i <- 1..3 do
        {:ok, peer, remote} = :peer.start(%{name: :"discovery_peer_#{i}_#{id}"})
        on_exit(fn -> if Process.alive?(peer), do: :peer.stop(peer) end)
        :ok = :erpc.call(remote, :code, :add_paths, [:code.get_path()])
        {:ok, _} = :erpc.call(remote, Application, :ensure_all_started, [:durable_server])
        {remote, peer}
      end

    nodes = Enum.sort([node() | Keyword.keys(peers)])

    sharing =
      case mode do
        :independent -> []
        :mixed -> Enum.take(nodes, 3)
        :shared -> nodes
      end

    name = :"SharedDiscovery#{id}"
    prefix = "shared-discovery-#{id}/"
    cache = :"durable_server_discovery_skip_#{name}"
    store = test_object_store()

    record = %StoredState{
      vsn: 1,
      state: %{count: 37},
      meta: %Meta{
        permanent: true,
        node_str: "retired@localhost",
        supervisor: name,
        key: "record",
        prefix: prefix,
        module: TestCounterServer,
        status: :stopped_permanent
      }
    }

    assert {:ok, %{etag: etag}} =
             Backends.ObjectStore.put_object(store, prefix <> "record", record, [])

    for n <- nodes do
      opts = [
        name: name,
        prefix: prefix,
        object_store: test_object_store_opts(),
        max_children: %{TestCounterServer => if(n == List.last(nodes), do: 10, else: 0)},
        discovery_burst_count: 0,
        initial_discovery_delay_ms: 300_000,
        discovery_interval_ms: 300_000
      ]

      opts = if n in sharing, do: Keyword.put(opts, :shared_discovery, true), else: opts

      {:ok, _} =
        :erpc.call(n, Supervisor, :start_child, [
          DurableServer.AppSupervisor,
          {DurableServer.Supervisor, opts}
        ])

      on_exit(fn ->
        if n == node() or n in Node.list() do
          :erpc.call(n, Supervisor, :terminate_child, [DurableServer.AppSupervisor, name])
          :erpc.call(n, Supervisor, :delete_child, [DurableServer.AppSupervisor, name])
        end
      end)

      handler =
        :erpc.call(n, :telemetry_test, :attach_event_handlers, [self(), [[:finch, :recv, :stop]]])

      on_exit(fn ->
        if n == node() or n in Node.list(), do: :erpc.call(n, :telemetry, :detach, [handler])
      end)
    end

    # Wait until the supervisors have observed each other before counting requests.
    for n <- nodes do
      eventually(fn ->
        scope = DurableServer.Supervisor.presence_pg_scope(name)
        group = DurableServer.Supervisor.__supervisor_presence_group__(name)
        groups = :erpc.call(n, :pg, :which_groups, [scope])
        listing = Enum.find(groups, &match?({DurableServer.DiscoveryListing, _, _}, &1))

        length(:erpc.call(n, :pg, :get_members, [scope, group])) == 4 and
          length(:erpc.call(n, :pg, :get_members, [scope, listing])) == length(sharing)
      end)
    end

    for n <- nodes do
      discover(n, name)
      assert [{_, ^etag, _, _}] = :erpc.call(n, :ets, :lookup, [cache, "record"])
    end

    assert request_counts(prefix) == {requests, 4}

    assert {:ok, %{etag: next_etag}} =
             Backends.ObjectStore.put_object(
               store,
               prefix <> "record",
               %{record | state: %{count: 42}},
               []
             )

    for n <- nodes do
      discover(n, name)
      assert [{_, ^next_etag, _, _}] = :erpc.call(n, :ets, :lookup, [cache, "record"])
      assert nil == :erpc.call(n, DurableServer.Supervisor, :lookup, [name, "record"])
    end

    assert request_counts(prefix) == {requests, 4}

    for n <- nodes, do: discover(n, name)
    assert request_counts(prefix) == {requests, 0}

    # A write just after the scanners list can miss the follower's first sweep.
    # A second sweep, even immediately afterwards, must see the new key.
    for n <- Enum.take(nodes, 2), do: discover(n, name)

    assert {:ok, %{etag: late_etag}} =
             Backends.ObjectStore.put_object(store, prefix <> "late", record, [])

    eligible = List.last(nodes)
    discover(eligible, name)
    seen = :erpc.call(eligible, :ets, :lookup, [cache, "late"])

    if mode == :shared,
      do: assert(seen == []),
      else: assert(match?([{_, ^late_etag, _, _}], seen))

    discover(eligible, name)
    assert [{_, ^late_etag, _, _}] = :erpc.call(eligible, :ets, :lookup, [cache, "late"])

    # Only the last node has capacity. Killing an elected peer must still allow recovery.
    lost = Enum.find(Enum.take(nodes, 2), &Keyword.has_key?(peers, &1))
    :peer.stop(Keyword.fetch!(peers, lost))
    remaining = List.delete(nodes, lost)
    graceful = %{record | meta: %{record.meta | status: :stopped_graceful, key: "recover"}}
    assert {:ok, _} = Backends.ObjectStore.put_object(store, prefix <> "recover", graceful, [])
    for n <- remaining, do: discover(n, name)

    eventually(fn ->
      match?({_, _}, :erpc.call(eligible, DurableServer.Supervisor, :lookup, [name, "recover"]))
    end)

    {owner, _} = :erpc.call(eligible, DurableServer.Supervisor, :lookup, [name, "recover"])
    assert node(owner) == eligible
    assert GenServer.call(owner, :get_count) == 37

    for n <- remaining do
      eventually(fn ->
        match?({^owner, _}, :erpc.call(n, DurableServer.Supervisor, :lookup, [name, "recover"]))
      end)
    end
  end

  defp discover(n, name) do
    manager = {LifecycleManager.name(name), n}
    send(manager, :discover_and_restart)
    eventually(fn -> :sys.get_state(manager).current_discovery_task == nil end)
  end

  defp eventually(fun, attempts \\ 250)
  defp eventually(fun, 0), do: assert(fun.())

  defp eventually(fun, attempts) do
    unless fun.() do
      Process.sleep(20)
      eventually(fun, attempts - 1)
    end
  end

  defp request_counts(prefix, lists \\ 0, gets \\ 0) do
    receive do
      {[:finch, :recv, :stop], _, _, %{request: req}} ->
        query = URI.decode_query(req.query || "")

        cond do
          query["list-type"] == "2" and query["prefix"] == prefix ->
            request_counts(prefix, lists + 1, gets)

          req.method == "GET" and String.ends_with?(req.path, "/" <> prefix <> "record") ->
            request_counts(prefix, lists, gets + 1)

          true ->
            request_counts(prefix, lists, gets)
        end
    after
      0 -> {lists, gets}
    end
  end
end
