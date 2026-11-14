defmodule DurableServer.DistributedOwnershipTest do
  use ExUnit.Case, async: false

  alias DurableServer.DistributedTestClient, as: Client
  alias DurableServer.StoredState

  @moduletag :integration
  @moduletag :distributed
  @moduletag :capture_log
  @moduletag timeout: 90_000

  setup do
    id = DurableServer.UUID.uuid4()
    cookie = :"ownership_cookie_#{id}"
    store = :"ownership_store_#{id}"
    supervisor = :"ownership_supervisor_#{id}"
    tmp_dir = Path.expand("tmp/distributed_ownership/#{id}")
    on_exit(fn -> File.rm_rf!(tmp_dir) end)

    members =
      for index <- 1..3 do
        {:ok, peer, node} =
          :peer.start(%{
            name: :"ownership_#{id}_#{index}",
            connection: :standard_io,
            peer_down: :continue,
            shutdown: :halt,
            args: [~c"-setcookie", Atom.to_charlist(cookie), ~c"-connect_all", ~c"false"]
          })

        # Control uses stdio, so it remains available through a distribution
        # partition. Teardown halts only these disposable VMs, not the host.
        on_exit(fn -> :peer.stop(peer) end)
        :ok = :peer.call(peer, :code, :add_paths, [:code.get_path()])
        {:ok, _} = :peer.call(peer, Application, :ensure_all_started, [:ekv])
        {:ok, _} = :peer.call(peer, Application, :ensure_all_started, [:durable_server])
        :ok = :peer.call(peer, Logger, :configure, [[level: :warning]])

        {:ok, _} =
          :peer.call(peer, Client, :start_store, [store, Path.expand("#{tmp_dir}/#{index}")])

        %{peer: peer, node: node}
      end

    for member <- members, other <- members, member != other do
      assert :peer.call(member.peer, Node, :connect, [other.node])
    end

    await_cluster(members, store)

    for member <- members do
      assert {:ok, _} = rpc(member, :start_durable, [supervisor, store])
    end

    {:ok, members: members, store: store, supervisor: supervisor, cookie: cookie}
  end

  test "concurrent starts and acknowledged increments agree across three nodes", context do
    %{members: members, supervisor: supervisor} = context
    key = "concurrent"

    results =
      for member <- members, _ <- 1..4 do
        Task.async(fn -> rpc(member, :ensure_counter, [supervisor, key]) end)
      end
      |> Enum.map(&Task.await(&1, 15_000))

    pids =
      Enum.map(results, fn result ->
        assert {:ok, {pid, _}} = result
        pid
      end)

    assert [owner] = Enum.uniq(pids)

    acknowledgements =
      for member <- members do
        Task.async(fn -> rpc(member, :invoke, [owner, :increment_and_sync]) end)
      end
      |> Enum.map(&Task.await(&1, 15_000))

    assert Enum.sort(acknowledgements) == [{:ok, 1}, {:ok, 2}, {:ok, 3}]
    assert_durable_count(members, supervisor, key, 3)
  end

  test "a minority cannot acknowledge writes and stale tokens stay fenced after healing",
       context do
    %{members: [minority | majority] = members, supervisor: supervisor, cookie: cookie} = context
    key = "partition"
    assert {:ok, {old_owner, _}} = rpc(minority, :ensure_counter, [supervisor, key])
    assert node(old_owner) == minority.node
    assert {:ok, 1} = rpc(minority, :invoke, [old_owner, :increment_and_sync])
    assert {:ok, stale} = rpc(minority, :stored, [supervisor, key])

    isolate(minority, majority)

    assert {:indeterminate, _} = rpc(minority, :invoke, [old_owner, :increment_and_sync])
    assert :ok = rpc(hd(majority), :discover, [supervisor])
    new_owner = await_owner(majority, supervisor, key, old_owner)
    owner_member = Enum.find(majority, &(&1.node == node(new_owner)))

    assert {:ok, 1} = rpc(owner_member, :invoke, [new_owner, :get_count])
    assert {:ok, 2} = rpc(owner_member, :invoke, [new_owner, :increment_and_sync])
    assert_durable_count(majority, supervisor, key, 2)

    for member <- majority do
      assert true = :peer.call(minority.peer, :erlang, :set_cookie, [member.node, cookie])
      assert :peer.call(minority.peer, Node, :connect, [member.node])
    end

    await_cluster(members, context.store)

    await(fn -> not :peer.call(minority.peer, Process, :alive?, [old_owner]) end)
    await(fn -> :peer.call(minority.peer, DurableServer.Supervisor, :ready?, [supervisor]) end)

    assert {:error, :conflict} =
             rpc(minority, :stale_write, [supervisor, key, stale.body, stale.etag])

    assert_durable_count(members, supervisor, key, 2)
  end

  test "acknowledged state survives abrupt owner VM loss and majority recovery", context do
    %{members: [owner_member | survivors], supervisor: supervisor} = context
    key = "vm-loss"
    assert {:ok, {old_owner, _}} = rpc(owner_member, :ensure_counter, [supervisor, key])

    for expected <- 1..3 do
      assert {:ok, ^expected} = rpc(owner_member, :invoke, [old_owner, :increment_and_sync])
    end

    # halt bypasses terminate callbacks and final sync; this is not graceful stop.
    :ok = :peer.cast(owner_member.peer, :erlang, :halt, [])
    await(fn -> match?({:down, _}, :peer.get_state(owner_member.peer)) end)
    assert :ok = rpc(hd(survivors), :discover, [supervisor])
    new_owner = await_owner(survivors, supervisor, key, old_owner)
    new_member = Enum.find(survivors, &(&1.node == node(new_owner)))

    assert {:ok, 3} = rpc(new_member, :invoke, [new_owner, :get_count])
    assert {:ok, 4} = rpc(new_member, :invoke, [new_owner, :increment_and_sync])
    assert_durable_count(survivors, supervisor, key, 4)
  end

  defp isolate(minority, majority) do
    for member <- majority do
      # disconnect alone is insufficient: distribution can reconnect on send.
      assert true =
               :peer.call(minority.peer, :erlang, :set_cookie, [member.node, :partitioned])

      assert :peer.call(minority.peer, Node, :disconnect, [member.node])
    end

    await(fn -> :peer.call(minority.peer, Node, :list, []) == [] end)

    for member <- majority do
      await(fn -> minority.node not in :peer.call(member.peer, Node, :list, []) end)
    end
  end

  defp await_cluster(members, store) do
    for member <- members do
      expected = members |> Enum.reject(&(&1 == member)) |> Enum.map(& &1.node) |> Enum.sort()
      await(fn -> rpc(member, :connected_store_members, [store]) == expected end)
    end
  end

  defp await_owner(members, supervisor, key, old_owner) do
    await(fn ->
      Enum.all?(members, fn member ->
        case rpc(member, :lookup, [supervisor, key]) do
          {pid, _} -> pid != old_owner and node(pid) in Enum.map(members, & &1.node)
          nil -> false
        end
      end)
    end)

    owners = Enum.map(members, fn member -> elem(rpc(member, :lookup, [supervisor, key]), 0) end)
    assert [owner] = Enum.uniq(owners)
    owner
  end

  defp assert_durable_count(members, supervisor, key, expected) do
    for member <- members do
      assert {:ok, %{body: %StoredState{state: %{count: ^expected}}}} =
               rpc(member, :stored, [supervisor, key])
    end
  end

  defp rpc(member, function, args), do: :peer.call(member.peer, Client, function, args, 15_000)

  defp await(fun, timeout \\ 20_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_until(fun, deadline)
  end

  defp await_until(fun, deadline) do
    unless fun.() do
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("cluster condition was not met before the deadline")
      end

      Process.sleep(25)
      await_until(fun, deadline)
    end
  end
end
