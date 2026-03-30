defmodule Group.DistributedTest do
  use ExUnit.Case

  @moduletag :capture_log
  @moduletag timeout: 30_000

  alias Group.TestCluster

  defp start_group_on_peers(peers, opts) do
    for {_pid, node} <- peers do
      TestCluster.start_group(node, opts)
    end
  end

  defp keys_for_shard(cluster, prefix, num_shards, target_shard, count) do
    Stream.iterate(0, &(&1 + 1))
    |> Stream.map(fn i -> "#{prefix}/#{i}" end)
    |> Stream.filter(fn key -> :erlang.phash2({cluster, key}, num_shards) == target_shard end)
    |> Enum.take(count)
  end

  defp receive_batches_until(expected_event_count, acc \\ []) do
    received = Enum.sum(Enum.map(acc, &length/1))

    if received >= expected_event_count do
      Enum.reverse(acc)
    else
      receive do
        {:got_batch, events} ->
          receive_batches_until(expected_event_count, [events | acc])
      after
        5000 ->
          flunk("timed out waiting for #{expected_event_count} batched events, got #{received}")
      end
    end
  end

  describe "registration replication" do
    test "register replicates to other nodes" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_test_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Register on node A
      remote_pid = TestCluster.spawn_register(node_a, name, "user/1", %{role: :server})

      # Should be visible on node B
      TestCluster.assert_eventually(fn ->
        case TestCluster.rpc!(node_b, Group, :lookup, [name, "user/1"]) do
          {pid, %{role: :server}} when is_pid(pid) -> true
          _ -> false
        end
      end)

      # Unregister
      TestCluster.rpc!(node_a, Process, :exit, [remote_pid, :kill])

      # Should be gone from node B
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "user/1"]) == nil
      end)
    end
  end

  describe "PG join/leave replication" do
    test "join replicates to other nodes" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_pg_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Join on node A
      remote_pid = TestCluster.spawn_join(node_a, name, "room/1", %{role: :player})

      # Should be visible on node B
      TestCluster.assert_eventually(fn ->
        members = TestCluster.rpc!(node_b, Group, :members, [name, "room/1"])

        case members do
          [{pid, %{role: :player}}] when is_pid(pid) -> true
          _ -> false
        end
      end)

      # Kill process on A
      Process.exit(remote_pid, :kill)

      # Should be gone from node B
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :members, [name, "room/1"]) == []
      end)
    end
  end

  describe "node discovery (late joiner)" do
    test "late joiner receives existing data" do
      peers = TestCluster.start_peers(2)

      [{_, node_a}, {_, _node_b}] = peers
      name = :"dist_late_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      # Start group on A and B
      start_group_on_peers(peers, opts)

      # Register and join on A
      TestCluster.spawn_register_and_join(
        node_a,
        name,
        "user/1",
        %{type: :reg},
        "room/1",
        %{type: :pg}
      )

      # Wait for replication to B before starting C
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :lookup, [name, "user/1"]) != nil
      end)

      # Start a 3rd node
      [{late_pid, node_c}] = TestCluster.start_peers(1)

      on_exit(fn ->
        TestCluster.stop_peers(peers)
        TestCluster.stop_peers([{late_pid, node_c}])
      end)

      TestCluster.start_group(node_c, opts)

      # Late joiner should see existing data
      TestCluster.assert_eventually(fn ->
        lookup = TestCluster.rpc!(node_c, Group, :lookup, [name, "user/1"])
        members = TestCluster.rpc!(node_c, Group, :members, [name, "room/1"])
        lookup != nil and length(members) > 0
      end)
    end
  end

  describe "node disconnect cleanup" do
    test "dead node's entries are cleaned up" do
      peers = TestCluster.start_peers(2)

      [{_, node_a}, {peer_b_pid, node_b}] = peers
      name = :"dist_cleanup_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Register and join on node B
      TestCluster.spawn_register_and_join(
        node_b,
        name,
        "user/1",
        %{node: :b},
        "room/1",
        %{node: :b}
      )

      # Verify data on node A
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :lookup, [name, "user/1"]) != nil
      end)

      # Stop node B
      :peer.stop(peer_b_pid)

      # Node A should clean up B's entries
      TestCluster.assert_eventually(
        fn ->
          lookup = TestCluster.rpc!(node_a, Group, :lookup, [name, "user/1"])
          members = TestCluster.rpc!(node_a, Group, :members, [name, "room/1"])
          lookup == nil and members == []
        end,
        timeout: 5000
      )

      # Clean up remaining peer
      [{peer_a_pid, _}] = Enum.filter(peers, fn {_, n} -> n == node_a end)
      on_exit(fn -> :peer.stop(peer_a_pid) end)
    end

    test "process DOWN cleanup arrives as one remote batch per dead pid" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_down_batch_#{System.unique_integer([:positive])}"
      num_shards = 2
      opts = [name: name, shards: num_shards]

      [reg_key, join_key] =
        keys_for_shard(nil, "down_batch", num_shards, 0, 2)

      start_group_on_peers(peers, opts)

      pid =
        TestCluster.spawn_register_and_join(
          node_a,
          name,
          reg_key,
          %{type: :reg},
          join_key,
          %{type: :pg}
        )

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, reg_key]) != nil and
          TestCluster.rpc!(node_b, Group, :members, [name, join_key]) != []
      end)

      TestCluster.spawn_batch_forwarder(node_b, name, :all, self())
      assert_receive {:monitor_ready, _}, 5000

      Process.exit(pid, :kill)

      assert_receive {:got_batch, events}, 5000
      assert Enum.sort(Enum.map(events, & &1.type)) == [:left, :unregistered]
      assert Enum.sort(Enum.map(events, & &1.key)) == Enum.sort([join_key, reg_key])
      refute_receive {:got_batch, _}, 100
    end

    test "process DOWN cleanup batches up to 32 member pids per shard turn" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_down_batch_many_#{System.unique_integer([:positive])}"
      num_shards = 2
      opts = [name: name, shards: num_shards]

      keys =
        keys_for_shard(nil, "down_batch_many", num_shards, 0, 66)
        |> Enum.chunk_every(2)

      start_group_on_peers(peers, opts)

      pids =
        Enum.map(keys, fn [reg_key, join_key] ->
          TestCluster.spawn_register_and_join(
            node_a,
            name,
            reg_key,
            %{type: :reg},
            join_key,
            %{type: :pg}
          )
        end)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :registry_count, [name]) == 33 and
          TestCluster.rpc!(node_b, Group, :member_count, [name, "down_batch_many/"]) == 33
      end)

      TestCluster.spawn_batch_forwarder(node_b, name, :all, self())
      assert_receive {:monitor_ready, _}, 5000

      :ok = TestCluster.kill_pids(node_a, pids)

      batches = receive_batches_until(66)
      batch_sizes = Enum.map(batches, &length/1)

      assert Enum.sum(batch_sizes) == 66
      assert length(batches) >= 2
      assert Enum.max(batch_sizes) <= 64
    end
  end

  describe "event delivery across nodes" do
    test "monitor receives events from remote registrations" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_events_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Wait for peer discovery to complete
      TestCluster.assert_eventually(fn ->
        nodes = TestCluster.rpc!(node_a, Group, :nodes, [name])
        node_b in nodes
      end)

      # Set up monitor on node A that forwards events to us
      TestCluster.spawn_monitor_forwarder(node_a, name, "user/", self())
      assert_receive {:monitor_ready, _}, 5000

      # Register on node B
      TestCluster.spawn_register(node_b, name, "user/123", %{from: :node_b})

      # Node A's monitor should receive the event and forward it
      assert_receive {:got_event, %Group.Event{type: :registered, key: "user/123"}}, 5000
    end
  end

  describe "named cluster isolation across nodes" do
    test "cluster members are isolated" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      name = :"dist_cluster_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Connect A and B to "game" cluster
      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      # Connect A and C to "chat" cluster
      TestCluster.rpc!(node_a, Group, :connect, [name, "chat"])
      TestCluster.rpc!(node_c, Group, :connect, [name, "chat"])

      # Join on B in "game"
      TestCluster.spawn_join(node_b, name, "room/1", %{type: :game}, cluster: "game")

      # Join on C in "chat"
      TestCluster.spawn_join(node_c, name, "room/1", %{type: :chat}, cluster: "chat")

      # A should see both (connected to both clusters)
      TestCluster.assert_eventually(fn ->
        game_members =
          TestCluster.rpc!(node_a, Group, :members, [name, "room/1", [cluster: "game"]])

        chat_members =
          TestCluster.rpc!(node_a, Group, :members, [name, "room/1", [cluster: "chat"]])

        length(game_members) == 1 and length(chat_members) == 1
      end)

      # B should only see game
      game_on_b = TestCluster.rpc!(node_b, Group, :members, [name, "room/1", [cluster: "game"]])
      chat_on_b = TestCluster.rpc!(node_b, Group, :members, [name, "room/1", [cluster: "chat"]])
      assert length(game_on_b) == 1
      assert chat_on_b == []

      # C should only see chat
      game_on_c = TestCluster.rpc!(node_c, Group, :members, [name, "room/1", [cluster: "game"]])
      chat_on_c = TestCluster.rpc!(node_c, Group, :members, [name, "room/1", [cluster: "chat"]])
      assert game_on_c == []
      assert length(chat_on_c) == 1
    end
  end

  describe "conflict resolution — partition heal" do
    @tag timeout: 60_000
    test "merge invokes resolve_conflict and kills loser process" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_merge_resolve_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Verify connectivity
      pid_init = TestCluster.spawn_register(node_a, name, "init", %{})

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "init"]) != nil
      end)

      TestCluster.rpc!(node_a, Process, :exit, [pid_init, :kill])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "init"]) == nil
      end)

      # Set up nodedown monitors
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())

      # Partition
      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # Register same key on both sides. A first (lower timestamp), B second.
      pid_a = TestCluster.spawn_register(node_a, name, "user/conflict", %{side: :a})
      # Ensure B's registration gets a strictly higher timestamp
      Process.sleep(50)
      pid_b = TestCluster.spawn_register(node_b, name, "user/conflict", %{side: :b})

      # Both are alive before heal
      assert TestCluster.rpc!(node_a, Process, :alive?, [pid_a])
      assert TestCluster.rpc!(node_b, Process, :alive?, [pid_b])

      # Reconnect
      TestCluster.reconnect_nodes(node_a, node_b)

      # After convergence, both sides agree on the winner
      TestCluster.assert_eventually(
        fn ->
          lookup_a = TestCluster.rpc!(node_a, Group, :lookup, [name, "user/conflict"])
          lookup_b = TestCluster.rpc!(node_b, Group, :lookup, [name, "user/conflict"])

          case {lookup_a, lookup_b} do
            {{pid, _}, {pid, _}} when is_pid(pid) -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )

      # The loser (pid_a, lower timestamp) should be killed by resolve_conflict.
      # The default resolver calls Process.exit(loser, {:group_registry_conflict, ...}).
      TestCluster.assert_eventually(
        fn -> not TestCluster.rpc!(node_a, Process, :alive?, [pid_a]) end,
        timeout: 5000
      )

      # Winner should be pid_b (higher timestamp)
      {winner_pid, _} = TestCluster.rpc!(node_a, Group, :lookup, [name, "user/conflict"])
      assert winner_pid == pid_b

      # ETS should be consistent
      TestCluster.flush_shards(node_a, name)
      TestCluster.flush_shards(node_b, name)

      assert :ok =
               TestCluster.rpc!(node_a, Group.TestCluster, :assert_ets_consistent, [name])

      assert :ok =
               TestCluster.rpc!(node_b, Group.TestCluster, :assert_ets_consistent, [name])
    end

    @tag timeout: 60_000
    test "same key registered on both sides during partition" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_conflict_#{System.unique_integer([:positive])}"

      opts = [
        name: name,
        shards: 2,
        resolve_registry_conflict: {Group.TestConflictResolver, :resolve, []}
      ]

      start_group_on_peers(peers, opts)

      # Verify initial connectivity
      pid_init = TestCluster.spawn_register(node_a, name, "user/init", %{v: 0})

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "user/init"]) != nil
      end)

      # Clean up init key
      TestCluster.rpc!(node_a, Process, :exit, [pid_init, :kill])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "user/init"]) == nil
      end)

      # Set up nodedown monitors so we know when disconnect takes effect on both sides
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())

      # Disconnect A from B
      TestCluster.disconnect_nodes(node_a, node_b)

      # Wait for both sides to confirm the disconnect
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # Register same key on both sides during partition
      # spawn_register now waits for registration to complete before returning
      _pid_a = TestCluster.spawn_register(node_a, name, "user/conflict", %{side: :a})
      Process.sleep(50)
      _pid_b = TestCluster.spawn_register(node_b, name, "user/conflict", %{side: :b})

      # Verify each side sees its own registration
      assert TestCluster.rpc!(node_a, Group, :lookup, [name, "user/conflict"]) != nil
      assert TestCluster.rpc!(node_b, Group, :lookup, [name, "user/conflict"]) != nil

      # Reconnect
      TestCluster.reconnect_nodes(node_a, node_b)

      # After reconnect, exactly one registration should survive on both nodes
      TestCluster.assert_eventually(
        fn ->
          lookup_a = TestCluster.rpc!(node_a, Group, :lookup, [name, "user/conflict"])
          lookup_b = TestCluster.rpc!(node_b, Group, :lookup, [name, "user/conflict"])

          case {lookup_a, lookup_b} do
            {{pid_a, _}, {pid_b, _}} when is_pid(pid_a) and is_pid(pid_b) ->
              # Same pid wins on both sides
              pid_a == pid_b

            _ ->
              false
          end
        end,
        timeout: 10_000
      )
    end
  end

  describe "partition healing with full data sync" do
    @tag timeout: 60_000
    test "mutated data on both sides syncs after reconnect" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      name = :"dist_heal_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Wait for Erlang-level connectivity so disconnect_nodes actually works
      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      # Set up nodedown monitor on A
      TestCluster.monitor_nodes_on(node_a, self())

      # Disconnect C from A and B
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)

      # Wait for A to confirm it saw C go down
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      # While partitioned: register keys on A, join groups on C
      # flush_shards ensures nodedown is processed before registering
      TestCluster.spawn_register(node_a, name, "user/from_a", %{origin: :a}, flush_shards: 2)
      TestCluster.spawn_join(node_c, name, "room/from_c", %{origin: :c})

      # Wait for A's registration to replicate to B before checking isolation
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "user/from_a"]) != nil
      end)

      # Verify A doesn't see C's data and C doesn't see A's data
      assert TestCluster.rpc!(node_c, Group, :lookup, [name, "user/from_a"]) == nil
      assert TestCluster.rpc!(node_a, Group, :members, [name, "room/from_c"]) == []

      # Reconnect C to A (B should follow via transitive connectivity)
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # Assert all data is eventually consistent across all 3 nodes
      TestCluster.assert_eventually(
        fn ->
          # A sees C's joins
          members_on_a = TestCluster.rpc!(node_a, Group, :members, [name, "room/from_c"])
          # C sees A's registrations
          lookup_on_c = TestCluster.rpc!(node_c, Group, :lookup, [name, "user/from_a"])
          # B sees both
          members_on_b = TestCluster.rpc!(node_b, Group, :members, [name, "room/from_c"])
          lookup_on_b = TestCluster.rpc!(node_b, Group, :lookup, [name, "user/from_a"])

          length(members_on_a) == 1 and lookup_on_c != nil and
            length(members_on_b) == 1 and lookup_on_b != nil
        end,
        timeout: 10_000
      )
    end
  end

  describe "rapid process death during replication" do
    test "no stale entries persist after spawn-register-kill" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_rapid_death_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Spawn, register, and immediately kill processes several times
      for i <- 1..5 do
        TestCluster.spawn_register_then_kill(node_a, name, "user/ephemeral_#{i}", %{i: i})
      end

      # Assert no stale entries on node B
      TestCluster.assert_eventually(
        fn ->
          Enum.all?(1..5, fn i ->
            TestCluster.rpc!(node_b, Group, :lookup, [name, "user/ephemeral_#{i}"]) == nil
          end)
        end,
        timeout: 5000
      )
    end
  end

  describe "concurrent same-key registration across nodes" do
    test "only one registration survives" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_race_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Simultaneously register the same key on both nodes
      task_a =
        Task.async(fn ->
          TestCluster.spawn_register(node_a, name, "user/race", %{side: :a})
        end)

      task_b =
        Task.async(fn ->
          TestCluster.spawn_register(node_b, name, "user/race", %{side: :b})
        end)

      Task.await(task_a)
      Task.await(task_b)

      # After conflict resolution, exactly one registration should survive
      # and both nodes should agree on the winner
      TestCluster.assert_eventually(
        fn ->
          lookup_a = TestCluster.rpc!(node_a, Group, :lookup, [name, "user/race"])
          lookup_b = TestCluster.rpc!(node_b, Group, :lookup, [name, "user/race"])

          case {lookup_a, lookup_b} do
            {{pid_a, _}, {pid_b, _}} when is_pid(pid_a) and is_pid(pid_b) ->
              pid_a == pid_b

            _ ->
              false
          end
        end,
        timeout: 5000
      )
    end
  end

  describe "node flapping" do
    @tag timeout: 60_000
    test "rapid disconnect/reconnect cycles don't corrupt state" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_flap_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Register data on A
      TestCluster.spawn_register(node_a, name, "user/stable", %{v: 1})

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "user/stable"]) != nil
      end)

      # Set up nodedown monitor on A
      TestCluster.monitor_nodes_on(node_a, self())

      # Rapidly disconnect/reconnect B 3 times
      for _i <- 1..3 do
        TestCluster.disconnect_nodes(node_a, node_b)
        assert_receive {:nodedown_on_remote, ^node_b}, 5000
        TestCluster.reconnect_nodes(node_a, node_b)
        # Wait for actual data replication to confirm full handshake
        TestCluster.assert_eventually(
          fn ->
            TestCluster.rpc!(node_b, Group, :lookup, [name, "user/stable"]) != nil
          end,
          timeout: 5000
        )
      end

      # After final reconnect, data should be consistent on both nodes
      TestCluster.assert_eventually(
        fn ->
          lookup_a = TestCluster.rpc!(node_a, Group, :lookup, [name, "user/stable"])
          lookup_b = TestCluster.rpc!(node_b, Group, :lookup, [name, "user/stable"])
          lookup_a != nil and lookup_b != nil
        end,
        timeout: 10_000
      )
    end
  end

  describe "multiple simultaneous node failures" do
    test "surviving node cleans up all dead nodes' entries" do
      peers = TestCluster.start_peers(3)

      [{peer_a_pid, node_a}, {peer_b_pid, node_b}, {peer_c_pid, node_c}] = peers
      name = :"dist_multi_fail_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Register unique keys on each node
      TestCluster.spawn_register(node_a, name, "user/a", %{node: :a})
      TestCluster.spawn_register(node_b, name, "user/b", %{node: :b})
      TestCluster.spawn_register(node_c, name, "user/c", %{node: :c})

      # Verify all visible on A
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :lookup, [name, "user/b"]) != nil and
          TestCluster.rpc!(node_a, Group, :lookup, [name, "user/c"]) != nil
      end)

      # Stop B and C simultaneously
      :peer.stop(peer_b_pid)
      :peer.stop(peer_c_pid)

      # A should clean up all of B's and C's entries
      TestCluster.assert_eventually(
        fn ->
          lookup_b = TestCluster.rpc!(node_a, Group, :lookup, [name, "user/b"])
          lookup_c = TestCluster.rpc!(node_a, Group, :lookup, [name, "user/c"])
          lookup_b == nil and lookup_c == nil
        end,
        timeout: 5000
      )

      # A's own data should be intact
      assert TestCluster.rpc!(node_a, Group, :lookup, [name, "user/a"]) != nil

      on_exit(fn -> :peer.stop(peer_a_pid) end)
    end
  end

  describe "cross-shard process death" do
    test "process registered in one shard and joined in another shard cleans up on both" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      num_shards = 4
      name = :"dist_cross_shard_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: num_shards]

      start_group_on_peers(peers, opts)

      # Find keys that hash to different shards
      {reg_key, join_key} = TestCluster.keys_for_different_shards(num_shards)

      # Verify they actually hash to different shards
      shard_reg = :erlang.phash2({nil, reg_key}, num_shards)
      shard_join = :erlang.phash2({nil, join_key}, num_shards)
      assert shard_reg != shard_join

      # Spawn one process that registers under reg_key and joins join_key
      pid =
        TestCluster.spawn_register_and_join_keys(
          node_a,
          name,
          reg_key,
          %{type: :reg},
          join_key,
          %{type: :pg}
        )

      # Verify both entries visible on B
      TestCluster.assert_eventually(fn ->
        lookup = TestCluster.rpc!(node_b, Group, :lookup, [name, reg_key])
        members = TestCluster.rpc!(node_b, Group, :members, [name, join_key])
        lookup != nil and length(members) > 0
      end)

      # Kill the process on A
      TestCluster.rpc!(node_a, Process, :exit, [pid, :kill])

      # Both the registration AND the group membership should be cleaned up on B
      TestCluster.assert_eventually(
        fn ->
          lookup = TestCluster.rpc!(node_b, Group, :lookup, [name, reg_key])
          members = TestCluster.rpc!(node_b, Group, :members, [name, join_key])
          lookup == nil and members == []
        end,
        timeout: 5000
      )
    end
  end

  describe "event ordering across nodes" do
    test "register → update → unregister sequence delivers events in order" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_event_order_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Wait for peer discovery to complete
      TestCluster.assert_eventually(fn ->
        nodes = TestCluster.rpc!(node_b, Group, :nodes, [name])
        node_a in nodes
      end)

      # Set up monitor on B that forwards events to us
      TestCluster.spawn_monitor_forwarder(node_b, name, "user/", self())
      assert_receive {:monitor_ready, _}, 5000

      # On A: register with meta v:1, re-register with meta v:2, then unregister
      TestCluster.spawn_register_update_unregister(
        node_a,
        name,
        "user/ordered",
        %{v: 1},
        %{v: 2}
      )

      # Assert B receives events in order
      assert_receive {:got_event,
                      %Group.Event{type: :registered, key: "user/ordered", meta: %{v: 1}}},
                     5000

      assert_receive {:got_event,
                      %Group.Event{
                        type: :registered,
                        key: "user/ordered",
                        meta: %{v: 2},
                        previous_meta: %{v: 1}
                      }},
                     5000

      assert_receive {:got_event, %Group.Event{type: :unregistered, key: "user/ordered"}},
                     5000
    end
  end

  describe "cluster disconnect" do
    test "purges both registry and pg entries on remote node" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"disc_reg_pg_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      TestCluster.assert_eventually(fn ->
        length(TestCluster.rpc!(node_a, Group, :nodes, [name, "game"])) >= 2
      end)

      # Register AND join on A in "game"
      TestCluster.spawn_register_in_cluster(node_a, name, "player/1", %{r: true}, "game")
      TestCluster.spawn_join(node_a, name, "room/1", %{j: true}, cluster: "game")

      # B sees both
      TestCluster.assert_eventually(fn ->
        lookup = TestCluster.rpc!(node_b, Group, :lookup, [name, "player/1", [cluster: "game"]])
        members = TestCluster.rpc!(node_b, Group, :members, [name, "room/1", [cluster: "game"]])
        lookup != nil and length(members) == 1
      end)

      # A disconnects
      TestCluster.rpc!(node_a, Group, :disconnect, [name, "game"])

      # B should see neither
      TestCluster.assert_eventually(
        fn ->
          lookup = TestCluster.rpc!(node_b, Group, :lookup, [name, "player/1", [cluster: "game"]])
          members = TestCluster.rpc!(node_b, Group, :members, [name, "room/1", [cluster: "game"]])
          lookup == nil and members == []
        end,
        timeout: 5000
      )
    end

    test "removes disconnecting node from remote cluster_nodes" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"disc_nodes_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      TestCluster.assert_eventually(fn ->
        nodes_b = TestCluster.rpc!(node_b, Group, :nodes, [name, "game"])
        node_a in nodes_b
      end)

      # A disconnects
      TestCluster.rpc!(node_a, Group, :disconnect, [name, "game"])

      # B should no longer list A in "game" cluster
      TestCluster.assert_eventually(
        fn ->
          nodes_b = TestCluster.rpc!(node_b, Group, :nodes, [name, "game"])
          node_a not in nodes_b
        end,
        timeout: 5000
      )

      # A should report not connected
      refute TestCluster.rpc!(node_a, Group, :connected?, [name, "game"])
    end

    test "replication stops after disconnect" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"disc_no_repl_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      TestCluster.assert_eventually(fn ->
        length(TestCluster.rpc!(node_a, Group, :nodes, [name, "game"])) >= 2
      end)

      # A disconnects from "game"
      TestCluster.rpc!(node_a, Group, :disconnect, [name, "game"])

      # Wait for disconnect to propagate
      TestCluster.assert_eventually(
        fn ->
          nodes_b = TestCluster.rpc!(node_b, Group, :nodes, [name, "game"])
          node_a not in nodes_b
        end,
        timeout: 5000
      )

      # B registers in "game" — A should NOT receive it
      TestCluster.spawn_register_in_cluster(node_b, name, "new_key", %{from: :b}, "game")

      # Verify B sees its own registration
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "new_key", [cluster: "game"]]) != nil
      end)

      # Give any stray replication time to arrive
      Process.sleep(300)

      # A should NOT see B's entry (not in cluster anymore)
      assert TestCluster.rpc!(node_a, Group, :lookup, [name, "new_key", [cluster: "game"]]) == nil
    end

    test "fires events with reason :cluster_disconnect" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"disc_events_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      TestCluster.assert_eventually(fn ->
        length(TestCluster.rpc!(node_b, Group, :nodes, [name, "game"])) >= 2
      end)

      # Monitor on B for "game" cluster events
      TestCluster.spawn_monitor_forwarder(node_b, name, :all, self(), cluster: "game")
      assert_receive {:monitor_ready, _}, 5000

      # A joins in "game"
      TestCluster.spawn_join(node_a, name, "room/1", %{player: :a}, cluster: "game")
      assert_receive {:got_event, %Group.Event{type: :joined, key: "room/1"}}, 5000

      # A disconnects
      TestCluster.rpc!(node_a, Group, :disconnect, [name, "game"])

      # B should receive :left event with reason :cluster_disconnect
      assert_receive {:got_event,
                      %Group.Event{type: :left, key: "room/1", reason: :cluster_disconnect}},
                     5000
    end

    test "disconnect then re-connect works cleanly" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"disc_reconnect_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      TestCluster.assert_eventually(fn ->
        length(TestCluster.rpc!(node_b, Group, :nodes, [name, "game"])) >= 2
      end)

      # A registers in "game"
      TestCluster.spawn_register_in_cluster(node_a, name, "key/1", %{v: 1}, "game")

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "key/1", [cluster: "game"]]) != nil
      end)

      # A disconnects
      TestCluster.rpc!(node_a, Group, :disconnect, [name, "game"])

      TestCluster.assert_eventually(
        fn ->
          TestCluster.rpc!(node_b, Group, :lookup, [name, "key/1", [cluster: "game"]]) == nil
        end,
        timeout: 5000
      )

      # A re-connects
      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])

      TestCluster.assert_eventually(
        fn ->
          nodes_b = TestCluster.rpc!(node_b, Group, :nodes, [name, "game"])
          node_a in nodes_b
        end,
        timeout: 5000
      )

      # A registers new key
      TestCluster.spawn_register_in_cluster(node_a, name, "key/2", %{v: 2}, "game")

      # B should see the new key
      TestCluster.assert_eventually(
        fn ->
          TestCluster.rpc!(node_b, Group, :lookup, [name, "key/2", [cluster: "game"]]) != nil
        end,
        timeout: 5000
      )
    end

    test "disconnect is idempotent for non-member cluster" do
      peers = TestCluster.start_peers(1)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}] = peers
      name = :"disc_idempotent_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      TestCluster.start_group(node_a, opts)

      # Disconnect from a cluster we never joined — should not crash
      assert TestCluster.rpc!(node_a, Group, :disconnect, [name, "nonexistent"]) == :ok
      # Do it again
      assert TestCluster.rpc!(node_a, Group, :disconnect, [name, "nonexistent"]) == :ok
    end

    test "nodedown after disconnect cleans up all data" do
      peers = TestCluster.start_peers(2)

      [{_, node_a}, {peer_b_pid, node_b}] = peers
      name = :"disc_then_down_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      TestCluster.assert_eventually(fn ->
        length(TestCluster.rpc!(node_a, Group, :nodes, [name, "game"])) >= 2
      end)

      # B registers in both nil and "game" clusters
      TestCluster.spawn_register(node_b, name, "nil_key", %{cluster: nil})
      TestCluster.spawn_register_in_cluster(node_b, name, "game_key", %{cluster: :game}, "game")

      # A sees both
      TestCluster.assert_eventually(fn ->
        nil_lookup = TestCluster.rpc!(node_a, Group, :lookup, [name, "nil_key"])

        game_lookup =
          TestCluster.rpc!(node_a, Group, :lookup, [name, "game_key", [cluster: "game"]])

        nil_lookup != nil and game_lookup != nil
      end)

      # B disconnects from "game"
      TestCluster.rpc!(node_b, Group, :disconnect, [name, "game"])

      # A should see game_key gone but nil_key still there
      TestCluster.assert_eventually(
        fn ->
          TestCluster.rpc!(node_a, Group, :lookup, [name, "game_key", [cluster: "game"]]) == nil
        end,
        timeout: 5000
      )

      assert TestCluster.rpc!(node_a, Group, :lookup, [name, "nil_key"]) != nil

      # Now B crashes
      :peer.stop(peer_b_pid)

      # A should clean up nil_key too
      TestCluster.assert_eventually(
        fn ->
          TestCluster.rpc!(node_a, Group, :lookup, [name, "nil_key"]) == nil
        end,
        timeout: 5000
      )

      [{peer_a_pid, _}] = Enum.filter(peers, fn {_, n} -> n == node_a end)
      on_exit(fn -> :peer.stop(peer_a_pid) end)
    end

    test "empty cluster removed on last disconnect" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"disc_empty_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      TestCluster.assert_eventually(fn ->
        length(TestCluster.rpc!(node_b, Group, :nodes, [name, "game"])) >= 2
      end)

      # Both disconnect
      TestCluster.rpc!(node_a, Group, :disconnect, [name, "game"])
      TestCluster.rpc!(node_b, Group, :disconnect, [name, "game"])

      # "game" should not appear in all_clusters on either node
      TestCluster.assert_eventually(
        fn ->
          clusters_a = TestCluster.rpc!(node_a, Group.Replica.Data, :all_clusters, [name])
          clusters_b = TestCluster.rpc!(node_b, Group.Replica.Data, :all_clusters, [name])
          "game" not in clusters_a and "game" not in clusters_b
        end,
        timeout: 5000
      )
    end
  end

  describe "named cluster late joiner" do
    test "late joiner receives data spread across shards" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_cluster_late_shards_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 8]

      start_group_on_peers(peers, opts)

      # A connects to "game" and registers/joins multiple keys (to hit different shards)
      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.spawn_register_in_cluster(node_a, name, "player/1", %{id: 1}, "game")
      TestCluster.spawn_register_in_cluster(node_a, name, "player/2", %{id: 2}, "game")
      TestCluster.spawn_join(node_a, name, "room/1", %{player: :a1}, cluster: "game")
      TestCluster.spawn_join(node_a, name, "room/2", %{player: :a2}, cluster: "game")

      # Wait for A's local data to settle
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :lookup, [name, "player/2", [cluster: "game"]]) != nil
      end)

      # B connects to "game" (late joiner)
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      # B should see ALL of A's data (registry + pg, across shards)
      TestCluster.assert_eventually(
        fn ->
          p1 = TestCluster.rpc!(node_b, Group, :lookup, [name, "player/1", [cluster: "game"]])
          p2 = TestCluster.rpc!(node_b, Group, :lookup, [name, "player/2", [cluster: "game"]])
          r1 = TestCluster.rpc!(node_b, Group, :members, [name, "room/1", [cluster: "game"]])
          r2 = TestCluster.rpc!(node_b, Group, :members, [name, "room/2", [cluster: "game"]])
          p1 != nil and p2 != nil and length(r1) == 1 and length(r2) == 1
        end,
        timeout: 10_000
      )
    end

    test "new member receives existing cluster data" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      name = :"dist_cluster_late_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Connect A and B to "game" cluster
      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      # Wait for cluster connectivity
      TestCluster.assert_eventually(fn ->
        nodes_a = TestCluster.rpc!(node_a, Group, :nodes, [name, "game"])
        nodes_b = TestCluster.rpc!(node_b, Group, :nodes, [name, "game"])
        length(nodes_a) >= 1 and length(nodes_b) >= 1
      end)

      # Join on A in "game" cluster, register on B in "game" cluster
      TestCluster.spawn_join(node_a, name, "room/1", %{player: :a}, cluster: "game")
      TestCluster.spawn_register_in_cluster(node_b, name, "game_user/1", %{from: :b}, "game")

      # Verify A sees B's game cluster registration
      TestCluster.assert_eventually(
        fn ->
          members = TestCluster.rpc!(node_a, Group, :members, [name, "room/1", [cluster: "game"]])

          lookup =
            TestCluster.rpc!(node_a, Group, :lookup, [name, "game_user/1", [cluster: "game"]])

          length(members) == 1 and lookup != nil
        end,
        timeout: 5000
      )

      # Now connect C to "game" cluster
      TestCluster.rpc!(node_c, Group, :connect, [name, "game"])

      # C should eventually see both A's join and B's registration
      TestCluster.assert_eventually(
        fn ->
          members = TestCluster.rpc!(node_c, Group, :members, [name, "room/1", [cluster: "game"]])

          lookup =
            TestCluster.rpc!(node_c, Group, :lookup, [name, "game_user/1", [cluster: "game"]])

          length(members) == 1 and lookup != nil
        end,
        timeout: 10_000
      )
    end

    test "connect before peer discovery delivers data" do
      # Reproduces the connect/2 + peer_connect race:
      # 1. Start Group on both A and B, let peer discovery complete (nil only)
      # 2. A connects to "game" and registers data
      # 3. B connects to "game" — B's connect/2 must reach A even though
      #    the random shard's remote_shards may not include A yet
      #    (only shard S that processed peer_connect_ack has it)
      #
      # The fix: handle_call(:cluster_connect) reads peers from shared ETS
      # (cluster_nodes for nil cluster) instead of per-shard remote_shards.
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_connect_race_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 4]

      start_group_on_peers(peers, opts)

      # A connects to "game" and registers
      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.spawn_register_in_cluster(node_a, name, "player/1", %{id: 1}, "game")

      # B connects to "game"
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      # B should see A's data
      TestCluster.assert_eventually(
        fn ->
          TestCluster.rpc!(node_b, Group, :lookup, [name, "player/1", [cluster: "game"]]) != nil
        end,
        timeout: 5_000
      )
    end
  end

  describe "Group.nodes tracks actual peers" do
    test "only returns nodes running Group, not all Erlang nodes" do
      # Start 3 Erlang nodes, but only start Group on 2 of them
      peers = TestCluster.start_peers(3)

      [{_, node_a}, {_, node_b}, {_peer_c_pid, node_c}] = peers
      name = :"dist_nodes_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      # Start Group only on A and B
      TestCluster.start_group(node_a, opts)
      TestCluster.start_group(node_b, opts)

      # A should see B but NOT C
      TestCluster.assert_eventually(fn ->
        nodes = TestCluster.rpc!(node_a, Group, :nodes, [name])
        node_b in nodes and node_c not in nodes
      end)

      # Now start Group on C
      TestCluster.start_group(node_c, opts)

      # A should now see both B and C
      TestCluster.assert_eventually(fn ->
        nodes = TestCluster.rpc!(node_a, Group, :nodes, [name])
        node_b in nodes and node_c in nodes
      end)

      on_exit(fn -> TestCluster.stop_peers(peers) end)
    end

    test "nodedown removes peer from Group.nodes" do
      peers = TestCluster.start_peers(2)

      [{_, node_a}, {peer_b_pid, node_b}] = peers
      name = :"dist_nodes_down_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # A should see B
      TestCluster.assert_eventually(fn ->
        nodes = TestCluster.rpc!(node_a, Group, :nodes, [name])
        node_b in nodes
      end)

      # Stop B
      :peer.stop(peer_b_pid)

      # A should no longer see B
      TestCluster.assert_eventually(
        fn ->
          nodes = TestCluster.rpc!(node_a, Group, :nodes, [name])
          node_b not in nodes
        end,
        timeout: 5000
      )

      [{peer_a_pid, _}] = Enum.filter(peers, fn {_, n} -> n == node_a end)
      on_exit(fn -> :peer.stop(peer_a_pid) end)
    end
  end

  describe "sender-side filtering" do
    test "no cross-cluster data leakage" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      name = :"dist_filter_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # A in "game" + "chat", B in "game" only, C in "chat" only
      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_a, Group, :connect, [name, "chat"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_c, Group, :connect, [name, "chat"])

      # Wait for cluster connectivity
      TestCluster.assert_eventually(fn ->
        game_on_a = TestCluster.rpc!(node_a, Group, :nodes, [name, "game"])
        chat_on_a = TestCluster.rpc!(node_a, Group, :nodes, [name, "chat"])
        length(game_on_a) >= 1 and length(chat_on_a) >= 1
      end)

      # Register in "game" on B, register in "chat" on C
      TestCluster.spawn_register_in_cluster(node_b, name, "game_key/1", %{from: :b}, "game")
      TestCluster.spawn_register_in_cluster(node_c, name, "chat_key/1", %{from: :c}, "chat")

      # A should see both (it's in both clusters)
      TestCluster.assert_eventually(fn ->
        game_lookup =
          TestCluster.rpc!(node_a, Group, :lookup, [name, "game_key/1", [cluster: "game"]])

        chat_lookup =
          TestCluster.rpc!(node_a, Group, :lookup, [name, "chat_key/1", [cluster: "chat"]])

        game_lookup != nil and chat_lookup != nil
      end)

      # B should have NO "chat" data
      assert TestCluster.rpc!(node_b, Group, :lookup, [name, "chat_key/1", [cluster: "chat"]]) ==
               nil

      # C should have NO "game" data
      assert TestCluster.rpc!(node_c, Group, :lookup, [name, "game_key/1", [cluster: "game"]]) ==
               nil
    end
  end

  describe "peer discovery syncs shared clusters" do
    test "late joiner with named cluster gets both nil and named cluster data" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_shared_cluster_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      # Start Group on A only, connect to "game", add data
      TestCluster.start_group(node_a, opts)
      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.spawn_register(node_a, name, "nil_key", %{cluster: nil})

      TestCluster.spawn_register_in_cluster(
        node_a,
        name,
        "game_key",
        %{cluster: :game},
        "game"
      )

      # Start Group on B, connect to "game"
      TestCluster.start_group(node_b, opts)
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      # B should eventually see both nil cluster data and "game" cluster data
      TestCluster.assert_eventually(
        fn ->
          nil_lookup = TestCluster.rpc!(node_b, Group, :lookup, [name, "nil_key"])

          game_lookup =
            TestCluster.rpc!(node_b, Group, :lookup, [name, "game_key", [cluster: "game"]])

          nil_lookup != nil and game_lookup != nil
        end,
        timeout: 10_000
      )
    end
  end

  describe "partition heal re-syncs shared clusters" do
    @tag timeout: 60_000
    test "nil and named cluster data syncs after reconnect" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      name = :"dist_heal_cluster_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # A and C both join "game" cluster
      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_c, Group, :connect, [name, "game"])

      TestCluster.assert_eventually(fn ->
        nodes = TestCluster.rpc!(node_a, Group, :nodes, [name, "game"])
        length(nodes) >= 1
      end)

      # Wait for Erlang-level connectivity so disconnect_nodes actually works
      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      # Set up nodedown monitors on A before partitioning
      TestCluster.monitor_nodes_on(node_a, self())

      # Partition C from A and B
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)

      # Wait for A to confirm it saw C go down
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      # Register data during partition
      # flush_shards ensures nodedown is processed before registering
      TestCluster.spawn_register(node_a, name, "nil_from_a", %{origin: :a}, flush_shards: 2)

      TestCluster.spawn_register_in_cluster(
        node_c,
        name,
        "game_from_c",
        %{origin: :c},
        "game"
      )

      # Verify isolation during partition
      TestCluster.assert_eventually(fn ->
        # Wait for A's registration to replicate to B (the remaining connected peer)
        TestCluster.rpc!(node_b, Group, :lookup, [name, "nil_from_a"]) != nil
      end)

      assert TestCluster.rpc!(node_c, Group, :lookup, [name, "nil_from_a"]) == nil

      assert TestCluster.rpc!(node_a, Group, :lookup, [
               name,
               "game_from_c",
               [cluster: "game"]
             ]) == nil

      # Heal partition
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # All data should sync for both nil and "game" clusters
      TestCluster.assert_eventually(
        fn ->
          nil_on_c = TestCluster.rpc!(node_c, Group, :lookup, [name, "nil_from_a"])

          game_on_a =
            TestCluster.rpc!(node_a, Group, :lookup, [name, "game_from_c", [cluster: "game"]])

          nil_on_c != nil and game_on_a != nil
        end,
        timeout: 10_000
      )
    end
  end

  describe "rolling restart" do
    test "new node syncs data from surviving nodes" do
      peers = TestCluster.start_peers(2)

      [{peer_a_pid, node_a}, {_, node_b}] = peers
      name = :"dist_rolling_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Register data on both nodes
      TestCluster.spawn_register(node_a, name, "from_a", %{origin: :a})
      TestCluster.spawn_register(node_b, name, "from_b", %{origin: :b})

      # Wait for replication before stopping A
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "from_a"]) != nil
      end)

      # Stop node A
      :peer.stop(peer_a_pid)

      # B should have cleaned up A's entries
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "from_a"]) == nil
      end)

      # B should still have its own
      assert TestCluster.rpc!(node_b, Group, :lookup, [name, "from_b"]) != nil

      # Start a new node A'
      [{new_a_pid, new_node_a}] = TestCluster.start_peers(1)
      TestCluster.start_group(new_node_a, opts)

      # New A should sync from B
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(new_node_a, Group, :lookup, [name, "from_b"]) != nil
      end)

      on_exit(fn ->
        TestCluster.stop_peers([{new_a_pid, new_node_a}])
        # Stop remaining original peer B
        [{peer_b_pid, _}] = Enum.filter(peers, fn {_, n} -> n == node_b end)
        :peer.stop(peer_b_pid)
      end)
    end
  end

  describe "peer discovery — nodeup vs Node.list() coverage" do
    test "Group starting after Erlang connection discovers via Node.list()" do
      # Exercises the init Node.list() path: B is already Erlang-connected
      # to A but only A has Group running. When Group starts on B, B's init
      # enumerates Node.list() and sends peer_connect to A (no nodeup fires
      # because A was already connected before monitor_nodes subscription).
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_nodelist_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      # Wait for full Erlang mesh between peers (transitive via test node)
      TestCluster.assert_eventually(fn ->
        node_a in TestCluster.rpc!(node_b, Node, :list, [])
      end)

      # Start Group on A only, register data
      TestCluster.start_group(node_a, opts)
      TestCluster.spawn_register(node_a, name, "user/1", %{from: :a})
      TestCluster.spawn_join(node_a, name, "room/1", %{from: :a})

      # Now start Group on B — must discover A via init Node.list(), not nodeup
      TestCluster.start_group(node_b, opts)

      # B should see A's data (proves Node.list() init path works)
      TestCluster.assert_eventually(
        fn ->
          lookup = TestCluster.rpc!(node_b, Group, :lookup, [name, "user/1"])
          members = TestCluster.rpc!(node_b, Group, :members, [name, "room/1"])

          match?({_pid, %{from: :a}}, lookup) and
            match?([{_pid, %{from: :a}}], members)
        end,
        timeout: 5000
      )

      # A should also see B as a peer (B's peer_connect was received)
      TestCluster.assert_eventually(fn ->
        node_b in TestCluster.rpc!(node_a, Group, :nodes, [name])
      end)
    end

    test "staggered Group startup across 3 nodes discovers all peers" do
      # All 3 nodes are Erlang-connected from the start, but Group starts
      # at different times. Each must discover the others regardless of
      # whether discovery happens via nodeup or Node.list().
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      name = :"dist_stagger_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      # Wait for full Erlang mesh
      TestCluster.assert_eventually(fn ->
        a_nodes = TestCluster.rpc!(node_a, Node, :list, [])
        node_b in a_nodes and node_c in a_nodes
      end)

      # Start Group on A, register data
      TestCluster.start_group(node_a, opts)
      TestCluster.spawn_register(node_a, name, "from_a", %{origin: :a})

      # Start Group on B (A already connected, C already connected but no Group)
      TestCluster.start_group(node_b, opts)
      TestCluster.spawn_register(node_b, name, "from_b", %{origin: :b})

      # A and B should discover each other
      TestCluster.assert_eventually(fn ->
        match?({_, %{origin: :b}}, TestCluster.rpc!(node_a, Group, :lookup, [name, "from_b"])) and
          match?({_, %{origin: :a}}, TestCluster.rpc!(node_b, Group, :lookup, [name, "from_a"]))
      end)

      # Start Group on C (A and B already connected AND running Group)
      TestCluster.start_group(node_c, opts)
      TestCluster.spawn_register(node_c, name, "from_c", %{origin: :c})

      # All 3 should see all data
      expected = %{"from_a" => :a, "from_b" => :b, "from_c" => :c}

      TestCluster.assert_eventually(
        fn ->
          Enum.all?([node_a, node_b, node_c], fn check_node ->
            Enum.all?(expected, fn {key, origin} ->
              match?(
                {_, %{origin: ^origin}},
                TestCluster.rpc!(check_node, Group, :lookup, [name, key])
              )
            end)
          end)
        end,
        timeout: 5000
      )

      # All should see 2 peers
      for node <- [node_a, node_b, node_c] do
        nodes = TestCluster.rpc!(node, Group, :nodes, [name])
        assert length(nodes) == 2, "#{node} sees #{length(nodes)} peers, expected 2"
      end
    end

    test "new Erlang connection after Group start discovers via nodeup" do
      # Exercises the nodeup path: A starts Group, then a brand new peer node
      # is started (new Erlang connection triggers nodeup on A).
      peers_a = TestCluster.start_peers(1)
      [{_, node_a}] = peers_a

      name = :"dist_nodeup_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      TestCluster.start_group(node_a, opts)
      TestCluster.spawn_register(node_a, name, "user/early", %{from: :a})

      # Start a brand new peer — this triggers nodeup on A
      peers_b = TestCluster.start_peers(1)
      [{_, node_b}] = peers_b

      on_exit(fn ->
        TestCluster.stop_peers(peers_a)
        TestCluster.stop_peers(peers_b)
      end)

      TestCluster.start_group(node_b, opts)
      TestCluster.spawn_register(node_b, name, "user/late", %{from: :b})

      # Both should see each other's data
      TestCluster.assert_eventually(
        fn ->
          match?(
            {_, %{from: :b}},
            TestCluster.rpc!(node_a, Group, :lookup, [name, "user/late"])
          ) and
            match?(
              {_, %{from: :a}},
              TestCluster.rpc!(node_b, Group, :lookup, [name, "user/early"])
            )
        end,
        timeout: 5000
      )
    end
  end

  describe "chatty convergence" do
    @tag timeout: 60_000
    test "many clusters, registrations, groups, churn, and dispatch all converge" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      nodes = [node_a, node_b, node_c]
      name = :"dist_chatty_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 4]

      start_group_on_peers(peers, opts)

      # --- Phase 1: connect clusters across nodes ---
      # 10 "org" clusters, each node joins a subset
      cluster_count = 10

      clusters =
        for i <- 1..cluster_count do
          "org/#{i}"
        end

      # Every node connects to every cluster
      for node <- nodes, cluster <- clusters do
        TestCluster.rpc!(node, Group, :connect, [name, cluster])
      end

      # --- Phase 2: registrations spread across clusters and nodes ---
      # 5 registrations per cluster per node = 150 total
      reg_pids =
        for {node, ni} <- Enum.with_index(nodes),
            cluster <- clusters,
            ri <- 1..5 do
          key = "user/#{ni}_#{ri}"
          TestCluster.spawn_register_in_cluster(node, name, key, %{node: ni, i: ri}, cluster)
          {node, cluster, key}
        end

      # --- Phase 3: process group joins across clusters ---
      # 3 groups per cluster, 2 members per group from different nodes = 60 joins
      group_pids =
        for {cluster, ci} <- Enum.with_index(clusters),
            gi <- 1..3 do
          group_key = "room/#{ci}_#{gi}"
          node1 = Enum.at(nodes, rem(ci + gi, 3))
          node2 = Enum.at(nodes, rem(ci + gi + 1, 3))

          pid1 =
            TestCluster.spawn_join(node1, name, group_key, %{seat: 1}, cluster: cluster)

          pid2 =
            TestCluster.spawn_join(node2, name, group_key, %{seat: 2}, cluster: cluster)

          {cluster, group_key, [pid1, pid2]}
        end

      # --- Phase 4: verify all data converged on all nodes ---
      # Check registrations: every node should see every registration
      for {_reg_node, cluster, key} <- reg_pids do
        TestCluster.assert_eventually(
          fn ->
            Enum.all?(nodes, fn check_node ->
              TestCluster.rpc!(check_node, Group, :lookup, [name, key, [cluster: cluster]]) != nil
            end)
          end,
          timeout: 10_000
        )
      end

      # Check groups: every node should see 2 members per group
      for {cluster, group_key, _pids} <- group_pids do
        TestCluster.assert_eventually(
          fn ->
            Enum.all?(nodes, fn check_node ->
              members =
                TestCluster.rpc!(check_node, Group, :members, [
                  name,
                  group_key,
                  [cluster: cluster]
                ])

              length(members) == 2
            end)
          end,
          timeout: 10_000
        )
      end

      # --- Phase 5: churn — kill some processes and verify cleanup ---
      # Kill 2 registrations per cluster (20 total) by killing the remote pid
      killed_keys =
        for cluster <- Enum.take(clusters, 5),
            ni <- 0..1 do
          key = "user/#{ni}_1"

          {pid, _meta} =
            TestCluster.rpc!(Enum.at(nodes, ni), Group, :lookup, [name, key, [cluster: cluster]])

          Process.exit(pid, :kill)
          {cluster, key}
        end

      # Verify killed registrations are cleaned up on all nodes
      for {cluster, key} <- killed_keys do
        TestCluster.assert_eventually(
          fn ->
            Enum.all?(nodes, fn check_node ->
              TestCluster.rpc!(check_node, Group, :lookup, [name, key, [cluster: cluster]]) == nil
            end)
          end,
          timeout: 10_000
        )
      end

      # --- Phase 6: disconnect and verify cleanup ---

      # 6a: Node A disconnects from "org/10" — A's registrations and groups
      # should be purged from B and C, but B and C keep their own data
      dropped_cluster = "org/10"
      TestCluster.rpc!(node_a, Group, :disconnect, [name, dropped_cluster])

      # A's registrations in org/10 should be gone everywhere
      TestCluster.assert_eventually(
        fn ->
          Enum.all?(nodes, fn check_node ->
            TestCluster.rpc!(check_node, Group, :lookup, [
              name,
              "user/0_1",
              [cluster: dropped_cluster]
            ]) == nil
          end)
        end,
        timeout: 10_000
      )

      # B and C still see each other's org/10 data
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [
          name,
          "user/1_2",
          [cluster: dropped_cluster]
        ]) != nil and
          TestCluster.rpc!(node_c, Group, :lookup, [
            name,
            "user/2_2",
            [cluster: dropped_cluster]
          ]) != nil
      end)

      # 6b: B and C also disconnect — full cluster teardown
      TestCluster.rpc!(node_b, Group, :disconnect, [name, dropped_cluster])
      TestCluster.rpc!(node_c, Group, :disconnect, [name, dropped_cluster])

      # All org/10 data should be gone everywhere
      TestCluster.assert_eventually(
        fn ->
          Enum.all?(nodes, fn check_node ->
            TestCluster.rpc!(check_node, Group, :lookup, [
              name,
              "user/1_2",
              [cluster: dropped_cluster]
            ]) == nil
          end)
        end,
        timeout: 10_000
      )

      # Surviving registrations in other clusters should still be there
      surviving_key = "user/2_2"
      surviving_cluster = "org/6"

      TestCluster.assert_eventually(
        fn ->
          Enum.all?(nodes, fn check_node ->
            TestCluster.rpc!(check_node, Group, :lookup, [
              name,
              surviving_key,
              [cluster: surviving_cluster]
            ]) != nil
          end)
        end,
        timeout: 10_000
      )

      # --- Phase 7: dispatch works across remaining clusters ---
      # Join a group, dispatch to it from another node, verify delivery
      dispatch_cluster = "org/3"
      dispatch_group = "dispatch_room"

      receiver =
        TestCluster.spawn_join_forwarder(node_a, name, dispatch_group, self(),
          cluster: dispatch_cluster
        )

      # Wait for join to replicate
      TestCluster.assert_eventually(fn ->
        members =
          TestCluster.rpc!(node_c, Group, :members, [
            name,
            dispatch_group,
            [cluster: dispatch_cluster]
          ])

        Enum.any?(members, fn {pid, _} -> pid == receiver end)
      end)

      # Dispatch from node_c
      TestCluster.rpc!(node_c, Group, :dispatch, [
        name,
        dispatch_group,
        {:ping, 42},
        [cluster: dispatch_cluster]
      ])

      assert_receive {:forwarded, {:ping, 42}}, 5_000

      # --- Phase 8: late cluster connect with late joiner ---
      # A and B connect to a new cluster and populate it with data.
      # C joins later and must receive all existing data.
      late_cluster = "late/lobby"

      # A and B connect and register data
      TestCluster.rpc!(node_a, Group, :connect, [name, late_cluster])
      TestCluster.rpc!(node_b, Group, :connect, [name, late_cluster])

      TestCluster.spawn_register_in_cluster(
        node_a,
        name,
        "late_user/0",
        %{late: true},
        late_cluster
      )

      TestCluster.spawn_register_in_cluster(
        node_b,
        name,
        "late_user/1",
        %{late: true},
        late_cluster
      )

      TestCluster.spawn_join(node_a, name, "late_room", %{seat: 0}, cluster: late_cluster)
      TestCluster.spawn_join(node_b, name, "late_room", %{seat: 1}, cluster: late_cluster)

      # Wait for A and B to converge between themselves
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :lookup, [name, "late_user/1", [cluster: late_cluster]]) !=
          nil and
          TestCluster.rpc!(node_b, Group, :lookup, [name, "late_user/0", [cluster: late_cluster]]) !=
            nil
      end)

      # C joins late — should receive all existing data from A and B
      TestCluster.rpc!(node_c, Group, :connect, [name, late_cluster])

      # C adds its own data
      TestCluster.spawn_register_in_cluster(
        node_c,
        name,
        "late_user/2",
        %{late: true},
        late_cluster
      )

      TestCluster.spawn_join(node_c, name, "late_room", %{seat: 2}, cluster: late_cluster)

      # All nodes should see all 3 registrations and 3 group members
      TestCluster.assert_eventually(
        fn ->
          Enum.all?(nodes, fn check_node ->
            r0 =
              TestCluster.rpc!(check_node, Group, :lookup, [
                name,
                "late_user/0",
                [cluster: late_cluster]
              ])

            r1 =
              TestCluster.rpc!(check_node, Group, :lookup, [
                name,
                "late_user/1",
                [cluster: late_cluster]
              ])

            r2 =
              TestCluster.rpc!(check_node, Group, :lookup, [
                name,
                "late_user/2",
                [cluster: late_cluster]
              ])

            members =
              TestCluster.rpc!(check_node, Group, :members, [
                name,
                "late_room",
                [cluster: late_cluster]
              ])

            r0 != nil and r1 != nil and r2 != nil and length(members) == 3
          end)
        end,
        timeout: 10_000
      )

      # --- Phase 9: final consistency check ---
      # All nodes should agree on sample keys across surviving clusters
      for cluster <- Enum.take(clusters, 9) do
        sample_key = "user/2_3"

        results =
          Enum.map(nodes, fn node ->
            TestCluster.rpc!(node, Group, :lookup, [name, sample_key, [cluster: cluster]])
          end)

        # All nodes should agree (all non-nil or all nil)
        assert length(Enum.uniq_by(results, &is_nil/1)) == 1,
               "Nodes disagree on #{sample_key} in #{cluster}: #{inspect(results)}"
      end
    end
  end

  describe "ETS table consistency" do
    @tag timeout: 60_000
    test "no orphaned reg_by_pid entries after partition heal with conflicting keys" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_ets_orphan_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 4]

      start_group_on_peers(peers, opts)

      # Verify connectivity
      pid_init = TestCluster.spawn_register(node_a, name, "init", %{})

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "init"]) != nil
      end)

      TestCluster.rpc!(node_a, Process, :exit, [pid_init, :kill])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "init"]) == nil
      end)

      # Set up nodedown monitors
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())

      # Partition
      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # Register same key on both sides during partition.
      # B registers later (higher timestamp) — B will win the merge.
      _pid_a = TestCluster.spawn_register(node_a, name, "conflict/1", %{side: :a})
      # Ensure B's registration gets a strictly higher timestamp
      Process.sleep(50)
      _pid_b = TestCluster.spawn_register(node_b, name, "conflict/1", %{side: :b})

      # Heal partition
      TestCluster.reconnect_nodes(node_a, node_b)

      # Wait for convergence — one winner on both sides
      TestCluster.assert_eventually(
        fn ->
          lookup_a = TestCluster.rpc!(node_a, Group, :lookup, [name, "conflict/1"])
          lookup_b = TestCluster.rpc!(node_b, Group, :lookup, [name, "conflict/1"])

          case {lookup_a, lookup_b} do
            {{pid_a, _}, {pid_b, _}} when is_pid(pid_a) and is_pid(pid_b) ->
              pid_a == pid_b

            _ ->
              false
          end
        end,
        timeout: 10_000
      )

      # Flush all shards to drain async fan-out messages
      TestCluster.flush_shards(node_a, name)
      TestCluster.flush_shards(node_b, name)

      # ETS dual-index tables should be consistent (no orphaned by_pid entries)
      assert :ok =
               TestCluster.rpc!(node_a, Group.TestCluster, :assert_ets_consistent, [name])

      assert :ok =
               TestCluster.rpc!(node_b, Group.TestCluster, :assert_ets_consistent, [name])
    end

    @tag timeout: 60_000
    test "orphaned by_pid entry does not corrupt winner on loser process death" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_ets_corrupt_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 4]

      start_group_on_peers(peers, opts)

      # Verify connectivity
      pid_init = TestCluster.spawn_register(node_a, name, "init", %{})

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "init"]) != nil
      end)

      TestCluster.rpc!(node_a, Process, :exit, [pid_init, :kill])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "init"]) == nil
      end)

      # Set up nodedown monitors
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())

      # Partition
      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # A registers first (lower timestamp), B registers later (higher timestamp).
      # After heal, B's entry wins via merge_remote_cluster_data on A.
      pid_a = TestCluster.spawn_register(node_a, name, "conflict/2", %{side: :a})
      # Ensure B's registration gets a strictly higher timestamp
      Process.sleep(50)
      _pid_b = TestCluster.spawn_register(node_b, name, "conflict/2", %{side: :b})

      # Heal partition
      TestCluster.reconnect_nodes(node_a, node_b)

      # Wait for convergence
      TestCluster.assert_eventually(
        fn ->
          lookup_a = TestCluster.rpc!(node_a, Group, :lookup, [name, "conflict/2"])
          lookup_b = TestCluster.rpc!(node_b, Group, :lookup, [name, "conflict/2"])

          case {lookup_a, lookup_b} do
            {{pid, _}, {pid, _}} when is_pid(pid) -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )

      # Capture the winner
      {winner_pid, _} = TestCluster.rpc!(node_a, Group, :lookup, [name, "conflict/2"])

      # Now kill the loser process (pid_a on node_a).
      # If there's an orphaned by_pid entry for pid_a, the DOWN handler will
      # incorrectly delete the winner's reg_by_key entry for "conflict/2".
      TestCluster.rpc!(node_a, Process, :exit, [pid_a, :kill])

      # Flush shards to process the DOWN message and any replication
      TestCluster.flush_shards(node_a, name)
      TestCluster.flush_shards(node_b, name)

      # The winner's entry should still be visible on both nodes
      lookup_a = TestCluster.rpc!(node_a, Group, :lookup, [name, "conflict/2"])
      lookup_b = TestCluster.rpc!(node_b, Group, :lookup, [name, "conflict/2"])

      assert {^winner_pid, _} = lookup_a,
             "Winner's entry was deleted on node_a after loser died (orphaned by_pid corruption)"

      assert {^winner_pid, _} = lookup_b,
             "Winner's entry was deleted on node_b after loser died"

      # Tables should also be fully consistent
      assert :ok =
               TestCluster.rpc!(node_a, Group.TestCluster, :assert_ets_consistent, [name])

      assert :ok =
               TestCluster.rpc!(node_b, Group.TestCluster, :assert_ets_consistent, [name])
    end

    @tag timeout: 60_000
    test "many conflicting keys across shards during partition" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_ets_multi_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 8]

      start_group_on_peers(peers, opts)

      # Verify connectivity
      pid_init = TestCluster.spawn_register(node_a, name, "init", %{})

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "init"]) != nil
      end)

      TestCluster.rpc!(node_a, Process, :exit, [pid_init, :kill])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "init"]) == nil
      end)

      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())

      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # Register 20 conflicting keys on each side (spread across 8 shards).
      # A registers first, then B registers — B gets higher timestamps so B wins merges.
      num_keys = 20

      a_pids =
        for i <- 1..num_keys do
          TestCluster.spawn_register(node_a, name, "conflict/#{i}", %{side: :a, i: i})
        end

      # Ensure B's registrations get strictly higher timestamps
      Process.sleep(50)

      b_pids =
        for i <- 1..num_keys do
          TestCluster.spawn_register(node_b, name, "conflict/#{i}", %{side: :b, i: i})
        end

      # Also register non-conflicting keys to add noise
      for i <- 1..10 do
        TestCluster.spawn_register(node_a, name, "only_a/#{i}", %{side: :a})
      end

      for i <- 1..10 do
        TestCluster.spawn_register(node_b, name, "only_b/#{i}", %{side: :b})
      end

      TestCluster.reconnect_nodes(node_a, node_b)

      # Wait for all conflicting keys to converge
      TestCluster.assert_eventually(
        fn ->
          Enum.all?(1..num_keys, fn i ->
            la = TestCluster.rpc!(node_a, Group, :lookup, [name, "conflict/#{i}"])
            lb = TestCluster.rpc!(node_b, Group, :lookup, [name, "conflict/#{i}"])

            case {la, lb} do
              {{pa, _}, {pb, _}} -> pa == pb
              _ -> false
            end
          end)
        end,
        timeout: 10_000
      )

      TestCluster.flush_shards(node_a, name)
      TestCluster.flush_shards(node_b, name)

      assert :ok =
               TestCluster.rpc!(node_a, Group.TestCluster, :assert_ets_consistent, [name])

      assert :ok =
               TestCluster.rpc!(node_b, Group.TestCluster, :assert_ets_consistent, [name])

      # Kill all losing processes — winners must survive
      winners =
        for i <- 1..num_keys do
          {pid, _} = TestCluster.rpc!(node_a, Group, :lookup, [name, "conflict/#{i}"])
          pid
        end

      Enum.each(a_pids, fn pid ->
        if pid not in winners do
          TestCluster.rpc!(node_a, Process, :exit, [pid, :kill])
        end
      end)

      Enum.each(b_pids, fn pid ->
        if pid not in winners do
          TestCluster.rpc!(node_b, Process, :exit, [pid, :kill])
        end
      end)

      TestCluster.flush_shards(node_a, name)
      TestCluster.flush_shards(node_b, name)

      # All winners must still be visible
      for i <- 1..num_keys do
        la = TestCluster.rpc!(node_a, Group, :lookup, [name, "conflict/#{i}"])
        assert la != nil, "conflict/#{i} winner was lost after loser cleanup"
      end

      assert :ok =
               TestCluster.rpc!(node_a, Group.TestCluster, :assert_ets_consistent, [name])

      assert :ok =
               TestCluster.rpc!(node_b, Group.TestCluster, :assert_ets_consistent, [name])
    end

    @tag timeout: 60_000
    test "named cluster partition heal: no orphans" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_ets_named_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 4]

      start_group_on_peers(peers, opts)

      # Both join named cluster
      TestCluster.rpc!(node_a, Group, :connect, [name, "lobby"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "lobby"])

      # Verify connectivity with a probe key
      pid_init =
        TestCluster.spawn_register_in_cluster(node_a, name, "probe", %{}, "lobby")

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "probe", [cluster: "lobby"]]) != nil
      end)

      TestCluster.rpc!(node_a, Process, :exit, [pid_init, :kill])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "probe", [cluster: "lobby"]]) == nil
      end)

      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())

      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # Register conflicting keys in named cluster on both sides.
      # A first, then B — B gets higher timestamp.
      _pid_a =
        TestCluster.spawn_register_in_cluster(node_a, name, "user/1", %{side: :a}, "lobby")

      # Ensure B's registration gets a strictly higher timestamp
      Process.sleep(50)

      _pid_b =
        TestCluster.spawn_register_in_cluster(node_b, name, "user/1", %{side: :b}, "lobby")

      # Also register in nil cluster for cross-cluster noise
      TestCluster.spawn_register(node_a, name, "nil_key", %{})
      TestCluster.spawn_register(node_b, name, "nil_key_b", %{})

      # Reconnect — both nodes need to re-connect to "lobby" as well since
      # cluster membership for remote node was purged on nodedown
      TestCluster.reconnect_nodes(node_a, node_b)

      # Wait for named cluster conflict to resolve
      TestCluster.assert_eventually(
        fn ->
          la =
            TestCluster.rpc!(node_a, Group, :lookup, [name, "user/1", [cluster: "lobby"]])

          lb =
            TestCluster.rpc!(node_b, Group, :lookup, [name, "user/1", [cluster: "lobby"]])

          case {la, lb} do
            {{pa, _}, {pb, _}} -> pa == pb
            _ -> false
          end
        end,
        timeout: 10_000
      )

      TestCluster.flush_shards(node_a, name)
      TestCluster.flush_shards(node_b, name)

      assert :ok =
               TestCluster.rpc!(node_a, Group.TestCluster, :assert_ets_consistent, [name])

      assert :ok =
               TestCluster.rpc!(node_b, Group.TestCluster, :assert_ets_consistent, [name])
    end

    @tag timeout: 60_000
    test "node death: all table types fully cleaned on survivor" do
      peers = TestCluster.start_peers(2)

      [{peer_a_pid, node_a}, {peer_b_pid, node_b}] = peers

      name = :"dist_ets_nodedown_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 4]

      start_group_on_peers(peers, opts)

      # B connects to named clusters
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "chat"])
      # A also connects so it's a shared cluster
      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_a, Group, :connect, [name, "chat"])

      # B registers in nil cluster
      TestCluster.spawn_register(node_b, name, "user/b1", %{t: :nil_reg})
      TestCluster.spawn_register(node_b, name, "user/b2", %{t: :nil_reg})

      # B joins in nil cluster
      TestCluster.spawn_join(node_b, name, "room/nil", %{t: :nil_pg})

      # B registers in named clusters
      TestCluster.spawn_register_in_cluster(node_b, name, "player/1", %{}, "game")
      TestCluster.spawn_register_in_cluster(node_b, name, "player/2", %{}, "game")
      TestCluster.spawn_register_in_cluster(node_b, name, "chatter/1", %{}, "chat")

      # B joins in named clusters
      TestCluster.spawn_join(node_b, name, "arena/1", %{}, cluster: "game")
      TestCluster.spawn_join(node_b, name, "channel/1", %{}, cluster: "chat")

      # Wait for all to replicate to A
      TestCluster.assert_eventually(
        fn ->
          TestCluster.rpc!(node_a, Group, :lookup, [name, "user/b2"]) != nil and
            TestCluster.rpc!(node_a, Group, :lookup, [
              name,
              "player/2",
              [cluster: "game"]
            ]) != nil and
            TestCluster.rpc!(node_a, Group, :lookup, [
              name,
              "chatter/1",
              [cluster: "chat"]
            ]) != nil and
            length(TestCluster.rpc!(node_a, Group, :members, [name, "room/nil"])) == 1 and
            length(
              TestCluster.rpc!(node_a, Group, :members, [
                name,
                "arena/1",
                [cluster: "game"]
              ])
            ) == 1
        end,
        timeout: 5000
      )

      # Kill node B
      :peer.stop(peer_b_pid)

      # Wait for nodedown cleanup
      TestCluster.assert_eventually(
        fn ->
          TestCluster.rpc!(node_a, Group, :lookup, [name, "user/b1"]) == nil and
            TestCluster.rpc!(node_a, Group, :lookup, [name, "user/b2"]) == nil and
            TestCluster.rpc!(node_a, Group, :lookup, [
              name,
              "player/1",
              [cluster: "game"]
            ]) == nil and
            TestCluster.rpc!(node_a, Group, :lookup, [
              name,
              "chatter/1",
              [cluster: "chat"]
            ]) == nil and
            TestCluster.rpc!(node_a, Group, :members, [name, "room/nil"]) == [] and
            TestCluster.rpc!(node_a, Group, :members, [
              name,
              "arena/1",
              [cluster: "game"]
            ]) == [] and
            TestCluster.rpc!(node_a, Group, :members, [
              name,
              "channel/1",
              [cluster: "chat"]
            ]) == []
        end,
        timeout: 5000
      )

      # All ETS tables should be consistent (no orphaned by_pid entries)
      assert :ok =
               TestCluster.rpc!(node_a, Group.TestCluster, :assert_ets_consistent, [name])

      # Verify node B's entries are truly gone from ALL tables (not just by_key)
      num_shards = 4

      for shard <- 0..(num_shards - 1) do
        for table_fn <- [
              &Group.Replica.Data.reg_by_key_table/2,
              &Group.Replica.Data.reg_by_pid_table/2,
              &Group.Replica.Data.pg_by_key_table/2,
              &Group.Replica.Data.pg_by_pid_table/2
            ] do
          table = table_fn.(name, shard)

          size =
            TestCluster.rpc!(node_a, :ets, :info, [table, :size])

          assert size == 0,
                 "Table #{table} on node_a has #{size} entries after node_b death"
        end
      end

      on_exit(fn -> :peer.stop(peer_a_pid) end)
    end

    @tag timeout: 60_000
    test "partition heal + immediate process death: no stale entries" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_ets_healdie_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 4]

      start_group_on_peers(peers, opts)

      pid_init = TestCluster.spawn_register(node_a, name, "init", %{})

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "init"]) != nil
      end)

      TestCluster.rpc!(node_a, Process, :exit, [pid_init, :kill])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "init"]) == nil
      end)

      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())

      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # Register + join on both sides during partition (capture ALL pids)
      a_pids =
        for i <- 1..10 do
          reg_pid = TestCluster.spawn_register(node_a, name, "key/#{i}", %{side: :a})
          join_pid = TestCluster.spawn_join(node_a, name, "grp/#{i}", %{side: :a})
          [reg_pid, join_pid]
        end
        |> List.flatten()

      b_pids =
        for i <- 1..10 do
          reg_pid = TestCluster.spawn_register(node_b, name, "bkey/#{i}", %{side: :b})
          join_pid = TestCluster.spawn_join(node_b, name, "bgrp/#{i}", %{side: :b})
          [reg_pid, join_pid]
        end
        |> List.flatten()

      # Heal partition
      TestCluster.reconnect_nodes(node_a, node_b)

      # Wait for some replication to start
      TestCluster.assert_eventually(
        fn ->
          TestCluster.rpc!(node_b, Group, :lookup, [name, "key/1"]) != nil
        end,
        timeout: 10_000
      )

      # IMMEDIATELY kill all processes on both sides — race with ongoing merge
      Enum.each(a_pids, fn pid ->
        TestCluster.rpc!(node_a, Process, :exit, [pid, :kill])
      end)

      Enum.each(b_pids, fn pid ->
        TestCluster.rpc!(node_b, Process, :exit, [pid, :kill])
      end)

      # Wait for all cleanup to propagate (both registry and pg)
      TestCluster.assert_eventually(
        fn ->
          Enum.all?(1..10, fn i ->
            TestCluster.rpc!(node_a, Group, :lookup, [name, "key/#{i}"]) == nil and
              TestCluster.rpc!(node_a, Group, :lookup, [name, "bkey/#{i}"]) == nil and
              TestCluster.rpc!(node_b, Group, :lookup, [name, "key/#{i}"]) == nil and
              TestCluster.rpc!(node_b, Group, :lookup, [name, "bkey/#{i}"]) == nil and
              TestCluster.rpc!(node_a, Group, :members, [name, "grp/#{i}"]) == [] and
              TestCluster.rpc!(node_a, Group, :members, [name, "bgrp/#{i}"]) == [] and
              TestCluster.rpc!(node_b, Group, :members, [name, "grp/#{i}"]) == [] and
              TestCluster.rpc!(node_b, Group, :members, [name, "bgrp/#{i}"]) == []
          end)
        end,
        timeout: 10_000
      )

      # Tables must be consistent AND empty
      assert :ok =
               TestCluster.rpc!(node_a, Group.TestCluster, :assert_ets_consistent, [name])

      assert :ok =
               TestCluster.rpc!(node_b, Group.TestCluster, :assert_ets_consistent, [name])

      num_shards = 4

      for node <- [node_a, node_b], shard <- 0..(num_shards - 1) do
        for {label, table_fn} <- [
              {"reg_by_key", &Group.Replica.Data.reg_by_key_table/2},
              {"reg_by_pid", &Group.Replica.Data.reg_by_pid_table/2},
              {"pg_by_key", &Group.Replica.Data.pg_by_key_table/2},
              {"pg_by_pid", &Group.Replica.Data.pg_by_pid_table/2}
            ] do
          table = table_fn.(name, shard)
          size = TestCluster.rpc!(node, :ets, :info, [table, :size])

          assert size == 0,
                 "#{label} shard #{shard} on #{node} has #{size} stale entries"
        end
      end
    end

    @tag timeout: 60_000
    test "node flapping: ETS consistent after rapid disconnect/reconnect cycles" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_ets_flap_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 4]

      start_group_on_peers(peers, opts)

      # Both join a named cluster
      TestCluster.rpc!(node_a, Group, :connect, [name, "live"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "live"])

      # Register data in both nil and named clusters
      TestCluster.spawn_register(node_a, name, "stable/a", %{})
      TestCluster.spawn_register_in_cluster(node_a, name, "live/a", %{}, "live")
      TestCluster.spawn_join(node_b, name, "room/nil", %{})
      TestCluster.spawn_join(node_b, name, "room/live", %{}, cluster: "live")

      # Wait for full replication
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "stable/a"]) != nil and
          TestCluster.rpc!(node_a, Group, :members, [name, "room/nil"]) != []
      end)

      TestCluster.monitor_nodes_on(node_a, self())

      # Flap 3 times
      for _i <- 1..3 do
        TestCluster.disconnect_nodes(node_a, node_b)
        assert_receive {:nodedown_on_remote, ^node_b}, 5000

        TestCluster.reconnect_nodes(node_a, node_b)

        # Wait for data to re-sync
        TestCluster.assert_eventually(
          fn ->
            TestCluster.rpc!(node_b, Group, :lookup, [name, "stable/a"]) != nil and
              TestCluster.rpc!(node_a, Group, :members, [name, "room/nil"]) != []
          end,
          timeout: 5000
        )
      end

      TestCluster.flush_shards(node_a, name)
      TestCluster.flush_shards(node_b, name)

      # After all flapping, tables must be consistent
      assert :ok =
               TestCluster.rpc!(node_a, Group.TestCluster, :assert_ets_consistent, [name])

      assert :ok =
               TestCluster.rpc!(node_b, Group.TestCluster, :assert_ets_consistent, [name])
    end

    @tag timeout: 60_000
    test "cluster disconnect + nodedown: overlapping cleanup paths" do
      peers = TestCluster.start_peers(2)

      [{peer_a_pid, node_a}, {peer_b_pid, node_b}] = peers

      name = :"dist_ets_disc_down_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 4]

      start_group_on_peers(peers, opts)

      # Both join named cluster
      TestCluster.rpc!(node_a, Group, :connect, [name, "ephemeral"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "ephemeral"])

      # B registers and joins in the named cluster
      TestCluster.spawn_register_in_cluster(node_b, name, "eph/1", %{}, "ephemeral")
      TestCluster.spawn_register_in_cluster(node_b, name, "eph/2", %{}, "ephemeral")
      TestCluster.spawn_join(node_b, name, "eph_room", %{}, cluster: "ephemeral")

      # Also nil cluster data
      TestCluster.spawn_register(node_b, name, "nil/b1", %{})

      # Wait for replication
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :lookup, [name, "eph/2", [cluster: "ephemeral"]]) !=
          nil and
          TestCluster.rpc!(node_a, Group, :lookup, [name, "nil/b1"]) != nil
      end)

      # B disconnects from the named cluster
      TestCluster.rpc!(node_b, Group, :disconnect, [name, "ephemeral"])

      # Wait for disconnect to propagate to A
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :lookup, [name, "eph/1", [cluster: "ephemeral"]]) ==
          nil
      end)

      # Then B dies entirely
      :peer.stop(peer_b_pid)

      # A should clean up everything — both the cluster disconnect and nodedown
      TestCluster.assert_eventually(
        fn ->
          TestCluster.rpc!(node_a, Group, :lookup, [name, "eph/1", [cluster: "ephemeral"]]) ==
            nil and
            TestCluster.rpc!(node_a, Group, :lookup, [name, "nil/b1"]) == nil and
            TestCluster.rpc!(node_a, Group, :members, [
              name,
              "eph_room",
              [cluster: "ephemeral"]
            ]) == []
        end,
        timeout: 5000
      )

      assert :ok =
               TestCluster.rpc!(node_a, Group.TestCluster, :assert_ets_consistent, [name])

      # Only A's own nil cluster membership should remain (and "ephemeral" since A is still connected)
      num_shards = 4

      for shard <- 0..(num_shards - 1) do
        for {label, table_fn} <- [
              {"reg_by_key", &Group.Replica.Data.reg_by_key_table/2},
              {"reg_by_pid", &Group.Replica.Data.reg_by_pid_table/2},
              {"pg_by_key", &Group.Replica.Data.pg_by_key_table/2},
              {"pg_by_pid", &Group.Replica.Data.pg_by_pid_table/2}
            ] do
          table = table_fn.(name, shard)
          size = TestCluster.rpc!(node_a, :ets, :info, [table, :size])

          assert size == 0,
                 "#{label} shard #{shard} on node_a has #{size} entries after B disconnect+death"
        end
      end

      on_exit(fn -> :peer.stop(peer_a_pid) end)
    end
  end

  describe "Jepsen-style: deterministic conflict tiebreaker" do
    @tag timeout: 60_000
    test "conflict resolution converges even with near-simultaneous registrations" do
      # Verifies that both nodes agree on the same winner after partition heal.
      # With the old broken `>=` tiebreaker, equal timestamps would cause mutual
      # kill (both processes die). The deterministic pid-based tiebreaker prevents this.
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_tiebreak_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Verify connectivity
      pid_init = TestCluster.spawn_register(node_a, name, "init", %{})

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "init"]) != nil
      end)

      TestCluster.rpc!(node_a, Process, :exit, [pid_init, :kill])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "init"]) == nil
      end)

      # Partition
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())
      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # Register same key on both sides with minimal delay (near-simultaneous)
      pid_a = TestCluster.spawn_register(node_a, name, "user/race", %{side: :a})
      pid_b = TestCluster.spawn_register(node_b, name, "user/race", %{side: :b})

      # Heal
      TestCluster.reconnect_nodes(node_a, node_b)

      # Both nodes must agree on exactly one winner
      TestCluster.assert_eventually(
        fn ->
          lookup_a = TestCluster.rpc!(node_a, Group, :lookup, [name, "user/race"])
          lookup_b = TestCluster.rpc!(node_b, Group, :lookup, [name, "user/race"])

          case {lookup_a, lookup_b} do
            {{pid, _}, {pid, _}} when is_pid(pid) -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )

      # The winner must be alive (mutual kill bug would leave key unregistered
      # or registered to a dead pid)
      {winner_pid, _} = TestCluster.rpc!(node_a, Group, :lookup, [name, "user/race"])
      assert TestCluster.rpc!(node(winner_pid), Process, :alive?, [winner_pid])

      # Exactly one of {pid_a, pid_b} should be alive
      a_alive = TestCluster.rpc!(node_a, Process, :alive?, [pid_a])
      b_alive = TestCluster.rpc!(node_b, Process, :alive?, [pid_b])
      assert a_alive != b_alive, "Expected exactly one survivor, got a=#{a_alive} b=#{b_alive}"

      TestCluster.flush_shards(node_a, name)
      TestCluster.flush_shards(node_b, name)
      assert :ok = TestCluster.rpc!(node_a, Group.TestCluster, :assert_ets_consistent, [name])
      assert :ok = TestCluster.rpc!(node_b, Group.TestCluster, :assert_ets_consistent, [name])
    end
  end

  describe "Jepsen-style: nodedown cleans cluster_nodes on all shards" do
    @tag timeout: 60_000
    test "dead node removed from cluster_nodes even with many shards" do
      # Exercises the race where a non-zero shard processes a late peer_connect
      # from a dead node after shard 0 already cleaned cluster_nodes. With many
      # shards, the window for this race is wider.
      peers = TestCluster.start_peers(2)

      [{peer_a_pid, node_a}, {peer_b_pid, node_b}] = peers
      name = :"dist_stale_cn_#{System.unique_integer([:positive])}"
      # Many shards increases the chance of cross-shard timing issues
      opts = [name: name, shards: 8]

      start_group_on_peers(peers, opts)

      # Both connect to a named cluster
      TestCluster.rpc!(node_a, Group, :connect, [name, "ephemeral"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "ephemeral"])

      # B registers some data
      TestCluster.spawn_register_in_cluster(node_b, name, "eph/key", %{}, "ephemeral")
      TestCluster.spawn_register(node_b, name, "nil/key", %{})

      # Wait for replication
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :lookup, [name, "nil/key"]) != nil
      end)

      # Kill B
      :peer.stop(peer_b_pid)

      # A should not have B in any cluster_nodes — nil or named
      TestCluster.assert_eventually(
        fn ->
          nil_nodes = TestCluster.rpc!(node_a, Group, :nodes, [name])
          named_nodes = TestCluster.rpc!(node_a, Group, :nodes, [name, "ephemeral"])
          nil_nodes == [] and named_nodes == [node_a]
        end,
        timeout: 10_000
      )

      # Verify the raw ETS tables have no trace of B
      cn_table = Group.Replica.Data.cluster_nodes_table(name)
      nc_table = Group.Replica.Data.node_clusters_table(name)

      cn_entries = TestCluster.rpc!(node_a, :ets, :tab2list, [cn_table])
      nc_entries = TestCluster.rpc!(node_a, :ets, :tab2list, [nc_table])

      for {cluster, nd} <- cn_entries do
        assert nd != node_b,
               "Dead node #{node_b} still in cluster_nodes for cluster #{inspect(cluster)}"
      end

      for {nd, cluster} <- nc_entries do
        assert nd != node_b,
               "Dead node #{node_b} still in node_clusters for cluster #{inspect(cluster)}"
      end

      # All shards should be clean
      TestCluster.flush_shards(node_a, name)
      assert :ok = TestCluster.rpc!(node_a, Group.TestCluster, :assert_ets_consistent, [name])

      on_exit(fn -> :peer.stop(peer_a_pid) end)
    end
  end

  describe "Jepsen-style: conflict resolution lifecycle events" do
    @tag timeout: 60_000
    test "monitor receives :unregistered for evicted pid on partition heal" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_evict_event_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Set up monitors on both nodes to capture events
      monitor_a = TestCluster.spawn_monitor_forwarder(node_a, name, :all, self())
      monitor_b = TestCluster.spawn_monitor_forwarder(node_b, name, :all, self())
      assert_receive {:monitor_ready, ^monitor_a}, 5000
      assert_receive {:monitor_ready, ^monitor_b}, 5000

      # Verify connectivity
      pid_init = TestCluster.spawn_register(node_a, name, "init", %{})

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "init"]) != nil
      end)

      TestCluster.rpc!(node_a, Process, :exit, [pid_init, :kill])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, Group, :lookup, [name, "init"]) == nil
      end)

      # Drain any events from the init phase
      flush_events()

      # Partition
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())
      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # Register same key on both sides. A first, B second (B gets higher timestamp).
      pid_a = TestCluster.spawn_register(node_a, name, "user/evict", %{side: :a})
      Process.sleep(50)
      pid_b = TestCluster.spawn_register(node_b, name, "user/evict", %{side: :b})

      # Drain partition-side registration events
      flush_events()

      # Heal
      TestCluster.reconnect_nodes(node_a, node_b)

      # Wait for convergence — pid_b should win (higher timestamp)
      TestCluster.assert_eventually(
        fn ->
          lookup_a = TestCluster.rpc!(node_a, Group, :lookup, [name, "user/evict"])
          lookup_b = TestCluster.rpc!(node_b, Group, :lookup, [name, "user/evict"])

          match?({^pid_b, _}, lookup_a) and match?({^pid_b, _}, lookup_b)
        end,
        timeout: 10_000
      )

      # Node A should have dispatched :unregistered for pid_a (the evicted local entry)
      # when resolve_conflict picked pid_b as winner
      assert_received_event(:unregistered, "user/evict", pid_a, :resolve_conflict)

      # pid_b should eventually have a :registered event on node A
      # (from the winner's re-broadcast replicate_register)
      assert_received_event(:registered, "user/evict", pid_b)

      # ETS consistency
      TestCluster.flush_shards(node_a, name)
      TestCluster.flush_shards(node_b, name)
      assert :ok = TestCluster.rpc!(node_a, Group.TestCluster, :assert_ets_consistent, [name])
      assert :ok = TestCluster.rpc!(node_b, Group.TestCluster, :assert_ets_consistent, [name])
    end
  end

  describe "event batching" do
    test "nodedown batches all same-shard events into one message" do
      peers = TestCluster.start_peers(2)
      [{_peer_a, node_a}, {peer_b, node_b}] = peers
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      num_shards = 4
      name = :"batch_nodedown_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: num_shards]

      start_group_on_peers(peers, opts)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :nodes, [name]) == [node_b]
      end)

      # Find 3 keys that hash to the same shard (shard 0)
      keys =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(fn i -> "nodedown_batch/key_#{i}" end)
        |> Stream.filter(fn key -> :erlang.phash2({nil, key}, num_shards) == 0 end)
        |> Enum.take(3)

      # Register all 3 from node_b
      for key <- keys do
        TestCluster.spawn_register(node_b, name, key, %{k: key})
      end

      # Wait for replication to node_a
      for key <- keys do
        TestCluster.assert_eventually(fn ->
          TestCluster.rpc!(node_a, Group, :lookup, [name, key]) != nil
        end)
      end

      # Set up batch-aware monitor on node_a
      forwarder = TestCluster.spawn_batch_forwarder(node_a, name, :all, self())
      assert_receive {:monitor_ready, ^forwarder}, 1000

      # Kill node_b — nodedown triggers bulk purge
      :peer.stop(peer_b)

      # All 3 :unregistered events should arrive in a single batch
      # (they're on the same shard, processed in one nodedown handler turn)
      assert_receive {:got_batch, events}, 5000
      unreg_events = Enum.filter(events, &(&1.type == :unregistered))
      assert length(unreg_events) == 3
      assert Enum.map(unreg_events, & &1.key) |> Enum.sort() == Enum.sort(keys)
    end

    test "cluster disconnect batches purged events into one message per shard" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      num_shards = 4
      name = :"batch_disconnect_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: num_shards]

      start_group_on_peers(peers, opts)

      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      TestCluster.assert_eventually(fn ->
        length(TestCluster.rpc!(node_b, Group, :nodes, [name, "game"])) >= 2
      end)

      # Find 3 keys that hash to the same shard for the "game" cluster
      keys =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(fn i -> "disc_batch/key_#{i}" end)
        |> Stream.filter(fn key -> :erlang.phash2({"game", key}, num_shards) == 0 end)
        |> Enum.take(3)

      # A joins all 3 in "game" cluster
      for key <- keys do
        TestCluster.spawn_join(node_a, name, key, %{k: key}, cluster: "game")
      end

      # Wait for replication to B
      for key <- keys do
        TestCluster.assert_eventually(fn ->
          TestCluster.rpc!(node_b, Group, :members, [name, key, [cluster: "game"]]) != []
        end)
      end

      # Set up batch-aware monitor on B for "game" cluster
      forwarder = TestCluster.spawn_batch_forwarder(node_b, name, :all, self(), cluster: "game")
      assert_receive {:monitor_ready, ^forwarder}, 1000

      # A disconnects from "game" — triggers bulk purge on B
      TestCluster.rpc!(node_a, Group, :disconnect, [name, "game"])

      # All 3 :left events should arrive in one batch
      assert_receive {:got_batch, events}, 5000
      left_events = Enum.filter(events, &(&1.type == :left))
      assert length(left_events) == 3
      assert Enum.all?(left_events, &(&1.reason == :cluster_disconnect))
      assert Enum.map(left_events, & &1.key) |> Enum.sort() == Enum.sort(keys)
    end

    test "partition heal (cluster_state merge) batches new entries into one message" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      num_shards = 4
      name = :"batch_heal_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: num_shards]

      start_group_on_peers(peers, opts)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :nodes, [name]) == [node_b]
      end)

      # Partition
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())
      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # Find 3 keys that hash to the same shard
      keys =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(fn i -> "heal_batch/key_#{i}" end)
        |> Stream.filter(fn key -> :erlang.phash2({nil, key}, num_shards) == 0 end)
        |> Enum.take(3)

      # Register all 3 on A during partition
      for key <- keys do
        TestCluster.spawn_register(node_a, name, key, %{k: key})
      end

      # Set up batch monitor on B during partition (before data arrives)
      forwarder = TestCluster.spawn_batch_forwarder(node_b, name, :all, self())
      assert_receive {:monitor_ready, ^forwarder}, 1000

      # Reconnect — cluster_state exchange merges all 3 entries in one handler turn
      TestCluster.reconnect_nodes(node_a, node_b)

      # All 3 :registered events should arrive in a single batch
      assert_receive {:got_batch, events}, 5000
      reg_events = Enum.filter(events, &(&1.type == :registered))
      assert length(reg_events) == 3
      assert Enum.map(reg_events, & &1.key) |> Enum.sort() == Enum.sort(keys)
    end

    test "Group.connect on existing cluster batches incoming data" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      num_shards = 4
      name = :"batch_connect_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: num_shards]

      start_group_on_peers(peers, opts)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :nodes, [name]) == [node_b]
      end)

      # B connects to "game" and joins 3 same-shard keys
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      keys =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(fn i -> "connect_batch/key_#{i}" end)
        |> Stream.filter(fn key -> :erlang.phash2({"game", key}, num_shards) == 0 end)
        |> Enum.take(3)

      for key <- keys do
        TestCluster.spawn_join(node_b, name, key, %{k: key}, cluster: "game")
      end

      # Set up batch monitor on A for "game" BEFORE connecting
      forwarder =
        TestCluster.spawn_batch_forwarder(node_a, name, :all, self(), cluster: "game")

      assert_receive {:monitor_ready, ^forwarder}, 1000

      # A connects — receives cluster_state with all 3 entries from B
      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])

      # All 3 :joined events should arrive in a single batch
      assert_receive {:got_batch, events}, 5000
      join_events = Enum.filter(events, &(&1.type == :joined))
      assert length(join_events) == 3
      assert Enum.map(join_events, & &1.key) |> Enum.sort() == Enum.sort(keys)
    end
  end

  describe "prefix members across nodes" do
    test "prefix query excludes registrations from all nodes" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"prefix_dist_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 4]

      start_group_on_peers(peers, opts)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :nodes, [name]) == [node_b]
      end)

      # Each node registers under a different "regional_index/<region>" key
      TestCluster.spawn_register(node_a, name, "regional_index/iad", %{region: "iad"})
      TestCluster.spawn_register(node_b, name, "regional_index/ord", %{region: "ord"})

      # Wait for replication — exact lookups work on both nodes
      for node <- [node_a, node_b], key <- ["regional_index/iad", "regional_index/ord"] do
        TestCluster.assert_eventually(fn ->
          TestCluster.rpc!(node, Group, :lookup, [name, key]) != nil
        end)
      end

      # Prefix query from node_a should ignore registrations
      members_a = TestCluster.rpc!(node_a, Group, :members, [name, "regional_index/"])
      assert members_a == []

      # Prefix query from node_b should also ignore registrations
      members_b = TestCluster.rpc!(node_b, Group, :members, [name, "regional_index/"])
      assert members_b == []
    end

    test "prefix query finds PG joins from all nodes" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"prefix_pg_dist_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 4]

      start_group_on_peers(peers, opts)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :nodes, [name]) == [node_b]
      end)

      # Each node joins under a different "room/<id>" key
      pid_a = TestCluster.spawn_join(node_a, name, "room/1", %{node: :a})
      pid_b = TestCluster.spawn_join(node_b, name, "room/2", %{node: :b})

      # Wait for replication
      for node <- [node_a, node_b], key <- ["room/1", "room/2"] do
        TestCluster.assert_eventually(fn ->
          TestCluster.rpc!(node, Group, :members, [name, key]) != []
        end)
      end

      # Prefix query from node_a should find both joins
      members_a = TestCluster.rpc!(node_a, Group, :members, [name, "room/"])
      assert length(members_a) == 2
      pids = Enum.map(members_a, &elem(&1, 0)) |> Enum.sort()
      assert Enum.sort([pid_a, pid_b]) == pids
    end

    test "prefix query finds only joins in mixed registration/join data" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"prefix_mixed_dist_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 4]

      start_group_on_peers(peers, opts)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :nodes, [name]) == [node_b]
      end)

      # Node A registers, node B joins — different keys under same prefix
      TestCluster.spawn_register(node_a, name, "svc/alpha", %{type: :reg})
      TestCluster.spawn_join(node_b, name, "svc/beta", %{type: :pg})

      # Wait for replication
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :lookup, [name, "svc/alpha"]) != nil
      end)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :members, [name, "svc/beta"]) != []
      end)

      # Prefix query should find only the join
      members = TestCluster.rpc!(node_a, Group, :members, [name, "svc/"])
      assert [{_, %{type: :pg}}] = members
    end
  end

  # Helpers for event assertion tests

  defp flush_events do
    receive do
      {:got_event, %Group.Event{}} -> flush_events()
    after
      100 -> :ok
    end
  end

  defp assert_received_event(type, key, pid, reason) do
    assert_received_event_loop(type, key, pid, reason, 5000)
  end

  defp assert_received_event(type, key, pid) do
    assert_received_event_loop(type, key, pid, nil, 5000)
  end

  defp assert_received_event_loop(type, key, pid, reason, timeout) do
    receive do
      {:got_event, %Group.Event{type: ^type, key: ^key, pid: ^pid, reason: ^reason}} ->
        :ok

      {:got_event, %Group.Event{type: ^type, key: ^key, pid: ^pid}} when reason == nil ->
        :ok

      {:got_event, %Group.Event{}} ->
        # Not the event we're looking for, keep draining
        assert_received_event_loop(type, key, pid, reason, timeout)
    after
      timeout ->
        flunk(
          "Expected #{type} event for key=#{inspect(key)} pid=#{inspect(pid)}" <>
            if(reason, do: " reason=#{inspect(reason)}", else: "") <>
            " but didn't receive it within #{timeout}ms"
        )
    end
  end
end
