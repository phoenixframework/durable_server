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

      Process.sleep(200)

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

      Process.sleep(200)

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
        node_a, name, "user/1", %{type: :reg}, "room/1", %{type: :pg}
      )

      Process.sleep(200)

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
        node_b, name, "user/1", %{node: :b}, "room/1", %{node: :b}
      )

      Process.sleep(200)

      # Verify data on node A
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, Group, :lookup, [name, "user/1"]) != nil
      end)

      # Stop node B
      :peer.stop(peer_b_pid)

      # Node A should clean up B's entries
      TestCluster.assert_eventually(fn ->
        lookup = TestCluster.rpc!(node_a, Group, :lookup, [name, "user/1"])
        members = TestCluster.rpc!(node_a, Group, :members, [name, "room/1"])
        lookup == nil and members == []
      end, timeout: 5000)

      # Clean up remaining peer
      [{peer_a_pid, _}] = Enum.filter(peers, fn {_, n} -> n == node_a end)
      on_exit(fn -> :peer.stop(peer_a_pid) end)
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

      # Set up monitor on node A that forwards events to us
      TestCluster.spawn_monitor_forwarder(node_a, name, "user/", self())

      # Wait for monitor setup and peer discovery to complete
      Process.sleep(300)

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

      Process.sleep(300)

      # A should see both (connected to both clusters)
      TestCluster.assert_eventually(fn ->
        game_members = TestCluster.rpc!(node_a, Group, :members, [name, "room/1", [cluster: "game"]])
        chat_members = TestCluster.rpc!(node_a, Group, :members, [name, "room/1", [cluster: "chat"]])
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
    test "same key registered on both sides during partition" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_conflict_#{System.unique_integer([:positive])}"

      opts = [
        name: name,
        shards: 2,
        resolve_registry_conflict:
          {Group.TestConflictResolver, :resolve, []}
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

      # Disconnect A from B
      TestCluster.disconnect_nodes(node_a, node_b)
      Process.sleep(500)

      # Register same key on both sides during partition
      _pid_a = TestCluster.spawn_register(node_a, name, "user/conflict", %{side: :a})
      Process.sleep(50)
      _pid_b = TestCluster.spawn_register(node_b, name, "user/conflict", %{side: :b})

      Process.sleep(200)

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
      Process.sleep(300)

      # Disconnect C from A and B
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      Process.sleep(500)

      # While partitioned: register keys on A, join groups on C
      TestCluster.spawn_register(node_a, name, "user/from_a", %{origin: :a})
      TestCluster.spawn_join(node_c, name, "room/from_c", %{origin: :c})

      Process.sleep(200)

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
      Process.sleep(200)

      # Spawn, register, and immediately kill processes several times
      for i <- 1..5 do
        TestCluster.spawn_register_then_kill(node_a, name, "user/ephemeral_#{i}", %{i: i})
      end

      # Wait for replication and cleanup
      Process.sleep(500)

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
      Process.sleep(200)

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

      # Rapidly disconnect/reconnect B 3 times
      for _i <- 1..3 do
        TestCluster.disconnect_nodes(node_a, node_b)
        Process.sleep(300)
        TestCluster.reconnect_nodes(node_a, node_b)
        Process.sleep(500)
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

      Process.sleep(300)

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
      Process.sleep(200)

      # Find keys that hash to different shards
      {reg_key, join_key} = TestCluster.keys_for_different_shards(num_shards)

      # Verify they actually hash to different shards
      shard_reg = :erlang.phash2({nil, reg_key}, num_shards)
      shard_join = :erlang.phash2({nil, join_key}, num_shards)
      assert shard_reg != shard_join

      # Spawn one process that registers under reg_key and joins join_key
      pid = TestCluster.spawn_register_and_join_keys(
        node_a, name, reg_key, %{type: :reg}, join_key, %{type: :pg}
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

      # Set up monitor on B that forwards events to us
      TestCluster.spawn_monitor_forwarder(node_b, name, "user/", self())

      # Wait for monitor setup and peer discovery
      Process.sleep(300)

      # On A: register with meta v:1, re-register with meta v:2, then unregister
      TestCluster.spawn_register_update_unregister(
        node_a, name, "user/ordered", %{v: 1}, %{v: 2}
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

      assert_receive {:got_event,
                      %Group.Event{type: :unregistered, key: "user/ordered"}},
                     5000
    end
  end

  describe "cluster disconnect with active members" do
    test "remote nodes purge entries when node disconnects from cluster" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"dist_cluster_disc_#{System.unique_integer([:positive])}"
      opts = [name: name, shards: 2]

      start_group_on_peers(peers, opts)

      # Connect A and B to "game" cluster
      TestCluster.rpc!(node_a, Group, :connect, [name, "game"])
      TestCluster.rpc!(node_b, Group, :connect, [name, "game"])

      # Wait for cluster connectivity to be established (ack exchange)
      TestCluster.assert_eventually(fn ->
        nodes_a = TestCluster.rpc!(node_a, Group, :nodes, [name, "game"])
        nodes_b = TestCluster.rpc!(node_b, Group, :nodes, [name, "game"])
        length(nodes_a) >= 1 and length(nodes_b) >= 1
      end)

      # Join "room/1" on A in "game" cluster
      TestCluster.spawn_join(node_a, name, "room/1", %{player: :a}, cluster: "game")

      # Verify B sees the member
      TestCluster.assert_eventually(
        fn ->
          members = TestCluster.rpc!(node_b, Group, :members, [name, "room/1", [cluster: "game"]])
          length(members) == 1
        end,
        timeout: 5000
      )

      # A disconnects from "game" cluster
      TestCluster.rpc!(node_a, Group, :disconnect, [name, "game"])

      # B should purge A's entries for "game" cluster
      TestCluster.assert_eventually(
        fn ->
          members = TestCluster.rpc!(node_b, Group, :members, [name, "room/1", [cluster: "game"]])
          members == []
        end,
        timeout: 5000
      )
    end
  end

  describe "named cluster late joiner" do
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
          lookup = TestCluster.rpc!(node_a, Group, :lookup, [name, "game_user/1", [cluster: "game"]])
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
          lookup = TestCluster.rpc!(node_c, Group, :lookup, [name, "game_user/1", [cluster: "game"]])
          length(members) == 1 and lookup != nil
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

      Process.sleep(200)

      # Stop node A
      :peer.stop(peer_a_pid)
      Process.sleep(500)

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
end
