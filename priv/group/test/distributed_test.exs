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
end
