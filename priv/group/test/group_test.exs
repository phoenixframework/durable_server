defmodule GroupTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  setup do
    name = :"test_group_#{System.unique_integer([:positive])}"
    start_supervised!({Group, name: name, shards: 4, log: false})
    {:ok, name: name}
  end

  describe "join/3 and leave/2" do
    test "joined process appears in members/2", %{name: name} do
      key = "chat/room/#{System.unique_integer([:positive])}"
      meta = %{role: :listener}

      :ok = Group.join(name, key, meta)

      members = Group.members(name, key)
      assert length(members) == 1
      my_pid = self()
      assert {^my_pid, ^meta} = hd(members)
    end

    test "joined process triggers :joined event to subscribers", %{name: name} do
      key = "chat/room/#{System.unique_integer([:positive])}"

      # Subscribe first
      :ok = Group.monitor(name, key)

      # Spawn a process to join
      test_pid = self()

      spawn_pid =
        spawn(fn ->
          :ok = Group.join(name, key, %{role: :worker})
          send(test_pid, :joined)
          # Keep alive to avoid immediate :left event
          Process.sleep(:infinity)
        end)

      receive do
        :joined -> :ok
      after
        1000 -> flunk("Spawned process didn't join in time")
      end

      # Should receive :joined event
      assert_receive {:group, [%Group.Event{type: :joined} = event], _}, 1000
      assert event.supervisor == name
      assert event.key == key
      assert event.pid == spawn_pid
      assert event.meta == %{role: :worker}
      assert event.previous_meta == nil
    end

    test "leave/2 removes from members and triggers :left event", %{name: name} do
      key = "chat/room/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)
      :ok = Group.join(name, key, %{role: :listener})

      assert_receive {:group, [%Group.Event{type: :joined}], _}, 1000

      assert length(Group.members(name, key)) == 1

      :ok = Group.leave(name, key)

      # Should receive :left event
      assert_receive {:group, [%Group.Event{type: :left} = event], _}, 1000
      assert event.key == key
      assert event.pid == self()
      assert event.reason != nil

      assert Group.members(name, key) == []
    end

    test "process death triggers automatic :left event", %{name: name} do
      key = "chat/room/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)

      test_pid = self()

      pid =
        spawn(fn ->
          :ok = Group.join(name, key, %{role: :temp})
          send(test_pid, :ready)

          receive do
            :exit -> :ok
          end
        end)

      receive do
        :ready -> :ok
      after
        1000 -> flunk("Process didn't join in time")
      end

      assert_receive {:group, [%Group.Event{type: :joined, pid: ^pid}], _}, 1000
      assert length(Group.members(name, key)) == 1

      # Kill the process
      Process.exit(pid, :kill)

      # Should receive :left event
      assert_receive {:group, [%Group.Event{type: :left} = event], _}, 1000
      assert event.pid == pid
      assert event.key == key

      # Should be removed from members
      assert Group.members(name, key) == []
    end

    test "leave/2 returns error when not a member", %{name: name} do
      key = "nonexistent/key"
      assert {:error, :not_in_group} = Group.leave(name, key)
    end

    test "re-join updates metadata in place", %{name: name} do
      key = "rejoin/test/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)

      # First join succeeds
      assert :ok = Group.join(name, key, %{v: 1})

      assert_receive {:group, [%Group.Event{type: :joined, previous_meta: nil, meta: %{v: 1}}],
                      _},
                     1000

      # Second join also succeeds and updates metadata
      assert :ok = Group.join(name, key, %{v: 2})

      # Should receive :joined event with previous_meta
      assert_receive {:group, [%Group.Event{type: :joined} = event], _}, 1000
      assert event.meta == %{v: 2}
      assert event.previous_meta == %{v: 1}

      # Metadata is updated
      [{_pid, %{v: 2}}] = Group.members(name, key)
    end
  end

  describe "register/unregister" do
    test "register makes process discoverable via lookup", %{name: name} do
      key = "user/#{System.unique_integer([:positive])}"

      :ok = Group.register(name, key, %{module: :test})

      {pid, meta} = Group.lookup(name, key)
      assert pid == self()
      assert meta == %{module: :test}
    end

    test "register triggers :registered event", %{name: name} do
      key = "user/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)
      :ok = Group.register(name, key, %{module: :test})

      assert_receive {:group, [%Group.Event{type: :registered} = event], _}, 1000
      assert event.key == key
      assert event.pid == self()
      assert event.meta == %{module: :test}
      assert event.previous_meta == nil
    end

    test "double register returns :taken", %{name: name} do
      key = "user/#{System.unique_integer([:positive])}"

      :ok = Group.register(name, key, %{module: :test})

      # Another process tries to register same key
      test_pid = self()

      spawn(fn ->
        result = Group.register(name, key, %{module: :other})
        send(test_pid, {:register_result, result})
      end)

      assert_receive {:register_result, {:error, :taken}}, 1000
    end

    test "re-register by same process updates meta", %{name: name} do
      key = "user/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)
      :ok = Group.register(name, key, %{v: 1})
      assert_receive {:group, [%Group.Event{type: :registered, previous_meta: nil}], _}, 1000

      :ok = Group.register(name, key, %{v: 2})

      assert_receive {:group,
                      [%Group.Event{type: :registered, meta: %{v: 2}, previous_meta: %{v: 1}}],
                      _},
                     1000

      {_pid, %{v: 2}} = Group.lookup(name, key)
    end

    test "unregister removes from lookup and fires event", %{name: name} do
      key = "user/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)
      :ok = Group.register(name, key, %{module: :test})
      assert_receive {:group, [%Group.Event{type: :registered}], _}, 1000

      :ok = Group.unregister(name, key)
      assert_receive {:group, [%Group.Event{type: :unregistered} = event], _}, 1000
      assert event.key == key
      assert event.reason == :unregister

      assert Group.lookup(name, key) == nil
    end

    test "process death auto-unregisters", %{name: name} do
      key = "user/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)

      test_pid = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, key, %{module: :test})
          send(test_pid, :ready)
          Process.sleep(:infinity)
        end)

      receive do
        :ready -> :ok
      after
        1000 -> flunk("Process didn't register in time")
      end

      assert_receive {:group, [%Group.Event{type: :registered, pid: ^pid}], _}, 1000
      assert Group.lookup(name, key) != nil

      Process.exit(pid, :kill)

      assert_receive {:group, [%Group.Event{type: :unregistered, pid: ^pid}], _}, 1000
      assert Group.lookup(name, key) == nil
    end
  end

  describe "members/2" do
    test "returns only joined processes", %{name: name} do
      key = "only/joined/#{System.unique_integer([:positive])}"

      :ok = Group.join(name, key, %{role: :standalone})

      members = Group.members(name, key)
      assert length(members) == 1
      my_pid = self()
      assert {^my_pid, %{role: :standalone}} = hd(members)
    end

    test "returns empty list for non-existent key", %{name: name} do
      assert Group.members(name, "nonexistent/key") == []
    end

    test "does not return registered processes", %{name: name} do
      key = "registered/#{System.unique_integer([:positive])}"

      :ok = Group.register(name, key, %{type: :server})

      assert Group.members(name, key) == []
    end

    test "returns only joined processes when both registered and joined entries exist", %{
      name: name
    } do
      key = "both/#{System.unique_integer([:positive])}"

      :ok = Group.register(name, key, %{type: :server})

      test_pid = self()

      joiner =
        spawn(fn ->
          :ok = Group.join(name, key, %{type: :client})
          send(test_pid, :joined)
          Process.sleep(:infinity)
        end)

      receive do
        :joined -> :ok
      after
        1000 -> flunk("Process didn't join")
      end

      members = Group.members(name, key)
      assert [{^joiner, %{type: :client}}] = members
    end
  end

  describe "prefix members" do
    test "returns joined processes matching prefix", %{name: name} do
      prefix = "room/#{System.unique_integer([:positive])}/"
      key1 = prefix <> "a"
      key2 = prefix <> "b"
      other_key = "other/key"

      :ok = Group.join(name, key1, %{id: 1})
      :ok = Group.join(name, key2, %{id: 2})
      :ok = Group.join(name, other_key, %{id: 3})

      members = Group.members(name, prefix)
      assert length(members) == 2
      metas = Enum.map(members, fn {_pid, meta} -> meta end) |> Enum.sort_by(& &1.id)
      assert metas == [%{id: 1}, %{id: 2}]
    end

    test "does not return registered processes matching prefix", %{name: name} do
      prefix = "user/#{System.unique_integer([:positive])}/"
      key1 = prefix <> "alice"
      key2 = prefix <> "bob"
      other_key = "other/reg"

      test_pid = self()

      for {key, meta} <- [
            {key1, %{name: "alice"}},
            {key2, %{name: "bob"}},
            {other_key, %{name: "other"}}
          ] do
        pid =
          spawn(fn ->
            :ok = Group.register(name, key, meta)
            send(test_pid, {:registered, self()})
            Process.sleep(:infinity)
          end)

        receive do
          {:registered, ^pid} -> pid
        after
          1000 -> flunk("register timed out")
        end

        pid
      end

      assert Group.members(name, prefix) == []
    end

    test "returns only joined processes matching prefix", %{name: name} do
      prefix = "mixed/#{System.unique_integer([:positive])}/"
      reg_key = prefix <> "server"
      join_key = prefix <> "client"

      test_pid = self()

      reg_pid =
        spawn(fn ->
          :ok = Group.register(name, reg_key, %{type: :server})
          send(test_pid, {:registered, self()})
          Process.sleep(:infinity)
        end)

      receive do
        {:registered, ^reg_pid} -> :ok
      after
        1000 -> flunk("register timed out")
      end

      :ok = Group.join(name, join_key, %{type: :client})

      members = Group.members(name, prefix)
      assert [{_, %{type: :client}}] = members
    end

    test "returns empty list for prefix with no matches", %{name: name} do
      assert Group.members(name, "nonexistent/prefix/") == []
    end

    test "exact key lookup still works (no trailing slash)", %{name: name} do
      key = "exact/#{System.unique_integer([:positive])}"
      :ok = Group.join(name, key, %{exact: true})

      members = Group.members(name, key)
      assert length(members) == 1
    end

    test "prefix works with named clusters", %{name: name} do
      cluster = "game_#{System.unique_integer([:positive])}"
      prefix = "room/#{System.unique_integer([:positive])}/"

      :ok = Group.connect(name, cluster)

      # Join in named cluster
      :ok = Group.join(name, prefix <> "a", %{cluster: :named}, cluster: cluster)
      # Join in default cluster (same prefix)
      :ok = Group.join(name, prefix <> "b", %{cluster: :default})

      named_members = Group.members(name, prefix, cluster: cluster)
      assert length(named_members) == 1
      assert [{_, %{cluster: :named}}] = named_members

      default_members = Group.members(name, prefix)
      assert length(default_members) == 1
      assert [{_, %{cluster: :default}}] = default_members
    end

    test "prefix finds keys across different shards", %{name: name} do
      # Use enough keys that they're likely to hash to different shards (4 shards in test)
      prefix = "shard_spread/#{System.unique_integer([:positive])}/"

      for i <- 1..20 do
        :ok = Group.join(name, prefix <> "item_#{i}", %{i: i})
      end

      members = Group.members(name, prefix)
      assert length(members) == 20

      # Verify all items present
      found_ids = Enum.map(members, fn {_pid, meta} -> meta.i end) |> Enum.sort()
      assert found_ids == Enum.to_list(1..20)
    end

    test "register raises on key ending with /", %{name: name} do
      assert_raise ArgumentError, ~r/must not end with/, fn ->
        Group.register(name, "bad/key/", %{})
      end
    end

    test "join raises on key ending with /", %{name: name} do
      assert_raise ArgumentError, ~r/must not end with/, fn ->
        Group.join(name, "bad/key/", %{})
      end
    end

    test "unregister raises on key ending with /", %{name: name} do
      assert_raise ArgumentError, ~r/must not end with/, fn ->
        Group.unregister(name, "bad/key/")
      end
    end

    test "leave raises on key ending with /", %{name: name} do
      assert_raise ArgumentError, ~r/must not end with/, fn ->
        Group.leave(name, "bad/key/")
      end
    end
  end

  describe "self-events" do
    test "joining process receives its own :joined event if subscribed", %{name: name} do
      key = "self/events/#{System.unique_integer([:positive])}"

      # Subscribe first
      :ok = Group.monitor(name, key)

      # Then join
      :ok = Group.join(name, key, %{self: true})

      # Should receive our own :joined event
      assert_receive {:group, [%Group.Event{type: :joined} = event], _}, 1000
      assert event.pid == self()
      assert event.meta == %{self: true}
      assert event.previous_meta == nil
    end
  end

  describe "monitor/demonitor" do
    test "double subscribe is idempotent", %{name: name} do
      key = "user/test"

      assert :ok = Group.monitor(name, key)
      assert :ok = Group.monitor(name, key)

      # Spawn a process to join (use join, not start_child)
      test_pid = self()

      pid =
        spawn(fn ->
          :ok = Group.join(name, key, %{role: :worker})
          send(test_pid, :joined)
          Process.sleep(:infinity)
        end)

      receive do
        :joined -> :ok
      after
        1000 -> flunk("Process didn't join in time")
      end

      # Should only receive one event (not duplicated)
      assert_receive {:group, [%Group.Event{type: :joined, pid: ^pid}], _}, 1000
      refute_receive {:group, _, _}, 100
    end

    test "overlapping subscriptions still deliver one event", %{name: name} do
      scope = "sprite_channels/#{System.unique_integer([:positive])}"
      key = "#{scope}/state"

      :ok = Group.monitor(name, :all)
      :ok = Group.monitor(name, key)
      :ok = Group.monitor(name, "sprite_channels/")
      :ok = Group.monitor(name, "#{scope}/")

      :ok = Group.join(name, key, %{v: 1})

      assert_receive {:group, [%Group.Event{type: :joined, key: ^key} = event], _}, 1000
      assert event.pid == self()
      refute_receive {:group, _, _}, 100
    end

    test "demonitor stops events", %{name: name} do
      key = "user/"

      :ok = Group.monitor(name, key)

      test_pid = self()

      spawn(fn ->
        :ok = Group.join(name, "user/first", %{})
        send(test_pid, :first_joined)
        Process.sleep(:infinity)
      end)

      receive do
        :first_joined -> :ok
      after
        1000 -> flunk("First process didn't join in time")
      end

      assert_receive {:group, [%Group.Event{type: :joined, key: "user/first"}], _}, 1000

      # Unsubscribe
      :ok = Group.demonitor(name, key)

      spawn(fn ->
        :ok = Group.join(name, "user/second", %{})
        send(test_pid, :second_joined)
        Process.sleep(:infinity)
      end)

      receive do
        :second_joined -> :ok
      after
        1000 -> flunk("Second process didn't join in time")
      end

      # Should NOT receive the second event
      refute_receive {:group, _, _}, 200
    end
  end

  describe "named clusters" do
    test "connect/disconnect/connected? manage cluster lifecycle", %{name: name} do
      cluster = "game_servers"

      # Initially not connected
      refute Group.connected?(name, cluster)

      # Connect
      assert :ok = Group.connect(name, cluster)
      assert Group.connected?(name, cluster)

      # Disconnect
      assert :ok = Group.disconnect(name, cluster)
    end

    test "join/leave work with cluster: option", %{name: name} do
      cluster = "game_cluster"
      key = "room/#{System.unique_integer([:positive])}"

      # Connect to the cluster first
      :ok = Group.connect(name, cluster)

      # Join in the named cluster
      :ok = Group.join(name, key, %{role: :player}, cluster: cluster)

      # Should appear in named cluster members
      members = Group.members(name, key, cluster: cluster)
      assert length(members) == 1
      my_pid = self()
      assert [{^my_pid, %{role: :player}}] = members

      # Should NOT appear in default cluster members
      assert Group.members(name, key) == []

      # Leave the named cluster
      :ok = Group.leave(name, key, cluster: cluster)
      assert Group.members(name, key, cluster: cluster) == []
    end

    test "events in one cluster don't leak to another", %{name: name} do
      cluster1 = "cluster_a"
      cluster2 = "cluster_b"
      key = "shared/key/#{System.unique_integer([:positive])}"

      # Connect to both clusters
      :ok = Group.connect(name, cluster1)
      :ok = Group.connect(name, cluster2)

      # Subscribe to cluster1 only
      :ok = Group.monitor(name, :all, cluster: cluster1)

      # Spawn process to join cluster1
      test_pid = self()

      pid1 =
        spawn(fn ->
          :ok = Group.join(name, key, %{cluster: 1}, cluster: cluster1)
          send(test_pid, {:joined, 1})
          Process.sleep(:infinity)
        end)

      receive do
        {:joined, 1} -> :ok
      after
        1000 -> flunk("Process didn't join cluster1 in time")
      end

      # Should receive event from cluster1
      assert_receive {:group, [%Group.Event{type: :joined, pid: ^pid1, cluster: ^cluster1}], _},
                     1000

      # Spawn process to join cluster2
      _pid2 =
        spawn(fn ->
          :ok = Group.join(name, key, %{cluster: 2}, cluster: cluster2)
          send(test_pid, {:joined, 2})
          Process.sleep(:infinity)
        end)

      receive do
        {:joined, 2} -> :ok
      after
        1000 -> flunk("Process didn't join cluster2 in time")
      end

      # Should NOT receive event from cluster2
      refute_receive {:group, _, _}, 200

      # Now subscribe to cluster2 and verify we can receive events
      :ok = Group.monitor(name, :all, cluster: cluster2)

      pid3 =
        spawn(fn ->
          :ok = Group.join(name, key, %{cluster: 2, extra: true}, cluster: cluster2)
          send(test_pid, {:joined, 3})
          Process.sleep(:infinity)
        end)

      receive do
        {:joined, 3} -> :ok
      after
        1000 -> flunk("Process didn't join cluster2 in time")
      end

      assert_receive {:group, [%Group.Event{type: :joined, pid: ^pid3, cluster: ^cluster2}], _},
                     1000
    end

    test "members/3 returns only members from specified cluster", %{name: name} do
      cluster = "isolated_cluster"
      key = "room/#{System.unique_integer([:positive])}"

      :ok = Group.connect(name, cluster)

      # Join default cluster
      :ok = Group.join(name, key, %{location: :default})

      # Join named cluster (need different process since same pid can't join same key twice)
      test_pid = self()

      other_pid =
        spawn(fn ->
          :ok = Group.join(name, key, %{location: :named}, cluster: cluster)
          send(test_pid, :ready)
          Process.sleep(:infinity)
        end)

      receive do
        :ready -> :ok
      after
        1000 -> flunk("Process didn't join in time")
      end

      # Default cluster should only have our process
      default_members = Group.members(name, key)
      assert length(default_members) == 1
      my_pid = self()
      assert [{^my_pid, %{location: :default}}] = default_members

      # Named cluster should only have the spawned process
      named_members = Group.members(name, key, cluster: cluster)
      assert length(named_members) == 1
      assert [{^other_pid, %{location: :named}}] = named_members
    end

    test "default cluster works without cluster: option", %{name: name} do
      key = "default/test/#{System.unique_integer([:positive])}"

      # Subscribe without cluster option (default cluster)
      :ok = Group.monitor(name, key)

      # Join without cluster option (default cluster)
      :ok = Group.join(name, key, %{v: 1})

      # Should receive event with cluster: nil
      assert_receive {:group, [%Group.Event{type: :joined} = event], _}, 1000
      assert event.cluster == nil
      assert event.meta == %{v: 1}
      assert event.previous_meta == nil

      # Members without cluster option
      members = Group.members(name, key)
      assert length(members) == 1
    end

    test "dispatch works with cluster: option", %{name: name} do
      cluster = "broadcast_cluster"
      key = "broadcast/#{System.unique_integer([:positive])}"

      :ok = Group.connect(name, cluster)

      # Join the named cluster
      :ok = Group.join(name, key, %{}, cluster: cluster)

      # Broadcast to named cluster
      :ok = Group.dispatch(name, key, {:test_message, :from_cluster}, cluster: cluster)

      assert_receive {:test_message, :from_cluster}, 1000

      # Broadcast to default cluster (we're not there)
      :ok = Group.dispatch(name, key, {:test_message, :from_default})

      # Should NOT receive (we're not in default cluster for this key)
      refute_receive {:test_message, :from_default}, 200
    end

    test "dispatch_local only sends to local members", %{name: name} do
      key = "dispatch_local/#{System.unique_integer([:positive])}"

      # Join from self (local)
      :ok = Group.join(name, key, %{})

      :ok = Group.dispatch_local(name, key, {:local_msg, 1})
      assert_receive {:local_msg, 1}, 1000

      # Also works with cluster: option
      cluster = "dispatch_local_cluster"
      :ok = Group.connect(name, cluster)
      :ok = Group.join(name, key, %{}, cluster: cluster)

      :ok = Group.dispatch_local(name, key, {:local_msg, 2}, cluster: cluster)
      assert_receive {:local_msg, 2}, 1000

      # Default cluster dispatch_local should not deliver cluster message
      :ok = Group.dispatch_local(name, key, {:local_msg, 3})
      assert_receive {:local_msg, 3}, 1000
      refute_receive {:local_msg, _}, 200
    end

    test "monitor/demonitor work with cluster: option", %{name: name} do
      cluster = "sub_cluster"
      key = "sub/test/#{System.unique_integer([:positive])}"

      :ok = Group.connect(name, cluster)

      # Subscribe to named cluster
      :ok = Group.monitor(name, key, cluster: cluster)

      # Spawn and join
      test_pid = self()

      spawn(fn ->
        :ok = Group.join(name, key, %{}, cluster: cluster)
        send(test_pid, :joined)
        Process.sleep(:infinity)
      end)

      receive do
        :joined -> :ok
      after
        1000 -> flunk("Process didn't join in time")
      end

      assert_receive {:group, [%Group.Event{type: :joined, cluster: ^cluster}], _}, 1000

      # Unsubscribe from named cluster
      :ok = Group.demonitor(name, key, cluster: cluster)

      # Spawn another process to join
      spawn(fn ->
        :ok = Group.join(name, key, %{second: true}, cluster: cluster)
        send(test_pid, :joined2)
        Process.sleep(:infinity)
      end)

      receive do
        :joined2 -> :ok
      after
        1000 -> flunk("Second process didn't join in time")
      end

      # Should NOT receive event after unsubscribe
      refute_receive {:group, _, _}, 200
    end
  end

  describe "ETS table consistency" do
    test "tables are consistent after register + unregister", %{name: name} do
      :ok = Group.register(name, "ets/reg1", %{v: 1})
      :ok = Group.register(name, "ets/reg2", %{v: 2})
      :ok = Group.unregister(name, "ets/reg1")

      assert Group.TestCluster.assert_ets_consistent(name) == :ok
    end

    test "tables are consistent after join + leave", %{name: name} do
      :ok = Group.join(name, "ets/grp1", %{v: 1})
      :ok = Group.join(name, "ets/grp2", %{v: 2})
      :ok = Group.leave(name, "ets/grp1")

      assert Group.TestCluster.assert_ets_consistent(name) == :ok
    end

    test "tables are consistent after process death cleans up", %{name: name} do
      test_pid = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, "ets/death_reg", %{})
          :ok = Group.join(name, "ets/death_grp", %{})
          send(test_pid, :ready)
          Process.sleep(:infinity)
        end)

      receive do
        :ready -> :ok
      after
        1000 -> flunk("timeout")
      end

      # Verify entries exist
      assert Group.lookup(name, "ets/death_reg") != nil
      assert length(Group.members(name, "ets/death_grp")) == 1

      # Kill and wait for cleanup
      Process.exit(pid, :kill)
      Process.sleep(100)

      assert Group.lookup(name, "ets/death_reg") == nil
      assert Group.members(name, "ets/death_grp") == []

      assert Group.TestCluster.assert_ets_consistent(name) == :ok
    end

    test "tables are empty after all processes die", %{name: name} do
      test_pid = self()

      pids =
        for i <- 1..10 do
          spawn(fn ->
            :ok = Group.register(name, "ets/clean_reg_#{i}", %{i: i})
            :ok = Group.join(name, "ets/clean_grp", %{i: i})
            send(test_pid, {:ready, self()})
            Process.sleep(:infinity)
          end)
        end

      for pid <- pids do
        receive do
          {:ready, ^pid} -> :ok
        after
          1000 -> flunk("timeout")
        end
      end

      # Kill all
      Enum.each(pids, &Process.exit(&1, :kill))
      Process.sleep(200)

      # Tables should be consistent and empty
      assert Group.TestCluster.assert_ets_consistent(name) == :ok

      num_shards = Group.get_config(name).num_shards

      total_reg =
        Enum.sum(
          for s <- 0..(num_shards - 1) do
            :ets.info(Group.Replica.Data.reg_by_key_table(name, s), :size)
          end
        )

      total_reg_pid =
        Enum.sum(
          for s <- 0..(num_shards - 1) do
            :ets.info(Group.Replica.Data.reg_by_pid_table(name, s), :size)
          end
        )

      total_pg =
        Enum.sum(
          for s <- 0..(num_shards - 1) do
            :ets.info(Group.Replica.Data.pg_by_key_table(name, s), :size)
          end
        )

      total_pg_pid =
        Enum.sum(
          for s <- 0..(num_shards - 1) do
            :ets.info(Group.Replica.Data.pg_by_pid_table(name, s), :size)
          end
        )

      assert total_reg == 0, "reg_by_key has #{total_reg} orphaned entries"
      assert total_reg_pid == 0, "reg_by_pid has #{total_reg_pid} orphaned entries"
      assert total_pg == 0, "pg_by_key has #{total_pg} orphaned entries"
      assert total_pg_pid == 0, "pg_by_pid has #{total_pg_pid} orphaned entries"
    end

    test "tables are consistent after cluster disconnect", %{name: name} do
      cluster = "ets_cleanup_cluster"
      :ok = Group.connect(name, cluster)

      test_pid = self()

      spawn(fn ->
        :ok = Group.register(name, "ets/cluster_reg", %{}, cluster: cluster)
        :ok = Group.join(name, "ets/cluster_grp", %{}, cluster: cluster)
        send(test_pid, :ready)
        Process.sleep(:infinity)
      end)

      receive do
        :ready -> :ok
      after
        1000 -> flunk("timeout")
      end

      assert Group.lookup(name, "ets/cluster_reg", cluster: cluster) != nil

      :ok = Group.disconnect(name, cluster)

      # Cluster entries should be purged, tables consistent
      assert Group.TestCluster.assert_ets_consistent(name) == :ok
    end
  end

  describe "local_registry_count/1" do
    test "counts registered processes", %{name: name} do
      assert Group.local_registry_count(name) == 0

      :ok = Group.register(name, "key1", %{})
      assert Group.local_registry_count(name) == 1

      test_pid = self()

      spawn(fn ->
        :ok = Group.register(name, "key2", %{})
        send(test_pid, :registered)
        Process.sleep(:infinity)
      end)

      receive do
        :registered -> :ok
      after
        1000 -> flunk("didn't register")
      end

      assert Group.local_registry_count(name) == 2
    end
  end

  describe "local_member_count/2" do
    test "counts local group members", %{name: name} do
      group = "my_group"
      assert Group.local_member_count(name, group) == 0

      :ok = Group.join(name, group, %{})
      assert Group.local_member_count(name, group) == 1

      test_pid = self()

      spawn(fn ->
        :ok = Group.join(name, group, %{})
        send(test_pid, :joined)
        Process.sleep(:infinity)
      end)

      receive do
        :joined -> :ok
      after
        1000 -> flunk("didn't join")
      end

      assert Group.local_member_count(name, group) == 2
    end
  end

  describe "concurrent operations" do
    test "concurrent join/leave on same key doesn't produce duplicates", %{name: name} do
      key = "concurrent/#{System.unique_integer([:positive])}"
      test_pid = self()

      pids =
        for _i <- 1..10 do
          spawn(fn ->
            :ok = Group.join(name, key, %{})
            send(test_pid, {:joined, self()})
            Process.sleep(:infinity)
          end)
        end

      for _ <- 1..10 do
        receive do
          {:joined, _} -> :ok
        after
          2000 -> flunk("timeout waiting for joins")
        end
      end

      members = Group.members(name, key)
      member_pids = Enum.map(members, fn {pid, _} -> pid end)
      assert length(member_pids) == length(Enum.uniq(member_pids))
      assert length(member_pids) == 10

      # Kill a few and verify cleanup
      Enum.take(pids, 3)
      |> Enum.each(&Process.exit(&1, :kill))

      Process.sleep(100)

      members = Group.members(name, key)
      assert length(members) == 7
    end

    test "concurrent register attempts on same key", %{name: name} do
      key = "race/#{System.unique_integer([:positive])}"
      test_pid = self()

      for _i <- 1..5 do
        spawn(fn ->
          result = Group.register(name, key, %{pid: self()})
          send(test_pid, {:result, self(), result})
          Process.sleep(:infinity)
        end)
      end

      results =
        for _ <- 1..5 do
          receive do
            {:result, pid, result} -> {pid, result}
          after
            2000 -> flunk("timeout")
          end
        end

      ok_results = Enum.filter(results, fn {_, r} -> r == :ok end)
      error_results = Enum.filter(results, fn {_, r} -> r == {:error, :taken} end)

      assert length(ok_results) == 1
      assert length(error_results) == 4
    end
  end

  describe "event batching" do
    test "process death batches :unregistered and :left into one message", %{name: name} do
      key = "batch/reg_and_join/#{System.unique_integer([:positive])}"
      :ok = Group.monitor(name, :all)

      test_pid = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, key, %{r: 1})
          :ok = Group.join(name, key, %{j: 1})
          send(test_pid, :ready)
          Process.sleep(:infinity)
        end)

      assert_receive :ready, 1000

      # Drain the individual :registered and :joined events
      assert_receive {:group, [%Group.Event{type: :registered}], _}, 1000
      assert_receive {:group, [%Group.Event{type: :joined}], _}, 1000

      # Kill the process — DOWN handler should batch both cleanup events
      Process.exit(pid, :kill)

      assert_receive {:group, events, _}, 1000
      types = events |> Enum.map(& &1.type) |> Enum.sort()
      assert types == [:left, :unregistered]
      assert Enum.all?(events, &(&1.key == key))
      assert Enum.all?(events, &(&1.pid == pid))
    end

    test "process death batches multiple :left events for same-shard keys", %{name: name} do
      num_shards = Group.get_config(name).num_shards

      # Find 3 keys that hash to the same shard
      keys =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(fn i -> "batch/multi_#{i}" end)
        |> Stream.filter(fn key -> :erlang.phash2({nil, key}, num_shards) == 0 end)
        |> Enum.take(3)

      :ok = Group.monitor(name, :all)
      test_pid = self()

      pid =
        spawn(fn ->
          for key <- keys, do: :ok = Group.join(name, key, %{k: key})
          send(test_pid, :ready)
          Process.sleep(:infinity)
        end)

      assert_receive :ready, 1000

      # Drain individual :joined events
      for _key <- keys do
        assert_receive {:group, [%Group.Event{type: :joined}], _}, 1000
      end

      # Kill — all 3 :left events should arrive in one batch
      Process.exit(pid, :kill)

      assert_receive {:group, events, _}, 1000
      assert length(events) == 3
      assert Enum.all?(events, &(&1.type == :left))
      assert Enum.map(events, & &1.key) |> Enum.sort() == Enum.sort(keys)
    end

    test "single operations send single-event messages, not empty batches", %{name: name} do
      key = "batch/single/#{System.unique_integer([:positive])}"
      :ok = Group.monitor(name, :all)

      :ok = Group.register(name, key, %{})

      assert_receive {:group, events, _}, 1000
      assert [%Group.Event{type: :registered}] = events

      :ok = Group.unregister(name, key)

      assert_receive {:group, events, _}, 1000
      assert [%Group.Event{type: :unregistered}] = events
    end
  end
end
