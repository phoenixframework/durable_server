defmodule GroupTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  setup do
    name = :"test_group_#{System.unique_integer([:positive])}"
    start_supervised!({Group, name: name, shards: 4})
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
      assert_receive %Group.Event{type: :joined} = event, 1000
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

      assert_receive %Group.Event{type: :joined}, 1000

      assert length(Group.members(name, key)) == 1

      :ok = Group.leave(name, key)

      # Should receive :left event
      assert_receive %Group.Event{type: :left} = event, 1000
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

      assert_receive %Group.Event{type: :joined, pid: ^pid}, 1000
      assert length(Group.members(name, key)) == 1

      # Kill the process
      Process.exit(pid, :kill)

      # Should receive :left event
      assert_receive %Group.Event{type: :left} = event, 1000
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
      assert_receive %Group.Event{type: :joined, previous_meta: nil, meta: %{v: 1}}, 1000

      # Second join also succeeds and updates metadata
      assert :ok = Group.join(name, key, %{v: 2})

      # Should receive :joined event with previous_meta
      assert_receive %Group.Event{type: :joined} = event, 1000
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

      assert_receive %Group.Event{type: :registered} = event, 1000
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
      assert_receive %Group.Event{type: :registered, previous_meta: nil}, 1000

      :ok = Group.register(name, key, %{v: 2})
      assert_receive %Group.Event{type: :registered, meta: %{v: 2}, previous_meta: %{v: 1}}, 1000

      {_pid, %{v: 2}} = Group.lookup(name, key)
    end

    test "unregister removes from lookup and fires event", %{name: name} do
      key = "user/#{System.unique_integer([:positive])}"

      :ok = Group.monitor(name, key)
      :ok = Group.register(name, key, %{module: :test})
      assert_receive %Group.Event{type: :registered}, 1000

      :ok = Group.unregister(name, key)
      assert_receive %Group.Event{type: :unregistered} = event, 1000
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

      assert_receive %Group.Event{type: :registered, pid: ^pid}, 1000
      assert Group.lookup(name, key) != nil

      Process.exit(pid, :kill)

      assert_receive %Group.Event{type: :unregistered, pid: ^pid}, 1000
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

    test "returns both registered and joined processes", %{name: name} do
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
      assert length(members) == 2
      assert {^test_pid, %{type: :server}} = Enum.find(members, fn {p, _} -> p == test_pid end)
      assert {^joiner, %{type: :client}} = Enum.find(members, fn {p, _} -> p == joiner end)
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
      assert_receive %Group.Event{type: :joined} = event, 1000
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
      assert_receive %Group.Event{type: :joined, pid: ^pid}, 1000
      refute_receive %Group.Event{type: :joined, pid: ^pid}, 100
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

      assert_receive %Group.Event{type: :joined, key: "user/first"}, 1000

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
      refute_receive %Group.Event{type: :joined, key: "user/second"}, 200
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
      assert_receive %Group.Event{type: :joined, pid: ^pid1, cluster: ^cluster1}, 1000

      # Spawn process to join cluster2
      pid2 =
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
      refute_receive %Group.Event{type: :joined, pid: ^pid2, cluster: ^cluster2}, 200

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

      assert_receive %Group.Event{type: :joined, pid: ^pid3, cluster: ^cluster2}, 1000
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
      assert_receive %Group.Event{type: :joined} = event, 1000
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

      assert_receive %Group.Event{type: :joined, cluster: ^cluster}, 1000

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
      refute_receive %Group.Event{type: :joined}, 200
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
end
