defmodule GroupTest do
  use ExUnit.Case, async: true
  import DurableServer.TestHelper

  @moduletag :capture_log

  defmodule TestServer do
    use DurableServer, vsn: 1

    def dump_state(state), do: state

    def load_state(_old_vsn, persisted_state) do
      persisted_state
      |> Enum.map(fn {k, v} -> {String.to_atom(k), v} end)
      |> Map.new()
    end

    def init(loaded_state) do
      {:ok, Map.put_new(loaded_state, :count, 0), auto_sync: false, meta: %{module: __MODULE__}}
    end

    def handle_call(:get_count, _from, %{count: count} = state) do
      {:reply, count, state}
    end

    def handle_call(:increment, _from, %{count: count} = state) do
      new_state = %{state | count: count + 1}
      {:reply, count + 1, new_state, :sync}
    end
  end

  setup do
    supervisor_name = :"test_cluster_#{DurableServer.UUID.uuid4()}"
    prefix = "test_cluster_#{DurableServer.UUID.uuid4()}/"

    _supervisor_pid =
      start_supervised!({
        DurableServer.Supervisor,
        name: supervisor_name, prefix: prefix, object_store: test_object_store_opts()
      })

    {:ok, supervisor_name: supervisor_name, prefix: prefix}
  end

  describe "monitor/2" do
    test "subscribes to exact key and receives :registered event", %{supervisor_name: sup} do
      key = "user/#{DurableServer.UUID.uuid4()}"

      # Subscribe before starting server
      :ok = Group.monitor(sup, key)

      # Start a DurableServer
      {:ok, {pid, _meta}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      # Should receive :registered event
      assert_receive %Group.Event{type: :registered} = event, 1000
      assert event.supervisor == sup
      assert event.key == key
      assert event.pid == pid
      assert event.cluster == nil
      assert event.previous_meta == nil
      assert is_map(event.meta)
    end

    test "subscribes to prefix pattern and receives events for matching keys", %{
      supervisor_name: sup
    } do
      key1 = "chat/room1"
      key2 = "chat/room2"
      key3 = "other/room"

      :ok = Group.monitor(sup, "chat/")

      # Start servers
      {:ok, {pid1, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key1}})
      {:ok, {pid2, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key2}})
      {:ok, {_pid3, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key3}})

      # Should receive events for chat/ keys
      assert_receive %Group.Event{type: :registered, key: ^key1, pid: ^pid1}, 1000
      assert_receive %Group.Event{type: :registered, key: ^key2, pid: ^pid2}, 1000

      # Should NOT receive event for other/ keys
      refute_receive %Group.Event{type: :registered, key: ^key3}, 100
    end

    test "subscribes to :all and receives all events", %{supervisor_name: sup} do
      key1 = "user/123"
      key2 = "chat/room"
      key3 = "anything/else"

      :ok = Group.monitor(sup, :all)

      {:ok, {pid1, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key1}})
      {:ok, {pid2, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key2}})
      {:ok, {pid3, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key3}})

      assert_receive %Group.Event{type: :registered, key: ^key1, pid: ^pid1}, 1000
      assert_receive %Group.Event{type: :registered, key: ^key2, pid: ^pid2}, 1000
      assert_receive %Group.Event{type: :registered, key: ^key3, pid: ^pid3}, 1000
    end

    test "receives :unregistered event when DurableServer stops", %{supervisor_name: sup} do
      key = "user/#{DurableServer.UUID.uuid4()}"

      :ok = Group.monitor(sup, key)

      {:ok, {pid, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})
      assert_receive %Group.Event{type: :registered}, 1000

      # Stop the server
      ref = Process.monitor(pid)
      :ok = DurableServer.Supervisor.terminate_child(sup, pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1000

      # Should receive :unregistered event
      assert_receive %Group.Event{type: :unregistered} = event, 1000
      assert event.supervisor == sup
      assert event.key == key
      assert event.pid == pid
      assert event.reason != nil
    end

    test "double subscribe is idempotent", %{supervisor_name: sup} do
      key = "user/test"

      assert :ok = Group.monitor(sup, key)
      assert :ok = Group.monitor(sup, key)

      {:ok, {pid, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      # Should only receive one event (not duplicated)
      assert_receive %Group.Event{type: :registered, pid: ^pid}, 1000
      refute_receive %Group.Event{type: :registered, pid: ^pid}, 100
    end
  end

  describe "demonitor/2" do
    test "stops receiving events after unsubscribe", %{supervisor_name: sup} do
      key1 = "user/first"
      key2 = "user/second"

      :ok = Group.monitor(sup, "user/")

      {:ok, {pid1, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key1}})
      assert_receive %Group.Event{type: :registered, key: ^key1, pid: ^pid1}, 1000

      # Unsubscribe
      :ok = Group.demonitor(sup, "user/")

      # Start another server
      {:ok, {_pid2, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key2}})

      # Should NOT receive the second event
      refute_receive %Group.Event{type: :registered, key: ^key2}, 200
    end

    test "unsubscribe from non-existent subscription is ok", %{supervisor_name: sup} do
      assert :ok = Group.demonitor(sup, "nonexistent/")
    end
  end

  describe "join/3 and leave/2" do
    test "joined process appears in members/2", %{supervisor_name: sup} do
      key = "chat/room/#{DurableServer.UUID.uuid4()}"
      meta = %{role: :listener}

      :ok = Group.join(sup, key, meta)

      members = Group.members(sup, key)
      assert length(members) == 1
      my_pid = self()
      assert {^my_pid, ^meta} = hd(members)
    end

    test "joined process triggers :joined event to subscribers", %{supervisor_name: sup} do
      key = "chat/room/#{DurableServer.UUID.uuid4()}"

      # Subscribe first
      :ok = Group.monitor(sup, key)

      # Spawn a process to join
      test_pid = self()

      spawn_pid =
        spawn(fn ->
          :ok = Group.join(sup, key, %{role: :worker})
          send(test_pid, :joined)
          # Keep alive to avoid immediate :left event
          Process.sleep(5000)
        end)

      receive do
        :joined -> :ok
      after
        1000 -> flunk("Spawned process didn't join in time")
      end

      # Should receive :joined event
      assert_receive %Group.Event{type: :joined} = event, 1000
      assert event.supervisor == sup
      assert event.key == key
      assert event.pid == spawn_pid
      assert event.meta == %{role: :worker}
      assert event.previous_meta == nil
    end

    test "leave/2 removes from members and triggers :left event", %{supervisor_name: sup} do
      key = "chat/room/#{DurableServer.UUID.uuid4()}"

      :ok = Group.monitor(sup, key)
      :ok = Group.join(sup, key, %{role: :listener})

      assert_receive %Group.Event{type: :joined}, 1000

      assert length(Group.members(sup, key)) == 1

      :ok = Group.leave(sup, key)

      # Should receive :left event
      assert_receive %Group.Event{type: :left} = event, 1000
      assert event.key == key
      assert event.pid == self()
      assert event.reason != nil

      assert Group.members(sup, key) == []
    end

    test "process death triggers automatic :left event", %{supervisor_name: sup} do
      key = "chat/room/#{DurableServer.UUID.uuid4()}"

      :ok = Group.monitor(sup, key)

      test_pid = self()

      pid =
        spawn(fn ->
          :ok = Group.join(sup, key, %{role: :temp})
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
      assert length(Group.members(sup, key)) == 1

      # Kill the process
      Process.exit(pid, :kill)

      # Should receive :left event
      assert_receive %Group.Event{type: :left} = event, 1000
      assert event.pid == pid
      assert event.key == key

      # Should be removed from members
      assert Group.members(sup, key) == []
    end

    test "leave/2 returns error when not a member", %{supervisor_name: sup} do
      key = "nonexistent/key"
      assert {:error, :not_in_group} = Group.leave(sup, key)
    end

    test "re-join updates metadata in place", %{supervisor_name: sup} do
      key = "rejoin/test/#{DurableServer.UUID.uuid4()}"

      :ok = Group.monitor(sup, key)

      # First join succeeds
      assert :ok = Group.join(sup, key, %{v: 1})
      assert_receive %Group.Event{type: :joined, previous_meta: nil, meta: %{v: 1}}, 1000

      # Second join also succeeds and updates metadata
      assert :ok = Group.join(sup, key, %{v: 2})

      # Should receive :joined event with previous_meta
      assert_receive %Group.Event{type: :joined} = event, 1000
      assert event.meta == %{v: 2}
      assert event.previous_meta == %{v: 1}

      # Metadata is updated
      [{_pid, %{v: 2}}] = Group.members(sup, key)
    end
  end

  describe "members/2" do
    test "returns DurableServer and joined processes together", %{supervisor_name: sup} do
      key = "combined/#{DurableServer.UUID.uuid4()}"

      # Start a DurableServer
      {:ok, {server_pid, _}} =
        DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      # Join as a listener
      listener_meta = %{role: :listener}
      :ok = Group.join(sup, key, listener_meta)

      members = Group.members(sup, key)
      assert length(members) == 2

      pids = Enum.map(members, fn {pid, _} -> pid end)
      assert server_pid in pids
      assert self() in pids

      # Find our joined entry
      {^listener_meta, _} =
        Enum.find(members, fn {pid, _meta} -> pid == self() end)
        |> then(fn {pid, meta} -> {meta, pid} end)
    end

    test "returns empty list for non-existent key", %{supervisor_name: sup} do
      assert Group.members(sup, "nonexistent/key") == []
    end

    test "returns only DurableServer when no joined processes", %{supervisor_name: sup} do
      key = "only/server/#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      members = Group.members(sup, key)
      assert length(members) == 1
      assert {^pid, _meta} = hd(members)
    end

    test "returns only joined processes when no DurableServer", %{supervisor_name: sup} do
      key = "only/joined/#{DurableServer.UUID.uuid4()}"

      :ok = Group.join(sup, key, %{role: :standalone})

      members = Group.members(sup, key)
      assert length(members) == 1
      my_pid = self()
      assert {^my_pid, %{role: :standalone}} = hd(members)
    end
  end

  describe "self-events" do
    test "joining process receives its own :joined event if subscribed", %{supervisor_name: sup} do
      key = "self/events/#{DurableServer.UUID.uuid4()}"

      # Subscribe first
      :ok = Group.monitor(sup, key)

      # Then join
      :ok = Group.join(sup, key, %{self: true})

      # Should receive our own :joined event
      assert_receive %Group.Event{type: :joined} = event, 1000
      assert event.pid == self()
      assert event.meta == %{self: true}
      assert event.previous_meta == nil
    end
  end

  describe "integration" do
    test "full lifecycle: subscribe, start server, join, stop, leave", %{supervisor_name: sup} do
      key = "integration/test/#{DurableServer.UUID.uuid4()}"

      # 1. Subscribe
      :ok = Group.monitor(sup, key)

      # 2. Start DurableServer
      {:ok, {server_pid, _}} =
        DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      assert_receive %Group.Event{type: :registered, pid: ^server_pid}, 1000

      # 3. Verify members shows DurableServer
      members = Group.members(sup, key)
      assert length(members) == 1

      # 4. Join as listener
      :ok = Group.join(sup, key, %{role: :listener})
      assert_receive %Group.Event{type: :joined, pid: self_pid}, 1000
      assert self_pid == self()

      # 5. Verify members shows both
      members = Group.members(sup, key)
      assert length(members) == 2

      # 6. Stop DurableServer
      ref = Process.monitor(server_pid)
      :ok = DurableServer.Supervisor.terminate_child(sup, server_pid)
      assert_receive {:DOWN, ^ref, :process, ^server_pid, _}, 1000
      assert_receive %Group.Event{type: :unregistered, pid: ^server_pid}, 1000

      # 7. Verify members shows only joined process
      members = Group.members(sup, key)
      assert length(members) == 1
      my_pid = self()
      assert {^my_pid, _} = hd(members)

      # 8. Leave
      :ok = Group.leave(sup, key)
      assert_receive %Group.Event{type: :left, pid: self_pid}, 1000
      assert self_pid == self()

      # 9. Verify empty members
      assert Group.members(sup, key) == []

      # 10. Unsubscribe
      :ok = Group.demonitor(sup, key)

      # 11. Start new server - should NOT receive event
      {:ok, {_new_pid, _}} =
        DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key <> "/new"}})

      refute_receive %Group.Event{type: :registered}, 200
    end
  end

  describe "multiple supervisors" do
    test "events from one supervisor don't leak to another supervisor's subscribers" do
      # Create a second supervisor
      supervisor_name_2 = :"test_cluster_2_#{DurableServer.UUID.uuid4()}"
      prefix_2 = "test_cluster_2_#{DurableServer.UUID.uuid4()}/"

      _supervisor_pid_2 =
        start_supervised!(
          {
            DurableServer.Supervisor,
            name: supervisor_name_2, prefix: prefix_2, object_store: test_object_store_opts()
          },
          id: :sup2
        )

      # Use the same key for both supervisors
      key = "shared/key/#{DurableServer.UUID.uuid4()}"

      # Subscribe to sup2 only
      :ok = Group.monitor(supervisor_name_2, :all)

      # Start a DurableServer on sup2
      {:ok, {pid2, _}} =
        DurableServer.Supervisor.start_child(supervisor_name_2, {TestServer, %{key: key}})

      # Should receive event from sup2
      assert_receive %Group.Event{type: :registered, supervisor: ^supervisor_name_2, pid: ^pid2}, 1000

      # Now unsubscribe from sup2 and subscribe to sup1 (from setup)
      :ok = Group.demonitor(supervisor_name_2, :all)
    end

    test "subscribers only receive events from their subscribed supervisor", %{
      supervisor_name: sup1
    } do
      # Create a second supervisor
      sup2 = :"test_cluster_isolated_#{DurableServer.UUID.uuid4()}"
      prefix_2 = "test_cluster_isolated_#{DurableServer.UUID.uuid4()}/"

      _supervisor_pid_2 =
        start_supervised!(
          {
            DurableServer.Supervisor,
            name: sup2, prefix: prefix_2, object_store: test_object_store_opts()
          },
          id: :isolated_sup2
        )

      key = "test/isolation/#{DurableServer.UUID.uuid4()}"

      # Subscribe to sup1 only
      :ok = Group.monitor(sup1, :all)

      # Start a DurableServer on sup1 - should receive event
      {:ok, {pid1, _}} = DurableServer.Supervisor.start_child(sup1, {TestServer, %{key: key}})
      assert_receive %Group.Event{type: :registered, supervisor: ^sup1, pid: ^pid1}, 1000

      # Start a DurableServer on sup2 - should NOT receive event
      {:ok, {pid2, _}} = DurableServer.Supervisor.start_child(sup2, {TestServer, %{key: key}})
      refute_receive %Group.Event{type: :registered, supervisor: ^sup2, pid: ^pid2}, 200

      # Now subscribe to sup2 as well
      :ok = Group.monitor(sup2, :all)

      # Join on sup2 - should receive event now
      :ok = Group.join(sup2, key, %{role: :test})
      assert_receive %Group.Event{type: :joined, supervisor: ^sup2}, 1000

      # Join on sup1 - should also receive (we're subscribed to both now)
      :ok = Group.join(sup1, key, %{role: :test})
      assert_receive %Group.Event{type: :joined, supervisor: ^sup1}, 1000

      # Members are isolated per supervisor
      sup1_members = Group.members(sup1, key)
      sup2_members = Group.members(sup2, key)

      # Each should have 2 members: the DurableServer + our joined process
      assert length(sup1_members) == 2
      assert length(sup2_members) == 2

      # But the pids are different (different DurableServers)
      sup1_pids = Enum.map(sup1_members, fn {pid, _} -> pid end)
      sup2_pids = Enum.map(sup2_members, fn {pid, _} -> pid end)

      assert pid1 in sup1_pids
      assert pid2 in sup2_pids
      refute pid1 in sup2_pids
      refute pid2 in sup1_pids
    end
  end

  describe "named clusters" do
    test "connect/disconnect/connected? manage cluster lifecycle", %{supervisor_name: sup} do
      cluster = :game_servers

      # Initially not connected
      refute Group.connected?(sup, cluster)

      # Connect
      assert :ok = Group.connect(sup, cluster)
      assert Group.connected?(sup, cluster)

      # Disconnect (note: this is a no-op in current implementation)
      assert :ok = Group.disconnect(sup, cluster)
    end

    test "join/leave work with cluster: option", %{supervisor_name: sup} do
      cluster = :game_cluster
      key = "room/#{DurableServer.UUID.uuid4()}"

      # Connect to the cluster first
      :ok = Group.connect(sup, cluster)

      # Join in the named cluster
      :ok = Group.join(sup, key, %{role: :player}, cluster: cluster)

      # Should appear in named cluster members
      members = Group.members(sup, key, cluster: cluster)
      assert length(members) == 1
      my_pid = self()
      assert [{^my_pid, %{role: :player}}] = members

      # Should NOT appear in default cluster members
      assert Group.members(sup, key) == []

      # Leave the named cluster
      :ok = Group.leave(sup, key, cluster: cluster)
      assert Group.members(sup, key, cluster: cluster) == []
    end

    test "events in one cluster don't leak to another", %{supervisor_name: sup} do
      cluster1 = :cluster_a
      cluster2 = :cluster_b
      key = "shared/key/#{DurableServer.UUID.uuid4()}"

      # Connect to both clusters
      :ok = Group.connect(sup, cluster1)
      :ok = Group.connect(sup, cluster2)

      # Subscribe to cluster1 only
      :ok = Group.monitor(sup, :all, cluster: cluster1)

      # Spawn process to join cluster1
      test_pid = self()

      pid1 =
        spawn(fn ->
          :ok = Group.join(sup, key, %{cluster: 1}, cluster: cluster1)
          send(test_pid, {:joined, 1})
          Process.sleep(5000)
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
          :ok = Group.join(sup, key, %{cluster: 2}, cluster: cluster2)
          send(test_pid, {:joined, 2})
          Process.sleep(5000)
        end)

      receive do
        {:joined, 2} -> :ok
      after
        1000 -> flunk("Process didn't join cluster2 in time")
      end

      # Should NOT receive event from cluster2
      refute_receive %Group.Event{type: :joined, pid: ^pid2, cluster: ^cluster2}, 200

      # Now subscribe to cluster2 and verify we can receive events
      :ok = Group.monitor(sup, :all, cluster: cluster2)

      pid3 =
        spawn(fn ->
          :ok = Group.join(sup, key, %{cluster: 2, extra: true}, cluster: cluster2)
          send(test_pid, {:joined, 3})
          Process.sleep(5000)
        end)

      receive do
        {:joined, 3} -> :ok
      after
        1000 -> flunk("Process didn't join cluster2 in time")
      end

      assert_receive %Group.Event{type: :joined, pid: ^pid3, cluster: ^cluster2}, 1000
    end

    test "members/3 returns only members from specified cluster", %{supervisor_name: sup} do
      cluster = :isolated_cluster
      key = "room/#{DurableServer.UUID.uuid4()}"

      :ok = Group.connect(sup, cluster)

      # Join default cluster
      :ok = Group.join(sup, key, %{location: :default})

      # Join named cluster (need different process since same pid can't join same key twice)
      test_pid = self()

      other_pid =
        spawn(fn ->
          :ok = Group.join(sup, key, %{location: :named}, cluster: cluster)
          send(test_pid, :ready)
          Process.sleep(5000)
        end)

      receive do
        :ready -> :ok
      after
        1000 -> flunk("Process didn't join in time")
      end

      # Default cluster should only have our process
      default_members = Group.members(sup, key)
      assert length(default_members) == 1
      my_pid = self()
      assert [{^my_pid, %{location: :default}}] = default_members

      # Named cluster should only have the spawned process
      named_members = Group.members(sup, key, cluster: cluster)
      assert length(named_members) == 1
      assert [{^other_pid, %{location: :named}}] = named_members
    end

    test "default cluster works without cluster: option", %{supervisor_name: sup} do
      key = "default/test/#{DurableServer.UUID.uuid4()}"

      # Subscribe without cluster option (default cluster)
      :ok = Group.monitor(sup, key)

      # Join without cluster option (default cluster)
      :ok = Group.join(sup, key, %{v: 1})

      # Should receive event with cluster: nil
      assert_receive %Group.Event{type: :joined} = event, 1000
      assert event.cluster == nil
      assert event.meta == %{v: 1}
      assert event.previous_meta == nil

      # Members without cluster option
      members = Group.members(sup, key)
      assert length(members) == 1
    end

    test "dispatch works with cluster: option", %{supervisor_name: sup} do
      cluster = :broadcast_cluster
      key = "broadcast/#{DurableServer.UUID.uuid4()}"

      :ok = Group.connect(sup, cluster)

      # Join the named cluster
      :ok = Group.join(sup, key, %{}, cluster: cluster)

      # Broadcast to named cluster
      :ok = Group.dispatch(sup, key, {:test_message, :from_cluster}, cluster: cluster)

      assert_receive {:test_message, :from_cluster}, 1000

      # Broadcast to default cluster (we're not there)
      :ok = Group.dispatch(sup, key, {:test_message, :from_default})

      # Should NOT receive (we're not in default cluster for this key)
      refute_receive {:test_message, :from_default}, 200
    end

    test "monitor/demonitor work with cluster: option", %{supervisor_name: sup} do
      cluster = :sub_cluster
      key = "sub/test/#{DurableServer.UUID.uuid4()}"

      :ok = Group.connect(sup, cluster)

      # Subscribe to named cluster
      :ok = Group.monitor(sup, key, cluster: cluster)

      # Spawn and join
      test_pid = self()

      spawn(fn ->
        :ok = Group.join(sup, key, %{}, cluster: cluster)
        send(test_pid, :joined)
        Process.sleep(5000)
      end)

      receive do
        :joined -> :ok
      after
        1000 -> flunk("Process didn't join in time")
      end

      assert_receive %Group.Event{type: :joined, cluster: ^cluster}, 1000

      # Unsubscribe from named cluster
      :ok = Group.demonitor(sup, key, cluster: cluster)

      # Spawn another process to join
      spawn(fn ->
        :ok = Group.join(sup, key, %{second: true}, cluster: cluster)
        send(test_pid, :joined2)
        Process.sleep(5000)
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
end
