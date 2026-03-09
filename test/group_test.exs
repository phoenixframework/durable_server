defmodule GroupTest do
  use ExUnit.Case, async: true
  import DurableServer.TestHelper

  @moduletag :capture_log

  defmodule TestServer do
    use DurableServer, vsn: 1

    def dump_state(state), do: state

    def load_state(_old_vsn, persisted_state), do: persisted_state

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

    def handle_call({:update_meta, new_meta}, _from, state) do
      {:reply, :ok, state, meta: new_meta}
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

      # Should receive :registered event with extracted user meta
      assert_receive {:group, [%Group.Event{type: :registered} = event], _}, 1000
      assert event.supervisor == sup
      assert event.key == key
      assert event.pid == pid
      assert event.cluster == nil
      assert event.previous_meta == nil
      assert event.meta == %{module: TestServer}
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

      # Should receive events for chat/ keys with extracted user meta
      assert_receive {:group,
                      [
                        %Group.Event{
                          type: :registered,
                          key: ^key1,
                          pid: ^pid1,
                          meta: %{module: TestServer}
                        }
                      ], _},
                     1000

      assert_receive {:group,
                      [
                        %Group.Event{
                          type: :registered,
                          key: ^key2,
                          pid: ^pid2,
                          meta: %{module: TestServer}
                        }
                      ], _},
                     1000

      # Should NOT receive event for other/ keys
      refute_receive {:group, _, _}, 100
    end

    test "subscribes to :all and receives all events", %{supervisor_name: sup} do
      key1 = "user/123"
      key2 = "chat/room"
      key3 = "anything/else"

      :ok = Group.monitor(sup, :all)

      {:ok, {pid1, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key1}})
      {:ok, {pid2, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key2}})
      {:ok, {pid3, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key3}})

      server_meta = %{module: TestServer}

      assert_receive {:group,
                      [
                        %Group.Event{
                          type: :registered,
                          key: ^key1,
                          pid: ^pid1,
                          meta: ^server_meta
                        }
                      ], _},
                     1000

      assert_receive {:group,
                      [
                        %Group.Event{
                          type: :registered,
                          key: ^key2,
                          pid: ^pid2,
                          meta: ^server_meta
                        }
                      ], _},
                     1000

      assert_receive {:group,
                      [
                        %Group.Event{
                          type: :registered,
                          key: ^key3,
                          pid: ^pid3,
                          meta: ^server_meta
                        }
                      ], _},
                     1000
    end

    test "receives :unregistered event when DurableServer stops", %{supervisor_name: sup} do
      key = "user/#{DurableServer.UUID.uuid4()}"

      :ok = Group.monitor(sup, key)

      {:ok, {pid, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      assert_receive {:group, [%Group.Event{type: :registered, meta: %{module: TestServer}}], _},
                     1000

      # Stop the server
      ref = Process.monitor(pid)
      :ok = DurableServer.Supervisor.terminate_child(sup, pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1000

      # Should receive :unregistered event with extracted user meta
      assert_receive {:group, [%Group.Event{type: :unregistered} = event], _}, 1000
      assert event.supervisor == sup
      assert event.key == key
      assert event.pid == pid
      assert event.meta == %{module: TestServer}
      assert event.reason != nil
    end

    test "receives :registered event with previous_meta when DurableServer updates meta", %{
      supervisor_name: sup
    } do
      key = "user/#{DurableServer.UUID.uuid4()}"

      :ok = Group.monitor(sup, key)

      {:ok, {pid, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      # Initial registration
      assert_receive {:group,
                      [
                        %Group.Event{
                          type: :registered,
                          pid: ^pid,
                          meta: %{module: TestServer},
                          previous_meta: nil
                        }
                      ], _},
                     1000

      # Update meta
      :ok = GenServer.call(pid, {:update_meta, %{module: TestServer, status: :active}})

      # Should receive :registered event with previous_meta showing the old extracted meta
      assert_receive {:group, [%Group.Event{type: :registered} = event], _}, 1000
      assert event.pid == pid
      assert event.meta == %{module: TestServer, status: :active}
      assert event.previous_meta == %{module: TestServer}
    end

    test "double subscribe is idempotent", %{supervisor_name: sup} do
      key = "user/test"

      assert :ok = Group.monitor(sup, key)
      assert :ok = Group.monitor(sup, key)

      {:ok, {pid, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      # Should only receive one event (not duplicated), with extracted user meta
      assert_receive {:group,
                      [%Group.Event{type: :registered, pid: ^pid, meta: %{module: TestServer}}],
                      _},
                     1000

      refute_receive {:group, _, _}, 100
    end
  end

  describe "demonitor/2" do
    test "stops receiving events after unsubscribe", %{supervisor_name: sup} do
      key1 = "user/first"
      key2 = "user/second"

      :ok = Group.monitor(sup, "user/")

      {:ok, {pid1, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key1}})

      assert_receive {:group,
                      [
                        %Group.Event{
                          type: :registered,
                          key: ^key1,
                          pid: ^pid1,
                          meta: %{module: TestServer}
                        }
                      ], _},
                     1000

      # Unsubscribe
      :ok = Group.demonitor(sup, "user/")

      # Start another server
      {:ok, {_pid2, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key2}})

      # Should NOT receive the second event
      refute_receive {:group, _, _}, 200
    end

    test "unsubscribe from non-existent subscription is ok", %{supervisor_name: sup} do
      assert :ok = Group.demonitor(sup, "nonexistent/")
    end
  end

  describe "members/2" do
    test "returns only joined processes, not registered DurableServer", %{supervisor_name: sup} do
      key = "combined/#{DurableServer.UUID.uuid4()}"

      # Start a DurableServer (registers but does not join)
      {:ok, {_server_pid, _}} =
        DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      # Join as a listener
      listener_meta = %{role: :listener}
      :ok = Group.join(sup, key, listener_meta)

      members = Group.members(sup, key)
      assert length(members) == 1

      my_pid = self()
      assert [{^my_pid, ^listener_meta}] = members
    end

    test "returns empty list when only a DurableServer is registered", %{supervisor_name: sup} do
      key = "only/server/#{DurableServer.UUID.uuid4()}"

      {:ok, {_pid, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      assert Group.members(sup, key) == []
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

      assert_receive {:group,
                      [
                        %Group.Event{
                          type: :registered,
                          pid: ^server_pid,
                          meta: %{module: TestServer},
                          previous_meta: nil
                        }
                      ], _},
                     1000

      # 3. Verify DurableServer is discoverable via lookup (not members)
      assert {^server_pid, %{module: TestServer}} = Group.lookup(sup, key)
      assert Group.members(sup, key) == []

      # 4. Join as listener
      :ok = Group.join(sup, key, %{role: :listener})
      assert_receive {:group, [%Group.Event{type: :joined, pid: self_pid}], _}, 1000
      assert self_pid == self()

      # 5. Verify members shows only joined process
      my_pid = self()
      assert [{^my_pid, %{role: :listener}}] = Group.members(sup, key)

      # 6. Stop DurableServer
      ref = Process.monitor(server_pid)
      :ok = DurableServer.Supervisor.terminate_child(sup, server_pid)
      assert_receive {:DOWN, ^ref, :process, ^server_pid, _}, 1000

      assert_receive {:group,
                      [
                        %Group.Event{
                          type: :unregistered,
                          pid: ^server_pid,
                          meta: %{module: TestServer}
                        }
                      ], _},
                     1000

      # 7. Verify members still shows joined process (DurableServer was registry-only)
      assert [{^my_pid, %{role: :listener}}] = Group.members(sup, key)

      # 8. Leave
      :ok = Group.leave(sup, key)
      assert_receive {:group, [%Group.Event{type: :left, pid: self_pid}], _}, 1000
      assert self_pid == self()

      # 9. Verify empty members
      assert Group.members(sup, key) == []

      # 10. Unsubscribe
      :ok = Group.demonitor(sup, key)

      # 11. Start new server - should NOT receive event
      {:ok, {_new_pid, _}} =
        DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key <> "/new"}})

      refute_receive {:group, _, _}, 200
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

      # Should receive event from sup2 with extracted user meta
      assert_receive {:group,
                      [
                        %Group.Event{
                          type: :registered,
                          supervisor: ^supervisor_name_2,
                          pid: ^pid2,
                          meta: %{module: TestServer}
                        }
                      ], _},
                     1000

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

      # Start a DurableServer on sup1 - should receive event with extracted meta
      {:ok, {pid1, _}} = DurableServer.Supervisor.start_child(sup1, {TestServer, %{key: key}})

      assert_receive {:group,
                      [
                        %Group.Event{
                          type: :registered,
                          supervisor: ^sup1,
                          pid: ^pid1,
                          meta: %{module: TestServer}
                        }
                      ], _},
                     1000

      # Start a DurableServer on sup2 - should NOT receive event
      {:ok, {pid2, _}} = DurableServer.Supervisor.start_child(sup2, {TestServer, %{key: key}})
      refute_receive {:group, [%Group.Event{supervisor: ^sup2} | _], _}, 200

      # Now subscribe to sup2 as well
      :ok = Group.monitor(sup2, :all)

      # Join on sup2 - should receive event now
      :ok = Group.join(sup2, key, %{role: :test})
      assert_receive {:group, [%Group.Event{type: :joined, supervisor: ^sup2}], _}, 1000

      # Join on sup1 - should also receive (we're subscribed to both now)
      :ok = Group.join(sup1, key, %{role: :test})
      assert_receive {:group, [%Group.Event{type: :joined, supervisor: ^sup1}], _}, 1000

      # Members are PG-only (joined processes), isolated per supervisor
      sup1_members = Group.members(sup1, key) |> Map.new()
      sup2_members = Group.members(sup2, key) |> Map.new()

      my_pid = self()

      # Only the joined process appears (DurableServers register, not join)
      assert sup1_members == %{my_pid => %{role: :test}}
      assert sup2_members == %{my_pid => %{role: :test}}

      # DurableServers are still discoverable via lookup
      assert {^pid1, %{module: TestServer}} = Group.lookup(sup1, key)
      assert {^pid2, %{module: TestServer}} = Group.lookup(sup2, key)
      refute pid1 == pid2
    end
  end

  describe "GroupConflictResolver" do
    test "conflict resolver kills both processes for clean restart", %{supervisor_name: sup} do
      key = "conflict/test/#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      # Get the raw internal metadata (bypassing extract_meta)
      {^pid, meta} = Group.lookup(sup, key, extract_meta: & &1)

      # Spawn a fake "conflicting" process
      fake_pid = spawn(fn -> Process.sleep(:infinity) end)
      ref_real = Process.monitor(pid)
      ref_fake = Process.monitor(fake_pid)

      time = System.system_time()

      # Call the conflict resolver directly — this is what Group.Replica
      # calls during partition healing when it detects a conflict.
      config = Group.get_config(sup)
      {mod, func, extra_args} = config.resolve_registry_conflict

      winner =
        apply(mod, func, [
          sup,
          key,
          {pid, meta, time},
          {fake_pid, %DurableServer.GroupMeta{}, time + 1} | extra_args
        ])

      # Resolver returns first pid as nominal winner (both are killed anyway)
      assert winner == pid

      # Both processes are killed for clean restart
      assert_receive {:DOWN, ^ref_real, :process, ^pid, _}, 1000
      assert_receive {:DOWN, ^ref_fake, :process, ^fake_pid, _}, 1000
    end
  end
end
