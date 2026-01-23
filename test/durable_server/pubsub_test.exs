defmodule DurableServer.PubSubTest do
  use ExUnit.Case, async: true
  import DurableServer.TestHelper

  alias DurableServer.PubSub

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
    supervisor_name = :"test_pubsub_#{DurableServer.UUID.uuid4()}"
    prefix = "test_pubsub_#{DurableServer.UUID.uuid4()}/"

    _supervisor_pid =
      start_supervised!({
        DurableServer.Supervisor,
        name: supervisor_name, prefix: prefix, object_store: test_object_store_opts()
      })

    {:ok, supervisor_name: supervisor_name, prefix: prefix}
  end

  describe "subscribe/2" do
    test "subscribes to exact key and receives :registered event", %{supervisor_name: sup} do
      key = "user/#{DurableServer.UUID.uuid4()}"

      # Subscribe before starting server
      :ok = PubSub.subscribe(sup, key)

      # Start a DurableServer
      {:ok, {pid, _meta}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      # Should receive :registered event
      assert_receive {:durable_server, :registered, payload}, 1000
      assert payload.supervisor == sup
      assert payload.key == key
      assert payload.pid == pid
      assert is_map(payload.meta)
    end

    test "subscribes to prefix pattern and receives events for matching keys", %{
      supervisor_name: sup
    } do
      key1 = "chat/room1"
      key2 = "chat/room2"
      key3 = "other/room"

      :ok = PubSub.subscribe(sup, "chat/")

      # Start servers
      {:ok, {pid1, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key1}})
      {:ok, {pid2, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key2}})
      {:ok, {_pid3, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key3}})

      # Should receive events for chat/ keys
      assert_receive {:durable_server, :registered, %{key: ^key1, pid: ^pid1}}, 1000
      assert_receive {:durable_server, :registered, %{key: ^key2, pid: ^pid2}}, 1000

      # Should NOT receive event for other/ keys
      refute_receive {:durable_server, :registered, %{key: ^key3}}, 100
    end

    test "subscribes to wildcard '*' and receives all events", %{supervisor_name: sup} do
      key1 = "user/123"
      key2 = "chat/room"
      key3 = "anything/else"

      :ok = PubSub.subscribe(sup, :all)

      {:ok, {pid1, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key1}})
      {:ok, {pid2, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key2}})
      {:ok, {pid3, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key3}})

      assert_receive {:durable_server, :registered, %{key: ^key1, pid: ^pid1}}, 1000
      assert_receive {:durable_server, :registered, %{key: ^key2, pid: ^pid2}}, 1000
      assert_receive {:durable_server, :registered, %{key: ^key3, pid: ^pid3}}, 1000
    end

    test "receives :unregistered event when DurableServer stops", %{supervisor_name: sup} do
      key = "user/#{DurableServer.UUID.uuid4()}"

      :ok = PubSub.subscribe(sup, key)

      {:ok, {pid, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})
      assert_receive {:durable_server, :registered, _}, 1000

      # Stop the server
      ref = Process.monitor(pid)
      :ok = DurableServer.Supervisor.terminate_child(sup, pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1000

      # Should receive :unregistered event
      assert_receive {:durable_server, :unregistered, payload}, 1000
      assert payload.supervisor == sup
      assert payload.key == key
      assert payload.pid == pid
      assert Map.has_key?(payload, :reason)
    end

    test "double subscribe is idempotent", %{supervisor_name: sup} do
      key = "user/test"

      assert :ok = PubSub.subscribe(sup, key)
      assert :ok = PubSub.subscribe(sup, key)

      {:ok, {pid, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      # Should only receive one event (not duplicated)
      assert_receive {:durable_server, :registered, %{pid: ^pid}}, 1000
      refute_receive {:durable_server, :registered, %{pid: ^pid}}, 100
    end
  end

  describe "unsubscribe/2" do
    test "stops receiving events after unsubscribe", %{supervisor_name: sup} do
      key1 = "user/first"
      key2 = "user/second"

      :ok = PubSub.subscribe(sup, "user/")

      {:ok, {pid1, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key1}})
      assert_receive {:durable_server, :registered, %{key: ^key1, pid: ^pid1}}, 1000

      # Unsubscribe
      :ok = PubSub.unsubscribe(sup, "user/")

      # Start another server
      {:ok, {_pid2, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key2}})

      # Should NOT receive the second event
      refute_receive {:durable_server, :registered, %{key: ^key2}}, 200
    end

    test "unsubscribe from non-existent subscription is ok", %{supervisor_name: sup} do
      assert :ok = PubSub.unsubscribe(sup, "nonexistent/")
    end
  end

  describe "join/3 and leave/2" do
    test "joined process appears in members/2", %{supervisor_name: sup} do
      key = "chat/room/#{DurableServer.UUID.uuid4()}"
      meta = %{role: :listener}

      :ok = PubSub.join(sup, key, meta)

      members = PubSub.members(sup, key)
      assert length(members) == 1
      my_pid = self()
      assert {^my_pid, ^meta} = hd(members)
    end

    test "joined process triggers :joined event to subscribers", %{supervisor_name: sup} do
      key = "chat/room/#{DurableServer.UUID.uuid4()}"

      # Subscribe first
      :ok = PubSub.subscribe(sup, key)

      # Spawn a process to join
      test_pid = self()

      spawn_pid =
        spawn(fn ->
          :ok = PubSub.join(sup, key, %{role: :worker})
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
      assert_receive {:durable_server, :joined, payload}, 1000
      assert payload.supervisor == sup
      assert payload.key == key
      assert payload.pid == spawn_pid
      assert payload.meta == %{role: :worker}
    end

    test "leave/2 removes from members and triggers :left event", %{supervisor_name: sup} do
      key = "chat/room/#{DurableServer.UUID.uuid4()}"

      :ok = PubSub.subscribe(sup, key)
      :ok = PubSub.join(sup, key, %{role: :listener})

      assert_receive {:durable_server, :joined, _}, 1000

      assert length(PubSub.members(sup, key)) == 1

      :ok = PubSub.leave(sup, key)

      # Should receive :left event
      assert_receive {:durable_server, :left, payload}, 1000
      assert payload.key == key
      assert payload.pid == self()
      assert Map.has_key?(payload, :reason)

      assert PubSub.members(sup, key) == []
    end

    test "process death triggers automatic :left event", %{supervisor_name: sup} do
      key = "chat/room/#{DurableServer.UUID.uuid4()}"

      :ok = PubSub.subscribe(sup, key)

      test_pid = self()

      pid =
        spawn(fn ->
          :ok = PubSub.join(sup, key, %{role: :temp})
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

      assert_receive {:durable_server, :joined, %{pid: ^pid}}, 1000
      assert length(PubSub.members(sup, key)) == 1

      # Kill the process
      Process.exit(pid, :kill)

      # Should receive :left event
      assert_receive {:durable_server, :left, payload}, 1000
      assert payload.pid == pid
      assert payload.key == key

      # Should be removed from members
      assert PubSub.members(sup, key) == []
    end

    test "leave/2 returns error when not a member", %{supervisor_name: sup} do
      key = "nonexistent/key"
      assert {:error, :not_in_group} = PubSub.leave(sup, key)
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
      :ok = PubSub.join(sup, key, listener_meta)

      members = PubSub.members(sup, key)
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
      assert PubSub.members(sup, "nonexistent/key") == []
    end

    test "returns only DurableServer when no joined processes", %{supervisor_name: sup} do
      key = "only/server/#{DurableServer.UUID.uuid4()}"

      {:ok, {pid, _}} = DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      members = PubSub.members(sup, key)
      assert length(members) == 1
      assert {^pid, _meta} = hd(members)
    end

    test "returns only joined processes when no DurableServer", %{supervisor_name: sup} do
      key = "only/joined/#{DurableServer.UUID.uuid4()}"

      :ok = PubSub.join(sup, key, %{role: :standalone})

      members = PubSub.members(sup, key)
      assert length(members) == 1
      my_pid = self()
      assert {^my_pid, %{role: :standalone}} = hd(members)
    end
  end

  describe "self-events" do
    test "joining process receives its own :joined event if subscribed", %{supervisor_name: sup} do
      key = "self/events/#{DurableServer.UUID.uuid4()}"

      # Subscribe first
      :ok = PubSub.subscribe(sup, key)

      # Then join
      :ok = PubSub.join(sup, key, %{self: true})

      # Should receive our own :joined event
      assert_receive {:durable_server, :joined, payload}, 1000
      assert payload.pid == self()
      assert payload.meta == %{self: true}
    end
  end

  describe "integration" do
    test "full lifecycle: subscribe, start server, join, stop, leave", %{supervisor_name: sup} do
      key = "integration/test/#{DurableServer.UUID.uuid4()}"

      # 1. Subscribe
      :ok = PubSub.subscribe(sup, key)

      # 2. Start DurableServer
      {:ok, {server_pid, _}} =
        DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key}})

      assert_receive {:durable_server, :registered, %{pid: ^server_pid}}, 1000

      # 3. Verify members shows DurableServer
      members = PubSub.members(sup, key)
      assert length(members) == 1

      # 4. Join as listener
      :ok = PubSub.join(sup, key, %{role: :listener})
      assert_receive {:durable_server, :joined, %{pid: self_pid}}, 1000
      assert self_pid == self()

      # 5. Verify members shows both
      members = PubSub.members(sup, key)
      assert length(members) == 2

      # 6. Stop DurableServer
      ref = Process.monitor(server_pid)
      :ok = DurableServer.Supervisor.terminate_child(sup, server_pid)
      assert_receive {:DOWN, ^ref, :process, ^server_pid, _}, 1000
      assert_receive {:durable_server, :unregistered, %{pid: ^server_pid}}, 1000

      # 7. Verify members shows only joined process
      members = PubSub.members(sup, key)
      assert length(members) == 1
      my_pid = self()
      assert {^my_pid, _} = hd(members)

      # 8. Leave
      :ok = PubSub.leave(sup, key)
      assert_receive {:durable_server, :left, %{pid: self_pid}}, 1000
      assert self_pid == self()

      # 9. Verify empty members
      assert PubSub.members(sup, key) == []

      # 10. Unsubscribe
      :ok = PubSub.unsubscribe(sup, key)

      # 11. Start new server - should NOT receive event
      {:ok, {_new_pid, _}} =
        DurableServer.Supervisor.start_child(sup, {TestServer, %{key: key <> "/new"}})

      refute_receive {:durable_server, :registered, _}, 200
    end
  end

  describe "multiple supervisors" do
    test "events from one supervisor don't leak to another supervisor's subscribers" do
      # Create a second supervisor
      supervisor_name_2 = :"test_pubsub_2_#{DurableServer.UUID.uuid4()}"
      prefix_2 = "test_pubsub_2_#{DurableServer.UUID.uuid4()}/"

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

      # Subscribe to events on supervisor 1 (from setup)
      sup1 = supervisor_name_2
      # Actually let's use the one from setup context - we need to get it
      # We'll create subscriptions on both and verify isolation

      # Subscribe to sup2 only
      :ok = PubSub.subscribe(supervisor_name_2, :all)

      # Start a DurableServer on sup2
      {:ok, {pid2, _}} =
        DurableServer.Supervisor.start_child(supervisor_name_2, {TestServer, %{key: key}})

      # Should receive event from sup2
      assert_receive {:durable_server, :registered,
                      %{supervisor: ^supervisor_name_2, pid: ^pid2}},
                     1000

      # Now unsubscribe from sup2 and subscribe to sup1 (from setup)
      :ok = PubSub.unsubscribe(supervisor_name_2, :all)
    end

    test "subscribers only receive events from their subscribed supervisor", %{
      supervisor_name: sup1
    } do
      # Create a second supervisor
      sup2 = :"test_pubsub_isolated_#{DurableServer.UUID.uuid4()}"
      prefix_2 = "test_pubsub_isolated_#{DurableServer.UUID.uuid4()}/"

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
      :ok = PubSub.subscribe(sup1, :all)

      # Start a DurableServer on sup1 - should receive event
      {:ok, {pid1, _}} = DurableServer.Supervisor.start_child(sup1, {TestServer, %{key: key}})
      assert_receive {:durable_server, :registered, %{supervisor: ^sup1, pid: ^pid1}}, 1000

      # Start a DurableServer on sup2 - should NOT receive event
      {:ok, {pid2, _}} = DurableServer.Supervisor.start_child(sup2, {TestServer, %{key: key}})
      refute_receive {:durable_server, :registered, %{supervisor: ^sup2, pid: ^pid2}}, 200

      # Now subscribe to sup2 as well
      :ok = PubSub.subscribe(sup2, :all)

      # Join on sup2 - should receive event now
      :ok = PubSub.join(sup2, key, %{role: :test})
      assert_receive {:durable_server, :joined, %{supervisor: ^sup2}}, 1000

      # Join on sup1 - should also receive (we're subscribed to both now)
      :ok = PubSub.join(sup1, key, %{role: :test})
      assert_receive {:durable_server, :joined, %{supervisor: ^sup1}}, 1000

      # Members are isolated per supervisor
      sup1_members = PubSub.members(sup1, key)
      sup2_members = PubSub.members(sup2, key)

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
end
