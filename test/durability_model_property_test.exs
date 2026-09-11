defmodule DurableServer.DurabilityModelPropertyTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias DurableServer.{PropertyFixture, StorageBackend, StoredState}

  @moduletag :property
  @moduletag capture_log: [level: :warning]

  defmodule Counter do
    use DurableServer, vsn: 1

    def dump_state(state), do: state
    def load_state(_vsn, state), do: state
    # Disable periodic persistence so losing an unsynced update is deterministic.
    # This property tests explicit restarts, not permanent-object discovery.
    def init(state), do: {:ok, state, auto_sync: false, sync_every_ms: nil}

    def handle_call(:read, _from, state), do: {:reply, state.count, state}

    def handle_call({:add, delta, :unsynced}, _from, state),
      do: {:reply, state.count + delta, %{state | count: state.count + delta}}

    def handle_call({:add, delta, :sync}, _from, state),
      do: {:reply, state.count + delta, %{state | count: state.count + delta}, :sync}

    def handle_call({:add, delta, :sync_metadata}, _from, state),
      do:
        {:reply, state.count + delta, %{state | count: state.count + delta},
         {:sync, %{status: :running}}}

    def handle_call({:add, delta, :sync_option}, _from, state),
      do: {:reply, state.count + delta, %{state | count: state.count + delta}, sync: true}
  end

  setup do
    root = Path.expand("tmp/durability_property/#{DurableServer.UUID.uuid4()}")
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, root: root}
  end

  property "strict acknowledgements survive crashes; unsynced updates need not", %{root: root} do
    check all(
            initial <- integer(-100..100),
            delta <- member_of([-5, -1, 1, 5]),
            mode <- member_of([:sync, :sync_metadata, :sync_option]),
            commands <- list_of(command(), max_length: 40)
          ) do
      PropertyFixture.with_sample(root, :durable, fn fixture ->
        pid = start_counter(fixture.supervisor, initial)
        model = %{pid: pid, memory: initial, durable: initial}

        # Mandatory witness prevents a generated sequence from passing without a
        # strict acknowledgement followed by a crash, including during shrinking.
        commands =
          [{:add, delta, mode}, {:add, delta, :unsynced}, :crash_restart] ++
            commands ++ [:crash_restart]

        Enum.reduce(commands, model, fn command, model ->
          model = step(command, model, fixture)
          assert GenServer.call(model.pid, :read) == model.memory

          assert {:ok, %{body: %StoredState{state: %{count: persisted}}}} =
                   StorageBackend.get_object(fixture.backend, "property/counter",
                     consistent: true
                   )

          assert persisted == model.durable
          model
        end)
      end)
    end
  end

  defp command do
    frequency([
      {6,
       tuple(
         {constant(:add), integer(-100..100),
          member_of([:unsynced, :sync, :sync_metadata, :sync_option])}
       )},
      {2, constant(:crash_restart)},
      {1, constant(:graceful_restart)}
    ])
  end

  defp step({:add, delta, mode} = command, model, _fixture) do
    expected = model.memory + delta
    # Do not catch call exits as failed writes: an unacknowledged call is unknown.
    # This fault-free command must return an acknowledgement or fail the property.
    assert GenServer.call(model.pid, command) == expected
    durable = if mode == :unsynced, do: model.durable, else: expected
    %{model | memory: expected, durable: durable}
  end

  defp step(restart, model, fixture) when restart in [:crash_restart, :graceful_restart] do
    ref = Process.monitor(model.pid)

    case restart do
      :crash_restart -> Process.exit(model.pid, :kill)
      :graceful_restart -> DurableServer.Supervisor.terminate_child(fixture.supervisor, model.pid)
    end

    expected_reason = if restart == :crash_restart, do: :killed, else: :normal
    assert_receive {:DOWN, ^ref, :process, _, ^expected_reason}, 1_000
    await_unregistered(fixture.supervisor)

    durable = if restart == :crash_restart, do: model.durable, else: model.memory
    # A deliberately different initial value detects accidentally starting fresh.
    pid = start_counter(fixture.supervisor, durable + 1)
    refute pid == model.pid
    %{pid: pid, memory: durable, durable: durable}
  end

  defp start_counter(supervisor, initial) do
    assert {:ok, {pid, _}} =
             DurableServer.Supervisor.start_child(
               supervisor,
               {Counter, key: "counter", initial_state: %{count: initial}}
             )

    pid
  end

  defp await_unregistered(supervisor) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    await_unregistered(supervisor, deadline)
  end

  defp await_unregistered(supervisor, deadline) do
    if DurableServer.Supervisor.lookup(supervisor, "counter") do
      assert System.monotonic_time(:millisecond) < deadline, "dead owner remained registered"
      Process.sleep(1)
      await_unregistered(supervisor, deadline)
    end
  end
end
