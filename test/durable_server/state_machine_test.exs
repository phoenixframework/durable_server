defmodule DurableServer.StateMachineTest do
  use ExUnit.Case, async: true
  import DurableServer.TestHelper

  @moduletag :capture_log

  defmodule Machine do
    use DurableServer, vsn: 1, behaviour: :gen_statem

    def callback_mode, do: [:handle_event_function, :state_enter]

    def dump_state({name, data}) do
      %{"name" => Atom.to_string(name), "count" => data.count}
    end

    def load_state(_vsn, %{"name" => name, "count" => count}) do
      name =
        case name do
          "idle" -> :idle
          "running" -> :running
          "done" -> :done
        end

      {name, %{count: count}}
    end

    def init({name, data}, info) do
      data = Map.put(data, :observer, info.observer)

      {:ok, name, data,
       [
         {:durable,
          [
            meta: %{phase: name},
            auto_sync: info.auto_sync,
            permanent: info.permanent,
            sync_every_ms: 60_000
          ]}
       ]}
    end

    def handle_event(:enter, previous, name, data) do
      send(data.observer, {:entered, self(), previous, name})
      :keep_state_and_data
    end

    def handle_event({:call, from}, :get, name, data),
      do: {:keep_state_and_data, [{:reply, from, {name, data.count}}]}

    def handle_event({:call, from}, :start, _name, data) do
      {:next_state, :running, %{data | count: data.count + 1},
       [{:reply, from, :ok}, {:durable, [sync: true, meta: %{phase: :running}]}]}
    end

    def handle_event({:call, from}, :increment, _name, data),
      do: {:keep_state, %{data | count: data.count + 1}, [{:reply, from, :ok}]}

    def handle_event({:call, from}, :throw_transition, _name, data) do
      throw(
        {:next_state, :running, %{data | count: data.count + 1},
         [{:reply, from, :ok}, {:durable, [sync: true]}]}
      )
    end

    def handle_event({:call, from}, :hold_terminate, _name, data),
      do: {:keep_state, Map.put(data, :hold_terminate?, true), [{:reply, from, :ok}]}

    def handle_event({:call, from}, {:switch_callback, action}, _name, _data),
      do: {:keep_state_and_data, [{:reply, from, :ok}, action]}

    def handle_event({:call, from}, :deferred_reply, _name, _data) do
      send(self(), {:reply_later, from})
      :keep_state_and_data
    end

    def handle_event(:info, {:reply_later, from}, _name, _data) do
      :gen_statem.reply(from, :deferred)
      :keep_state_and_data
    end

    def handle_event(:cast, :needs_running, :idle, data) do
      send(data.observer, {:postponed, self()})
      {:keep_state_and_data, [:postpone]}
    end

    def handle_event(:cast, :needs_running, :running, data) do
      send(data.observer, {:processed, self()})
      :keep_state_and_data
    end

    def handle_event(:cast, :increment, _name, data),
      do: {:keep_state, %{data | count: data.count + 1}}

    def handle_event({:call, from}, :repeat, _name, _data),
      do: {:repeat_state_and_data, [{:reply, from, :ok}]}

    def handle_event({:call, from}, :arm_timeouts, _name, _data) do
      {:keep_state_and_data,
       [{:state_timeout, 50, :expired}, {{:timeout, :probe}, 100, :expired}, {:reply, from, :ok}]}
    end

    def handle_event(type, :expired, name, data) do
      send(data.observer, {:expired, self(), type, name})
      :keep_state_and_data
    end

    def handle_event({:call, from}, :finish, _name, data) do
      {:next_state, :done, %{data | count: data.count + 1},
       [{:durable, [sync: true]}, {:next_event, :internal, {:committed, from}}]}
    end

    def handle_event(:internal, {:committed, from}, name, data) do
      send(data.observer, {:committed_event, self(), name})
      {:keep_state_and_data, [{:reply, from, {name, data.count}}]}
    end

    def handle_event({:call, from}, {:stop, reason}, _name, data),
      do: {:stop_and_reply, reason, [{:reply, from, :bye}], data}

    def handle_sync(_info, old, {name, data} = current) do
      if observer = Map.get(data, :observer),
        do: send(observer, {:synced, self(), old, current})

      {:ok, {name, data}}
    end

    def code_change(0, _name, data, 1),
      do: {:ok, :running, %{data | count: data.count + 100}}

    def code_change(_old, name, data, :upgrade),
      do: {:ok, name, %{data | count: data.count + 10}}

    def code_change(_old, _name, _data, :reject), do: {:error, :upgrade_rejected}

    def code_change(_old, name, data, _extra), do: {:ok, name, data}

    def terminate(reason, name, data) do
      if Map.get(data, :hold_terminate?, false) do
        send(data.observer, {:terminate_waiting, self()})

        receive do
          :finish_terminate -> :ok
        end
      end

      send(data.observer, {:terminated, self(), reason, name, data.count})
      {:finished, data.observer}
    end

    def after_terminate({:finished, observer}, info) do
      send(observer, {:after_terminate, self(), :finished, info.final_status})
      :ok
    end
  end

  defmodule FaultBackend do
    @behaviour DurableServer.StorageBackend
    alias DurableServer.Backends.ObjectStore, as: Backend

    def init_backend(opts) do
      {:ok, config} = Backend.init_backend(Keyword.fetch!(opts, :store))
      state = %{delegate: config.state, table: Keyword.fetch!(opts, :table)}
      {:ok, %{config | state: state}}
    end

    for {function, arity} <- [
          ensure_ready: 1,
          get_object: 3,
          list_all_objects_stream: 3,
          delete_object: 2,
          delete_object: 3,
          update_object: 4,
          encode: 2,
          decode: 2
        ] do
      args = Macro.generate_arguments(arity - 1, __MODULE__)

      def unquote(function)(%{delegate: delegate}, unquote_splicing(args)),
        do: Backend.unquote(function)(delegate, unquote_splicing(args))
    end

    def try_claim(%{delegate: delegate, table: table}, key, body) do
      case :ets.lookup(table, {:block_claim, key}) do
        [{{:block_claim, ^key}, observer}] ->
          send(observer, {:claim_blocked, self()})

          receive do
            :continue_claim -> :ok
          end

        [] ->
          :ok
      end

      Backend.try_claim(delegate, key, body)
    end

    def put_object(%{delegate: delegate, table: table}, key, body, opts) do
      case :ets.lookup(table, :fail_key) do
        [{:fail_key, ^key}] -> {:error, :unavailable}
        _ -> Backend.put_object(delegate, key, body, opts)
      end
    end
  end

  defmodule StateFunctions do
    use DurableServer, vsn: 1, behaviour: :gen_statem

    def callback_mode, do: :state_functions
    def dump_state({name, count}), do: %{"name" => Atom.to_string(name), "count" => count}
    def load_state(_, %{"name" => "idle", "count" => count}), do: {:idle, count}
    def load_state(_, %{"name" => "running", "count" => count}), do: {:running, count}

    def idle({:call, from}, :start, count),
      do: {:next_state, :running, count + 1, [{:reply, from, :ok}, {:durable, [sync: true]}]}

    def running({:call, from}, :get, count),
      do: {:keep_state_and_data, [{:reply, from, count}]}
  end

  defmodule GenServerCallbacks do
    use DurableServer, vsn: 1, behaviour: GenServer

    def dump_state(state), do: state
    def load_state(_vsn, state), do: state
    def handle_call(:get, _from, state), do: {:reply, state, state}

    def code_change(_old, _state, :reject), do: {:error, :upgrade_rejected}
    def code_change(_old, state, _extra), do: {:ok, state}
  end

  defmodule AliasedGenServerCallbacks do
    alias GenServer, as: Server
    use DurableServer, vsn: 1, behaviour: Server

    defdelegate dump_state(state), to: GenServerCallbacks
    defdelegate load_state(vsn, state), to: GenServerCallbacks
    defdelegate handle_call(request, from, state), to: GenServerCallbacks
  end

  defmodule InitResult do
    use DurableServer, vsn: 1, behaviour: :gen_statem

    def callback_mode, do: :handle_event_function

    defdelegate dump_state(state), to: StateFunctions
    defdelegate load_state(vsn, state), to: StateFunctions

    def init({:idle, 0}), do: :ignore
    def init({:idle, 1}), do: {:stop, :not_ready}
    def init({:idle, 2}), do: {:error, :not_ready}
    def init({:idle, 3}), do: raise("init failed")

    def handle_event(_, _, _, _), do: :keep_state_and_data
  end

  defmodule InitialEntry do
    use DurableServer, vsn: 1, behaviour: :gen_statem

    def callback_mode, do: [:handle_event_function, :state_enter]

    defdelegate dump_state(state), to: Machine
    defdelegate load_state(vsn, state), to: Machine
    defdelegate terminate(reason, name, data), to: Machine
    defdelegate after_terminate(result, info), to: Machine

    def init({name, data}, info) do
      data = Map.merge(data, %{observer: info.observer, actions: info.init_actions})
      __MODULE__.init({name, data})
    end

    def init({name, data}), do: {:ok, name, data, Map.get(data, :actions, [])}

    def handle_event(:enter, :idle, :idle, data) do
      send(data.observer, {:initial_entry, self(), data})

      receive do
        :raise -> raise "initial entry failed"
        {:return, result} -> result
      end
    end

    def handle_event(:enter, :idle, :running, data) do
      send(data.observer, {:entered, self(), :idle, :running})
      :keep_state_and_data
    end

    def handle_event(:internal, :advance, :idle, data),
      do: {:next_state, :running, data}

    def handle_event({:call, from}, :get, name, data),
      do: {:keep_state_and_data, [{:reply, from, {name, data.count}}]}
  end

  setup context do
    id = System.unique_integer([:positive])
    supervisor = :"statem_sup_#{id}"
    prefix = "statem_#{id}/"

    table_owner = start_supervised!({Agent, fn -> :ets.new(:statem_faults, [:public]) end})
    table = Agent.get(table_owner, & &1)

    start_supervised!(
      {DurableServer.Supervisor,
       name: supervisor,
       prefix: prefix,
       backend: {FaultBackend, store: test_object_store_opts(), table: table},
       init_info: %{
         observer: self(),
         auto_sync: context[:auto_sync] || false,
         permanent: context[:permanent] || false,
         init_actions: context[:init_actions] || []
       },
       initial_discovery_delay_ms: {60_000, 60_000},
       graceful_shutdown_timeout_ms: 1_000}
    )

    %{supervisor: supervisor, prefix: prefix, table: table}
  end

  defp start_machine(context, module \\ Machine, initial \\ {:idle, %{count: 0}}) do
    key = "machine-#{System.unique_integer([:positive])}"

    assert {:ok, {pid, _meta}} =
             DurableServer.Supervisor.start_child(
               context.supervisor,
               {module, key: key, initial_state: initial}
             )

    {pid, key}
  end

  defp stored!(context, key) do
    assert {:ok, stored} =
             DurableServer.fetch_stored_state(
               context.supervisor,
               %{key: key, prefix: context.prefix},
               consistent: true
             )

    stored
  end

  test "validates initial state against the callback behaviour on the destination node",
       context do
    for {module, initial_state, expected} <- [
          {GenServerCallbacks, {:idle, %{}}, "must be a map"},
          {Machine, %{}, "must be {state_name, data}"}
        ] do
      error =
        assert_raise ArgumentError, fn ->
          DurableServer.Supervisor.__start_child__(
            context.supervisor,
            {module, [key: "invalid-state", initial_state: initial_state], nil},
            max_placement_retries: 0
          )
        end

      assert error.message =~ expected
    end
  end

  test "retains compatibility with GenServer call and cast clients",
       context do
    {pid, _key} = start_machine(context)
    assert GenServer.call(pid, :get) == {:idle, 0}

    assert {:idle,
            %DurableServer{
              behaviour:
                {DurableServer.Behaviours.StateMachine,
                 %{mode: :handle_event_function, state_enter?: true}},
              user_state: {:idle, %{count: 0}}
            }} = :sys.get_state(pid)

    assert_receive {:entered, ^pid, :idle, :idle}
    :gen_statem.cast(pid, :increment)
    GenServer.cast(pid, :increment)
    assert :gen_statem.call(pid, :get) == {:idle, 2}
    assert GenServer.call(pid, :deferred_reply) == :deferred
    assert :gen_statem.call(pid, :deferred_reply) == :deferred
  end

  test "blocked lock acquisition does not block the supervisor from starting other children",
       context do
    key = "blocked-#{System.unique_integer([:positive])}"
    :ets.insert(context.table, {{:block_claim, context.prefix <> key}, self()})

    blocked =
      Task.async(fn ->
        DurableServer.Supervisor.start_child(
          context.supervisor,
          {Machine, key: key, initial_state: {:idle, %{count: 0}}}
        )
      end)

    assert_receive {:claim_blocked, pid}, 1_000

    try do
      fast = Task.async(fn -> start_machine(context) end)
      assert {fast_pid, _key} = Task.await(fast, 2_000)
      assert :gen_statem.call(fast_pid, :get) == {:idle, 0}
      assert Task.yield(blocked, 0) == nil
    after
      send(pid, :continue_claim)
    end

    assert {:ok, {^pid, _meta}} = Task.await(blocked, 2_000)
    assert :gen_statem.call(pid, :get) == {:idle, 0}
  end

  test "native parent shutdown terminates the state machine and persists its state", context do
    {pid, key} = start_machine(context)
    assert :gen_statem.call(pid, :increment) == :ok
    assert :proc_lib.translate_initial_call(pid) == {Machine, :init, 1}
    ref = Process.monitor(pid)
    supervisor = DurableServer.Supervisor.get_dynamic_supervisor(context.supervisor)

    assert :ok = DynamicSupervisor.terminate_child(supervisor, pid)
    assert_receive {:terminated, ^pid, :shutdown, :idle, 1}
    assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}
    assert stored!(context, key).meta.status == :stopped_graceful
    assert stored!(context, key).state == %{"name" => "idle", "count" => 1}
  end

  test "syncs the state name and data before replies and updates registry metadata", context do
    {pid, key} = start_machine(context)
    assert :gen_statem.call(pid, :start) == :ok
    assert stored!(context, key).state == %{"name" => "running", "count" => 1}
    assert {^pid, %{phase: :running}} = DurableServer.Supervisor.lookup(context.supervisor, key)
    assert_receive {:synced, ^pid, {:idle, %{count: 0}}, {:running, %{count: 1}}}
    assert_receive {:entered, ^pid, :idle, :running}
    assert :gen_statem.call(pid, :finish) == {:done, 2}
    assert stored!(context, key).state == %{"name" => "done", "count" => 2}
  end

  test "postpones events until a state change, not a data update or durable sync", context do
    {pid, _key} = start_machine(context)
    :gen_statem.cast(pid, :needs_running)
    assert_receive {:postponed, ^pid}
    assert :gen_statem.call(pid, :increment) == :ok
    send(pid, {:durable, :sync})
    assert :gen_statem.call(pid, :get) == {:idle, 1}
    refute_receive {:postponed, ^pid}, 20
    refute_receive {:processed, ^pid}, 20
    assert :gen_statem.call(pid, :start) == :ok
    assert_receive {:processed, ^pid}
  end

  test "native state timeouts cancel on transitions while named timeouts survive", context do
    {pid, _key} = start_machine(context)
    assert :gen_statem.call(pid, :arm_timeouts) == :ok
    assert :gen_statem.call(pid, :start) == :ok
    assert_receive {:expired, ^pid, {:timeout, :probe}, :running}, 1_000
    refute_receive {:expired, ^pid, :state_timeout, _}, 20
    assert :gen_statem.call(pid, :arm_timeouts) == :ok
    assert_receive {:expired, ^pid, :state_timeout, :running}, 1_000
  end

  test "repeat_state invokes state entry without changing the state name", context do
    {pid, _key} = start_machine(context)
    assert_receive {:entered, ^pid, :idle, :idle}
    assert :gen_statem.call(pid, :repeat) == :ok
    assert_receive {:entered, ^pid, :idle, :idle}
  end

  @tag auto_sync: true
  test "auto sync also applies to callbacks returning native actions", context do
    {pid, key} = start_machine(context)
    assert :gen_statem.call(pid, :increment) == :ok
    assert stored!(context, key).state == %{"name" => "idle", "count" => 1}
  end

  test "recovers persisted state through the existing supervisor API", context do
    {pid, key} = start_machine(context)
    assert :gen_statem.call(pid, :start) == :ok
    assert :ok = DurableServer.Supervisor.terminate_child(context.supervisor, pid)
    assert_receive {:terminated, ^pid, :normal, :running, 1}
    assert stored!(context, key).meta.status == :stopped_graceful

    assert {:ok, {restarted, _}} =
             DurableServer.Supervisor.ensure_started_child(
               context.supervisor,
               {Machine, key: key, initial_state: {:idle, %{count: -1}}}
             )

    assert :gen_statem.call(restarted, :get) == {:running, 1}
  end

  test "supports state_functions and non-map native data", context do
    {pid, key} = start_machine(context, StateFunctions, {:idle, 0})
    assert :gen_statem.call(pid, :start) == :ok
    assert :gen_statem.call(pid, :get) == 1

    assert {:running,
            %DurableServer{
              behaviour:
                {DurableServer.Behaviours.StateMachine,
                 %{mode: :state_functions, state_enter?: false}}
            }} = :sys.get_state(pid)

    assert stored!(context, key).state == %{"name" => "running", "count" => 1}
  end

  test "accepts GenServer and aliases as the behaviour option", context do
    for module <- [GenServerCallbacks, AliasedGenServerCallbacks] do
      {pid, _key} = start_machine(context, module, %{"count" => 0})
      assert DurableServer.__callback_behaviour__(module) == GenServer
      assert GenServer.call(pid, :get) == %{"count" => 0}

      assert %DurableServer{behaviour: {DurableServer.Behaviours.GenServer, %{}}} =
               :sys.get_state(pid)
    end
  end

  test "GenServer processes without a behaviour field still dispatch and upgrade", context do
    {pid, _key} = start_machine(context, GenServerCallbacks, %{"count" => 0})
    :sys.replace_state(pid, &Map.delete(&1, :behaviour))
    assert GenServer.call(pid, :get) == %{"count" => 0}
    :ok = :sys.suspend(pid)

    try do
      assert :ok = :sys.change_code(pid, GenServerCallbacks, 1, :upgrade)

      assert %DurableServer{behaviour: {DurableServer.Behaviours.GenServer, %{}}} =
               :sys.get_state(pid)
    after
      :ok = :sys.resume(pid)
    end

    assert GenServer.call(pid, :get) == %{"count" => 0}
  end

  test "native init refusals release the storage claim and return to the starter", context do
    for {count, expected} <- [{0, :ignore}, {1, {:error, :not_ready}}, {2, {:error, :not_ready}}] do
      key = "init-result-#{System.unique_integer([:positive])}"

      assert DurableServer.Supervisor.start_child(
               context.supervisor,
               {InitResult, key: key, initial_state: {:idle, count}}
             ) == expected

      assert stored!(context, key).meta.status == :stopped_graceful
      assert DurableServer.Supervisor.lookup(context.supervisor, key) == nil
    end
  end

  test "an exception during init returns the failure to the starter", context do
    key = "init-crash-#{System.unique_integer([:positive])}"

    assert {:error, {%RuntimeError{message: "init failed"}, _stacktrace}} =
             DurableServer.Supervisor.start_child(
               context.supervisor,
               {InitResult, key: key, initial_state: {:idle, 3}}
             )
  end

  test "initial entry failures invoke termination and persist crash history", context do
    {pid, key} = start_machine(context, InitialEntry)
    assert_receive {:initial_entry, ^pid, %{count: 0}}
    ref = Process.monitor(pid)
    send(pid, :raise)

    assert_receive {:terminated, ^pid, %RuntimeError{message: "initial entry failed"}, :idle, 0}

    assert_receive {:DOWN, ^ref, :process, ^pid, {%RuntimeError{}, _}}
    stored = stored!(context, key)
    assert stored.meta.status == :crashed
    assert [%{reason: reason, timestamp: timestamp}] = stored.meta.crash_history
    assert reason =~ "initial entry failed"
    assert is_integer(timestamp)
  end

  for kind <- [
        :repeat_state,
        :repeat_state_with_actions,
        :repeat_state_and_data,
        :repeat_state_and_data_with_actions
      ] do
    @tag repeat_kind: kind, init_actions: [{:next_event, :internal, :advance}]
    test "initial entry preserves the previous state through #{kind}", context do
      {pid, _key} = start_machine(context, InitialEntry)

      for _ <- 1..2 do
        assert_receive {:initial_entry, ^pid, data}

        result =
          case context.repeat_kind do
            :repeat_state -> {:repeat_state, %{data | count: data.count + 1}}
            :repeat_state_with_actions -> {:repeat_state, %{data | count: data.count + 1}, []}
            :repeat_state_and_data -> :repeat_state_and_data
            :repeat_state_and_data_with_actions -> {:repeat_state_and_data, []}
          end

        send(pid, {:return, result})
      end

      assert_receive {:initial_entry, ^pid, data}
      send(pid, {:return, :keep_state_and_data})
      assert_receive {:entered, ^pid, :idle, :running}
      assert :gen_statem.call(pid, :get) == {:running, data.count}
    end
  end

  for action <- [:postpone, {:postpone, true}, {:postpone, false}] do
    @tag init_actions: [action, {:next_event, :internal, :advance}]
    test "init ignores #{inspect(action)} without replaying the bootstrap event", context do
      {pid, _key} = start_machine(context, InitialEntry)

      assert_receive {:initial_entry, ^pid, _data}
      send(pid, {:return, :keep_state_and_data})
      assert_receive {:entered, ^pid, :idle, :running}
      assert :gen_statem.call(pid, :get) == {:running, 0}
    end
  end

  @tag permanent: true
  test "the lifecycle manager restarts a native state machine from its persisted state",
       context do
    {pid, key} = start_machine(context)
    assert :gen_statem.call(pid, :start) == :ok
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

    # Make the orphan eligible immediately without waiting for a real dead
    # node's heartbeat timeout. Recovery still takes the normal claim path.
    stored = stored!(context, key)

    meta = %{
      stored.meta
      | status: :crashed,
        node_str: "statem-dead@host",
        node_ref: "dead",
        last_heartbeat_at: System.system_time(:millisecond) - 600_000
    }

    %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(context.supervisor)

    assert {:ok, _} =
             DurableServer.StorageBackend.put_object(backend, context.prefix <> key, %{
               stored
               | meta: meta
             })

    send(DurableServer.LifecycleManager.name(context.supervisor), :discover_and_restart)
    assert_receive {:entered, restarted, :running, :running} when restarted != pid, 5_000
    assert :gen_statem.call(restarted, :get) == {:running, 1}
    assert {:running, %DurableServer{}} = :sys.get_state(restarted)
    assert stored!(context, key).meta.status == :running
  end

  test "hot upgrades invoke code_change/4 and retain durable state and metadata", context do
    {pid, key} = start_machine(context)
    :ok = :sys.suspend(pid)

    try do
      assert :ok = :sys.change_code(pid, Machine, 1, :upgrade)

      assert {:idle,
              %DurableServer{
                behaviour:
                  {DurableServer.Behaviours.StateMachine,
                   %{mode: :handle_event_function, state_enter?: true}},
                user_state: {:idle, %{count: 10}},
                vsn: 1
              }} =
               :sys.get_state(pid)
    after
      :ok = :sys.resume(pid)
    end

    send(pid, {:durable, :sync})
    assert :gen_statem.call(pid, :get) == {:idle, 10}
    assert stored!(context, key).state == %{"name" => "idle", "count" => 10}
  end

  test "schema-version migrations update the native state name before dispatch", context do
    {pid, key} = start_machine(context)
    :sys.replace_state(pid, fn {name, durable} -> {name, %{durable | vsn: 0}} end)
    assert :gen_statem.call(pid, :get) == {:running, 100}
    assert {:running, %{vsn: 1, old_vsn: 0}} = :sys.get_state(pid)
    send(pid, {:durable, :sync})
    assert :gen_statem.call(pid, :get) == {:running, 100}
    assert stored!(context, key).state == %{"name" => "running", "count" => 100}
  end

  test "rejected hot upgrades preserve state for both behaviours", context do
    for {module, api, initial_state, reply} <- [
          {GenServerCallbacks, GenServer, %{"count" => 3}, %{"count" => 3}},
          {Machine, :gen_statem, {:idle, %{count: 3}}, {:idle, 3}}
        ] do
      {pid, _key} = start_machine(context, module, initial_state)
      assert api.call(pid, :get) == reply
      :ok = :sys.suspend(pid)

      try do
        before = :sys.get_state(pid)

        assert {:error, {:error, :upgrade_rejected}} =
                 :sys.change_code(pid, module, 1, :reject)

        assert :sys.get_state(pid) == before
      after
        :ok = :sys.resume(pid)
      end

      assert api.call(pid, :get) == reply
    end
  end

  test "native thrown callback returns still pass through durability", context do
    {pid, key} = start_machine(context)
    assert :gen_statem.call(pid, :throw_transition) == :ok
    assert stored!(context, key).state == %{"name" => "running", "count" => 1}
    assert {:running, %DurableServer{}} = :sys.get_state(pid)
  end

  test "an explicit sync failure cannot acknowledge or execute inserted events", context do
    {pid, key} = start_machine(context)
    :ets.insert(context.table, {:fail_key, context.prefix <> key})
    ref = Process.monitor(pid)
    assert catch_exit(:gen_statem.call(pid, :finish))

    assert_receive {:DOWN, ^ref, :process, ^pid,
                    {:shutdown, {:durable, {:fatal_exit, {:sync_failed, :unavailable}}}}},
                   1_000

    refute_receive {:committed_event, ^pid, _}, 20
    assert stored!(context, key).state == %{"name" => "idle", "count" => 0}
  end

  test "supervisor stop waits for user termination and final persistence", context do
    {pid, key} = start_machine(context)
    assert :gen_statem.call(pid, :increment) == :ok
    assert :gen_statem.call(pid, :hold_terminate) == :ok
    task = Task.async(fn -> DurableServer.Supervisor.terminate_child(context.supervisor, pid) end)
    assert_receive {:terminate_waiting, ^pid}
    assert Task.yield(task, 20) == nil
    assert stored!(context, key).meta.status == :running
    send(pid, :finish_terminate)
    assert Task.await(task) == :ok
    assert_receive {:after_terminate, ^pid, :finished, :stopped_graceful}
    assert stored!(context, key).meta.status == :stopped_graceful
    assert stored!(context, key).state == %{"name" => "idle", "count" => 1}
  end

  test "native stop_and_reply replies before termination while still persisting final state",
       context do
    {pid, key} = start_machine(context)
    assert :gen_statem.call(pid, :increment) == :ok
    assert :gen_statem.call(pid, :hold_terminate) == :ok
    ref = Process.monitor(pid)

    assert :gen_statem.call(pid, {:stop, :normal}) == :bye
    assert_receive {:terminate_waiting, ^pid}
    assert stored!(context, key).meta.status == :running
    send(pid, :finish_terminate)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    assert_receive {:after_terminate, ^pid, :finished, :stopped_graceful}
    assert stored!(context, key).state == %{"name" => "idle", "count" => 1}
  end

  test "supervisor cordon persists final state and blocks a subsequent start", context do
    {pid, key} = start_machine(context)
    assert :gen_statem.call(pid, :increment) == :ok

    assert :ok = DurableServer.Supervisor.terminate_and_cordon_child(context.supervisor, pid)
    assert stored!(context, key).meta.status == :cordoned
    assert stored!(context, key).state == %{"name" => "idle", "count" => 1}

    assert {:error, :cordoned} =
             DurableServer.Supervisor.start_child(
               context.supervisor,
               {Machine, key: key, initial_state: {:idle, %{count: 0}}}
             )
  end

  test "OTP shutdown drains a native child and persists its final state", context do
    {pid, key} = start_machine(context)
    assert :gen_statem.call(pid, :increment) == :ok
    ref = Process.monitor(pid)
    %{storage_backend: backend} = DurableServer.Supervisor.__get_config__(context.supervisor)

    assert :ok = stop_supervised(context.supervisor)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    assert_receive {:terminated, ^pid, :normal, :idle, 1}
    assert_receive {:after_terminate, ^pid, :finished, :stopped_graceful}

    assert {:ok, stored} =
             DurableServer.fetch_stored_state(
               backend,
               %{key: key, prefix: context.prefix, supervisor: context.supervisor},
               consistent: true
             )

    assert stored.meta.status == :stopped_graceful
    assert stored.state == %{"name" => "idle", "count" => 1}
  end

  test "callback-module switching cannot bypass persistence", context do
    for action <- [
          {:change_callback_module, StateFunctions},
          {:push_callback_module, StateFunctions},
          :pop_callback_module
        ] do
      {pid, _key} = start_machine(context)
      assert catch_exit(:gen_statem.call(pid, {:switch_callback, action}))
    end
  end

  test "switching the process engine requires a restart" do
    old_genserver = %DurableServer{module: Machine, user_state: %{count: 0}}

    assert {:error, :behaviour_change_requires_restart} =
             DurableServer.code_change(1, old_genserver, [])

    old_statem = %DurableServer{
      module: GenServerCallbacks,
      user_state: {:idle, %{count: 0}},
      behaviour:
        {DurableServer.Behaviours.StateMachine,
         %{mode: :handle_event_function, state_enter?: true}}
    }

    assert {:error, :behaviour_change_requires_restart} =
             DurableServer.code_change(1, old_statem, [])
  end

  test "stop_and_reply retains durable deletion semantics", context do
    {pid, key} = start_machine(context)
    ref = Process.monitor(pid)
    assert :gen_statem.call(pid, {:stop, :delete}) == :bye
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 2_000
    assert_receive {:terminated, ^pid, :normal, :idle, 0}

    assert {:error, :not_found} =
             DurableServer.fetch_stored_state(
               context.supervisor,
               %{key: key, prefix: context.prefix},
               consistent: true
             )
  end
end
