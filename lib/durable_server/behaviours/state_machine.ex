defmodule DurableServer.Behaviours.StateMachine do
  @moduledoc false

  @behaviour :gen_statem

  @type user_state :: {:gen_statem.state(), :gen_statem.data()}
  @callback dump_state(user_state()) :: map()
  @callback load_state(pos_integer() | nil, map()) :: user_state()
  @callback init(user_state(), map()) :: :gen_statem.init_result(:gen_statem.state())
  @callback handle_sync(map(), user_state(), user_state()) :: {:ok, user_state()}
  @callback after_terminate(term(), map()) :: term()
  @optional_callbacks init: 2, handle_sync: 3, after_terminate: 2

  @doc false
  def defaults do
    quote do
      @behaviour :gen_statem
      @behaviour DurableServer.Behaviours.StateMachine

      def init({name, data}), do: {:ok, name, data}
      def handle_sync(_info, _old_state, new_state), do: {:ok, new_state}
      def terminate(_reason, _name, _data), do: :ok
      def after_terminate(_return, _info), do: :ok
      def code_change(_old_vsn, name, data, _extra), do: {:ok, name, data}

      defoverridable init: 1,
                     handle_sync: 3,
                     terminate: 3,
                     after_terminate: 2,
                     code_change: 4
    end
  end

  @doc false
  def start_link(info, opts) do
    :proc_lib.start_link(__MODULE__, :init, [{info, opts}])
  end

  @impl true
  def callback_mode, do: [:handle_event_function, :state_enter]

  @impl true
  def init({info, opts}) do
    {:ok, durable, {:continue, bootstrap}} = DurableServer.init(info)
    # Acknowledge before lock acquisition so storage cannot block other starts.
    # DurableServer separately notifies the caller when bootstrap completes.
    :proc_lib.init_ack({:ok, self()})

    case DurableServer.handle_continue(bootstrap, durable) do
      {:noreply, initialized, {:statem, actions}} ->
        {name, _data} = initialized.user_state
        :gen_statem.enter_loop(__MODULE__, opts, name, initialized, self(), actions)

      {:stop, reason, stopped} ->
        DurableServer.terminate(reason, stopped)
        exit(reason)
    end
  end

  @impl true
  def handle_event(:enter, previous, name, durable) do
    {__MODULE__, adapter_state} = durable.behaviour

    if adapter_state.state_enter? do
      durable_event(:__dispatch__, [:enter, previous, durable], name)
    else
      {:keep_state, durable}
    end
  end

  def handle_event({:call, from}, request, name, durable) do
    durable_event(:handle_call, [request, from, durable], name, from)
  end

  def handle_event(:cast, request, name, durable) do
    durable_event(:handle_cast, [request, durable], name)
  end

  def handle_event(:info, message, name, durable) do
    durable_event(:handle_info, [message, durable], name)
  end

  def handle_event(type, content, name, durable) do
    durable_event(:__dispatch__, [type, content, durable], name)
  end

  @impl true
  def terminate(reason, _name, durable) do
    result = DurableServer.terminate(reason, durable)
    {__MODULE__, adapter_state} = durable.behaviour

    # DurableServer's supervisor control calls acknowledge only after the
    # terminate callback and final storage write. Native stop_and_reply sends
    # replies before terminate, so it cannot implement these control calls.
    if reply = adapter_state[:stop_reply] do
      {from, response} = reply
      :gen_statem.reply(from, response)
    end

    result
  end

  @impl true
  def code_change(old_vsn, _name, durable, extra) do
    case DurableServer.code_change(old_vsn, durable, extra) do
      {:ok, updated} ->
        {name, _data} = updated.user_state
        {:ok, name, updated}

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  def init_callback(args, durable) do
    init_result(durable, invoke_callback(durable.module, :init, args))
  end

  @doc false
  def terminate(reason, durable) do
    {name, data} = durable.user_state
    invoke_callback(durable.module, :terminate, [reason, name, data])
  end

  @doc false
  def code_change(old_vsn, durable, extra) do
    {name, data} = durable.user_state
    {__MODULE__, adapter_state} = durable.behaviour

    case invoke_callback(durable.module, :code_change, [old_vsn, name, data, extra]) do
      {:ok, name, data} ->
        adapter_state = Map.merge(adapter_state, callback_config(durable.module))
        {:ok, {name, data}, adapter_state}

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  def validate_user_state!({_name, _data} = user_state), do: user_state

  def validate_user_state!(user_state) do
    raise ArgumentError,
          "expected state-machine user state to be {state_name, data}, got: #{inspect(user_state)}"
  end

  defp init_result(durable, result) do
    case result do
      {:ok, name, data} ->
        init_result(durable, {:ok, name, data, []})

      {:ok, name, data, actions} ->
        {actions, opts} = split_actions(actions)
        durable = %{durable | behaviour: {__MODULE__, callback_config(durable.module)}}
        validate_state!(durable, name)
        {durable, {:ok, {name, data}, {:statem, actions}, opts}}

      result when result == :ignore ->
        {durable, result}

      {tag, reason} when tag in [:stop, :error] ->
        {durable, {:statem_init_stop, reason}}

      other ->
        {durable, {:bad_statem_init_return, other}}
    end
  end

  defp callback_config(module) do
    modes = actions_list(invoke_callback(module, :callback_mode, []))
    {enters, modes} = Enum.split_with(modes, &(&1 == :state_enter))

    case modes do
      [mode] when mode in [:handle_event_function, :state_functions] ->
        %{mode: mode, state_enter?: enters != []}

      _ ->
        raise ArgumentError,
              "invalid #{inspect(module)}.callback_mode/0: #{inspect(modes ++ enters)}"
    end
  end

  @doc false
  def dispatch(type, content, durable) do
    {name, data} = durable.user_state
    {__MODULE__, adapter_state} = durable.behaviour
    validate_state!(durable, name)

    result =
      case adapter_state.mode do
        :handle_event_function ->
          invoke_callback(durable.module, :handle_event, [type, content, name, data])

        :state_functions ->
          invoke_callback(durable.module, name, [type, content, data])
      end

    callback_result(result, durable)
  end

  # OTP accepts thrown values from native callbacks as ordinary returns. Catch
  # them before returning to OTP, or they could skip durability and replace the
  # wrapper's data with the application's data.
  defp invoke_callback(module, function, args) do
    apply(module, function, args)
  catch
    :throw, result -> result
  end

  defp durable_event(function, args, name, from \\ nil) do
    apply(DurableServer, function, args) |> native_result(name, from)
  catch
    # A throw from a storage or durability callback is not a native return.
    :throw, value -> :erlang.raise(:error, {:nocatch, value}, __STACKTRACE__)
  end

  defp callback_result({:next_state, name, data}, durable),
    do: transition(:next_state, name, data, [], durable)

  defp callback_result({:next_state, name, data, actions}, durable),
    do: transition(:next_state, name, data, actions, durable)

  defp callback_result({kind, data}, durable) when kind in [:keep_state, :repeat_state],
    do: callback_result({kind, data, []}, durable)

  defp callback_result({kind, data, actions}, durable)
       when kind in [:keep_state, :repeat_state] do
    {name, _data} = durable.user_state
    transition(kind, name, data, actions, durable)
  end

  defp callback_result(kind, durable) when kind in [:keep_state_and_data, :repeat_state_and_data],
    do: callback_result({kind, []}, durable)

  defp callback_result({kind, actions}, durable)
       when kind in [:keep_state_and_data, :repeat_state_and_data] do
    {name, data} = durable.user_state
    kind = if kind == :repeat_state_and_data, do: :repeat_state, else: :keep_state
    transition(kind, name, data, actions, durable)
  end

  defp callback_result(:stop, durable), do: callback_result({:stop, :normal}, durable)

  defp callback_result({:stop, reason}, durable) do
    {_name, data} = durable.user_state
    callback_result({:stop, reason, data}, durable)
  end

  defp callback_result({:stop, reason, data}, durable) do
    {name, _data} = durable.user_state
    DurableServer.__statem_stop__(reason, {name, data}, durable)
  end

  defp callback_result({:stop_and_reply, reason, replies}, durable) do
    {_name, data} = durable.user_state
    callback_result({:stop_and_reply, reason, replies, data}, durable)
  end

  defp callback_result({:stop_and_reply, reason, replies, data}, durable) do
    {name, _data} = durable.user_state
    {:stop, reason, durable} = DurableServer.__statem_stop__(reason, {name, data}, durable)
    {:statem_stop_and_reply, reason, replies, durable}
  end

  defp callback_result(other, _durable),
    do: raise(ArgumentError, "invalid state-machine callback return: #{inspect(other)}")

  defp transition(kind, name, data, actions, durable) do
    validate_state!(durable, name)
    {actions, opts} = split_actions(actions)
    {:noreply, durable} = DurableServer.__statem_transition__({name, data}, opts, durable)
    {:noreply, durable, {:statem, kind, actions}}
  end

  defp split_actions(actions) do
    {native, opts} =
      Enum.reduce(actions_list(actions), {[], []}, fn
        {:durable, options}, {native, opts} when is_list(options) ->
          {native, Keyword.merge(opts, options)}

        action, _acc
        when action == :pop_callback_module or
               (is_tuple(action) and tuple_size(action) == 2 and
                  elem(action, 0) in [:change_callback_module, :push_callback_module]) ->
          raise ArgumentError,
                "callback-module switching bypasses DurableServer: #{inspect(action)}"

        action, {native, opts} ->
          {[action | native], opts}
      end)

    {Enum.reverse(native), opts}
  end

  defp actions_list(actions) when is_list(actions), do: actions
  defp actions_list(action), do: [action]

  defp validate_state!(%{behaviour: {__MODULE__, %{mode: :state_functions}}}, name)
       when not is_atom(name),
       do: raise(ArgumentError, "state_functions requires an atom state, got: #{inspect(name)}")

  defp validate_state!(_durable, _name), do: :ok

  defp native_result({:noreply, durable}, _previous, _from) do
    {name, _data} = durable.user_state
    {:next_state, name, durable}
  end

  defp native_result({:noreply, durable, {:statem, kind, actions}}, previous, _from) do
    {name, _data} = durable.user_state

    if kind == :repeat_state and name === previous,
      do: {:repeat_state, durable, actions},
      else: {:next_state, name, durable, actions}
  end

  defp native_result({:reply, response, durable}, _previous, from) do
    {name, _data} = durable.user_state
    {:next_state, name, durable, [{:reply, from, response}]}
  end

  defp native_result({:stop, reason, response, durable}, _previous, from) do
    {__MODULE__, adapter_state} = durable.behaviour
    adapter_state = Map.put(adapter_state, :stop_reply, {from, response})
    durable = %{durable | behaviour: {__MODULE__, adapter_state}}
    {:stop, reason, durable}
  end

  defp native_result({:stop, reason, durable}, _previous, _from),
    do: {:stop, reason, durable}

  defp native_result({:statem_stop_and_reply, reason, replies, durable}, _previous, _from),
    do: {:stop_and_reply, reason, replies, durable}
end
