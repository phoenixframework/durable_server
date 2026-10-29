defmodule DurableServer.Behaviours.GenServer do
  @moduledoc false

  def defaults do
    quote do
      @behaviour DurableServer
      # Default implementations
      def init(state) do
        {:ok, state}
      end

      def handle_call(_request, _from, state) do
        {:reply, :ok, state}
      end

      def handle_cast(_request, state) do
        {:noreply, state}
      end

      def handle_info(_msg, state) do
        {:noreply, state}
      end

      def handle_continue(_continue, state) do
        {:noreply, state}
      end

      def handle_sync(_info, _old_state, new_state) do
        {:ok, new_state}
      end

      def terminate(_reason, _state) do
        :ok
      end

      def after_terminate(_terminate_return, _info) do
        :ok
      end

      # Default code_change for hot upgrades: no migration needed
      # Override this to provide version-specific state migrations:
      #
      #   def code_change(1, old_state, 2) do
      #     # Migrate from v1 to v2
      #     {:ok, Map.put(old_state, :new_field, "default")}
      #   end
      #
      #   def code_change(_, state, _) do
      #     # No migration for other versions
      #     {:ok, state}
      #   end
      def code_change(_old_vsn, state, _new_vsn) do
        {:ok, state}
      end

      defoverridable init: 1,
                     handle_call: 3,
                     handle_cast: 2,
                     handle_info: 2,
                     handle_continue: 2,
                     handle_sync: 3,
                     terminate: 2,
                     after_terminate: 2,
                     code_change: 3
    end
  end

  def start_link(info, opts) do
    GenServer.start_link(DurableServer, info, opts)
  end

  def init_callback(args, durable) do
    {durable, apply(durable.module, :init, args)}
  end

  def dispatch({:call, from}, request, %{module: module, user_state: user_state} = durable) do
    result = module.handle_call(request, from, user_state)
    DurableServer.__process_callback_result__(result, durable)
  end

  def dispatch(:cast, request, %{module: module, user_state: user_state} = durable) do
    result = module.handle_cast(request, user_state)
    DurableServer.__process_callback_result__(result, durable)
  end

  def dispatch(:info, message, %{module: module, user_state: user_state} = durable) do
    result = module.handle_info(message, user_state)
    DurableServer.__process_callback_result__(result, durable)
  end

  def dispatch(:continue, continue, %{module: module, user_state: user_state} = durable) do
    result = module.handle_continue(continue, user_state)
    DurableServer.__process_callback_result__(result, durable)
  end

  def terminate(reason, durable) do
    durable.module.terminate(reason, durable.user_state)
  end

  def code_change(old_vsn, durable, extra) do
    case durable.module.code_change(old_vsn, durable.user_state, extra) do
      {:ok, user_state} -> {:ok, user_state, %{}}
      {:error, _reason} = error -> error
    end
  end

  def validate_user_state!(%{} = user_state), do: user_state

  def validate_user_state!(user_state) do
    raise ArgumentError,
          "expected callback to return a map of user state, got: #{inspect(user_state)}"
  end
end
