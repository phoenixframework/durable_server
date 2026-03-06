defmodule DurableServer.TestCounterServer do
  use DurableServer, vsn: 1

  def dump_state(state), do: %{count: state.count}

  def load_state(_old_vsn, persisted_state) when is_map(persisted_state) do
    count = Map.get(persisted_state, :count, Map.get(persisted_state, "count", 0))
    %{count: count}
  end

  def init(%{count: count} = state) when is_integer(count) do
    init_result(count, state)
  end

  def init(state) when is_map(state) do
    init_result(0, state)
  end

  def init(_state), do: {:ok, %{count: 0}}

  defp init_result(count, %{permanent: true}) do
    {:ok, %{count: count}, permanent: true}
  end

  defp init_result(count, _state) do
    {:ok, %{count: count}}
  end

  def handle_call(:get_count, _from, %{count: count} = state) do
    {:reply, count, state}
  end

  def handle_call(:increment_and_sync, _from, %{count: count} = state) do
    new_state = %{state | count: count + 1}
    {:reply, new_state.count, new_state, :sync}
  end
end
