defmodule DurableServer.TestTemporalServer do
  use DurableServer, vsn: 1

  def dump_state(state) do
    Map.take(state, [:occurred_at, :nested])
  end

  def load_state(_old_vsn, %{
        "occurred_at" => occurred_at,
        "nested" => %{"occurred_at" => nested_occurred_at} = nested
      })
      when is_binary(occurred_at) and is_binary(nested_occurred_at) do
    %{
      occurred_at: occurred_at,
      nested: nested,
      loaded_shape: :json_string
    }
  end

  def load_state(_old_vsn, %{
        occurred_at: %DateTime{} = occurred_at,
        nested: %{occurred_at: %DateTime{} = _nested_occurred_at} = nested
      }) do
    %{
      occurred_at: occurred_at,
      nested: nested,
      loaded_shape: :native_term
    }
  end

  def load_state(_old_vsn, persisted_state) do
    %{
      persisted_state: persisted_state,
      loaded_shape: :other
    }
  end

  def init(init_state) when is_map(init_state) do
    {:ok, Map.put_new(init_state, :loaded_shape, :fresh)}
  end

  def handle_call(:sync_now, _from, state) do
    {:reply, :ok, state, :sync}
  end

  def handle_call(:get_loaded_shape, _from, state) do
    {:reply, state.loaded_shape, state}
  end

  def handle_call(:get_snapshot, _from, state) do
    {:reply, state, state}
  end
end
