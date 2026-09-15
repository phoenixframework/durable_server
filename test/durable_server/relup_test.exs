defmodule DurableServer.RelupTest do
  use ExUnit.Case, async: true

  defmodule CallbackServer do
    use DurableServer, vsn: 1

    @impl true
    def dump_state(state), do: state

    @impl true
    def load_state(_old_vsn, state), do: state
  end

  test "child specs identify the wrapper and callback modules used by the process" do
    init_arg = %{
      module: CallbackServer,
      init_from: {make_ref(), self()},
      init_arg: [],
      boot_info: %{},
      supervisor_name: __MODULE__,
      config: %{}
    }

    assert %{modules: modules} = DurableServer.child_spec(init_arg)
    assert DurableServer in modules
    assert CallbackServer in modules
  end
end
