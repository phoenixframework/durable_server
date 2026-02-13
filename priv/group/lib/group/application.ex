defmodule Group.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    Application.put_env(:syn, :event_handler, Group.SynEventHandler)
    # syn uses :erlang.function_exported/3 to dispatch callbacks, which
    # requires the module to be loaded. syn's ensure_event_handler_loaded()
    # runs at syn start (before this env is set), so we must load it here.
    {:module, _} = Code.ensure_loaded(Group.SynEventHandler)
    Supervisor.start_link([], strategy: :one_for_one, name: Group.Supervisor)
  end
end
