defmodule DurableServer.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    # TODO: Extract Group into its own library (group/ path dep) with its own Application.
    # These two children (syn event_handler config + Group.Registry) belong there.
    Application.put_env(:syn, :event_handler, Group.SynEventHandler)

    children = [
      {Finch, name: DurableServer.Finch},
      {Registry, keys: :duplicate, name: Group.Registry},
      {Task.Supervisor, name: DurableServer.TaskSupervisor}
    ]

    opts = [strategy: :one_for_one, name: DurableServer.AppSupervisor]
    Supervisor.start_link(children, opts)
  end
end
