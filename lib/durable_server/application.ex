defmodule DurableServer.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    # Configure syn event handler for DurableServer conflict resolution and pubsub
    Application.put_env(:syn, :event_handler, DurableServer.SynEventHandler)

    children = [
      {Finch, name: DurableServer.Finch},
      {Registry, keys: :duplicate, name: DurableServer.Cluster.Registry},
      {Task.Supervisor, name: DurableServer.TaskSupervisor}
    ]

    opts = [strategy: :one_for_one, name: DurableServer.AppSupervisor]
    Supervisor.start_link(children, opts)
  end
end
