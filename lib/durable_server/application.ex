defmodule DurableServer.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    # Configure syn event handler for DurableServer conflict resolution and pubsub
    Application.put_env(:syn, :event_handler, DurableServer.SynEventHandler)

    children = [
      {Finch, name: DurableServer.Finch},
      {Task.Supervisor, name: DurableServer.TaskSupervisor},
      {Registry, keys: :duplicate, name: DurableServer.PubSub.Registry}
    ]

    opts = [strategy: :one_for_one, name: DurableServer.AppSupervisor]
    Supervisor.start_link(children, opts)
  end
end
