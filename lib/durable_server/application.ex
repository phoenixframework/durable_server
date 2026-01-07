defmodule DurableServer.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Finch, name: DurableServer.Finch},
      {Task.Supervisor, name: DurableServer.TaskSupervisor}
    ]

    opts = [strategy: :one_for_one, name: DurableServer.AppSupervisor]
    Supervisor.start_link(children, opts)
  end
end
