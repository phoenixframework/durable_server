defmodule Group.Replica.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: :"#{name}_replica_sup")
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    num_shards = Keyword.fetch!(opts, :num_shards)

    children =
      for i <- 0..(num_shards - 1) do
        %{
          id: {Group.Replica, i},
          start:
            {Group.Replica, :start_link, [[name: name, shard_index: i, num_shards: num_shards]]}
        }
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
