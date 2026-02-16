defmodule Group.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: :"#{name}_group_sup")
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    num_shards = Keyword.get(opts, :shards, 8)
    callbacks = Keyword.get(opts, :callbacks, %{})
    extract_meta = Keyword.get(opts, :extract_meta)
    resolve_registry_conflict = Keyword.get(opts, :resolve_registry_conflict)
    log = Keyword.get(opts, :log, :info)

    # persistent_term config — must be set before children start (Replica reads it)
    config = %{callbacks: callbacks, num_shards: num_shards, log: log}
    config = if extract_meta, do: Map.put(config, :extract_meta, extract_meta), else: config

    config =
      if resolve_registry_conflict,
        do: Map.put(config, :resolve_registry_conflict, resolve_registry_conflict),
        else: config

    :persistent_term.put({Group, name}, config)

    children = [
      {Group.Replica.Data, name: name, num_shards: num_shards},
      {Group.Replica.Supervisor, name: name, num_shards: num_shards},
      {Registry, keys: :duplicate, name: Group.registry_name(name)}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
