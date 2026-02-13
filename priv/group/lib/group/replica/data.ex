defmodule Group.Replica.Data do
  @moduledoc false

  use GenServer

  # GenServer that owns ETS tables for all shards.
  # Survives Replica shard crashes via rest_for_one supervisor strategy.
  # Provides a pure function API for all ETS operations.

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    num_shards = Keyword.fetch!(opts, :num_shards)
    GenServer.start_link(__MODULE__, {name, num_shards}, name: data_name(name))
  end

  def data_name(name), do: :"#{name}_data"

  # =====================================================================
  # Registry operations
  # =====================================================================

  def registry_insert(name, shard, cluster, key, pid, meta, time, mref, node) do
    table = reg_by_key_table(name, shard)
    :ets.insert(table, {{cluster, key}, pid, meta, time, mref, node})
    table_pid = reg_by_pid_table(name, shard)
    :ets.insert(table_pid, {pid, cluster, key, meta, time, mref, node})
    :ok
  end

  def registry_delete(name, shard, cluster, key) do
    table = reg_by_key_table(name, shard)
    :ets.delete(table, {cluster, key})
    table_pid = reg_by_pid_table(name, shard)
    # Delete matching entries from by_pid table
    :ets.match_delete(table_pid, {:_, cluster, key, :_, :_, :_, :_})
    :ok
  end

  def registry_lookup(name, shard, cluster, key) do
    table = reg_by_key_table(name, shard)

    case :ets.lookup(table, {cluster, key}) do
      [{{^cluster, ^key}, pid, meta, time, mref, node}] ->
        {pid, meta, time, mref, node}

      [] ->
        nil
    end
  end

  def registry_lookup_by_pid(name, shard, pid) do
    table = reg_by_pid_table(name, shard)

    :ets.lookup(table, pid)
    |> Enum.map(fn {^pid, cluster, key, meta, time, mref, node} ->
      {cluster, key, meta, time, mref, node}
    end)
  end

  # =====================================================================
  # Process group operations
  # =====================================================================

  def pg_insert(name, shard, cluster, key, pid, meta, time, mref, node) do
    table = pg_by_key_table(name, shard)
    :ets.insert(table, {{cluster, key, pid}, meta, time, mref, node})
    table_pid = pg_by_pid_table(name, shard)
    :ets.insert(table_pid, {{pid, cluster, key}, meta, time, mref, node})
    :ok
  end

  def pg_delete(name, shard, cluster, key, pid) do
    table = pg_by_key_table(name, shard)
    :ets.delete(table, {cluster, key, pid})
    table_pid = pg_by_pid_table(name, shard)
    :ets.delete(table_pid, {pid, cluster, key})
    :ok
  end

  def pg_lookup(name, shard, cluster, key, pid) do
    table = pg_by_key_table(name, shard)

    case :ets.lookup(table, {cluster, key, pid}) do
      [{{^cluster, ^key, ^pid}, meta, time, mref, node}] ->
        {meta, time, mref, node}

      [] ->
        nil
    end
  end

  def pg_members(name, shard, cluster, key) do
    table = pg_by_key_table(name, shard)
    # Use match spec to find all entries with the given {cluster, key, _pid} prefix
    match_spec = [
      {{{cluster, key, :"$1"}, :"$2", :_, :_, :_}, [], [{{:"$1", :"$2"}}]}
    ]

    :ets.select(table, match_spec)
  end

  # =====================================================================
  # Monitor helpers (per-shard, no cross-shard coordination)
  # =====================================================================

  def find_monitor_for_pid(name, shard, pid) do
    # Check by_pid registry table first (it's a bag keyed by pid)
    table_reg = reg_by_pid_table(name, shard)

    case :ets.select(table_reg, [{{pid, :_, :_, :_, :_, :"$1", :_}, [], [:"$1"]}], 1) do
      {[mref], _} ->
        mref

      :"$end_of_table" ->
        # Check by_pid pg table (ordered_set keyed by {pid, cluster, key})
        table_pg = pg_by_pid_table(name, shard)

        case :ets.select(table_pg, [
               {{{pid, :_, :_}, :_, :_, :"$1", :_}, [], [:"$1"]}
             ], 1) do
          {[mref], _} -> mref
          :"$end_of_table" -> nil
        end
    end
  end

  def maybe_demonitor(name, shard, pid) do
    # Count remaining entries for this pid across both tables in this shard
    reg_count = length(:ets.lookup(reg_by_pid_table(name, shard), pid))

    pg_count =
      if reg_count > 0 do
        # Still has entries, don't demonitor
        1
      else
        table_pg = pg_by_pid_table(name, shard)

        case :ets.select(table_pg, [{{{pid, :_, :_}, :_, :_, :_, :_}, [], [true]}], 1) do
          {[true], _} -> 1
          :"$end_of_table" -> 0
        end
      end

    if reg_count == 0 and pg_count == 0 do
      :ok
    else
      :still_monitored
    end
  end

  # =====================================================================
  # Bulk operations
  # =====================================================================

  def entries_by_pid(name, shard, pid) do
    reg_entries =
      :ets.lookup(reg_by_pid_table(name, shard), pid)
      |> Enum.map(fn {^pid, cluster, key, meta, time, mref, node} ->
        {:registry, cluster, key, pid, meta, time, mref, node}
      end)

    pg_table = pg_by_pid_table(name, shard)

    pg_entries =
      :ets.select(pg_table, [
        {{{pid, :"$1", :"$2"}, :"$3", :"$4", :"$5", :"$6"}, [],
         [{{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6"}}]}
      ])
      |> Enum.map(fn {cluster, key, meta, time, mref, node} ->
        {:pg, cluster, key, pid, meta, time, mref, node}
      end)

    reg_entries ++ pg_entries
  end

  def local_data(name, shard, cluster) do
    local_node = node()

    reg_table = reg_by_key_table(name, shard)

    reg_entries =
      :ets.select(reg_table, [
        {{{cluster, :"$1"}, :"$2", :"$3", :"$4", :_, :"$5"},
         [{:==, :"$5", local_node}], [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])

    pg_table = pg_by_key_table(name, shard)

    pg_entries =
      :ets.select(pg_table, [
        {{{cluster, :"$1", :"$2"}, :"$3", :"$4", :_, :"$5"},
         [{:==, :"$5", local_node}], [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])

    {reg_entries, pg_entries}
  end

  def all_local_data(name, shard) do
    local_node = node()

    reg_table = reg_by_key_table(name, shard)

    reg_entries =
      :ets.select(reg_table, [
        {{{:"$1", :"$2"}, :"$3", :"$4", :"$5", :_, :"$6"},
         [{:==, :"$6", local_node}], [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])

    pg_table = pg_by_key_table(name, shard)

    pg_entries =
      :ets.select(pg_table, [
        {{{:"$1", :"$2", :"$3"}, :"$4", :"$5", :_, :"$6"},
         [{:==, :"$6", local_node}], [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])

    {reg_entries, pg_entries}
  end

  def purge_node(name, shard, dead_node) do
    reg_table = reg_by_key_table(name, shard)
    reg_pid_table = reg_by_pid_table(name, shard)

    purged_reg =
      :ets.select(reg_table, [
        {{{:"$1", :"$2"}, :"$3", :"$4", :"$5", :_, :"$6"},
         [{:==, :"$6", dead_node}], [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])

    for {cluster, key, _pid, _meta, _time} <- purged_reg do
      :ets.delete(reg_table, {cluster, key})
      :ets.match_delete(reg_pid_table, {:_, cluster, key, :_, :_, :_, dead_node})
    end

    pg_table = pg_by_key_table(name, shard)
    pg_pid_table = pg_by_pid_table(name, shard)

    purged_pg =
      :ets.select(pg_table, [
        {{{:"$1", :"$2", :"$3"}, :"$4", :"$5", :_, :"$6"},
         [{:==, :"$6", dead_node}], [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])

    for {cluster, key, pid, _meta, _time} <- purged_pg do
      :ets.delete(pg_table, {cluster, key, pid})
      :ets.delete(pg_pid_table, {pid, cluster, key})
    end

    {purged_reg, purged_pg}
  end

  # =====================================================================
  # Counting
  # =====================================================================

  def local_registry_count(name, num_shards, cluster) do
    local_node = node()

    Enum.reduce(0..(num_shards - 1), 0, fn shard, acc ->
      table = reg_by_key_table(name, shard)

      count =
        :ets.select_count(table, [
          {{{cluster, :_}, :_, :_, :_, :_, :"$1"},
           [{:==, :"$1", local_node}], [true]}
        ])

      acc + count
    end)
  end

  def local_pg_count(name, num_shards, cluster, key) do
    local_node = node()

    Enum.reduce(0..(num_shards - 1), 0, fn shard, acc ->
      table = pg_by_key_table(name, shard)

      count =
        :ets.select_count(table, [
          {{{cluster, key, :_}, :_, :_, :_, :"$1"},
           [{:==, :"$1", local_node}], [true]}
        ])

      acc + count
    end)
  end

  # =====================================================================
  # Cluster membership (shared table)
  # =====================================================================

  def cluster_nodes(name, cluster) do
    table = cluster_nodes_table(name)

    case :ets.lookup(table, cluster) do
      [{^cluster, nodes}] -> nodes
      [] -> MapSet.new()
    end
  end

  def put_cluster_nodes(name, cluster, nodes) do
    table = cluster_nodes_table(name)
    :ets.insert(table, {cluster, nodes})
    :ok
  end

  def delete_cluster_nodes(name, cluster) do
    table = cluster_nodes_table(name)
    :ets.delete(table, cluster)
    :ok
  end

  def all_clusters(name) do
    table = cluster_nodes_table(name)
    :ets.select(table, [{{:"$1", :_}, [], [:"$1"]}])
  end

  # =====================================================================
  # Table names
  # =====================================================================

  def reg_by_key_table(name, shard), do: :"#{name}_s#{shard}_reg_by_key"
  def reg_by_pid_table(name, shard), do: :"#{name}_s#{shard}_reg_by_pid"
  def pg_by_key_table(name, shard), do: :"#{name}_s#{shard}_pg_by_key"
  def pg_by_pid_table(name, shard), do: :"#{name}_s#{shard}_pg_by_pid"
  def cluster_nodes_table(name), do: :"#{name}_cluster_nodes"

  # =====================================================================
  # GenServer callbacks
  # =====================================================================

  @impl true
  def init({name, num_shards}) do
    for shard <- 0..(num_shards - 1) do
      :ets.new(reg_by_key_table(name, shard), [
        :set, :public, :named_table,
        read_concurrency: true
      ])

      :ets.new(reg_by_pid_table(name, shard), [
        :bag, :public, :named_table,
        read_concurrency: true
      ])

      :ets.new(pg_by_key_table(name, shard), [
        :ordered_set, :public, :named_table,
        read_concurrency: true
      ])

      :ets.new(pg_by_pid_table(name, shard), [
        :ordered_set, :public, :named_table,
        read_concurrency: true
      ])
    end

    :ets.new(cluster_nodes_table(name), [
      :set, :public, :named_table,
      read_concurrency: true
    ])

    {:ok, %{name: name, num_shards: num_shards}}
  end
end
