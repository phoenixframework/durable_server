defmodule Group.Replica.Data do
  @moduledoc false
  use GenServer

  _archdoc = """
  GenServer that owns ETS tables for all shards.

  Survives Replica shard crashes via rest_for_one supervisor strategy.
  Provides a pure function API for all ETS operations.

  ## ETS Table Layout

  Each shard owns 4 tables. There is also 1 shared cluster_nodes table per Group instance.

  ### reg_by_key — `:set`, keyed by `{cluster, key}`

      {{cluster, key}, pid, meta, time, node}

  Primary registry lookup table. `:set` enforces one registration per key per cluster.
  `registry_lookup/4` does a direct `ets.lookup` on `{cluster, key}` — O(1) constant time.
  `registry_delete/4` does a direct `ets.delete` — O(1).

  ### reg_by_pid — `:bag`, keyed by `pid`

      {pid, cluster, key, meta, time, node}

  Reverse index for process death cleanup. `:bag` because a single pid could be registered
  under different keys in different clusters (though unusual). `ets.lookup(table, pid)` returns
  all entries for that pid — O(number of entries for that pid), typically 1.

  `registry_delete` uses `ets.match_delete` with `{:_, cluster, key, :_, :_, :_}` to
  remove the specific entry from the bag. This scans entries for the pid's bucket — O(small).

  ### pg_by_key — `:ordered_set`, keyed by `{cluster, key, pid}`

      {{cluster, key, pid}, meta, time, node}

  Primary process group table. `:ordered_set` is chosen so that `pg_members/4` can use
  `ets.select` with a match spec on `{cluster, key, :"$1"}` to efficiently find all pids
  for a given group. Because ordered_set sorts by key, entries for the same `{cluster, key}`
  are contiguous, so the select is a bounded range scan — O(members in group), not O(table).

  Direct lookups (`pg_lookup/5`) and deletes (`pg_delete/5`) are O(log N) on ordered_set.

  ### pg_by_pid — `:ordered_set`, keyed by `{pid, cluster, key}`

      {{pid, cluster, key}, meta, time, node}

  Reverse index for process death cleanup. `:ordered_set` keyed by `{pid, ...}` so that
  `entries_by_pid` can select all entries for a pid as a contiguous range scan. Also used
  by `maybe_demonitor` to check if a pid has any remaining entries (select with limit 1).

  ### cluster_nodes — `:bag`, keyed by cluster name

      {cluster, node}

  Shared across all shards. One row per {cluster, node} pair — `:bag` deduplicates
  exact tuples on insert, so concurrent adds of the same node are idempotent with no
  read-modify-write race. Used for both the default cluster (nil) and named clusters.
  The nil cluster is maintained by the peer_connect protocol — nodes are added on peer
  discovery and removed on nodedown/shard death. `Group.nodes/1` reads nil cluster from ETS.

  ## Match Spec Patterns

  All match specs use `{:==, :"$N", value}` guards to filter on runtime values (e.g. node
  name). This is the correct ETS match spec syntax — `:const` is not valid. Literal values
  from Elixir variables (like `cluster` or `key`) are interpolated directly into the match
  pattern tuple positions and work as exact-match filters without needing a guard.

  ## Bulk Operations & Their Costs

  - `purge_node/3`: Full table scan via `ets.select` filtering by node, then individual
    deletes. O(table size) for the scan, but this only runs on nodedown — rare path.

  - `local_data/3`: Full table scan filtering by `node() == local_node`.
    Only runs during discovery/sync protocol — initial connection or reconnection.

  - `local_registry_count`, `local_pg_count`: Uses `ets.select_count` with a guard.
    Full scan but returns only the count without materializing results.

  - `entries_by_pid/3`: Direct key lookup on the by_pid tables. O(entries for that pid).

  ## Process Monitors

  Monitors live entirely in the Replica GenServer's `state.monitors` map (`pid => mref`).
  ETS stores pids but not monitor refs — mref is not needed in ETS.

  On Replica crash, the BEAM cleans up all monitors owned by the dead process. On restart,
  `rebuild_monitors/1` scans the surviving ETS tables for pids and calls `Process.monitor`
  fresh. This is the only reason ETS matters for monitors: without surviving pid entries,
  local processes that registered before the crash would be orphaned — nobody would monitor
  them, and their ETS entries would persist forever if they later died.

  Remote data doesn't need this protection — the discovery protocol re-syncs everything
  from remote nodes on restart. Only local process entries need the ETS scan.

  The `state.monitors` map also deduplicates: a pid registered under multiple keys in the
  same shard gets one monitor, not one per key.

  `maybe_demonitor/3` checks whether a pid still has any remaining entries across both
  tables before allowing demonitor. Short-circuits: checks reg_by_pid first (key lookup),
  falls back to pg_by_pid only if empty (select with limit 1 — existence check, not scan).

  ## Concurrency

  All tables are `:public` with `read_concurrency: true`. Reads happen directly from any
  process (the Replica GenServer, Group API callers, etc.). Writes are serialized through
  the Replica GenServer for each shard, ensuring consistent paired updates to both the
  by_key and by_pid tables. The Data GenServer itself only owns the tables (for crash
  survival via rest_for_one) — it handles no messages after init.
  """

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    num_shards = Keyword.fetch!(opts, :num_shards)
    GenServer.start_link(__MODULE__, {name, num_shards}, name: data_name(name))
  end

  def data_name(name), do: :"#{name}_data"

  # =====================================================================
  # Registry operations
  # =====================================================================

  def registry_insert(name, shard, cluster, key, pid, meta, time, node) do
    table = reg_by_key_table(name, shard)
    :ets.insert(table, {{cluster, key}, pid, meta, time, node})
    table_pid = reg_by_pid_table(name, shard)
    :ets.insert(table_pid, {pid, cluster, key, meta, time, node})
    :ok
  end

  def registry_delete(name, shard, cluster, key) do
    table = reg_by_key_table(name, shard)
    :ets.delete(table, {cluster, key})
    table_pid = reg_by_pid_table(name, shard)
    # Delete matching entries from by_pid table
    :ets.match_delete(table_pid, {:_, cluster, key, :_, :_, :_})
    :ok
  end

  def registry_lookup(name, shard, cluster, key) do
    table = reg_by_key_table(name, shard)

    case :ets.lookup(table, {cluster, key}) do
      [{{^cluster, ^key}, pid, meta, time, node}] ->
        {pid, meta, time, node}

      [] ->
        nil
    end
  end

  def registry_lookup_by_pid(name, shard, pid) do
    table = reg_by_pid_table(name, shard)

    :ets.lookup(table, pid)
    |> Enum.map(fn {^pid, cluster, key, meta, time, node} ->
      {cluster, key, meta, time, node}
    end)
  end

  # =====================================================================
  # Process group operations
  # =====================================================================

  def pg_insert(name, shard, cluster, key, pid, meta, time, node) do
    table = pg_by_key_table(name, shard)
    :ets.insert(table, {{cluster, key, pid}, meta, time, node})
    table_pid = pg_by_pid_table(name, shard)
    :ets.insert(table_pid, {{pid, cluster, key}, meta, time, node})
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
      [{{^cluster, ^key, ^pid}, meta, time, node}] ->
        {meta, time, node}

      [] ->
        nil
    end
  end

  def pg_members(name, shard, cluster, key) do
    table = pg_by_key_table(name, shard)
    # Use match spec to find all entries with the given {cluster, key, _pid} prefix
    match_spec = [
      {{{cluster, key, :"$1"}, :"$2", :_, :_}, [], [{{:"$1", :"$2"}}]}
    ]

    :ets.select(table, match_spec)
  end

  # =====================================================================
  # Monitor helpers (per-shard, no cross-shard coordination)
  # =====================================================================

  def maybe_demonitor(name, shard, pid) do
    # Count remaining entries for this pid across both tables in this shard
    reg_count = length(:ets.lookup(reg_by_pid_table(name, shard), pid))

    pg_count =
      if reg_count > 0 do
        # Still has entries, don't demonitor
        1
      else
        table_pg = pg_by_pid_table(name, shard)

        case :ets.select(table_pg, [{{{pid, :_, :_}, :_, :_, :_}, [], [true]}], 1) do
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
      |> Enum.map(fn {^pid, cluster, key, meta, time, node} ->
        {:registry, cluster, key, pid, meta, time, node}
      end)

    pg_table = pg_by_pid_table(name, shard)

    pg_entries =
      :ets.select(pg_table, [
        {{{pid, :"$1", :"$2"}, :"$3", :"$4", :"$5"}, [], [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])
      |> Enum.map(fn {cluster, key, meta, time, node} ->
        {:pg, cluster, key, pid, meta, time, node}
      end)

    reg_entries ++ pg_entries
  end

  def local_data(name, shard, cluster) do
    local_node = node()

    reg_table = reg_by_key_table(name, shard)

    reg_entries =
      :ets.select(reg_table, [
        {{{cluster, :"$1"}, :"$2", :"$3", :"$4", :"$5"}, [{:==, :"$5", local_node}],
         [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])

    pg_table = pg_by_key_table(name, shard)

    pg_entries =
      :ets.select(pg_table, [
        {{{cluster, :"$1", :"$2"}, :"$3", :"$4", :"$5"}, [{:==, :"$5", local_node}],
         [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])

    {reg_entries, pg_entries}
  end

  def local_data_by_cluster(name, shard, clusters) do
    cluster_set = MapSet.new(clusters)
    local_node = node()

    reg_table = reg_by_key_table(name, shard)

    reg_by_cluster =
      :ets.select(reg_table, [
        {{{:"$1", :"$2"}, :"$3", :"$4", :"$5", :"$6"}, [{:==, :"$6", local_node}],
         [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])
      |> Enum.filter(fn {cluster, _, _, _, _} -> MapSet.member?(cluster_set, cluster) end)
      |> Enum.group_by(&elem(&1, 0), fn {_, key, pid, meta, time} -> {key, pid, meta, time} end)

    pg_table = pg_by_key_table(name, shard)

    pg_by_cluster =
      :ets.select(pg_table, [
        {{{:"$1", :"$2", :"$3"}, :"$4", :"$5", :"$6"}, [{:==, :"$6", local_node}],
         [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])
      |> Enum.filter(fn {cluster, _, _, _, _} -> MapSet.member?(cluster_set, cluster) end)
      |> Enum.group_by(&elem(&1, 0), fn {_, key, pid, meta, time} -> {key, pid, meta, time} end)

    {reg_by_cluster, pg_by_cluster}
  end

  def purge_node(name, shard, dead_node) do
    reg_table = reg_by_key_table(name, shard)
    reg_pid_table = reg_by_pid_table(name, shard)

    purged_reg =
      :ets.select(reg_table, [
        {{{:"$1", :"$2"}, :"$3", :"$4", :"$5", :"$6"}, [{:==, :"$6", dead_node}],
         [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])

    for {cluster, key, _pid, _meta, _time} <- purged_reg do
      :ets.delete(reg_table, {cluster, key})
      :ets.match_delete(reg_pid_table, {:_, cluster, key, :_, :_, dead_node})
    end

    pg_table = pg_by_key_table(name, shard)
    pg_pid_table = pg_by_pid_table(name, shard)

    purged_pg =
      :ets.select(pg_table, [
        {{{:"$1", :"$2", :"$3"}, :"$4", :"$5", :"$6"}, [{:==, :"$6", dead_node}],
         [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
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
          {{{cluster, :_}, :_, :_, :_, :"$1"}, [{:==, :"$1", local_node}], [true]}
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
          {{{cluster, key, :_}, :_, :_, :"$1"}, [{:==, :"$1", local_node}], [true]}
        ])

      acc + count
    end)
  end

  # =====================================================================
  # Cluster membership (shared table — :bag, one row per {cluster, node})
  # =====================================================================

  def cluster_nodes(name, cluster) do
    table = cluster_nodes_table(name)
    :ets.lookup(table, cluster) |> Enum.map(&elem(&1, 1))
  end

  def add_cluster_node(name, cluster, node) do
    table = cluster_nodes_table(name)
    :ets.insert(table, {cluster, node})
    :ok
  end

  def remove_cluster_node(name, cluster, node) do
    table = cluster_nodes_table(name)
    :ets.delete_object(table, {cluster, node})
    :ok
  end

  def delete_cluster_nodes(name, cluster) do
    table = cluster_nodes_table(name)
    :ets.delete(table, cluster)
    :ok
  end

  def all_clusters(name) do
    table = cluster_nodes_table(name)
    :ets.select(table, [{{:"$1", :_}, [], [:"$1"]}]) |> Enum.uniq()
  end

  def my_clusters(name) do
    local_node = node()
    table = cluster_nodes_table(name)
    :ets.select(table, [{{:"$1", :"$2"}, [{:==, :"$2", local_node}], [:"$1"]}])
  end

  def purge_cluster_node(name, node) do
    table = cluster_nodes_table(name)
    :ets.match_delete(table, {:_, node})
    :ok
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
        :set,
        :public,
        :named_table,
        read_concurrency: true
      ])

      :ets.new(reg_by_pid_table(name, shard), [
        :bag,
        :public,
        :named_table,
        read_concurrency: true
      ])

      :ets.new(pg_by_key_table(name, shard), [
        :ordered_set,
        :public,
        :named_table,
        read_concurrency: true
      ])

      :ets.new(pg_by_pid_table(name, shard), [
        :ordered_set,
        :public,
        :named_table,
        read_concurrency: true
      ])
    end

    :ets.new(cluster_nodes_table(name), [
      :bag,
      :public,
      :named_table,
      read_concurrency: true
    ])

    {:ok, %{name: name, num_shards: num_shards}}
  end
end
