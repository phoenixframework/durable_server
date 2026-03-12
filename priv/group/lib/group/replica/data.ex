defmodule Group.Replica.Data do
  @moduledoc false
  use GenServer

  _archdoc = """
  GenServer that owns ETS tables for all shards.

  Survives Replica shard crashes via rest_for_one supervisor strategy.
  Provides a pure function API for all ETS operations.

  ## ETS Table Layout

  Each shard owns 4 tables. There are also 2 shared cluster membership tables per Group instance.

  ### reg_by_key — `:set`, keyed by `{cluster, key}`

      {{cluster, key}, pid, meta, time, node}

  Primary registry lookup table. `:set` enforces one registration per key per cluster.
  `registry_lookup/4` does a direct `ets.lookup` on `{cluster, key}` — O(1) constant time.
  `registry_delete/4` does a direct `ets.delete` — O(1).

  ### reg_by_pid — `:ordered_set`, keyed by `{pid, cluster, key}`

      {{pid, cluster, key}, meta, time, node}

  Reverse index for process death cleanup. `:ordered_set` keyed by `{pid, ...}` so that
  `entries_by_pid` can select all entries for a pid as a contiguous range scan. Also used
  by `maybe_demonitor` to check if a pid has any remaining entries (select with limit 1).

  Re-registration (metadata update) overwrites the existing entry in place — no stale
  accumulation. Direct deletes use `ets.delete(table, {pid, cluster, key})` — O(log N).

  Mirrors the `pg_by_pid` design.

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

  Forward index: cluster → nodes. One row per {cluster, node} pair — `:bag` deduplicates
  exact tuples on insert, so concurrent adds of the same node are idempotent with no
  read-modify-write race. `cluster_nodes/2` does a direct bucket lookup — O(nodes in cluster).

  ### node_clusters — `:bag`, keyed by node

      {node, cluster}

  Reverse index: node → clusters. Mirrors cluster_nodes for efficient node-centric lookups.
  `my_clusters/1` does a bucket lookup — O(clusters for this node) instead of a full table
  scan. `purge_cluster_node/2` uses this to find all clusters for a dead node, then does
  targeted deletes from both tables — O(clusters for node) instead of O(total entries).

  Both tables are shared across all shards. Used for the default cluster (nil) and named
  clusters. The nil cluster is maintained by the peer_connect protocol — nodes are added on
  peer discovery and removed on nodedown/shard death. `Group.nodes/1` reads nil cluster
  from cluster_nodes.

  ## Match Spec Patterns

  All match specs use `{:==, :"$N", value}` guards to filter on runtime values (e.g. node
  name). This is the correct ETS match spec syntax — `:const` is not valid. Literal values
  from Elixir variables (like `cluster` or `key`) are interpolated directly into the match
  pattern tuple positions and work as exact-match filters without needing a guard.

  ## Bulk Operations & Their Costs

  - `purge_node/3`: Full table scan via `ets.select` filtering by node, then individual
    deletes. O(table size) for the scan, but this only runs on nodedown — rare path.

  - `local_data_by_cluster/3`: Full table scan filtering by `node() == local_node`,
    grouped by cluster. Only runs during discovery/sync protocol.

  - `local_registry_count`, `local_pg_count`: Uses `ets.select_count` with a guard.
    Full scan but returns only the count without materializing results.

  - `entries_by_pid/3`: Range scan on the by_pid ordered_set tables. O(entries for that pid).

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
    :ets.insert(table_pid, {{pid, cluster, key}, meta, time, node})
    :ok
  end

  def registry_delete(name, shard, cluster, key, pid) do
    table = reg_by_key_table(name, shard)
    :ets.delete(table, {cluster, key})
    table_pid = reg_by_pid_table(name, shard)
    :ets.delete(table_pid, {pid, cluster, key})
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

    :ets.select(table, [
      {{{pid, :"$1", :"$2"}, :"$3", :"$4", :"$5"}, [], [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
    ])
  end

  def registry_lookup_by_prefix(name, shard, cluster, prefix) do
    table = reg_by_key_table(name, shard)
    prefix_end = next_binary_prefix(prefix)

    :ets.select(table, [
      {{{cluster, :"$1"}, :"$2", :"$3", :_, :_},
       [{:andalso, {:>=, :"$1", prefix}, {:<, :"$1", prefix_end}}], [{{:"$2", :"$3"}}]}
    ])
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

  def pg_members_with_node(name, shard, cluster, key) do
    table = pg_by_key_table(name, shard)

    match_spec = [
      {{{cluster, key, :"$1"}, :"$2", :_, :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}
    ]

    :ets.select(table, match_spec)
  end

  def pg_members_by_prefix(name, shard, cluster, prefix) do
    table = pg_by_key_table(name, shard)
    prefix_end = next_binary_prefix(prefix)

    :ets.select(table, [
      {{{cluster, :"$1", :"$2"}, :"$3", :_, :_},
       [{:andalso, {:>=, :"$1", prefix}, {:<, :"$1", prefix_end}}], [{{:"$2", :"$3"}}]}
    ])
  end

  def pg_members_local(name, shard, cluster, key) do
    local_node = node()
    table = pg_by_key_table(name, shard)

    match_spec = [
      {{{cluster, key, :"$1"}, :_, :_, :"$2"}, [{:==, :"$2", local_node}], [:"$1"]}
    ]

    :ets.select(table, match_spec)
  end

  @doc """
  Delete all entries for a pid from this shard. Used on process DOWN.
  Deletes from by_key tables individually (need the key), but batch-deletes
  from by_pid tables with match_delete (all entries for this pid are contiguous).
  Returns {reg_entries, pg_entries} for dispatch.
  """
  def delete_all_for_pid(name, shard, pid) do
    reg_table = reg_by_key_table(name, shard)
    reg_pid_table = reg_by_pid_table(name, shard)

    reg_entries =
      :ets.select(reg_pid_table, [
        {{{pid, :"$1", :"$2"}, :"$3", :"$4", :"$5"}, [], [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])

    for {cluster, key, _meta, _time, _node} <- reg_entries do
      :ets.delete(reg_table, {cluster, key})
    end

    # Batch-delete all reg_by_pid entries for this pid
    :ets.match_delete(reg_pid_table, {{pid, :_, :_}, :_, :_, :_})

    pg_table = pg_by_key_table(name, shard)
    pg_pid_table = pg_by_pid_table(name, shard)

    pg_entries =
      :ets.select(pg_pid_table, [
        {{{pid, :"$1", :"$2"}, :"$3", :"$4", :"$5"}, [], [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])

    for {cluster, key, _meta, _time, _node} <- pg_entries do
      :ets.delete(pg_table, {cluster, key, pid})
    end

    # Batch-delete all pg_by_pid entries for this pid
    :ets.match_delete(pg_pid_table, {{pid, :_, :_}, :_, :_, :_})

    {reg_entries, pg_entries}
  end

  # =====================================================================
  # Monitor helpers (per-shard, no cross-shard coordination)
  # =====================================================================

  def maybe_demonitor(name, shard, pid) do
    # Check remaining entries for this pid across both tables in this shard
    table_reg = reg_by_pid_table(name, shard)

    has_reg =
      case :ets.select(table_reg, [{{{pid, :_, :_}, :_, :_, :_}, [], [true]}], 1) do
        {[true], _} -> true
        :"$end_of_table" -> false
      end

    has_pg =
      if has_reg do
        true
      else
        table_pg = pg_by_pid_table(name, shard)

        case :ets.select(table_pg, [{{{pid, :_, :_}, :_, :_, :_}, [], [true]}], 1) do
          {[true], _} -> true
          :"$end_of_table" -> false
        end
      end

    if has_reg or has_pg, do: :still_monitored, else: :ok
  end

  # =====================================================================
  # Bulk operations
  # =====================================================================

  def entries_by_pid(name, shard, pid) do
    reg_table = reg_by_pid_table(name, shard)

    reg_entries =
      :ets.select(reg_table, [
        {{{pid, :"$1", :"$2"}, :"$3", :"$4", :"$5"}, [], [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])
      |> Enum.map(fn {cluster, key, meta, time, node} ->
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

    for {cluster, key, pid, _meta, _time} <- purged_reg do
      :ets.delete(reg_table, {cluster, key})
      :ets.delete(reg_pid_table, {pid, cluster, key})
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
  # Cluster membership (dual-index: cluster_nodes + node_clusters)
  # =====================================================================

  def cluster_nodes(name, cluster) do
    table = cluster_nodes_table(name)
    :ets.lookup(table, cluster) |> Enum.map(&elem(&1, 1))
  end

  def add_cluster_node(name, cluster, node) do
    :ets.insert(cluster_nodes_table(name), {cluster, node})
    :ets.insert(node_clusters_table(name), {node, cluster})
    :ok
  end

  def remove_cluster_node(name, cluster, node) do
    :ets.delete_object(cluster_nodes_table(name), {cluster, node})
    :ets.delete_object(node_clusters_table(name), {node, cluster})
    :ok
  end

  def all_clusters(name) do
    table = cluster_nodes_table(name)
    :ets.select(table, [{{:"$1", :_}, [], [:"$1"]}]) |> Enum.uniq()
  end

  def my_clusters(name) do
    table = node_clusters_table(name)
    :ets.lookup(table, node()) |> Enum.map(&elem(&1, 1))
  end

  def purge_cluster_node(name, dead_node) do
    reverse = node_clusters_table(name)
    forward = cluster_nodes_table(name)

    for {^dead_node, cluster} <- :ets.lookup(reverse, dead_node) do
      :ets.delete_object(forward, {cluster, dead_node})
    end

    :ets.delete(reverse, dead_node)
    :ok
  end

  # =====================================================================
  # Helpers
  # =====================================================================

  defp next_binary_prefix(prefix) do
    size = byte_size(prefix) - 1
    <<head::binary-size(size), last_byte>> = prefix
    <<head::binary, last_byte + 1>>
  end

  # =====================================================================
  # Table names
  # =====================================================================

  def reg_by_key_table(name, shard), do: :"#{name}_s#{shard}_reg_by_key"
  def reg_by_pid_table(name, shard), do: :"#{name}_s#{shard}_reg_by_pid"
  def pg_by_key_table(name, shard), do: :"#{name}_s#{shard}_pg_by_key"
  def pg_by_pid_table(name, shard), do: :"#{name}_s#{shard}_pg_by_pid"
  def cluster_nodes_table(name), do: :"#{name}_cluster_nodes"
  def node_clusters_table(name), do: :"#{name}_node_clusters"

  # =====================================================================
  # GenServer callbacks
  # =====================================================================

  @impl true
  def init({name, num_shards}) do
    # ETS performance options:
    # - read_concurrency: splits table into read-optimized segments (less lock contention)
    # - decentralized_counters: reduces contention on table size counter (OTP 23+)
    # Note: write_concurrency is intentionally omitted — the sharded GenServer already
    # serializes writes per shard, so ETS write locking is never contended. Adding
    # write_concurrency adds overhead (~30-40% on serial benchmarks) without benefit.
    set_opts = [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      decentralized_counters: true
    ]

    ordered_set_opts = [
      :ordered_set,
      :public,
      :named_table,
      read_concurrency: true,
      decentralized_counters: true
    ]

    bag_opts = [
      :bag,
      :public,
      :named_table,
      read_concurrency: true,
      decentralized_counters: true
    ]

    for shard <- 0..(num_shards - 1) do
      :ets.new(reg_by_key_table(name, shard), set_opts)
      :ets.new(reg_by_pid_table(name, shard), ordered_set_opts)
      :ets.new(pg_by_key_table(name, shard), ordered_set_opts)
      :ets.new(pg_by_pid_table(name, shard), ordered_set_opts)
    end

    :ets.new(cluster_nodes_table(name), bag_opts)
    :ets.new(node_clusters_table(name), bag_opts)

    {:ok, %{name: name, num_shards: num_shards}}
  end
end
