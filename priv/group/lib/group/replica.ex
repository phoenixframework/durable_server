defmodule Group.Replica do
  @moduledoc false

  use GenServer

  _archdoc = ~S"""
  Sharded GenServer: peer discovery, replication, monitoring, conflict resolution.

  One per shard. Registered as :"#{name}_replica_#{shard_index}".

  ## Message Protocol

  | Message                                                    | Direction      | Purpose                          |
  |------------------------------------------------------------|----------------|----------------------------------|
  | `{:peer_connect, pid, shard, num_shards, clusters}`        | A→B (per-shard)| Establish peer relationship      |
  | `{:peer_connect_ack, pid, shard, num_shards, clusters}`    | B→A (per-shard)| Acknowledge peer                 |
  | `{:cluster_state, cluster, reg_data, pg_data}`             | both           | Per-cluster data snapshot        |
  | `{:replicate_register, cluster, key, pid, meta, time, reason}` | broadcast  | Propagate registration           |
  | `{:replicate_unregister, cluster, key, pid, meta, reason}` | broadcast      | Propagate unregistration         |
  | `{:replicate_join, cluster, key, pid, meta, time, reason}` | broadcast      | Propagate join                   |
  | `{:replicate_leave, cluster, key, pid, meta, reason}`      | broadcast      | Propagate leave                  |
  | `{:cluster_connect, clusters, pid}`                        | S→remote S     | Node joining named clusters      |
  | `{:cluster_connect_ack, clusters, pid, cluster_data}`      | S→remote S     | Ack + bundled shard data         |
  | `{:cluster_disconnect, clusters, pid}`                     | shard 0→remote | Node leaving named clusters      |
  | `{:send_cluster_data, clusters, target_node}`              | local fan-out  | Notify siblings: send shard data |
  | `{:group_dispatch, pids, message}`                         | caller→remote  | Per-node fan-out for dispatch    |

  ## Protocol Flows

  ### 1. Peer Discovery (nodeup or init)

  Triggered by `nodeup` or `init`. Each shard independently discovers its
  counterpart on the remote node. Both sides exchange cluster lists, then send
  per-cluster `cluster_state` snapshots for shared clusters (always includes
  nil). Merge applies data; conflicts with local entries go through
  `resolve_conflict`.

      Node A shard i                          Node B shard i
      ────────────                            ────────────
           │                                       │
           │  {:peer_connect, pid, i, N, clusters} │
           │──────────────────────────────────────>│
           │                                       │── add A to nil cluster ETS
           │                                       │── compute shared clusters
           │                                       │── add A to shared named clusters
           │                                       │── monitor A's shard pid
           │                                       │
           │  {:peer_connect_ack, pid, i, N, clusters}
           │<──────────────────────────────────────│
           │── add B to nil cluster ETS            │
           │── compute shared clusters             │  {:cluster_state, C, reg, pg}
           │── add B to shared named clusters      │──────────────────────────────>│
           │── monitor B's shard pid               │  (one per shared cluster)     │
           │                                       │                               │
           │  {:cluster_state, C, reg, pg}         │                               │
           │──────────────────────────────>│        │                               │
           │  (one per shared cluster)     │        │                               │
           │                               │        │                               │
           ▼                               ▼        ▼                               ▼
      merge_remote_cluster_data       merge_remote_cluster_data
        ├─ no conflict: insert          ├─ no conflict: insert
        ├─ local vs remote: resolve_conflict (kill loser, re-broadcast winner)
        └─ both remote: timestamp wins  └─ both remote: timestamp wins

  ### 2. Steady-State Replication

  After peer discovery, all writes broadcast to cluster members. The nil cluster
  uses `remote_shards` (per-shard map); named clusters use `cluster_nodes` ETS.
  Reads (`lookup`, `members`) go directly to ETS — no GenServer involved.

      Node A shard i                          Node B shard i
      ────────────                            ────────────
           │                                       │
      Group.register(name, key, meta)              │
           │── ETS insert (by_key + by_pid)        │
           │── monitor pid                         │
           │                                       │
           │  {:replicate_register, C, key, pid, meta, time, reason}
           │──────────────────────────────────────>│
           │                                       │── lookup existing
           │                                       │   ├─ nil: insert
           │                                       │   ├─ same pid: update
           │                                       │   ├─ local conflict:
           │                                       │   │  resolve_conflict()
           │                                       │   └─ both remote:
           │                                       │      timestamp wins
           │                                       │
      Group.join(name, group, meta)                │
           │── ETS insert (by_key + by_pid)        │
           │                                       │
           │  {:replicate_join, C, key, pid, meta, time, reason}
           │──────────────────────────────────────>│
           │                                       │── insert (no overwrite
           │                                       │   conflict for PG)
           │                                       │
      Group.dispatch(name, group, msg)               │
           │── send directly to local pids           │
           │── group remote pids by node             │
           │── hash self() to pick shard j           │
           │                                         │
           │  {:group_dispatch, pids, msg}    Node B shard j
           │────────────────────────────────────────>│
           │                                         │── send msg to each local pid
           │                                         │

  Dispatch groups remote PG members by node and sends one
  `:group_dispatch` message per remote node, reducing cross-node
  messages from O(members) to O(nodes). The target shard is chosen
  by hashing the caller's pid (`phash2(self(), num_shards)`), so
  back-to-back dispatches from the same caller always route through
  the same shard, preserving per-sender message ordering.

  ### 3. Named Cluster Connect (random shard S + fan-out)

  `Group.connect/2` adds local node to ETS, picks random shard S, and sends
  one GenServer.call. Shard S notifies remote shard S, which acks with bundled
  data and fans out to siblings. Randomizing S load-balances across shards
  when many concurrent connects happen.

      Node A                                  Node B
      ──────                                  ──────
      Group.connect(name, "game")
        │── ETS: add self to "game"
        │── pick random shard S
        │
      Shard S                                 Shard S
      ───────                                 ───────
        │                                       │
        │  {:cluster_connect, ["game"], pid}    │
        │──────────────────────────────────────>│
        │                                       │── ETS: add A to "game"
        │                                       │── bundle shard S data
        │                                       │
        │  {:cluster_connect_ack, ["game"], pid, [{cluster, reg, pg}]}
        │<──────────────────────────────────────│
        │                                       │
        │                                  Shard S sends to siblings:
        │                                  {:send_cluster_data, ["game"], A}
        │                                       │
        │                                  Shards 0..N (except S):
        │                                       │── {:cluster_state, "game", reg, pg}
        │                                       │──────────────────────────────>│
        │                                       │   (to matching A shard)       │
        │                                       │                               │
        │── merge bundled ack data              │
        │── ETS: add B to "game"                │
        │── send shard S cluster_state ────────>│
        │── fan out to siblings:                │
        │   {:send_cluster_data, ["game"], B}   │
        │                                       │
      Shards 0..N (except S):                   │
        │── {:cluster_state, "game", reg, pg}   │
        │──────────────────────────────────────>│
        │   (to matching B shard)               │

  ### 4. Named Cluster Disconnect (all shards local + shard 0 broadcast)

  `Group.disconnect/2` removes local node from ETS, then calls ALL local shards
  to purge their entries. Only shard 0 broadcasts to remote shard 0, which fans
  out to siblings for per-shard purge.

      Node A                                  Node B
      ──────                                  ──────
      Group.disconnect(name, "game")
        │── ETS: remove self from "game"
        │
      Shards 0..N (all called):
        │── purge own entries for "game"+A
        │── dispatch :unregistered/:left events
        │
      Shard 0 only:
        │  {:cluster_disconnect, ["game"], pid}
        │──────────────────────────────────────>│ Shard 0
        │                                       │── ETS: remove A from "game"
        │                                       │── fan out to siblings:
        │                                       │   {:cluster_disconnect, ["game"], pid}
        │                                       │
        │                                  Shards 0..N:
        │                                       │── purge entries for "game"+A
        │                                       │── dispatch events

  ### 5. Partition Heal (peer discovery re-runs + conflict resolution)

  When a partition heals, `nodeup` triggers peer discovery on both sides.
  Both exchange `cluster_state` snapshots. Registry key conflicts where the
  existing entry is local go through `resolve_conflict` — the same path used
  for live contention. The default resolver kills the loser process.

  The tiebreaker must be deterministic regardless of which node is resolving.
  The default uses timestamp comparison, with pid ordering as a tiebreaker
  when timestamps are equal (`pid2 > pid1`). Erlang pids have a total order
  (by node name then id), so this produces the same winner on all nodes.
  Using a perspective-dependent tiebreaker (e.g. "remote wins on ties") would
  cause mutual kill — both nodes pick the other's pid, both processes die.

      Node A                                  Node B
      ──────                                  ──────
      (partition: A and B both register key K)
      A has: {K, pid_a, time_a, local}        B has: {K, pid_b, time_b, local}
           │                                       │
      ─────── partition heals (nodeup) ────────────
           │                                       │
           │  peer_connect / peer_connect_ack      │
           │<─────────────────────────────────────>│
           │                                       │
           │  {:cluster_state, nil, [{K, pid_b, ...}], []}
           │<──────────────────────────────────────│
           │                                       │
           │  {:cluster_state, nil, [{K, pid_a, ...}], []}
           │──────────────────────────────────────>│
           │                                       │
      merge: K exists locally                 merge: K exists locally
        resolve_conflict(                       resolve_conflict(
          local={pid_a, time_a},                  local={pid_b, time_b},
          remote={pid_b, time_b})                 remote={pid_a, time_a})
           │                                       │
      (assuming time_b > time_a):             (assuming time_b > time_a):
        pid_b wins (remote)                     pid_b wins (local)
        ├─ kill pid_a                           ├─ kill pid_a (cross-node, idempotent)
        ├─ delete pid_a entry                   ├─ re-insert pid_b with new timestamp
        ├─ insert pid_b                         └─ re-broadcast pid_b
        ├─ demonitor pid_a                         {:replicate_register, ...}
        ├─ dispatch :unregistered(pid_a)         │──────────────────────────>│
           │                                       │  (arrives as same-pid     │
           │                                       │   update — harmless)      │

  ### 6. Nodedown / Process Death Cleanup

  `nodedown` purges all remote node data. Local process `DOWN` purges the pid's
  entries and broadcasts unregister/leave to cluster members.

      Node A                                  Node B dies
      ──────                                  ──────────
           │                                       X
      {:nodedown, B}                               │
           │                                       │
      All shards (each independently):             │
        │── purge_cluster_node(B)                  │
        │   (remove B from all cluster_nodes       │
        │    and node_clusters — idempotent,        │
        │    guards against late peer_connect)     │
           │                                       │
        │── purge_node(shard, B)                   │
        │   (scan by_key for node==B,              │
        │    delete from both by_key + by_pid)     │
        │── dispatch :unregistered/:left events    │
        │── remove B from remote_shards            │

      ──────────────────────────────────────────────

      Local process dies                      Node B
      ──────────────────                      ──────
      {:DOWN, mref, :process, pid, reason}         │
           │                                       │
      Owning shard:                                │
        │── delete_all_for_pid(shard, pid)         │
        │   (scan by_pid, delete from by_key,      │
        │    match_delete from by_pid)             │
        │── broadcast per cluster:                 │
        │   {:replicate_unregister, ...}  ────────>│── delete if pid matches
        │   {:replicate_leave, ...}       ────────>│── delete if pid matches
        │── demonitor pid                          │
        │── dispatch events                        │

  ## Cluster Membership Tracking

  The nil cluster is tracked in ETS (cluster_nodes table), maintained by the
  peer_connect protocol. Nodes are added on peer discovery and removed on
  nodedown/shard death. This allows Group.nodes/1 to return actual Group peers
  rather than all Erlang nodes.

  ## Sharding

  Each key is routed to a shard via `:erlang.phash2({cluster, key}, num_shards)`.
  Including `cluster` in the hash input means the same key string in different
  clusters may land on different shards — this is intentional so named-cluster
  operations don't create false contention with nil-cluster operations.

  `phash2` produces near-uniform distribution across shards for diverse keyspaces.
  With 10K distinct keys across 2–8 shards, observed deviation from perfect
  uniformity is <2%. In practice, real workloads with varied key prefixes will
  see balanced shard load.

  **Hot keys:** A single extremely popular key (e.g. a chat room
  with thousands of joins/leaves) always hashes to one shard, so all *writes*
  for that key serialize through that shard's GenServer. However, *reads* —
  `Group.lookup/3` and `Group.members/3` — go directly to ETS and bypass the
  GenServer entirely. Since reads typically dominate, a hot key's impact on
  overall throughput is limited to write-heavy scenarios. Adding more shards
  does not help a single hot key (it still lands on one shard), but it does
  reduce contention between unrelated keys.

  Shard counts must match across all nodes in a cluster. The peer_connect
  handshake validates `num_shards` and raises on mismatch, since a disagreement
  would route the same key to different shards on different nodes, breaking
  replication consistency.

  ## Conflict Resolution is Synchronous

  The `:resolve_registry_conflict` callback runs synchronously inside the shard
  GenServer's `handle_info` (during `replicate_register` or `merge_remote_cluster_data`).
  This is intentional: the resolver's return value determines ETS mutations (delete
  loser entry, insert winner, demonitor evicted local pid, re-broadcast winner) that
  must happen atomically within a single `handle_info` turn. Making the resolver async
  would open a window where another `replicate_register`, `DOWN`, or `cluster_state`
  for the same key could race with the pending resolution, corrupting the dual-index
  ETS tables.

  Consequence: a blocking resolver stalls the **entire shard** — no registrations,
  joins, replication, or cleanup can proceed on that shard until the callback returns.
  Callers must ensure their resolver returns quickly. Any information needed for the
  decision (e.g. priority, version, creation time) should be carried in the
  registration metadata, not fetched at resolution time.

  ## Monitor Event Delivery

  Lifecycle events (`:registered`, `:unregistered`, `:joined`, `:left`) are delivered
  to `Group.monitor/3` subscribers in a batched diff of `{:group, events, info}` tuples.
  Each GenServer handler invocation is a natural batch boundary:

  - **Single operations** (register, join, leave, unregister, replicate_*): build one
    event, deliver one tuple with one event per matching subscriber.
  - **Bulk operations** (nodedown, process DOWN, cluster_disconnect, cluster_state
    merge): accumulate events into a local variable, then deliver one tuple per
    subscriber containing all matching events from that handler turn.

  Events are built by `build_event/6`, accumulated in reverse via prepend, and
  flushed by `notify_monitors/2` which reverses once, groups by cluster, matches
  against subscriber patterns (`:all`, `{:prefix, _}`, `{:exact, _}`), and sends
  one `{:group, events, %{name: name}}` per subscriber. Both functions are private
  to this module.

  `resolve_conflict/5` returns `{state, event_or_nil}` so callers can accumulate
  the event. `merge_remote_cluster_data/5` threads `{state, events}` through its
  reduce, generating `:registered`/`:joined` events for new entries and conflict
  events for existing ones. This means `cluster_state` merges (peer discovery,
  partition heal, `Group.connect`) produce batched diffs with all new entries.
  `build_purged_events/5` takes an events accumulator and prepends purged-entry
  events to it.
  """

  require Logger

  alias Group.Replica.Data

  defstruct [
    :name,
    :shard_index,
    :num_shards,
    remote_shards: %{},
    monitors: %{}
  ]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    shard_index = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, opts, name: shard_name(name, shard_index))
  end

  def shard_name(name, shard_index), do: :"#{name}_replica_#{shard_index}"

  def shard_for(name, cluster, key) do
    num_shards = Group.get_config(name).num_shards
    index = :erlang.phash2({cluster, key}, num_shards)
    shard_name(name, index)
  end

  def shard_index_for(cluster, key, num_shards) do
    :erlang.phash2({cluster, key}, num_shards)
  end

  # =====================================================================
  # GenServer callbacks
  # =====================================================================

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    shard_index = Keyword.fetch!(opts, :shard_index)
    num_shards = Keyword.fetch!(opts, :num_shards)

    :net_kernel.monitor_nodes(true)

    # Register self as nil cluster member
    Data.add_cluster_node(name, nil, node())

    state = %__MODULE__{
      name: name,
      shard_index: shard_index,
      num_shards: num_shards
    }

    # Rebuild monitors from any surviving ETS data (after shard crash/restart)
    state = rebuild_monitors(state)

    log_once(state, fn -> "#{log_prefix(state)} started (shards=#{num_shards})" end)

    # Discover peers on all known nodes
    registered_name = shard_name(name, shard_index)

    for remote_node <- Node.list() do
      send(
        {registered_name, remote_node},
        {:peer_connect, self(), shard_index, num_shards, Data.my_clusters(name)}
      )
    end

    {:ok, state}
  end

  # =====================================================================
  # Registration calls
  # =====================================================================

  @impl true
  def handle_call({:register, cluster, key, pid, meta}, _from, state) do
    %{name: name, shard_index: shard} = state

    case Data.registry_lookup(name, shard, cluster, key) do
      nil ->
        time = System.system_time()
        mref = monitor_pid(state, pid)
        Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        log_verbose(state, fn ->
          "#{log_prefix_shard(state)} register key=#{inspect(key)} pid=#{inspect(pid)} cluster=#{inspect(cluster)}"
        end)

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_register, cluster, key, pid, meta, time, :register}
        )

        state = put_monitor(state, pid, mref)

        event =
          build_event(name, :registered, key, pid, meta, %{previous_meta: nil, cluster: cluster})

        notify_monitors(name, [event])
        {:reply, :ok, state}

      {^pid, old_meta, _time, _node} when old_meta == meta ->
        # Same pid, same metadata — noop
        {:reply, :ok, state}

      {^pid, old_meta, _time, _node} ->
        # Same pid re-registering — update metadata
        time = System.system_time()
        Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        log_verbose(state, fn ->
          "#{log_prefix_shard(state)} re-register key=#{inspect(key)} pid=#{inspect(pid)} cluster=#{inspect(cluster)}"
        end)

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_register, cluster, key, pid, meta, time, :update}
        )

        event =
          build_event(name, :registered, key, pid, meta, %{
            previous_meta: old_meta,
            cluster: cluster
          })

        notify_monitors(name, [event])
        {:reply, :ok, state}

      _other ->
        {:reply, {:error, :taken}, state}
    end
  end

  def handle_call({:unregister, cluster, key}, _from, state) do
    %{name: name, shard_index: shard} = state

    case Data.registry_lookup(name, shard, cluster, key) do
      {pid, meta, _time, entry_node} when entry_node == node() ->
        Data.registry_delete(name, shard, cluster, key, pid)
        state = maybe_demonitor_pid(state, name, shard, pid)

        log_verbose(state, fn ->
          "#{log_prefix_shard(state)} unregister key=#{inspect(key)} pid=#{inspect(pid)} cluster=#{inspect(cluster)}"
        end)

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_unregister, cluster, key, pid, meta, :unregister}
        )

        event =
          build_event(name, :unregistered, key, pid, meta, %{
            reason: :unregister,
            cluster: cluster
          })

        notify_monitors(name, [event])
        {:reply, :ok, state}

      nil ->
        {:reply, {:error, :undefined}, state}

      {_pid, _meta, _time, _other_node} ->
        {:reply, {:error, :not_owner}, state}
    end
  end

  # =====================================================================
  # Process group calls
  # =====================================================================

  def handle_call({:join, cluster, key, pid, meta}, _from, state) do
    %{name: name, shard_index: shard} = state

    case Data.pg_lookup(name, shard, cluster, key, pid) do
      nil ->
        time = System.system_time()
        mref = monitor_pid(state, pid)
        Data.pg_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        log_verbose(state, fn ->
          "#{log_prefix_shard(state)} join key=#{inspect(key)} pid=#{inspect(pid)} cluster=#{inspect(cluster)}"
        end)

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_join, cluster, key, pid, meta, time, :join}
        )

        state = put_monitor(state, pid, mref)

        event =
          build_event(name, :joined, key, pid, meta, %{previous_meta: nil, cluster: cluster})

        notify_monitors(name, [event])
        {:reply, :ok, state}

      {old_meta, _time, _node} when old_meta == meta ->
        # Same metadata — noop
        {:reply, :ok, state}

      {old_meta, _time, _node} ->
        # Re-join with updated metadata
        time = System.system_time()
        Data.pg_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        log_verbose(state, fn ->
          "#{log_prefix_shard(state)} re-join key=#{inspect(key)} pid=#{inspect(pid)} cluster=#{inspect(cluster)}"
        end)

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_join, cluster, key, pid, meta, time, :update}
        )

        event =
          build_event(name, :joined, key, pid, meta, %{previous_meta: old_meta, cluster: cluster})

        notify_monitors(name, [event])
        {:reply, :ok, state}
    end
  end

  def handle_call({:leave, cluster, key, pid}, _from, state) do
    %{name: name, shard_index: shard} = state

    case Data.pg_lookup(name, shard, cluster, key, pid) do
      nil ->
        {:reply, {:error, :not_in_group}, state}

      {meta, _time, _node} ->
        Data.pg_delete(name, shard, cluster, key, pid)
        state = maybe_demonitor_pid(state, name, shard, pid)

        log_verbose(state, fn ->
          "#{log_prefix_shard(state)} leave key=#{inspect(key)} pid=#{inspect(pid)} cluster=#{inspect(cluster)}"
        end)

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_leave, cluster, key, pid, meta, :leave}
        )

        event = build_event(name, :left, key, pid, meta, %{reason: :leave, cluster: cluster})
        notify_monitors(name, [event])
        {:reply, :ok, state}
    end
  end

  # =====================================================================
  # Cluster connect/disconnect (broadcast to all shards, rare operation)
  # =====================================================================

  def handle_call({:cluster_connect, clusters}, _from, state) do
    %{name: name} = state

    log(state, fn ->
      "#{log_prefix(state)} cluster_connect #{inspect(clusters)}"
    end)

    # Use shared ETS (cluster_nodes for nil cluster) instead of per-shard
    # remote_shards. The nil cluster ETS is populated by any shard processing
    # peer_connect (incoming), so it's available before this shard's
    # peer_connect_ack populates remote_shards. This eliminates the race where
    # connect/2 runs before peer discovery completes on the random shard.
    shard_name = shard_name(name, state.shard_index)
    peers = Data.cluster_nodes(name, nil) -- [node()]

    for target_node <- peers do
      send({shard_name, target_node}, {:cluster_connect, clusters, self()})
    end

    {:reply, :ok, state}
  end

  def handle_call({:cluster_disconnect, clusters}, _from, state) do
    %{name: name, shard_index: shard} = state

    log_once(state, fn ->
      "#{log_prefix(state)} cluster_disconnect #{inspect(clusters)}"
    end)

    events =
      Enum.reduce(clusters, [], fn cluster, acc ->
        {purged_reg, purged_pg} = purge_cluster_entries(name, shard, cluster, node())
        build_purged_events(name, purged_reg, purged_pg, :cluster_disconnect, acc)
      end)

    if shard == 0 do
      broadcast_to_peers(state, {:cluster_disconnect, clusters, self()})
    end

    notify_monitors(name, events)
    {:reply, :ok, state}
  end

  # =====================================================================
  # Replication receive (handle_info)
  # =====================================================================

  @impl true
  def handle_info({:replicate_register, cluster, key, pid, meta, time, _reason}, state) do
    %{name: name, shard_index: shard} = state

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} replicate_register key=#{inspect(key)} from #{node(pid)}"
    end)

    {state, event} =
      case Data.registry_lookup(name, shard, cluster, key) do
        nil ->
          Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))

          {state,
           build_event(name, :registered, key, pid, meta, %{previous_meta: nil, cluster: cluster})}

        {^pid, old_meta, _old_time, _node} ->
          # Same pid updating
          Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))

          {state,
           build_event(name, :registered, key, pid, meta, %{
             previous_meta: old_meta,
             cluster: cluster
           })}

        {existing_pid, existing_meta, existing_time, existing_node}
        when existing_node == node() ->
          # Conflict: incoming remote registration vs local entry
          resolve_conflict(
            state,
            cluster,
            key,
            {existing_pid, existing_meta, existing_time},
            {pid, meta, time}
          )

        {existing_pid, _existing_meta, existing_time, _existing_node} ->
          # Both remote — keep the more recent one
          if time > existing_time do
            if existing_pid != pid do
              Data.registry_delete(name, shard, cluster, key, existing_pid)
            end

            Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))

            {state,
             build_event(name, :registered, key, pid, meta, %{
               previous_meta: nil,
               cluster: cluster
             })}
          else
            {state, nil}
          end
      end

    if event, do: notify_monitors(name, [event])
    {:noreply, state}
  end

  def handle_info({:replicate_unregister, cluster, key, pid, meta, reason}, state) do
    %{name: name, shard_index: shard} = state

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} replicate_unregister key=#{inspect(key)}"
    end)

    case Data.registry_lookup(name, shard, cluster, key) do
      {^pid, _meta, _time, _node} ->
        Data.registry_delete(name, shard, cluster, key, pid)

        event =
          build_event(name, :unregistered, key, pid, meta, %{reason: reason, cluster: cluster})

        notify_monitors(name, [event])

      _ ->
        :ok
    end

    {:noreply, state}
  end

  def handle_info({:replicate_join, cluster, key, pid, meta, time, reason}, state) do
    %{name: name, shard_index: shard} = state

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} replicate_join key=#{inspect(key)} pid=#{inspect(pid)} from #{node(pid)}"
    end)

    event =
      case Data.pg_lookup(name, shard, cluster, key, pid) do
        nil ->
          Data.pg_insert(name, shard, cluster, key, pid, meta, time, node(pid))
          build_event(name, :joined, key, pid, meta, %{previous_meta: nil, cluster: cluster})

        {old_meta, _old_time, _node} ->
          Data.pg_insert(name, shard, cluster, key, pid, meta, time, node(pid))
          previous_meta = if reason == :update, do: old_meta, else: nil

          build_event(name, :joined, key, pid, meta, %{
            previous_meta: previous_meta,
            cluster: cluster
          })
      end

    notify_monitors(name, [event])
    {:noreply, state}
  end

  def handle_info({:replicate_leave, cluster, key, pid, meta, reason}, state) do
    %{name: name, shard_index: shard} = state

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} replicate_leave key=#{inspect(key)}"
    end)

    case Data.pg_lookup(name, shard, cluster, key, pid) do
      nil ->
        {:noreply, state}

      {_meta, _time, _node} ->
        Data.pg_delete(name, shard, cluster, key, pid)

        event = build_event(name, :left, key, pid, meta, %{reason: reason, cluster: cluster})
        notify_monitors(name, [event])
        {:noreply, state}
    end
  end

  # =====================================================================
  # Peer discovery protocol
  # =====================================================================

  def handle_info(
        {:peer_connect, remote_pid, remote_shard_index, remote_num_shards, remote_clusters},
        state
      )
      when remote_shard_index == state.shard_index do
    if remote_num_shards != state.num_shards do
      raise "Group shard count mismatch: local=#{state.num_shards} remote=#{remote_num_shards} from #{node(remote_pid)}"
    end

    %{name: name, shard_index: shard} = state
    remote_node = node(remote_pid)

    # Add remote node to nil cluster (all Group peers are in nil)
    Data.add_cluster_node(name, nil, remote_node)

    # Compute shared clusters
    my_clusters = Data.my_clusters(name)
    shared = compute_shared_clusters(my_clusters, remote_clusters)

    # Add remote node to shared named clusters
    for cluster <- shared, cluster != nil do
      Data.add_cluster_node(name, cluster, remote_node)
    end

    already_known = Map.has_key?(state.remote_shards, remote_node)

    state =
      if already_known do
        state
      else
        Process.monitor(remote_pid)
        %{state | remote_shards: Map.put(state.remote_shards, remote_node, remote_pid)}
      end

    # Send ack with our cluster list
    send_to_peer(
      state,
      remote_node,
      {:peer_connect_ack, self(), shard, state.num_shards, my_clusters}
    )

    log_once(state, fn ->
      "#{log_prefix(state)} peer_connect from #{remote_node} (#{length(shared)} shared clusters)"
    end)

    # Send cluster_state for all shared clusters in one pass (single table scan
    # instead of one scan per cluster — O(N) vs O(C×N))
    send_cluster_states(state, shared, remote_node)

    {:noreply, state}
  end

  def handle_info({:peer_connect, _remote_pid, _other_shard, _num_shards, _clusters}, state) do
    # Wrong shard index, ignore
    {:noreply, state}
  end

  def handle_info(
        {:peer_connect_ack, remote_pid, remote_shard_index, remote_num_shards, remote_clusters},
        state
      )
      when remote_shard_index == state.shard_index do
    if remote_num_shards != state.num_shards do
      raise "Group shard count mismatch: local=#{state.num_shards} remote=#{remote_num_shards} from #{node(remote_pid)}"
    end

    %{name: name} = state
    remote_node = node(remote_pid)

    # Add remote node to nil cluster
    Data.add_cluster_node(name, nil, remote_node)

    # Compute shared clusters
    my_clusters = Data.my_clusters(name)
    shared = compute_shared_clusters(my_clusters, remote_clusters)

    # Add remote node to shared named clusters
    for cluster <- shared, cluster != nil do
      Data.add_cluster_node(name, cluster, remote_node)
    end

    already_known = Map.has_key?(state.remote_shards, remote_node)

    state =
      if already_known do
        state
      else
        Process.monitor(remote_pid)
        %{state | remote_shards: Map.put(state.remote_shards, remote_node, remote_pid)}
      end

    log_once(state, fn ->
      "#{log_prefix(state)} peer_connect_ack from #{remote_node} (#{length(shared)} shared clusters)"
    end)

    # Send cluster_state for all shared clusters in one pass
    send_cluster_states(state, shared, remote_node)

    {:noreply, state}
  end

  def handle_info({:peer_connect_ack, _remote_pid, _other_shard, _num_shards, _clusters}, state) do
    {:noreply, state}
  end

  # =====================================================================
  # Cluster state (unified handler for peer discovery + cluster join)
  # =====================================================================

  def handle_info({:cluster_state, cluster, reg_data, pg_data}, state) do
    %{name: name} = state

    # Guard: skip merge for named clusters we're not a member of
    if cluster_member?(name, cluster) do
      log_once(state, fn ->
        "#{log_prefix(state)} cluster_state cluster=#{inspect(cluster)} (#{length(reg_data)} reg, #{length(pg_data)} pg entries)"
      end)

      log_verbose(state, fn ->
        "#{log_prefix_shard(state)} merging cluster=#{inspect(cluster)} (#{length(reg_data)} reg, #{length(pg_data)} pg entries)"
      end)

      {state, events} = merge_remote_cluster_data(state, cluster, reg_data, pg_data)
      notify_monitors(name, events)
      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  # =====================================================================
  # Cluster connect/disconnect from remote
  # =====================================================================

  def handle_info({:cluster_connect, clusters, remote_pid}, state) do
    %{name: name} = state
    remote_node = node(remote_pid)

    shared =
      Enum.filter(clusters, fn c ->
        node() in Data.cluster_nodes(name, c)
      end)

    log_once(state, fn ->
      "#{log_prefix(state)} #{remote_node} cluster_connect #{inspect(shared)} (#{length(shared)}/#{length(clusters)} shared)"
    end)

    for cluster <- shared do
      Data.add_cluster_node(name, cluster, remote_node)
    end

    if shared != [] do
      # Bundle this shard's cluster data directly into the ack (one cross-node
      # message instead of ack + N separate cluster_state messages)
      {reg_by_cluster, pg_by_cluster} =
        Data.local_data_by_cluster(name, state.shard_index, shared)

      cluster_data =
        for cluster <- shared do
          reg_data = Map.get(reg_by_cluster, cluster, [])
          pg_data = Map.get(pg_by_cluster, cluster, [])
          {cluster, reg_data, pg_data}
        end

      send_to_peer(state, remote_node, {:cluster_connect_ack, shared, self(), cluster_data})
      fan_out_to_siblings(state, {:send_cluster_data, shared, remote_node})
    end

    {:noreply, state}
  end

  def handle_info({:cluster_connect_ack, clusters, remote_pid, cluster_data}, state) do
    %{name: name} = state
    remote_node = node(remote_pid)

    # Guard: skip if remote node went down (nodedown race) or if we left the
    # cluster (connect+disconnect race). Without these, a delayed ack would
    # re-add a dead/irrelevant node to cluster_nodes permanently.
    {state, events} =
      if Map.has_key?(state.remote_shards, remote_node) do
        active = Enum.filter(clusters, fn c -> node() in Data.cluster_nodes(name, c) end)

        for cluster <- active do
          Data.add_cluster_node(name, cluster, remote_node)
        end

        if active != [] do
          # Merge the data bundled in the ack
          {new_state, events} =
            Enum.reduce(cluster_data, {state, []}, fn {cluster, reg_data, pg_data},
                                                      {acc_state, acc_events} ->
              if cluster in active and (reg_data != [] or pg_data != []) do
                merge_remote_cluster_data(acc_state, cluster, reg_data, pg_data, acc_events)
              else
                {acc_state, acc_events}
              end
            end)

          send_cluster_states(new_state, active, remote_node)
          fan_out_to_siblings(new_state, {:send_cluster_data, active, remote_node})
          {new_state, events}
        else
          {state, []}
        end
      else
        {state, []}
      end

    notify_monitors(name, events)
    {:noreply, state}
  end

  def handle_info({:cluster_disconnect, clusters, remote_pid}, state) do
    %{name: name, shard_index: shard} = state
    remote_node = node(remote_pid)

    log_once(state, fn ->
      "#{log_prefix(state)} #{remote_node} cluster_disconnect #{inspect(clusters)}"
    end)

    if shard == 0 do
      for cluster <- clusters do
        Data.remove_cluster_node(name, cluster, remote_node)
      end

      fan_out_to_siblings(state, {:cluster_disconnect, clusters, remote_pid})
    end

    events =
      Enum.reduce(clusters, [], fn cluster, acc ->
        {purged_reg, purged_pg} = purge_cluster_entries(name, shard, cluster, remote_node)
        build_purged_events(name, purged_reg, purged_pg, :cluster_disconnect, acc)
      end)

    notify_monitors(name, events)
    {:noreply, state}
  end

  # =====================================================================
  # Node up/down
  # =====================================================================

  def handle_info({:nodeup, remote_node}, state) do
    %{shard_index: shard, name: name} = state
    shard_registered_name = shard_name(name, shard)

    send(
      {shard_registered_name, remote_node},
      {:peer_connect, self(), shard, state.num_shards, Data.my_clusters(name)}
    )

    {:noreply, state}
  end

  def handle_info({:nodedown, dead_node}, state) do
    %{name: name, shard_index: shard} = state

    # Remove cluster memberships from shared tables. Every shard calls this
    # unconditionally (not just shard 0) to handle the race where a non-zero
    # shard processes a late peer_connect from the dead node (re-adding it to
    # cluster_nodes) AFTER shard 0's nodedown already cleaned it. Since :bag
    # delete_object is idempotent, redundant calls from multiple shards are safe.
    Data.purge_cluster_node(name, dead_node)

    # Purge all data from the dead node
    {purged_reg, purged_pg} = Data.purge_node(name, shard, dead_node)

    log_once(state, fn ->
      "#{log_prefix(state)} nodedown #{dead_node} (purged #{length(purged_reg)} reg, #{length(purged_pg)} pg entries)"
    end)

    events = build_purged_events(name, purged_reg, purged_pg, :nodedown)
    notify_monitors(name, events)
    state = %{state | remote_shards: Map.delete(state.remote_shards, dead_node)}
    {:noreply, state}
  end

  # =====================================================================
  # Process DOWN
  # =====================================================================

  def handle_info({:DOWN, _mref, :process, pid, reason}, state) do
    %{name: name, shard_index: shard} = state

    remote_node = node(pid)

    if remote_node != node() and Map.get(state.remote_shards, remote_node) == pid do
      # Remote shard process died — purge its cluster memberships and node data.
      # Unconditional (not gated on shard 0) — same reasoning as nodedown handler.
      Data.purge_cluster_node(name, remote_node)
      {purged_reg, purged_pg} = Data.purge_node(name, shard, remote_node)

      log_verbose(state, fn ->
        "#{log_prefix_shard(state)} remote_shard_down #{remote_node} (purged #{length(purged_reg)} reg, #{length(purged_pg)} pg)"
      end)

      events = build_purged_events(name, purged_reg, purged_pg, {:nodedown, remote_node})
      notify_monitors(name, events)
      state = %{state | remote_shards: Map.delete(state.remote_shards, remote_node)}
      state = %{state | monitors: Map.delete(state.monitors, pid)}
      {:noreply, state}
    else
      # Regular process died — clean up its entries in this shard
      # Batch-delete from by_pid tables (one match_delete per table type)
      # and individual deletes from by_key tables
      {purged_reg, purged_pg} = Data.delete_all_for_pid(name, shard, pid)

      log_verbose(state, fn ->
        "#{log_prefix_shard(state)} process_down pid=#{inspect(pid)} reason=#{inspect(reason)} (#{length(purged_reg) + length(purged_pg)} entries cleaned)"
      end)

      events =
        Enum.reduce(purged_reg, [], fn {cluster, key, meta, _time, _node}, acc ->
          broadcast_to_cluster(
            state,
            cluster,
            {:replicate_unregister, cluster, key, pid, meta, reason}
          )

          [
            build_event(name, :unregistered, key, pid, meta, %{reason: reason, cluster: cluster})
            | acc
          ]
        end)

      events =
        Enum.reduce(purged_pg, events, fn {cluster, key, meta, _time, _node}, acc ->
          broadcast_to_cluster(
            state,
            cluster,
            {:replicate_leave, cluster, key, pid, meta, reason}
          )

          [build_event(name, :left, key, pid, meta, %{reason: reason, cluster: cluster}) | acc]
        end)

      notify_monitors(name, events)
      state = %{state | monitors: Map.delete(state.monitors, pid)}
      {:noreply, state}
    end
  end

  def handle_info({:send_cluster_data, clusters, target_node}, state) do
    %{name: name} = state

    active = Enum.filter(clusters, fn c -> node() in Data.cluster_nodes(name, c) end)

    if active != [] do
      send_cluster_states(state, active, target_node)
    end

    {:noreply, state}
  end

  def handle_info({:group_dispatch, pids, message}, state) do
    for pid <- pids, do: send(pid, message)
    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # =====================================================================
  # Internal helpers
  # =====================================================================

  defp send_to_peer(state, target_node, message) do
    shard_name = shard_name(state.name, state.shard_index)
    send({shard_name, target_node}, message)
  end

  defp broadcast_to_peers(state, message) do
    shard_name = shard_name(state.name, state.shard_index)

    for {target_node, _pid} <- state.remote_shards do
      send({shard_name, target_node}, message)
    end
  end

  defp broadcast_to_cluster(state, nil = _cluster, message) do
    broadcast_to_peers(state, message)
  end

  defp broadcast_to_cluster(state, cluster, message) do
    %{name: name} = state
    shard_name = shard_name(name, state.shard_index)

    for target_node <- Data.cluster_nodes(name, cluster), target_node != node() do
      send({shard_name, target_node}, message)
    end
  end

  defp fan_out_to_siblings(state, message) do
    %{name: name, shard_index: shard_index, num_shards: num_shards} = state

    for i <- 0..(num_shards - 1), i != shard_index do
      send(shard_name(name, i), message)
    end
  end

  defp monitor_pid(state, pid) do
    Map.get(state.monitors, pid) || Process.monitor(pid)
  end

  defp put_monitor(state, pid, mref) do
    %{state | monitors: Map.put_new(state.monitors, pid, mref)}
  end

  defp maybe_demonitor_pid(state, name, shard, pid) do
    case Data.maybe_demonitor(name, shard, pid) do
      :ok ->
        case Map.pop(state.monitors, pid) do
          {nil, monitors} ->
            %{state | monitors: monitors}

          {mref, monitors} ->
            Process.demonitor(mref, [:flush])
            %{state | monitors: monitors}
        end

      :still_monitored ->
        state
    end
  end

  defp rebuild_monitors(state) do
    %{name: name, shard_index: shard} = state
    local = node()

    # Scan all entries in this shard's tables and re-establish monitors
    # Only monitor local pids — remote pids are cleaned up by their owning
    # node's DOWN handler (broadcast) or by nodedown.
    reg_table = Data.reg_by_pid_table(name, shard)

    monitors =
      try do
        :ets.foldl(
          fn {{pid, _cluster, _key}, _meta, _time, entry_node}, acc ->
            if entry_node == local and not Map.has_key?(acc, pid) do
              mref = Process.monitor(pid)
              Map.put(acc, pid, mref)
            else
              acc
            end
          end,
          %{},
          reg_table
        )
      rescue
        ArgumentError -> %{}
      end

    pg_table = Data.pg_by_pid_table(name, shard)

    monitors =
      try do
        :ets.foldl(
          fn {{pid, _cluster, _key}, _meta, _time, entry_node}, acc ->
            if entry_node == local and not Map.has_key?(acc, pid) do
              mref = Process.monitor(pid)
              Map.put(acc, pid, mref)
            else
              acc
            end
          end,
          monitors,
          pg_table
        )
      rescue
        ArgumentError -> monitors
      end

    %{state | monitors: monitors}
  end

  defp cluster_member?(name, cluster) do
    node() in Data.cluster_nodes(name, cluster)
  end

  # Additive merge: inserts new entries and resolves conflicts, but does not
  # delete local entries missing from the incoming snapshot. This is safe because
  # Erlang dist uses TCP — either all replicate_* messages arrive in order (no
  # stale entries) or the connection dies and nodedown purges everything before
  # cluster_state can arrive. There is no case where stale entries survive into
  # the merge.
  defp merge_remote_cluster_data(state, cluster, reg_data, pg_data, events \\ []) do
    %{name: name, shard_index: shard, num_shards: num_shards} = state

    # Registry merge uses Enum.reduce to thread state + events, because
    # resolve_conflict modifies state.monitors (demonitor evicted local pids)
    # and may produce an event.
    {state, events} =
      Enum.reduce(reg_data, {state, events}, fn {key, pid, meta, time}, {acc_state, acc_events} ->
        if shard_index_for(cluster, key, num_shards) != shard do
          {acc_state, acc_events}
        else
          case Data.registry_lookup(name, shard, cluster, key) do
            nil ->
              Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))
              event = build_event(name, :registered, key, pid, meta, %{cluster: cluster})
              {acc_state, [event | acc_events]}

            {^pid, _meta, existing_time, _node} ->
              # Same pid (bounceback or metadata update) — apply if newer
              if time > existing_time do
                Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))
              end

              {acc_state, acc_events}

            {existing_pid, existing_meta, existing_time, existing_node}
            when existing_node == node() ->
              # Local entry vs incoming remote — use full conflict resolution
              # (kills loser, re-broadcasts winner, invokes user's resolver)
              {new_state, event} =
                resolve_conflict(
                  acc_state,
                  cluster,
                  key,
                  {existing_pid, existing_meta, existing_time},
                  {pid, meta, time}
                )

              acc_events = if event, do: [event | acc_events], else: acc_events
              {new_state, acc_events}

            {existing_pid, _meta, existing_time, _node} when time > existing_time ->
              # Both remote — keep the more recent one
              if existing_pid != pid do
                Data.registry_delete(name, shard, cluster, key, existing_pid)
              end

              Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))
              {acc_state, acc_events}

            _ ->
              {acc_state, acc_events}
          end
        end
      end)

    events =
      Enum.reduce(pg_data, events, fn {key, pid, meta, time}, acc_events ->
        if shard_index_for(cluster, key, num_shards) != shard do
          acc_events
        else
          case Data.pg_lookup(name, shard, cluster, key, pid) do
            nil ->
              Data.pg_insert(name, shard, cluster, key, pid, meta, time, node(pid))
              [build_event(name, :joined, key, pid, meta, %{cluster: cluster}) | acc_events]

            {_meta, existing_time, _node} when time > existing_time ->
              Data.pg_insert(name, shard, cluster, key, pid, meta, time, node(pid))
              acc_events

            _ ->
              acc_events
          end
        end
      end)

    {state, events}
  end

  defp resolve_conflict(
         state,
         cluster,
         key,
         {local_pid, local_meta, local_time},
         {remote_pid, remote_meta, remote_time}
       ) do
    %{name: name, shard_index: shard} = state

    config = Group.get_config(name)

    winner_pid =
      case Map.get(config, :resolve_registry_conflict) do
        nil ->
          # Default: keep most recent, kill loser
          default_resolve_conflict(
            name,
            cluster,
            key,
            {local_pid, local_meta, local_time},
            {remote_pid, remote_meta, remote_time}
          )

        {mod, func, extra_args} ->
          apply(mod, func, [
            name,
            key,
            {local_pid, local_meta, local_time},
            {remote_pid, remote_meta, remote_time} | extra_args
          ])
      end

    cond do
      winner_pid == remote_pid ->
        # Remote wins — replace local entry
        Data.registry_delete(name, shard, cluster, key, local_pid)
        state = maybe_demonitor_pid(state, name, shard, local_pid)
        time = System.system_time()

        Data.registry_insert(
          name,
          shard,
          cluster,
          key,
          remote_pid,
          remote_meta,
          time,
          node(remote_pid)
        )

        # Dispatch lifecycle events so monitors see the eviction.
        # The :registered event for remote_pid will arrive via the winner's
        # re-broadcast (replicate_register), so we only dispatch :unregistered here.
        event =
          build_event(name, :unregistered, key, local_pid, local_meta, %{
            reason: :resolve_conflict,
            cluster: cluster
          })

        {state, event}

      winner_pid == local_pid ->
        # Local wins — re-broadcast to override remote
        time = System.system_time()

        Data.registry_insert(
          name,
          shard,
          cluster,
          key,
          local_pid,
          local_meta,
          time,
          node(local_pid)
        )

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_register, cluster, key, local_pid, local_meta, time, :resolve_conflict}
        )

        {state, nil}

      true ->
        # Neither wins — remove both
        Data.registry_delete(name, shard, cluster, key, local_pid)
        state = maybe_demonitor_pid(state, name, shard, local_pid)

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_unregister, cluster, key, local_pid, local_meta, :resolve_conflict}
        )

        event =
          build_event(name, :unregistered, key, local_pid, local_meta, %{
            reason: :resolve_conflict,
            cluster: cluster
          })

        {state, event}
    end
  end

  defp default_resolve_conflict(_name, _cluster, key, {pid1, _meta1, time1}, {pid2, meta2, time2}) do
    # Tiebreaker must be deterministic regardless of which node is resolving.
    # Using `>=` would pick the remote on BOTH nodes when timestamps are equal,
    # causing mutual kill (both processes die, key becomes unregistered).
    # Erlang pids have a total order (by node name then id), so pid comparison
    # gives a consistent tiebreaker across all nodes.
    {winner_pid, loser_pid} =
      if time2 > time1 or (time2 == time1 and pid2 > pid1), do: {pid2, pid1}, else: {pid1, pid2}

    Logger.error(fn ->
      "#{inspect(__MODULE__)}: registry conflict detected: key=#{inspect(key)}, " <>
        "pid1=#{inspect(pid1)}, pid2=#{inspect(pid2)}, picking #{inspect(winner_pid)} as winner"
    end)

    Process.exit(loser_pid, {:group_registry_conflict, key, meta2})
    winner_pid
  end

  # Gather local data for all shared clusters in ONE table scan (instead of C scans)
  # and send per-cluster cluster_state messages. One O(N) scan vs C × O(N) scans.
  defp send_cluster_states(state, clusters, target_node) do
    %{name: name, shard_index: shard} = state
    {reg_by_cluster, pg_by_cluster} = Data.local_data_by_cluster(name, shard, clusters)

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} sending cluster_states to #{target_node} (#{length(clusters)} clusters)"
    end)

    for cluster <- clusters do
      reg_data = Map.get(reg_by_cluster, cluster, [])
      pg_data = Map.get(pg_by_cluster, cluster, [])

      if reg_data != [] or pg_data != [] do
        send_to_peer(state, target_node, {:cluster_state, cluster, reg_data, pg_data})
      end
    end
  end

  defp compute_shared_clusters(my_clusters, remote_clusters) do
    my_set = MapSet.new(my_clusters)
    remote_set = MapSet.new(remote_clusters)
    MapSet.intersection(my_set, remote_set) |> MapSet.to_list()
  end

  defp purge_cluster_entries(name, shard, cluster, target_node) do
    # Remove entries for a specific cluster and node
    reg_table = Data.reg_by_key_table(name, shard)
    reg_pid_table = Data.reg_by_pid_table(name, shard)

    purged_reg =
      :ets.select(reg_table, [
        {{{cluster, :"$1"}, :"$2", :"$3", :"$4", :"$5"}, [{:==, :"$5", target_node}],
         [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])
      |> Enum.map(fn {key, pid, meta, time} -> {cluster, key, pid, meta, time} end)

    for {^cluster, key, pid, _meta, _time} <- purged_reg do
      :ets.delete(reg_table, {cluster, key})
      :ets.delete(reg_pid_table, {pid, cluster, key})
    end

    pg_table = Data.pg_by_key_table(name, shard)
    pg_pid_table = Data.pg_by_pid_table(name, shard)

    purged_pg =
      :ets.select(pg_table, [
        {{{cluster, :"$1", :"$2"}, :"$3", :"$4", :"$5"}, [{:==, :"$5", target_node}],
         [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])
      |> Enum.map(fn {key, pid, meta, time} -> {cluster, key, pid, meta, time} end)

    for {^cluster, key, pid, _meta, _time} <- purged_pg do
      :ets.delete(pg_table, {cluster, key, pid})
      :ets.delete(pg_pid_table, {pid, cluster, key})
    end

    {purged_reg, purged_pg}
  end

  defp build_purged_events(name, purged_reg, purged_pg, reason, events \\ []) do
    events =
      Enum.reduce(purged_reg, events, fn {cluster, key, pid, meta, _time}, acc ->
        [
          build_event(name, :unregistered, key, pid, meta, %{reason: reason, cluster: cluster})
          | acc
        ]
      end)

    Enum.reduce(purged_pg, events, fn {cluster, key, pid, meta, _time}, acc ->
      [build_event(name, :left, key, pid, meta, %{reason: reason, cluster: cluster}) | acc]
    end)
  end

  defp build_event(name, event_type, key, pid, meta, extra) do
    extract_fn = extract_meta_fn(name)

    %Group.Event{
      type: event_type,
      supervisor: name,
      cluster: Map.get(extra, :cluster),
      key: key,
      pid: pid,
      meta: extract_fn.(meta),
      previous_meta:
        case Map.get(extra, :previous_meta) do
          nil -> nil
          prev -> extract_fn.(prev)
        end,
      reason: Map.get(extra, :reason)
    }
  end

  defp extract_meta_fn(name) do
    case Group.get_config(name) do
      %{extract_meta: {mod, func, args}} -> fn meta -> apply(mod, func, [meta | args]) end
      _ -> & &1
    end
  end

  # =====================================================================
  # Monitor notification
  # =====================================================================

  defp notify_monitors(_name, []), do: :ok

  defp notify_monitors(name, events) do
    events = Enum.reverse(events)
    events_by_cluster = Enum.group_by(events, & &1.cluster)

    subscriber_events =
      Enum.reduce(events_by_cluster, %{}, fn {cluster, cluster_events}, acc ->
        subs = get_subscribers_with_patterns(name, cluster)

        Enum.reduce(cluster_events, acc, fn event, inner ->
          matching_pids =
            for {pattern, sub_pid} <- subs,
                matches_pattern?(pattern, event.key),
                uniq: true,
                do: sub_pid

          Enum.reduce(matching_pids, inner, fn sub_pid, acc ->
            Map.update(acc, sub_pid, [event], &[event | &1])
          end)
        end)
      end)

    for {sub_pid, sub_events} <- subscriber_events do
      send(sub_pid, {:group, Enum.reverse(sub_events), %{name: name}})
    end

    :ok
  end

  defp matches_pattern?(:all, _key), do: true
  defp matches_pattern?({:exact, pattern}, key), do: pattern == key
  defp matches_pattern?({:prefix, prefix}, key), do: String.starts_with?(key, prefix)

  defp get_subscribers_with_patterns(name, cluster) do
    Group.registry_name(name)
    |> Registry.select([
      {
        {{:"$1", :"$2", :"$3"}, :"$4", :_},
        [{:andalso, {:==, :"$1", name}, {:==, :"$2", cluster}}],
        [{{:"$3", :"$4"}}]
      }
    ])
    |> Enum.uniq()
  rescue
    ArgumentError -> []
  end

  # =====================================================================
  # Logging helpers
  # =====================================================================

  defp log(state, message_fn) when is_function(message_fn, 0) do
    case Group.get_config(state.name) do
      %{log: false} -> :ok
      _ -> Logger.info(message_fn)
    end
  end

  defp log_verbose(state, message_fn) when is_function(message_fn, 0) do
    case Group.get_config(state.name) do
      %{log: :verbose} -> Logger.info(message_fn)
      _ -> :ok
    end
  end

  defp log_once(state, message_fn) do
    if state.shard_index == 0, do: log(state, message_fn)
  end

  defp log_prefix(state) do
    "[Group #{inspect(state.name)}]"
  end

  defp log_prefix_shard(state) do
    "[Group #{inspect(state.name)}/#{state.shard_index}]"
  end
end
