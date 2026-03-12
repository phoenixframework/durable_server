# Group — CLAUDE.md

## What is Group

Distributed process registry + process groups + lifecycle monitoring + isolated subclusters.

## Project Structure

```
lib/
  group.ex              — Public API: register, join, members, monitor, dispatch, connect/disconnect
  group/event.ex        — %Group.Event{} struct
  group/supervisor.ex   — Top-level supervisor (rest_for_one: Data → Replica.Supervisor → Registry)
  group/replica/
    data.ex             — GenServer that owns ETS tables. Pure function API for all ETS ops.
    supervisor.ex       — one_for_one supervisor for Replica shards
  group/replica.ex      — Sharded GenServer: replication, peer discovery, conflict resolution, monitoring
  group/application.ex  — Empty app supervisor (Group instances are started by consumers)
test/
  test_helper.exs       — Starts distribution (Node.start), disables prevent_overlapping_partitions
  group_test.exs        — Local tests (async: true)
  distributed_test.exs  — Multi-node tests using OTP :peer
  support/
    test_cluster.ex     — Peer node helpers (start_peers, spawn_register, spawn_join, etc.)
    test_conflict_resolver.ex — Custom resolver for tests
priv/bench/             — Benchmarks (run_local.sh, run_distributed.sh)
```

## Running Tests

```bash
mix test           # all tests
mix test test/group_test.exs           # local only
mix test test/distributed_test.exs     # distributed only
```

Tests require `elixirc_paths(:test)` includes `test/support/`. Distributed tests use OTP `:peer` module for real Erlang nodes.

## Running Benchmarks

```bash
cd priv/bench && ./run_local.sh
cd priv/bench && ./run_distributed.sh
```

## Architecture

### Supervision Tree

```
Group.Supervisor (rest_for_one)
├── Group.Replica.Data        — owns all ETS tables, survives shard crashes
├── Group.Replica.Supervisor  — one_for_one, N shard GenServers
│   ├── Replica shard 0
│   ├── Replica shard 1
│   └── ...
└── Registry (Elixir)         — :duplicate, for monitor subscriptions
```

`rest_for_one` means: if Data dies, Replica.Supervisor restarts (rebuilds monitors from surviving ETS). If Replica.Supervisor dies, Registry restarts (monitors re-subscribe).

### Sharding

`phash2({cluster, key}, num_shards)` routes to shard. Default 8 shards. Must match across all nodes (validated on peer_connect). Including cluster in hash avoids false contention between default and named cluster operations.

### ETS Tables (per shard × 4 + 2 shared)

| Table | Type | Key | Tuple |
|-------|------|-----|-------|
| `reg_by_key` | `:set` | `{cluster, key}` | `{{cluster, key}, pid, meta, time, node}` |
| `reg_by_pid` | `:ordered_set` | `{pid, cluster, key}` | `{{pid, cluster, key}, meta, time, node}` |
| `pg_by_key` | `:ordered_set` | `{cluster, key, pid}` | `{{cluster, key, pid}, meta, time, node}` |
| `pg_by_pid` | `:ordered_set` | `{pid, cluster, key}` | `{{pid, cluster, key}, meta, time, node}` |
| `cluster_nodes` | `:bag` | cluster | `{cluster, node}` |
| `node_clusters` | `:bag` | node | `{node, cluster}` |

**Why ordered_set for by_pid tables**: Contiguous range scans for `entries_by_pid` and `delete_all_for_pid` (process death cleanup). Also enables efficient existence checks with `select(..., 1)`.

**Why ordered_set for pg_by_key**: `pg_members/4` scans `{cluster, key, *}` as a contiguous range — O(members in group), not O(table).

**Why set for reg_by_key**: Only needs direct lookup/delete by `{cluster, key}` — O(1).

All tables: `:public`, `read_concurrency: true`, `decentralized_counters: true`. No `write_concurrency` — writes serialize through shard GenServer anyway.

### Reads vs Writes

- **Reads** (`lookup`, `members`, `local_registry_count`) go directly to ETS — no GenServer involved
- **Writes** (`register`, `join`, `leave`, `unregister`) go through shard GenServer via `GenServer.call`
- **Replication** arrives as `handle_info` messages on shard GenServers

### Config

Stored in `persistent_term` keyed by `{Group, name}`. Map with: `num_shards`, `log`, `callbacks`, optionally `extract_meta`, `resolve_registry_conflict`.

## Key Protocols

### Peer Discovery (per-shard, on nodeup/init)

1. Each shard sends `{:peer_connect, pid, shard, num_shards, clusters}` to its counterpart
2. Receiver adds sender to nil cluster ETS, computes shared clusters, sends `{:peer_connect_ack, ...}`
3. Both sides send `{:cluster_state, cluster, reg_data, pg_data}` for each shared cluster
4. `merge_remote_cluster_data` applies data: new entries insert, conflicts go through `resolve_conflict`

### Replication (steady state)

After discovery, writes broadcast to cluster members:
- nil cluster: uses `state.remote_shards` map
- Named clusters: uses `cluster_nodes` ETS table
- Messages: `replicate_register`, `replicate_unregister`, `replicate_join`, `replicate_leave`

### Conflict Resolution

`resolve_conflict/6` handles ALL registry key conflicts (live `replicate_register` AND partition heal `merge_remote_cluster_data`).

- Default resolver: most recent timestamp wins; pid ordering tiebreaker on equal timestamps
- **Tiebreaker MUST be deterministic across all nodes**: `time2 > time1 or (time2 == time1 and pid2 > pid1)`. Using `>=` causes mutual kill.
- Custom: `resolve_registry_conflict: {mod, func, extra_args}` option
- Loser is killed with `{:group_registry_conflict, key, meta}`
- Runs synchronously inside shard GenServer — must return quickly

### Named Cluster Connect/Disconnect

- `connect/2`: adds to ETS, picks random shard S, S notifies remote S, remote acks with bundled data + fans out to siblings
- `disconnect/2`: removes from ETS, calls ALL local shards to purge, shard 0 broadcasts to remotes

### Nodedown / Process Death

- `nodedown`: every shard calls `purge_cluster_node` (unconditional — not gated on shard 0) then `purge_node`
- Process DOWN: `delete_all_for_pid` + broadcast unregister/leave per entry
- Remote shard DOWN (monitored pid): treated like nodedown for that node

### Monitor Events

Events delivered as `{:group, [%Group.Event{}, ...], %{name: name}}`. Batched per handler turn:
- Single ops: one event per message
- Bulk ops (nodedown, process DOWN, cluster_state merge): all events in one message

Patterns: `:all`, `{:exact, key}`, `{:prefix, "prefix/"}`

### Prefix Queries

`Group.members(name, "prefix/")` scans ALL shards (can't hash prefix to one shard). Uses ETS range guards:
```elixir
{:andalso, {:>=, :"$1", prefix}, {:<, :"$1", next_binary_prefix(prefix)}}
```
where `next_binary_prefix` increments the last byte of the prefix string.

Keys ending with `"/"` are rejected by `validate_key!/1` in register/unregister/join/leave — trailing slash is reserved for prefix queries.

### Dispatch

`dispatch/4` sends to all members (registry + PG). Groups remote PG members by node, sends one `{:group_dispatch, pids, message}` per remote node (O(nodes) not O(members)). Target shard chosen by `phash2(self(), num_shards)` for per-sender ordering.

`dispatch_local/4` skips cross-node messaging.

## ETS Match Spec Patterns

- Use `{:==, :"$N", value}` for runtime variables in guards (e.g., filtering by node)
- **NOT** `{:const, value}` — it's invalid in ETS match specs
- Literal Elixir variables interpolate directly into match pattern tuple positions as exact-match filters
- For result bodies, runtime values can't be embedded directly — use `Enum.map` post-select

## Distributed Test Patterns

- `Group.TestCluster.start_peers(N)` starts N real Erlang nodes via OTP `:peer`
- All helpers (`spawn_register`, `spawn_join`, `spawn_monitor_forwarder`) use `:erpc.call` with compiled modules from `test/support/`
- `Node.spawn` with anonymous functions won't work — remote node needs the defining module's beam file
- `assert_eventually/2` polls with retries for async replication
- `flush_shards/2` does `:sys.get_state` on all shards to drain message queues
- `assert_ets_consistent/1` verifies dual-index tables match
- Partition tests use 3 nodes (isolate 1 from other 2). 2-node partitions are unreliable because the test node bridges them.
- `Supervisor.start_link` links to caller — in RPC context, must `Process.unlink(pid)` or supervisor dies on RPC return
- `test_helper.exs`: calls `Node.start/2`, sets cookie, disables `prevent_overlapping_partitions`

## Critical Invariants

1. **purge_cluster_node is unconditional** — every shard calls it on nodedown/DOWN, not just shard 0. Late peer_connect on non-zero shard can re-add dead node after shard 0 cleaned it.
2. **Dispatch :unregistered for evicted local pid** in "remote wins" branch of resolve_conflict — monitors need to see the eviction.
3. **merge_remote_cluster_data uses Enum.reduce** (not `for`) to thread `{state, events}` through, since resolve_conflict modifies `state.monitors`.
4. **Additive merge only** — cluster_state merge inserts but never deletes. TCP ordering guarantees no stale entries survive into the merge.
5. **PG tables have no overwrite conflicts** — `pg_by_key` key includes pid: `{cluster, key, pid}`.
6. **cluster_state handler has cluster_member? guard** — rejects data for clusters we left.

## Logging

- `log:` option: `:info` (default), `false` (silent), `:verbose` (all shards)
- `log/2` (normal), `log_verbose/2` (verbose only), `log_once/2` (shard 0 only)
- All use `Logger.info` — `:verbose` is Group's own flag, not Logger level
- Registry conflict is unconditional `Logger.error`
- Runtime change: `Group.log_level(name, level)`
