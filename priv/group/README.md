# Group

Eventually Consistent distributed process registry, process groups,
lifecycle monitoring, and isolated subclusters for Elixir. No external dependencies.

## Features

- **Registry** — unique key-to-process mapping, cluster-wide. One process per
  key, enforced across all nodes.
- **Process groups** — many processes per key with join/leave. Discoverable via
  `members/2`.
- **Lifecycle monitoring** — pattern-based event subscriptions. Get notified
  when processes register, unregister, join, or leave anywhere in the cluster.
- **Named subclusters** — isolate registries and groups into named clusters
  where only connected nodes participate.
- **Sharded writes** — writes fan out across N GenServer shards to reduce
  contention. Reads go directly to ETS.

## Installation

```elixir
def deps do
  [{:group, "~> 0.1.0"}]
end
```

## Quick Start

Start a Group instance under your supervision tree:

```elixir
children = [
  {Group, name: :my_app}
]
```

### Registry

```elixir
# Register the calling process under a unique key
:ok = Group.register(:my_app, "user/123", %{name: "Alice"})

# Look up by key — returns {pid, meta} or nil
{pid, %{name: "Alice"}} = Group.lookup(:my_app, "user/123")

# Unregister (also happens automatically on process death)
:ok = Group.unregister(:my_app, "user/123")
```

### Process Groups

```elixir
# Join a group (many processes can join the same key)
:ok = Group.join(:my_app, "chat/room/42", %{role: :member})

# List all members — returns [{pid, meta}, ...]
members = Group.members(:my_app, "chat/room/42")

# Leave
:ok = Group.leave(:my_app, "chat/room/42")
```

`members/2` returns joined processes for a key. Registered processes are not
included — use `lookup/2` for those.
Keys ending with `"/"` perform a prefix query across all shards:

```elixir
# All members in rooms under "chat/"
Group.members(:my_app, "chat/")
```

### Monitoring

Subscribe to lifecycle events matching a pattern:

```elixir
# Prefix match — all keys starting with "user/"
:ok = Group.monitor(:my_app, "user/")

# Exact match
:ok = Group.monitor(:my_app, "user/123")

# Everything
:ok = Group.monitor(:my_app, :all)
```

Events arrive as `{:group, events, info}` tuples in the monitoring process's mailbox:

```elixir
def handle_info({:group, events, _info}, state) do
  Enum.each(events, fn
    %Group.Event{type: :registered, key: key, pid: pid, meta: meta} ->
      # a process registered at `key`
      :ok
    %Group.Event{type: :unregistered, key: key, meta: meta, reason: reason} ->
      # a registered process died or unregistered
      :ok
    %Group.Event{type: :joined, key: key, pid: pid, meta: meta} ->
      # a process joined the group at `key`
      :ok
    %Group.Event{type: :left, key: key, pid: pid, meta: meta, reason: reason} ->
      # a process left or died
      :ok
  end)
  {:noreply, state}
end
```

Single operations (register, join) produce one event per tuple. Bulk operations
(nodedown, process death) batch all events from that operation into one tuple.

### Dispatch

Send a message to all members of a key:

```elixir
:ok = Group.dispatch(:my_app, "chat/room/42", {:new_message, "hello"})
```

### Named Clusters

Isolate groups and registries into named subclusters. Only nodes that have
called `connect/2` for a cluster participate in that cluster's replication.

```elixir
# Connect this node to a named cluster
:ok = Group.connect(:my_app, "game_servers_123")

# All operations accept a :cluster option
:ok = Group.join(:my_app, "room/1", %{}, cluster: "game_servers_123")
members = Group.members(:my_app, "room/1", cluster: "game_servers_123")
:ok = Group.monitor(:my_app, :all, cluster: "game_servers_123")
```

### Nodes

```elixir
# All Group peers (nodes that completed peer discovery), excluding self
Group.nodes(:my_app)

# All nodes in a named cluster
Group.nodes(:my_app, "game_servers_123")
```

### Runtime Log Level

Toggle verbose logging at runtime without restart:

```elixir
Group.log_level(:my_app, :verbose)  # turn on verbose
Group.log_level(:my_app, :info)     # back to normal
Group.log_level(:my_app, false)     # silence all Group logs
```

## Events

Events are delivered as `{:group, events, %{name: name}}` tuples containing
`%Group.Event{}` structs:

```elixir
%Group.Event{
  type: :registered | :unregistered | :joined | :left,
  supervisor: :my_app,
  cluster: nil | "cluster_name",
  key: "user/123",
  pid: #PID<0.150.0>,
  meta: %{},
  previous_meta: nil | %{},    # old meta on re-register/re-join
  reason: nil | term()          # exit reason on unregistered/left
}
```

| Event | Trigger |
|---|---|
| `:registered` | `register/4` — new or re-register (updates meta) |
| `:unregistered` | Process died or `unregister/3` called |
| `:joined` | `join/4` — new or re-join (updates meta) |
| `:left` | Process died or `leave/3` called |

Re-registering or re-joining an existing key updates the metadata in place and
delivers an event with `previous_meta` set to the old value.

## Consistency Model

All operations are **eventually consistent**:

- Writes (`register`, `join`, etc.) return immediately after updating local ETS.
- Changes replicate to other nodes asynchronously over Erlang distribution.
- During network partitions, nodes may have divergent views.
- When partitions heal, state is re-synced via `cluster_state` messages.
- Registry conflicts (same key registered on two nodes during a partition) can
  be resolved with a configurable `resolve_registry_conflict` callback. The
  losing process is killed with `{:group_registry_conflict, key, meta}`.

## Configuration

```elixir
{Group,
  name: :my_app,
  shards: 8,                                   # number of write shards (default)
  log: :info,                                  # :info | :verbose | false
  resolve_registry_conflict: {MyResolver, :resolve, []},  # partition conflict resolver
  extract_meta: {MyApp, :extract_meta, []}     # transform meta on read
}
```

### Options

- **`name`** (required) — atom identifying this Group instance. Passed as the
  first argument to all API functions.
- **`shards`** — number of GenServer shards for write operations. Defaults to 8.
  Must match across all nodes.
- **`log`** — logging level. `:info` (default) logs peer discovery, node
  connects/disconnects, and cluster membership changes. `:verbose` additionally
  logs per-shard operations (register, join, leave, process deaths, replication).
  `false` disables all Group log output. All log output uses `Logger.info`.
  Can be changed at runtime with `Group.log_level/2`.
- **`resolve_registry_conflict`** — `{module, function, extra_args}` callback
  invoked as `apply(mod, fun, [name, key, {pid1, meta1, time1}, {pid2, meta2, time2} | extra_args])`.
  Called when partition healing or concurrent registration finds the same key
  registered on two nodes. Must return the winning pid. Runs synchronously
  inside the shard GenServer — must return quickly and never block.
- **`extract_meta`** — `{module, function, args}` or `fun(meta)` applied to
  metadata on reads (`lookup`, `members`). Useful for stripping internal fields.

## Architecture

```
Group.Supervisor (:"my_app_group_sup")
├── Group.Replica.Data        — owns all ETS tables, survives shard crashes
├── Group.Replica.Supervisor  — supervises N shard GenServers
│   ├── Group.Replica (shard 0)
│   ├── Group.Replica (shard 1)
│   └── ...
└── Registry                  — local monitor subscriptions (:"my_app_group_registry")
```

### Sharding

Keys are routed to shards via `:erlang.phash2({cluster, key}, num_shards)`.
Including the cluster in the hash avoids false contention between the default
cluster and named clusters.

**Reads** (`lookup`, `members`) go directly to ETS — no GenServer hop. This is
the hot path and runs at millions of ops/sec.

**Writes** (`register`, `join`, etc.) go through the shard's GenServer, which
updates ETS and broadcasts replication messages. Multiple shards reduce write
contention for unrelated keys.

### ETS Tables

Each shard owns 4 ETS tables:

| Table | Type | Key | Purpose |
|---|---|---|---|
| `reg_by_key` | `:set` | `{cluster, key}` | Registry lookup — O(1) |
| `reg_by_pid` | `:ordered_set` | `{pid, cluster, key}` | Reverse index for death cleanup |
| `pg_by_key` | `:ordered_set` | `{cluster, key, pid}` | Group membership lookup |
| `pg_by_pid` | `:ordered_set` | `{pid, cluster, key}` | Reverse index for death cleanup |

Plus 2 shared tables: `cluster_nodes` (`:bag`, cluster→nodes) and
`node_clusters` (`:bag`, node→clusters) providing dual-index cluster membership
lookups.

`Group.Replica.Data` owns all tables and is supervised with `rest_for_one` so
tables survive shard crashes.

### Peer Discovery

When Group starts (or a new Erlang node connects), shards exchange
`peer_connect` / `peer_connect_ack` messages with their counterparts on other
nodes. This handshake:

1. Validates that shard counts match (raises on mismatch).
2. Exchanges cluster membership lists.
3. Sends `cluster_state` snapshots for shared clusters — the full registry and
   group data, delivered in a single message per cluster.

This is how a new node catches up to the existing cluster state.

### Replication

After the initial sync, steady-state changes propagate via broadcast messages
(`replicate_register`, `replicate_join`, etc.) sent from the writing shard to
the corresponding shard on all peer nodes in the relevant cluster.

### Process Death Cleanup

Shards monitor all registered/joined processes. On `DOWN`, the shard:

1. Removes entries from both the primary and reverse-index ETS tables.
2. Broadcasts `replicate_unregister` / `replicate_leave` to peer nodes.
3. Fires `:unregistered` / `:left` events to local monitors.

### Node Disconnect

On `nodedown`, each shard purges all entries owned by the disconnected node
from its ETS tables and fires events for each removed entry.

## Testing

```bash
mix test
```

See [`test/README.md`](test/README.md) for details on the distributed test
infrastructure.

## Benchmarks

```bash
cd priv/bench

# Local (single-node)
./run_local.sh

# Distributed (3 separate BEAM VMs)
./run_distributed.sh
./run_distributed.sh --shards 4
```

See [`priv/bench/README.md`](priv/bench/README.md) for scenario descriptions.

## License

MIT
