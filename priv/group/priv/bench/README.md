# Group Benchmarks

Baseline performance numbers for Group's core operations — local ETS reads,
GenServer writes with shard scaling, and distributed replication across
separate BEAM VMs.

## Running

```bash
cd priv/group/priv/bench
mix deps.get
```

### Local benchmarks

Single-node, no distribution required:

```bash
./run_local.sh
```

### Distributed benchmarks

Uses 3 separate BEAM VMs (coordinator + 2 replicas) as OS processes:

```bash
./run_distributed.sh
```

The script compiles once, starts both replicas in the background, then launches
the coordinator. Replicas are killed automatically on exit.

## Local Scenarios

All local benchmarks run for both the default (nil) cluster and a named cluster
(`"game"`) to verify there's no performance difference between the two paths.

### 1. Lookup throughput

Pure ETS read — the hot path for Group. Registers 10K keys, then measures 100K
random `Group.lookup/3` calls.

Reports ops/sec and p50/p99/max latency.

### 2. Members throughput

ETS read that returns a list. Joins 100 processes to each of 100 groups (10K
memberships total), then measures 100K random `Group.members/3` calls.

Slower than lookup because each call copies a 100-element list out of ETS.

### 3. Register throughput (shard scaling)

Measures concurrent `Group.register/4` calls — each of 10K spawned processes
registers itself in parallel. Varies shard count (1, 2, 4, schedulers_online)
to show how write throughput scales with sharding.

### 4. Register/unregister cycle

Sequential register + unregister pairs from a single process. Measures the
GenServer round-trip cost of two writes back-to-back (10K cycles).

Reports per-cycle latency percentiles.

### 5. Join throughput (shard scaling)

Same shape as register throughput but with `Group.join/4`. 10K processes each
join a group concurrently, varying shard count.

### 6. Monitor event delivery

Calls `Group.monitor(:bench, :all)`, then registers 5K keys and measures the
time until all 5K `:registered` events are received by the monitoring process.

## Distributed Scenarios

Three separate BEAM VMs on 127.0.0.1 — each with its own schedulers, memory
allocator, and GC. The coordinator drives all operations via `:erpc.call` with
MFA (module/function/args) to the replica nodes.

### 1. Replication latency

The core distributed measurement. Registers a key on replica1, then spin-polls
`Group.lookup` on replica2 until it appears. Repeats 1,000 times.

Reports p50/p99/max latency covering the full path: GenServer call on replica1,
Erlang distribution message, GenServer cast on replica2, ETS insert.

### 2. Bulk sync (new peer catches up)

Measures how fast a new node catches up to an existing peer's state. Registers
N keys on replica1 (1K and 10K), then starts Group on replica2 and polls until
all N entries are visible.

Group sends all data in a single `cluster_state` message on peer discovery, so
this is bounded by serialization + network, not per-key round-trips.

### 3. Concurrent cross-node writes

Both replicas register 5K keys simultaneously (10K total), then waits for full
convergence — both nodes see all 10K entries.

Measures total throughput including the time for all replication messages to
settle.

### 4. Named cluster replication latency

Same as scenario 1 (replication latency) but within a named cluster
(`"game"`). Both replicas call `Group.connect/2` before measuring.

Useful for verifying that the named cluster replication path has no overhead
compared to the default nil cluster.

### 5. Process death cleanup replication

The critical distributed cleanup path. Registers 1K and 5K processes on
replica1, kills them all, then measures how long until replica2 sees zero
entries. Exercises: local DOWN handler → `replicate_unregister` broadcast →
remote ETS cleanup.

This scenario catches O(N²) message amplification bugs where remote nodes
redundantly monitor pids and re-broadcast cleanup messages.

### 6. Register/die churn throughput

Sustained churn: 10 waves of 500 register+kill cycles on replica1, measuring
total wall time including convergence on replica2. Simulates steady-state
deploy churn where processes are constantly starting and stopping.

### 7. Join/die cleanup replication

Same as scenario 5 but for process groups. Spawns 1K processes on replica1,
all joining the same group key, then kills them all. Measures cleanup
convergence on replica2 via the `replicate_leave` path.

All members hash to the same shard (single key), making this the worst case
for shard contention during bulk cleanup.

## Architecture

```
priv/group/priv/bench/
├── mix.exs                          # depends on :group via path: "../../"
├── run_distributed.sh               # starts 3 VMs, cleans up on exit
├── README.md
├── lib/
│   ├── group_bench.ex               # CLI entry — dispatches local/distributed
│   ├── group_bench/
│   │   ├── local.ex                 # 6 local benchmarks
│   │   ├── distributed.ex           # coordinator: connects + drives 7 benchmarks
│   │   ├── replica.ex               # helpers called by coordinator via :erpc
│   │   └── helpers.ex               # timing, formatting, percentile math
```

The bench suite is a standalone Mix project that depends on `:group`. Protocol
consolidation is enabled for realistic production performance. Distributed
benchmarks use separate OS processes (not `:peer`) so each node gets its own
scheduler pool and memory allocator.
