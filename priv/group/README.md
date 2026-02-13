# Group

Distributed process groups, registry, lifecycle monitoring, and isolated subclusters for Elixir.

Built on top of [syn](https://github.com/ostinelli/syn), Group provides a higher-level API for:

- **Distributed registry** - Unique key-to-process mapping across all nodes
- **Process groups** - Multiple processes per key with join/leave semantics
- **Lifecycle monitoring** - Pattern-matched event subscriptions for registry and group changes
- **Isolated subclusters** - Partition groups and registries into named clusters

## Installation

Add `group` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:group, "~> 0.1.0"}
  ]
end
```

## Quick Start

Start a Group under your supervision tree:

```elixir
children = [
  {Group, name: :my_group}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

### Joining and Querying Groups

```elixir
# Join a group with metadata
:ok = Group.join(:my_group, "chat/room/42", %{role: :member})

# List all members
members = Group.members(:my_group, "chat/room/42")
# => [{#PID<0.150.0>, %{role: :member}}]

# Leave the group
:ok = Group.leave(:my_group, "chat/room/42")
```

### Monitoring Lifecycle Events

```elixir
# Monitor all keys under a prefix
:ok = Group.monitor(:my_group, "chat/")

# Monitor a specific key
:ok = Group.monitor(:my_group, "user/123")

# Monitor everything
:ok = Group.monitor(:my_group, :all)

# Events arrive as %Group.Event{} structs
receive do
  %Group.Event{type: :joined, key: key, pid: pid, meta: meta} ->
    IO.puts("#{inspect(pid)} joined #{key}")

  %Group.Event{type: :left, key: key, pid: pid, reason: reason} ->
    IO.puts("#{inspect(pid)} left #{key}: #{inspect(reason)}")
end
```

### Dispatching Messages

```elixir
# Send a message to all members of a key
:ok = Group.dispatch(:my_group, "chat/room/42", {:new_message, "hello"})
```

### Named Clusters

Isolate groups into named subclusters where only connected nodes participate:

```elixir
# Connect to a named cluster
:ok = Group.connect(:my_group, :game_servers)

# Operations scoped to the cluster
:ok = Group.join(:my_group, "room/1", %{}, cluster: :game_servers)
members = Group.members(:my_group, "room/1", cluster: :game_servers)
:ok = Group.monitor(:my_group, :all, cluster: :game_servers)
```

## Event Types

Events are delivered as `%Group.Event{}` structs:

| Event | Trigger |
|---|---|
| `:registered` | Process registered via `register/4` |
| `:unregistered` | Registered process died or was unregistered |
| `:joined` | Process joined via `join/4` |
| `:left` | Joined process died or called `leave/3` |

## Consistency Model

All operations are **eventually consistent**. Writes return immediately after local update, and other nodes receive updates asynchronously via Erlang distribution. During network partitions, nodes may have divergent views. When partitions heal, conflicts are resolved automatically.

## License

MIT License. See [LICENSE.md](LICENSE.md) for details.
