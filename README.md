# DurableServer

DurableServer provides durable, distributed GenServer processes backed by object storage.

It implements fault-tolerant, stateful processes that can survive node failures, restarts, and deployments by automatically persisting state to S3-compatible object storage (like [Tigris](https://www.tigrisdata.com/)) and coordinating across a distributed cluster.

## Key Features

- **Durable state**: Automatically persists state to object storage with configurable sync intervals
- **Cluster coordination**: Uses distributed registry for process discovery and health monitoring
- **Capacity-aware placement**: Monitors CPU, memory, and disk usage to route new processes to nodes with available capacity
- **Sticky placement**: Environment variable-based placement preferences (e.g., same machine, same region, etc.) with time-gated fallback to ensure servers restart on preferred nodes when possible
- **Automatic recovery**: Failed processes are detected and restarted across the cluster
- **Graceful shutdown**: Ensures state is synchronized before termination
- **PubSub**: Subscribe to lifecycle events and broadcast messages between DurableServers and other processes (e.g., Phoenix Channels)

## Installation

Add `durable_server` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:durable_server, "~> 0.1.0"}
  ]
end
```

For releases, add `:os_mon` to `extra_applications`:

```elixir
def application do
  [
    mod: {MyApp.Application, []},
    extra_applications: [:logger, :runtime_tools, :os_mon]
  ]
end
```

## Basic Usage

```elixir
defmodule MyCounterServer do
  use DurableServer, vsn: 1

  def dump_state(state), do: %{count: state.count}

  def load_state(_old_vsn, %{"count" => count}), do: %{count: count}

  def init(%{count: count} = state) do
    {:ok, Map.merge(state, %{started_at: DateTime.utc_now()})}
  end

  def handle_call(:increment, _from, state) do
    new_state = %{state | count: state.count + 1}
    {:reply, new_state.count, new_state}
  end

  def handle_call(:get_count, _from, state) do
    {:reply, state.count, state}
  end
end
```

Start the supervisor (typically in your application supervision tree):

```elixir
children = [
  {DurableServer.Supervisor,
   name: MyDurableSup,
   prefix: "my_app/",
   object_store: [
     bucket: "my-bucket",
     access_key_id: System.fetch_env!("DURABLE_AWS_ACCESS_KEY_ID"),
     secret_access_key: System.fetch_env!("DURABLE_AWS_SECRET_ACCESS_KEY"),
     s3_endpoint: System.fetch_env!("DURABLE_AWS_ENDPOINT_URL_S3"),
     default_region: System.fetch_env!("DURABLE_AWS_REGION")
   ]}
]
```

Start and use individual servers:

```elixir
{:ok, {pid, _meta}} = DurableServer.Supervisor.start_child(
  MyDurableSup,
  {MyCounterServer, %{key: "user_123", count: 0}}
)

GenServer.call(pid, :increment)  # => 1
GenServer.call(pid, :increment)  # => 2
GenServer.call(pid, :get_count)  # => 2
```

## Configuration Options

DurableServer supports these options in the `init/1` return tuple:

- `:auto_sync` - Enable automatic periodic syncing (default: false)
- `:sync_every_ms` - Sync interval in milliseconds (default: 30_000)
- `:meta` - Optional metadata included in the global registry

## State Synchronization

State is synchronized to storage in these scenarios:

1. **Manual sync**: Return `:sync` from any callback: `{:noreply, state, :sync}`
2. **Automatic sync**: When `:auto_sync` is enabled, changes sync on the `:sync_every_ms` interval
3. **Graceful shutdown**: State is always synced before termination

## PubSub

`DurableServer.PubSub` provides a pub/sub system for DurableServer lifecycle events and enables coordination between DurableServers and other processes (like Phoenix Channels).

### Subscribing to Events

Subscribe to lifecycle events for DurableServers:

```elixir
# Subscribe to a specific key
:ok = DurableServer.PubSub.subscribe(MyDurableSup, "user/123")

# Subscribe to all keys with a prefix
:ok = DurableServer.PubSub.subscribe(MyDurableSup, "user/")

# Subscribe to all events
:ok = DurableServer.PubSub.subscribe(MyDurableSup, :all)
```

Subscribers receive messages in their mailbox:

```elixir
def handle_info({:durable_server, :registered, %{key: key, pid: pid}}, state) do
  # A DurableServer started
end

def handle_info({:durable_server, :unregistered, %{key: key, reason: reason}}, state) do
  # A DurableServer stopped
end
```

Event types: `:registered`, `:unregistered`, `:updated`, `:joined`, `:left`

### Joining as a Member

Non-DurableServer processes can join keys to be discoverable and receive broadcasts:

```elixir
# Join a key (e.g., from a Phoenix Channel)
:ok = DurableServer.PubSub.join(MyDurableSup, "room/123", %{type: :channel})

# Query all members of a key (DurableServers + joined processes)
members = DurableServer.PubSub.members(MyDurableSup, "room/123")
# => [{#PID<0.150.0>, %{...}}, {#PID<0.200.0>, %{type: :channel}}]

# Leave when done (also happens automatically on process death)
:ok = DurableServer.PubSub.leave(MyDurableSup, "room/123")
```

### Broadcasting to Members

Send messages to all members of a key:

```elixir
# From a DurableServer, broadcast to all connected channels
DurableServer.PubSub.broadcast(MyDurableSup, state.key, {:new_message, message})
```

### Subscribe vs Join

- **`subscribe/2`**: Receive lifecycle events (`:registered`, `:unregistered`, etc.) - system-generated
- **`join/3`**: Be discoverable via `members/2` and receive `broadcast/3` messages - application-level

These are independent - joining does not subscribe you to events, and subscribing does not make you discoverable.

## Running Tests

### Unit Tests (with LocalStack)

Start LocalStack for S3-compatible storage:

```bash
docker run -d --name localstack -p 4566:4566 localstack/localstack
```

Run the tests:

```bash
mix test
```

### Integration Tests (with Tigris)

Set the required environment variables:
> *Note*: You can add these to a gitignored .env in this project and they will be loaded
automatically in `test_helper.exs`

```bash
export DURABLE_AWS_ACCESS_KEY_ID=<your-tigris-access-key>
export DURABLE_AWS_SECRET_ACCESS_KEY=<your-tigris-secret-key>
export DURABLE_AWS_ENDPOINT_URL_S3=https://t3.storage.dev
export DURABLE_AWS_ENDPOINT_URL_IAM=https://iam.storage.dev
export DURABLE_AWS_REGION=<your-region>
export DURABLE_BUCKET=<your-bucket-name>
```

Run integration tests (which hit t3.storage.dev directly):

```bash
mix test --include integration
```

