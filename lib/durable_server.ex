defmodule DurableServer do
  @moduledoc ~S"""
  DurableServer provides durable, distributed GenServer processes backed by pluggable storage.

  DurableServer implements fault-tolerant, stateful processes that can survive node failures,
  restarts, and deployments by automatically persisting state to storage and
  coordinating across a distributed cluster.

  ## Key Features

  - **Durable state**: Automatically persists state to storage with configurable sync intervals
  - **Cluster coordination**: Uses distributed registry for process discovery and health monitoring
  - **Capacity-aware placement**: Monitors CPU, memory, and disk usage to route new processes
    to nodes with available capacity
  - **Sticky placement**: Environment variable-based placement preferences (e.g., same machine,
    same region via `FLY_REGION`, etc.) with time-gated fallback to preferred nodes
  - **Automatic recovery**: Failed processes are detected and restarted across the cluster
  - **Graceful shutdown**: Ensures state is synchronized before termination
    via `DurableServer.Terminator`

  ## Architecture

  DurableServers must be started through `DurableServer.Supervisor`, which provides:

  - Prefix-based isolation between different supervisor instances
  - Graceful shutdown coordination via Terminator GenServer
  - Automatic lifecycle management and restart capabilities with coordination across the cluster

  See `DurableServer.Supervisor` for supervisor setup and configuration options.

  ## Basic Usage

      defmodule MyCounterServer do
        use DurableServer, vsn: 1

        def dump_state(state) do
          %{count: state.count}
        end

        def load_state(_old_vsn, %{"count" => count} = _dumped_state) do
          %{count: count}
        end

        def init(%{count: count} = state) do
          IO.puts("Starting with count #{count}")
          {:ok, Map.merge(state, %{started_at: DateTime.utc_now()}), permanent: true}
        end

        def handle_call(:increment, _from, state) do
          new_state = %{state | count: state.count + 1}
          {:reply, new_state.count, new_state}
        end

        def handle_call(:get_count, _from, state) do
          {:reply, state.count, state}
        end

        def handle_call(:reset, _from, state) do
          {:reply, :ok, %{state | count: 0}}
        end
      end

      # Start the supervisor first (typically in your application.ex supervision tree):

      children = [
        ...,
        {DurableServer.Supervisor, name: MyDurableSup, prefix: "durable/"}
      ]

      # or start directly if you simply want to demo:
      {:ok, supervisor_pid} = DurableServer.Supervisor.start_link(
        name: MyDurableSup,
        prefix: "durable/"
      )

      # Start individual servers through the supervisor
      {:ok, {pid, _meta}} = DurableServer.Supervisor.start_child(
        MyDurableSup,
        {MyCounterServer, key: "user_123", initial_state: %{count: 0}}
      )

      # Use the server
      GenServer.call(pid, :increment)  # => 1
      GenServer.call(pid, :increment)  # => 2
      GenServer.call(pid, :get_count)  # => 2

  *Note*: for releases, `:os_mon` must be added to `extra_applications` in `mix.exs`:

      def application do
        [
          mod: {My.Application, []},
          extra_applications: [:logger, :runtime_tools, :os_mon]
        ]
      end

  ## Advanced Example: Session Manager

      defmodule UserSessionServer do
        use DurableServer, vsn: 2

        def dump_state(state), do: Map.take(state, [:user_id, :session, :last_activity_at])

        # migration logic for version 1 -> 2
        def load_state(vsn, dumped_state) do
          case vsn do
            1 ->
              # migrate to v2 logic

            _ ->
              %{
                user_id: Map.fetch!(dumped_state, ["user_id"]),
                session: Map.get(dumped_state, "session" || %{},
                last_activity: dumped_state["last_activity_at"],
              }
            end
        end

        def init(%{} = loaded_state) do
          init_state = %{loaded_state | last_activity_at: System.system_time(:millisecond)}
          {:ok, init_state, sync_every_ms: 30_000}
        end

        def handle_call({:update_session, func}, _from, state) do
          %{} = new_session = func.(state.session)
          new_state = %{state | session: new_session, last_activity: System.system_time(:millisecond)}
          {:reply, :ok, new_state}
        end

        def handle_call(:get_session, _from, state) do
          {:reply, state.session, %{state | last_activity: System.system_time(:millisecond)}}
        end

        def handle_call(:logout, _from, state) do
          {:stop, :normal, :ok, %{state | last_activity: System.system_time(:millisecond)}}
        end
      end

  ## Configuration Options

  DurableServer supports these options in the `init/1` or `init/2` return tuple:

  - `:auto_sync` - Enable automatic periodic syncing (default: false)
  - `:sync_every_ms` - Sync interval in milliseconds (default: 30_000)
  - `:meta` - Optional metadata to include for the globally registered server which is
    returned alongside the pid with `DurableServer.Supervisor.lookup/2`.
  - `:permanent` - Mark server for automatic restart by LifecycleManager (default: false)

  ## Accessing Runtime Info

  DurableServer provides runtime information through the optional `init/2` callback.
  The `info` map contains supervisor references and any user-defined data configured
  via the supervisor's `:init_info` option.

  ### Built-in Keys

  The following keys are always present in the info map:

  - `:key` - DurableServer key
  - `:supervisor` - The `DurableServer.Supervisor` name
  - `:task_supervisor` - Task supervisor for spawning async tasks
  - `:dynamic_supervisor` - The DynamicSupervisor managing DurableServer processes

  ### User-defined Keys

  Pass custom data to all servers via the supervisor's `:init_info` option:

      # In your supervision tree
      {DurableServer.Supervisor,
       name: MyApp.DurableSup,
       prefix: "myapp/",
       init_info: %{api_client: MyApp.APIClient, config: %{timeout: 5000}}}

  Then access it in your server's `init/2`:

      def init(state, info) do
        api_client = info.api_client
        timeout = info.config.timeout
        {:ok, %{state | api_client: api_client, timeout: timeout}}
      end

  ### Choosing Between init/1 and init/2

  - Use `init/1` if you don't need access to supervisor references or custom init_info
  - Use `init/2` if you need the task supervisor, dynamic supervisor, or custom data

  Both callbacks are optional. If you implement `init/2`, it takes precedence.
  If neither is implemented, the default `init/1` returns `{:ok, state}`.

  ## State Synchronization

  State is synchronized to storage in these scenarios:

  1. **Manual sync**: Return `:sync` from any callback, ie: `{:noreply, state, :sync}`
     You can also combine sync with other actions via callback options,
     e.g. `{:noreply, state, {:continue, term}, sync: true}`. Manual sync is a strict
     durability boundary: the configured backend exhausts its bounded transient retry
     policy before returning an error, and DurableServer terminates without acknowledging
     the callback if the write still fails.
  2. **Automatic sync**: When `:auto_sync` is enabled all changes are immediately written when
    any callback returns, or the `:sync_every_ms` interval can be provided to periodically sync changes.
    Automatic and periodic syncs log transient failures and keep the server alive so a later
    sync can persist the dirty state. Storage conflicts remain fatal.
  3. **Graceful shutdown**: Automatically synced during normal termination, ie: cold deploys
  4. **Before stopping**: When returning `{:stop, reason, state}` from callbacks

  ## Stopping Behavior

  DurableServer supports different stop reasons with specific behaviors regarding exit signal propagation:

  ### Shutdown-wrapped stops (exit signal propagates to linked processes)
  - `{:stop, {:shutdown, :delete}, state}` - Stops and deletes from storage, exit signal propagates
  - `{:stop, {:shutdown, :permanent}, state}` - Stops permanently, exit signal propagates.
    `:permanent` stop will make the server no longer elligable for permanent restarts and it will remain
    stopped until explicitly started by `DurableSuper.Supervisor.start_child/2`.
  - `{:stop, {:shutdown, :normal}, state}` - Normal stop, exit signal propagates (syncs as stopped_graceful)

  Shutdown-wrapped exits propagate to linked processes (allowing them to react) but don't kill them.

  ### Non-shutdown stops (exit signal does NOT propagate to linked processes)
  - `{:stop, :delete, state}` - Stops and deletes, silent termination (no exit signal)
  - `{:stop, :permanent, state}` - Stops permanently, silent termination (no exit signal)
  - `{:stop, :normal, state}` - Normal stop, silent termination (syncs as stopped_graceful)

  Non-shutdown stops are transformed to `:normal` exits which don't propagate to linked processes.

  ### Error stops
  - `{:stop, {:error, reason}, state}` - Stops with error, marks as crashed, exit signal propagates

  Use shutdown-wrapped stops when linked processes need to be notified of the shutdown.
  Use non-shutdown stops for silent termination without notifying linked processes.

  ## Error Handling and Recovery

  DurableServers are designed to be resilient:

  - **Process crashes**: `LifecycleManager` detects failures and restarts servers
  - **Node failures**: Other nodes claim and restart orphaned processes
  - **Storage failures**: Retries and graceful degradation where possible
  - **Region-aware network partitions**: Consistent hashing ensures only one node manages each key
    and places servers in their initial region where possible

  ## Best Practices

  1. **Always use DurableServer.Supervisor**: Never start DurableServers directly
  2. **Design for restarts**: Assume your process can be restarted on any node at any time
  3. **Ensure `load_state/2` handles migrations and avoids side effects**
    You **must** implement state migrations for schema changes across code changes, which is handled
    by bumping your `:vsn` option to `use DurableServer` and matching in your `load_state/2` on
    old versions.

    *Note*: A lock is not aquired until `init/1` is entered, so your `load_state/2` callbacks should always
    be a pure function without side effects. ie if you need process messaging, pubsub, or to perform work
    on process start, do so after loading your state within `init/1`.
  4. **Consider appropriate sync intervals**: Balance durability vs performance needs

  ## Distribution and Clustering

  DurableServers work seamlessly in distributed environments:

  - Processes register in a cluster-wide registry with their unique keys
  - Permanent servers are started across the cluster and guarantee only a single key
    is started globally at a given time
  - Servers can be configured with sticky placement preferences to restart on the same
    machine or in the same region where they were running
  - Health monitoring detects failures across the cluster
  - Automatic failover ensures high availability

  See `DurableServer.Supervisor` documentation for cluster configuration options.

  ## Capacity-Aware Placement

  DurableServers support automatic capacity-aware placement with remote fallback.

  ### Local Placement (Default)

  When starting a child, the local node is tried first. If capacity limits are exceeded,
  remote placement is attempted automatically.

  ### Remote Placement

  If local capacity is exhausted, DurableServer automatically tries remote nodes:

  1. **Same-region nodes first** - Prioritizes nodes in the same region for lower latency
  2. **Least busy nodes** - Selects nodes with the lowest utilization across all limits
  3. **Configurable retries** - Default 3 remote nodes tried, configurable via `max_placement_retries`

  ### Capacity Limits

  Configure capacity limits when starting a supervisor:

      {DurableServer.Supervisor,
       name: MyDurableSup,
       prefix: "durable/",
       max_children: %{
         :total => 100,                     # Max total children on this node
         MyModule => 50                     # Max MyModule children on this node
       },
       max_cpu: 80,                         # Max CPU % before rejecting
       max_memory: 85,                      # Max memory % before rejecting
       max_disk: {90, "/data"}}             # Max disk % on mount point before rejecting

  Unlike CPU and memory limits, disk limits are bypassed for sticky restarts (children
  returning to their previous node) since part of the disk usage is the child's own data.

  ### Placement Options

  Control remote placement behavior per start_child call:

      # Default: Try local, then up to 3 remote nodes
      DurableServer.Supervisor.start_child(sup, {MyServer, key: "user_1", initial_state: %{}})

      # Local only, no remote fallback
      DurableServer.Supervisor.start_child(sup, {MyServer, key: "user_1", initial_state: %{}},
        max_placement_retries: 0)

      # Try local, then up to 5 remote nodes
      DurableServer.Supervisor.start_child(sup, {MyServer, key: "user_1", initial_state: %{}},
        max_placement_retries: 5)

  **Note:** Automatic restarts from `LifecycleManager` always use `max_placement_retries: 0`
  to place processes on their current node only, deferring to other node LifecycleManagers to
  manager their own node-local placement.

  See `DurableServer.Supervisor` for full configuration details.

  ## Sticky Placement

  Sticky placement allows DurableServers to prefer restarting on nodes with specific
  characteristics (e.g., same machine, same region) before falling back to other nodes.
  This is particularly useful for things like Litestream-backed databases to avoid
  unnecessary S3 restores when the database is already available locally.

  ### Sticky Configuration

  Configure sticky placement per-module when starting a supervisor using a keyword list
  where keys are environment variable names (as atoms) and values are delay times in milliseconds:

      {DurableServer.Supervisor,
       name: MyDurableSup,
       prefix: "durable/",
       sticky_placement: %{
         MyDatabaseServer => [
           FLY_MACHINE_ID: 10_000,
           FLY_REGION: 20_000,
           any: 0
         ]
       }}

  Sticky placement uses environment variables to create a progressive fallback strategy
  with cumulative time windows. Each delay value specifies how much time to add before
  the **next** level can claim. From the above configuration:

  1. **Level 0** (immediate): Only nodes matching `FLY_MACHINE_ID` can claim
  2. **Level 1** (after 10s): Nodes matching `FLY_REGION` can claim
  3. **Level 2** (after 30s): Any node (`:any`) can claim

  The delays are **cumulative** - each level unlocks at the sum of all previous delays:
  - Level 0 unlocks at 0ms (always immediate)
  - Level 1 unlocks at 10,000ms (sum of delays before level 1)
  - Level 2 unlocks at 30,000ms (10s + 20s)

  The **last** level's delay value is unused (no subsequent level), so `0` is conventional.
  Earlier levels remain eligible even after later levels unlock, maintaining preference order.

  ### Common Patterns

  **Machine stickiness with region fallback (no `:any`):**

      sticky_placement: %{
        MyServer => [
          FLY_MACHINE_ID: 20_000,
          FLY_REGION: 0
        ]
      }

  Same machine claims immediately, same region claims after 20s. Without `:any`, nodes
  in other regions can **never** claim - the server will only run in its original region.

  **Region stickiness, falling back to any node:**

      sticky_placement: %{
        MyServer => [
          FLY_REGION: 20_000,
          any: 0
        ]
      }

  Same region claims immediately, any node can claim after 20s.

  **Custom environment variables:**

      sticky_placement: %{
        MyServer => [
          DATACENTER: 15_000,
          AVAILABILITY_ZONE: 30_000,
          any: 0
        ]
      }

  Same datacenter claims immediately, same availability zone after 15s, any node after 45s.

  **Strict region pinning (no fallback):**

      sticky_placement: %{
        MyServer => [
          FLY_REGION: 0
        ]
      }

  Only nodes with matching `FLY_REGION` can claim, and they can claim immediately.
  Without `:any`, non-matching nodes can **never** claim the server - it will only run
  on nodes with the same `FLY_REGION` as where it was originally started. Use this when
  data locality is critical and you'd rather the server stay down than run in the wrong
  location.

  ### Default Sticky Placement

  Apply the same sticky placement configuration to all modules:

      {DurableServer.Supervisor,
       name: MyDurableSup,
       prefix: "durable/",
       default_sticky_placement: [
         FLY_REGION: 20_000,
         any: 0
       ]}

  Per-module configurations override the default.

  ### Updating Sticky Placement Configuration

  When a DurableServer starts, its sticky placement is captured based on the module
  configuration and the node's current environment variables. This placement is persisted
  with the server's state in object storage.

  If you later change the module's sticky placement configuration (for example, adding
  `:any` as a fallback level), running servers retain their original placement from when
  they started. To ensure proper orphan claiming behavior, the lifecycle manager automatically
  augments persisted placement with the `:any` level if present in the updated module config.

  For example, if you change from:

      sticky_placement: %{MyServer => [FLY_MACHINE_ID: 60_000, FLY_REGION: 0]}

  To:

      sticky_placement: %{MyServer => [FLY_MACHINE_ID: 60_000, FLY_REGION: 120_000, any: 0]}

  Servers started before the change will have their persisted placement augmented with the
  `:any` level at runtime. This ensures they can still be claimed by any node after their
  specific placement preferences are exhausted, using the delay specified in the module config.

  Other environment variable levels cannot be added retroactively since their values were
  determined when the server originally started.

  ### Important Notes

  - Environment variable values are captured when the server first starts
  - Values are stored in the server's metadata in object storage
  - nil environment variable values are preserved and can match
  - The `:any` atom matches any node, regardless of environment variables
  - Time windows are cumulative, not independent intervals
  - Earlier preference levels remain eligible after later levels unlock

  ## Monitoring Events with Group

  DurableServer uses `Group` for distributed process groups, registry, and lifecycle monitoring.

  You can call into the Group instance of your Supervisor to monitor DurableServer events:

  ```elixir
  # Monitor a specific key
  :ok = Group.monitor(MyDurableSup, "user/123")

  # Monitor all keys with a prefix
  :ok = Group.monitor(MyDurableSup, "user/")

  # Monitor all events
  :ok = Group.monitor(MyDurableSup, :all)
  ```

  Monitors receive `{:group, events, info}` tuples in their mailbox:

  ```elixir
  def handle_info({:group, events, _info}, state) do
    Enum.each(events, fn
      %Group.Event{type: :registered, key: key, pid: pid, previous_meta: nil} ->
        # A DurableServer started (previous_meta is nil for first registration)
        :ok
      %Group.Event{type: :unregistered, key: key, reason: reason} ->
        # A DurableServer stopped
        :ok
      _ -> :ok
    end)
    {:noreply, state}
  end
  ```

  Event types: `:registered`, `:unregistered`, `:joined`, `:left`

  `:registered` and `:joined` events include a `previous_meta` field (`nil` for new, old meta
  for re-register/re-join). Single operations produce one event per tuple; bulk operations
  (nodedown, process death) batch all events together.

  ### Joining as a Member

  Non-DurableServer processes can join keys to be discoverable and receive dispatched messages:

  ```elixir
  # Join a key (e.g., from a Phoenix Channel)
  :ok = Group.join(MyDurableSup, "room/123", %{type: :channel})

  # Re-joining updates metadata in place
  :ok = Group.join(MyDurableSup, "room/123", %{type: :channel, status: :active})

  # Query all members of a key (DurableServers + joined processes)
  members = Group.members(MyDurableSup, "room/123")
  # => [{#PID<0.150.0>, %{...}}, {#PID<0.200.0>, %{type: :channel, status: :active}}]

  # Leave when done (also happens automatically on process death)
  :ok = Group.leave(MyDurableSup, "room/123")
  ```

  ### Dispatching to Members

  Send messages to all members of a key:

  ```elixir
  # From a DurableServer, broadcast to all connected channels
  Group.dispatch(MyDurableSup, state.key, {:new_message, message})
  ```

  ### Monitor vs Join

  - **`monitor/2`**: Receive lifecycle events
    (`:registered`, `:unregistered`, `:joined`, `:left`) - system-generated
  - **`join/3`**: Be discoverable via `members/2` and receive `dispatch/3`
    messages - application-level

  These are independent - joining does not monitor events, and monitoring does not make you discoverable.
  """

  use GenServer
  require Logger

  alias DurableServer
  alias DurableServer.{LifecycleManager, CircuitBreaker, StoredState, Meta}
  alias DurableServer.ObjectStore
  alias DurableServer.StorageBackend

  @type init_option ::
          {:auto_sync, boolean()}
          | {:sync_every_ms, pos_integer()}
          | {:meta, map()}
          | {:permanent, boolean()}

  @type user_meta :: map()
  @type sync_action :: :sync
  @type callback_option :: {:meta, user_meta()} | {:sync, boolean()}
  @type callback_options :: [callback_option()]
  @type timeout_action ::
          timeout() | :hibernate | {:continue, term()} | sync_action()

  @doc """
  Initializes the DurableServer with loaded state.

  This callback is invoked after the server acquires its global lock and loads
  any persisted state. You can implement either `init/1` or `init/2`:

  - `init/1` - Receives only the loaded state
  - `init/2` - Receives the loaded state and an info map with runtime information

  If you implement `init/2`, it takes precedence over `init/1`.

  ## The Info Map (init/2)

  The `info` map in `init/2` contains:

  - `:key` - The DurableServer key
  - `:supervisor` - The supervisor name (e.g., `MyApp.DurableSup`)
  - `:task_supervisor` - The task supervisor for async operations
  - `:dynamic_supervisor` - The dynamic supervisor managing DurableServer processes
  - Any user-defined keys from the supervisor's `:init_info` option

  ## Return Values

  - `{:ok, state}` - Initialize with the given state
  - `{:ok, state, opts}` - Initialize with state and options
  - `:ignore` - Don't start the server, sync as stopped_graceful

  ## Options

  - `:auto_sync` - Enable automatic syncing on every callback return (default: `false`)
  - `:sync_every_ms` - Periodic sync interval in milliseconds (default: `30_000`)
  - `:meta` - User metadata returned by `DurableServer.Supervisor.lookup/2`
  - `:permanent` - Mark server for automatic restart by LifecycleManager (default: `false`)

  ## Examples

      # Simple init/1
      def init(state) do
        {:ok, state, permanent: true}
      end

      # init/2 with runtime info
      def init(state, info) do
        # Access built-in values
        %{key: key, task_supervisor: task_sup} = info

        # Access user-defined values from supervisor's init_info
        api_client = info.api_client

        {:ok, Map.merge(state, %{task_sup: task_sup, api_client: api_client})}
      end

  """
  @callback init(loaded_state :: map()) ::
              :ignore
              | {:ok, state :: term()}
              | {:ok, state :: term(), [init_option()]}

  @callback init(loaded_state :: map(), info :: map()) ::
              :ignore
              | {:ok, state :: term()}
              | {:ok, state :: term(), [init_option()]}

  @callback handle_call(request :: term(), from :: GenServer.from(), state :: term()) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout_action()}
              | {:reply, reply, new_state, callback_options()}
              | {:reply, reply, new_state, timeout_action(), callback_options()}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout_action()}
              | {:noreply, new_state, callback_options()}
              | {:noreply, new_state, timeout_action(), callback_options()}
              | {:stop, reason, reply, new_state}
              | {:stop, {:shutdown, :delete}, reply, new_state}
              | {:stop, {:shutdown, :permanent}, reply, new_state}
              | {:stop, :delete, reply, new_state}
              | {:stop, :permanent, reply, new_state}
              | {:stop, reason, new_state}
              | {:stop, {:shutdown, :delete}, new_state}
              | {:stop, {:shutdown, :permanent}, new_state}
              | {:stop, :delete, new_state}
              | {:stop, :permanent, new_state}
            when reply: term(), new_state: term(), reason: term()

  @callback handle_cast(request :: term(), state :: term()) ::
              {:noreply, new_state}
              | {:noreply, new_state, timeout_action()}
              | {:noreply, new_state, callback_options()}
              | {:noreply, new_state, timeout_action(), callback_options()}
              | {:stop, reason :: term(), new_state}
              | {:stop, {:shutdown, :delete}, new_state}
              | {:stop, {:shutdown, :permanent}, new_state}
              | {:stop, :delete, new_state}
              | {:stop, :permanent, new_state}
            when new_state: term()

  @callback handle_info(msg :: :timeout | term(), state :: term()) ::
              {:noreply, new_state}
              | {:noreply, new_state, timeout_action()}
              | {:noreply, new_state, callback_options()}
              | {:noreply, new_state, timeout_action(), callback_options()}
              | {:stop, reason :: term(), new_state}
              | {:stop, {:shutdown, :delete}, new_state}
              | {:stop, {:shutdown, :permanent}, new_state}
              | {:stop, :delete, new_state}
              | {:stop, :permanent, new_state}
            when new_state: term()

  @callback handle_continue(continue :: term(), state :: term()) ::
              {:noreply, new_state}
              | {:noreply, new_state, timeout_action()}
              | {:noreply, new_state, callback_options()}
              | {:noreply, new_state, timeout_action(), callback_options()}
              | {:stop, reason :: term(), new_state}
              | {:stop, {:shutdown, :delete}, new_state}
              | {:stop, {:shutdown, :permanent}, new_state}
              | {:stop, :delete, new_state}
              | {:stop, :permanent, new_state}
            when new_state: term()

  @doc """
  Optional callback invoked inline after DurableServer successfully writes user
  state to the configured storage backend.

  `handle_sync/3` only runs after an actual backend write that includes changed
  dumped user state. It is not called when a sync request noops because the
  dumped user state is unchanged, and it is not called for metadata-only writes
  such as final status persistence during termination.

  For normal callback-triggered syncs, the callback receives the user state
  before the callback that triggered the sync and the user state that was just
  persisted. Lifecycle syncs may pass the same value for both state arguments.
  The returned state updates the in-memory process state only; it is not
  included in the already-completed storage write, and DurableServer does not
  recursively sync or call `handle_sync/3` again for changes returned from this
  callback.

  This makes `handle_sync/3` appropriate for side effects and transient runtime
  bookkeeping, such as updating non-dumped fields, refs, counters, or cached
  notification state. If you return changes that affect `dump_state/1`, those
  changes remain in memory and are only eligible for a later explicit,
  automatic, or lifecycle sync.

  The info map contains:

    * `:key` - DurableServer key
    * `:supervisor` - Supervisor name
    * `:status` - The persisted `DurableServer.Meta.status` for this write

  ## Examples

      def handle_sync(%{status: :deleting}, _old_state, new_state) do
        {:ok, new_state}
      end

      def handle_sync(%{status: :running}, old_state, new_state) do
        if old_state.count != new_state.count do
          Logger.info("persisted count=\#{new_state.count}")
        end

        {:ok, new_state}
      end
  """
  @callback handle_sync(info :: map(), old_state :: map(), new_state :: map()) :: {:ok, map()}

  @callback terminate(reason :: term(), state :: term()) :: term()

  @doc """
  Optional callback invoked after `terminate/2` and after final status sync.

  This callback is only invoked when the final status sync completed successfully
  for a graceful stop (`final_status: :stopped_graceful` and `sync_result: :ok`).

  The first argument is exactly the return value from `terminate/2`.
  The second argument is an info map:

    * `:key` - DurableServer key
    * `:supervisor` - Supervisor name
    * `:final_status` - Final persisted status atom
    * `:sync_result` - `:ok | {:error, term()}`
    * `:reason` - Termination reason passed to `terminate/2`
  """
  @callback after_terminate(terminate_return :: term(), info :: map()) :: term()

  @callback code_change(old_vsn :: term() | {:down, term()}, state :: term(), extra :: term()) ::
              {:ok, new_state :: term()} | {:error, reason :: term()}

  @doc """
  Transform user state into a map for persistence.

  This required callback is used when saving state through the configured storage backend.
  It allows you to:
  - Filter out keys that shouldn't be persisted (like PIDs, refs, etc.)
  - Transform the state shape for storage
  - Remove ephemeral data

  The returned value must be a plain map at the top level. Nested values are passed
  through to the configured backend as-is, so they only need to be encodable by the
  backend you are using.

  This means persisted shapes may differ by backend. For example:

  - `DurableServer.Backends.ObjectStore` typically encodes to and decodes from JSON-shaped data
    with string keys
  - `DurableServer.Backends.EKVStore` may preserve richer Elixir terms

  If you plan to move data between backends, `load_state/2` should be prepared to
  handle multiple persisted shapes during the migration window.

  ## Examples

      def dump_state(%{count: count, temp_data: _temp} = state) do
        # Only persist count, filter out temp_data
        %{count: count}
      end
  """
  @callback dump_state(state :: term()) :: map()

  @doc """
  Transform backend-decoded persisted state back into user state format.

  This required callback is used when loading state from the configured backend.
  It allows you to:
  - Convert backend-specific persisted shapes into your runtime state format
  - Set default values for missing keys
  - Initialize ephemeral state that wasn't persisted

  On first boot for a never-before-persisted server, DurableServer encodes and
  decodes the result of `dump_state/1` through the configured backend before
  calling `load_state/2`. This keeps the first-boot shape consistent with the
  shape you will receive on later restarts for that backend.

  Persisted state is backend-dependent. For example:

  - `DurableServer.Backends.ObjectStore` usually passes JSON-decoded maps with string keys
  - `DurableServer.Backends.EKVStore` may pass maps with atom keys or other native Elixir terms

  During backend migrations, it is valid for `load_state/2` to receive multiple
  historical shapes until the migration is complete.

  For a server that has never been persisted, the old_vsn will be `nil`.

  *Note*: the function is NOT guaranteed to be idempotent. The durable server
  is not considered started until *after* `load_state/2` is run and a lock is
  succesfully obtained with your loaded state. Concurrent nodes can race your
  state load and aquire the lock before you, so this function should not issue
  side effects like calling other processes. Peform such side effect work
  inside `init/1`, which is gauranteed to have started your durable server with
  a successful global lock.

  ## Examples

      def load_state(_old_vsn, dumped_state) do
        # Convert string keys to atoms and add ephemeral state
        %{
          count: Map.fetch!(dumped_state, "count"),
          temp_data: nil,
          status: :initialized
        }
      end
  """
  @callback load_state(old_vsn :: pos_integer() | nil, persisted_state :: map()) :: map()

  @optional_callbacks init: 1,
                      init: 2,
                      handle_call: 3,
                      handle_cast: 2,
                      handle_info: 2,
                      handle_continue: 2,
                      handle_sync: 3,
                      terminate: 2,
                      after_terminate: 2,
                      code_change: 3

  defstruct object_store: nil,
            key: nil,
            prefix: nil,
            etag: nil,
            pid: nil,
            preloaded_boot: false,
            bootstrapped: false,
            init_from_ref: nil,
            init_from_pid: nil,
            init_reply_to: nil,
            status: nil,
            last_heartbeat_at: nil,
            vsn: nil,
            old_vsn: nil,
            node_ref: nil,
            node_str: nil,
            supervisor: nil,
            dynamic_supervisor: nil,
            task_supervisor: nil,
            circuit_breaker: nil,
            auto_sync: nil,
            sync_every_ms: nil,
            sync_timer_ref: nil,
            user_state: nil,
            user_meta: nil,
            crash_history: [],
            module: nil,
            last_synced_user_state_hash: nil,
            final_status_set: nil,
            terminator_handled: false,
            delete_requester: nil,
            cordon_requester: nil,
            permanent: false,
            was_permanently_crashed: false,
            user_initiated_stop: nil,
            start_time: nil,
            restart_attempt_node: nil,
            restart_attempt_time: nil,
            restart_attempt_ttl: nil,
            sticky_placement_history: [],
            sticky_placement_history_limit: 5

  @type user_stop_reason ::
          nil
          | :normal
          | :delete
          | :permanent
          | {:shutdown, :delete}
          | {:shutdown, :permanent}
          | {:shutdown, :normal}
          | {:error, term()}

  @durable :durable
  @max_crash_reason_length 500
  @max_sync_retries 5
  @bootstrap_continue {@durable, :bootstrap}

  defguardp is_callback_action(action)
            when is_atom(action) or is_tuple(action) or
                   (is_integer(action) and action >= 0)

  defmacro __using__(opts) do
    vsn =
      case Keyword.fetch(opts, :vsn) do
        {:ok, val} when is_integer(val) and val > 0 ->
          val

        {:ok, val} ->
          raise ArgumentError, "vsn must be a positive integer, got: #{inspect(val)}"

        :error ->
          raise ArgumentError,
                "the current :vsn must be provided:, ie: `use DurableServer, vsn: 1`"
      end

    quote do
      @behaviour DurableServer
      @vsn unquote(vsn)
      import unquote(__MODULE__)

      def __durable_server_config__() do
        %{
          vsn: @vsn
        }
      end

      # Default implementations
      def init(state) do
        {:ok, state}
      end

      def handle_call(_request, _from, state) do
        {:reply, :ok, state}
      end

      def handle_cast(_request, state) do
        {:noreply, state}
      end

      def handle_info(_msg, state) do
        {:noreply, state}
      end

      def handle_continue(_continue, state) do
        {:noreply, state}
      end

      def handle_sync(_info, _old_state, new_state) do
        {:ok, new_state}
      end

      def terminate(_reason, _state) do
        :ok
      end

      def after_terminate(_terminate_return, _info) do
        :ok
      end

      # Default code_change for hot upgrades: no migration needed
      # Override this to provide version-specific state migrations:
      #
      #   def code_change(1, old_state, 2) do
      #     # Migrate from v1 to v2
      #     {:ok, Map.put(old_state, :new_field, "default")}
      #   end
      #
      #   def code_change(_, state, _) do
      #     # No migration for other versions
      #     {:ok, state}
      #   end
      def code_change(_old_vsn, state, _new_vsn) do
        {:ok, state}
      end

      defoverridable init: 1,
                     handle_call: 3,
                     handle_cast: 2,
                     handle_info: 2,
                     handle_continue: 2,
                     handle_sync: 3,
                     terminate: 2,
                     after_terminate: 2,
                     code_change: 3
    end
  end

  def child_spec(init_arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [init_arg]},
      type: :worker,
      restart: :temporary,
      shutdown: 30_000
    }
  end

  def start_link(
        %{
          module: _module,
          init_from: _init_from,
          init_arg: _init_arg,
          boot_info: _boot_info,
          supervisor_name: _supervisor_name,
          config: _config
        } = info
      ) do
    GenServer.start_link(__MODULE__, info)
  end

  @impl true
  def init(%{
        module: module,
        init_from: init_from,
        init_arg: init_arg,
        boot_info: boot_info,
        supervisor_name: supervisor_name,
        config: config
      })
      when is_atom(supervisor_name) and is_map(config) do
    {from_ref, from_pid, reply_to} = normalize_init_from(init_from)
    prefix = Map.fetch!(config, :prefix)
    circuit_breaker = Map.fetch!(config, :circuit_breaker)
    object_store = Map.fetch!(config, :storage_backend)
    sticky_placement_history_limit = Map.fetch!(config, :sticky_placement_history_limit)
    # trap exits to handle crashes and coordinate with Terminator
    Process.flag(:trap_exit, true)

    # Set $initial_call to the callback module so hot code upgrades work correctly
    # Without this, $initial_call would be {DurableServer, :init, 1} and hot
    # upgrades to the callback module (e.g., MyApp.OrgTracker) wouldn't be detected
    Process.put(:"$initial_call", {module, :init, 1})

    key = Keyword.fetch!(init_arg, :key)

    # Sticky preloaded boots bypass disk checks because their data is already on this node.
    is_sticky_local =
      case boot_info do
        %{preloaded: %{body: %StoredState{}, etag: _etag}, is_sticky_local: is_sticky_local} ->
          is_sticky_local

        _ ->
          false
      end

    # check capacity limits before attempting lock acquisition
    # bypass disk check if this is a sticky restart (data already on this node's disk)
    capacity_opts = if is_sticky_local, do: [bypass_disk_check: true], else: []

    state = %DurableServer{
      object_store: object_store,
      key: key,
      prefix: prefix,
      module: module,
      supervisor: supervisor_name,
      dynamic_supervisor: DurableServer.Supervisor.get_dynamic_supervisor(supervisor_name),
      task_supervisor: DurableServer.Supervisor.get_task_supervisor(supervisor_name),
      circuit_breaker: circuit_breaker,
      node_str: to_string(Node.self()),
      pid: self(),
      preloaded_boot: preloaded_boot?(boot_info),
      node_ref: DurableServer.Supervisor.node_ref(supervisor_name),
      init_from_ref: from_ref,
      init_from_pid: from_pid,
      init_reply_to: reply_to,
      sticky_placement_history_limit: sticky_placement_history_limit
    }

    bootstrap = %{
      init_from: init_from,
      init_arg: init_arg,
      boot_info: boot_info,
      config: config,
      capacity_opts: capacity_opts
    }

    {:ok, state, {:continue, {@bootstrap_continue, bootstrap}}}
  end

  defp handle_ignore(%DurableServer{} = state, _init_from) do
    case sync_to_storage(state, meta: %{status: :stopped_graceful}) do
      {:ok, %DurableServer{} = _new_state} ->
        send(state.init_reply_to, {state.init_from_ref, :ignore})
        {:stop, {:shutdown, {@durable, :ignored}}, state}

      {:error, sync_reason} ->
        Logger.error("Failed to update status before :ignore: #{inspect(sync_reason)}")
        send(state.init_reply_to, {state.init_from_ref, :ignore})
        {:stop, {:shutdown, {@durable, :ignored}}, state}
    end
  end

  defp handle_init(%DurableServer{} = state, user_state, opts, continue_or_timeout) do
    opts = Keyword.validate!(opts, [:auto_sync, :sync_every_ms, :meta, :permanent])

    new_state = %{
      state
      | auto_sync: Keyword.get(opts, :auto_sync, false),
        user_state: user_state,
        user_meta: opts[:meta],
        sync_every_ms: Keyword.get(opts, :sync_every_ms, 30_000),
        permanent: Keyword.get(opts, :permanent, false),
        start_time: System.system_time(:millisecond)
    }

    new_state = register_pid(new_state)
    # always sync status to :running on startup, since if we're starting up, we're running
    # this ensures that crashed servers get their status updated when restarted
    case sync_to_storage(new_state, meta: %{status: :running}) do
      {:ok, new_state} ->
        # schedule our first sync
        new_state = %{schedule_sync(new_state) | bootstrapped: true}

        # send caller that called start_child our metadata
        send(new_state.init_reply_to, {new_state.init_from_ref, new_state.user_meta})

        if continue_or_timeout do
          {:noreply, new_state, continue_or_timeout}
        else
          {:noreply, new_state}
        end

      {:error, reason} ->
        Logger.error(
          "#{inspect(state.module)} (key=#{state.key}) failed to sync startup status :running: #{inspect(reason)}"
        )

        case repair_failed_boot_storage(new_state) do
          :ok ->
            :ok

          :noop ->
            :ok

          {:error, cleanup_reason} ->
            Logger.warning(
              "#{inspect(state.module)} (key=#{state.key}) failed to repair startup state after sync failure: #{inspect(cleanup_reason)}"
            )
        end

        send(new_state.init_reply_to, {new_state.init_from_ref, {:error, reason}})
        {:stop, {:shutdown, {@durable, {:init_failed, reason}}}, new_state}
    end
  end

  # fetch existing raw object + metadata, or reuse preloaded boot data.
  defp fetch_existing_state_raw(
         %StorageBackend{} = store,
         %{key: key, prefix: prefix, supervisor: _supervisor_name} = request,
         boot_info,
         opts
       ) do
    if Keyword.get(opts, :consistent, false) do
      case fetch_stored_state(store, request, opts) do
        {:ok, %StoredState{} = existing_raw_data} -> {:ok, existing_raw_data}
        {:error, _reason} -> :error
      end
    else
      case boot_info_preloaded_object(boot_info) do
        %{body: %StoredState{} = stored_state, etag: etag} ->
          {:ok,
           attach_stored_state_context(%{stored_state | etag: etag}, %{
             key: key,
             prefix: prefix,
             supervisor: request.supervisor
           })}

        _ ->
          case fetch_stored_state(store, request, opts) do
            {:ok, %StoredState{} = existing_raw_data} -> {:ok, existing_raw_data}
            {:error, _reason} -> :error
          end
      end
    end
  end

  defp preloaded_boot?(nil), do: false

  defp preloaded_boot?(%{} = boot_info),
    do: match?(%{body: %StoredState{}, etag: _etag}, boot_info_preloaded_object(boot_info))

  defp boot_info_preloaded_object(nil), do: nil
  defp boot_info_preloaded_object(%{} = boot_info) when map_size(boot_info) == 0, do: nil

  defp boot_info_preloaded_object(
         %{
           preloaded: %{body: %StoredState{}, etag: _etag} = preloaded,
           is_sticky_local: is_sticky_local
         } = boot_info
       )
       when is_boolean(is_sticky_local) and map_size(boot_info) == 2 and
              map_size(preloaded) == 2,
       do: preloaded

  defp boot_info_preloaded_object(
         %{
           preloaded: %{body: %StoredState{}, etag: _etag} = preloaded,
           is_sticky_local: is_sticky_local,
           restart_attempt_node: restart_attempt_node
         } = boot_info
       )
       when is_boolean(is_sticky_local) and is_binary(restart_attempt_node) and
              map_size(boot_info) == 3 and map_size(preloaded) == 2,
       do: preloaded

  defp load_fresh_init_state(module, init_arg, object_store, opts \\ []) do
    opts = Keyword.validate!(opts, [:etag])
    initial_state = Keyword.fetch!(init_arg, :initial_state)

    with dumped_init_state <-
           initial_state
           |> module.dump_state()
           |> validate_dumped_state!(module),
         {:ok, encoded_init_state} <-
           StorageBackend.encode(object_store, dumped_init_state),
         {:ok, client_init_state} <-
           StorageBackend.decode(object_store, encoded_init_state) do
      loaded_user_state = load_user_state(module, _old_vsn = nil, client_init_state)
      {:ok, {loaded_user_state, _old_vsn = nil, Keyword.get(opts, :etag), _meta = nil}}
    end
  end

  defp maybe_confirm_deleting_tombstone(
         _store,
         _request,
         %StoredState{} = existing,
         _preloaded_boot = true
       ),
       do: {:ok, existing}

  defp maybe_confirm_deleting_tombstone(
         %StorageBackend{} = store,
         %{key: _key, prefix: _prefix, supervisor: _supervisor_name} = request,
         %StoredState{meta: %Meta{} = meta} = existing,
         _preloaded_boot = false
       ) do
    # A stale read can observe a tombstone after DELETE already completed.
    if Meta.deleting?(meta) do
      case fetch_stored_state(store, request, consistent: true) do
        {:ok, %StoredState{} = current} -> {:ok, current}
        {:error, :not_found} -> :not_found
        {:error, reason} -> {:error, reason}
      end
    else
      {:ok, existing}
    end
  end

  defp reread_expired_state(%StorageBackend{} = store, %{key: _key, prefix: _prefix} = request) do
    case fetch_stored_state(store, request, consistent: true) do
      {:ok, %StoredState{meta: %Meta{} = meta} = stored_state} ->
        if Meta.cordoned?(meta) do
          {:error, :cordoned}
        else
          case check_lock(meta, Map.fetch!(request, :supervisor)) do
            :expired -> {:ok, stored_state}
            {:locked, lock_pid} -> {:error, {:already_started, lock_pid}}
          end
        end

      {:error, :not_found} ->
        :error

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp aquire_init_lock(%{
         module: module,
         key: key,
         prefix: prefix,
         object_store: object_store,
         user_state: user_state,
         old_vsn: old_vsn,
         etag: etag,
         meta: meta,
         supervisor_name: supervisor_name,
         circuit_breake: circuit_breaker,
         init_from: init_from,
         preloaded_boot: preloaded_boot,
         sticky_placement_history_limit: history_limit
       }) do
    {init_from_ref, init_from_pid, init_reply_to} = normalize_init_from(init_from)
    config = module.__durable_server_config__()

    # Load existing placement history from meta, or start with empty list for new servers
    sticky_placement_history =
      case meta do
        nil -> []
        %Meta{sticky_placement_history: history} when is_list(history) -> history
        %Meta{} -> []
      end

    state = %DurableServer{
      object_store: object_store,
      # full key with prefix for use in all storage ops
      key: key,
      # original unprefixed key passed by user, used for group registry
      prefix: prefix,
      preloaded_boot: preloaded_boot,
      vsn: config.vsn,
      etag: etag,
      old_vsn: old_vsn,
      user_state: user_state,
      module: module,
      supervisor: supervisor_name,
      dynamic_supervisor: DurableServer.Supervisor.get_dynamic_supervisor(supervisor_name),
      task_supervisor: DurableServer.Supervisor.get_task_supervisor(supervisor_name),
      circuit_breaker: circuit_breaker,
      last_synced_user_state_hash: nil,
      node_str: to_string(Node.self()),
      pid: self(),
      status: :running,
      last_heartbeat_at: System.system_time(:millisecond),
      node_ref: DurableServer.Supervisor.node_ref(supervisor_name),
      init_from_ref: init_from_ref,
      init_from_pid: init_from_pid,
      init_reply_to: init_reply_to,
      sticky_placement_history: sticky_placement_history,
      sticky_placement_history_limit: history_limit
    }

    case acquire_lock(state, meta) do
      {:ok, %DurableServer{} = new_state} ->
        {:ok, new_state}

      {:error, reason} ->
        case reason do
          {:already_started, _} ->
            Logger.warning("Failed to initialize DurableServer: #{inspect(reason)}")

          _ ->
            Logger.error("Failed to initialize DurableServer: #{inspect(reason)}")
        end

        {:error, reason}
    end
  end

  defp acquire_delete_lock(
         %StorageBackend{} = store,
         %{key: key, prefix: prefix, supervisor: supervisor_name}
       ) do
    storage_key = prefix <> key

    Logger.info("delete: trying to aquire delete lock for #{storage_key}")
    # first try to claim (object doesn't exist)
    deleting_data = %StoredState{
      vsn: 1,
      state: %{},
      meta: %DurableServer.Meta{
        status: :deleting,
        pid: nil,
        node_str: Atom.to_string(Node.self()),
        node_ref: nil,
        last_heartbeat_at: System.system_time(:millisecond),
        crash_history: []
      }
    }

    case StorageBackend.get_object(store, storage_key, consistent: true) do
      # object already deleted, so we proceed as normal
      {:error, :not_found} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}

      # object still exists and we have its etag for writing our lock
      {:ok, %{body: %StoredState{meta: %Meta{} = meta}, etag: current_etag}} ->
        Logger.info("delete: #{storage_key} exists, attempting to claim orphaned lock")

        meta =
          meta_with_runtime_context(meta, %{key: key, prefix: prefix, supervisor: supervisor_name})

        cond do
          Meta.deleting?(meta) and delete_tombstone_delete_window_open?(meta) ->
            {:ok, current_etag}

          Meta.deleting?(meta) and delete_tombstone_active?(meta) ->
            {:error, {:locked, :deleting}}

          true ->
            result =
              StorageBackend.update_object(
                store,
                storage_key,
                fn
                  # someone raced us on the lock. If they wrote a delete tombstone,
                  # use that tombstone etag; otherwise treat their fresh lock as live.
                  %{body: %StoredState{meta: %Meta{} = meta}, etag: etag}
                  when etag != current_etag ->
                    meta =
                      meta_with_runtime_context(meta, %{
                        key: key,
                        prefix: prefix,
                        supervisor: supervisor_name
                      })

                    cond do
                      Meta.deleting?(meta) ->
                        {:error, {:deleting, etag}}

                      Meta.cordoned?(meta) ->
                        {:ok, deleting_data}

                      true ->
                        {:error, {:locked, meta.pid}}
                    end

                  %{body: %StoredState{}, etag: etag} when etag != current_etag ->
                    {:error, {:locked, nil}}

                  %{body: %StoredState{} = stored_state, etag: ^current_etag} ->
                    stored_state =
                      attach_stored_state_context(stored_state, %{
                        key: key,
                        prefix: prefix,
                        supervisor: supervisor_name
                      })

                    if Meta.cordoned?(stored_state.meta) do
                      Logger.info("delete: #{storage_key} found to be cordoned, claimed key")

                      {:ok, deleting_data}
                    else
                      case check_lock(stored_state.meta, supervisor_name) do
                        :expired ->
                          Logger.info(
                            "delete: #{storage_key} found to be expired, claimed expired key"
                          )

                          {:ok, deleting_data}

                        {:locked, lock_pid} ->
                          Logger.info(
                            "delete: cannot claim lock for delete on #{storage_key} - locked by #{inspect(lock_pid)}"
                          )

                          {:error, {:locked, lock_pid}}
                      end
                    end

                  %{body: other, etag: _etag} ->
                    {:error, {:unexpected_value_type, other}}
                end,
                timeout: :infinity,
                max_retries: 0
              )

            case result do
              {:ok, %{etag: tombstone_etag}} -> {:ok, tombstone_etag}
              {:error, {:deleting, tombstone_etag}} -> {:ok, tombstone_etag}
              # object no longer exists, so we proceed as normal
              {:error, :not_found} -> {:ok, nil}
              {:error, {:locked, lock_pid}} -> {:error, {:locked, lock_pid}}
              {:error, reason} -> {:error, reason}
            end
        end

      {:ok, %{body: other}} ->
        {:error, {:unexpected_value_type, other}}
    end
  end

  defp register_pid(%DurableServer{} = state) do
    case DurableServer.Supervisor.__register_child__(
           state.supervisor,
           state.key,
           %DurableServer.GroupMeta{
             key: state.key,
             module: state.module,
             storage_key: storage_key(state),
             node_ref: state.node_ref,
             start_time: state.start_time,
             user_meta: state.user_meta,
             supervisor: state.supervisor
           }
         ) do
      :ok ->
        state

      {:error, :taken} when not state.bootstrapped ->
        case DurableServer.Supervisor.lookup(state.supervisor, state.key) do
          {pid, _meta} when is_pid(pid) ->
            init_error = {:already_started, pid}
            send(state.init_reply_to, {state.init_from_ref, {:error, init_error}})
            exit({:shutdown, {@durable, {:init_failed, init_error}}})

          nil ->
            fatal_invalid_lock_claim!(state)
        end

      # Losing an owned registration after bootstrap is an invariant violation.
      {:error, :taken} ->
        fatal_invalid_lock_claim!(state)
    end
  end

  defp fatal_invalid_lock_claim!(%DurableServer{} = state) do
    fatal_exit!("invalid lock claim for key #{state.key}: #{inspect(node: node(), pid: self())}")
  end

  defp update_registry_meta(%DurableServer{user_meta: user_meta} = state, new_user_meta)
       when is_map(new_user_meta) and new_user_meta == user_meta do
    state
  end

  defp update_registry_meta(%DurableServer{} = state, new_user_meta) when is_map(new_user_meta) do
    # update group registry with new user metadata while preserving internal metadata
    register_pid(%{state | user_meta: new_user_meta})
  end

  defp conditional_delete_opts(%StorageBackend{} = store, etag) when is_binary(etag) do
    if StorageBackend.supports?(store, :conditional_delete?), do: [etag: etag], else: []
  end

  defp conditional_delete_opts(%StorageBackend{}, _etag), do: []

  @doc false
  def __delete_request__(_supervisor, pid, timeout, _config)
      when is_pid(pid) and is_integer(timeout) do
    delete_by_pid(pid, timeout)
  end

  def __delete_request__(supervisor_name, key, timeout, config)
      when is_binary(key) and is_integer(timeout) do
    case DurableServer.Supervisor.lookup(supervisor_name, key) do
      {pid, _meta} ->
        # pid is in registry, try to delete thru that first, falling back to object store lock if it fails
        case delete_by_pid(pid, timeout) do
          :ok ->
            :ok

          {:error, :noproc} ->
            delete_with_lock_attempt(key, timeout, config)

          {:error, :timeout} ->
            delete_with_lock_attempt(key, timeout, config)

          {:error, _reason} = error ->
            error
        end

      nil ->
        delete_with_lock_attempt(key, timeout, config)
    end
  end

  defp delete_with_lock_attempt(key, timeout, config) do
    # Attempts to atomically acquire a deletion lock on the object using the same
    # lock acquisition logic as init. If successful, marks as :deleting and deletes.
    # If the object is locked by an active process, sends a delete message to the process instead.
    %{prefix: prefix, storage_backend: store} = config
    storage_key = prefix <> key

    case acquire_delete_lock(store, %{
           key: key,
           prefix: prefix,
           supervisor: Map.get(config, :name)
         }) do
      {:ok, nil} ->
        Logger.info("Object #{storage_key} already deleted")
        :ok

      # TODO have lifecycle manager handle cleaning up delete locks that failed at this point
      {:ok, tombstone_etag} when is_binary(tombstone_etag) ->
        # successfully acquired lock and marked as :deleting, now delete the object
        delete_opts = conditional_delete_opts(store, tombstone_etag)

        case StorageBackend.delete_object(store, storage_key, delete_opts) do
          :ok ->
            Logger.info("Successfully deleted #{storage_key} after acquiring delete lock")
            :ok

          {:error, :not_found} ->
            Logger.info("Object #{storage_key} already deleted")
            :ok

          {:error, :conflict} ->
            Logger.info(
              "Delete tombstone for #{storage_key} was replaced before physical delete; skipping stale delete"
            )

            :ok

          {:error, reason} ->
            Logger.error(
              "Failed to delete #{storage_key} after acquiring lock: #{inspect(reason)}"
            )

            {:error, reason}
        end

      {:error, {:locked, pid}} ->
        # object is locked by active process, message it to delete itself
        Logger.info(
          "Object #{storage_key} is locked, sending delete message to process #{inspect(pid)}"
        )

        if is_pid(pid) do
          delete_by_pid(pid, timeout)
        else
          {:error, {:locked, pid}}
        end

      {:error, reason} ->
        Logger.error("Failed to acquire delete lock for #{storage_key}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp delete_by_pid(pid, timeout) when is_pid(pid) and is_integer(timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    ref = make_ref()
    monitor_ref = Process.monitor(pid)
    send(pid, {@durable, {:delete_request, ref, self()}})

    try do
      receive do
        {:delete_in_progress, ^ref} ->
          Logger.info("Process #{inspect(pid)} accepted delete request")
          await_delete_completion(pid, ref, monitor_ref, deadline, nil, nil)

        # process is dead and did not process our delete request
        {:DOWN, ^monitor_ref, :process, ^pid, _reason} ->
          {:error, :noproc}
      after
        remaining_delete_timeout(deadline) -> {:error, :timeout}
      end
    after
      Process.demonitor(monitor_ref, [:flush])
    end
  end

  defp remaining_delete_timeout(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end

  defp await_delete_completion(_pid, _ref, _monitor_ref, _deadline, :ok, down_reason)
       when not is_nil(down_reason) do
    :ok
  end

  defp await_delete_completion(_pid, _ref, _monitor_ref, _deadline, {:error, reason}, down_reason)
       when not is_nil(down_reason) do
    {:error, reason}
  end

  defp await_delete_completion(pid, ref, monitor_ref, deadline, delete_result, down_reason) do
    remaining_timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    if remaining_timeout <= 0 do
      delete_timeout_result(delete_result, down_reason)
    else
      receive do
        {:delete_complete, ^ref, :ok} ->
          await_delete_completion(pid, ref, monitor_ref, deadline, :ok, down_reason)

        {:delete_complete, ^ref, {:error, _reason} = result} ->
          await_delete_completion(pid, ref, monitor_ref, deadline, result, down_reason)

        {:DOWN, ^monitor_ref, :process, ^pid, reason} ->
          await_delete_completion(pid, ref, monitor_ref, deadline, delete_result, reason)
      after
        remaining_timeout ->
          delete_timeout_result(delete_result, down_reason)
      end
    end
  end

  defp delete_timeout_result({:error, reason}, _down_reason), do: {:error, reason}
  defp delete_timeout_result(:ok, nil), do: {:error, :timeout}
  defp delete_timeout_result(nil, nil), do: {:error, :timeout}

  defp delete_timeout_result(nil, down_reason),
    do: {:error, {:delete_not_confirmed, down_reason}}

  defp delete_timeout_result(:ok, _down_reason), do: :ok

  @doc false
  def __cordon_request__(_supervisor, pid, timeout, _config)
      when is_pid(pid) and is_integer(timeout) do
    cordon_by_pid(pid, timeout)
  end

  def __cordon_request__(supervisor_name, key, timeout, config)
      when is_atom(supervisor_name) and is_binary(key) and is_integer(timeout) do
    cordon_key_attempt(supervisor_name, key, timeout, config, _retries = 1)
  end

  @doc false
  def __uncordon_request__(supervisor_name, key, config)
      when is_atom(supervisor_name) and is_binary(key) do
    uncordon_key_attempt(supervisor_name, key, config, _retries = 1)
  end

  defp cordon_key_attempt(supervisor_name, key, timeout, config, retries) do
    %{prefix: prefix, storage_backend: store} = config
    storage_key = prefix <> key

    case fetch_stored_state(store, %{key: key, prefix: prefix, supervisor: supervisor_name},
           consistent: true
         ) do
      {:ok, %StoredState{meta: %Meta{} = meta} = stored_state} ->
        cond do
          Meta.cordoned?(meta) ->
            :ok

          Meta.deleting?(meta) ->
            {:error, {:locked, :deleting}}

          Meta.running?(meta) ->
            case check_lock(meta, supervisor_name) do
              {:locked, pid} when is_pid(pid) ->
                case cordon_by_pid(pid, timeout) do
                  {:error, :noproc} when retries > 0 ->
                    cordon_key_attempt(supervisor_name, key, timeout, config, retries - 1)

                  {:error, {:cordon_not_confirmed, _reason}} when retries > 0 ->
                    cordon_key_attempt(supervisor_name, key, timeout, config, retries - 1)

                  other ->
                    other
                end

              {:locked, lock_pid} ->
                {:error, {:locked, lock_pid}}

              :expired ->
                put_cordoned_stored_state(
                  store,
                  storage_key,
                  stored_state,
                  fn ->
                    cordon_key_attempt(supervisor_name, key, timeout, config, retries - 1)
                  end,
                  retries
                )
            end

          true ->
            put_cordoned_stored_state(
              store,
              storage_key,
              stored_state,
              fn -> cordon_key_attempt(supervisor_name, key, timeout, config, retries - 1) end,
              retries
            )
        end

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp uncordon_key_attempt(supervisor_name, key, config, retries) do
    %{prefix: prefix, storage_backend: store} = config
    storage_key = prefix <> key

    case fetch_stored_state(store, %{key: key, prefix: prefix, supervisor: supervisor_name},
           consistent: true
         ) do
      {:ok, %StoredState{meta: %Meta{} = meta} = stored_state} ->
        if Meta.cordoned?(meta) do
          put_uncordoned_stored_state(
            store,
            storage_key,
            stored_state,
            fn -> uncordon_key_attempt(supervisor_name, key, config, retries - 1) end,
            retries
          )
        else
          :ok
        end

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp put_cordoned_stored_state(
         store,
         storage_key,
         %StoredState{} = stored_state,
         rerun_key_flow,
         retries
       ) do
    updated_meta =
      stored_state.meta
      |> Meta.put_status(:cordoned)
      |> Meta.clear_restart_attempt()

    put_status_stored_state(
      store,
      storage_key,
      %{stored_state | meta: updated_meta},
      rerun_key_flow,
      retries
    )
  end

  defp put_uncordoned_stored_state(
         store,
         storage_key,
         %StoredState{} = stored_state,
         rerun_key_flow,
         retries
       ) do
    updated_meta =
      stored_state.meta
      |> Meta.put_status(:stopped_graceful)
      |> Meta.clear_restart_attempt()

    put_status_stored_state(
      store,
      storage_key,
      %{stored_state | meta: updated_meta},
      rerun_key_flow,
      retries
    )
  end

  defp put_status_stored_state(
         %StorageBackend{} = store,
         storage_key,
         %StoredState{etag: etag} = stored_state,
         rerun_key_flow,
         retries
       )
       when is_binary(storage_key) and is_function(rerun_key_flow, 0) do
    case StorageBackend.put_object(store, storage_key, stored_state, etag: etag) do
      {:ok, _object} ->
        :ok

      {:error, :conflict} when retries > 0 ->
        # Conflict means our lock view is stale. Re-enter the key flow so the
        # next attempt performs a consistent read and fresh lock/orphan check.
        rerun_key_flow.()

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp cordon_by_pid(pid, timeout) when is_pid(pid) and is_integer(timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    ref = make_ref()
    monitor_ref = Process.monitor(pid)
    send(pid, {@durable, {:cordon_request, ref, self()}})

    try do
      receive do
        {:cordon_in_progress, ^ref} ->
          Logger.info("Process #{inspect(pid)} accepted cordon request")
          await_cordon_completion(pid, ref, monitor_ref, deadline, nil, nil)

        {:DOWN, ^monitor_ref, :process, ^pid, _} ->
          {:error, :noproc}
      after
        remaining_delete_timeout(deadline) ->
          {:error, :timeout}
      end
    after
      Process.demonitor(monitor_ref, [:flush])
    end
  end

  defp await_cordon_completion(_pid, _ref, _monitor_ref, _deadline, :ok, down_reason)
       when not is_nil(down_reason) do
    :ok
  end

  defp await_cordon_completion(_pid, _ref, _monitor_ref, _deadline, {:error, reason}, down_reason)
       when not is_nil(down_reason) do
    {:error, reason}
  end

  defp await_cordon_completion(pid, ref, monitor_ref, deadline, cordon_result, down_reason) do
    remaining_timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    if remaining_timeout <= 0 do
      cordon_timeout_result(cordon_result, down_reason)
    else
      receive do
        {:cordon_complete, ^ref, :ok} ->
          await_cordon_completion(pid, ref, monitor_ref, deadline, :ok, down_reason)

        {:cordon_complete, ^ref, {:error, _reason} = result} ->
          await_cordon_completion(pid, ref, monitor_ref, deadline, result, down_reason)

        {:DOWN, ^monitor_ref, :process, ^pid, reason} ->
          await_cordon_completion(pid, ref, monitor_ref, deadline, cordon_result, reason)
      after
        remaining_timeout ->
          cordon_timeout_result(cordon_result, down_reason)
      end
    end
  end

  defp cordon_timeout_result({:error, reason}, _down_reason), do: {:error, reason}
  defp cordon_timeout_result(:ok, nil), do: {:error, :timeout}
  defp cordon_timeout_result(nil, nil), do: {:error, :timeout}

  defp cordon_timeout_result(nil, down_reason),
    do: {:error, {:cordon_not_confirmed, down_reason}}

  defp cordon_timeout_result(:ok, _down_reason), do: :ok

  @doc false
  def claim_restart_attempt(%ObjectStore{} = store, %StoredState{} = stored_state, opts) do
    backend = StorageBackend.new(DurableServer.Backends.ObjectStore, store)
    claim_restart_attempt(backend, stored_state, opts)
  end

  def claim_restart_attempt(%StorageBackend{} = store, %StoredState{} = stored_state, opts) do
    do_claim_restart_attempt(store, stored_state, opts, :check_lock)
  end

  @doc false
  def claim_restart_attempt_with_verified_expired_lock(
        %ObjectStore{} = store,
        %StoredState{} = stored_state,
        opts
      ) do
    backend = StorageBackend.new(DurableServer.Backends.ObjectStore, store)
    claim_restart_attempt_with_verified_expired_lock(backend, stored_state, opts)
  end

  def claim_restart_attempt_with_verified_expired_lock(
        %StorageBackend{} = store,
        %StoredState{} = stored_state,
        opts
      ) do
    do_claim_restart_attempt(store, stored_state, opts, :verified_expired_lock)
  end

  defp do_claim_restart_attempt(
         %StorageBackend{} = store,
         %StoredState{} = stored_state,
         opts,
         lock_check
       )
       when lock_check in [:check_lock, :verified_expired_lock] do
    opts = Keyword.validate!(opts, [:ttl])
    ttl_ms = Keyword.fetch!(opts, :ttl)
    %{meta: meta} = stored_state

    storage_key = stored_state.prefix <> stored_state.key

    cond do
      Meta.currently_restarting?(meta) ->
        {:error, :already_claimed}

      Meta.cordoned?(meta) ->
        {:error, :not_eligible}

      Meta.stopped_permanently?(meta) ->
        {:error, :not_eligible}

      not restart_claim_lock_available?(meta, lock_check) ->
        {:error, :not_eligible}

      true ->
        updated_meta =
          Meta.put_restart_attempt(meta, %{
            restart_attempt_node: to_string(Node.self()),
            ttl_ms: ttl_ms
          })

        updated_stored_state = %{stored_state | meta: updated_meta}

        case StorageBackend.put_object(store, storage_key, updated_stored_state,
               etag: stored_state.etag
             ) do
          {:ok, %{body: _, etag: _} = obj} -> {:ok, obj}
          # someone raced us b/w list_objects and update
          {:error, :conflict} -> {:error, :not_eligible}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp restart_claim_lock_available?(%Meta{}, :verified_expired_lock), do: true

  defp restart_claim_lock_available?(%Meta{} = meta, :check_lock) do
    not match?({:locked, _}, check_lock(meta))
  end

  @doc """
  Clear restart attempt metadata from a server object.
  """
  def clear_restart_attempt(%ObjectStore{} = store, data) do
    backend = StorageBackend.new(DurableServer.Backends.ObjectStore, store)
    clear_restart_attempt(backend, data)
  end

  def clear_restart_attempt(%StorageBackend{} = store, %{
        key: key,
        prefix: prefix,
        body: body,
        etag: etag
      })
      when is_binary(key) do
    storage_key = prefix <> key

    case body do
      %StoredState{meta: %Meta{} = meta} = stored_state ->
        stored_state =
          attach_stored_state_context(
            %{stored_state | etag: etag},
            %{key: key, prefix: prefix, supervisor: meta.supervisor}
          )

        updated_meta = Meta.clear_restart_attempt(meta)
        updated = %{stored_state | meta: updated_meta}

        case StorageBackend.put_object(store, storage_key, updated, etag: etag) do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, reason}
        end

      other ->
        {:error, {:unexpected_value_type, other}}
    end
  end

  @doc """
  Get just the metadata for a server without the full object.
  """
  def get_server_metadata(%ObjectStore{} = store, path) do
    backend = StorageBackend.new(DurableServer.Backends.ObjectStore, store)
    get_server_metadata(backend, path)
  end

  def get_server_metadata(%StorageBackend{} = store, %{key: key, prefix: prefix}) do
    case fetch_stored_state(store, %{key: key, prefix: prefix}, consistent: true) do
      {:ok, %{meta: %Meta{} = meta}} ->
        {:ok, meta}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def handle_call(
        {@durable, {:stop_with_status, status, reason}},
        _from,
        %DurableServer{} = state
      ) do
    # Defer final status persistence to terminate/2 so user callback terminate/2
    # has completed before lock visibility changes.
    updated_state = %{state | final_status_set: status}
    {:stop, reason, :ok, updated_state}
  end

  def handle_call(
        {@durable, {:stop_for_rehome, restart_attempt_node, restart_attempt_ttl_ms, reason}},
        _from,
        %DurableServer{} = state
      )
      when is_binary(restart_attempt_node) and is_integer(restart_attempt_ttl_ms) and
             restart_attempt_ttl_ms > 0 do
    updated_state =
      state
      |> put_restart_attempt(restart_attempt_node, restart_attempt_ttl_ms)
      |> Map.put(:final_status_set, :stopped_graceful)

    {:stop, reason, :ok, updated_state}
  end

  def handle_call({@durable, :get_etag}, _from, %DurableServer{etag: etag} = state) do
    {:reply, {:ok, etag}, state}
  end

  def handle_call(request, from, %DurableServer{} = state) do
    state = maybe_migrate_on_callback(state)
    result = state.module.handle_call(request, from, state.user_state)
    process_callback_result(result, state)
  end

  @impl true
  def handle_cast(request, %__MODULE__{} = state) do
    state = maybe_migrate_on_callback(state)
    result = state.module.handle_cast(request, state.user_state)
    process_callback_result(result, state)
  end

  @impl true
  # custom group registry resolver
  def handle_info(
        {:EXIT, _pid, {:shutdown, {@durable, {:fatal_exit, :registry_conflict}}}},
        %__MODULE__{} = state
      ) do
    fatal_exit!(
      "#{state.key} netsplit recovery chose the other side as winner: #{inspect(key: state.key, node: node(), pid: self())}"
    )
  end

  def handle_info({:shutdown, {@durable, {:fatal_exit, reason}}}, %__MODULE__{} = _state) do
    fatal_exit!(reason)
  end

  # default group registry resolver
  def handle_info({:EXIT, _pid, {:group_registry_conflict, key, _meta}}, %__MODULE__{} = state) do
    fatal_exit!(
      "#{state.key} netsplit recovery chose the other side as winner: #{inspect(key: key, node: node(), pid: self())}"
    )
  end

  def handle_info({@durable, :sync}, %__MODULE__{} = state) do
    case sync_to_storage(state) do
      {:ok, %DurableServer{} = new_state} ->
        {:noreply, schedule_sync(new_state)}

      {:error, :conflict} ->
        fatal_sync_conflict!(state)

      {:error, reason} ->
        # continue without stopping for transient errors (ie timeouts), but log the error
        Logger.error("#{state.key} failed periodic sync: #{inspect(reason)}")
        {:noreply, schedule_sync(state)}
    end
  end

  def handle_info({@durable, {:sync_and_stop, reason}}, %__MODULE__{} = state) do
    Logger.info(
      "DurableServer #{state.key} received graceful shutdown request: #{inspect(reason)}"
    )

    # Mark that Terminator handled this shutdown and defer status persistence to
    # terminate/2 after user callback terminate/2 has run.
    updated_state =
      %{state | terminator_handled: true, final_status_set: :stopped_graceful}

    # stop normally so the terminator can track our shutdown
    {:stop, :normal, updated_state}
  end

  def handle_info({@durable, {:delete_request, ref, requester_pid}}, %__MODULE__{} = state) do
    Logger.info(
      "DurableServer #{state.key} received delete request from #{inspect(requester_pid)}"
    )

    # notify the requester that we're starting deletion
    send(requester_pid, {:delete_in_progress, ref})

    # Defer status persistence to terminate/2 after user callback terminate/2 has run.
    updated_state =
      %{
        state
        | user_initiated_stop: {:shutdown, :delete},
          final_status_set: :deleting,
          delete_requester: {requester_pid, ref}
      }

    # stop with delete reason to trigger deletion in terminate/2
    {:stop, {:shutdown, :delete}, updated_state}
  end

  def handle_info({@durable, {:cordon_request, ref, requester_pid}}, %__MODULE__{} = state) do
    Logger.info(
      "DurableServer #{state.key} received cordon request from #{inspect(requester_pid)}"
    )

    send(requester_pid, {:cordon_in_progress, ref})

    updated_state = %{
      state
      | final_status_set: :cordoned,
        cordon_requester: {requester_pid, ref}
    }

    {:stop, :normal, updated_state}
  end

  def handle_info(msg, %DurableServer{} = state) do
    state = maybe_migrate_on_callback(state)
    result = state.module.handle_info(msg, state.user_state)
    process_callback_result(result, state)
  end

  @impl true
  def handle_continue({@bootstrap_continue, bootstrap}, %DurableServer{} = state) do
    bootstrap_init(state, bootstrap)
  end

  @impl true
  def handle_continue(continue, %DurableServer{} = state) do
    state = maybe_migrate_on_callback(state)
    result = state.module.handle_continue(continue, state.user_state)
    process_callback_result(result, state)
  end

  @impl true
  def terminate(_reason, %DurableServer{bootstrapped: false}) do
    :ok
  end

  @impl true
  # for a fatal exit we do not attempt to persist anything
  def terminate({:shutdown, {@durable, {:fatal_exit, reason}}}, state) do
    Logger.error("fatal exit from #{state.key}: #{inspect(reason)}")
    :ok
  end

  def terminate(reason, %DurableServer{} = state) do
    # Ensure user callback terminate/2 finishes before we persist final status metadata.
    terminate_return = state.module.terminate(reason, state.user_state)

    {final_status, sync_result, delete_result} =
      case state.user_initiated_stop do
        nil ->
          handle_external_terminate(reason, state)

        user_stop ->
          handle_user_initiated_terminate(user_stop, reason, state)
      end

    maybe_invoke_after_terminate(state, terminate_return, reason, final_status, sync_result)
    maybe_notify_delete_requester(state, delete_result)
    maybe_notify_cordon_requester(state, sync_result)
  end

  defp bootstrap_init(
         %DurableServer{
           module: module,
           supervisor: supervisor_name,
           object_store: object_store,
           key: key,
           prefix: prefix,
           circuit_breaker: circuit_breaker,
           preloaded_boot: preloaded_boot,
           init_from_ref: from_ref,
           init_from_pid: from_pid,
           init_reply_to: reply_to,
           sticky_placement_history_limit: sticky_placement_history_limit
         } = state,
         %{
           init_from: init_from,
           init_arg: init_arg,
           boot_info: boot_info,
           config: config,
           capacity_opts: capacity_opts
         }
       ) do
    with :ok <- maybe_check_global_lock_circuit_breaker(circuit_breaker, preloaded_boot),
         :ok <-
           LifecycleManager.check_capacity(
             supervisor_name,
             module,
             Keyword.put(capacity_opts, :skip_count_limits, true)
           ) do
      current_node_str = to_string(Node.self())

      storage_request = %{key: key, prefix: prefix, supervisor: supervisor_name}

      load_result =
        case fetch_existing_state_raw(
               object_store,
               storage_request,
               boot_info,
               consistent: false
             ) do
          {:ok, %StoredState{} = existing} ->
            case maybe_confirm_deleting_tombstone(
                   object_store,
                   storage_request,
                   existing,
                   preloaded_boot
                 ) do
              {:ok, %StoredState{} = existing} ->
                %{meta: %Meta{} = meta} = existing

                if Meta.cordoned?(meta) do
                  {:error, :cordoned}
                else
                  case active_restart_claim(meta, boot_info, current_node_str) do
                    {:claimed, claimant_node} ->
                      {:error, {:restart_claimed, claimant_node}}

                    :ok ->
                      case check_lock(meta, supervisor_name) do
                        {:locked, lock_pid} ->
                          {:error, {:already_started, lock_pid}}

                        :expired ->
                          if Meta.deleting?(meta) do
                            load_fresh_init_state(module, init_arg, object_store,
                              etag: existing.etag
                            )
                          else
                            if preloaded_boot do
                              loaded_state = load_user_state(module, existing.vsn, existing.state)
                              {:ok, {loaded_state, existing.vsn, existing.etag, meta}}
                            else
                              case reread_expired_state(object_store, storage_request) do
                                {:ok,
                                 %StoredState{
                                   meta: %Meta{} = current_meta,
                                   vsn: current_vsn,
                                   state: current_raw_state,
                                   etag: current_etag
                                 }} ->
                                  loaded_state =
                                    load_user_state(module, current_vsn, current_raw_state)

                                  {:ok, {loaded_state, current_vsn, current_etag, current_meta}}

                                {:error, {:already_started, lock_pid}} ->
                                  {:error, {:already_started, lock_pid}}

                                :error ->
                                  load_fresh_init_state(module, init_arg, object_store)

                                {:error, reason} ->
                                  {:error, reason}
                              end
                            end
                          end
                      end
                  end
                end

              :not_found ->
                load_fresh_init_state(module, init_arg, object_store)

              {:error, reason} ->
                {:error, reason}
            end

          :error ->
            load_fresh_init_state(module, init_arg, object_store)
        end

      case load_result do
        {:ok, {loaded_init_state, old_vsn, etag, meta}} ->
          current_node_ref = DurableServer.Supervisor.node_ref(supervisor_name)

          same_caller? =
            meta && meta.init_from_ref == from_ref && meta.init_from_pid == from_pid &&
              meta.node_str == current_node_str && meta.node_ref == current_node_ref

          crashed_with_same_caller? = meta && Meta.permanently_crashed?(meta) && same_caller?

          if crashed_with_same_caller? do
            Logger.info(
              "Refusing to restart permanently crashed server (same caller, automatic restart): #{key}"
            )

            send(reply_to, {from_ref, {:error, :permanently_crashed}})
            {:stop, {:shutdown, {@durable, {:init_failed, :permanently_crashed}}}, state}
          else
            case aquire_init_lock(%{
                   module: module,
                   key: key,
                   prefix: prefix,
                   object_store: object_store,
                   user_state: loaded_init_state,
                   old_vsn: old_vsn,
                   etag: etag,
                   meta: meta,
                   supervisor_name: supervisor_name,
                   circuit_breake: circuit_breaker,
                   init_from: init_from,
                   preloaded_boot: preloaded_boot,
                   sticky_placement_history_limit: sticky_placement_history_limit
                 }) do
              {:ok, %DurableServer{} = locked_state} ->
                info = %{
                  key: key,
                  supervisor: supervisor_name,
                  task_supervisor: DurableServer.Supervisor.get_task_supervisor(supervisor_name),
                  dynamic_supervisor:
                    DurableServer.Supervisor.get_dynamic_supervisor(supervisor_name)
                }

                init_info = Map.fetch!(config, :init_info)
                info = Map.merge(info, init_info)

                init_result =
                  if function_exported?(module, :init, 2) do
                    module.init(loaded_init_state, info)
                  else
                    module.init(loaded_init_state)
                  end

                case init_result do
                  :ignore ->
                    handle_ignore(locked_state, init_from)

                  {:ok, user_state} ->
                    handle_init(locked_state, user_state, [], _continue_or_timeout = nil)

                  {:ok, user_state, opts} when is_list(opts) ->
                    handle_init(locked_state, user_state, opts, _continue_or_timeout = nil)

                  {:ok, user_state, {tag, _} = continue_or_timeout}
                  when tag in [:continue, :timeout] ->
                    handle_init(locked_state, user_state, [], continue_or_timeout)

                  {:ok, user_state, {tag, _} = continue_or_timeout, opts}
                  when tag in [:continue, :timeout] and is_list(opts) ->
                    handle_init(locked_state, user_state, opts, continue_or_timeout)

                  other ->
                    Logger.error("Invalid init return from #{module}: #{inspect(other)}")
                    send(reply_to, {from_ref, {:error, {:bad_init_return, other}}})

                    {:stop, {:shutdown, {@durable, {:init_failed, {:bad_init_return, other}}}},
                     state}
                end

              {:error, reason} ->
                send(reply_to, {from_ref, {:error, reason}})
                {:stop, {:shutdown, {@durable, {:init_failed, reason}}}, state}
            end
          end

        {:error, reason} ->
          send(reply_to, {from_ref, {:error, reason}})
          {:stop, {:shutdown, {@durable, {:init_failed, reason}}}, state}
      end
    else
      {:circuit_open, cooldown_ms} ->
        Logger.error(
          "global lock circuit breaker open for #{cooldown_ms}ms, refusing lock acquisition for #{inspect(key)}"
        )

        send(reply_to, {from_ref, {:error, {:circuit_open, :network_partition}}})

        {:stop, {:shutdown, {@durable, {:init_failed, {:circuit_open, :network_partition}}}},
         state}

      {:error, {:limit_reached, reason, details}} ->
        log_capacity_limit(reason, details, supervisor_name, module)
        send(reply_to, {from_ref, {:error, {:capacity_limit, reason}}})
        {:stop, {:shutdown, {@durable, {:init_failed, {:capacity_limit, reason}}}}, state}
    end
  end

  defp normalize_init_from({from_ref, from_pid, reply_to})
       when is_reference(from_ref) and is_pid(from_pid) do
    {from_ref, from_pid, reply_to}
  end

  defp normalize_init_from({from_ref, from_pid})
       when is_reference(from_ref) and is_pid(from_pid) do
    {from_ref, from_pid, from_pid}
  end

  # user-initiated termination
  defp handle_user_initiated_terminate(user_stop, _reason, %DurableServer{} = state) do
    case user_stop do
      user_stop when user_stop in [:delete, {:shutdown, :delete}] ->
        # delete storage for :delete or {:shutdown, :delete}
        Logger.info("DurableServer #{state.key} terminating for deletion - removing from storage")

        final_status = state.final_status_set || :deleting

        {sync_result, tombstone_etag} =
          case maybe_sync_final_status(state, final_status) do
            {:ok, etag} when is_binary(etag) -> {:ok, etag}
            other -> {other, nil}
          end

        delete_result =
          case sync_result do
            :ok ->
              delete_opts = conditional_delete_opts(state.object_store, tombstone_etag)

              case StorageBackend.delete_object(
                     state.object_store,
                     storage_key(state),
                     delete_opts
                   ) do
                :ok ->
                  Logger.info("Successfully deleted storage for #{state.key}")
                  :ok

                {:error, :not_found} ->
                  Logger.info("Storage already deleted for #{state.key}")
                  :ok

                {:error, :conflict} ->
                  Logger.info(
                    "Delete tombstone for #{state.key} was replaced before physical delete; skipping stale delete"
                  )

                  :ok

                {:error, reason} ->
                  Logger.error("Failed to delete storage for #{state.key}: #{inspect(reason)}")
                  {:error, reason}
              end

            {:error, reason} ->
              Logger.error(
                "Skipping physical delete for #{state.key} because delete tombstone sync failed: #{inspect(reason)}"
              )

              {:error, reason}
          end

        {final_status, sync_result, delete_result}

      user_stop
      when user_stop in [:normal, :permanent, {:shutdown, :permanent}, {:shutdown, :normal}] ->
        final_status =
          state.final_status_set ||
            case user_stop do
              stop when stop in [:permanent, {:shutdown, :permanent}] -> :stopped_permanent
              _ -> :stopped_graceful
            end

        sync_result = maybe_sync_final_status(state, final_status)

        Logger.info(
          "DurableServer #{state.key} shutting down gracefully via user stop (#{inspect(user_stop)})"
        )

        {final_status, sync_result, nil}

      {:error, error_reason} ->
        final_status = state.final_status_set || :crashed
        sync_result = maybe_sync_final_status(state, final_status)

        Logger.info("DurableServer #{state.key} stopping with error (#{inspect(error_reason)})")

        {final_status, sync_result, nil}
    end
  end

  # external termination - not user-initiated
  defp handle_external_terminate(reason, %DurableServer{} = state) do
    case state.final_status_set do
      status when not is_nil(status) ->
        sync_result = maybe_sync_final_status(state, status)
        {status, sync_result, nil}

      nil ->
        case reason do
          :shutdown ->
            # external graceful shutdown (e.g., supervisor terminate_child)
            Logger.info(
              "DurableServer #{state.key} shutting down gracefully (reason: #{inspect(reason)})"
            )

            final_status = :stopped_graceful
            sync_result = maybe_sync_final_status(state, final_status)
            {final_status, sync_result, nil}

          {:shutdown, _} ->
            # external graceful shutdown (e.g., supervisor terminate_child)
            Logger.info(
              "DurableServer #{state.key} shutting down gracefully (reason: #{inspect(reason)})"
            )

            final_status = :stopped_graceful
            sync_result = maybe_sync_final_status(state, final_status)
            {final_status, sync_result, nil}

          :normal ->
            {nil, :ok, nil}

          _crash_reason ->
            # this is a crash - update status using crash tracking system
            Logger.error(
              "DurableServer #{state.key} crashed with reason: #{inspect(reason)} - updating crash status"
            )

            # create crash entry
            crash_entry = %{
              timestamp: System.system_time(:millisecond),
              reason: String.slice(inspect(reason), 0, @max_crash_reason_length),
              node_ref: state.node_ref
            }

            # update crash status with tracking
            # if this server was previously permanently crashed and crashes again, restore that status
            final_status =
              if state.was_permanently_crashed do
                # server was previously permanently crashed - if explicitly restarted and crashes again,
                # it goes straight back to permanently crashed without going through crash counting
                case update_server_status_directly(state, :permanently_crashed) do
                  {:ok, %DurableServer{} = _new_state} -> :permanently_crashed
                  {:error, _} -> :crashed
                end
              else
                # normal crash tracking logic using CircuitBreaker
                current_meta = dump_meta(state)

                {status, updated_crash_history} =
                  CircuitBreaker.check_object_crash_status(
                    state.circuit_breaker,
                    current_meta,
                    crash_entry
                  )

                # update storage with both the new status and crash history
                case update_server_status_and_crash_history(state, status, updated_crash_history) do
                  {:ok, _new_state} -> status
                  {:error, _} -> :crashed
                end
              end

            case final_status do
              :crashed ->
                Logger.info("DurableServer #{state.key} marked as crashed")

              :permanently_crashed ->
                if state.was_permanently_crashed do
                  Logger.warning(
                    "DurableServer #{state.key} restored to permanently crashed after crashing again"
                  )
                else
                  Logger.warning(
                    "DurableServer #{state.key} marked as permanently crashed after repeated failures"
                  )
                end
            end

            {final_status, :ok, nil}
        end
    end
  end

  defp maybe_notify_delete_requester(
         %DurableServer{delete_requester: {requester_pid, ref}},
         result
       )
       when is_pid(requester_pid) and is_reference(ref) do
    send(requester_pid, {:delete_complete, ref, result || {:error, :delete_not_attempted}})
    :ok
  end

  defp maybe_notify_delete_requester(%DurableServer{}, _result), do: :ok

  defp maybe_notify_cordon_requester(
         %DurableServer{cordon_requester: {requester_pid, ref}},
         :ok
       )
       when is_pid(requester_pid) and is_reference(ref) do
    send(requester_pid, {:cordon_complete, ref, :ok})
    :ok
  end

  defp maybe_notify_cordon_requester(
         %DurableServer{cordon_requester: {requester_pid, ref}},
         {:error, reason}
       )
       when is_pid(requester_pid) and is_reference(ref) do
    send(requester_pid, {:cordon_complete, ref, {:error, reason}})
    :ok
  end

  defp maybe_notify_cordon_requester(%DurableServer{cordon_requester: {requester_pid, ref}}, _)
       when is_pid(requester_pid) and is_reference(ref) do
    send(requester_pid, {:cordon_complete, ref, {:error, :cordon_not_confirmed}})
    :ok
  end

  defp maybe_notify_cordon_requester(%DurableServer{}, _result), do: :ok

  defp maybe_sync_final_status(%DurableServer{} = state, :deleting) do
    sync_delete_tombstone_to_storage(state)
  end

  defp maybe_sync_final_status(%DurableServer{} = state, status) when is_atom(status) do
    case persist_final_status(state, status) do
      {:ok, %DurableServer{}} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp persist_final_status(%DurableServer{} = state, status) when is_atom(status) do
    report_sync_and_stop = state.terminator_handled and status == :stopped_graceful

    case sync_to_storage(state, meta: %{status: status}) do
      {:ok, %DurableServer{} = new_state} ->
        if report_sync_and_stop do
          LifecycleManager.report_diagnostic(state.supervisor, :sync_and_stop_ok)
        end

        {:ok, new_state}

      {:error, sync_reason} ->
        if report_sync_and_stop do
          LifecycleManager.report_diagnostic(state.supervisor, :sync_and_stop_error)
        end

        Logger.error(
          "Failed to persist final status #{inspect(status)} for #{state.key}: #{inspect(sync_reason)}"
        )

        {:error, sync_reason}
    end
  end

  defp sync_delete_tombstone_to_storage(%DurableServer{} = state) do
    old_user_state = state.user_state
    old_last_synced_user_state_hash = state.last_synced_user_state_hash
    {%DurableServer{} = new_state, _dumped_user_state} = dump_user_state(state)
    user_state_changed? = new_state.last_synced_user_state_hash != old_last_synced_user_state_hash
    now_ms = System.system_time(:millisecond)

    new_state = %{new_state | last_heartbeat_at: now_ms}

    data = %StoredState{
      vsn: new_state.vsn,
      state: %{},
      meta: delete_tombstone_meta(new_state, now_ms)
    }

    case put_object(new_state, storage_key(new_state), data) do
      {:ok, %DurableServer{} = new_state} ->
        synced_state = %{new_state | status: :deleting}

        {:ok, %DurableServer{}} =
          maybe_apply_handle_sync(synced_state, old_user_state, user_state_changed?)

        {:ok, new_state.etag}

      {:error, sync_reason} ->
        Logger.error(
          "Failed to persist final status :deleting for #{state.key}: #{inspect(sync_reason)}"
        )

        {:error, sync_reason}
    end
  end

  defp delete_tombstone_meta(%DurableServer{} = state, now_ms) when is_integer(now_ms) do
    %Meta{
      status: :deleting,
      pid: nil,
      supervisor: nil,
      module: nil,
      node_ref: nil,
      node_str: state.node_str || to_string(Node.self()),
      last_heartbeat_at: now_ms,
      crash_history: []
    }
  end

  defp maybe_invoke_after_terminate(
         %DurableServer{} = state,
         terminate_return,
         reason,
         :stopped_graceful,
         :ok
       ) do
    if function_exported?(state.module, :after_terminate, 2) do
      info = %{
        key: state.key,
        supervisor: state.supervisor,
        final_status: :stopped_graceful,
        sync_result: :ok,
        reason: reason
      }

      try do
        _ = state.module.after_terminate(terminate_return, info)
        :ok
      rescue
        exception ->
          Logger.error("""
          after_terminate callback failed for #{state.key}: #{Exception.message(exception)}
          """)

          :ok
      catch
        kind, caught ->
          Logger.error("""
          after_terminate callback failed for #{state.key}: #{inspect({kind, caught})}
          """)

          :ok
      end
    else
      :ok
    end
  end

  defp maybe_invoke_after_terminate(
         _state,
         _terminate_return,
         _reason,
         _final_status,
         _sync_result
       ),
       do: :ok

  @impl true
  def code_change(old_vsn, %__MODULE__{} = state, extra) do
    case state.module.code_change(old_vsn, state.user_state, extra) do
      {:ok, new_user_state} ->
        {:ok, %{state | user_state: new_user_state}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp dump_meta(%DurableServer{} = state) do
    sticky_placement = build_sticky_placement(state.supervisor, state.module)

    # Update placement history - only add if different from most recent entry
    sticky_placement_history =
      update_sticky_placement_history(
        state.sticky_placement_history,
        sticky_placement,
        state.sticky_placement_history_limit
      )

    %Meta{
      key: state.key,
      prefix: state.prefix,
      supervisor: state.supervisor,
      task_supervisor: state.task_supervisor,
      dynamic_supervisor: state.dynamic_supervisor,
      node_ref: state.node_ref || raise(ArgumentError, "empty node_ref"),
      node_str: state.node_str || raise(ArgumentError, "empty node_str"),
      pid: state.pid || raise(ArgumentError, "empty pid"),
      status: state.status || raise(ArgumentError, "empty status"),
      module: state.module || raise(ArgumentError, "empty module"),
      last_heartbeat_at: state.last_heartbeat_at,
      permanent: state.permanent,
      crash_history: state.crash_history,
      sticky_placement: sticky_placement,
      sticky_placement_history: sticky_placement_history,
      restart_attempt_node: state.restart_attempt_node,
      restart_attempt_time: state.restart_attempt_time,
      restart_attempt_ttl: state.restart_attempt_ttl,
      # store the init_from caller info in meta so we can distinguish automatic vs explicit restarts
      init_from_ref: state.init_from_ref,
      init_from_pid: state.init_from_pid
    }
  end

  defp put_restart_attempt(%DurableServer{} = state, restart_attempt_node, ttl_ms)
       when is_binary(restart_attempt_node) and is_integer(ttl_ms) and ttl_ms > 0 do
    restart_attempt_time = System.system_time(:millisecond)

    %{
      state
      | restart_attempt_node: restart_attempt_node,
        restart_attempt_time: restart_attempt_time,
        restart_attempt_ttl: restart_attempt_time + ttl_ms
    }
  end

  # Updates placement history, adding new entry only if placement changed
  # Prunes oldest entries if exceeding the configured limit
  defp update_sticky_placement_history(history, current_placement, limit) do
    case history do
      [%{placement: ^current_placement} | _] ->
        # No change, return history as-is
        history

      _ ->
        # Add new entry with timestamp, prune oldest if needed
        new_entry = %{at: System.system_time(:millisecond), placement: current_placement}
        Enum.take([new_entry | history], limit)
    end
  end

  defp build_sticky_placement(supervisor, module) do
    case DurableServer.Supervisor.__get_sticky_placement_for_module__(supervisor, module) do
      nil ->
        nil

      list ->
        # Extract env vars from keyword list: [FLY_MACHINE_ID: 10_000, FLY_REGION: 20_000]
        list
        |> Enum.map(fn {env_var_atom, _delay} ->
          case env_var_atom do
            :any ->
              %{env_var: :any, value: :any}

            _ ->
              env_var = to_string(env_var_atom)
              %{env_var: env_var, value: System.get_env(env_var)}
          end
        end)
    end
  end

  defp acquire_lock(%__MODULE__{} = state, meta) when is_struct(meta, Meta) or is_nil(meta) do
    # check if we're taking over a permanently crashed server and load existing crash history
    was_permanently_crashed = if meta, do: Meta.permanently_crashed?(meta), else: false
    existing_crash_history = if meta, do: meta.crash_history, else: []

    # prepare state for storage, loading existing crash history
    state = %{
      state
      | last_heartbeat_at: System.system_time(:millisecond),
        was_permanently_crashed: was_permanently_crashed,
        crash_history: existing_crash_history
    }

    {state, dumped_user_state} = dump_user_state(state)
    dumped_meta = dump_meta(state)

    # if we're taking over a permanently crashed server, clear that status atomically
    final_dumped_meta =
      if was_permanently_crashed do
        Meta.put_status(dumped_meta, :running)
      else
        dumped_meta
      end

    data = %StoredState{
      vsn: state.vsn,
      state: dumped_user_state,
      meta: final_dumped_meta
    }

    case do_lock_object(state, data, state.supervisor) do
      {:ok, %DurableServer{} = new_state} ->
        {:ok, new_state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Hot upgrade introspection: check if module version has changed and migrate state if needed
  defp maybe_migrate_on_callback(%__MODULE__{} = state) do
    current_vsn = state.module.__durable_server_config__().vsn

    if current_vsn == state.vsn do
      # Already up to date
      state
    else
      # Version mismatch - need migration
      Logger.info(
        "[DurableServer] Hot upgrade: #{state.key} migrating from v#{inspect(state.vsn)} to v#{inspect(current_vsn)}"
      )

      {:ok, new_user_state} = state.module.code_change(state.vsn, state.user_state, current_vsn)

      %{state | vsn: current_vsn, old_vsn: state.vsn, user_state: new_user_state}
    end
  end

  defp process_callback_result(result, %__MODULE__{} = state) do
    case result do
      {:reply, reply, new_user_state} ->
        old_user_state = state.user_state

        new_state =
          state
          |> update_state(new_user_state)
          |> auto_sync_to_storage(old_user_state)

        {:reply, reply, new_state}

      # Handle action + options tuple.
      {:reply, reply, new_user_state, action, opts}
      when is_callback_action(action) and is_list(opts) ->
        old_user_state = state.user_state
        {updated_state, sync?} = apply_callback_options(state, opts)

        {final_state, final_action} =
          handle_action(updated_state, new_user_state, action, old_user_state)

        final_state =
          maybe_sync_with_option(final_state, sync? and not sync_action?(action), old_user_state)

        if final_action do
          {:reply, reply, final_state, final_action}
        else
          {:reply, reply, final_state}
        end

      {:reply, reply, new_user_state, opts} when is_list(opts) ->
        old_user_state = state.user_state
        {updated_state, sync?} = apply_callback_options(state, opts)

        new_state =
          updated_state
          |> update_state(new_user_state)
          |> maybe_sync_or_auto_sync(sync?, old_user_state)

        {:reply, reply, new_state}

      {:reply, reply, new_user_state, action} when is_callback_action(action) ->
        {final_state, final_action} =
          handle_action(state, new_user_state, action, state.user_state)

        if final_action do
          {:reply, reply, final_state, final_action}
        else
          {:reply, reply, final_state}
        end

      {:noreply, new_user_state} ->
        old_user_state = state.user_state

        {:noreply,
         state
         |> update_state(new_user_state)
         |> auto_sync_to_storage(old_user_state)}

      {:noreply, new_user_state, action, opts}
      when is_callback_action(action) and is_list(opts) ->
        old_user_state = state.user_state
        {updated_state, sync?} = apply_callback_options(state, opts)

        {final_state, final_action} =
          handle_action(updated_state, new_user_state, action, old_user_state)

        final_state =
          maybe_sync_with_option(final_state, sync? and not sync_action?(action), old_user_state)

        if final_action do
          {:noreply, final_state, final_action}
        else
          {:noreply, final_state}
        end

      {:noreply, new_user_state, opts} when is_list(opts) ->
        old_user_state = state.user_state
        {updated_state, sync?} = apply_callback_options(state, opts)

        new_state =
          updated_state
          |> update_state(new_user_state)
          |> maybe_sync_or_auto_sync(sync?, old_user_state)

        {:noreply, new_state}

      {:noreply, new_user_state, action} when is_callback_action(action) ->
        {final_state, final_action} =
          handle_action(state, new_user_state, action, state.user_state)

        if final_action do
          {:noreply, final_state, final_action}
        else
          {:noreply, final_state}
        end

      {:stop, {:shutdown, :delete}, reply, new_user_state} ->
        # shutdown-wrapped delete
        stopped_state =
          update_state(
            %{state | user_initiated_stop: {:shutdown, :delete}, final_status_set: :deleting},
            new_user_state
          )

        {:stop, {:shutdown, :delete}, reply, stopped_state}

      {:stop, {:shutdown, :delete}, new_user_state} ->
        # shutdown-wrapped delete
        stopped_state =
          update_state(
            %{state | user_initiated_stop: {:shutdown, :delete}, final_status_set: :deleting},
            new_user_state
          )

        {:stop, {:shutdown, :delete}, stopped_state}

      {:stop, :delete, reply, new_user_state} ->
        # non-shutdown wrapped delete - transform to :normal (doesn't propagate exit to linked processes)
        stopped_state =
          update_state(
            %{state | user_initiated_stop: :delete, final_status_set: :deleting},
            new_user_state
          )

        {:stop, :normal, reply, stopped_state}

      {:stop, :delete, new_user_state} ->
        # non-shutdown wrapped delete - transform to :normal (doesn't propagate exit to linked processes)
        stopped_state =
          update_state(
            %{state | user_initiated_stop: :delete, final_status_set: :deleting},
            new_user_state
          )

        {:stop, :normal, stopped_state}

      {:stop, {:shutdown, :normal}, reply, new_user_state} ->
        stopped_state =
          update_state(
            %{
              state
              | user_initiated_stop: {:shutdown, :normal},
                final_status_set: :stopped_graceful
            },
            new_user_state
          )

        {:stop, {:shutdown, :normal}, reply, stopped_state}

      {:stop, {:shutdown, :normal}, new_user_state} ->
        stopped_state =
          update_state(
            %{
              state
              | user_initiated_stop: {:shutdown, :normal},
                final_status_set: :stopped_graceful
            },
            new_user_state
          )

        {:stop, {:shutdown, :normal}, stopped_state}

      {:stop, {:shutdown, :permanent}, reply, new_user_state} ->
        # shutdown-wrapped permanent stop
        stopped_state =
          update_state(
            %{
              state
              | user_initiated_stop: {:shutdown, :permanent},
                final_status_set: :stopped_permanent
            },
            new_user_state
          )

        {:stop, {:shutdown, :permanent}, reply, stopped_state}

      {:stop, {:shutdown, :permanent}, new_user_state} ->
        # shutdown-wrapped permanent stop
        stopped_state =
          update_state(
            %{
              state
              | user_initiated_stop: {:shutdown, :permanent},
                final_status_set: :stopped_permanent
            },
            new_user_state
          )

        {:stop, {:shutdown, :permanent}, stopped_state}

      {:stop, :permanent, reply, new_user_state} ->
        # non-shutdown wrapped permanent - transform to :normal (doesn't propagate exit to linked processes)
        stopped_state =
          update_state(
            %{state | user_initiated_stop: :permanent, final_status_set: :stopped_permanent},
            new_user_state
          )

        {:stop, :normal, reply, stopped_state}

      {:stop, :permanent, new_user_state} ->
        # non-shutdown wrapped permanent - transform to :normal (doesn't propagate exit to linked processes)
        stopped_state =
          update_state(
            %{state | user_initiated_stop: :permanent, final_status_set: :stopped_permanent},
            new_user_state
          )

        {:stop, :normal, stopped_state}

      {:stop, :normal, reply, new_user_state} ->
        stopped_state =
          update_state(
            %{state | user_initiated_stop: :normal, final_status_set: :stopped_graceful},
            new_user_state
          )

        {:stop, :normal, reply, stopped_state}

      {:stop, :normal, new_user_state} ->
        stopped_state =
          update_state(
            %{state | user_initiated_stop: :normal, final_status_set: :stopped_graceful},
            new_user_state
          )

        {:stop, :normal, stopped_state}

      {:stop, {:error, _reason} = error_reason, reply, new_user_state} ->
        stopped_state =
          update_state(
            %{state | user_initiated_stop: error_reason, final_status_set: :crashed},
            new_user_state
          )

        {:stop, error_reason, reply, stopped_state}

      {:stop, {:error, _reason} = error_reason, new_user_state} ->
        stopped_state =
          update_state(
            %{state | user_initiated_stop: error_reason, final_status_set: :crashed},
            new_user_state
          )

        {:stop, error_reason, stopped_state}

      {:stop, reason, _reply, _new_user_state} ->
        raise ArgumentError, """
        Invalid stop reason: #{inspect(reason)}

        Supported stop reasons:
          - :normal
          - :delete
          - :permanent
          - {:shutdown, :delete}
          - {:shutdown, :permanent}
          - {:shutdown, :normal}
          - {:error, reason}
        """

      {:stop, reason, _new_user_state} ->
        raise ArgumentError, """
        Invalid stop reason: #{inspect(reason)}

        Supported stop reasons:
          - :normal
          - :delete
          - :permanent
          - {:shutdown, :delete}
          - {:shutdown, :permanent}
          - {:shutdown, :normal}
          - {:error, reason}
        """

      other ->
        Logger.error("Invalid callback result: #{inspect(other)}")
        {:stop, {:bad_callback_return, other}, state}
    end
  end

  defp handle_action(%__MODULE__{} = state, new_user_state, action, old_user_state) do
    state = update_state(state, new_user_state)

    case action do
      :sync ->
        {do_sync(state, nil, old_user_state), nil}

      {:sync, %{} = metadata} ->
        {do_sync(state, metadata, old_user_state), nil}

      other_action ->
        # handle timeout, hibernate, continue actions
        {state, other_action}
    end
  end

  defp apply_callback_options(%__MODULE__{} = state, opts) when is_list(opts) do
    opts = Keyword.validate!(opts, [:meta, :sync])
    sync? = validate_sync_option!(opts)

    state =
      case Keyword.fetch(opts, :meta) do
        {:ok, new_user_meta} -> update_registry_meta(state, new_user_meta)
        :error -> state
      end

    {state, sync?}
  end

  defp validate_sync_option!(opts) do
    case Keyword.get(opts, :sync, false) do
      value when is_boolean(value) ->
        value

      other ->
        raise ArgumentError, "expected :sync option to be a boolean, got: #{inspect(other)}"
    end
  end

  defp sync_action?(:sync), do: true
  defp sync_action?({:sync, %{} = _metadata}), do: true
  defp sync_action?(_), do: false

  defp maybe_sync_or_auto_sync(%__MODULE__{} = state, true, old_user_state),
    do: do_sync(state, nil, old_user_state)

  defp maybe_sync_or_auto_sync(%__MODULE__{} = state, false, old_user_state),
    do: auto_sync_to_storage(state, old_user_state)

  defp maybe_sync_with_option(%__MODULE__{} = state, true, old_user_state),
    do: do_sync(state, nil, old_user_state)

  defp maybe_sync_with_option(%__MODULE__{} = state, false, _old_user_state), do: state

  defp do_sync(%__MODULE__{} = state, metadata, old_user_state) do
    old_user_state = old_user_state || state.user_state

    sync_result =
      case metadata do
        nil -> sync_to_storage(state, old_user_state: old_user_state)
        %{} = metadata -> sync_to_storage(state, meta: metadata, old_user_state: old_user_state)
      end

    case sync_result do
      {:ok, %DurableServer{} = synced_state} ->
        synced_state

      {:error, reason} ->
        if is_map(metadata) do
          Logger.error("Failed to sync state with metadata: #{inspect(reason)}")
        else
          Logger.error("Failed to sync state: #{inspect(reason)}")
        end

        # The backend has already exhausted its bounded retry policy. Explicit sync is a
        # durability boundary, so never acknowledge the callback after the final write fails.
        fatal_exit!({:sync_failed, reason})
    end
  end

  defp validate_user_state!(%{} = user_state), do: user_state

  defp validate_user_state!(user_state) do
    raise ArgumentError,
          "expected callback to return a map of user state, got: #{inspect(user_state)}"
  end

  defp update_state(%__MODULE__{} = state, new_user_state) do
    new_user_state = validate_user_state!(new_user_state)
    %{state | user_state: new_user_state}
  end

  defp auto_sync_to_storage(%DurableServer{module: module, key: key} = state, old_user_state) do
    if state.auto_sync do
      old_user_state = old_user_state || state.user_state

      # if auto sync fails we continue, but log
      case sync_to_storage(state, old_user_state: old_user_state) do
        {:ok, %DurableServer{} = new_state} ->
          new_state

        {:error, :conflict} ->
          fatal_sync_conflict!(state)

        {:error, reason} ->
          Logger.error(fn ->
            "#{inspect(module)} (key=#{key}) unable to auto_sync: #{inspect(reason)}"
          end)

          state
      end
    else
      state
    end
  end

  defp fatal_sync_conflict!(%DurableServer{} = state) do
    fatal_exit!(
      "#{state.key} object updated out from underneath: #{inspect(node: node(), pid: self())}"
    )
  end

  defp sync_to_storage(%DurableServer{} = state, opts \\ []) do
    opts = Keyword.validate!(opts, [:meta, :old_user_state])
    old_user_state = Keyword.get(opts, :old_user_state, state.user_state)

    # if meta overrides are provided, we always force sync
    {meta_overrides, force} =
      case Keyword.fetch(opts, :meta) do
        {:ok, %{} = meta} -> {meta, true}
        :error -> {%{}, false}
      end

    allowed_override_keys = [:status, :last_heartbeat_at]
    invalid_keys = Map.keys(meta_overrides) -- allowed_override_keys

    unless Enum.empty?(invalid_keys) do
      raise ArgumentError,
            "invalid metadata override keys: #{inspect(invalid_keys)}, allowed: #{inspect(allowed_override_keys)}"
    end

    old_last_synced_user_state_hash = state.last_synced_user_state_hash
    {%DurableServer{} = new_state, dumped_user_state} = dump_user_state(state)
    user_state_changed? = new_state.last_synced_user_state_hash != old_last_synced_user_state_hash

    if force || user_state_changed? do
      new_state = %{new_state | last_heartbeat_at: System.system_time(:millisecond)}
      %Meta{} = base_meta = dump_meta(new_state)

      %Meta{} =
        new_meta =
        case meta_overrides do
          %{status: status} -> Meta.put_status(base_meta, status)
          %{} -> base_meta
        end

      data = %StoredState{
        vsn: new_state.vsn,
        state: dumped_user_state,
        meta: new_meta
      }

      case put_object(new_state, storage_key(new_state), data) do
        {:ok, %DurableServer{} = new_state} ->
          synced_state = %{new_state | status: new_meta.status}
          maybe_apply_handle_sync(synced_state, old_user_state, user_state_changed?)

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:ok, new_state}
    end
  end

  defp maybe_apply_handle_sync(%DurableServer{} = state, old_user_state, true) do
    {:ok, apply_handle_sync!(state, old_user_state)}
  end

  defp maybe_apply_handle_sync(%DurableServer{} = state, _old_user_state, false) do
    {:ok, state}
  end

  defp apply_handle_sync!(%DurableServer{} = state, old_user_state) do
    info = %{key: state.key, supervisor: state.supervisor, status: state.status}

    case state.module.handle_sync(info, old_user_state, state.user_state) do
      {:ok, new_user_state} ->
        update_state(state, new_user_state)

      other ->
        raise ArgumentError,
              "expected handle_sync/3 to return {:ok, state}, got: #{inspect(other)}"
    end
  end

  # If bootstrap fails after the initial lock claim, storage still contains the
  # pre-init record we wrote during lock acquisition. Repair that record only if
  # it is still owned by this exact boot attempt.
  defp repair_failed_boot_storage(%DurableServer{} = state) do
    repaired_state = %{
      state
      | status: :stopped_graceful,
        last_heartbeat_at: System.system_time(:millisecond)
    }

    {%DurableServer{} = repaired_state, dumped_user_state} = dump_user_state(repaired_state)
    repaired_meta = dump_meta(repaired_state)
    key_ctx = %{key: state.key, prefix: state.prefix, supervisor: state.supervisor}

    case StorageBackend.update_object(
           state.object_store,
           storage_key(state),
           fn
             %{body: %StoredState{meta: %Meta{} = meta} = stored_state} ->
               if same_boot_owner?(state, meta) do
                 updated_data =
                   stored_state
                   |> attach_stored_state_context(key_ctx)
                   |> Map.put(:state, dumped_user_state)
                   |> Map.put(:meta, repaired_meta)

                 {:ok, updated_data}
               else
                 {:error, :ownership_mismatch}
               end

             %{body: other} ->
               {:error, {:unexpected_value_type, other}}
           end,
           max_retries: 0
         ) do
      {:ok, _object} ->
        :ok

      {:error, :ownership_mismatch} ->
        :noop

      {:error, :not_found} ->
        :noop

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp same_boot_owner?(%DurableServer{} = state, %Meta{} = meta) do
    meta.pid == state.pid and meta.node_ref == state.node_ref and meta.node_str == state.node_str
  end

  defp schedule_sync(%__MODULE__{} = state, sync_every_ms \\ nil) do
    if state.sync_timer_ref do
      Process.cancel_timer(state.sync_timer_ref)

      receive do
        :sync -> :ok
      after
        0 -> :ok
      end
    end

    sync_ms = sync_every_ms || state.sync_every_ms

    if sync_ms do
      timer_ref = Process.send_after(self(), {@durable, :sync}, sync_ms)
      %{state | sync_timer_ref: timer_ref}
    else
      %{state | sync_timer_ref: nil}
    end
  end

  @doc """
  Fetches the DurableServer's current state from storage.
  """
  def fetch_stored_state(source, request, opts \\ [])

  def fetch_stored_state(supervisor_name, %{key: key, prefix: prefix}, opts)
      when is_atom(supervisor_name) do
    %{storage_backend: storage_backend} = DurableServer.Supervisor.__get_config__(supervisor_name)

    fetch_stored_state(
      storage_backend,
      %{key: key, prefix: prefix, supervisor: supervisor_name},
      opts
    )
  end

  def fetch_stored_state(%ObjectStore{} = store, %{key: _key, prefix: _prefix} = request, opts) do
    backend = StorageBackend.new(DurableServer.Backends.ObjectStore, store)
    fetch_stored_state(backend, request, opts)
  end

  def fetch_stored_state(%StorageBackend{} = store, %{key: key, prefix: prefix} = request, opts) do
    opts = Keyword.validate!(opts, [:consistent])

    case StorageBackend.get_object(store, prefix <> key, opts) do
      {:ok, %{body: %StoredState{} = stored_state, etag: etag}} ->
        {:ok,
         attach_stored_state_context(
           %{stored_state | etag: etag},
           stored_state_context(stored_state, request)
         )}

      {:ok, %{body: other}} ->
        {:error, {:unexpected_value_type, other}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc false
  # we allow users to piggy back on group registry so passhtru aribtrary meta
  def extract_user_meta(%DurableServer.GroupMeta{user_meta: user_meta}), do: user_meta
  def extract_user_meta(meta), do: meta

  @doc false
  def __fetch_stored_state_for_conflict_resolution__(supervisor_name, storage_key)
      when is_atom(supervisor_name) and is_binary(storage_key) do
    # Fetch the current etag from storage for conflict resolution
    # This is called during group conflict resolution
    try do
      %{storage_backend: store} = DurableServer.Supervisor.__get_config__(supervisor_name)

      case StorageBackend.get_object(store, storage_key, consistent: true) do
        {:ok, %{etag: etag}} -> {:ok, etag}
        {:error, _} = error -> error
      end
    catch
      kind, reason ->
        {:error, {kind, reason}}
    end
  end

  @doc false
  def check_lock(%Meta{} = meta, supervisor_name) when is_atom(supervisor_name) do
    check_lock(
      meta_with_runtime_context(meta, %{
        key: meta.key,
        prefix: meta.prefix,
        supervisor: supervisor_name
      })
    )
  end

  @doc false
  def check_lock(%Meta{supervisor: sup_name, pid: pid} = meta) do
    case check_lock_status(meta) do
      {:error, _reason} ->
        report_lock_check_result(sup_name, {:locked, pid})
        {:locked, pid}

      other ->
        other
    end
  end

  @doc false
  def check_lock_status(
        %Meta{supervisor: sup_name, node_ref: stored_node_ref, node_str: node_str, pid: pid} =
          meta
      ) do
    report_lock_diagnostic(sup_name, :check_lock_calls)

    cond do
      # delete tombstones are not live process locks and may not have owner fields
      Meta.deleting?(meta) ->
        report_lock_diagnostic(sup_name, :check_lock_deleting)
        delete_tombstone_lock_status(meta)

      Meta.cordoned?(meta) ->
        report_lock_diagnostic(sup_name, :check_lock_cordoned)
        {:locked, :cordoned}

      # if the pid lock holder wrote the graceful stop, it's gone
      Meta.stopped_graceful?(meta) ->
        report_lock_diagnostic(sup_name, :check_lock_stopped_graceful)
        :expired

      # local node - call directly and compare node_refs
      node_str == to_string(node()) ->
        result = __check_lock__(pid, stored_node_ref, sup_name)
        report_lock_check_result(sup_name, result)
        result

      # remote node, try rpc if we see the node online
      node_str in Enum.map(Node.list(), &to_string/1) ->
        report_lock_diagnostic(sup_name, :check_lock_rpc_attempt)
        # remote node - use erpc
        rpc_result =
          erpc_call(
            String.to_existing_atom(node_str),
            __MODULE__,
            :__check_lock__,
            [
              pid,
              stored_node_ref,
              sup_name
            ]
          )

        case rpc_result do
          {:locked, lock_pid} ->
            report_lock_check_result(sup_name, {:locked, lock_pid})
            {:locked, lock_pid}

          :expired ->
            report_lock_check_result(sup_name, :expired)
            :expired

          # node/network failures - fallback to node health check to check expired status
          {:error, {:erpc, :noconnection}} ->
            report_lock_rpc_failure(sup_name, node_str, :noconnection)
            check_lock_via_node_health(meta)

          {:error, {:erpc, :timeout}} ->
            report_lock_rpc_failure(sup_name, node_str, :timeout)
            check_lock_via_node_health(meta)

          {:error, {:erpc, :notsup}} ->
            report_lock_rpc_failure(sup_name, node_str, :notsup)
            check_lock_via_node_health(meta)
        end

      # node isn't known to us, check heartbeat cache to see if node is healthy
      true ->
        report_lock_diagnostic(sup_name, :check_lock_node_not_connected)
        check_lock_via_node_health(meta)
    end
  end

  defp check_lock_via_node_health(%Meta{} = meta) do
    %Meta{supervisor: sup_name, node_ref: stored_node_ref} = meta

    case LifecycleManager.lookup_node_health(meta) do
      # node is alive but not connected, treat as healthy until it goes stale
      # this could be temporary net split where both sides can reach object storage
      # but not eachother
      {:healthy, %{node_ref: ^stored_node_ref}} ->
        report_lock_check_result(sup_name, {:locked, meta.pid})
        {:locked, meta.pid}

      # if node is healhty but has a newer node_ref, the node has been bounced for this
      # object and the pid is necessarily done, so we treat as expired
      {:healthy, %{node_ref: new_node_ref}} when new_node_ref > stored_node_ref ->
        report_lock_check_result(sup_name, :expired)
        :expired

      # if node is healhty but has an older node_ref, a new node has come online
      # and placed a lock on this key and our node cache is not yet up to date
      {:healthy, %{node_ref: new_node_ref}} when new_node_ref < stored_node_ref ->
        report_lock_check_result(sup_name, {:locked, meta.pid})
        {:locked, meta.pid}

      # A stale local cache view is not strong enough evidence to steal a lock.
      # Confirm against storage heartbeat before expiring the lock.
      :stale ->
        report_lock_diagnostic(sup_name, :check_lock_heartbeat_stale)
        check_lock_via_storage_heartbeat(meta)

      # no heartbeat data in local cache - fetch directly from storage as fallback
      # this prevents incorrectly treating a node as expired just because we haven't
      # refreshed our cache since that node joined the cluster
      :unknown ->
        report_lock_diagnostic(sup_name, :check_lock_heartbeat_unknown)
        check_lock_via_storage_heartbeat(meta)
    end
  end

  defp delete_tombstone_lock_status(%Meta{} = meta) do
    case delete_tombstone_age_and_timing(meta) do
      {:ok, age_ms, staleness_ms, _margin_ms} when age_ms > staleness_ms -> :expired
      {:ok, _age_ms, _staleness_ms, _margin_ms} -> {:locked, :deleting}
      :unknown -> {:locked, :deleting}
    end
  end

  defp delete_tombstone_active?(%Meta{} = meta) do
    case delete_tombstone_lock_status(meta) do
      {:locked, :deleting} -> true
      :expired -> false
    end
  end

  defp delete_tombstone_delete_window_open?(%Meta{} = meta) do
    case delete_tombstone_age_and_timing(meta) do
      {:ok, age_ms, staleness_ms, margin_ms} ->
        age_ms <= staleness_ms - margin_ms

      :unknown ->
        false
    end
  end

  defp delete_tombstone_age_and_timing(%Meta{last_heartbeat_at: timestamp} = meta)
       when is_integer(timestamp) do
    case delete_tombstone_timing_ms(meta) do
      {:ok, staleness_ms, margin_ms} ->
        {:ok, System.system_time(:millisecond) - timestamp, staleness_ms, margin_ms}

      :unknown ->
        :unknown
    end
  end

  defp delete_tombstone_age_and_timing(%Meta{}), do: :unknown

  defp delete_tombstone_timing_ms(%Meta{supervisor: supervisor_name})
       when is_atom(supervisor_name) do
    try do
      case DurableServer.Supervisor.__get_config__(supervisor_name) do
        %{
          heartbeat_staleness_threshold_ms: heartbeat_ms,
          delete_tombstone_delete_request_margin_ms: margin_ms
        }
        when is_integer(heartbeat_ms) and heartbeat_ms > 0 and is_integer(margin_ms) and
               margin_ms > 0 ->
          {:ok, heartbeat_ms + margin_ms, margin_ms}

        _ ->
          :unknown
      end
    rescue
      _ -> :unknown
    catch
      _, _ -> :unknown
    end
  end

  defp delete_tombstone_timing_ms(%Meta{}), do: :unknown

  # Fallback when local heartbeat cache returns :unknown - fetch heartbeat directly from storage
  defp check_lock_via_storage_heartbeat(
         %Meta{supervisor: supervisor_name, node_str: node_str, node_ref: stored_node_ref} = meta
       ) do
    report_lock_diagnostic(supervisor_name, :check_lock_storage_heartbeat_fetch)

    case LifecycleManager.fetch_node_heartbeat_from_storage(
           supervisor_name,
           node_str,
           consistent: true
         ) do
      {:healthy, %{node_ref: ^stored_node_ref}} ->
        report_lock_check_result(supervisor_name, {:locked, meta.pid})
        {:locked, meta.pid}

      {:healthy, %{node_ref: new_node_ref}} when new_node_ref > stored_node_ref ->
        report_lock_check_result(supervisor_name, :expired)
        :expired

      {:healthy, %{node_ref: new_node_ref}} when new_node_ref < stored_node_ref ->
        report_lock_check_result(supervisor_name, {:locked, meta.pid})
        {:locked, meta.pid}

      :stale ->
        report_lock_check_result(supervisor_name, :expired)
        :expired

      # No heartbeat in storage - node may have crashed before writing any heartbeat,
      # or heartbeat was cleaned up. Treat as expired.
      :not_found ->
        report_lock_check_result(supervisor_name, :expired)
        :expired

      # Storage fetch failed - be conservative and assume lock is held to avoid
      # incorrectly stealing a lock due to transient storage errors
      {:error, reason} ->
        report_lock_diagnostic(supervisor_name, :check_lock_storage_heartbeat_error)
        {:error, reason}
    end
  end

  defp report_lock_diagnostic(supervisor_name, key) do
    LifecycleManager.report_diagnostic(supervisor_name, key)
  rescue
    _ -> :ok
  end

  defp report_lock_check_result(supervisor_name, {:locked, _pid}) do
    report_lock_diagnostic(supervisor_name, :check_lock_locked)
  end

  defp report_lock_check_result(supervisor_name, :expired) do
    report_lock_diagnostic(supervisor_name, :check_lock_expired)
  end

  defp report_lock_rpc_failure(supervisor_name, node_str, reason)
       when is_binary(node_str) and reason in [:timeout, :noconnection, :notsup] do
    event_key =
      case reason do
        :timeout -> :check_lock_rpc_timeout
        :noconnection -> :check_lock_rpc_noconnection
        :notsup -> :check_lock_rpc_notsup
      end

    report_lock_diagnostic(supervisor_name, event_key)
  end

  # called via erpc on remote rpc call
  def __check_lock__(pid, stored_node_ref, supervisor_name)
      when is_pid(pid) and is_atom(supervisor_name) do
    cond do
      node(pid) != Node.self() ->
        raise ArgumentError, "invalid __check_lock__ for pid this node does not own"

      Process.alive?(pid) ->
        # check if the current node_ref for this supervisor matches the stored one
        # this protects against pid reuse if this VM was bounced
        current_node_ref = DurableServer.Supervisor.node_ref(supervisor_name)

        if current_node_ref == stored_node_ref do
          {:locked, pid}
        else
          :expired
        end

      true ->
        :expired
    end
  end

  defp await_raced_registration_error(
         %DurableServer{} = state,
         %StoredState{} = data,
         retries \\ 0
       ) do
    # wait up to 5s
    if retries > 50 do
      # we should have seen the registration come up by now, check object storage for current value
      # (possible netsplit) before falling back to :noproc
      case fetch_existing_state_raw(
             state.object_store,
             %{
               key: state.key,
               prefix: state.prefix,
               supervisor: state.supervisor
             },
             %{},
             consistent: true
           ) do
        {:ok, %StoredState{meta: %Meta{} = meta} = stored_state} ->
          # increment global lock failure - we found a lock in storage but never saw it in syn
          # this indicates network partition/flapping
          maybe_increment_global_lock_failures(state)

          cond do
            Meta.deleting?(meta) ->
              case delete_tombstone_lock_status(meta) do
                :expired ->
                  try_lock_object_via_update(%{state | etag: stored_state.etag}, data)

                {:locked, :deleting} ->
                  {:error, {:already_started, :deleting}}
              end

            is_pid(meta.pid) ->
              {:error, {:already_started, meta.pid}}

            true ->
              {:error, {:already_started, :noproc}}
          end

        :error ->
          {:error, {:already_started, :noproc}}
      end
    else
      case DurableServer.Supervisor.lookup(state.supervisor, state.key) do
        {pid, _meta} ->
          {:error, {:already_started, pid}}

        nil ->
          Process.sleep(100)
          await_raced_registration_error(state, data, retries + 1)
      end
    end
  end

  defp do_lock_object(
         %DurableServer{object_store: store} = state,
         data,
         supervisor_name
       )
       when is_atom(supervisor_name) do
    # if we have an etag, the object exists so jump straight to update based lock claim
    # if etag is nil, obj didn't exist at fetch time, so try to be first to claim it
    if state.etag do
      try_lock_object_via_update(state, data)
    else
      case StorageBackend.try_claim(store, storage_key(state), data) do
        # we won the first ever insert for this key
        {:ok, {:claimed, new_etag}} ->
          Logger.info(
            "we won the first ever claim for #{inspect(state.key)} (#{inspect(old_etag: state.etag, new_etag: new_etag)})"
          )

          {:ok, %{state | etag: new_etag}}

        # someone beat us just now, in between read and try_claim, we'll await their registration
        {:error, :already_claimed} ->
          Logger.info(
            "we raced the first ever claim for #{inspect(state.key)} awaiting registration (#{inspect(old_etag: state.etag)})"
          )

          await_raced_registration_error(state, data)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp try_lock_object_via_update(
         %DurableServer{key: key, object_store: store} = state,
         data
       ) do
    case StorageBackend.put_object(store, storage_key(state), data, etag: state.etag) do
      # obj still matched our etag, we got the claim
      {:ok, %{etag: new_etag}} ->
        Logger.info(
          "won the lock for #{inspect(key)} (#{inspect(old_etag: state.etag, new_etag: new_etag)})"
        )

        {:ok, %{state | etag: new_etag}}

      # someone raced us between our read and write attempt
      # we don't need to check the lock health because we know the claim just happened
      {:error, :conflict} ->
        Logger.info(
          "raced the lock for #{inspect(key)} awaiting registration (#{inspect(old_etag: state.etag)})"
        )

        await_raced_registration_error(state, data)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp erpc_call(node, mod, func, args, timeout \\ 5_000)
       when is_atom(mod) and is_atom(func) and is_list(args) and is_integer(timeout) do
    try do
      DurableServer.Supervisor.safe_erpc_call(node, mod, func, args, timeout)
    catch
      :throw, value -> {:error, {:throw, value}}
      :exit, reason -> {:error, {:exit, reason}}
      :error, {:erpc, reason} -> {:error, {:erpc, reason}}
      :error, {exception, reason, stack} -> {:error, {exception, reason, stack}}
    end
  end

  defp dump_user_state(%DurableServer{} = state) do
    dumped_state =
      state.user_state
      |> state.module.dump_state()
      |> validate_dumped_state!(state.module)

    hash = :crypto.hash(:sha256, :erlang.term_to_binary(dumped_state))
    {%{state | last_synced_user_state_hash: hash}, dumped_state}
  end

  defp load_user_state(module, old_vsn, persisted_state) do
    module.load_state(old_vsn, persisted_state)
  end

  defp put_object(%DurableServer{} = state, storage_key, data) do
    opts = [max_retries: @max_sync_retries]
    put_opts = if state.etag, do: Keyword.put(opts, :etag, state.etag), else: opts

    case StorageBackend.put_object(state.object_store, storage_key, data, put_opts) do
      {:ok, %{etag: new_etag}} -> {:ok, %{state | etag: new_etag}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp storage_key(%DurableServer{} = state), do: state.prefix <> state.key

  defp update_metadata(%DurableServer{} = state, %Meta{} = updated_meta) do
    storage_key = storage_key(state)
    store = state.object_store

    case StorageBackend.get_object(store, storage_key, consistent: true) do
      {:ok, %{body: %StoredState{meta: %Meta{} = current_meta} = stored_state, etag: etag}} ->
        if same_boot_owner?(state, current_meta) do
          updated_data =
            stored_state
            |> attach_stored_state_context(%{
              key: state.key,
              prefix: state.prefix,
              supervisor: state.supervisor
            })
            |> Map.put(:meta, updated_meta)

          case put_object(%{state | etag: etag}, storage_key, updated_data) do
            {:ok, %DurableServer{} = new_state} -> {:ok, new_state}
            {:error, reason} -> {:error, reason}
          end
        else
          {:error, :ownership_mismatch}
        end

      {:ok, %{body: other}} ->
        {:error, {:unexpected_value_type, other}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp attach_stored_state_context(%StoredState{meta: %Meta{} = meta} = stored_state, %{
         key: key,
         prefix: prefix,
         supervisor: supervisor
       }) do
    %StoredState{
      stored_state
      | key: key,
        prefix: prefix,
        meta: meta_with_runtime_context(meta, %{key: key, prefix: prefix, supervisor: supervisor})
    }
  end

  defp stored_state_context(%StoredState{meta: %Meta{}}, %{
         key: key,
         prefix: prefix,
         supervisor: supervisor
       }) do
    %{key: key, prefix: prefix, supervisor: supervisor}
  end

  defp stored_state_context(%StoredState{meta: %Meta{} = meta}, %{key: key, prefix: prefix}) do
    %{key: key, prefix: prefix, supervisor: meta.supervisor}
  end

  defp meta_with_runtime_context(%Meta{} = meta, %{
         key: key,
         prefix: prefix,
         supervisor: supervisor
       }) do
    %{
      meta
      | key: key,
        prefix: prefix,
        supervisor: supervisor
    }
  end

  defp validate_dumped_state!(dumped_state, module) when is_atom(module) do
    cond do
      is_struct(dumped_state) ->
        raise ArgumentError, """
        DurableServer cannot persist a top-level struct from #{inspect(module)}.dump_state/1, got:

            #{inspect(dumped_state)}

        Return a plain map at the top level and move any struct encoding into your
        app-level dump_state/1 and load_state/2 callbacks if needed.
        """

      is_map(dumped_state) ->
        dumped_state

      true ->
        raise ArgumentError,
              "#{inspect(module)}.dump_state/1 must return a map, got: #{inspect(dumped_state)}"
    end
  end

  defp update_server_status_directly(%DurableServer{} = state, status) do
    case get_server_metadata(state.object_store, %{key: state.key, prefix: state.prefix}) do
      {:ok, %Meta{} = meta} ->
        updated_meta = Meta.put_status(meta, status)
        update_metadata(state, updated_meta)

      {:error, _} ->
        {:error, :not_found}
    end
  end

  defp update_server_status_and_crash_history(%DurableServer{} = state, status, crash_history) do
    case get_server_metadata(state.object_store, %{key: state.key, prefix: state.prefix}) do
      {:ok, %Meta{} = meta} ->
        updated_meta =
          meta
          |> Meta.put_status(status)
          |> Meta.put_crash_history(crash_history)

        update_metadata(state, updated_meta)

      {:error, _} ->
        {:error, :not_found}
    end
  end

  defp maybe_check_global_lock_circuit_breaker(_circuit_breaker, true), do: :ok

  defp maybe_check_global_lock_circuit_breaker(circuit_breaker, false) do
    CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker)
  end

  defp active_restart_claim(%Meta{} = meta, boot_info, current_node_str)
       when is_binary(current_node_str) do
    if Meta.currently_restarting?(meta) &&
         not authorized_restart_attempt_boot?(meta, boot_info, current_node_str) do
      {:claimed, meta.restart_attempt_node}
    else
      :ok
    end
  end

  defp authorized_restart_attempt_boot?(
         %Meta{restart_attempt_node: restart_attempt_node},
         boot_info,
         _current_node_str
       )
       when is_binary(restart_attempt_node) do
    boot_info_restart_attempt_node(boot_info) == restart_attempt_node
  end

  defp authorized_restart_attempt_boot?(%Meta{}, _boot_info, _current_node_str), do: false

  defp boot_info_restart_attempt_node(
         %{
           preloaded: %{body: %StoredState{}, etag: _etag} = preloaded,
           is_sticky_local: is_sticky_local,
           restart_attempt_node: restart_attempt_node
         } = boot_info
       )
       when is_boolean(is_sticky_local) and is_binary(restart_attempt_node) and
              map_size(boot_info) == 3 and map_size(preloaded) == 2,
       do: restart_attempt_node

  defp boot_info_restart_attempt_node(nil), do: nil
  defp boot_info_restart_attempt_node(%{} = boot_info) when map_size(boot_info) == 0, do: nil
  defp boot_info_restart_attempt_node(%{}), do: nil

  defp maybe_increment_global_lock_failures(%DurableServer{preloaded_boot: true}), do: :ok

  defp maybe_increment_global_lock_failures(%DurableServer{} = state) do
    CircuitBreaker.increment_global_lock_failures(state.circuit_breaker)
  end

  # we want to IMMEDIATELY exit, but use `{:shutdown, term}` to prevent our `DynamicSupervisor`
  # from restarting us (it is `restart: :transient`)
  def fatal_exit!(pid, reason) when is_pid(pid) do
    Process.exit(pid, fatal_exit_signal(reason))
  end

  def fatal_exit!(reason) do
    exit(fatal_exit_signal(reason))
  end

  defp fatal_exit_signal(reason) do
    {:shutdown, {@durable, {:fatal_exit, reason}}}
  end

  defp log_capacity_limit(:max_children_total, details, supervisor, module) do
    Logger.info("""
    DurableServer total child limit reached - Cannot start #{inspect(module)} on #{Node.self()}
    Supervisor: #{supervisor}
    Current children: #{details.current}
    Limit: #{details.limit}
    """)
  end

  defp log_capacity_limit(:max_children_module, details, supervisor, module) do
    Logger.info("""
    DurableServer module child limit reached - Cannot start #{inspect(module)} on #{Node.self()}
    Supervisor: #{supervisor}
    Module: #{inspect(details.module)}
    Current count: #{details.current}
    Limit: #{details.limit}
    """)
  end

  defp log_capacity_limit(:max_cpu, details, supervisor, module) do
    Logger.info("""
    DurableServer cpu limit reached - Cannot start #{inspect(module)} on #{Node.self()}
    Supervisor: #{supervisor}
    Current CPU: #{details.current}%
    Limit: #{details.limit}%
    """)
  end

  defp log_capacity_limit(:max_memory, details, supervisor, module) do
    Logger.info("""
    DurableServer memory limit reached - Cannot start #{inspect(module)} on #{Node.self()}
    Supervisor: #{supervisor}
    Current memory: #{details.current}%
    Limit: #{details.limit}%
    """)
  end

  defp log_capacity_limit(:node_shutting_down, _details, supervisor, module) do
    Logger.info("""
    DurableServer node shutting down - Cannot start #{inspect(module)} on #{Node.self()}
    Supervisor: #{supervisor}
    """)
  end

  defp log_capacity_limit(:max_disk, details, supervisor, module) do
    Logger.info("""
    DurableServer disk limit reached - Cannot start #{inspect(module)} on #{Node.self()}
    Supervisor: #{supervisor}
    Mount point: #{details.mount_point}
    Current disk: #{details.current}%
    Limit: #{details.limit}%
    """)
  end
end
