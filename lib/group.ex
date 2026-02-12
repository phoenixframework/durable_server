defmodule Group do
  @moduledoc """
  Distributed process groups, registry, lifecycle monitoring, and isolated subclusters.

  This module provides:
  - **Distributed registry**: Unique key => process mapping across all nodes
  - **Process groups**: Allow processes to join/leave keys (many processes per key)
  - **Isolated subclusters**: Partition groups and registries into named subclusters for isolated messaging
  - **Lifecycle monitoring**: Monitor lifecycle events for registry and group changes

  ## Consistency Model

  All operations are **eventually consistent**. The underlying syn library uses
  Erlang distribution to propagate state across nodes, which means:

  - Writes (register, join, etc.) return immediately after local update
  - Other nodes receive updates asynchronously via Erlang distribution
  - During network partitions, nodes may have divergent views
  - When partitions heal, conflicts are resolved (see `Group.SynEventHandler`)

  ## Clusters

  By default, all operations use the supervisor's default cluster (syn scope). You can
  optionally create isolated subclusters where only connected nodes receive events.

  ### Default vs Named Clusters

  - **Default cluster**: Uses scope `:"durable_<supervisor_name>"` - all nodes share this
  - **Named clusters**: Use scope `:"durable_<supervisor_name>__cluster__<cluster_name>"`

  Named clusters are useful when you want to partition your registry/pubsub layer while still
  maintaining global uniqueness for DurableServers (which always register in the default
  cluster).

  ### Important: DurableServer Registration

  DurableServers always register in the **default cluster** to ensure global uniqueness
  via the distributed locking mechanism. Named clusters are purely for isolating your own
  registries, process groups, and subscriptions to isolated subclusters. If a DurableServer
  wants to participate in an isolated cluster, it can call `connect/2` and `join/4`
  inside its `init` callback.

  ## Core Concepts

  ### Monitoring vs Memberships

  - **Monitoring** (`monitor/2`, `demonitor/2`): Receive events in your mailbox
    when DurableServers or other processes register/join matching keys anywhere in the
    cluster. Supports pattern matching on keys.

  - **Memberships** (`join/3`, `leave/2`): Make your process discoverable cluster-wide
    via `members/2`. Triggers `:joined`/`:left` events to monitors.

  These are independent - joining a key does NOT automatically monitor events,
  and monitoring does NOT make you discoverable via `members/2`.

  ### String Groups vs Atom Groups

  Process groups can use either **string** or **atom** names:

  - **String groups** (e.g., `"room/123"`): Trigger `:joined`/`:left` pub/sub events.
    Monitors can pattern-match on these using prefix patterns like `"room/"`.

  - **Atom groups** (e.g., `:my_module`): Do NOT trigger pub/sub events. Useful for
    internal bookkeeping where you do not need or want pubsub network overhead.

  ## Event Types

  Events are delivered as `%Group{}` structs to monitoring processes:

      %Group{
        type: event_type,
        supervisor: supervisor_name,
        cluster: cluster_name,  # nil for default cluster
        key: key,
        pid: pid,
        meta: meta,             # always user-provided meta (internal keys stripped)
        previous_meta: ...,     # nil for new, old meta for re-register/re-join
        reason: ...             # set on :unregistered/:left events
      }

  | Event Type      | Trigger                                        | Extra Fields        |
  |-----------------|------------------------------------------------|---------------------|
  | `:registered`   | Process registered via `register/3` (new or re-register) | `:previous_meta` (`nil` if new, old meta if update) |
  | `:unregistered` | Process unregistered or died                   | `:reason`           |
  | `:joined`       | Process joined group via `join/3` (new or re-join)       | `:previous_meta` (`nil` if new, old meta if update) |
  | `:left`         | Process left group or died                     | `:reason`           |

  DurableServers automatically register/unregister during their lifecycle, so these
  events can be used to track DurableServer start/stop.

  ## Pattern Types

  Monitors support three pattern types:

  - `"user/123"` - Exact match, only events for this specific key
  - `"user/"` - Prefix match, all keys starting with "user/"
  - `:all` - All events for this supervisor

  ## Self-Events

  A process that monitors a pattern and then joins a matching key will receive
  its own `:joined` event. Similarly for `:left` when leaving. Filter these in your
  handler if needed:

      def handle_info(%Group{type: :joined, pid: pid}, state) when pid == self() do
        # Ignore our own join event
        {:noreply, state}
      end

  ## Examples

  ### Basic Monitoring

      # Monitor all events for a specific key
      :ok = Group.monitor(MySup, "user/123")

      # Monitor all keys under a prefix
      :ok = Group.monitor(MySup, "chat/")

      # Monitor all events
      :ok = Group.monitor(MySup, :all)

      # Handle events in a GenServer
      def handle_info(%Group{type: :registered, key: key, pid: pid}, state) do
        IO.puts("DurableServer started: \#{key}")
        {:noreply, state}
      end

      def handle_info(%Group{type: :unregistered, key: key, reason: reason}, state) do
        IO.puts("DurableServer stopped: \#{key}, reason: \#{inspect(reason)}")
        {:noreply, state}
      end

  ### Joining as a Member

      # Join a key to be discoverable by other processes
      :ok = Group.join(MySup, "game/room/42", %{role: :spectator})

      # Query all members of a key (DurableServers + joined processes)
      members = Group.members(MySup, "game/room/42")
      # => [{#PID<0.150.0>, %{module: GameRoom, ...}}, {#PID<0.200.0>, %{role: :spectator}}]

      # Leave when done
      :ok = Group.leave(MySup, "game/room/42")

  ### Using Named Clusters

      # Connect this node to a named cluster
      :ok = Group.connect(MySup, :game_servers)

      # Join a group in the named cluster
      :ok = Group.join(MySup, "room/123", %{role: :member}, cluster: :game_servers)

      # Monitor events in the named cluster
      :ok = Group.monitor(MySup, :all, cluster: :game_servers)

      # Members and dispatch also support cluster option
      Group.members(MySup, "room/123", cluster: :game_servers)
      Group.dispatch(MySup, "room/123", {:msg, "hi"}, cluster: :game_servers)

  ## Architecture Notes

  - **Events are cluster-wide**: syn callbacks (`on_process_registered`, `on_process_joined`,
    etc.) fire on ALL nodes in the cluster. This means a monitor on Node A receives
    events when a DurableServer registers on Node B.

  - **Monitors** are stored per-node in an Elixir `Registry`, enabling pattern matching
    and automatic cleanup when monitoring processes die.

  - **Memberships** use syn process groups for cluster-wide distribution and automatic
    cleanup when member processes die.
  """

  defstruct [:type, :supervisor, :cluster, :key, :pid, :meta, :reason, :previous_meta]

  @registry Group.Registry

  # ===========================================================================
  # Cluster Management (Node <-> Cluster)
  # ===========================================================================

  @doc """
  Connect the local node to a named cluster.

  This adds the current node to the syn scope for the named cluster, allowing it
  to send and receive process group events within that cluster.

  ## Parameters

  - `supervisor_name` - The DurableServer.Supervisor name
  - `cluster_name` - The name of the cluster to connect to (atom)

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  def connect(supervisor_name, cluster_name)
      when is_atom(supervisor_name) and is_atom(cluster_name) do
    scope = cluster_scope(supervisor_name, cluster_name)

    case :syn.add_node_to_scopes([scope]) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Disconnect the local node from a named cluster.

  ## Parameters

  - `supervisor_name` - The DurableServer.Supervisor name
  - `cluster_name` - The name of the cluster to disconnect from (atom)

  ## Returns

  - `:ok` always
  """
  def disconnect(supervisor_name, cluster_name)
      when is_atom(supervisor_name) and is_atom(cluster_name) do
    scope = cluster_scope(supervisor_name, cluster_name)
    :syn.remove_node_from_scopes([scope])
  end

  @doc """
  Check if the local node is connected to a named cluster.

  ## Parameters

  - `supervisor_name` - The DurableServer.Supervisor name
  - `cluster_name` - The name of the cluster to check

  ## Returns

  - `true` if connected
  - `false` if not connected
  """
  def connected?(supervisor_name, cluster_name)
      when is_atom(supervisor_name) and is_atom(cluster_name) do
    scope = cluster_scope(supervisor_name, cluster_name)
    scope in :syn.node_scopes()
  end

  @doc """
  List all nodes in a cluster.

  ## Parameters

  - `supervisor_name` - The DurableServer.Supervisor name
  - `cluster_name` - The cluster name (optional, defaults to nil for default cluster)

  ## Returns

  - List of node atoms
  """
  def nodes(supervisor_name, cluster_name \\ nil)

  def nodes(supervisor_name, nil) when is_atom(supervisor_name) do
    scope = DurableServer.Supervisor.syn_scope(supervisor_name)

    try do
      :syn.subcluster_nodes(:pg, scope)
    rescue
      # If scope doesn't exist yet
      ArgumentError -> []
    catch
      :exit, _ -> []
    end
  end

  def nodes(supervisor_name, cluster_name)
      when is_atom(supervisor_name) and is_atom(cluster_name) do
    scope = cluster_scope(supervisor_name, cluster_name)

    try do
      :syn.subcluster_nodes(:pg, scope)
    rescue
      ArgumentError -> []
    catch
      :exit, _ -> []
    end
  end

  # ===========================================================================
  # Registry (Process Registration)
  # ===========================================================================

  @doc """
  Register the calling process in the cluster registry.

  This registers a process with a unique key in the cluster. Only one process
  can be registered with a given key at a time (cluster-wide uniqueness).

  Use `register/4` when you need exactly one process per key. Use `join/4`
  when multiple processes should be able to share the same key.

  ## Parameters

  - `supervisor_name` - The DurableServer.Supervisor name
  - `key` - The unique key to register under
  - `meta` - Metadata map to associate with the registration
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Register in a named cluster instead of the default cluster

  ## Returns

  - `:ok` on success
  - `{:error, :taken}` if another process is already registered with this key
  """
  def register(supervisor_name, key, meta, opts \\ [])

  def register(supervisor_name, key, meta, opts)
      when is_atom(supervisor_name) and is_binary(key) and is_map(meta) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    scope = resolve_scope(supervisor_name, cluster)

    case :syn.register(scope, key, self(), meta) do
      :ok -> :ok
      {:error, :taken} -> {:error, :taken}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Unregister a process from the cluster registry.

  This removes a process registration. Typically not needed as registrations
  are automatically cleaned up when the process dies.

  ## Parameters

  - `supervisor_name` - The DurableServer.Supervisor name
  - `key` - The key to unregister
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Unregister from a named cluster instead of the default cluster

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  def unregister(supervisor_name, key, opts \\ [])

  def unregister(supervisor_name, key, opts)
      when is_atom(supervisor_name) and is_binary(key) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    scope = resolve_scope(supervisor_name, cluster)

    case :syn.unregister(scope, key) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Look up a registered process by key.

  Returns the pid and metadata for the process registered at the given key,
  or `nil` if no process is registered.

  ## Parameters

  - `supervisor_name` - The DurableServer.Supervisor name
  - `key` - The key to look up
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Look up in a named cluster instead of the default cluster

  ## Returns

  - `{pid, meta}` if a process is registered at the key
  - `nil` if no process is registered
  """
  def lookup(supervisor_name, key, opts \\ [])

  def lookup(supervisor_name, key, opts)
      when is_atom(supervisor_name) and is_binary(key) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    scope = resolve_scope(supervisor_name, cluster)

    try do
      case :syn.lookup(scope, key) do
        {pid, meta} when is_pid(pid) -> {pid, meta}
        :undefined -> nil
      end
    catch
      :error, {:invalid_scope, _} -> nil
    end
  end

  # ===========================================================================
  # Lifecycle Monitoring
  # ===========================================================================

  @doc """
  Monitor lifecycle events matching the given pattern.

  The calling process will receive `%Group{}` structs for matching keys:

      %Group{type: :registered, supervisor: sup, key: "user/123", pid: pid, meta: meta, ...}

  ## Patterns

  - `"exact/key"` - exact key match
  - `"prefix/"` - all keys starting with "prefix/"
  - `:all` - all keys

  ## Options

  - `:cluster` - Monitor events from a named cluster (default: nil for default cluster)

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  def monitor(supervisor_name, pattern_string, opts \\ [])
      when is_atom(supervisor_name) and (is_binary(pattern_string) or pattern_string == :all) do
    cluster = Keyword.get(opts, :cluster)
    pattern = parse_pattern(pattern_string)
    key = {supervisor_name, cluster, pattern}

    case Registry.register(@registry, key, nil) do
      {:ok, _} -> :ok
      {:error, {:already_registered, _}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Stop monitoring lifecycle events for the given pattern.

  ## Options

  - `:cluster` - The cluster to demonitor from (default: nil for default cluster)

  ## Returns

  - `:ok` always (demonitoring a non-existent monitor is a no-op)
  """
  def demonitor(supervisor_name, pattern_string, opts \\ []) when is_atom(supervisor_name) do
    cluster = Keyword.get(opts, :cluster)
    pattern = parse_pattern(pattern_string)
    key = {supervisor_name, cluster, pattern}
    Registry.unregister(@registry, key)
    :ok
  end

  # ===========================================================================
  # Process Groups (Process <-> Group)
  # ===========================================================================

  @doc """
  Join a group as a member.

  The process will:
  - Be discoverable via `members/2`
  - Be automatically removed when it dies

  **String groups** (e.g., `"room/123"`) trigger `:joined`/`:left` events to monitors.
  **Atom groups** (e.g., `:my_module`) do not trigger events - use for internal tracking.

  Re-joining an already-joined group updates the metadata in place.

  Note: Joining does NOT automatically monitor events.
  Call `monitor/2` separately if you want to receive events.

  ## Parameters

  - `supervisor_name` - The DurableServer.Supervisor name
  - `group` - The group to join (string or atom)
  - `meta` - Metadata map (default: `%{}`)
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Join a named cluster instead of the default cluster

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  def join(supervisor_name, group, meta \\ %{}, opts \\ [])

  def join(supervisor_name, group, meta, opts)
      when is_atom(supervisor_name) and (is_binary(group) or is_atom(group)) and is_map(meta) and
             is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    scope = resolve_scope(supervisor_name, cluster)

    case :syn.join(scope, group, self(), meta) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Leave a group that was previously joined.

  ## Parameters

  - `supervisor_name` - The DurableServer.Supervisor name
  - `group` - The group to leave (string or atom)
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Leave from a named cluster instead of the default cluster

  ## Returns

  - `:ok` on success
  - `{:error, :not_in_group}` if not a member
  """
  def leave(supervisor_name, group, opts \\ [])

  def leave(supervisor_name, group, opts)
      when is_atom(supervisor_name) and (is_binary(group) or is_atom(group)) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    scope = resolve_scope(supervisor_name, cluster)
    pid = self()

    case :syn.leave(scope, group, pid) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # ===========================================================================
  # Queries
  # ===========================================================================

  @doc """
  List all members of a group.

  For **string groups**: Returns both registered processes (via `register/5`) and
  joined processes (via `join/5`) - this is typical for application keys
  like `"room/123"`.

  For **atom groups**: Returns only joined processes - atom groups are used for
  internal tracking (e.g., all processes of a module).

  ## Parameters

  - `supervisor_name` - The DurableServer.Supervisor name
  - `group` - The group to query (string or atom)
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Query a named cluster instead of the default cluster

  ## Returns

  - List of `{pid, meta}` tuples
  """
  def members(supervisor_name, group, opts \\ [])

  def members(supervisor_name, group, opts)
      when is_atom(supervisor_name) and is_binary(group) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    scope = resolve_scope(supervisor_name, cluster)

    # DurableServer from registry (one or none)
    registry_result =
      case :syn.lookup(scope, group) do
        {pid, meta} -> [{pid, extract_user_meta(meta)}]
        :undefined -> []
      end

    # Joined pids from process group (zero or more)
    group_members =
      try do
        :syn.members(scope, group)
      rescue
        # syn raises ArgumentError if the group doesn't exist
        ArgumentError -> []
      end

    registry_result ++ group_members
  end

  def members(supervisor_name, group, opts)
      when is_atom(supervisor_name) and is_atom(group) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    scope = resolve_scope(supervisor_name, cluster)

    # Atom groups are only process groups, no registry lookup
    try do
      :syn.members(scope, group)
    rescue
      ArgumentError -> []
    end
  end

  @doc """
  Count processes registered in the local node's registry.

  This counts only processes registered via `register/5` on the local node.

  ## Parameters

  - `supervisor_name` - The DurableServer.Supervisor name
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Count in a named cluster instead of the default cluster

  ## Returns

  - Integer count
  """
  def local_registry_count(supervisor_name, opts \\ [])

  def local_registry_count(supervisor_name, opts)
      when is_atom(supervisor_name) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    scope = resolve_scope(supervisor_name, cluster)
    :syn.local_registry_count(scope)
  end

  @doc """
  Count processes in a group on the local node.

  ## Parameters

  - `supervisor_name` - The DurableServer.Supervisor name
  - `group` - The group to count (string or atom)
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Count in a named cluster instead of the default cluster

  ## Returns

  - Integer count
  """
  def local_member_count(supervisor_name, group, opts \\ [])

  def local_member_count(supervisor_name, group, opts)
      when is_atom(supervisor_name) and (is_binary(group) or is_atom(group)) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    scope = resolve_scope(supervisor_name, cluster)

    try do
      :syn.local_member_count(scope, group)
    rescue
      ArgumentError -> 0
    end
  end

  # ===========================================================================
  # Dispatch
  # ===========================================================================

  @doc """
  Dispatch a message to all members of a key.

  Sends `message` to all processes that have joined the key via `join/3`, as well as
  any DurableServer registered at that key. This is useful for application-level
  messaging between a DurableServer and connected clients (e.g., Phoenix Channels).

  ## Dispatch vs Monitor

  There are two ways to receive messages in this module:

  - **`monitor/2`** - Receive *lifecycle events* (`:registered`, `:unregistered`, etc.)
    when DurableServers or processes join/leave keys matching a pattern. These are
    system-generated events.

  - **`dispatch/3`** - Receive *application messages* sent explicitly by your code.
    Only members of the exact key receive the message.

  Use `monitor` to react to lifecycle changes. Use `dispatch` to send your own
  messages to members.

  ## Filtering by Metadata

  `dispatch/3` sends to all members. If you need to filter by metadata (e.g., only
  send to members with `%{type: :channel}`), use `members/2` directly:

      for {pid, %{type: :channel}} <- Group.members(sup, key) do
        send(pid, message)
      end

  ## Options

  - `:cluster` - Dispatch to a named cluster instead of the default cluster

  ## Returns

  - `:ok` always
  """
  def dispatch(supervisor_name, key, message, opts \\ [])

  def dispatch(supervisor_name, key, message, opts)
      when is_atom(supervisor_name) and is_binary(key) and is_list(opts) do
    for {pid, _meta} <- members(supervisor_name, key, opts) do
      send(pid, message)
    end

    :ok
  end

  # ===========================================================================
  # Internal
  # ===========================================================================

  @doc false
  def __dispatch__(supervisor_name, event_type, key, pid, meta, extra \\ %{}) do
    cluster = Map.get(extra, :cluster)
    subscribers = get_subscribers_for_key(supervisor_name, cluster, key)

    event = %Group{
      type: event_type,
      supervisor: supervisor_name,
      cluster: cluster,
      key: key,
      pid: pid,
      meta: extract_user_meta(meta),
      previous_meta: Map.get(extra, :previous_meta),
      reason: Map.get(extra, :reason)
    }

    for subscriber_pid <- subscribers do
      send(subscriber_pid, event)
    end

    :ok
  end

  # Compute the syn scope for a named cluster.
  # Uses __cluster__ (double underscore) as delimiter to avoid ambiguity with
  # supervisor names that might contain underscores.
  defp cluster_scope(supervisor_name, cluster_name) do
    :"durable_#{supervisor_name}__cluster__#{cluster_name}"
  end

  # Resolve the syn scope: default cluster uses Supervisor.syn_scope, named clusters use cluster_scope
  defp resolve_scope(supervisor_name, nil) do
    DurableServer.Supervisor.syn_scope(supervisor_name)
  end

  defp resolve_scope(supervisor_name, cluster_name) do
    cluster_scope(supervisor_name, cluster_name)
  end

  # Parse a pattern into an internal pattern representation
  # - :all matches all keys
  # - "prefix/" matches all keys starting with "prefix/"
  # - "exact/key" matches only that exact key
  defp parse_pattern(:all), do: :all

  defp parse_pattern(pattern) when is_binary(pattern) do
    if String.ends_with?(pattern, "/") do
      {:prefix, pattern}
    else
      {:exact, pattern}
    end
  end

  # Check if a pattern matches a key
  defp matches_pattern?(:all, _key), do: true
  defp matches_pattern?({:exact, pattern}, key), do: pattern == key
  defp matches_pattern?({:prefix, prefix}, key), do: String.starts_with?(key, prefix)

  # Get all local subscriber pids whose patterns match the given key and cluster
  defp get_subscribers_for_key(supervisor_name, cluster, key) do
    # Get all registrations and filter by pattern match
    # Registry keys are {supervisor_name, cluster, pattern}
    @registry
    |> Registry.select([
      {
        {{:"$1", :"$2", :"$3"}, :"$4", :_},
        [{:andalso, {:==, :"$1", supervisor_name}, {:==, :"$2", cluster}}],
        [{{:"$3", :"$4"}}]
      }
    ])
    |> Enum.filter(fn {pattern, _pid} -> matches_pattern?(pattern, key) end)
    |> Enum.map(fn {_pattern, pid} -> pid end)
    |> Enum.uniq()
  rescue
    # Registry doesn't exist yet during startup
    # (syn callbacks can fire before DurableServer.Application starts the registry)
    ArgumentError -> []
  end

  # Extract user_meta from DurableServer meta, or return the meta as-is for joined pids
  defp extract_user_meta(%{user_meta: user_meta}), do: user_meta
  defp extract_user_meta(meta) when is_map(meta), do: meta
end
