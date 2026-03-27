defmodule Group do
  @moduledoc """
  Distributed process groups, registry, lifecycle monitoring, and isolated subclusters.

  This module provides:
  - **Distributed registry**: Unique key => process mapping across all nodes
  - **Process groups**: Allow processes to join/leave keys (many processes per key)
  - **Isolated subclusters**: Partition groups and registries into named subclusters for isolated messaging
  - **Lifecycle monitoring**: Monitor lifecycle events for registry and group changes

  ## Consistency Model

  All operations are **eventually consistent**. The built-in replication layer uses
  Erlang distribution to propagate state across nodes, which means:

  - Writes (register, join, etc.) return immediately after local update
  - Other nodes receive updates asynchronously via Erlang distribution
  - During network partitions, nodes may have divergent views
  - When partitions heal, conflicts are resolved. The losing process is killed
    with `{:group_registry_conflict, key, meta}`

  ## Clusters

  By default, all operations use the default cluster (`nil`). You can
  optionally create isolated subclusters where only connected nodes receive events.

  Named clusters use string names (e.g., `"game_servers"`).

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

  ## Event Types

  Events are delivered as `{:group, events, info}` tuples:

      {:group, [
        %Group.Event{
          type: event_type,
          supervisor: name,
          cluster: cluster_name,  # nil for default cluster
          key: key,
          pid: pid,
          meta: meta,             # always user-provided meta (internal keys stripped)
          previous_meta: ...,     # nil for new, old meta for re-register/re-join
          reason: ...             # set on :unregistered/:left events
        }
      ], %{name: name}}

  | Event           | Trigger                           | Extra Fields     |
  |-----------------|-----------------------------------|------------------|
  | `:registered`   | `register/3` (new or re-register) | `:previous_meta` |
  | `:unregistered` | Process unregistered or died      | `:reason`        |
  | `:joined`       | `join/3` (new or re-join)         | `:previous_meta` |
  | `:left`         | Process left group or died        | `:reason`        |

  `:previous_meta` is `nil` for new registrations/joins, or the old
  metadata map when re-registering/re-joining.

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

      def handle_info({:group, events, _info}, state) do
        Enum.each(events, fn
          %Group.Event{type: :joined, pid: pid} when pid != self() ->
            # Handle other processes' join events
            :ok
          _ -> :ok
        end)
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
      def handle_info({:group, events, _info}, state) do
        Enum.each(events, fn
          %Group.Event{type: :registered, key: key} ->
            IO.puts("DurableServer started: \#{key}")
          %Group.Event{type: :unregistered, key: key, reason: reason} ->
            IO.puts("DurableServer stopped: \#{key}, reason: \#{inspect(reason)}")
          _ -> :ok
        end)
        {:noreply, state}
      end

  ### Joining as a Member

      # Join a key to be discoverable by other processes
      :ok = Group.join(MySup, "game/room/42", %{role: :spectator})

      # Query all members of a key (joined processes only)
      members = Group.members(MySup, "game/room/42")
      # => [{#PID<0.200.0>, %{role: :spectator}}]

      # Leave when done
      :ok = Group.leave(MySup, "game/room/42")

  ### Using Named Clusters

      # Connect this node to a named cluster
      :ok = Group.connect(MySup, "game_servers")

      # Join a group in the named cluster
      :ok = Group.join(MySup, "room/123", %{role: :member}, cluster: "game_servers")

      # Monitor events in the named cluster
      :ok = Group.monitor(MySup, :all, cluster: "game_servers")

      # Members and dispatch also support cluster option
      Group.members(MySup, "room/123", cluster: "game_servers")
      Group.dispatch(MySup, "room/123", {:msg, "hi"}, cluster: "game_servers")

  ## Architecture Notes

  - **Events are cluster-wide**: Replication callbacks fire on ALL nodes in the cluster.
    This means a monitor on Node A receives events when a DurableServer registers on Node B.

  - **Monitors** are stored per-node in an Elixir `Registry`, enabling pattern matching
    and automatic cleanup when monitoring processes die.

  - **Memberships** use built-in process groups for cluster-wide distribution and automatic
    cleanup when member processes die.
  """

  alias Group.Replica
  alias Group.Replica.Data

  # ===========================================================================
  # Startup
  # ===========================================================================

  @doc """
  Returns a child spec for starting Group under a supervisor.

  ## Options

  - `:name` (required) — atom identifying this Group instance
  - `:shards` — number of GenServer shards (default: 8). Must match across all nodes
  - `:log` — logging level. `:info` (default) logs peer discovery, node events,
    and cluster membership changes. `:verbose` additionally logs per-shard
    replication messages. `false` disables all Group log output.
  - `:resolve_registry_conflict` — `{module, function, extra_args}` callback invoked when
    two nodes hold the same registry key (partition heal or concurrent registration).
    Called as `apply(module, function, [name, key, {pid1, meta1, time1}, {pid2, meta2, time2} | extra_args])`.
    Must return the winner pid. The loser is killed with `{:group_registry_conflict, key, meta}`.
    **Important:** This callback runs synchronously inside the shard GenServer — it must
    return quickly and never block. Any information needed for the decision should be
    carried in the registration metadata, not fetched at resolution time.
  - `:extract_meta` — `{module, function, args}` to transform metadata on reads
  """
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)
    %{id: {__MODULE__, name}, start: {Group.Supervisor, :start_link, [opts]}, type: :supervisor}
  end

  def start_link(opts), do: Group.Supervisor.start_link(opts)

  # ===========================================================================
  # Cluster Management (Node <-> Cluster)
  # ===========================================================================

  @doc """
  Connect the local node to a named cluster.

  This adds the current node to the cluster, allowing it to send and receive
  process group events within that cluster.

  ## Parameters

  - `name` - The Group name
  - `cluster_name` - The name of the cluster to connect to (binary string)

  ## Returns

  - `:ok` on success
  """
  def connect(name, cluster_or_clusters)
      when is_atom(name) do
    clusters = List.wrap(cluster_or_clusters)
    local = node()
    new_clusters = Enum.reject(clusters, fn c -> local in Data.cluster_nodes(name, c) end)

    if new_clusters != [] do
      for cluster <- new_clusters do
        Data.add_cluster_node(name, cluster, local)
      end

      notify_shard = :rand.uniform(get_config(name).num_shards) - 1

      GenServer.call(
        Replica.shard_name(name, notify_shard),
        {:cluster_connect, new_clusters},
        60_000
      )
    end

    :ok
  end

  @doc """
  Disconnect the local node from one or more named clusters.

  Accepts a single cluster name (binary) or a list of cluster names.
  """
  def disconnect(name, cluster_or_clusters)
      when is_atom(name) do
    clusters = List.wrap(cluster_or_clusters)

    for cluster <- clusters do
      Data.remove_cluster_node(name, cluster, node())
    end

    num_shards = get_config(name).num_shards

    for i <- 0..(num_shards - 1) do
      GenServer.call(Replica.shard_name(name, i), {:cluster_disconnect, clusters}, 60_000)
    end

    :ok
  end

  @doc """
  Check if the local node is connected to a named cluster.

  ## Parameters

  - `name` - The Group name
  - `cluster_name` - The name of the cluster to check (binary string)

  ## Returns

  - `true` if connected
  - `false` if not connected
  """
  def connected?(name, cluster_name)
      when is_atom(name) and is_binary(cluster_name) do
    node() in Data.cluster_nodes(name, cluster_name)
  end

  @doc """
  List all nodes running this Group instance.

  Returns nodes that have completed the peer discovery handshake, excluding
  the local node. Unlike `Node.list()`, this only includes nodes actually
  running this Group instance, not all connected Erlang nodes.

  ## Parameters

  - `name` - The Group name

  ## Returns

  - List of node atoms
  """
  def nodes(name) when is_atom(name) do
    try do
      Data.cluster_nodes(name, nil) -- [node()]
    rescue
      ArgumentError -> []
    end
  end

  @doc """
  List all nodes in a named cluster.

  ## Parameters

  - `name` - The Group name
  - `cluster_name` - The cluster name (binary string)

  ## Returns

  - List of node atoms
  """
  def nodes(name, cluster_name)
      when is_atom(name) and is_binary(cluster_name) do
    try do
      Data.cluster_nodes(name, cluster_name)
    rescue
      ArgumentError -> []
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

  - `name` - The Group name
  - `key` - The unique key to register under
  - `meta` - Metadata map to associate with the registration
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Register in a named cluster instead of the default cluster

  ## Returns

  - `:ok` on success
  - `{:error, :taken}` if another process is already registered with this key
  """
  def register(name, key, meta, opts \\ [])

  def register(name, key, meta, opts)
      when is_atom(name) and is_binary(key) and is_map(meta) and is_list(opts) do
    validate_key!(key)
    cluster = Keyword.get(opts, :cluster)
    validate_cluster_connected!(name, cluster)
    shard = Replica.shard_for(name, cluster, key)

    GenServer.call(shard, {:register, cluster, key, self(), meta})
  end

  @doc """
  Unregister a process from the cluster registry.

  This removes a process registration. Typically not needed as registrations
  are automatically cleaned up when the process dies.

  ## Parameters

  - `name` - The Group name
  - `key` - The key to unregister
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Unregister from a named cluster instead of the default cluster

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  def unregister(name, key, opts \\ [])

  def unregister(name, key, opts)
      when is_atom(name) and is_binary(key) and is_list(opts) do
    validate_key!(key)
    cluster = Keyword.get(opts, :cluster)
    validate_cluster_connected!(name, cluster)
    shard = Replica.shard_for(name, cluster, key)

    GenServer.call(shard, {:unregister, cluster, key})
  end

  @doc """
  Look up a registered process by key.

  Returns the pid and metadata for the process registered at the given key,
  or `nil` if no process is registered.

  ## Parameters

  - `name` - The Group name
  - `key` - The key to look up
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Look up in a named cluster instead of the default cluster
  - `:extract_meta` - Override the configured extract_meta callback for this call

  ## Returns

  - `{pid, meta}` if a process is registered at the key
  - `nil` if no process is registered
  """
  def lookup(name, key, opts \\ [])

  def lookup(name, key, opts)
      when is_atom(name) and is_binary(key) and is_list(opts) do
    case get_config(name) do
      nil ->
        nil

      config ->
        cluster = Keyword.get(opts, :cluster)
        extract_meta_fn = resolve_extract_meta(name, opts)
        num_shards = config.num_shards
        shard = Replica.shard_index_for(cluster, key, num_shards)

        case Data.registry_lookup(name, shard, cluster, key) do
          {pid, meta, _time, _node} ->
            {pid, extract_meta_fn.(meta)}

          nil ->
            nil
        end
    end
  rescue
    ArgumentError -> nil
  end

  # ===========================================================================
  # Lifecycle Monitoring
  # ===========================================================================

  @doc """
  Monitor lifecycle events matching the given pattern.

  The calling process will receive `{:group, events, info}` tuples containing matching events:

      {:group, [%Group.Event{type: :registered, ...}], %{name: name}}

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
  def monitor(name, pattern_string, opts \\ [])
      when is_atom(name) and (is_binary(pattern_string) or pattern_string == :all) do
    cluster = Keyword.get(opts, :cluster)
    pattern = parse_pattern(pattern_string)
    key = {name, cluster, pattern}

    case Registry.register(registry_name(name), key, nil) do
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
  def demonitor(name, pattern_string, opts \\ []) when is_atom(name) do
    cluster = Keyword.get(opts, :cluster)
    pattern = parse_pattern(pattern_string)
    key = {name, cluster, pattern}
    Registry.unregister(registry_name(name), key)
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

  Re-joining an already-joined group updates the metadata in place.

  Note: Joining does NOT automatically monitor events.
  Call `monitor/2` separately if you want to receive events.

  ## Parameters

  - `name` - The Group name
  - `group` - The group to join (string)
  - `meta` - Metadata map (default: `%{}`)
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Join a named cluster instead of the default cluster

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  def join(name, group, meta \\ %{}, opts \\ [])

  def join(name, group, meta, opts)
      when is_atom(name) and is_binary(group) and is_map(meta) and
             is_list(opts) do
    validate_key!(group)
    cluster = Keyword.get(opts, :cluster)
    validate_cluster_connected!(name, cluster)
    shard = Replica.shard_for(name, cluster, group)

    GenServer.call(shard, {:join, cluster, group, self(), meta})
  end

  @doc """
  Leave a group that was previously joined.

  ## Parameters

  - `name` - The Group name
  - `group` - The group to leave (string)
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Leave from a named cluster instead of the default cluster

  ## Returns

  - `:ok` on success
  - `{:error, :not_in_group}` if not a member
  """
  def leave(name, group, opts \\ [])

  def leave(name, group, opts)
      when is_atom(name) and is_binary(group) and is_list(opts) do
    validate_key!(group)
    cluster = Keyword.get(opts, :cluster)
    validate_cluster_connected!(name, cluster)
    shard = Replica.shard_for(name, cluster, group)

    GenServer.call(shard, {:leave, cluster, group, self()})
  end

  # ===========================================================================
  # Queries
  # ===========================================================================

  @doc """
  List all members of a group (process group entries only).

  Returns processes that have joined via `join/3`. Registry entries
  (via `register/3`) are not included — use `lookup/3` for those.

  Supports prefix matching: if `group` ends with `"/"`, returns all
  members whose group key starts with that prefix. Prefix queries scan
  all shards. Exact queries hit a single shard.

  ## Parameters

  - `name` - The Group name
  - `group` - The group to query (string). Append `"/"` for prefix matching.
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Query a named cluster instead of the default cluster
  - `:extract_meta` - Override the configured extract_meta callback for this call

  ## Returns

  - List of `{pid, meta}` tuples
  """
  def members(name, group, opts \\ [])

  def members(name, group, opts)
      when is_atom(name) and is_binary(group) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    extract_meta_fn = resolve_extract_meta(name, opts)
    num_shards = get_config(name).num_shards

    if String.ends_with?(group, "/") do
      members_by_prefix(name, num_shards, cluster, group, extract_meta_fn)
    else
      members_exact(name, num_shards, cluster, group, extract_meta_fn)
    end
  end

  defp members_exact(name, num_shards, cluster, group, extract_meta_fn) do
    shard = Replica.shard_index_for(cluster, group, num_shards)

    Data.pg_members(name, shard, cluster, group)
    |> Enum.map(fn {pid, meta} -> {pid, extract_meta_fn.(meta)} end)
  end

  defp members_by_prefix(name, num_shards, cluster, prefix, extract_meta_fn) do
    Enum.flat_map(0..(num_shards - 1), fn shard ->
      Data.pg_members_by_prefix(name, shard, cluster, prefix)
      |> Enum.map(fn {pid, meta} -> {pid, extract_meta_fn.(meta)} end)
    end)
  end

  @doc """
  Count processes registered in this node's local replicated ETS view.

  This returns the caller node's current view of the cluster registry.
  It is eventually consistent across nodes and may differ briefly during
  propagation, partitions, or healing.

  ## Parameters

  - `name` - The Group name
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Count in a named cluster instead of the default cluster

  ## Returns

  - Integer count
  """
  def registry_count(name, opts \\ [])

  def registry_count(name, opts)
      when is_atom(name) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    num_shards = get_config(name).num_shards
    Data.registry_count(name, num_shards, cluster)
  end

  @doc """
  Count processes registered in the local node's registry.

  This counts only processes registered via `register/5` on the local node.

  ## Parameters

  - `name` - The Group name
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Count in a named cluster instead of the default cluster

  ## Returns

  - Integer count
  """
  def local_registry_count(name, opts \\ [])

  def local_registry_count(name, opts)
      when is_atom(name) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    num_shards = get_config(name).num_shards
    Data.local_registry_count(name, num_shards, cluster)
  end

  @doc """
  Count group members in this node's local replicated ETS view.

  This returns the caller node's current view of process group membership.
  It is eventually consistent across nodes and may differ briefly during
  propagation, partitions, or healing.

  Supports prefix matching: if `group` ends with `"/"`, counts all
  members whose group key starts with that prefix.

  ## Parameters

  - `name` - The Group name
  - `group` - The group to count (string). Append `"/"` for prefix matching.
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Count in a named cluster instead of the default cluster

  ## Returns

  - Integer count
  """
  def member_count(name, group, opts \\ [])

  def member_count(name, group, opts)
      when is_atom(name) and is_binary(group) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    num_shards = get_config(name).num_shards

    if String.ends_with?(group, "/") do
      Data.pg_count_by_prefix(name, num_shards, cluster, group)
    else
      Data.pg_count(name, num_shards, cluster, group)
    end
  end

  @doc """
  Count processes in a group on the local node.

  Supports prefix matching: if `group` ends with `"/"`, counts all local
  members whose group key starts with that prefix.

  ## Parameters

  - `name` - The Group name
  - `group` - The group to count (string). Append `"/"` for prefix matching.
  - `opts` - Keyword list of options

  ## Options

  - `:cluster` - Count in a named cluster instead of the default cluster

  ## Returns

  - Integer count
  """
  def local_member_count(name, group, opts \\ [])

  def local_member_count(name, group, opts)
      when is_atom(name) and is_binary(group) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    num_shards = get_config(name).num_shards

    if String.ends_with?(group, "/") do
      Data.local_pg_count_by_prefix(name, num_shards, cluster, group)
    else
      Data.local_pg_count(name, num_shards, cluster, group)
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
  def dispatch(name, key, message, opts \\ [])

  def dispatch(name, key, message, opts)
      when is_atom(name) and is_binary(key) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    num_shards = get_config(name).num_shards
    shard = Replica.shard_index_for(cluster, key, num_shards)
    local = node()

    # Registry entry (one or none) — always direct send, even cross-node
    case Data.registry_lookup(name, shard, cluster, key) do
      {pid, _meta, _time, _node} -> send(pid, message)
      nil -> :ok
    end

    # PG members — group remote pids by node for batched fan-out
    case Data.pg_members_with_node(name, shard, cluster, key) do
      [] ->
        :ok

      members ->
        {local_pids, remote_by_node} =
          Enum.reduce(members, {[], %{}}, fn {pid, _meta, node}, {locals, remotes} ->
            if node == local do
              {[pid | locals], remotes}
            else
              {locals, Map.update(remotes, node, [pid], &[pid | &1])}
            end
          end)

        for pid <- local_pids, do: send(pid, message)

        # Hash caller pid to pick a shard — same caller always hits the same
        # shard, preserving per-sender message ordering across dispatches.
        dispatch_shard = :erlang.phash2(self(), num_shards)
        shard_name = Replica.shard_name(name, dispatch_shard)

        for {target_node, pids} <- remote_by_node do
          send({shard_name, target_node}, {:group_dispatch, pids, message})
        end
    end

    :ok
  end

  @doc """
  Like `dispatch/4`, but only sends to members on the local node.

  Skips all cross-node messaging. Useful when you know remote nodes will
  handle their own local dispatch (e.g., a broadcast originating on each node).

  ## Options

  - `:cluster` - Dispatch to a named cluster instead of the default cluster

  ## Returns

  - `:ok` always
  """
  def dispatch_local(name, key, message, opts \\ [])

  def dispatch_local(name, key, message, opts)
      when is_atom(name) and is_binary(key) and is_list(opts) do
    cluster = Keyword.get(opts, :cluster)
    num_shards = get_config(name).num_shards
    shard = Replica.shard_index_for(cluster, key, num_shards)
    local = node()

    # Registry entry — only if local
    case Data.registry_lookup(name, shard, cluster, key) do
      {pid, _meta, _time, ^local} -> send(pid, message)
      _ -> :ok
    end

    # PG members — local only, filtered in the match spec
    for pid <- Data.pg_members_local(name, shard, cluster, key) do
      send(pid, message)
    end

    :ok
  end

  # ===========================================================================
  # Public Helpers
  # ===========================================================================

  @doc """
  Set the log level at runtime. Accepts `:info`, `:verbose`, or `false`.

  Updates the persistent_term config so the change takes effect immediately
  on all shards without restart.
  """
  def log_level(name, level) when level in [:info, :verbose, false] do
    config = get_config(name)
    :persistent_term.put({__MODULE__, name}, %{config | log: level})
    :ok
  end

  @doc false
  def get_config(name) when is_atom(name) do
    :persistent_term.get({__MODULE__, name}, nil)
  end

  # ===========================================================================
  # Internal
  # ===========================================================================

  defp validate_key!(key) do
    if String.ends_with?(key, "/") do
      raise ArgumentError,
            "key #{inspect(key)} must not end with \"/\" — trailing slash is reserved for prefix queries"
    end
  end

  defp validate_cluster_connected!(_name, nil), do: :ok

  defp validate_cluster_connected!(name, cluster) do
    unless node() in Data.cluster_nodes(name, cluster) do
      raise ArgumentError,
            "not connected to cluster #{inspect(cluster)}. Call Group.connect(#{inspect(name)}, #{inspect(cluster)}) first"
    end
  end

  @doc false
  def registry_name(name) when is_atom(name), do: :"#{name}_group_registry"

  defp parse_pattern(:all), do: :all

  defp parse_pattern(pattern) when is_binary(pattern) do
    if String.ends_with?(pattern, "/") do
      {:prefix, pattern}
    else
      {:exact, pattern}
    end
  end

  # Resolve the extract_meta function: per-call override > persistent_term config > identity
  defp resolve_extract_meta(name, opts) do
    case Keyword.get(opts, :extract_meta) do
      nil ->
        case :persistent_term.get({__MODULE__, name}, nil) do
          %{extract_meta: {mod, func, args}} -> fn meta -> apply(mod, func, [meta | args]) end
          _ -> & &1
        end

      {mod, func, args} ->
        fn meta -> apply(mod, func, [meta | args]) end

      func when is_function(func, 1) ->
        func
    end
  end
end
