defmodule DurableServer.Supervisor do
  @moduledoc """
  Supervisor for DurableServer processes with lifecycle management and graceful shutdown.

  DurableServer.Supervisor provides a scoped environment for managing DurableServer
  processes similar to how Task.Supervisor manages Task processes. Each supervisor
  instance maintains its own lifecycle manager, heartbeat system, and object storage
  namespace, preventing conflicts between different applications or components.

  ## Usage

  Start a DurableServer.Supervisor in your application supervision tree:

      children = [
        {DurableServer.Supervisor, name: MyApp.DurableSup, prefix: "myapp/"}
      ]

  Then start DurableServer processes through the supervisor:

      DurableServer.Supervisor.start_child(
        MyApp.DurableSup,
        {MyServer, %{key: "user_123"}}
      )

  ## Architecture

  Each DurableServer.Supervisor creates the following supervision tree:

      MyApp.DurableSup
      ├── TaskSupervisor          # The task supervisor for for async internal operations
      ├── DynamicSupervisor       # The supervisor for all `DurableServer` processes on this node
      ├── LifecycleManager        # Monitors and restarts crashed servers
      └── Terminator              # Coordinates graceful shutdown

  ### Components

  **LifecycleManager**: Automatically detects and restarts crashed or orphaned
  DurableServer processes within this supervisor's scope. Uses object storage
  queries and node heartbeats to identify servers that need restart.

  **Terminator**: Handles graceful shutdown by instructing all DurableServer
  processes to sync their state before termination. Waits for confirmation
  (up to a timeout) before allowing the supervisor to shut down.

  ### Object Storage Scoping

  Each supervisor uses a unique prefix for object storage to prevent naming
  conflicts:

      prefix: "myapp/"
      # Results in keys like: myapp/user_123, myapp/session_456

  ### Node Heartbeats

  The LifecycleManager maintains node-level heartbeats in object storage at
  `{prefix}nodes/{node_name}` and caches them locally for efficient health
  checking during restart decisions.

  ## Configuration Options

  - `:name` - Required. Registered name for this supervisor instance
  - `:prefix` - Required. Object storage prefix for scoping (should end with "/")
  - `:max_children` - Maximum concurrent DurableServer processes (default: :infinity)
  - `:discovery_interval_ms` - How often to scan for orphaned servers (default: 60_000)
  - `:heartbeat_interval_ms` - How often to write node heartbeats (default: 10_000)
  - `:dead_node_threshold_ms` - How long before a node is considered permanently dead and cleaned up
    (default: 86_400_000 = 24 hours)
  - `:crash_threshold_count` - Number of crashes before marking object as permanently crashed
    (default: 5)
  - `:crash_threshold_window_ms` - Time window for crash threshold counting
    (default: 3_600_000 = 1 hour)
  - `:module_circuit_breaker_count` - Module-wide crash limit before circuit breaker opens
    (default: 50)
  - `:module_circuit_breaker_window_ms` - Time window for module circuit breaker
    (default: 300_000 = 5 minutes)
  - `:module_circuit_breaker_cooldown_ms` - Cooldown period when module circuit breaker opens
    (default: 600_000 = 10 minutes)
  - `:object_store` - The configured `DurableServer.ObjectStore`. Defaults to preconfigured store.
  - `:max_cpu` - Maximum CPU usage percentage (1-100) before rejecting new children on this node.
    When CPU usage reaches this threshold, new placements will be routed to other nodes.
  - `:max_memory` - Maximum memory usage percentage (1-100) before rejecting new children on this node.
    When memory usage reaches this threshold, new placements will be routed to other nodes.
  - `:max_disk` - Maximum disk usage as `{percent, mount_point}` tuple (e.g., `{90, "/data"}`).
    When disk usage on the specified mount point reaches the threshold, new placements will be
    routed to other nodes. Unlike CPU and memory limits, disk limits are bypassed for sticky
    restarts (children returning to their previous node) since part of the disk usage is the
    child's own data.
  - `:heartbeat_meta` - Optional node metadata as a map or zero-arity function returning a map.
    Metadata is included in heartbeats and can be queried via `get_cluster_nodes/1` for admin
    dashboards or other informational purposes. Keys are converted to strings during JSON
    serialization. Example: `heartbeat_meta: %{"region" => System.get_env("FLY_REGION")}`
    or `heartbeat_meta: fn -> %{"region" => System.get_env("FLY_REGION")} end`
  - `:sticky_placement_history_limit` - Maximum number of placement history entries to keep
    per server (default: 5). History tracks unique placement changes over time, useful for
    identifying displaced servers and re-homing decisions. Oldest entries are pruned first.

  ## Examples

      # Basic usage
      {DurableServer.Supervisor, name: MyApp.DurableSup, prefix: "myapp/"}

      # With custom intervals
      {DurableServer.Supervisor,
       name: MyApp.DurableSup,
       prefix: "myapp/",
       discovery_interval_ms: 30_000,
       heartbeat_interval_ms: 15_000}

      # With resource limits
      {DurableServer.Supervisor,
       name: MyApp.DurableSup,
       prefix: "myapp/",
       max_cpu: 80,
       max_memory: 85,
       max_disk: {90, "/data"}}

      # Start a server
      {:ok, {pid, _meta}} = DurableServer.Supervisor.start_child(
        MyApp.DurableSup,
        {MyUserServer, %{key: "user_123", name: "Alice"}}
      )

      # Terminate a specific server
      DurableServer.Supervisor.terminate_child(pid)
  """

  use Supervisor
  require Logger

  @durable :durable

  alias DurableServer
  alias DurableServer.{LifecycleManager, Terminator, CircuitBreaker, Meta}
  alias DurableServer.ObjectStore

  @max_start_child_tries 10
  @default_ready_timeout 5_000

  @doc """
  Checks if the DurableServer.Supervisor is ready to handle requests.

  Returns `true` if the supervisor process is registered, `false` otherwise.
  This is safe to call at any time, even if the supervisor hasn't started yet.
  """
  def ready?(supervisor_name) when is_atom(supervisor_name) do
    Process.whereis(supervisor_name) != nil
  end

  @doc """
  Blocks until the supervisor is ready or timeout expires.

  Returns `:ok` if ready, `{:error, :timeout}` if timeout expires.

  This is intended to be called via RPC on a node that may still be booting.
  The remote node will block until its supervisor is ready, preventing
  ETS errors from concurrent access during startup.

  ## Options

  - `:timeout` - Maximum time to wait in milliseconds (default: #{@default_ready_timeout})
  - `:poll_interval` - How often to check readiness in milliseconds (default: 100)
  """
  def wait_until_ready(supervisor_name, opts \\ []) when is_atom(supervisor_name) do
    timeout = Keyword.get(opts, :timeout, @default_ready_timeout)
    poll_interval = Keyword.get(opts, :poll_interval, 100)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_wait_until_ready(supervisor_name, deadline, poll_interval)
  end

  defp do_wait_until_ready(supervisor_name, deadline, poll_interval) do
    if ready?(supervisor_name) do
      :ok
    else
      remaining = deadline - System.monotonic_time(:millisecond)

      if remaining <= 0 do
        {:error, :timeout}
      else
        Process.sleep(min(poll_interval, remaining))
        do_wait_until_ready(supervisor_name, deadline, poll_interval)
      end
    end
  end

  @doc """
  Looks up a global durable server by key.

  *Note*: the provided key is *not* prefixed – the configured supervisor prefix
  will automatically be applied when looking up the key from underlying storage.

  ## Examples

      {DurableServer.Supervisor, name: MyDurableSup, prefix: "myapp/"}
      {:ok, {pid, _meta}} = DurableServer.Supervisor.start_child(
        MyDurableSup,
        {Counter, %{key: "counter123", value: 0}}
      )

      iex> {pid, _meta} = DurableServer.Supervisor.lookup(MyDurableUp, "counter123")
  """
  def lookup(sup_name, key) when is_atom(sup_name) and is_binary(key) do
    try do
      case :syn.lookup(syn_scope(sup_name), key) do
        {pid, meta} when is_pid(pid) ->
          # handle case where node-local DOWN from a caller races syn cleanup
          if node(pid) == Node.self() && !Process.alive?(pid) do
            nil
          else
            {pid, meta.user_meta}
          end

        :undefined ->
          nil
      end
    catch
      :error, {:invalid_scope, _} -> nil
    end
  end

  @doc """
  Gets all cluster nodes from the heartbeat cache with their heartbeat metadata.

  Returns a map of node names to node info maps containing heartbeat_meta.

  ## Examples

      iex> get_cluster_nodes(MyApp.DurableSupervisor)
      %{
        "node1@host" => %{heartbeat_meta: %{"region" => "ord"}},
        "node2@host" => %{heartbeat_meta: nil}
      }

  """
  def get_cluster_nodes(supervisor_name) when is_atom(supervisor_name) do
    LifecycleManager.get_cluster_nodes(supervisor_name)
  end

  @doc """
  Gets detailed information about a server from storage.

  Returns a rich map with server information regardless of whether the server
  is currently running. This is useful for admin dashboards, debugging, and
  re-homing decisions.

  ## Return Value

  Returns `{:ok, info_map}` on success or `{:error, :not_found}` if the server
  doesn't exist in storage.

  The info map contains:

    * `:key` - The server's unique key
    * `:module` - The DurableServer module
    * `:vsn` - The state version
    * `:status` - Server status (`:running`, `:stopped_graceful`, `:crashed`, etc.)
    * `:permanent` - Whether the server is marked as permanent
    * `:last_heartbeat_at` - Timestamp of last heartbeat (milliseconds)
    * `:node` - The node where the server last ran (from storage)
    * `:sticky_placement` - Current placement values (where it last ran)
    * `:sticky_placement_history` - History of placement changes (most recent first)
    * `:crash_history` - List of crash entries (most recent first), each with `:timestamp` and `:reason`
    * `:user_state` - The raw user state (JSON decoded from storage)
    * `:pid` - PID if currently running, `nil` otherwise
    * `:running` - Boolean indicating if server is currently running

  ## Placement History

  The `sticky_placement_history` tracks placement changes over time. Each entry
  contains an `:at` timestamp and `:placement` values. Only unique placements are
  recorded (no duplicates when placement doesn't change). The history is capped
  at a configurable limit (default 5), with oldest entries pruned first.

  The first entry is the most recent placement, and the last entry is the oldest
  known placement (which may be the original if history hasn't been pruned):

      info = DurableServer.Supervisor.get_server_info(MySup, "user_123")
      case info.sticky_placement_history do
        [current | _rest] ->
          # current.placement is where it's running now
          # current.at is when it moved there
        [] ->
          # No placement history (no sticky config or new server)
      end

  ## Examples

      iex> DurableServer.Supervisor.get_server_info(MyDurableSup, "user_123")
      {:ok, %{
        key: "user_123",
        module: MyServer,
        vsn: 1,
        status: :running,
        permanent: true,
        last_heartbeat_at: 1704067200000,
        node: "node1@host",
        sticky_placement: [%{env_var: "FLY_REGION", value: "sjc"}],
        sticky_placement_history: [
          %{at: 1704067200000, placement: [%{env_var: "FLY_REGION", value: "sjc"}]},
          %{at: 1704000000000, placement: [%{env_var: "FLY_REGION", value: "ord"}]}
        ],
        user_state: %{"count" => 42},
        pid: #PID<0.123.0>,
        running: true
      }}

      iex> DurableServer.Supervisor.get_server_info(MyDurableSup, "nonexistent")
      {:error, :not_found}

  """
  def get_server_info(sup_name, key) when is_atom(sup_name) and is_binary(key) do
    %{object_store: object_store, prefix: prefix} = __get_config__(sup_name)

    case DurableServer.fetch_stored_state(object_store, %{key: key, prefix: prefix}) do
      {:ok, %DurableServer.StoredState{meta: meta, state: user_state, vsn: vsn}} ->
        # Check if server is currently running
        {pid, running} =
          case lookup(sup_name, key) do
            {pid, _user_meta} -> {pid, true}
            nil -> {nil, false}
          end

        info = %{
          key: key,
          module: meta.module,
          vsn: vsn,
          status: meta.status,
          permanent: meta.permanent,
          last_heartbeat_at: meta.last_heartbeat_at,
          node: meta.node_str,
          sticky_placement: meta.sticky_placement,
          sticky_placement_history: meta.sticky_placement_history,
          crash_history: meta.crash_history,
          user_state: user_state,
          pid: pid,
          running: running
        }

        {:ok, info}

      {:error, _reason} ->
        {:error, :not_found}
    end
  end

  @doc """
  Streams server info for all servers in storage.

  Returns a Stream that yields info maps for each server found in storage.
  Failed fetches are filtered out. Excludes internal node metadata objects.

  This is useful for admin dashboards that need to iterate over all servers
  without loading everything into memory at once.

  ## Examples

      # Stream all servers
      DurableServer.Supervisor.stream_all_server_info(MySup)
      |> Enum.to_list()

      # Stream only permanently crashed servers
      DurableServer.Supervisor.stream_all_server_info(MySup)
      |> Stream.filter(fn info -> info.status == :permanently_crashed end)
      |> Enum.to_list()

  """
  def stream_all_server_info(sup_name) when is_atom(sup_name) do
    alias DurableServer.ObjectStore
    %{object_store: object_store, prefix: prefix} = __get_config__(sup_name)

    case ObjectStore.list_objects(object_store, prefix) do
      {:ok, %{keys: keys}} ->
        keys
        |> Stream.reject(fn %{key: key} -> String.contains?(key, "/__nodes/") end)
        |> Stream.map(fn %{key: storage_key} ->
          key = String.trim_leading(storage_key, prefix)
          get_server_info(sup_name, key)
        end)
        |> Stream.filter(fn
          {:ok, _info} -> true
          _ -> false
        end)
        |> Stream.map(fn {:ok, info} -> info end)

      {:error, _reason} ->
        Stream.map([], & &1)
    end
  end

  @doc """
  Gets the unique node reference for this supervisor instance.

  *Note*: other nodes will rpc us and call this function, which can race our
  table creation and config insert, so we handle those cases explicitly.

  The node_ref is used to detect when a node has been restarted to avoid
  PID reuse from making stale locks appear valid. Each supervisor maintains
  its own node_ref in ets storage that gets cleaned up when supervisor dies.
  """
  def node_ref(supervisor_name) when is_atom(supervisor_name) do
    table_name = ets_table_name(supervisor_name)

    case :ets.lookup(table_name, :node_ref) do
      [{:node_ref, node_ref}] ->
        node_ref

      # possibly still initializing
      [] ->
        Logger.warning(
          "no ets table entry for node_ref `#{inspect(table_name)}` found for #{inspect(supervisor_name)}"
        )

        nil
    end
  rescue
    _ ->
      # possibly still initializing
      Logger.warning(
        "no ets table `#{inspect(ets_table_name(supervisor_name))}` found for #{inspect(supervisor_name)}"
      )

      nil
  end

  @doc """
  Returns the current capacity map for this supervisor.

  Returns a map with `:total` (total children across all modules) and per-module capacity information,
  or `nil` if no limits are configured.

  ## Examples

      iex> current_capacity(MySupervisor)
      %{
        :total => %{current: 50, limit: 100},
        MyModule => %{current: 10, limit: 20}
      }

      iex> current_capacity(UnlimitedSupervisor)
      nil
  """
  def current_capacity(supervisor_name) when is_atom(supervisor_name) do
    %{syn_scope: scope, ets_table: table_name} = __get_config__(supervisor_name)

    limits =
      case :ets.lookup(table_name, :capacity_limits) do
        [{:capacity_limits, limits}] -> limits
        [] -> %{}
      end

    # return nil if no capacity limits configured
    case limits[:max_children] do
      empty when empty in [nil, %{}] ->
        nil

      %{} = max_children ->
        capacity_map = %{}

        # add total capacity if configured
        capacity_map =
          if total_limit = max_children[:total] do
            current = :syn.local_registry_count(scope)
            Map.put(capacity_map, :total, %{current: current, limit: total_limit})
          else
            capacity_map
          end

        # add per-module capacity for each configured module
        capacity_map =
          max_children
          |> Enum.reject(fn {k, _v} -> k == :total end)
          |> Enum.reduce(capacity_map, fn {module, limit}, acc ->
            current =
              try do
                :syn.local_member_count(scope, module)
              rescue
                _ -> 0
              end

            Map.put(acc, module, %{current: current, limit: limit})
          end)

        if map_size(capacity_map) > 0, do: capacity_map, else: nil
    end
  rescue
    _ -> nil
  end

  @doc """
  Starts a DurableServer.Supervisor with the given options.

  ## Options

  - `:name` - Required. The registered name for this supervisor
  - `:prefix` - Required. Object storage prefix (should end with "/")
  - `:max_children` - Maximum concurrent children (default: :infinity)
  - `:discovery_interval_ms` - Lifecycle discovery interval (default: 60_000)
  - `:heartbeat_interval_ms` - Node heartbeat interval (default: 10_000)
  - `:dead_node_threshold_ms` - Dead node cleanup threshold (default: 300_000)
  - `:crash_threshold_count` - Crashes before permanent crash (default: 5)
  - `:crash_threshold_window_ms` - Crash threshold window (default: 3_600_000)
  - `:module_circuit_breaker_count` - Module crash limit (default: 50)
  - `:module_circuit_breaker_window_ms` - Module circuit breaker window (default: 300_000)
  - `:module_circuit_breaker_cooldown_ms` - Module circuit breaker cooldown (default: 600_000)
  - `:object_store` - The configured `DurableServer.ObjectStore`. Defaults to preconfigured store.
  """
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    prefix = Keyword.fetch!(opts, :prefix)

    unless String.ends_with?(prefix, "/") do
      raise ArgumentError, "prefix must end with '/', got: #{inspect(prefix)}"
    end

    # claim the prefix to prevent conflicts between supervisors
    prefix_key = {__MODULE__, :prefix, prefix}

    case :persistent_term.get(prefix_key, nil) do
      nil ->
        :persistent_term.put(prefix_key, name)

      existing_name when existing_name != name ->
        raise ArgumentError,
              "prefix #{inspect(prefix)} is already claimed by supervisor #{inspect(existing_name)}"

      ^name ->
        raise ArgumentError,
              "the prefix #{inspect(prefix)} has already been claimed by another process"
    end

    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Starts a DurableServer child process under this supervisor.

  ## Examples

      # Start with init args
      {:ok, {pid, meta}} = DurableServer.Supervisor.start_child(
        MyApp.DurableSup,
        {MyServer, %{key: "server_1", initial_value: 42}}
      )

      # The server module must use DurableServer
      defmodule MyServer do
        use DurableServer, vsn: 1

        def init(%{key: key, initial_value: value}) do
          {:ok, %{key: key, value: value}, meta: %{my: "meta"}}
        end
      end
  """
  def start_child(supervisor, {module, init_arg} = child_spec, opts \\ []) do
    opts = Keyword.validate!(opts, [:max_placement_retries])
    max_placement_retries = Keyword.get(opts, :max_placement_retries, 3)

    # When max_placement_retries is 0, this is a remote placement call from another node.
    # Wait for the supervisor to be ready to handle the race where RPC arrives before
    # ETS tables are created during node startup/rolling deploys.
    if max_placement_retries == 0 do
      case wait_until_ready(supervisor) do
        :ok ->
          :ok

        {:error, :timeout} ->
          Logger.warning(
            "DurableServer.Supervisor #{inspect(supervisor)} not ready after #{@default_ready_timeout}ms on remote placement"
          )

          # Return error so caller can try another node
          throw({:error, :not_ready})
      end
    end

    case do_start_child(supervisor, {module, init_arg}, 0) do
      {:ok, result} ->
        {:ok, result}

      {:error, {:capacity_limit, reason}} when max_placement_retries > 0 ->
        Logger.info("""
        DurableServer local capacity exceeded for #{inspect(module)} on #{Node.self()}
        Reason: #{inspect(reason)}
        Attempting remote placement (max retries: #{max_placement_retries})
        """)

        try_remote_placement(supervisor, child_spec, max_placement_retries)

      error ->
        error
    end
  end

  defp do_start_child(supervisor, {module, _init_arg}, retries)
       when retries > @max_start_child_tries do
    raise RuntimeError,
          "#{inspect(supervisor)} failed to `DurableServer.Supervisor.start_child` for #{inspect(module)} after #{@max_start_child_tries} tries"
  end

  defp do_start_child(supervisor, {module, init_arg}, retries) do
    key =
      case init_arg do
        {:restart, %{key: key}} ->
          key

        %{key: key} ->
          key

        _ ->
          raise ArgumentError,
                "start_child expects a map with :key field, got: #{inspect(init_arg)}"
      end

    dynamic_sup = get_dynamic_supervisor(supervisor)
    config = __get_config__(supervisor)
    init_ref = make_ref()
    init_from = {init_ref, self()}

    child_spec =
      Supervisor.child_spec(
        {DurableServer,
         %{
           module: module,
           init_from: init_from,
           init_arg: init_arg,
           supervisor_name: supervisor,
           config: config
         }},
        id: key
      )

    case DynamicSupervisor.start_child(dynamic_sup, child_spec) do
      {:ok, pid} ->
        monitor_ref = Process.monitor(pid)

        receive do
          {^init_ref, meta} -> {:ok, {pid, meta}}
          {:DOWN, ^monitor_ref, :process, ^pid, reason} -> {:error, reason}
        end

      :ignore ->
        receive do
          {^init_ref, :ignore} ->
            :ignore

          {^init_ref, {:error, {:already_started, pid}}} ->
            # wait up to 100ms * max retries (2.5s) for metadata to be synced before giving up on retries
            if retries > 0, do: Process.sleep(250)
            # we raced a start, retry start child to grab raced pid's metadata
            # node-local syn meta will be immediately there, but remote node meta could still be in flight (or pid is already DOWN)
            # if we find there is nothing in the registry, we RPC out to get remote node's meta.
            # if we find nothing there, we retry the start + lookup combo which will either get
            # the already started pid and its now synced metadata, or we end up starting ourselves up to @max_start_child_tries tries
            # and with a max caller wait time of 2.5s
            case lookup(supervisor, key) do
              {pid, meta} ->
                {:error, {:already_started, {pid, meta}}}

              nil ->
                remote_node = node(pid)

                # If we raced syn registration, immediately try to rpc out to the owning node for its node-local
                # syn metadata - as long as the node appears healthy.
                #
                # If the node does not appear healthy, we retry the start
                case LifecycleManager.lookup_node_health(%{
                       supervisor: supervisor,
                       node_str: to_string(remote_node)
                     }) do
                  {:healthy, _node_ref} ->
                    case :erpc.call(node(pid), __MODULE__, :lookup, [supervisor, key]) do
                      {pid, meta} when is_pid(pid) ->
                        {:error, {:already_started, {pid, meta}}}

                      nil ->
                        Logger.info(
                          "node-local metadata missing from #{inspect(node(pid))} so assuming raced pid is gone. Retrying start for #{inspect(key)}"
                        )

                        do_start_child(supervisor, {module, init_arg}, retries + 1)

                      {:error, {:erpc, erpc_reason}} ->
                        Logger.info(
                          "still waiting on metadata replication from #{inspect(node(pid))} (#{inspect(erpc_reason)}). Retrying start for #{inspect(key)}"
                        )

                        do_start_child(supervisor, {module, init_arg}, retries + 1)
                    end

                  unhealthy when unhealthy in [:stale, :unknown] ->
                    Logger.info(
                      "node #{inspect(node(pid))} no longer healthy, so assuming raced pid is gone. Retrying start for #{inspect(key)}"
                    )

                    do_start_child(supervisor, {module, init_arg}, retries + 1)
                end
            end

          {^init_ref, {:error, reason}} ->
            {:error, reason}
        after
          1000 -> exit(:timeout)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp try_remote_placement(supervisor, {module, init_arg} = child_spec, max_retries) do
    # Extract key and sticky_placement from init_arg
    # If we have {:restart, %{body: body, ...}}, extract sticky_placement directly
    # to avoid duplicate object storage lookup
    {key, sticky_placement} =
      case init_arg do
        {:restart, %{key: key, body: body}} ->
          # Get augmented sticky placement (handles module config updates like :any)
          augmented = __get_augmented_sticky_placement__(supervisor, module, key, body)
          {key, augmented}

        %{key: key} ->
          {key, nil}

        _ ->
          {nil, nil}
      end

    eligible_nodes =
      LifecycleManager.find_eligible_nodes(supervisor, module,
        limit: max_retries,
        key: key,
        sticky_placement: sticky_placement
      )

    case eligible_nodes do
      [] ->
        Logger.warning("""
        DurableServer: No eligible nodes found for #{inspect(module)}
        All nodes may be at capacity or unreachable
        """)

        {:error, {:capacity_limit, :no_available_nodes}}

      nodes ->
        try_nodes(supervisor, child_spec, nodes)
    end
  end

  defp extract_sticky_placement_from_body(key, supervisor_name, body) do
    config = __get_config__(supervisor_name)

    case JSON.decode(body) do
      {:ok, %{"meta" => meta_str}} when is_binary(meta_str) ->
        meta = Meta.decode_from_binary(meta_str, %{key: key, prefix: config.prefix})
        meta.sticky_placement

      _ ->
        nil
    end
  end

  # Get the sticky placement for a key, augmented with any new module config levels
  # This is the single source of truth for getting sticky placement - it handles:
  # 1. Extracting from body if provided (avoids duplicate S3 lookup)
  # 2. Loading from storage if body not provided
  # 3. Augmenting with new module config levels (e.g. :any added after process started)
  @doc false
  def __get_augmented_sticky_placement__(supervisor, module, key, body \\ nil) do
    # Extract or load the persisted sticky placement
    persisted =
      if body do
        extract_sticky_placement_from_body(key, supervisor, body)
      else
        config = __get_config__(supervisor)
        storage_key = config.prefix <> key

        case ObjectStore.get_object(config.object_store, storage_key) do
          {:ok, %{body: body}} ->
            extract_sticky_placement_from_body(key, supervisor, body)

          {:error, _} ->
            nil
        end
      end

    # Get module config and augment
    module_config = __get_sticky_placement_for_module__(supervisor, module)
    augment_with_module_config(persisted, module_config)
  end

  # Augment persisted sticky_placement with any new levels added to module config
  # This allows processes started before :any was added to still match nodes at the :any level
  defp augment_with_module_config(nil, _module_config), do: nil
  defp augment_with_module_config(persisted, nil), do: persisted
  defp augment_with_module_config([], _module_config), do: []

  defp augment_with_module_config(persisted, module_config)
       when is_list(persisted) and is_list(module_config) do
    # Check if persisted already has :any
    has_any? =
      Enum.any?(persisted, fn
        %{env_var: :any} -> true
        _ -> false
      end)

    # Check if module config has :any
    module_has_any? = Keyword.has_key?(module_config, :any)

    # If module config has :any but persisted doesn't, append it
    # We only augment with :any since it's a wildcard that matches all nodes
    # For other env vars, we can't add them without knowing their value
    if module_has_any? and not has_any? do
      persisted ++ [%{env_var: :any, value: :any}]
    else
      persisted
    end
  end

  defp try_nodes(_supervisor, _child_spec, []) do
    {:error, {:capacity_limit, :all_placement_attempts_failed}}
  end

  defp try_nodes(supervisor, {module, _init_arg} = child_spec, [node | rest]) do
    Logger.info("Attempting to place #{inspect(module)} on remote node #{inspect(node)}")

    # NOTE: we MUST pass max_placement_retries: 0 to prevent recursive retry on the other side
    try do
      result =
        :erpc.call(node, __MODULE__, :start_child, [
          supervisor,
          child_spec,
          [max_placement_retries: 0]
        ])

      case result do
        {:ok, {pid, meta}} ->
          Logger.info("Successfully placed #{inspect(module)} on #{inspect(node)}")
          {:ok, {pid, meta}}

        {:error, {:capacity_limit, reason}} ->
          Logger.info(
            "Node #{inspect(node)} also at capacity: #{inspect(reason)}, trying next node"
          )

          try_nodes(supervisor, child_spec, rest)

        {:error, other} ->
          case other do
            {:already_started, _} ->
              Logger.warning(
                "Failed to start on #{inspect(node)}: #{inspect(other)}, trying next node"
              )

            _ ->
              Logger.error(
                "Failed to start on #{inspect(node)}: #{inspect(other)}, trying next node"
              )
          end

          try_nodes(supervisor, child_spec, rest)
      end
    catch
      :throw, {:error, :not_ready} ->
        Logger.warning("Node #{inspect(node)} not ready (still starting up), trying next node")

        try_nodes(supervisor, child_spec, rest)

      :error, {:erpc, erpc_reason} ->
        Logger.warning(
          "ERPC to #{inspect(node)} failed: #{inspect(erpc_reason)}, trying next node"
        )

        try_nodes(supervisor, child_spec, rest)

      :exit, {:exception, {:shutdown, _}} ->
        Logger.warning("Node #{inspect(node)} is shutting down, trying next node")

        try_nodes(supervisor, child_spec, rest)
    end
  end

  @doc """
  Ensures a DurableServer child process is started under this supervisor.

  Unlike `start_child/2`, this function first checks the registry for an existing
  process before attempting to start a new one. This is useful when you want to
  ensure a process exists but don't know if it's already running.

  ## Returns

  - `{:ok, {pid, meta}}` - Process is running (either found or newly started)
  - `{:error, reason}` - Failed to start the process

  ## Examples

      # Will start if not running, or return existing process
      {:ok, {pid, meta}} = DurableServer.Supervisor.ensure_started_child(
        MyApp.DurableSup,
        {MyServer, %{key: "server_1", initial_value: 42}}
      )

      # Calling again returns the same process
      {:ok, {^pid, ^meta}} = DurableServer.Supervisor.ensure_started_child(
        MyApp.DurableSup,
        {MyServer, %{key: "server_1", initial_value: 42}}
      )
  """
  def ensure_started_child(supervisor, {module, init_arg} = child_spec, opts \\ []) do
    key =
      case init_arg do
        %{key: key} ->
          key

        _ ->
          raise ArgumentError,
                "ensure_started_child expects a map with :key field, got: #{inspect(init_arg)}"
      end

    case lookup(supervisor, key) do
      {pid, meta} ->
        {:ok, {pid, meta}}

      nil ->
        # Try to fetch stored object to check sticky placement before attempting local start
        config = __get_config__(supervisor)
        storage_key = config.prefix <> key

        {stored_object, sticky_placement} =
          case ObjectStore.get_object(config.object_store, storage_key) do
            {:ok, %{body: body, etag: etag}} ->
              # Get augmented sticky placement (handles module config updates like :any)
              augmented_placement =
                __get_augmented_sticky_placement__(supervisor, module, key, body)

              {{:ok, %{body: body, etag: etag}}, augmented_placement}

            {:error, _} ->
              {nil, nil}
          end

        # Check if we should respect sticky placement and skip local start
        # For ensure_started (non-crash scenarios), we respect sticky placement WITHOUT time delays
        # Time delays are only for orphan/crash recovery handled by LifecycleManager
        # Also track if local node matches sticky level 0 with a SPECIFIC env var (for disk check bypass)
        {should_skip_local, is_sticky_local} =
          case sticky_placement do
            nil ->
              # No sticky placement, proceed with normal local-first logic
              {_should_skip_local = false, _is_sticky_local = false}

            [%{env_var: :any, value: :any} | _] ->
              # First level is :any, so local node is acceptable but we don't know
              # if data is specifically here (could have been on any node)
              {_should_skip_local = false, _is_sticky_local = false}

            placement ->
              # Have specific sticky placement (first element is not :any)
              # Check if local node matches
              env_var_names =
                DurableServer.Supervisor.collect_sticky_placement_env_vars(supervisor)

              my_env_vars =
                env_var_names
                |> Enum.map(fn var_name -> {var_name, System.get_env(var_name)} end)
                |> Enum.into(%{})

              # Check if we match any level (0 = exact, 1 = less specific, etc.)
              matching_level =
                Enum.find_index(placement, fn preference ->
                  case preference do
                    %{env_var: :any, value: :any} ->
                      true

                    %{env_var: env_var, value: expected_value} ->
                      Map.get(my_env_vars, env_var) == expected_value

                    _ ->
                      false
                  end
                end)

              # Skip local start only if we don't match level 0 (exact match)
              # This ensures servers stay on their sticky placement node for specific matches
              if matching_level == 0 do
                # Level 0 match with specific env var - use local, bypass disk check
                # (we know it's specific because :any at level 0 is handled above)
                {_should_skip_local = false, _is_sticky_local = true}
              else
                # Level > 0 match - skip local to try finding better remote match
                # Even if we matched :any at level 1+, there might be a level 0 match remotely
                {_should_skip_local = true, _is_sticky_local = false}
              end
          end

        # Use {:restart, ...} pattern if we have stored object, otherwise regular init
        # Include is_sticky_local flag for disk check bypass when restarting on sticky node
        child_spec_with_restart =
          case stored_object do
            {:ok, %{body: body, etag: etag}} ->
              {module,
               {:restart, %{key: key, body: body, etag: etag, is_sticky_local: is_sticky_local}}}

            nil ->
              child_spec
          end

        # If we should skip local due to sticky placement, go straight to remote placement
        cond do
          should_skip_local ->
            Logger.info(
              "Skipping local start for #{key} due to sticky placement mismatch, trying remote placement"
            )

            case try_remote_placement(supervisor, child_spec_with_restart, 3) do
              {:ok, result} ->
                {:ok, result}

              {:error, {:capacity_limit, :no_available_nodes}} ->
                # No healthy remote nodes found - fall back to local as last resort
                # Use max_placement_retries: 0 to only try local (no point retrying remotes)
                Logger.info(
                  "No healthy remote nodes found for #{key}, falling back to local start"
                )

                fallback_to_local_start(supervisor, child_spec_with_restart)

              {:error, {:capacity_limit, :all_placement_attempts_failed}} ->
                # All remote nodes failed (ERPC errors, capacity, etc.) - fall back to local
                # This can happen when sticky placement skipped local but all remotes are unreachable
                # Use max_placement_retries: 0 to only try local (we already tried remotes)
                Logger.info(
                  "All remote placement attempts failed for #{key}, falling back to local start"
                )

                fallback_to_local_start(supervisor, child_spec_with_restart)

              {:error, reason} ->
                {:error, reason}
            end

          true ->
            # Normal flow: try local first, then remote if capacity exceeded
            case start_child(supervisor, child_spec_with_restart, opts) do
              {:ok, {pid, meta}} ->
                {:ok, {pid, meta}}

              {:error, {:already_started, other}} ->
                {other_pid, other_meta} = other
                {:ok, {other_pid, other_meta}}

              {:error, reason} ->
                {:error, reason}
            end
        end
    end
  end

  defp fallback_to_local_start(supervisor, child_spec) do
    case start_child(supervisor, child_spec, max_placement_retries: 0) do
      {:ok, {pid, meta}} ->
        {:ok, {pid, meta}}

      {:error, {:already_started, other}} ->
        {other_pid, other_meta} = other
        {:ok, {other_pid, other_meta}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Terminates a specific DurableServer child process gracefully.

  The child will be given time to sync its state before termination.
  """
  def terminate_child(supervisor_name, pid) when is_atom(supervisor_name) and is_pid(pid) do
    Logger.debug(fn -> "terminating #{inspect(pid)} for #{inspect(supervisor_name)}" end)
    GenServer.call(pid, {@durable, {:stop_with_status, :stopped_graceful, :normal}})
  end

  def terminate_child(supervisor_name, key) when is_atom(supervisor_name) and is_binary(key) do
    Logger.debug(fn -> "looking up pid to terminate #{key} for #{inspect(supervisor_name)}" end)

    case lookup(supervisor_name, key) do
      {pid, _} -> terminate_child(supervisor_name, pid)
      nil -> {:error, :noproc}
    end
  end

  @doc """
  Terminates a specific DurableServer child process gracefully, and unmark it for permanent restart.

  Useful to stop a previously permanently started durable server so that it won't be considered a
  candidated for permanent restart in the future.

  The child will be given time to sync its state before termination.
  """
  def terminate_child_permanent(supervisor_name, pid)
      when is_atom(supervisor_name) and is_pid(pid) do
    Logger.info(fn ->
      "terminating #{inspect(pid)} (permanently) for #{inspect(supervisor_name)}"
    end)

    GenServer.call(pid, {@durable, {:stop_with_status, :stopped_permanent, :normal}})
  end

  def terminate_child_permanent(supervisor_name, key)
      when is_atom(supervisor_name) and is_binary(key) do
    Logger.info(fn ->
      "looking up pid to terminate (permanently) #{key} for #{inspect(supervisor_name)}"
    end)

    case lookup(supervisor_name, key) do
      {pid, _} -> terminate_child_permanent(supervisor_name, pid)
      nil -> {:error, :noproc}
    end
  end

  @doc """
  Rehomes a DurableServer child to a different node, bypassing sticky placement.

  This is useful for manual rebalancing or administrative operations. The operation:

  1. Terminates the process gracefully on its current node (if running)
  2. Starts the process on the target node (or any eligible node if no target specified)

  ## Parameters

  - `supervisor` - The DurableServer.Supervisor name
  - `child_spec` - The child spec tuple `{module, init_arg}` with a `:key` field
  - `opts` - Options:
    - `:target_node` - Specific node atom to place on (optional, defaults to best available)
    - `:force` - If true, ignore sticky placement entirely (default: true)

  ## Returns

  - `{:ok, {pid, meta}}` - Successfully rehomed the process
  - `{:error, reason}` - Failed to rehome

  ## Examples

      # Rehome to a specific node
      {:ok, {pid, meta}} = DurableServer.Supervisor.rehome_child(
        MySup,
        {MyServer, %{key: "server_1"}},
        target_node: :"node2@host"
      )

      # Rehome to any available node (ignoring sticky placement)
      {:ok, {pid, meta}} = DurableServer.Supervisor.rehome_child(
        MySup,
        {MyServer, %{key: "server_1"}}
      )
  """
  def rehome_child(supervisor, {module, init_arg} = child_spec, opts \\ []) do
    opts = Keyword.validate!(opts, [:target_node, :force, :shutdown_timeout])
    shutdown_timeout = Keyword.get(opts, :shutdown_timeout, 15_000)

    key =
      case init_arg do
        %{key: key} ->
          key

        _ ->
          raise ArgumentError,
                "rehome_child expects a map with :key field, got: #{inspect(init_arg)}"
      end

    target_node = Keyword.get(opts, :target_node)
    force = Keyword.get(opts, :force, true)

    # Step 1: Terminate existing process if running
    case lookup(supervisor, key) do
      {pid, _meta} ->
        Logger.info("Rehoming #{key}: terminating on #{node(pid)}")
        monitor_ref = Process.monitor(pid)
        terminate_child(supervisor, pid)

        # Wait for the process to terminate
        receive do
          {:DOWN, ^monitor_ref, :process, ^pid, _reason} ->
            :ok
        after
          shutdown_timeout ->
            # Process didn't terminate within timeout, demonitor and continue anyway
            Process.demonitor(monitor_ref, [:flush])
            Logger.warning("Process #{inspect(pid)} for #{key} did not terminate within 5s")
            :ok
        end

      nil ->
        :ok
    end

    # Step 2: Start on target node or find best placement
    cond do
      target_node != nil ->
        # Explicit target node specified
        Logger.info("Rehoming #{key}: placing on specified target #{inspect(target_node)}")

        try do
          result =
            :erpc.call(target_node, __MODULE__, :start_child, [
              supervisor,
              child_spec,
              [max_placement_retries: 0]
            ])

          case result do
            {:ok, {pid, meta}} ->
              Logger.info("Successfully rehomed #{key} to #{inspect(target_node)}")
              {:ok, {pid, meta}}

            {:error, reason} ->
              Logger.error(
                "Failed to rehome #{key} to #{inspect(target_node)}: #{inspect(reason)}"
              )

              {:error, reason}
          end
        catch
          :throw, {:error, :not_ready} ->
            Logger.error(
              "Node #{inspect(target_node)} not ready (still starting up) for rehoming #{key}"
            )

            {:error, :not_ready}

          :error, {:erpc, erpc_reason} ->
            Logger.error("ERPC to #{inspect(target_node)} failed: #{inspect(erpc_reason)}")
            {:error, {:erpc, erpc_reason}}
        end

      force ->
        # Force placement ignoring sticky - try remote placement across all eligible nodes
        Logger.info("Rehoming #{key}: finding best available node (ignoring sticky placement)")

        eligible_nodes =
          LifecycleManager.find_eligible_nodes(supervisor, module,
            limit: 3,
            # Pass nil for both to ignore sticky placement
            key: nil,
            sticky_placement: nil
          )

        case eligible_nodes do
          [] ->
            # No remote nodes, try local
            Logger.info("No remote nodes available, trying local for rehoming #{key}")

            case start_child(supervisor, child_spec, max_placement_retries: 0) do
              {:ok, {pid, meta}} -> {:ok, {pid, meta}}
              {:error, {:already_started, other}} -> {:ok, other}
              {:error, reason} -> {:error, reason}
            end

          nodes ->
            try_nodes(supervisor, child_spec, nodes)
        end

      true ->
        # Normal ensure_started logic (respects sticky)
        Logger.info("Rehoming #{key}: using normal placement logic")
        ensure_started_child(supervisor, child_spec)
    end
  end

  @doc """
  Terminates a DurableServer child process AND deletes its object storage.

  This permanently removes the server and all its persisted state. The operation:

  1. Finds the running process (if any) by PID or key
  2. Terminates the process gracefully (allowing final state sync)
  3. Deletes the object storage data

  ## Parameters

  - `supervisor` - The DurableServer.Supervisor name
  - `pid_or_key` - Either a PID of the running process or the key string

  ## Returns

  - `:ok` - Successfully terminated process and deleted storage
  - `{:error, reason}` - Failed to delete (process may still be terminated)

  ## Examples

      # Delete by PID
      :ok = DurableServer.Supervisor.terminate_and_delete_child(MySup, pid)

      # Delete by key
      :ok = DurableServer.Supervisor.terminate_and_delete_child(MySup, "user_123")
  """
  def terminate_and_delete_child(supervisor, pid_or_key, timeout \\ 5000)

  def terminate_and_delete_child(supervisor, pid_or_key, timeout)
      when (is_pid(pid_or_key) or is_binary(pid_or_key)) and is_integer(timeout) do
    config = __get_config__(supervisor)
    DurableServer.__delete_request__(supervisor, pid_or_key, timeout, config)
  end

  @doc """
  Returns the count of currently running DurableServer processes.
  """
  def count_children(supervisor) do
    dynamic_sup = get_dynamic_supervisor(supervisor)
    DynamicSupervisor.count_children(dynamic_sup)
  end

  @doc """
  Lists all currently running DurableServer child processes on this node's supervisor.
  """
  def which_children(supervisor) do
    dynamic_sup = get_dynamic_supervisor(supervisor)
    DynamicSupervisor.which_children(dynamic_sup)
  end

  @doc """
  Gets all global members matching this supervisor name on the cluster along with their metadata.

  Returns a map of all members in the form `%{key => {pid, meta}}`.

  ## Examples

      # Get all members for a supervisor
      DurableServer.Supervisor.global_members(MySup)
      #=> %{"user_1" => {#PID<0.123.0>, %{...}}, "user_2" => {#PID<0.124.0>, %{...}}}

      # Get only members for a specific module
      DurableServer.Supervisor.global_members(MySup, MyServer)
      #=> %{"user_1" => {#PID<0.123.0>, %{...}}}
  """
  def global_members(sup_name) when is_atom(sup_name) do
    %{syn_scope: scope} = __get_config__(sup_name)

    scope
    |> :syn.members(sup_name)
    |> Enum.reduce(%{}, fn {pid, meta}, acc ->
      if node(pid) == Node.self() && !Process.alive?(pid) do
        acc
      else
        Map.put(acc, meta.key, {pid, meta})
      end
    end)
  end

  def global_members(sup_name, module) when is_atom(sup_name) and is_atom(module) do
    %{syn_scope: scope} = __get_config__(sup_name)

    scope
    |> :syn.members(module)
    |> Enum.reduce(%{}, fn {pid, meta}, acc ->
      if node(pid) == Node.self() && !Process.alive?(pid) do
        acc
      else
        Map.put(acc, meta.key, {pid, meta})
      end
    end)
  rescue
    ArgumentError -> %{}
  end

  @doc false
  def __register_child__(sup_name, key, pid, meta)
      when is_atom(sup_name) and is_binary(key) and is_pid(pid) do
    %{syn_scope: scope, ets_table: table_name} = __get_config__(sup_name)

    case :syn.register(scope, key, pid, meta) do
      :ok ->
        :ok = :syn.join(scope, sup_name, pid, meta)

        # Also register in module-specific scope for per-module counting
        # Scopes are created at startup, but we check at runtime to handle hot code reloads
        if module = meta[:module] do
          [{:capacity_limits, limits}] = :ets.lookup(table_name, :capacity_limits)

          if is_map(limits[:max_children]) and Map.has_key?(limits[:max_children], module) do
            :ok = :syn.join(scope, module, pid, meta)
          end
        end

        :ok

      {:error, :taken} ->
        {:error, :taken}
    end
  end

  def get_dynamic_supervisor(supervisor) do
    :"#{supervisor}_dynamic"
  end

  def get_task_supervisor(supervisor) do
    :"#{supervisor}_task_sup"
  end

  @impl Supervisor
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    prefix = Keyword.fetch!(opts, :prefix)

    # Extract infrastructure options with defaults
    finch = Keyword.get(opts, :finch, DurableServer.Finch)
    task_sup = Keyword.get(opts, :task_supervisor, DurableServer.TaskSupervisor)

    # Build ObjectStore from :object_store keyword list or use provided struct
    object_store =
      case Keyword.fetch!(opts, :object_store) do
        %ObjectStore{} = store ->
          store

        object_store_opts when is_list(object_store_opts) ->
          ObjectStore.new(
            object_store_opts
            |> Keyword.put_new(:finch, finch)
            |> Keyword.put_new(:task_supervisor, task_sup)
          )
      end

    # bucket must exist for any durable object storage to work
    :ok = ObjectStore.ensure_bucket_exists(object_store)

    # Extract and validate capacity limits
    capacity_limits = extract_capacity_limits(opts)

    # Extract and validate sticky placement config
    sticky_placement_config = extract_sticky_placement_config(opts)

    # Extract and validate heartbeat_meta config
    heartbeat_meta = extract_heartbeat_meta_config(opts)

    # For DynamicSupervisor, use :infinity or integer (not map)
    max_children =
      case Keyword.get(opts, :max_children, :infinity) do
        val when is_map(val) -> :infinity
        val -> val
      end

    # Start os_mon if resource limits configured
    if capacity_limits[:max_cpu] || capacity_limits[:max_memory] || capacity_limits[:max_disk] do
      case Application.ensure_all_started(:os_mon) do
        {:ok, _} ->
          :ok

        {:error, reason} ->
          Logger.warning("Failed to start os_mon, resource limits disabled: #{inspect(reason)}")
      end
    end

    # create ets table for config and node_ref storage (cleaned up when supervisor dies)
    table_name = ets_table_name(name)
    ^table_name = :ets.new(table_name, [:named_table, :set, :public, read_concurrency: true])

    # generate and store node_ref in ets
    node_ref = System.system_time(:microsecond)
    :ets.insert(table_name, {:node_ref, node_ref})

    syn_sup_scope = syn_scope(name)
    :syn.add_node_to_scopes([syn_sup_scope])

    # create CircuitBreaker struct and initialize ETS table
    circuit_breaker =
      CircuitBreaker.new(name, %{
        object_store: object_store,
        crash_threshold_count: Keyword.get(opts, :crash_threshold_count, 5),
        crash_threshold_window_ms: Keyword.get(opts, :crash_threshold_window_ms, 60 * 60 * 1000),
        module_circuit_breaker_count: Keyword.get(opts, :module_circuit_breaker_count, 50),
        module_circuit_breaker_window_ms:
          Keyword.get(opts, :module_circuit_breaker_window_ms, 5 * 60 * 1000),
        module_circuit_breaker_cooldown_ms:
          Keyword.get(opts, :module_circuit_breaker_cooldown_ms, 10 * 60 * 1000),
        global_lock_failure_count: Keyword.get(opts, :global_lock_failure_count, 10),
        global_lock_failure_window_ms:
          Keyword.get(opts, :global_lock_failure_window_ms, 30 * 1000),
        global_lock_failure_cooldown_ms:
          Keyword.get(opts, :global_lock_failure_cooldown_ms, 60 * 1000)
      })

    # store configuration in ETS for child processes to access
    config = %{
      name: name,
      syn_scope: syn_sup_scope,
      prefix: prefix,
      object_store: object_store,
      discovery_interval_ms: Keyword.get(opts, :discovery_interval_ms, 60_000),
      heartbeat_interval_ms: Keyword.get(opts, :heartbeat_interval_ms, 10_000),
      graceful_shutdown_timeout_ms: Keyword.get(opts, :graceful_shutdown_timeout_ms, 30_000),
      dead_node_threshold_ms: Keyword.get(opts, :dead_node_threshold_ms, 5 * 60 * 1000),
      sticky_placement_history_limit: Keyword.get(opts, :sticky_placement_history_limit, 5),
      circuit_breaker: circuit_breaker,
      ets_table: table_name
    }

    Logger.info("starting #{inspect(name)}: #{inspect(config)}")

    :ets.insert(table_name, {:config, config})
    :ets.insert(table_name, {:capacity_limits, capacity_limits})
    :ets.insert(table_name, {:sticky_placement_config, sticky_placement_config})

    :ets.insert(
      table_name,
      {:resource_metrics, {_cpu = nil, _memory = nil, _disk = nil, _ts = nil}}
    )

    dynamic_sup_name = get_dynamic_supervisor(name)
    task_sup_name = get_task_supervisor(name)

    children = [
      {Task.Supervisor, name: task_sup_name},
      {DynamicSupervisor,
       name: dynamic_sup_name,
       strategy: :one_for_one,
       max_children: max_children,
       max_restarts: 1000,
       max_seconds: 5,
       shutdown: 60_000},
      {LifecycleManager,
       supervisor_name: name,
       task_supervisor: task_sup_name,
       object_store: object_store,
       config: config,
       circuit_breaker: circuit_breaker,
       capacity_limits: capacity_limits,
       heartbeat_meta: heartbeat_meta,
       shutdown: 60_000},
      {Terminator, supervisor_name: name, config: config, shutdown: 60_000}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  @doc false
  def syn_scope(sup_name) when is_atom(sup_name), do: :"durable_#{sup_name}"

  @doc false
  def __get_config__(supervisor_name) do
    table_name = ets_table_name(supervisor_name)

    case :ets.lookup(table_name, :config) do
      [{:config, config}] ->
        config

      [] ->
        raise "DurableServer.Supervisor ets table not found for #{inspect(supervisor_name)}. Make sure the supervisor is started."
    end
  end

  @doc false
  def __ets_table_name__(supervisor_name) do
    ets_table_name(supervisor_name)
  end

  defp ets_table_name(supervisor_name) do
    :"durable_supervisor_#{supervisor_name}"
  end

  defp extract_capacity_limits(opts) do
    limits = %{}

    # Handle max_children - can be :infinity, integer, or map
    case Keyword.get(opts, :max_children) do
      nil ->
        limits

      :infinity ->
        limits

      limit when is_integer(limit) ->
        %{:total => limit}

      limit_map when is_map(limit_map) ->
        validate_max_children_map!(limit_map)
        Map.put(limits, :max_children, limit_map)

      other ->
        raise ArgumentError,
              "max_children must be :infinity, integer, or map (%{:total => 123, MyModule => 456}), got: #{inspect(other)}"
    end
    |> maybe_add_limit(:max_cpu, opts[:max_cpu])
    |> maybe_add_limit(:max_memory, opts[:max_memory])
    |> maybe_add_disk_limit(opts[:max_disk])
  end

  defp maybe_add_disk_limit(limits, nil), do: limits

  defp maybe_add_disk_limit(limits, {percent, mount_point})
       when is_integer(percent) and percent > 0 and percent <= 100 and is_binary(mount_point) do
    Map.put(limits, :max_disk, %{percent: percent, mount_point: mount_point})
  end

  defp maybe_add_disk_limit(_limits, value) do
    raise ArgumentError,
          "max_disk must be a tuple {percent, mount_point} where percent is 1-100 and mount_point is a string, got: #{inspect(value)}"
  end

  defp validate_max_children_map!(max_children) when is_map(max_children) do
    Enum.each(max_children, fn
      {:total, val} when is_integer(val) and val > 0 ->
        :ok

      {mod, val} when is_atom(mod) and is_integer(val) and val > 0 ->
        :ok

      other ->
        raise ArgumentError, "Invalid max_children entry: #{inspect(other)}"
    end)
  end

  defp maybe_add_limit(limits, _key, nil), do: limits

  defp maybe_add_limit(limits, key, value)
       when is_integer(value) and value > 0 and value <= 100 do
    Map.put(limits, key, value)
  end

  defp maybe_add_limit(_limits, key, value) do
    raise ArgumentError, "#{key} must be an integer between 1 and 100, got: #{inspect(value)}"
  end

  defp extract_sticky_placement_config(opts) do
    sticky_placement = Keyword.get(opts, :sticky_placement, %{})
    default_sticky = Keyword.get(opts, :default_sticky_placement)

    validate_sticky_placement_config!(sticky_placement)

    if default_sticky do
      validate_sticky_placement_entry!(:default, default_sticky)
    end

    %{
      per_module: sticky_placement,
      default: default_sticky
    }
  end

  defp validate_sticky_placement_config!(config) when is_map(config) do
    Enum.each(config, fn {module, placement_config} ->
      validate_sticky_placement_entry!(module, placement_config)
    end)
  end

  defp validate_sticky_placement_entry!(module, config) do
    case config do
      list when is_list(list) ->
        validate_keyword_sticky_placement!(module, list)

      other ->
        raise ArgumentError,
              "sticky_placement for #{inspect(module)} must be a keyword list like [FLY_MACHINE_ID: 10_000, FLY_REGION: 20_000], got: #{inspect(other)}"
    end
  end

  defp validate_keyword_sticky_placement!(module, list) do
    unless Keyword.keyword?(list) do
      raise ArgumentError,
            "sticky_placement for #{inspect(module)} must be a keyword list with env vars and delays, got: #{inspect(list)}"
    end

    Enum.each(list, fn {env_var_atom, delay} ->
      # Validate delay
      unless is_integer(delay) and delay >= 0 do
        raise ArgumentError,
              "sticky_placement delays for #{inspect(module)} must be non-negative integers, got: #{inspect(delay)} for #{inspect(env_var_atom)}"
      end

      # Validate env var (convert atom to string and check)
      env_var_str = Atom.to_string(env_var_atom)

      unless env_var_atom == :any or String.match?(env_var_str, ~r/^[A-Z][A-Z0-9_]*$/) do
        raise ArgumentError,
              "sticky_placement env var for #{inspect(module)} should be an uppercase env var like FLY_MACHINE_ID or :any, got: #{inspect(env_var_atom)}"
      end
    end)
  end

  defp extract_heartbeat_meta_config(opts) do
    case Keyword.get(opts, :heartbeat_meta) do
      nil ->
        nil

      %{} = map ->
        map

      func when is_function(func, 0) ->
        # Validate that the function returns a map at config time
        case func.() do
          %{} = _map ->
            func

          other ->
            raise ArgumentError,
                  "heartbeat_meta function must return a map, got: #{inspect(other)}"
        end

      other ->
        raise ArgumentError,
              "heartbeat_meta must be a map or a zero-arity function returning a map, got: #{inspect(other)}"
    end
  end

  @doc false
  def __get_sticky_placement_for_module__(supervisor_name, module)
      when is_atom(supervisor_name) and is_atom(module) do
    table_name = ets_table_name(supervisor_name)

    [{:sticky_placement_config, %{per_module: per_module, default: default}}] =
      :ets.lookup(table_name, :sticky_placement_config)

    case Map.get(per_module, module) do
      nil -> default
      config -> config
    end
  end

  @doc false
  def collect_sticky_placement_env_vars(supervisor_name) when is_atom(supervisor_name) do
    table_name = ets_table_name(supervisor_name)

    [{:sticky_placement_config, %{per_module: per_module, default: default}}] =
      :ets.lookup(table_name, :sticky_placement_config)

    # Collect all env vars from all module configs
    all_configs = Map.values(per_module) ++ if(default, do: [default], else: [])

    all_configs
    |> Enum.flat_map(fn list ->
      Enum.map(list, fn {env_var_atom, _delay} ->
        if env_var_atom == :any, do: :any, else: Atom.to_string(env_var_atom)
      end)
    end)
    |> Enum.reject(&(&1 == :any))
    |> Enum.uniq()
  end
end
