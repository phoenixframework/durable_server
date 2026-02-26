defmodule DurableServer.LifecycleManager do
  @moduledoc false

  _archdoc = """
  Manages the lifecycle and automatic restart of DurableServer processes across a cluster.

  ## Architecture

  The LifecycleManager detects and restart failed DurableServer processes across the cluster,
  while ensuring only a single process is restarted for a given key across the global cluster.

  The coordination for durable server starts takes three paths:

  1. **Fast path**: Uses `:syn`'s eventually consistent distributed registry for near-instant
    health detection of running servers. If an object's keys is already registered,
    then we don't restart it.
  2. **Slow path**: Falls back to node heartbeats + process lock validation for edge cases.
    Locks are carried out over an etag based object store claim.
  3. **Sticky placement**: Uses environment variable-based placement preferences with
    time-gated fallback to ensure servers restart on preferred nodes when possible.

  ## Node Heartbeat System

  Instead of per-object heartbeats, the lifecycle manager maintains node-level heartbeats:
  - Each node writes a single heartbeat to `__nodes/{node_name}` in object storage
  - Heartbeats are cached locally in ets for fast lookup during health checks

  ## Lifecycle Management

  The `DurableServer.Supervisor` starts several processes, including its own `Task.Supervisor` and
  `DynamicSupervisor`:

  - `DurableServer.Supervisor` - Main supervisor providing isolation and configuration
  - `DurableServer.LifecycleManager` - Monitors and restarts failed processes across the cluster
  - `DurableServer.Terminator` - Coordinates graceful shutdown to ensure state persistence

  ## Restart Lifecycle

  ### Server States
  - `:running` - Server is actively running and registered in `:syn`
  - `:stopped_graceful` - Server was intentionally stopped, eligible for restart
  - `:stopped_permanent` - Server was permanently stopped, never restart
  - `:crashed` - Server crashed or failed, always restart

  ### Discovery Process
  1. Write node heartbeat and refresh heartbeat cache
  2. List all DurableServer objects from ObjectStore (paginated with continuation tokens)
  3. Check health using check_server_health/2 (uses group + heartbeat cache)
  4. Apply consistent hashing to determine which servers this node should handle
  5. Detect orphaned servers (any node can claim these regardless of hash assignment)
  6. Attempt atomic restart claiming via DurableServer.claim_restart_attempt/2

  ### Restart Claiming Protocol
  Each restart attempt uses atomic ObjectStore operations to prevent race conditions:
  - `DurableServer.claim_restart_attempt/2` handles atomic claiming with TTL
  - If claim succeeds, this node "owns" the restart attempt for 30 seconds
  - If claim fails, another node is already handling the restart
  - TTL ensures stale claims are eventually cleaned up

  ## Health Detection

  ### `:syn` Registry (Fast Path)
  - Each DurableServer registers itself in the global :durable_servers scope on startup
  - Automatic deregistration on process termination or node failure
  - LifecycleManager skips restart attempts for servers present in `:syn`
  - `:syn` is eventually consistent, so slow path validation via rpc calls is
    still necessary as a fallback

  ### Node Heartbeats + Process Lock (Slow Path)
  Used when `:syn` registry appears incomplete or for final validation:
  - Check if target node is healthy via cached heartbeat data
  - Validate process lock via rpc call to ensure object store recorded node/pid is not still alive

  ## Edge Case Handling

  ### Rolling Deploys
  - Uses DNS-based node discovery (no connectivity checks during consistent hashing)
  - Assumes all DNS-returned nodes are valid for hash distribution
  - Orphan detection handles cases where assigned nodes are temporarily unreachable

  ### Network Partitions
  - Multiple nodes may attempt restart during partitions
  - Atomic ObjectStore operations ensure only one succeeds
  - Group registry eventual consistency provides healing after partition resolution

  ### Orphaned Servers
  Any server is considered orphaned and claimable by any node if:
  - Target node heartbeat is stale or missing
  - Process lock validation fails (lock expired)
  - Status is explicitly :crashed
  - Previous restart attempt has expired (past TTL)

  ### Object Storage Failures
  - Discovery continues if individual object reads fail (logged but skipped)
  - List operations retried at the GenServer level
  - Atomic claim failures are treated as "already claimed" rather than errors
  """

  use GenServer
  require Logger

  alias DurableServer.LifecycleManager
  alias DurableServer
  alias DurableServer.{StoredState, Meta, CircuitBreaker}
  alias DurableServer.ObjectStore

  defstruct supervisor_name: nil,
            task_sup: nil,
            config: nil,
            circuit_breaker: nil,
            object_store: nil,
            prefix: nil,
            node_module: nil,
            current_discovery_task: nil,
            current_heartbeat_task: nil,
            heartbeat_table: nil,
            discovery_interval_ms: nil,
            heartbeat_interval_ms: nil,
            capacity_limits: %{},
            heartbeat_meta: nil,
            last_successful_heartbeat_at: nil,
            heartbeat_deadline_timer: nil,
            # Last successful heartbeat timing for diagnostics
            last_heartbeat_timing: nil,
            discovery_skip_table: nil

  @max_heartbeat_retries 3
  # Buffer for heartbeat deadline - time for crash/cleanup before orphan threshold
  # Orphan threshold (20s) already includes grace period (2x heartbeat interval),
  # so we only need a small buffer for crash propagation time
  @heartbeat_deadline_buffer_ms :timer.seconds(2)
  @restart_claim_ttl_ms :timer.seconds(30)
  # batch size for accumulating keys before shuffling - provides randomization for load distribution
  # during cold deploys while maintaining bounded memory usage
  @shuffle_batch_size 20_000
  # parallel processing batch size - how many restart attempts to run concurrently
  @parallel_restart_batch_size 50
  # Threshold for considering a node's heartbeat stale (used in heartbeat validation/deadline)
  @heartbeat_staleness_threshold_ms :timer.seconds(20)
  # Threshold for considering a node unhealthy when finding eligible placement nodes
  @node_health_staleness_threshold_ms :timer.seconds(50)
  @resource_check_interval_ms :timer.seconds(60)
  # Skip set entries expire after 10 minutes. Deleted objects won't appear
  # in LIST anymore, so their entries would never self-invalidate via etag.
  # TTL ensures churned objects don't accumulate unbounded memory.
  @discovery_skip_ttl_ms :timer.minutes(10)
  @discovery_skip_sweep_interval_ms :timer.minutes(5)

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Returns the current heartbeat timing metrics for this node.

  Returns a map with:
  - `:last_heartbeat_timing` - The timing from the last successful heartbeat (put_ms, cache_ms, total_ms)
  - `:last_successful_heartbeat_at` - Timestamp of last successful heartbeat
  - `:node` - This node's name

  This is used by the admin dashboard to monitor heartbeat health across the cluster.
  """
  def get_heartbeat_metrics(supervisor_name) when is_atom(supervisor_name) do
    lifecycle_manager = get_lifecycle_manager_pid(supervisor_name)
    GenServer.call(lifecycle_manager, :get_heartbeat_metrics)
  end

  defp get_lifecycle_manager_pid(supervisor_name) do
    children = Supervisor.which_children(supervisor_name)

    Enum.find_value(children, fn
      {DurableServer.LifecycleManager, pid, :worker, _} when is_pid(pid) -> pid
      _ -> nil
    end)
  end

  def init(opts) do
    supervisor_name = Keyword.fetch!(opts, :supervisor_name)
    task_supervisor = Keyword.fetch!(opts, :task_supervisor)
    circuit_breaker = Keyword.fetch!(opts, :circuit_breaker)
    object_store = Keyword.fetch!(opts, :object_store)
    capacity_limits = Keyword.get(opts, :capacity_limits, %{})
    heartbeat_meta = Keyword.get(opts, :heartbeat_meta)

    config =
      Keyword.get(opts, :config, %{
        discovery_interval_ms: 60_000,
        heartbeat_interval_ms: 10_000,
        prefix: "test/"
      })

    # Validate heartbeat timing config
    # Nodes are considered stale at 2x heartbeat_interval. If heartbeat_interval is too long,
    # nodes would be considered stale before they even miss a heartbeat.
    max_heartbeat_interval = div(@heartbeat_staleness_threshold_ms, 2)

    if config.heartbeat_interval_ms > max_heartbeat_interval do
      raise ArgumentError, """
      Invalid heartbeat_interval_ms configuration: #{config.heartbeat_interval_ms}ms

      heartbeat_interval_ms must be <= #{max_heartbeat_interval}ms (half of heartbeat_staleness_threshold: #{@heartbeat_staleness_threshold_ms}ms).

      With the current value, nodes would be considered stale before they even
      have a chance to send their next heartbeat, causing unnecessary failovers.
      """
    end

    hearbeat_tab = heartbeat_table_name(supervisor_name)
    :ets.new(hearbeat_tab, [:set, :public, :named_table, read_concurrency: true])

    skip_tab = :"durable_server_discovery_skip_#{supervisor_name}"
    :ets.new(skip_tab, [:set, :public, :named_table])

    state = %LifecycleManager{
      supervisor_name: supervisor_name,
      task_sup: task_supervisor,
      config: config,
      circuit_breaker: circuit_breaker,
      object_store: object_store,
      prefix: config.prefix,
      node_module: Keyword.get(opts, :node_module, Node),
      current_discovery_task: nil,
      current_heartbeat_task: nil,
      heartbeat_table: hearbeat_tab,
      discovery_interval_ms: config.discovery_interval_ms,
      heartbeat_interval_ms: config.heartbeat_interval_ms,
      capacity_limits: capacity_limits,
      heartbeat_meta: heartbeat_meta,
      discovery_skip_table: skip_tab
    }

    # schedule periodic resource checks if limits configured
    # run immediately to populate metrics, then schedule periodic checks
    state =
      if capacity_limits != %{} do
        Process.send_after(self(), :check_resources, @resource_check_interval_ms)
        update_resource_metrics(state)
      else
        state
      end

    # start with a random delay to spread out discovery across nodes
    discovery_delay = :rand.uniform(5_000) + 1_000
    Process.send_after(self(), :discover_and_restart, discovery_delay)

    Process.send_after(self(), :heartbeat, config.heartbeat_interval_ms)
    Process.send_after(self(), :sweep_discovery_skip_set, @discovery_skip_sweep_interval_ms)

    # we MUST start with a populated node heartbeat cache
    # perform_heartbeat writes our heartbeat and refreshes the node health cache
    timing = perform_heartbeat(state)

    {:ok,
     %{
       state
       | last_successful_heartbeat_at: System.system_time(:millisecond),
         last_heartbeat_timing: timing
     }}
  end

  def handle_info(:heartbeat, %LifecycleManager{} = state) do
    now = System.system_time(:millisecond)
    deadline_ms = heartbeat_hard_deadline_ms()
    deadline_at = state.last_successful_heartbeat_at + deadline_ms

    # Check if we've already exceeded the deadline
    if now >= deadline_at do
      log(state, :error, fn ->
        "heartbeat deadline already exceeded before starting task, stopping supervisor tree"
      end)

      {:stop, {:heartbeat_deadline_exceeded, state.last_successful_heartbeat_at}, state}
    else
      # Cancel any existing deadline timer
      if state.heartbeat_deadline_timer do
        Process.cancel_timer(state.heartbeat_deadline_timer)
      end

      # Set watchdog timer to fire at deadline
      timer_delay = deadline_at - now
      deadline_timer = Process.send_after(self(), :heartbeat_deadline_exceeded, timer_delay)

      task =
        Task.Supervisor.async(state.task_sup, fn ->
          {:heartbeat, perform_heartbeat(state)}
        end)

      {:noreply,
       %{state | current_heartbeat_task: task, heartbeat_deadline_timer: deadline_timer}}
    end
  end

  def handle_info(:heartbeat_deadline_exceeded, %LifecycleManager{} = state) do
    deadline_ms = heartbeat_hard_deadline_ms()
    elapsed_since_last = System.system_time(:millisecond) - state.last_successful_heartbeat_at

    # Check if heartbeat success message arrived (race window)
    case state.current_heartbeat_task do
      %Task{ref: ref} ->
        receive do
          {^ref, {:heartbeat, timing}} ->
            # Heartbeat actually succeeded! Don't crash, but log the close call.
            Process.demonitor(ref, [:flush])
            now = System.system_time(:millisecond)
            Process.send_after(self(), :heartbeat, state.heartbeat_interval_ms)

            log(state, :warning, fn ->
              "heartbeat narrowly beat deadline: #{timing.total_ms}ms (put: #{timing.put_ms}ms, cache: #{timing.cache_ms}ms), deadline was #{deadline_ms}ms"
            end)

            {:noreply,
             %{
               state
               | current_heartbeat_task: nil,
                 heartbeat_deadline_timer: nil,
                 last_successful_heartbeat_at: now,
                 last_heartbeat_timing: timing
             }}
        after
          0 ->
            # No success in mailbox - truly failed, crash
            prev_timing_info =
              case state.last_heartbeat_timing do
                %{put_ms: put_ms, cache_ms: cache_ms, total_ms: total_ms} ->
                  ", previous heartbeat timing: #{total_ms}ms (put: #{put_ms}ms, cache: #{cache_ms}ms)"

                nil ->
                  ""
              end

            log(state, :error, fn ->
              "heartbeat deadline exceeded (#{elapsed_since_last}ms since last success, deadline #{deadline_ms}ms#{prev_timing_info}), stopping supervisor tree to prevent orphan conflicts"
            end)

            Task.shutdown(state.current_heartbeat_task, :brutal_kill)
            {:stop, {:heartbeat_deadline_exceeded, state.last_successful_heartbeat_at}, state}
        end

      nil ->
        # No task running, this is a stale timer - ignore
        {:noreply, %{state | heartbeat_deadline_timer: nil}}
    end
  end

  def handle_info(:check_resources, %LifecycleManager{} = state) do
    new_state = update_resource_metrics(state)
    Process.send_after(self(), :check_resources, @resource_check_interval_ms)

    {:noreply, new_state}
  end

  def handle_info(:sweep_discovery_skip_set, %LifecycleManager{} = state) do
    expire_before = System.monotonic_time(:millisecond) - @discovery_skip_ttl_ms

    expired =
      :ets.select_delete(state.discovery_skip_table, [
        {{:_, :_, :_, :"$1"}, [{:<, :"$1", expire_before}], [true]}
      ])

    if expired > 0 do
      log(state, :debug, fn ->
        "Swept #{expired} expired entries from discovery skip set"
      end)
    end

    Process.send_after(self(), :sweep_discovery_skip_set, @discovery_skip_sweep_interval_ms)
    {:noreply, state}
  end

  def handle_info(:discover_and_restart, %LifecycleManager{} = state) do
    with %Task{ref: ref} <- state.current_discovery_task do
      Process.demonitor(ref, [:flush])
    end

    task =
      Task.Supervisor.async(state.task_sup, fn ->
        {:discover, discover_and_restart_servers(state)}
      end)

    {:noreply, %{state | current_discovery_task: task}}
  end

  def handle_info(
        {ref, {:discover, :ok}},
        %LifecycleManager{} = state
      ) do
    # discovery task completed successfully, schedule next run
    case state do
      %LifecycleManager{current_discovery_task: %Task{ref: ^ref}} ->
        Process.demonitor(ref, [:flush])
        Process.send_after(self(), :discover_and_restart, state.discovery_interval_ms)
        {:noreply, %{state | current_discovery_task: nil}}

      %LifecycleManager{} ->
        {:noreply, state}
    end
  end

  def handle_info(
        {ref, {:heartbeat, timing}},
        %LifecycleManager{current_heartbeat_task: %Task{ref: ref}} = state
      ) do
    # heartbeat task completed successfully, cancel deadline timer and schedule next run
    if state.heartbeat_deadline_timer do
      Process.cancel_timer(state.heartbeat_deadline_timer)
    end

    Process.demonitor(ref, [:flush])
    Process.send_after(self(), :heartbeat, state.heartbeat_interval_ms)

    # Log timing at info level if heartbeat took longer than expected (>5s)
    if timing.total_ms > 5000 do
      log(state, :warning, fn ->
        "heartbeat completed but took #{timing.total_ms}ms (put: #{timing.put_ms}ms, cache: #{timing.cache_ms}ms)"
      end)
    end

    {:noreply,
     %{
       state
       | current_heartbeat_task: nil,
         heartbeat_deadline_timer: nil,
         last_successful_heartbeat_at: System.system_time(:millisecond),
         last_heartbeat_timing: timing
     }}
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %LifecycleManager{current_discovery_task: %Task{ref: ref}} = state
      ) do
    # discovery task crashed or failed, still schedule next run but log the error
    log(state, :error, fn -> "discovery task failed: #{inspect(reason)}" end)
    :ets.delete_all_objects(state.discovery_skip_table)
    Process.send_after(self(), :discover_and_restart, state.discovery_interval_ms)
    {:noreply, %{state | current_discovery_task: nil}}
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %LifecycleManager{current_heartbeat_task: %Task{ref: ref}} = state
      ) do
    # Heartbeat task crashed - we must fail the supervisor tree (one_for_all) because
    # our children will become orphan claimable if we can't write heartbeats.
    # If we continue running, other nodes will claim our children as orphaned,
    # allowing split brain scenario
    log(state, :error, fn ->
      "heartbeat task failed, stopping supervisor tree: #{inspect(reason)}"
    end)

    {:stop, {:heartbeat_failed, reason}, state}
  end

  def handle_call(:get_heartbeat_metrics, _from, %LifecycleManager{} = state) do
    resources = calculate_resource_map(state.supervisor_name)
    capacity = DurableServer.Supervisor.current_capacity(state.supervisor_name)
    heartbeat_meta = resolve_heartbeat_meta(state.heartbeat_meta)

    metrics = %{
      node: Node.self(),
      last_heartbeat_timing: state.last_heartbeat_timing,
      last_successful_heartbeat_at: state.last_successful_heartbeat_at,
      heartbeat_interval_ms: state.heartbeat_interval_ms,
      deadline_ms: heartbeat_hard_deadline_ms(),
      resources: resources,
      capacity: capacity,
      heartbeat_meta: heartbeat_meta
    }

    {:reply, metrics, state}
  end

  defp perform_heartbeat(%LifecycleManager{} = state) do
    start_time = System.monotonic_time(:millisecond)

    # Start cache refresh in parallel (runs concurrently with PUT)
    cache_task =
      Task.Supervisor.async(state.task_sup, fn ->
        cache_start = System.monotonic_time(:millisecond)
        result = refresh_node_heartbeat_cache(state)
        cache_duration = System.monotonic_time(:millisecond) - cache_start
        {result, cache_duration}
      end)

    # Do the critical heartbeat PUT inline
    put_start = System.monotonic_time(:millisecond)

    case write_node_heartbeat(state) do
      :ok ->
        put_duration = System.monotonic_time(:millisecond) - put_start
        log(state, :debug, fn -> "Node heartbeat written successfully in #{put_duration}ms" end)

      {:error, reason} ->
        put_duration = System.monotonic_time(:millisecond) - put_start

        # Kill the cache task since we're going to crash anyway
        Task.shutdown(cache_task, :brutal_kill)

        # we must fail the DurableSupervisor tree (one_for_all) if we fail to heartbeat because our
        # children will become orphan claimable and if we can't reach object storage we are in a failed state
        raise RuntimeError,
              "failed to write node heartbeat after #{@max_heartbeat_retries} attempts (#{put_duration}ms): #{inspect(reason)}"
    end

    put_duration = System.monotonic_time(:millisecond) - put_start

    # Wait for cache refresh
    {cache_result, cache_duration} = Task.await(cache_task, :timer.seconds(15))

    case cache_result do
      {:ok, _count, _cleaned_count, error_count} when error_count > 0 ->
        # A partial view of the cluster is dangerous - we could incorrectly treat
        # healthy nodes as expired and steal their locks
        raise RuntimeError,
              "failed to refresh heartbeat cache: #{error_count} heartbeat fetch errors"

      {:ok, count, cleaned_count, _error_count} when cleaned_count > 0 ->
        log(state, :debug, fn ->
          "Refreshed heartbeat cache with #{count} nodes, cleaned up #{cleaned_count} dead nodes in #{cache_duration}ms"
        end)

      {:ok, count, _cleaned_count, _error_count} ->
        log(state, :debug, fn ->
          "Refreshed heartbeat cache with #{count} nodes in #{cache_duration}ms"
        end)
    end

    :ok = CircuitBreaker.prune_stale_entries(state.circuit_breaker)

    total_duration = System.monotonic_time(:millisecond) - start_time
    %{put_ms: put_duration, cache_ms: cache_duration, total_ms: total_duration}
  end

  defp log(%LifecycleManager{} = state, level, func)
       when is_atom(level) and is_function(func, 0) do
    Logger.log(level, fn -> inspect(state.supervisor_name) <> ": " <> func.() end)
  end

  # Calculate the hard deadline for heartbeat operations.
  # We must complete heartbeat before other nodes consider us stale (orphan claimable).
  defp heartbeat_hard_deadline_ms do
    @heartbeat_staleness_threshold_ms - @heartbeat_deadline_buffer_ms
  end

  # this gets run async inside a task
  defp write_node_heartbeat(%LifecycleManager{} = state) do
    node_str = to_string(Node.self())
    node_ref = DurableServer.Supervisor.node_ref(state.supervisor_name)
    current_time = System.system_time(:millisecond)

    # calculate capacity and resource info
    capacity = DurableServer.Supervisor.current_capacity(state.supervisor_name)
    resources = calculate_resource_map(state.supervisor_name)

    # collect env vars used by sticky placement configs
    env_var_names =
      DurableServer.Supervisor.collect_sticky_placement_env_vars(state.supervisor_name)

    env_vars =
      env_var_names
      |> Enum.map(fn var_name -> {var_name, System.get_env(var_name)} end)
      |> Enum.into(%{})

    # resolve heartbeat_meta (can be a map or a function that returns a map)
    heartbeat_meta = resolve_heartbeat_meta(state.heartbeat_meta)

    heartbeat_data =
      %{
        node: node_str,
        node_ref: node_ref,
        last_heartbeat_at: current_time
      }
      |> maybe_put(:capacity, capacity)
      |> maybe_put(:resources, resources)
      |> maybe_put(:env_vars, env_vars)
      |> maybe_put(:heartbeat_meta, heartbeat_meta)

    key = "#{state.prefix}__nodes/#{node_str}"
    json_data = JSON.encode!(heartbeat_data)

    # Calculate remaining time until deadline for the PUT operation
    # On initial heartbeat (in init), last_successful_heartbeat_at is nil - use full deadline
    put_opts =
      case state.last_successful_heartbeat_at do
        nil ->
          [max_retries: @max_heartbeat_retries, timeout: heartbeat_hard_deadline_ms()]

        last_heartbeat_at ->
          deadline_at = last_heartbeat_at + heartbeat_hard_deadline_ms()
          remaining_ms = max(deadline_at - System.system_time(:millisecond), 0)
          [max_retries: @max_heartbeat_retries, timeout: remaining_ms]
      end

    case ObjectStore.put_object(state.object_store, key, json_data, put_opts) do
      {:ok, _} ->
        # update local ets cache with full capacity info
        :ets.insert(
          state.heartbeat_table,
          {node_str, node_ref, current_time, capacity, resources, env_vars, heartbeat_meta}
        )

        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_heartbeat_meta(nil), do: nil
  defp resolve_heartbeat_meta(%{} = map), do: map

  defp resolve_heartbeat_meta(func) when is_function(func, 0) do
    case func.() do
      %{} = map ->
        map

      other ->
        raise ArgumentError,
              "heartbeat_meta function must return a map, got: #{inspect(other)}"
    end
  end

  # this gets run async inside a task
  defp refresh_node_heartbeat_cache(%LifecycleManager{} = state) do
    dead_node_threshold_ms = state.config.dead_node_threshold_ms
    current_time = System.system_time(:millisecond)

    # Collect keys first, then fetch in parallel
    keys =
      ObjectStore.list_all_objects_stream(state.object_store, "#{state.prefix}__nodes/",
        error_handler: fn reason ->
          log(state, :warning, fn -> "List stream error: #{inspect(reason)}" end)
          :continue
        end
      )
      |> Enum.map(& &1.key)

    results =
      keys
      |> Task.async_stream(
        fn key ->
          case ObjectStore.get_object(state.object_store, key) do
            {:ok, %{body: body}} ->
              case JSON.decode(body) do
                {:ok, data} ->
                  case parse_heartbeat_data(data) do
                    {:ok,
                     {node, node_ref, timestamp, capacity, resources, env_vars, heartbeat_meta}} ->
                      if current_time - timestamp > dead_node_threshold_ms do
                        {:dead, key, node, node_ref, timestamp}
                      else
                        {:alive, node, node_ref, timestamp, capacity, resources, env_vars,
                         heartbeat_meta}
                      end

                    {:error, :invalid_format} ->
                      {:fetch_error, key, :invalid_format}
                  end

                {:error, decode_reason} ->
                  {:fetch_error, key, {:json_decode, decode_reason}}
              end

            {:error, reason} ->
              {:fetch_error, key, reason}
          end
        end,
        max_concurrency: 20,
        timeout: :timer.seconds(10),
        on_timeout: :kill_task
      )
      |> Enum.map(fn
        {:ok, result} -> result
        {:exit, reason} -> {:task_error, reason}
      end)

    # Separate results into categories
    {heartbeats, rest} =
      Enum.split_with(results, fn
        {:alive, _node, _node_ref, _timestamp, _capacity, _resources, _env_vars, _heartbeat_meta} ->
          true

        _ ->
          false
      end)

    {dead_nodes, errors} =
      Enum.split_with(rest, fn
        {:dead, _key, _node, _node_ref, _timestamp} -> true
        _ -> false
      end)

    # Log any errors
    Enum.each(errors, fn
      {:fetch_error, key, reason} ->
        log(state, :warning, fn ->
          "Failed to fetch heartbeat for #{key}: #{inspect(reason)}"
        end)

      {:task_error, reason} ->
        log(state, :warning, fn ->
          "Heartbeat fetch task failed: #{inspect(reason)}"
        end)
    end)

    error_count = length(errors)

    # extract heartbeat data for alive nodes
    live_heartbeats =
      Enum.map(heartbeats, fn {:alive, node, node_ref, timestamp, capacity, resources, env_vars,
                               heartbeat_meta} ->
        {node, node_ref, timestamp, capacity, resources, env_vars, heartbeat_meta}
      end)

    # attempt to clean up dead nodes (race conditions are OK, delete might fail)
    cleaned_count =
      dead_nodes
      |> Enum.map(fn {:dead, key, node, _node_ref, _timestamp} ->
        :ets.delete(state.heartbeat_table, node)

        case ObjectStore.delete_object(state.object_store, key) do
          :ok ->
            log(state, :info, fn -> "Cleaned up dead node heartbeat: #{node}" end)
            1

          {:error, reason} ->
            # race condition or other error - another node might have cleaned it up
            log(state, :info, fn ->
              "Failed to clean up dead node heartbeat #{inspect(node)}: #{inspect(reason)}"
            end)

            0
        end
      end)
      |> Enum.sum()

    :ets.insert(state.heartbeat_table, live_heartbeats)

    {:ok, length(live_heartbeats), cleaned_count, error_count}
  end

  defp heartbeat_table_name(supervisor_name) when is_atom(supervisor_name) do
    :"durable_server_heartbeats_#{supervisor_name}"
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
    table_name = heartbeat_table_name(supervisor_name)

    :ets.tab2list(table_name)
    |> Enum.map(fn {node_str, _node_ref, _timestamp, _capacity, _resources, _env_vars,
                    heartbeat_meta} ->
      {node_str, %{heartbeat_meta: heartbeat_meta}}
    end)
    |> Enum.into(%{})
  end

  @doc """
  Returns the node health as it exists in the heartbeat table cache.

  Returns node health status with capacity and resource details:

  - `{:healthy, %{node_ref: ref, capacity: map, resources: map}}` - Node is healthy with capacity info
  - `:stale` - Node heartbeat is stale
  - `:unknown` - Node not found in cache

  The capacity map contains current vs limit for total (all modules) and per-module:
  ```
  %{
    :total => %{current: 50, limit: 100},
    MyApp.Server => %{current: 5, limit: 10}
  }
  ```

  The resources map contains current metrics and the node's own thresholds:
  ```
  %{
    cpu: 75.2,          # Current CPU %
    max_cpu: 80,        # This node's configured threshold
    memory: 67.8,       # Current memory %
    max_memory: 85      # This node's configured threshold
  }
  ```

  This allows remote nodes to make informed claiming decisions using the target
  node's own configuration, which is important when nodes have heterogeneous
  hardware (different CPU types, memory amounts).
  """
  def lookup_node_health(%Meta{supervisor: supervisor_name, node_str: node_str}) do
    lookup_node_health(%{supervisor: supervisor_name, node_str: node_str})
  end

  def lookup_node_health(%{supervisor: supervisor_name, node_str: node_str})
      when is_atom(supervisor_name) and is_binary(node_str) do
    # Handle race condition where this is called via RPC on a node that just joined
    # but hasn't fully initialized its supervisor/ETS tables yet
    table_name = heartbeat_table_name(supervisor_name)

    with %{heartbeat_interval_ms: heartbeat_interval_ms} <-
           DurableServer.Supervisor.__get_config__(supervisor_name),
         [{^node_str, node_ref, timestamp, capacity, resources, _env_vars, _heartbeat_meta}] <-
           :ets.lookup(table_name, node_str) do
      current_time = System.system_time(:millisecond)

      if current_time - timestamp > heartbeat_interval_ms * 2 do
        :stale
      else
        {:healthy, %{node_ref: node_ref, capacity: capacity, resources: resources}}
      end
    else
      # Config not ready, or node not found in heartbeat table
      _ -> :unknown
    end
  rescue
    # ETS table doesn't exist yet (node still initializing)
    ArgumentError -> :unknown
  end

  @doc """
  Fetch a node's heartbeat directly from object storage.

  This is used as a fallback when the local heartbeat cache returns `:unknown`,
  to avoid incorrectly treating a healthy node as expired just because we haven't
  refreshed our cache since that node joined.

  When a healthy heartbeat is fetched, it is also written to the local ETS cache
  so subsequent lookups will find it without hitting storage.

  Returns:
  - `{:healthy, %{node_ref: node_ref}}` if heartbeat exists and is fresh
  - `:stale` if heartbeat exists but is too old
  - `:not_found` if no heartbeat exists for this node
  - `{:error, reason}` on fetch failure
  """
  def fetch_node_heartbeat_from_storage(supervisor_name, node_str)
      when is_atom(supervisor_name) and is_binary(node_str) do
    with %{
           prefix: prefix,
           heartbeat_interval_ms: heartbeat_interval_ms,
           object_store: object_store
         } <-
           DurableServer.Supervisor.__get_config__(supervisor_name) do
      key = "#{prefix}__nodes/#{node_str}"

      case DurableServer.ObjectStore.get_object(object_store, key) do
        {:ok, %{body: body}} ->
          case JSON.decode(body) do
            {:ok, data} ->
              case parse_heartbeat_data(data) do
                {:ok,
                 {_node_str, node_ref, timestamp, _capacity, _resources, _env_vars,
                  _heartbeat_meta}} ->
                  current_time = System.system_time(:millisecond)

                  if current_time - timestamp > heartbeat_interval_ms * 2 do
                    :stale
                  else
                    # Cache the fetched heartbeat so subsequent lookups are fast
                    cache_fetched_heartbeat(supervisor_name, data)
                    {:healthy, %{node_ref: node_ref}}
                  end

                {:error, :invalid_format} ->
                  {:error, :invalid_heartbeat_format}
              end

            {:error, _reason} ->
              {:error, :invalid_heartbeat_format}
          end

        {:error, :not_found} ->
          :not_found

        {:error, reason} ->
          {:error, reason}
      end
    else
      nil -> {:error, :supervisor_not_ready}
    end
  end

  # Cache a heartbeat fetched from storage into the local ETS table
  defp cache_fetched_heartbeat(supervisor_name, data) do
    case parse_heartbeat_data(data) do
      {:ok, heartbeat_tuple} ->
        table_name = heartbeat_table_name(supervisor_name)
        :ets.insert(table_name, heartbeat_tuple)

      {:error, :invalid_format} ->
        :ok
    end
  rescue
    # ETS table might not exist if supervisor is shutting down
    ArgumentError -> :ok
  end

  defp discover_and_restart_servers(%LifecycleManager{} = state) do
    skip_count = :ets.info(state.discovery_skip_table, :size)

    if skip_count > 0 do
      log(state, :info, fn ->
        "Discovery skip set: #{skip_count} cached non-restartable objects"
      end)
    end

    # list all keys with this supervisor's prefix, but exclude __nodes/ heartbeat objects
    ObjectStore.list_all_objects_stream(state.object_store, state.prefix,
      error_handler: fn reason ->
        log(state, :error, fn -> "Failed to list objects: #{inspect(reason)}" end)
        :continue
      end
    )
    |> Stream.reject(&String.starts_with?(&1.key, "#{state.prefix}__nodes/"))
    |> Stream.flat_map(fn %{key: storage_key, etag: etag} ->
      key = String.trim_leading(storage_key, state.prefix)

      cond do
        # already running — skip
        match?({_, _}, DurableServer.Supervisor.lookup(state.supervisor_name, key)) ->
          []

        # in skip set with matching etag — skip permanently non-restartable,
        # re-evaluate temporarily non-restartable (time gates, circuit breaker)
        discovery_skip?(state, key, etag) ->
          []

        true ->
          [{key, etag}]
      end
    end)
    # accumulate large batches of keys, then shuffle for randomized restart order
    # this prevents all servers from being restarted in storage enumeration order
    # which would cause the first node up during a cold deploy to claim everything
    |> Stream.chunk_every(@shuffle_batch_size)
    |> Stream.flat_map(fn items ->
      shuffled = Enum.shuffle(items)

      log(state, :info, fn ->
        "Shuffled batch of #{length(shuffled)} keys for distributed restart"
      end)

      shuffled
    end)
    # process in smaller parallel batches for restart attempts
    |> Stream.chunk_every(@parallel_restart_batch_size)
    |> Enum.each(fn
      [_ | _] = batch ->
        log(state, :debug, fn ->
          "Processing parallel batch of #{length(batch)} servers for potential restart"
        end)

        # process each item in parallel and block on results (Stream.run blocks)
        batch
        |> Task.async_stream(
          fn {key, etag} ->
            case get_restartable_object(state, key) do
              %{} = obj ->
                attempt_restart(state, obj)

              :skip ->
                # permanently non-restartable — cache without meta
                now = System.monotonic_time(:millisecond)
                :ets.insert(state.discovery_skip_table, {key, etag, :skip, now})
                :noop

              {:skip, %Meta{} = meta} ->
                # temporarily non-restartable (circuit breaker, time-gated placement,
                # health check) — cache trimmed meta for re-evaluation next round
                now = System.monotonic_time(:millisecond)
                trimmed = trim_meta_for_cache(meta)
                :ets.insert(state.discovery_skip_table, {key, etag, trimmed, now})
                :noop

              nil ->
                # fetch error — don't cache
                :noop
            end
          end,
          timeout: 60_000,
          max_concurrency: @parallel_restart_batch_size
        )
        |> Stream.run()

      [] ->
        nil
    end)

    :ok
  end

  defp get_restartable_object(%LifecycleManager{} = state, key) do
    # first check if server is eligible for restart based on metadata
    # first see if server exists in registry, and skip it if so
    case DurableServer.Supervisor.lookup(state.supervisor_name, key) do
      {_pid, _meta} ->
        nil

      nil ->
        case DurableServer.fetch_stored_state(state.object_store, %{
               key: key,
               prefix: state.prefix
             }) do
          {:ok, %StoredState{meta: %Meta{} = meta} = obj} ->
            cond do
              # never restart permanently crashed servers
              Meta.permanently_crashed?(meta) ->
                :skip

              # only restart servers marked as permanent (default is false - user must opt-in)
              not meta.permanent ->
                :skip

              # never restart explicitly stopped servers
              Meta.stopped_permanently?(meta) ->
                :skip

              # do not attempt to start modules that no longer exist
              not Code.ensure_loaded?(meta.module) ->
                Logger.warning(
                  "permanent durable server module #{inspect(meta.module)} not loaded"
                )

                :skip

              true ->
                # check module circuit breaker before proceeding
                case CircuitBreaker.check_module_circuit_breaker(
                       state.circuit_breaker,
                       meta.module
                     ) do
                  :ok ->
                    # proceed with existing health checks
                    if appears_restartable?(state, meta) do
                      obj
                    else
                      {:skip, meta}
                    end

                  {:circuit_open, cooldown_ms} ->
                    log(state, :debug, fn ->
                      "Module #{inspect(meta.module)} circuit breaker open for #{cooldown_ms}ms, skipping restart checks"
                    end)

                    {:skip, meta}
                end
            end

          {:error, {kind, _, _encoded}} when kind in [:error, :throw, :exit] ->
            # decode/parse failure — deterministic for same body, safe to cache
            log(state, :warning, fn ->
              "Failed to decode stored state for key #{key}: #{inspect(kind)}"
            end)

            :skip

          {:error, reason} ->
            # network/transient error — don't cache
            log(state, :warning, fn ->
              "Failed to get metadata for key #{key}: #{inspect(reason)}"
            end)

            nil
        end
    end
  end

  defp appears_restartable?(%LifecycleManager{} = state, %Meta{} = meta) do
    case check_server_health(state, meta) do
      :healthy ->
        false

      :orphaned ->
        # server is orphaned, check if this node should handle it
        orphan_claimable?(meta)
    end
  end

  defp orphan_claimable?(%Meta{} = meta) do
    # any orphaned server can be claimed by any node - this handles edge cases
    # where the assigned node is down/unreachable. Sticky placement preferences
    node_health = lookup_node_health(meta)

    # Node is unhealthy OR at capacity for this module
    node_unhealthy_or_full =
      node_health in [:stale, :unknown] or
        (match?({:healthy, _}, node_health) and
           not can_node_accept_module?(node_health, meta.module))

    # Get my env vars for sticky placement matching from supervisor config
    my_env_vars = get_my_env_vars(meta.supervisor)

    # Augment already-fetched sticky_placement with module config (e.g. :any added after process started).
    # We use meta.sticky_placement directly instead of doing another S3 GET — a failed GET would
    # return nil, causing find_my_matching_level(nil, _) to return 0, bypassing sticky placement entirely.
    augmented_sticky_placement =
      DurableServer.Supervisor.__augment_sticky_placement__(
        meta.supervisor,
        meta.module,
        meta.sticky_placement
      )

    # Find which sticky placement level I match (if any)
    my_matching_level = find_my_matching_level(augmented_sticky_placement, my_env_vars)

    # Get delays for this module
    delays = get_sticky_placement_delays(meta.supervisor, meta.module)

    # Server needs restart if:
    # - It crashed (status: :crashed)
    # - It was gracefully stopped while permanent (e.g., by Terminator during deploy)
    # - It has :running status but we're in orphan_claimable? (meaning the process died
    #   without updating storage - an abnormal crash that bypassed termination callbacks)
    # Non-permanent servers stay stopped.
    needs_restart =
      Meta.crashed?(meta) or
        (Meta.running?(meta) and meta.permanent) or
        (Meta.stopped_graceful?(meta) and meta.permanent)

    cond do
      # Crashed or gracefully stopped permanent servers: claim if we match a sticky level (respecting timing)
      needs_restart && my_matching_level != nil ->
        # We match some level, check if enough time has passed for our level
        can_claim_at_level?(meta, my_matching_level, delays)

      needs_restart && my_matching_level == nil ->
        # We don't match any sticky level. Since my_matching_level is nil, this means:
        # 1. There IS a sticky config (otherwise find_my_matching_level returns 0)
        # 2. :any is NOT in the config (otherwise we'd match :any)
        # 3. We don't match any specific env var
        # Therefore, this node can NEVER claim this orphan.
        false

      Meta.restart_attempt_expired?(meta) ->
        true

      node_unhealthy_or_full ->
        # Node is unhealthy/full, check sticky placement
        case my_matching_level do
          nil ->
            # We don't match any sticky level. Since my_matching_level is nil, this means:
            # 1. There IS a sticky config (otherwise find_my_matching_level returns 0)
            # 2. :any is NOT in the config (otherwise we'd match :any)
            # 3. We don't match any specific env var
            # Therefore, this node can NEVER claim this orphan.
            false

          level ->
            # We match a sticky level, check timing
            can_claim_reason = capacity_rejection_reason(node_health, meta.module)

            if can_claim_at_level?(meta, level, delays) do
              Logger.info("""
              Claiming orphan #{meta.key} from #{meta.node_str} (sticky level #{level})
              Reason: #{can_claim_reason}
              """)

              true
            else
              false
            end
        end

      true ->
        false
    end
  end

  # Find which level of sticky_placement I match (0-indexed), or nil if no match
  defp find_my_matching_level(nil, _my_env_vars), do: 0
  defp find_my_matching_level([], _my_env_vars), do: 0

  defp find_my_matching_level(sticky_placement, my_env_vars) when is_list(sticky_placement) do
    Enum.find_index(sticky_placement, fn preference ->
      case preference do
        %{env_var: :any, value: :any} ->
          true

        %{env_var: env_var, value: expected_value} ->
          Map.get(my_env_vars, env_var) == expected_value

        _ ->
          false
      end
    end)
  end

  # Check if enough time has passed for my level to claim
  defp can_claim_at_level?(meta, my_level, delays) do
    # Calculate cumulative delay for my level
    my_unlock_time = Enum.take(delays, my_level) |> Enum.sum()

    # Check if we've passed the unlock time
    !Meta.last_heartbeat_within_ms(meta, my_unlock_time)
  end

  # Get sticky placement delays for a module
  defp get_sticky_placement_delays(supervisor, module) do
    case DurableServer.Supervisor.__get_sticky_placement_for_module__(supervisor, module) do
      nil ->
        []

      list ->
        Enum.map(list, fn {_env_var_atom, delay} -> delay end)
    end
  end

  # Get my current env vars as a map, using the configured sticky placement env vars
  defp get_my_env_vars(supervisor_name) do
    env_var_names = DurableServer.Supervisor.collect_sticky_placement_env_vars(supervisor_name)

    env_var_names
    |> Enum.map(fn var_name -> {var_name, System.get_env(var_name)} end)
    |> Enum.into(%{})
  end

  defp discovery_skip?(%LifecycleManager{} = state, key, etag) do
    case :ets.lookup(state.discovery_skip_table, key) do
      [{^key, ^etag, :skip, _ts}] ->
        true

      [{^key, ^etag, %Meta{} = meta, _ts}] ->
        # Etag unchanged so stored state is identical, but time-dependent checks
        # (placement gates, circuit breaker, node health) may have changed.
        case CircuitBreaker.check_module_circuit_breaker(state.circuit_breaker, meta.module) do
          {:circuit_open, _} ->
            true

          :ok ->
            if appears_restartable?(state, meta) do
              # Now restartable — remove from cache, let async task do fresh GET
              :ets.delete(state.discovery_skip_table, key)
              false
            else
              true
            end
        end

      _ ->
        false
    end
  end

  # Strip fields not needed for re-evaluation to reduce ETS memory.
  # Drops sticky_placement_history (~2KB), crash_history, init_from_*, etc.
  defp trim_meta_for_cache(%Meta{} = meta) do
    %Meta{
      meta
      | sticky_placement_history: [],
        crash_history: [],
        init_from_ref: nil,
        init_from_pid: nil,
        restart_attempt_node: nil,
        prefix: nil
    }
  end

  defp try_restart_child(%LifecycleManager{} = state, {module, init_arg}) do
    if Code.ensure_loaded?(module) do
      # note: we must pass max_placement_retries: 0 to prevent remote placement
      # because lifecycle manager is only concered with its own local node
      DurableServer.Supervisor.start_child(
        state.supervisor_name,
        {module, init_arg},
        max_placement_retries: 0
      )
    else
      {:error, {:undef, module}}
    end
  end

  defp attempt_restart(
         %LifecycleManager{} = state,
         %StoredState{meta: %Meta{} = meta} = stored_state
       ) do
    case DurableServer.claim_restart_attempt(state.object_store, stored_state,
           ttl: @restart_claim_ttl_ms
         ) do
      {:ok, %{body: body, etag: etag}} ->
        module = meta.module

        case try_restart_child(
               state,
               {module, {:restart, %{key: meta.key, body: body, etag: etag}}}
             ) do
          {:ok, {_pid, _meta}} ->
            log(state, :info, fn ->
              "Successfully restarted DurableServer #{meta.key} on #{Node.self()}"
            end)

            # inc circuit breaker on successful restart (it's still a restart event)
            CircuitBreaker.increment_module_circuit_breaker(state.circuit_breaker, module)

          {:error, reason} ->
            log_level =
              case reason do
                {:already_started, _} -> :info
                _ -> :error
              end

            log(state, log_level, fn ->
              "Failed to restart DurableServer #{meta.key}: #{inspect(reason)}"
            end)

            CircuitBreaker.increment_module_circuit_breaker(state.circuit_breaker, module)

            DurableServer.clear_restart_attempt(state.object_store, %{
              key: stored_state.key,
              prefix: stored_state.prefix,
              body: body,
              etag: etag
            })
        end

      {:error, :already_claimed} ->
        # another node is handling this restart
        :ok

      {:error, :not_eligible} ->
        # server is not eligible for restart (still locked or permanently stopped)
        :ok

      {:error, reason} ->
        log(state, :warning, fn ->
          "Failed to claim restart for #{meta.key}: #{inspect(reason)}"
        end)

        :ok
    end
  end

  defp check_server_health(%LifecycleManager{} = state, %Meta{} = meta) do
    %{supervisor_name: supervisor_name} = state

    cond do
      # if it's stopped, no need to look up
      Meta.stopped_permanently?(meta) ->
        :healthy

      true ->
        # before taking slow path of checking locks via rpc, first see if server is alive in syn
        case Group.lookup(supervisor_name, meta.key, extract_meta: & &1) do
          {pid, registry_meta} when is_pid(pid) ->
            case registry_meta do
              %{node_ref: node_ref} ->
                if to_string(node(pid)) == meta.node_str and node_ref == meta.node_ref do
                  :healthy
                else
                  fetch_orphaned_slow_path(meta)
                end

              %{} ->
                fetch_orphaned_slow_path(meta)
            end

          nil ->
            fetch_orphaned_slow_path(meta)
        end
    end
  end

  defp fetch_orphaned_slow_path(%Meta{} = meta) do
    node_health = lookup_node_health(meta)

    cond do
      # check if the node appears down based on heartbeat cache
      node_health in [:stale, :unknown] ->
        :orphaned

      # node is healthy but can't accept this module (at capacity)
      match?({:healthy, _}, node_health) and
          not can_node_accept_module?(node_health, meta.module) ->
        :orphaned

      # check if the process lock has expired
      DurableServer.check_lock(meta) == :expired ->
        :orphaned

      # server explicitly marked as crashed
      Meta.crashed?(meta) ->
        :orphaned

      # previous restart attempt has expired
      Meta.restart_attempt_expired?(meta) ->
        :orphaned

      true ->
        :healthy
    end
  end

  @doc """
  Checks if the supervisor can accept a new child of the given module.

  Returns `:ok` if capacity is available, or
  `{:error, {:limit_reached, reason, details}}`
  if any limit is exceeded.

  This function is pure ETS/Group reads with no blocking operations.

  ## Options

    * `:bypass_disk_check` - when `true`, skips the `max_disk` limit check.
      Used for sticky restarts where the child's data already resides on
      the local disk, so rejecting based on disk usage would be counterproductive.
  """
  def check_capacity(supervisor_name, module, opts \\ []) do
    opts = Keyword.validate!(opts, [:bypass_disk_check])

    with :ok <- check_count_limits(supervisor_name, module),
         :ok <- check_resource_limits(supervisor_name, opts) do
      :ok
    end
  end

  defp check_count_limits(supervisor_name, module) do
    %{ets_table: table_name} =
      DurableServer.Supervisor.__get_config__(supervisor_name)

    [{:capacity_limits, limits}] = :ets.lookup(table_name, :capacity_limits)

    case limits[:max_children] do
      nil ->
        :ok

      max_children_limits ->
        global_count = Group.local_registry_count(supervisor_name)
        module_count = Group.local_member_count(supervisor_name, inspect(module))

        total_limit = max_children_limits[:total]
        module_limit = max_children_limits[module]

        cond do
          total_limit && global_count >= total_limit ->
            {:error,
             {:limit_reached, :max_children_total, %{current: global_count, limit: total_limit}}}

          module_limit && module_count >= module_limit ->
            {:error,
             {:limit_reached, :max_children_module,
              %{module: module, current: module_count, limit: module_limit}}}

          true ->
            :ok
        end
    end
  end

  defp check_resource_limits(supervisor_name, opts) do
    opts = Keyword.validate!(opts, [:bypass_disk_check])
    bypass_disk_check = Keyword.get(opts, :bypass_disk_check, false)

    %{ets_table: table_name} = DurableServer.Supervisor.__get_config__(supervisor_name)
    [{:capacity_limits, limits}] = :ets.lookup(table_name, :capacity_limits)

    max_cpu = limits[:max_cpu]
    max_memory = limits[:max_memory]
    max_disk = limits[:max_disk]

    if is_nil(max_cpu) and is_nil(max_memory) and is_nil(max_disk) do
      :ok
    else
      {cpu, memory, disk} =
        case :ets.lookup(table_name, :resource_metrics) do
          [{:resource_metrics, {cpu, memory, disk, _ts}}]
          when is_number(cpu) and is_number(memory) ->
            {cpu, memory, disk}

          _ ->
            {0.0, 0.0, nil}
        end

      cond do
        max_cpu && cpu >= max_cpu ->
          {:error, {:limit_reached, :max_cpu, %{current: cpu, limit: max_cpu}}}

        max_memory && memory >= max_memory ->
          {:error, {:limit_reached, :max_memory, %{current: memory, limit: max_memory}}}

        not bypass_disk_check && max_disk && disk && disk >= max_disk.percent ->
          {:error,
           {:limit_reached, :max_disk,
            %{current: disk, limit: max_disk.percent, mount_point: max_disk.mount_point}}}

        true ->
          :ok
      end
    end
  end

  defp update_resource_metrics(%LifecycleManager{supervisor_name: supervisor_name} = state) do
    cpu = calculate_cpu_percent()
    memory = calculate_memory_percent()
    timestamp = System.monotonic_time(:second)

    %{ets_table: table_name} = DurableServer.Supervisor.__get_config__(supervisor_name)
    [{:capacity_limits, limits}] = :ets.lookup(table_name, :capacity_limits)

    # Calculate disk if max_disk is configured
    disk =
      case limits[:max_disk] do
        %{mount_point: mount_point} -> calculate_disk_percent(mount_point)
        nil -> nil
      end

    :ets.insert(table_name, {:resource_metrics, {cpu, memory, disk, timestamp}})

    # log warnings if at 90% of limit
    check_warning_thresholds(supervisor_name, cpu, memory, disk, limits)

    state
  end

  defp check_warning_thresholds(supervisor_name, cpu, memory, disk, limits) do
    max_cpu = limits[:max_cpu]
    max_memory = limits[:max_memory]
    max_disk = limits[:max_disk]

    if max_cpu && cpu >= max_cpu * 0.9 do
      Logger.warning(
        "CPU usage high on #{Node.self()} for supervisor #{supervisor_name}: #{cpu}% (limit: #{max_cpu}%)"
      )
    end

    if max_memory && memory >= max_memory * 0.9 do
      Logger.warning(
        "Memory usage high on #{Node.self()} for supervisor #{supervisor_name}: #{memory}% (limit: #{max_memory}%)"
      )
    end

    if max_disk && disk && disk >= max_disk.percent * 0.9 do
      Logger.warning(
        "Disk usage high on #{Node.self()} for supervisor #{supervisor_name}: #{disk}% on #{max_disk.mount_point} (limit: #{max_disk.percent}%)"
      )
    end
  end

  defp calculate_cpu_percent do
    avg1 = :cpu_sup.avg1()
    num_cores = :erlang.system_info(:logical_processors_available)
    Float.round(avg1 / (num_cores * 256) * 100, 1)
  rescue
    error ->
      Logger.warning("Failed to get CPU usage: #{inspect(error)}")
      0.0
  end

  defp calculate_memory_percent do
    data = :memsup.get_system_memory_data()
    total = Keyword.fetch!(data, :total_memory)

    available =
      Keyword.get(data, :available_memory) ||
        Keyword.fetch!(data, :free_memory) + Keyword.get(data, :cached_memory, 0)

    used = total - available
    Float.round(used / total * 100, 1)
  rescue
    error ->
      Logger.warning("Failed to get memory usage: #{inspect(error)}")
      0.0
  end

  defp calculate_disk_percent(mount_point) do
    disk_data = :disksup.get_disk_data()

    # Find the matching mount point
    case Enum.find(disk_data, fn {mount, _size, _percent} ->
           to_string(mount) == mount_point
         end) do
      {_mount, _size, percent} ->
        # :disksup returns percent as integer already
        percent * 1.0

      nil ->
        # Mount point not found - treat as unconfigured (nil) rather than 0%
        Logger.warning(
          "Mount point #{mount_point} not found in disk data, max_disk limit disabled"
        )

        nil
    end
  rescue
    error ->
      Logger.warning("Failed to get disk usage: #{inspect(error)}, max_disk limit disabled")
      nil
  end

  defp calculate_resource_map(supervisor_name) do
    %{ets_table: table_name} = DurableServer.Supervisor.__get_config__(supervisor_name)

    {cpu, memory, disk} =
      case :ets.lookup(table_name, :resource_metrics) do
        [{:resource_metrics, {cpu, memory, disk, _ts}}] -> {cpu, memory, disk}
        [] -> {nil, nil, nil}
      end

    [{:capacity_limits, limits}] = :ets.lookup(table_name, :capacity_limits)
    max_cpu = limits[:max_cpu]
    max_memory = limits[:max_memory]
    max_disk = limits[:max_disk]

    # only include if at least one value is present
    # note: disk is only set if max_disk is configured AND mount point exists
    if cpu || memory || disk || max_cpu || max_memory do
      %{}
      |> maybe_put(:cpu, cpu)
      |> maybe_put(:max_cpu, max_cpu)
      |> maybe_put(:memory, memory)
      |> maybe_put(:max_memory, max_memory)
      |> maybe_put(:disk, disk)
      |> maybe_put(:max_disk, disk && max_disk && max_disk.percent)
    else
      nil
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp parse_capacity(nil), do: nil

  defp parse_capacity(capacity) when is_map(capacity) do
    Enum.into(capacity, %{}, fn
      {"total", value} -> {:total, parse_capacity_value(value)}
      {module_str, value} -> {String.to_existing_atom(module_str), parse_capacity_value(value)}
    end)
  rescue
    ArgumentError ->
      # module doesn't exist as an atom, return nil
      nil
  end

  defp parse_capacity_value(%{"current" => c, "limit" => l})
       when is_integer(c) and is_integer(l) do
    %{current: c, limit: l}
  end

  defp parse_capacity_value(_), do: nil

  defp parse_resources(nil), do: nil

  defp parse_resources(resources) when is_map(resources) do
    %{}
    |> maybe_put(:cpu, resources["cpu"])
    |> maybe_put(:max_cpu, resources["max_cpu"])
    |> maybe_put(:memory, resources["memory"])
    |> maybe_put(:max_memory, resources["max_memory"])
    |> then(fn map -> if map_size(map) > 0, do: map, else: nil end)
  end

  defp parse_heartbeat_meta(nil), do: nil
  defp parse_heartbeat_meta(%{} = heartbeat_meta), do: heartbeat_meta

  # Parses decoded heartbeat JSON into a structured tuple for ETS storage
  # Returns {:ok, {node_str, node_ref, timestamp, capacity, resources, env_vars, heartbeat_meta}}
  # or {:error, :invalid_format}
  defp parse_heartbeat_data(
         %{
           "node" => node_str,
           "node_ref" => node_ref,
           "last_heartbeat_at" => timestamp
         } = data
       ) do
    capacity = parse_capacity(data["capacity"])
    resources = parse_resources(data["resources"])
    env_vars = data["env_vars"] || %{}
    heartbeat_meta = parse_heartbeat_meta(data["heartbeat_meta"])

    {:ok, {node_str, node_ref, timestamp, capacity, resources, env_vars, heartbeat_meta}}
  end

  defp parse_heartbeat_data(_), do: {:error, :invalid_format}

  @doc """
  Finds nodes that can accept the given module, sorted by sticky placement preference and busyness.

  Returns a list of node atoms that have capacity for the module, sorted by:
  1. Sticky placement preference (most specific match first)
  2. Least busy nodes first

  Node busyness is calculated as the highest utilization ratio across all limits
  (global capacity, module capacity, CPU, memory). Nodes with lower utilization
  are prioritized for better load distribution.

  Always excludes the local node since we only lookup eligible remote nodes
  after local placement fails.

  ## Options

  - `:limit` - Maximum number of nodes to return (default: 3)
  - `:key` - The server key, used to load augmented sticky placement preferences

  ## Examples

      iex> find_eligible_nodes(MySupervisor, MyServer)
      [:node1@host, :node2@host]  # Sorted by sticky preference, then by least busy

      iex> find_eligible_nodes(MySupervisor, MyServer, limit: 5)
      [:node1@host, :node2@host, :node3@host]

  """
  def find_eligible_nodes(supervisor_name, module, opts \\ []) when is_atom(supervisor_name) do
    heartbeat_table = heartbeat_table_name(supervisor_name)
    # Handle case where table doesn't exist yet during startup
    case :ets.whereis(heartbeat_table) do
      :undefined ->
        []

      _tid ->
        limit = Keyword.get(opts, :limit, 3)
        key = Keyword.get(opts, :key)
        my_node = Node.self()

        # Get sticky placement - prefer passed in opts (already augmented), otherwise load and augment
        sticky_placement =
          cond do
            Keyword.has_key?(opts, :sticky_placement) ->
              Keyword.get(opts, :sticky_placement)

            key != nil ->
              # Load and augment persisted sticky placement for existing servers
              DurableServer.Supervisor.__get_augmented_sticky_placement__(
                supervisor_name,
                module,
                key
              )

            true ->
              # No key means this is for a new server - sticky placement doesn't apply yet
              # since we don't know what env vars it will have until it starts
              nil
          end

        now = System.system_time(:millisecond)

        # scan ETS cache for all live nodes
        :ets.tab2list(heartbeat_table)
        |> Enum.map(fn {node_str, node_ref, timestamp, capacity, resources, env_vars,
                        _heartbeat_meta} ->
          try do
            node = String.to_existing_atom(node_str)

            # Check if heartbeat is fresh enough to consider the node healthy
            # Use same threshold as orphan claiming to determine node staleness
            heartbeat_age_ms = now - timestamp

            health =
              if heartbeat_age_ms <= @node_health_staleness_threshold_ms do
                {:healthy, %{node_ref: node_ref, capacity: capacity, resources: resources}}
              else
                :stale
              end

            # Calculate sticky placement matching level for this node
            matching_level = find_matching_level(sticky_placement, env_vars)

            {node, health, matching_level, timestamp}
          rescue
            ArgumentError ->
              # String.to_existing_atom failed - node atom doesn't exist yet
              nil
          end
        end)
        |> Enum.reject(&is_nil/1)
        |> Enum.filter(fn {node, health, matching_level, _timestamp} ->
          # Always exclude local node - we only look up eligible nodes after local placement fails.
          # Pass matching_level so sticky nodes bypass disk check (child's data is on that node's disk).
          # When sticky_placement is configured, exclude nodes that don't match ANY level.
          # Without this, non-matching nodes (e.g. Singapore) are merely deprioritized (sort order 999)
          # but still eligible — and win when preferred nodes are busy. This is consistent with
          # orphan_claimable? which returns false for nil matching_level.
          node != my_node and
            can_node_accept_module?(health, module, matching_level: matching_level) and
            (sticky_placement in [nil, []] or matching_level != nil)
        end)
        # sort: prefer exact sticky placement matches first, then by least busy, then by most recent heartbeat
        |> Enum.sort_by(fn {_node, health, matching_level, timestamp} ->
          # Lower matching_level is better (0 = exact match, 1 = less specific, etc.)
          # nil means no match, treat as worst
          level_priority = if matching_level == nil, do: 999, else: matching_level
          busyness = calculate_node_busyness(health, module)
          # Negate timestamp so more recent (higher timestamp) comes first
          staleness = -timestamp
          {level_priority, busyness, staleness}
        end)
        |> Enum.take(limit)
        |> Enum.map(fn {node, _health, _matching_level, _timestamp} -> node end)
    end
  end

  # Find which sticky placement level matches the given env_vars from a node's heartbeat
  # Returns the matching level index (0 = exact match, 1 = less specific, etc.) or nil if no match
  defp find_matching_level(nil, _env_vars), do: nil
  defp find_matching_level([], _env_vars), do: nil

  defp find_matching_level(sticky_placement, env_vars) when is_list(sticky_placement) do
    Enum.find_index(sticky_placement, fn preference ->
      case preference do
        %{env_var: :any, value: :any} ->
          true

        %{env_var: env_var, value: expected_value} ->
          Map.get(env_vars, env_var) == expected_value

        _ ->
          false
      end
    end)
  end

  # calculate how "busy" a node is (0.0 = empty, 1.0 = full)
  # returns the highest utilization across all limits
  defp calculate_node_busyness({:healthy, info}, module) do
    ratios = []

    # check total capacity ratio
    ratios =
      case info[:capacity][:total] do
        %{current: current, limit: limit} when limit > 0 ->
          [current / limit | ratios]

        _ ->
          ratios
      end

    # check module-specific capacity ratio
    ratios =
      case info[:capacity][module] do
        %{current: current, limit: limit} when limit > 0 ->
          [current / limit | ratios]

        _ ->
          ratios
      end

    # check CPU ratio
    ratios =
      case {info[:resources][:cpu], info[:resources][:max_cpu]} do
        {cpu, max_cpu} when is_number(cpu) and is_number(max_cpu) and max_cpu > 0 ->
          [cpu / max_cpu | ratios]

        _ ->
          ratios
      end

    # check memory ratio
    ratios =
      case {info[:resources][:memory], info[:resources][:max_memory]} do
        {memory, max_memory}
        when is_number(memory) and is_number(max_memory) and max_memory > 0 ->
          [memory / max_memory | ratios]

        _ ->
          ratios
      end

    # return the maximum ratio (highest utilization)
    # if no ratios available, return 0.0 (least busy)
    case ratios do
      [] -> 0.0
      _ -> Enum.max(ratios)
    end
  end

  defp calculate_node_busyness(_, _), do: 1.0

  @doc """
  Checks if a node can accept a server for the given module based on capacity info.

  Returns true if:
  - Node has capacity for the module (count limits)
  - Node has resources available (CPU/memory under node's own thresholds)
  - Node has no capacity info (backwards compatibility)

  Returns false if:
  - Node is at global capacity
  - Node is at module-specific capacity
  - Node's CPU is at/above its max_cpu threshold
  - Node's memory is at/above its max_memory threshold
  - Node's disk is at/above its max_disk threshold (unless bypassed for sticky placement)

  ## Options

    * `:matching_level` - sticky placement matching level for this node. When `0`,
      the disk check is bypassed because the child's data is on this node's disk.
  """
  def can_node_accept_module?(node_health, module, opts \\ []) do
    opts = Keyword.validate!(opts, [:matching_level])
    matching_level = Keyword.get(opts, :matching_level)
    bypass_disk_check = matching_level == 0

    case node_health do
      {:healthy, %{capacity: nil, resources: nil}} ->
        # no capacity info (old heartbeat or no limits configured)
        true

      {:healthy, info} ->
        capacity_ok = check_capacity_ok(info[:capacity], module)
        resources_ok = check_resources_ok(info[:resources], bypass_disk_check: bypass_disk_check)

        capacity_ok and resources_ok

      _ ->
        # :stale, :unknown, or malformed
        false
    end
  end

  defp check_capacity_ok(nil, _module), do: true

  defp check_capacity_ok(capacity, module) when is_map(capacity) do
    # check total limit
    total_ok =
      case capacity[:total] do
        %{current: current, limit: limit} -> current < limit
        nil -> true
      end

    # check module-specific limit
    module_ok =
      case capacity[module] do
        %{current: current, limit: limit} -> current < limit
        nil -> true
      end

    total_ok and module_ok
  end

  defp check_resources_ok(nil, _opts), do: true

  defp check_resources_ok(resources, opts) when is_map(resources) do
    bypass_disk_check = Keyword.get(opts, :bypass_disk_check, false)

    # use the target node's own thresholds (they may differ from node-local ones)
    cpu_ok =
      case {resources[:cpu], resources[:max_cpu]} do
        {cpu, max_cpu} when is_number(cpu) and is_number(max_cpu) ->
          cpu < max_cpu

        {nil, _} ->
          true

        {_, nil} ->
          true
      end

    memory_ok =
      case {resources[:memory], resources[:max_memory]} do
        {memory, max_memory} when is_number(memory) and is_number(max_memory) ->
          memory < max_memory

        {nil, _} ->
          true

        {_, nil} ->
          true
      end

    disk_ok =
      if bypass_disk_check do
        true
      else
        case {resources[:disk], resources[:max_disk]} do
          {disk, max_disk} when is_number(disk) and is_number(max_disk) ->
            disk < max_disk

          {nil, _} ->
            true

          {_, nil} ->
            true
        end
      end

    cpu_ok and memory_ok and disk_ok
  end

  defp capacity_rejection_reason({:healthy, info}, module) do
    issues = []

    # Check total capacity
    issues =
      case info[:capacity][:total] do
        %{current: current, limit: limit} when current >= limit ->
          ["total: #{current}/#{limit}" | issues]

        _ ->
          issues
      end

    # Check module capacity
    issues =
      case info[:capacity][module] do
        %{current: current, limit: limit} when current >= limit ->
          ["#{inspect(module)}: #{current}/#{limit}" | issues]

        _ ->
          issues
      end

    # Check CPU
    issues =
      case info[:resources] do
        %{cpu: cpu, max_cpu: max_cpu} when cpu >= max_cpu ->
          ["CPU: #{cpu}% >= #{max_cpu}%" | issues]

        _ ->
          issues
      end

    # Check memory
    issues =
      case info[:resources] do
        %{memory: memory, max_memory: max_memory} when memory >= max_memory ->
          ["memory: #{memory}% >= #{max_memory}%" | issues]

        _ ->
          issues
      end

    case issues do
      [] -> "node at capacity (unknown reason)"
      _ -> "node at capacity: " <> Enum.join(issues, ", ")
    end
  end

  defp capacity_rejection_reason(:stale, _), do: "node heartbeat stale"
  defp capacity_rejection_reason(:unknown, _), do: "node heartbeat unknown"
end
