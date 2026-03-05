defmodule DurableServer.CircuitBreaker do
  @moduledoc false

  alias DurableServer.CircuitBreaker

  defstruct supervisor_name: nil, table_name: nil, config: nil, object_store: nil

  @type t :: %CircuitBreaker{
          supervisor_name: atom(),
          table_name: atom(),
          config: config()
        }

  @type config :: %{
          crash_threshold_count: non_neg_integer(),
          crash_threshold_window_ms: non_neg_integer(),
          module_circuit_breaker_count: non_neg_integer(),
          module_circuit_breaker_window_ms: non_neg_integer(),
          module_circuit_breaker_cooldown_ms: non_neg_integer(),
          global_lock_failure_count: non_neg_integer(),
          global_lock_failure_window_ms: non_neg_integer(),
          global_lock_failure_cooldown_ms: non_neg_integer()
        }

  @type crash_entry :: %{
          timestamp: integer(),
          reason: term(),
          node_ref: String.t()
        }

  @doc """
  Creates a new CircuitBreaker struct and initializes the ets table.

  This should only be called from `DurableServer.Supervisor.init/1`.
  The table is namespaced by supervisor name to allow multiple supervisors to coexist.
  """
  @spec new(atom(), config()) :: t()
  def new(supervisor_name, config) when is_atom(supervisor_name) do
    table_name = circuit_breaker_table_name(supervisor_name)
    {object_store, config} = Map.pop!(config, :object_store)

    case :ets.whereis(table_name) do
      :undefined ->
        :ets.new(table_name, [:set, :public, :named_table])

      _existing ->
        raise ArgumentError, "Circuit breaker ets table #{inspect(table_name)} already exists"
    end

    %CircuitBreaker{
      object_store: object_store,
      supervisor_name: supervisor_name,
      table_name: table_name,
      config: config
    }
  end

  @doc """
  Checks crash history and determines crash status for an object.

  Returns a tuple with the new status and updated crash history.
  The caller is responsible for updating the storage.
  """
  @spec check_object_crash_status(t(), map(), crash_entry()) ::
          {:crashed | :permanently_crashed, [crash_entry()]}
  def check_object_crash_status(%CircuitBreaker{config: config} = breaker, meta, crash_entry) do
    updated_history =
      add_crash_to_history(
        meta.crash_history,
        crash_entry,
        config
      )

    current_window_crashes = count_recent_crashes(breaker, updated_history)

    status =
      if current_window_crashes >= config.crash_threshold_count do
        :permanently_crashed
      else
        :crashed
      end

    {status, updated_history}
  end

  @doc """
  Checks if the module-wide circuit breaker allows operations.

  Returns `:ok` if operations are allowed, or `{:circuit_open, cooldown_ms}`
  if the circuit breaker is open.
  """
  @spec check_module_circuit_breaker(t(), module()) ::
          :ok | {:circuit_open, non_neg_integer()}
  def check_module_circuit_breaker(%CircuitBreaker{table_name: table, config: config}, module) do
    current_time = System.system_time(:millisecond)
    window_start = current_time - config.module_circuit_breaker_window_ms

    case :ets.lookup(table, module) do
      [{^module, _count, _last_reset, cooldown_until}] when current_time < cooldown_until ->
        {:circuit_open, cooldown_until - current_time}

      [{^module, _count, last_reset, _}] when last_reset < window_start ->
        # Reset window
        :ets.insert(table, {module, 0, current_time, 0})
        :ok

      [{^module, count, last_reset, _}] when count >= config.module_circuit_breaker_count ->
        # Open circuit breaker
        cooldown_until = current_time + config.module_circuit_breaker_cooldown_ms
        :ets.insert(table, {module, count, last_reset, cooldown_until})
        {:circuit_open, config.module_circuit_breaker_cooldown_ms}

      _ ->
        :ok
    end
  end

  @doc """
  Increments the module-wide circuit breaker counter.

  Called whenever a restart attempt is made (successful or not)
  to track the restart frequency for circuit breaker logic.
  """
  @spec increment_module_circuit_breaker(t(), module()) :: :ok
  def increment_module_circuit_breaker(%CircuitBreaker{table_name: table}, module) do
    current_time = System.system_time(:millisecond)

    # Use atomic update_counter to avoid race conditions
    try do
      :ets.update_counter(table, module, {2, 1})
    catch
      :error, :badarg ->
        # Key doesn't exist, insert initial entry and try again
        :ets.insert(table, {module, 0, current_time, 0})
        :ets.update_counter(table, module, {2, 1})
    end

    :ok
  end

  @doc """
  Checks if the global lock failure circuit breaker allows lock acquisition attempts.

  Returns `:ok` if lock acquisition attempts are allowed, or `{:circuit_open, cooldown_ms}`
  if too many lock failures have occurred recently (indicating network partition/flapping).
  """
  @spec check_global_lock_circuit_breaker(t()) ::
          :ok | {:circuit_open, non_neg_integer()}
  def check_global_lock_circuit_breaker(%CircuitBreaker{table_name: table, config: config}) do
    current_time = System.system_time(:millisecond)
    window_start = current_time - config.global_lock_failure_window_ms
    key = :global_lock_failures

    case :ets.lookup(table, key) do
      [{^key, _count, _last_reset, cooldown_until}] when current_time < cooldown_until ->
        {:circuit_open, cooldown_until - current_time}

      [{^key, _count, last_reset, _}] when last_reset < window_start ->
        # reset window
        :ets.insert(table, {key, 0, current_time, 0})
        :ok

      [{^key, count, last_reset, _}] when count >= config.global_lock_failure_count ->
        # open circuit breaker
        cooldown_until = current_time + config.global_lock_failure_cooldown_ms
        :ets.insert(table, {key, count, last_reset, cooldown_until})
        {:circuit_open, config.global_lock_failure_cooldown_ms}

      _ ->
        :ok
    end
  end

  @doc """
  Increments the global lock failure counter.

  Called whenever a lock acquisition attempt fails with {:already_started, pid},
  indicating another node owns the lock. During network partition/flapping,
  this prevents hammering object storage when we can't see remote nodes in group registry.
  """
  @spec increment_global_lock_failures(t()) :: :ok
  def increment_global_lock_failures(%CircuitBreaker{table_name: table}) do
    current_time = System.system_time(:millisecond)
    key = :global_lock_failures

    # use atomic update_counter to avoid race conditions
    try do
      :ets.update_counter(table, key, {2, 1})
    catch
      :error, :badarg ->
        # key doesn't exist, insert initial entry and try again
        :ets.insert(table, {key, 0, current_time, 0})
        :ets.update_counter(table, key, {2, 1})
    end

    :ok
  end

  @doc """
  Checks if remote placement attempts to `node_str` are currently rate-limited.

  Returns `:ok` when placement attempts are allowed, or
  `{:circuit_open, cooldown_ms}` when the node is in timeout cooldown.
  """
  @spec check_placement_node_timeout_circuit_breaker(t(), String.t()) ::
          :ok | {:circuit_open, non_neg_integer()}
  def check_placement_node_timeout_circuit_breaker(
        %CircuitBreaker{table_name: table},
        node_str
      )
      when is_binary(node_str) do
    key = {:placement_node_timeout, node_str}
    current_time = System.system_time(:millisecond)

    case :ets.lookup(table, key) do
      [{^key, _count, _last_reset, cooldown_until}] when current_time < cooldown_until ->
        {:circuit_open, cooldown_until - current_time}

      [{^key, _count, _last_reset, _cooldown_until}] ->
        # cleanup expired cooldown entries aggressively to keep table small
        :ets.delete(table, key)
        :ok

      _ ->
        :ok
    end
  end

  @doc """
  Opens a timeout cooldown for remote placement attempts to `node_str`.

  This is used to avoid repeatedly hammering nodes that are timing out during
  rolling deploys or transient network events.
  """
  @spec trip_placement_node_timeout_circuit_breaker(t(), String.t(), non_neg_integer()) :: :ok
  def trip_placement_node_timeout_circuit_breaker(
        %CircuitBreaker{table_name: table},
        node_str,
        cooldown_ms
      )
      when is_binary(node_str) and is_integer(cooldown_ms) and cooldown_ms > 0 do
    key = {:placement_node_timeout, node_str}
    current_time = System.system_time(:millisecond)
    cooldown_until = current_time + cooldown_ms

    :ets.insert(table, {key, 1, current_time, cooldown_until})
    :ok
  end

  @doc """
  Prunes stale entries from the circuit breaker ets table.

  Called periodically by the LifecycleManager to clean up old entries
  that are outside their respective time windows.
  """
  @spec prune_stale_entries(t()) :: :ok
  def prune_stale_entries(%CircuitBreaker{table_name: table, config: config}) do
    current_time = System.system_time(:millisecond)
    window_start = current_time - config.module_circuit_breaker_window_ms

    # Use select_delete to atomically remove stale entries
    # Delete entries where last_reset is older than window_start AND cooldown_until is not active
    match_spec = [
      {{:"$1", :"$2", :"$3", :"$4"},
       [{:and, {:<, :"$3", window_start}, {:"=<", :"$4", current_time}}], [true]}
    ]

    :ets.select_delete(table, match_spec)
    :ok
  end

  # Private functions

  @spec circuit_breaker_table_name(atom()) :: atom()
  defp circuit_breaker_table_name(supervisor_name) do
    :"circuit_breaker_#{supervisor_name}"
  end

  @spec add_crash_to_history([crash_entry()], crash_entry(), config()) :: [crash_entry()]
  defp add_crash_to_history(history, crash_entry, config) do
    current_time = crash_entry.timestamp
    window_start = current_time - config.crash_threshold_window_ms

    [crash_entry | history]
    |> Enum.filter(fn %{timestamp: ts} -> ts > window_start end)
    # Limit history size
    |> Enum.take(config.crash_threshold_count)
  end

  @spec count_recent_crashes(t(), [crash_entry()]) :: non_neg_integer()
  defp count_recent_crashes(%CircuitBreaker{} = breaker, crash_history) do
    current_time = System.system_time(:millisecond)
    window_start = current_time - breaker.config.crash_threshold_window_ms

    Enum.count(crash_history, fn %{timestamp: ts} -> ts > window_start end)
  end
end
