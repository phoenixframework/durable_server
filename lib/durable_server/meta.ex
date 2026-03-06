defmodule DurableServer.Meta do
  # represents the object metadata in storage
  alias DurableServer.Meta

  defstruct vsn: 1,
            module: nil,
            permanent: false,
            pid: nil,
            status: :stopped_graceful,
            key: nil,
            prefix: nil,
            sticky_placement: nil,
            sticky_placement_history: [],
            supervisor: nil,
            task_supervisor: nil,
            dynamic_supervisor: nil,
            node_ref: nil,
            node_str: nil,
            last_heartbeat_at: nil,
            crash_history: [],
            restart_attempt_node: nil,
            restart_attempt_time: nil,
            restart_attempt_ttl: nil,
            init_from_ref: nil,
            init_from_pid: nil

  @stopped_graceful :stopped_graceful
  @stopped_permanent :stopped_permanent
  @running :running
  @crashed :crashed
  @permanently_crashed :permanently_crashed
  @deleting :deleting

  @statuses [
    @stopped_graceful,
    @stopped_permanent,
    @running,
    @crashed,
    @permanently_crashed,
    @deleting
  ]

  # note we are using struct! so keys cannot be removed (but can be added)
  # if we remove keys we need to change the decode function to pluck out valid keys
  def decode_from_binary(meta_str, %{key: key, prefix: prefix}) when is_binary(meta_str) do
    meta_str
    |> Base.decode64!()
    |> :erlang.binary_to_term()
    |> from_storage_term(%{key: key, prefix: prefix})
  end

  def encode_to_binary(%Meta{} = meta) do
    meta
    |> to_storage_term()
    |> :erlang.term_to_binary()
    |> Base.encode64()
  end

  def from_storage_term(%Meta{} = meta, %{key: key, prefix: prefix}) do
    %{meta | key: key, prefix: prefix}
  end

  def from_storage_term(meta_map, %{key: key, prefix: prefix}) when is_map(meta_map) do
    valid_keys = Map.keys(%__MODULE__{})
    meta_map = Map.take(meta_map, valid_keys)

    unless Map.has_key?(meta_map, :status) do
      raise ArgumentError, "invalid meta storage term: #{inspect(meta_map)}"
    end

    %{struct!(Meta, meta_map) | key: key, prefix: prefix}
  end

  def to_storage_term(%Meta{} = meta) do
    meta
    |> Map.from_struct()
    |> Map.drop([:key, :prefix])
  end

  def running?(%Meta{} = meta) do
    meta.status == @running
  end

  def stopped_permanently?(%Meta{} = meta) do
    meta.status == @stopped_permanent
  end

  def currently_restarting?(%Meta{} = meta) do
    current_time = System.system_time(:millisecond)
    meta.restart_attempt_ttl && current_time < meta.restart_attempt_ttl
  end

  def last_heartbeat_within_ms(%Meta{} = meta, ms) do
    # check both the server's last heartbeat and the node's heartbeat timestamp
    # use whichever is more recent to avoid false-positive orphan claims
    current_time = System.system_time(:millisecond)
    node_timestamp = lookup_node_heartbeat_timestamp(meta)

    # use the most recent timestamp between server and node heartbeats
    most_recent_heartbeat =
      case {meta.last_heartbeat_at, node_timestamp} do
        {nil, nil} -> nil
        {server_ts, nil} -> server_ts
        {nil, node_ts} -> node_ts
        {server_ts, node_ts} -> max(server_ts, node_ts)
      end

    most_recent_heartbeat && current_time - most_recent_heartbeat < ms
  end

  # Lookup the node's heartbeat timestamp from the ETS cache
  # Returns {:ok, timestamp} if found with matching node_ref, or :not_found
  defp lookup_node_heartbeat_timestamp(%Meta{
         supervisor: supervisor,
         node_str: node_str,
         node_ref: expected_node_ref
       })
       when is_atom(supervisor) and is_binary(node_str) and is_integer(expected_node_ref) do
    table_name = :"durable_server_heartbeats_#{supervisor}"

    case :ets.lookup(table_name, node_str) do
      [{^node_str, ^expected_node_ref, timestamp, _region, _capacity, _resources, _env_vars}] ->
        # node found and node_ref matches - this is the current incarnation
        timestamp

      [{^node_str, _different_node_ref, _timestamp, _region, _capacity, _resources, _env_vars}] ->
        # node found but node_ref doesn't match - this is a different incarnation
        # don't use this stale data
        nil

      [] ->
        # node not found in ETS cache
        nil
    end
  end

  # Fallback for when node_ref is not an integer or fields are missing
  defp lookup_node_heartbeat_timestamp(_meta), do: nil

  def restart_attempt_expired?(%Meta{} = meta) do
    meta.restart_attempt_ttl && System.system_time(:millisecond) > meta.restart_attempt_ttl
  end

  def crashed?(%Meta{} = meta), do: meta.status == @crashed

  def stopped_graceful?(%Meta{} = meta), do: meta.status == @stopped_graceful

  def permanently_crashed?(%Meta{} = meta), do: meta.status == @permanently_crashed

  def put_status(%Meta{} = meta, status) when status in @statuses do
    %{meta | status: status}
  end

  def put_crash_history(%Meta{} = meta, history) when is_list(history) do
    %{meta | crash_history: history}
  end

  def clear_restart_attempt(%Meta{} = meta) do
    %{meta | restart_attempt_node: nil, restart_attempt_time: nil, restart_attempt_ttl: nil}
  end

  def put_restart_attempt(%Meta{} = meta, %{
        restart_attempt_node: node_str,
        ttl_ms: ttl_ms
      })
      when is_binary(node_str) and is_integer(ttl_ms) do
    current_time = System.system_time(:millisecond)

    %{
      meta
      | restart_attempt_node: to_string(node_str),
        restart_attempt_time: current_time,
        restart_attempt_ttl: current_time + ttl_ms
    }
  end
end
