defmodule DurableServer.Meta do
  # represents the object metadata in storage
  alias DurableServer.Meta

  defstruct vsn: 1,
            module: nil,
            lock_epoch: 0,
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
  @cordoned :cordoned

  @max_metadata_binary_bytes 65_536
  @max_metadata_base64_bytes div(@max_metadata_binary_bytes + 2, 3) * 4

  @statuses [
    @stopped_graceful,
    @stopped_permanent,
    @running,
    @crashed,
    @permanently_crashed,
    @deleting,
    @cordoned
  ]

  def decode_from_binary(meta_str, %{key: key, prefix: prefix}) when is_binary(meta_str) do
    with :ok <- validate_encoded_size(meta_str),
         {:ok, binary} <- decode_base64(meta_str),
         :ok <- validate_binary_format(binary),
         {:ok, term} <- decode_term(binary) do
      from_storage_term(term, %{key: key, prefix: prefix})
    else
      {:error, reason} ->
        raise ArgumentError, to_string(reason)
    end
  rescue
    error in ArgumentError ->
      raise ArgumentError, "invalid persisted metadata: #{Exception.message(error)}"
  end

  defp validate_encoded_size(meta_str) do
    if byte_size(meta_str) <= @max_metadata_base64_bytes do
      :ok
    else
      {:error, "encoded value exceeds #{@max_metadata_binary_bytes} byte limit"}
    end
  end

  defp decode_base64(meta_str) do
    case Base.decode64(meta_str) do
      {:ok, binary} -> {:ok, binary}
      :error -> {:error, "invalid base64"}
    end
  end

  # This encoder has never emitted compressed ETF. Rejecting it prevents a tiny
  # persisted value from expanding without a useful pre-decode size bound.
  defp validate_binary_format(<<131, 80, _compressed_size::32, _rest::binary>>),
    do: {:error, "compressed external terms are not accepted"}

  defp validate_binary_format(<<131, _rest::binary>> = binary) do
    if byte_size(binary) <= @max_metadata_binary_bytes do
      :ok
    else
      {:error, "decoded value exceeds #{@max_metadata_binary_bytes} byte limit"}
    end
  end

  defp validate_binary_format(_binary), do: {:error, "invalid external term"}

  # Persisted metadata contains native PIDs and references whose ETF includes
  # the originating node atom. A replacement VM may not have interned that old
  # node name yet, so `[:safe]` would make otherwise valid durable state
  # unreadable after a deploy. Decode the bounded, uncompressed term and then
  # enforce the metadata shape and allowed term types in `from_storage_term/2`.
  defp decode_term(binary) do
    {:ok, :erlang.binary_to_term(binary)}
  rescue
    _error -> {:error, "malformed external term"}
  end

  def encode_to_binary(%Meta{} = meta) do
    meta
    |> to_storage_term()
    |> :erlang.term_to_binary()
    |> Base.encode64()
  end

  def from_storage_term(%Meta{} = meta, context) do
    meta
    |> Map.from_struct()
    |> from_storage_term(context)
  end

  def from_storage_term(meta_map, %{key: key, prefix: prefix}) when is_map(meta_map) do
    unless safe_metadata_term?(meta_map) do
      raise ArgumentError, "metadata contains unsupported external terms"
    end

    valid_keys = Map.keys(Map.from_struct(%Meta{}))
    meta_map = Map.take(meta_map, valid_keys)
    validate_storage_term!(meta_map)
    %{struct!(Meta, meta_map) | key: key, prefix: prefix}
  end

  def from_storage_term(_term, _context) do
    raise ArgumentError, "invalid meta storage term"
  end

  defp validate_storage_term!(meta) do
    unless Map.has_key?(meta, :status) do
      raise ArgumentError, "metadata is missing required field :status"
    end

    validate_field!(meta, :vsn, &(is_integer(&1) and &1 > 0), "a positive integer")
    validate_field!(meta, :module, &is_atom/1, "an atom")

    validate_field!(
      meta,
      :lock_epoch,
      &(is_integer(&1) and &1 >= 0),
      "a non-negative integer"
    )

    validate_field!(meta, :permanent, &is_boolean/1, "a boolean")
    validate_field!(meta, :pid, &(is_nil(&1) or is_pid(&1)), "a pid or nil")
    validate_field!(meta, :status, &(&1 in @statuses), "a supported status")
    validate_field!(meta, :sticky_placement, &(is_nil(&1) or is_list(&1)), "a list or nil")
    validate_field!(meta, :sticky_placement_history, &is_list/1, "a list")
    validate_field!(meta, :supervisor, &(is_nil(&1) or is_atom(&1)), "an atom or nil")
    validate_field!(meta, :task_supervisor, &(is_nil(&1) or is_atom(&1)), "an atom or nil")
    validate_field!(meta, :dynamic_supervisor, &(is_nil(&1) or is_atom(&1)), "an atom or nil")

    validate_field!(
      meta,
      :node_ref,
      &(is_nil(&1) or is_integer(&1) or is_binary(&1)),
      "an integer, binary, or nil"
    )

    validate_field!(meta, :node_str, &(is_nil(&1) or is_binary(&1)), "a binary or nil")

    validate_field!(
      meta,
      :last_heartbeat_at,
      &(is_nil(&1) or is_integer(&1)),
      "an integer or nil"
    )

    validate_field!(meta, :crash_history, &is_list/1, "a list")

    validate_field!(
      meta,
      :restart_attempt_node,
      &(is_nil(&1) or is_binary(&1)),
      "a binary or nil"
    )

    validate_field!(
      meta,
      :restart_attempt_time,
      &(is_nil(&1) or is_integer(&1)),
      "an integer or nil"
    )

    validate_field!(
      meta,
      :restart_attempt_ttl,
      &(is_nil(&1) or is_integer(&1)),
      "an integer or nil"
    )

    validate_field!(
      meta,
      :init_from_ref,
      &(is_nil(&1) or is_reference(&1)),
      "a reference or nil"
    )

    validate_field!(
      meta,
      :init_from_pid,
      &(is_nil(&1) or is_pid(&1)),
      "a pid or nil"
    )

    :ok
  end

  defp validate_field!(meta, key, predicate, expected) do
    value = Map.get(meta, key, Map.fetch!(Map.from_struct(%Meta{}), key))

    unless predicate.(value) do
      raise ArgumentError, "invalid metadata field #{inspect(key)}: expected #{expected}"
    end
  end

  defp safe_metadata_term?(term)
       when is_atom(term) or is_binary(term) or is_number(term) or is_pid(term) or
              is_reference(term),
       do: true

  defp safe_metadata_term?(list) when is_list(list), do: Enum.all?(list, &safe_metadata_term?/1)

  defp safe_metadata_term?(tuple) when is_tuple(tuple) do
    tuple |> Tuple.to_list() |> Enum.all?(&safe_metadata_term?/1)
  end

  defp safe_metadata_term?(map) when is_map(map) do
    Enum.all?(map, fn {key, value} ->
      safe_metadata_term?(key) and safe_metadata_term?(value)
    end)
  end

  defp safe_metadata_term?(_term), do: false

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

  def deleting?(%Meta{} = meta), do: meta.status == @deleting

  def cordoned?(%Meta{} = meta), do: meta.status == @cordoned

  def permanently_crashed?(%Meta{} = meta), do: meta.status == @permanently_crashed

  def automatic_restart_disabled?(%Meta{permanent: false}), do: true

  def automatic_restart_disabled?(%Meta{permanent: true, status: status}) do
    status in [@deleting, @cordoned, @permanently_crashed, @stopped_permanent]
  end

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
