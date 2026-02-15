defmodule Group.Replica do
  @moduledoc false

  use GenServer

  _archdoc = ~S"""
  Sharded GenServer: peer discovery, replication, monitoring, conflict resolution.

  One per shard. Registered as :"#{name}_replica_#{shard_index}".

  ## Message Protocol

  | Message                                                    | Direction      | Purpose                          |
  |------------------------------------------------------------|----------------|----------------------------------|
  | `{:peer_connect, pid, shard, num_shards, clusters}`        | A→B (per-shard)| Establish peer relationship      |
  | `{:peer_connect_ack, pid, shard, num_shards, clusters}`    | B→A (per-shard)| Acknowledge peer                 |
  | `{:cluster_state, cluster, reg_data, pg_data}`             | both           | Per-cluster data snapshot        |
  | `{:replicate_register, cluster, key, pid, meta, time, reason}` | broadcast  | Propagate registration           |
  | `{:replicate_unregister, cluster, key, pid, meta, reason}` | broadcast      | Propagate unregistration         |
  | `{:replicate_join, cluster, key, pid, meta, time, reason}` | broadcast      | Propagate join                   |
  | `{:replicate_leave, cluster, key, pid, meta, reason}`      | broadcast      | Propagate leave                  |
  | `{:cluster_connect, clusters, pid}`                        | S→remote S     | Node joining named clusters      |
  | `{:cluster_connect_ack, clusters, pid}`                    | S→remote S     | Acknowledge cluster join + data  |
  | `{:cluster_disconnect, clusters, pid}`                     | shard 0→remote | Node leaving named clusters      |
  | `{:send_cluster_data, clusters, target_node}`              | local fan-out  | Notify siblings: send shard data |

  ## Protocol Flows

  ### Peer Discovery (nodeup or init)

  Both sides exchange cluster lists via peer_connect/peer_connect_ack, then send
  per-cluster cluster_state messages only for shared clusters. The nil cluster is
  always shared (all Group peers are in it). Peer discovery is per-shard — each
  shard independently discovers its remote counterpart.

  When `peer_connect_ack` discovers named shared clusters (i.e. both sides are
  already in the same named cluster), it sends a `cluster_connect` message to
  the remote to trigger the full fan-out data exchange. This handles the race
  where `connect/2` ran between peer_connect send and ack processing — see
  "Peer Discovery + Connect Race" below.

  ### Named Cluster Join (single shard notification + fan-out)

  1. Caller calls `Group.connect/2` which picks a random shard S and sends a
     single GenServer.call to shard S.
  2. Shard S broadcasts `cluster_connect` to remote shard S on all peers.
  3. Remote shard S adds membership to ETS, sends `cluster_connect_ack` + its
     shard S `cluster_state`, and fans out `{:send_cluster_data, clusters, node}`
     to all local sibling shards.
  4. Each sibling shard sends its own `cluster_state` to the matching remote shard.
  5. On receiving `cluster_connect_ack`, the initiator's shard S adds membership
     to ETS, sends its shard S `cluster_state`, and fans out to siblings.

  Randomizing the notification shard load-balances across shards when many
  concurrent `connect` calls happen (e.g., 10,000 clusters). This reduces
  cross-node messages from N² to 2N (one cluster_state per shard per direction)
  and eliminates redundant ETS membership inserts.

  ### Named Cluster Disconnect (shard 0 + fan-out)

  1. Caller calls `Group.disconnect/2` which calls all N local shards to purge
     their own entries, but only shard 0 broadcasts `cluster_disconnect` to remote
     shard 0s. (Disconnect still uses all shards since each must purge its own
     ETS entries.)
  2. Remote shard 0 removes membership from ETS and fans out to siblings.
  3. Each shard (0 and siblings) purges entries for the disconnected cluster+node.

  ### Peer Discovery + Connect Race

  `connect/2` picks a random shard S for its GenServer.call. If shard S hasn't
  processed its `peer_connect_ack` yet (remote_shards is empty),
  `broadcast_to_peers` silently sends to nobody and the cluster_connect is lost.

  The `peer_connect_ack` handler compensates: when it discovers named shared
  clusters (both sides already connected), it sends `cluster_connect` to the
  remote, triggering the standard ack + data + fan-out flow. This only fires
  in the race case — normally the ack is processed before `connect/2` is called,
  so `my_clusters` contains only nil and no compensation is needed.

  ### Steady-State Replication

  Operations broadcast replicate_* messages to cluster members. nil cluster uses
  remote_shards keys; named clusters use cluster_nodes ETS.

  ## Cluster Membership Tracking

  The nil cluster is tracked in ETS (cluster_nodes table), maintained by the
  peer_connect protocol. Nodes are added on peer discovery and removed on
  nodedown/shard death. This allows Group.nodes/1 to return actual Group peers
  rather than all Erlang nodes.

  ## Sharding

  Each key is routed to a shard via `:erlang.phash2({cluster, key}, num_shards)`.
  Including `cluster` in the hash input means the same key string in different
  clusters may land on different shards — this is intentional so named-cluster
  operations don't create false contention with nil-cluster operations.

  `phash2` produces near-uniform distribution across shards for diverse keyspaces.
  With 10K distinct keys across 2–8 shards, observed deviation from perfect
  uniformity is <2%. In practice, real workloads with varied key prefixes will
  see balanced shard load.

  **Hot keys":** A single extremely popular key (e.g. a chat room
  with thousands of joins/leaves) always hashes to one shard, so all *writes*
  for that key serialize through that shard's GenServer. However, *reads* —
  `Group.lookup/3` and `Group.members/3` — go directly to ETS and bypass the
  GenServer entirely. Since reads typically dominate, a hot key's impact on
  overall throughput is limited to write-heavy scenarios. Adding more shards
  does not help a single hot key (it still lands on one shard), but it does
  reduce contention between unrelated keys.

  Shard counts must match across all nodes in a cluster. The peer_connect
  handshake validates `num_shards` and raises on mismatch, since a disagreement
  would route the same key to different shards on different nodes, breaking
  replication consistency.
  """

  require Logger

  alias Group.Replica.Data

  defstruct [
    :name,
    :shard_index,
    :num_shards,
    remote_shards: %{},
    monitors: %{}
  ]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    shard_index = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, opts, name: shard_name(name, shard_index))
  end

  def shard_name(name, shard_index), do: :"#{name}_replica_#{shard_index}"

  def shard_for(name, cluster, key) do
    num_shards = Group.get_config(name).num_shards
    index = :erlang.phash2({cluster, key}, num_shards)
    shard_name(name, index)
  end

  def shard_index_for(cluster, key, num_shards) do
    :erlang.phash2({cluster, key}, num_shards)
  end

  # =====================================================================
  # GenServer callbacks
  # =====================================================================

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    shard_index = Keyword.fetch!(opts, :shard_index)
    num_shards = Keyword.fetch!(opts, :num_shards)

    :net_kernel.monitor_nodes(true)

    # Register self as nil cluster member
    Data.add_cluster_node(name, nil, node())

    state = %__MODULE__{
      name: name,
      shard_index: shard_index,
      num_shards: num_shards
    }

    # Rebuild monitors from any surviving ETS data (after shard crash/restart)
    state = rebuild_monitors(state)

    log_once(state, fn -> "#{log_prefix(state)} started (shards=#{num_shards})" end)

    # Discover peers on all known nodes
    registered_name = shard_name(name, shard_index)

    for remote_node <- Node.list() do
      send(
        {registered_name, remote_node},
        {:peer_connect, self(), shard_index, num_shards, Data.my_clusters(name)}
      )
    end

    {:ok, state}
  end

  # =====================================================================
  # Registration calls
  # =====================================================================

  @impl true
  def handle_call({:register, cluster, key, pid, meta}, _from, state) do
    %{name: name, shard_index: shard} = state

    case Data.registry_lookup(name, shard, cluster, key) do
      nil ->
        time = System.system_time()
        mref = monitor_pid(state, pid)
        Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_register, cluster, key, pid, meta, time, :register}
        )

        Group.__dispatch__(name, :registered, key, pid, meta, %{
          previous_meta: nil,
          cluster: cluster
        })

        {:reply, :ok, put_monitor(state, pid, mref)}

      {^pid, old_meta, _time, _node} ->
        # Same pid re-registering — update metadata
        time = System.system_time()
        Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_register, cluster, key, pid, meta, time, :update}
        )

        Group.__dispatch__(name, :registered, key, pid, meta, %{
          previous_meta: old_meta,
          cluster: cluster
        })

        {:reply, :ok, state}

      _other ->
        {:reply, {:error, :taken}, state}
    end
  end

  def handle_call({:unregister, cluster, key}, _from, state) do
    %{name: name, shard_index: shard} = state

    case Data.registry_lookup(name, shard, cluster, key) do
      {pid, meta, _time, entry_node} when entry_node == node() ->
        Data.registry_delete(name, shard, cluster, key, pid)
        state = maybe_demonitor_pid(state, name, shard, pid)

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_unregister, cluster, key, pid, meta, :unregister}
        )

        Group.__dispatch__(name, :unregistered, key, pid, meta, %{
          reason: :unregister,
          cluster: cluster
        })

        {:reply, :ok, state}

      nil ->
        {:reply, {:error, :undefined}, state}

      {_pid, _meta, _time, _other_node} ->
        {:reply, {:error, :not_owner}, state}
    end
  end

  # =====================================================================
  # Process group calls
  # =====================================================================

  def handle_call({:join, cluster, key, pid, meta}, _from, state) do
    %{name: name, shard_index: shard} = state

    case Data.pg_lookup(name, shard, cluster, key, pid) do
      nil ->
        time = System.system_time()
        mref = monitor_pid(state, pid)
        Data.pg_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_join, cluster, key, pid, meta, time, :join}
        )

        Group.__dispatch__(name, :joined, key, pid, meta, %{
          previous_meta: nil,
          cluster: cluster
        })

        {:reply, :ok, put_monitor(state, pid, mref)}

      {old_meta, _time, _node} ->
        # Re-join with updated metadata
        time = System.system_time()
        Data.pg_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_join, cluster, key, pid, meta, time, :update}
        )

        Group.__dispatch__(name, :joined, key, pid, meta, %{
          previous_meta: old_meta,
          cluster: cluster
        })

        {:reply, :ok, state}
    end
  end

  def handle_call({:leave, cluster, key, pid}, _from, state) do
    %{name: name, shard_index: shard} = state

    case Data.pg_lookup(name, shard, cluster, key, pid) do
      nil ->
        {:reply, {:error, :not_in_group}, state}

      {meta, _time, _node} ->
        Data.pg_delete(name, shard, cluster, key, pid)
        state = maybe_demonitor_pid(state, name, shard, pid)

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_leave, cluster, key, pid, meta, :leave}
        )

        Group.__dispatch__(name, :left, key, pid, meta, %{
          reason: :leave,
          cluster: cluster
        })

        {:reply, :ok, state}
    end
  end

  # =====================================================================
  # Cluster connect/disconnect (broadcast to all shards, rare operation)
  # =====================================================================

  def handle_call({:cluster_connect, clusters}, _from, state) do
    log_once(state, fn ->
      "#{log_prefix(state)} cluster_connect (#{length(clusters)} clusters)"
    end)

    broadcast_to_peers(state, {:cluster_connect, clusters, self()})

    {:reply, :ok, state}
  end

  def handle_call({:cluster_disconnect, clusters}, _from, state) do
    %{name: name, shard_index: shard} = state

    log_once(state, fn ->
      "#{log_prefix(state)} cluster_disconnect (#{length(clusters)} clusters)"
    end)

    for cluster <- clusters do
      {purged_reg, purged_pg} = purge_cluster_entries(name, shard, cluster, node())
      dispatch_purged(state, cluster, purged_reg, purged_pg, :cluster_disconnect)
    end

    if shard == 0 do
      broadcast_to_peers(state, {:cluster_disconnect, clusters, self()})
    end

    {:reply, :ok, state}
  end

  # =====================================================================
  # Replication receive (handle_info)
  # =====================================================================

  @impl true
  def handle_info({:replicate_register, cluster, key, pid, meta, time, _reason}, state) do
    %{name: name, shard_index: shard} = state

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} replicate_register key=#{inspect(key)} from #{node(pid)}"
    end)

    case Data.registry_lookup(name, shard, cluster, key) do
      nil ->
        Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        Group.__dispatch__(name, :registered, key, pid, meta, %{
          previous_meta: nil,
          cluster: cluster
        })

        {:noreply, state}

      {^pid, old_meta, _old_time, _node} ->
        # Same pid updating
        Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        Group.__dispatch__(name, :registered, key, pid, meta, %{
          previous_meta: old_meta,
          cluster: cluster
        })

        {:noreply, state}

      {existing_pid, existing_meta, existing_time, existing_node}
      when existing_node == node() ->
        # Conflict: incoming remote registration vs local entry
        state =
          resolve_conflict(
            state,
            cluster,
            key,
            {existing_pid, existing_meta, existing_time},
            {pid, meta, time}
          )

        {:noreply, state}

      {_existing_pid, _existing_meta, existing_time, _existing_node} ->
        # Both remote — keep the more recent one
        if time > existing_time do
          Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))

          Group.__dispatch__(name, :registered, key, pid, meta, %{
            previous_meta: nil,
            cluster: cluster
          })

          {:noreply, state}
        else
          {:noreply, state}
        end
    end
  end

  def handle_info({:replicate_unregister, cluster, key, pid, meta, reason}, state) do
    %{name: name, shard_index: shard} = state

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} replicate_unregister key=#{inspect(key)}"
    end)

    case Data.registry_lookup(name, shard, cluster, key) do
      {^pid, _meta, _time, _node} ->
        Data.registry_delete(name, shard, cluster, key, pid)

        Group.__dispatch__(name, :unregistered, key, pid, meta, %{
          reason: reason,
          cluster: cluster
        })

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:replicate_join, cluster, key, pid, meta, time, reason}, state) do
    %{name: name, shard_index: shard} = state

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} replicate_join key=#{inspect(key)} pid=#{inspect(pid)} from #{node(pid)}"
    end)

    case Data.pg_lookup(name, shard, cluster, key, pid) do
      nil ->
        Data.pg_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        Group.__dispatch__(name, :joined, key, pid, meta, %{
          previous_meta: nil,
          cluster: cluster
        })

        {:noreply, state}

      {old_meta, _old_time, _node} ->
        Data.pg_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        previous_meta = if reason == :update, do: old_meta, else: nil

        Group.__dispatch__(name, :joined, key, pid, meta, %{
          previous_meta: previous_meta,
          cluster: cluster
        })

        {:noreply, state}
    end
  end

  def handle_info({:replicate_leave, cluster, key, pid, meta, reason}, state) do
    %{name: name, shard_index: shard} = state

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} replicate_leave key=#{inspect(key)}"
    end)

    case Data.pg_lookup(name, shard, cluster, key, pid) do
      nil ->
        {:noreply, state}

      {_meta, _time, _node} ->
        Data.pg_delete(name, shard, cluster, key, pid)

        Group.__dispatch__(name, :left, key, pid, meta, %{
          reason: reason,
          cluster: cluster
        })

        {:noreply, state}
    end
  end

  # =====================================================================
  # Peer discovery protocol
  # =====================================================================

  def handle_info(
        {:peer_connect, remote_pid, remote_shard_index, remote_num_shards, remote_clusters},
        state
      )
      when remote_shard_index == state.shard_index do
    if remote_num_shards != state.num_shards do
      raise "Group shard count mismatch: local=#{state.num_shards} remote=#{remote_num_shards} from #{node(remote_pid)}"
    end

    %{name: name, shard_index: shard} = state
    remote_node = node(remote_pid)

    # Add remote node to nil cluster (all Group peers are in nil)
    Data.add_cluster_node(name, nil, remote_node)

    # Compute shared clusters
    my_clusters = Data.my_clusters(name)
    shared = compute_shared_clusters(my_clusters, remote_clusters)

    # Add remote node to shared named clusters
    for cluster <- shared, cluster != nil do
      Data.add_cluster_node(name, cluster, remote_node)
    end

    already_known = Map.has_key?(state.remote_shards, remote_node)

    state =
      if already_known do
        state
      else
        Process.monitor(remote_pid)
        %{state | remote_shards: Map.put(state.remote_shards, remote_node, remote_pid)}
      end

    # Send ack with our cluster list
    send_to_peer(
      state,
      remote_node,
      {:peer_connect_ack, self(), shard, state.num_shards, my_clusters}
    )

    log_once(state, fn ->
      "#{log_prefix(state)} peer_connect from #{remote_node} (#{length(shared)} shared clusters)"
    end)

    # Send cluster_state for all shared clusters in one pass (single table scan
    # instead of one scan per cluster — O(N) vs O(C×N))
    send_cluster_states(state, shared, remote_node)

    {:noreply, state}
  end

  def handle_info({:peer_connect, _remote_pid, _other_shard, _num_shards, _clusters}, state) do
    # Wrong shard index, ignore
    {:noreply, state}
  end

  def handle_info(
        {:peer_connect_ack, remote_pid, remote_shard_index, remote_num_shards, remote_clusters},
        state
      )
      when remote_shard_index == state.shard_index do
    if remote_num_shards != state.num_shards do
      raise "Group shard count mismatch: local=#{state.num_shards} remote=#{remote_num_shards} from #{node(remote_pid)}"
    end

    %{name: name} = state
    remote_node = node(remote_pid)

    # Add remote node to nil cluster
    Data.add_cluster_node(name, nil, remote_node)

    # Compute shared clusters
    my_clusters = Data.my_clusters(name)
    shared = compute_shared_clusters(my_clusters, remote_clusters)

    # Add remote node to shared named clusters
    for cluster <- shared, cluster != nil do
      Data.add_cluster_node(name, cluster, remote_node)
    end

    already_known = Map.has_key?(state.remote_shards, remote_node)

    state =
      if already_known do
        state
      else
        Process.monitor(remote_pid)
        %{state | remote_shards: Map.put(state.remote_shards, remote_node, remote_pid)}
      end

    log_once(state, fn ->
      "#{log_prefix(state)} peer_connect_ack from #{remote_node} (#{length(shared)} shared clusters)"
    end)

    # Send cluster_state for all shared clusters in one pass
    send_cluster_states(state, shared, remote_node)

    # If named clusters are shared, trigger the full cluster_connect protocol
    # on the remote so it does ack + data + fan-out. This compensates for the
    # case where our connect/2 broadcast was lost (remote_shards was empty at
    # the time because this ack hadn't been processed yet).
    named_shared = Enum.filter(shared, &(&1 != nil))

    if named_shared != [] do
      send_to_peer(state, remote_node, {:cluster_connect, named_shared, self()})
    end

    {:noreply, state}
  end

  def handle_info({:peer_connect_ack, _remote_pid, _other_shard, _num_shards, _clusters}, state) do
    {:noreply, state}
  end

  # =====================================================================
  # Cluster state (unified handler for peer discovery + cluster join)
  # =====================================================================

  def handle_info({:cluster_state, cluster, reg_data, pg_data}, state) do
    %{name: name} = state

    # Guard: skip merge for named clusters we're not a member of
    if cluster_member?(name, cluster) do
      log_once(state, fn ->
        "#{log_prefix(state)} cluster_state cluster=#{inspect(cluster)} (#{length(reg_data)} reg, #{length(pg_data)} pg entries)"
      end)

      log_verbose(state, fn ->
        "#{log_prefix_shard(state)} merging cluster=#{inspect(cluster)} (#{length(reg_data)} reg, #{length(pg_data)} pg entries)"
      end)

      state = merge_remote_cluster_data(state, cluster, reg_data, pg_data)
      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  # =====================================================================
  # Cluster connect/disconnect from remote
  # =====================================================================

  def handle_info({:cluster_connect, clusters, remote_pid}, state) do
    %{name: name} = state
    remote_node = node(remote_pid)

    shared =
      Enum.filter(clusters, fn c ->
        node() in Data.cluster_nodes(name, c)
      end)

    log_once(state, fn ->
      "#{log_prefix(state)} #{remote_node} cluster_connect (#{length(shared)}/#{length(clusters)} shared)"
    end)

    for cluster <- shared do
      Data.add_cluster_node(name, cluster, remote_node)
    end

    if shared != [] do
      send_to_peer(state, remote_node, {:cluster_connect_ack, shared, self()})
      send_cluster_states(state, shared, remote_node)
      fan_out_to_siblings(state, {:send_cluster_data, shared, remote_node})
    end

    {:noreply, state}
  end

  def handle_info({:cluster_connect_ack, clusters, remote_pid}, state) do
    %{name: name} = state
    remote_node = node(remote_pid)

    for cluster <- clusters do
      Data.add_cluster_node(name, cluster, remote_node)
    end

    send_cluster_states(state, clusters, remote_node)
    fan_out_to_siblings(state, {:send_cluster_data, clusters, remote_node})

    {:noreply, state}
  end

  def handle_info({:cluster_disconnect, clusters, remote_pid}, state) do
    %{name: name, shard_index: shard} = state
    remote_node = node(remote_pid)

    log_once(state, fn ->
      "#{log_prefix(state)} #{remote_node} cluster_disconnect (#{length(clusters)} clusters)"
    end)

    if shard == 0 do
      for cluster <- clusters do
        Data.remove_cluster_node(name, cluster, remote_node)
      end

      fan_out_to_siblings(state, {:cluster_disconnect, clusters, remote_pid})
    end

    for cluster <- clusters do
      {purged_reg, purged_pg} = purge_cluster_entries(name, shard, cluster, remote_node)
      dispatch_purged(state, cluster, purged_reg, purged_pg, :cluster_disconnect)
    end

    {:noreply, state}
  end

  # =====================================================================
  # Node up/down
  # =====================================================================

  def handle_info({:nodeup, remote_node}, state) do
    %{shard_index: shard, name: name} = state
    shard_registered_name = shard_name(name, shard)

    send(
      {shard_registered_name, remote_node},
      {:peer_connect, self(), shard, state.num_shards, Data.my_clusters(name)}
    )

    {:noreply, state}
  end

  def handle_info({:nodedown, dead_node}, state) do
    %{name: name, shard_index: shard} = state

    # Remove cluster memberships once (shared table, same work on every shard)
    if shard == 0, do: Data.purge_cluster_node(name, dead_node)

    # Purge all data from the dead node
    {purged_reg, purged_pg} = Data.purge_node(name, shard, dead_node)

    log_once(state, fn ->
      "#{log_prefix(state)} nodedown #{dead_node} (purged #{length(purged_reg)} reg, #{length(purged_pg)} pg entries)"
    end)

    # Fire events for purged entries
    for {cluster, key, pid, meta, _time} <- purged_reg do
      Group.__dispatch__(name, :unregistered, key, pid, meta, %{
        reason: :nodedown,
        cluster: cluster
      })
    end

    for {cluster, key, pid, meta, _time} <- purged_pg do
      Group.__dispatch__(name, :left, key, pid, meta, %{
        reason: :nodedown,
        cluster: cluster
      })
    end

    state = %{state | remote_shards: Map.delete(state.remote_shards, dead_node)}
    {:noreply, state}
  end

  # =====================================================================
  # Process DOWN
  # =====================================================================

  def handle_info({:DOWN, _mref, :process, pid, reason}, state) do
    %{name: name, shard_index: shard, num_shards: num_shards} = state

    remote_node = node(pid)

    if remote_node != node() and Map.get(state.remote_shards, remote_node) == pid do
      # Remote shard process died — purge its cluster memberships and node data
      if shard == 0, do: Data.purge_cluster_node(name, remote_node)
      {purged_reg, purged_pg} = Data.purge_node(name, shard, remote_node)

      log_verbose(state, fn ->
        "#{log_prefix_shard(state)} remote_shard_down #{remote_node} (purged #{length(purged_reg)} reg, #{length(purged_pg)} pg)"
      end)

      for {cluster, key, dead_pid, meta, _time} <- purged_reg do
        Group.__dispatch__(name, :unregistered, key, dead_pid, meta, %{
          reason: {:nodedown, remote_node},
          cluster: cluster
        })
      end

      for {cluster, key, dead_pid, meta, _time} <- purged_pg do
        Group.__dispatch__(name, :left, key, dead_pid, meta, %{
          reason: {:nodedown, remote_node},
          cluster: cluster
        })
      end

      state = %{state | remote_shards: Map.delete(state.remote_shards, remote_node)}
      state = %{state | monitors: Map.delete(state.monitors, pid)}
      {:noreply, state}
    else
      # Regular process died — clean up its entries in this shard
      entries = Data.entries_by_pid(name, shard, pid)

      # Filter to entries that belong to this shard (count before filter for logging)
      my_entries =
        Enum.filter(entries, fn
          {:registry, cluster, key, _pid, _meta, _time, _node} ->
            shard_index_for(cluster, key, num_shards) == shard

          {:pg, cluster, key, _pid, _meta, _time, _node} ->
            shard_index_for(cluster, key, num_shards) == shard
        end)

      log_verbose(state, fn ->
        "#{log_prefix_shard(state)} process_down pid=#{inspect(pid)} reason=#{inspect(reason)} (#{length(my_entries)} entries cleaned)"
      end)

      for entry <- my_entries do
        case entry do
          {:registry, cluster, key, ^pid, meta, _time, _entry_node} ->
            Data.registry_delete(name, shard, cluster, key, pid)

            broadcast_to_cluster(
              state,
              cluster,
              {:replicate_unregister, cluster, key, pid, meta, reason}
            )

            Group.__dispatch__(name, :unregistered, key, pid, meta, %{
              reason: reason,
              cluster: cluster
            })

          {:pg, cluster, key, ^pid, meta, _time, _entry_node} ->
            Data.pg_delete(name, shard, cluster, key, pid)

            broadcast_to_cluster(
              state,
              cluster,
              {:replicate_leave, cluster, key, pid, meta, reason}
            )

            Group.__dispatch__(name, :left, key, pid, meta, %{
              reason: reason,
              cluster: cluster
            })
        end
      end

      state = %{state | monitors: Map.delete(state.monitors, pid)}
      {:noreply, state}
    end
  end

  def handle_info({:send_cluster_data, clusters, target_node}, state) do
    %{name: name} = state

    active = Enum.filter(clusters, fn c -> node() in Data.cluster_nodes(name, c) end)

    if active != [] do
      send_cluster_states(state, active, target_node)
    end

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # =====================================================================
  # Internal helpers
  # =====================================================================

  defp send_to_peer(state, target_node, message) do
    shard_name = shard_name(state.name, state.shard_index)
    send({shard_name, target_node}, message)
  end

  defp broadcast_to_peers(state, message) do
    shard_name = shard_name(state.name, state.shard_index)

    for {target_node, _pid} <- state.remote_shards do
      send({shard_name, target_node}, message)
    end
  end

  defp broadcast_to_cluster(state, nil = _cluster, message) do
    broadcast_to_peers(state, message)
  end

  defp broadcast_to_cluster(state, cluster, message) do
    %{name: name} = state
    shard_name = shard_name(name, state.shard_index)

    for target_node <- Data.cluster_nodes(name, cluster), target_node != node() do
      send({shard_name, target_node}, message)
    end
  end

  defp fan_out_to_siblings(state, message) do
    %{name: name, shard_index: shard_index, num_shards: num_shards} = state

    for i <- 0..(num_shards - 1), i != shard_index do
      send(shard_name(name, i), message)
    end
  end

  defp monitor_pid(state, pid) do
    Map.get(state.monitors, pid) || Process.monitor(pid)
  end

  defp put_monitor(state, pid, mref) do
    %{state | monitors: Map.put_new(state.monitors, pid, mref)}
  end

  defp maybe_demonitor_pid(state, name, shard, pid) do
    case Data.maybe_demonitor(name, shard, pid) do
      :ok ->
        case Map.pop(state.monitors, pid) do
          {nil, monitors} ->
            %{state | monitors: monitors}

          {mref, monitors} ->
            Process.demonitor(mref, [:flush])
            %{state | monitors: monitors}
        end

      :still_monitored ->
        state
    end
  end

  defp rebuild_monitors(state) do
    %{name: name, shard_index: shard} = state
    local = node()

    # Scan all entries in this shard's tables and re-establish monitors
    # Only monitor local pids — remote pids are cleaned up by their owning
    # node's DOWN handler (broadcast) or by nodedown.
    reg_table = Data.reg_by_pid_table(name, shard)

    monitors =
      try do
        :ets.foldl(
          fn {{pid, _cluster, _key}, _meta, _time, entry_node}, acc ->
            if entry_node == local and not Map.has_key?(acc, pid) do
              mref = Process.monitor(pid)
              Map.put(acc, pid, mref)
            else
              acc
            end
          end,
          %{},
          reg_table
        )
      rescue
        ArgumentError -> %{}
      end

    pg_table = Data.pg_by_pid_table(name, shard)

    monitors =
      try do
        :ets.foldl(
          fn {{pid, _cluster, _key}, _meta, _time, entry_node}, acc ->
            if entry_node == local and not Map.has_key?(acc, pid) do
              mref = Process.monitor(pid)
              Map.put(acc, pid, mref)
            else
              acc
            end
          end,
          monitors,
          pg_table
        )
      rescue
        ArgumentError -> monitors
      end

    %{state | monitors: monitors}
  end

  defp cluster_member?(name, cluster) do
    node() in Data.cluster_nodes(name, cluster)
  end

  defp merge_remote_cluster_data(state, cluster, reg_data, pg_data) do
    %{name: name, shard_index: shard, num_shards: num_shards} = state

    for {key, pid, meta, time} <- reg_data,
        shard_index_for(cluster, key, num_shards) == shard do
      case Data.registry_lookup(name, shard, cluster, key) do
        nil ->
          Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        {_existing_pid, _meta, existing_time, _node} when time > existing_time ->
          Data.registry_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        _ ->
          :ok
      end
    end

    for {key, pid, meta, time} <- pg_data,
        shard_index_for(cluster, key, num_shards) == shard do
      case Data.pg_lookup(name, shard, cluster, key, pid) do
        nil ->
          Data.pg_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        {_meta, existing_time, _node} when time > existing_time ->
          Data.pg_insert(name, shard, cluster, key, pid, meta, time, node(pid))

        _ ->
          :ok
      end
    end

    state
  end

  defp resolve_conflict(
         state,
         cluster,
         key,
         {local_pid, local_meta, local_time},
         {remote_pid, remote_meta, remote_time}
       ) do
    %{name: name, shard_index: shard} = state

    config = Group.get_config(name)

    winner_pid =
      case Map.get(config, :resolve_registry_conflict) do
        nil ->
          # Default: keep most recent, kill loser
          default_resolve_conflict(
            name,
            cluster,
            key,
            {local_pid, local_meta, local_time},
            {remote_pid, remote_meta, remote_time}
          )

        {mod, func, extra_args} ->
          apply(mod, func, [
            name,
            key,
            {local_pid, local_meta, local_time},
            {remote_pid, remote_meta, remote_time} | extra_args
          ])
      end

    cond do
      winner_pid == remote_pid ->
        # Remote wins — replace local entry
        Data.registry_delete(name, shard, cluster, key, local_pid)
        state = maybe_demonitor_pid(state, name, shard, local_pid)
        time = System.system_time()

        Data.registry_insert(
          name,
          shard,
          cluster,
          key,
          remote_pid,
          remote_meta,
          time,
          node(remote_pid)
        )

        state

      winner_pid == local_pid ->
        # Local wins — re-broadcast to override remote
        time = System.system_time()

        Data.registry_insert(
          name,
          shard,
          cluster,
          key,
          local_pid,
          local_meta,
          time,
          node(local_pid)
        )

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_register, cluster, key, local_pid, local_meta, time, :resolve_conflict}
        )

        state

      true ->
        # Neither wins — remove both
        Data.registry_delete(name, shard, cluster, key, local_pid)
        state = maybe_demonitor_pid(state, name, shard, local_pid)

        broadcast_to_cluster(
          state,
          cluster,
          {:replicate_unregister, cluster, key, local_pid, local_meta, :resolve_conflict}
        )

        Group.__dispatch__(name, :unregistered, key, local_pid, local_meta, %{
          reason: :resolve_conflict,
          cluster: cluster
        })

        state
    end
  end

  defp default_resolve_conflict(_name, _cluster, key, {pid1, _meta1, time1}, {pid2, meta2, time2}) do
    {winner_pid, loser_pid} =
      if time2 >= time1, do: {pid2, pid1}, else: {pid1, pid2}

    Logger.error(fn ->
      "#{inspect(__MODULE__)}: registry conflict detected: key=#{inspect(key)}, " <>
        "pid1=#{inspect(pid1)}, pid2=#{inspect(pid2)}, picking #{inspect(winner_pid)} as winner"
    end)

    Process.exit(loser_pid, {:group_registry_conflict, key, meta2})
    winner_pid
  end

  # Gather local data for all shared clusters in ONE table scan (instead of C scans)
  # and send per-cluster cluster_state messages. One O(N) scan vs C × O(N) scans.
  defp send_cluster_states(state, clusters, target_node) do
    %{name: name, shard_index: shard} = state
    {reg_by_cluster, pg_by_cluster} = Data.local_data_by_cluster(name, shard, clusters)

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} sending cluster_states to #{target_node} (#{length(clusters)} clusters)"
    end)

    for cluster <- clusters do
      reg_data = Map.get(reg_by_cluster, cluster, [])
      pg_data = Map.get(pg_by_cluster, cluster, [])

      if reg_data != [] or pg_data != [] do
        send_to_peer(state, target_node, {:cluster_state, cluster, reg_data, pg_data})
      end
    end
  end

  defp compute_shared_clusters(my_clusters, remote_clusters) do
    my_set = MapSet.new(my_clusters)
    remote_set = MapSet.new(remote_clusters)
    MapSet.intersection(my_set, remote_set) |> MapSet.to_list()
  end

  defp purge_cluster_entries(name, shard, cluster, target_node) do
    # Remove entries for a specific cluster and node
    reg_table = Data.reg_by_key_table(name, shard)
    reg_pid_table = Data.reg_by_pid_table(name, shard)

    purged_reg =
      :ets.select(reg_table, [
        {{{cluster, :"$1"}, :"$2", :"$3", :"$4", :"$5"}, [{:==, :"$5", target_node}],
         [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])
      |> Enum.map(fn {key, pid, meta, time} -> {cluster, key, pid, meta, time} end)

    for {^cluster, key, pid, _meta, _time} <- purged_reg do
      :ets.delete(reg_table, {cluster, key})
      :ets.delete(reg_pid_table, {pid, cluster, key})
    end

    pg_table = Data.pg_by_key_table(name, shard)
    pg_pid_table = Data.pg_by_pid_table(name, shard)

    purged_pg =
      :ets.select(pg_table, [
        {{{cluster, :"$1", :"$2"}, :"$3", :"$4", :"$5"}, [{:==, :"$5", target_node}],
         [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])
      |> Enum.map(fn {key, pid, meta, time} -> {cluster, key, pid, meta, time} end)

    for {^cluster, key, pid, _meta, _time} <- purged_pg do
      :ets.delete(pg_table, {cluster, key, pid})
      :ets.delete(pg_pid_table, {pid, cluster, key})
    end

    {purged_reg, purged_pg}
  end

  defp dispatch_purged(state, cluster, purged_reg, purged_pg, reason) do
    %{name: name} = state

    for {^cluster, key, pid, meta, _time} <- purged_reg do
      Group.__dispatch__(name, :unregistered, key, pid, meta, %{
        reason: reason,
        cluster: cluster
      })
    end

    for {^cluster, key, pid, meta, _time} <- purged_pg do
      Group.__dispatch__(name, :left, key, pid, meta, %{
        reason: reason,
        cluster: cluster
      })
    end

    state
  end

  # =====================================================================
  # Logging helpers
  # =====================================================================

  defp log(state, message_fn) when is_function(message_fn, 0) do
    case Group.get_config(state.name) do
      %{log: false} -> :ok
      _ -> Logger.info(message_fn)
    end
  end

  defp log_verbose(state, message_fn) when is_function(message_fn, 0) do
    case Group.get_config(state.name) do
      %{log: :verbose} -> Logger.info(message_fn)
      _ -> :ok
    end
  end

  defp log_once(state, message_fn) do
    if state.shard_index == 0, do: log(state, message_fn)
  end

  defp log_prefix(state) do
    "[Group #{inspect(state.name)}]"
  end

  defp log_prefix_shard(state) do
    "[Group #{inspect(state.name)}/#{state.shard_index}]"
  end
end
