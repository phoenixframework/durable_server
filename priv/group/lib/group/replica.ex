defmodule Group.Replica do
  @moduledoc false

  use GenServer

  # Sharded GenServer: discovery, replication, monitoring, conflict resolution.
  # One per shard. Registered as :"#{name}_replica_#{shard_index}".
  # Each shard has a linked multicast_loop process for FIFO message ordering.

  require Logger

  alias Group.Replica.Data

  defstruct [
    :name,
    :shard_index,
    :num_shards,
    :multicast_pid,
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

    shard_registered_name = shard_name(name, shard_index)
    multicast_pid = spawn_link(fn -> multicast_loop(shard_registered_name) end)

    :net_kernel.monitor_nodes(true)

    state = %__MODULE__{
      name: name,
      shard_index: shard_index,
      num_shards: num_shards,
      multicast_pid: multicast_pid
    }

    # Rebuild monitors from any surviving ETS data (after shard crash/restart)
    state = rebuild_monitors(state)

    # Discover peers on all known nodes
    for remote_node <- Node.list() do
      send({shard_registered_name, remote_node},
        {:discover, self(), shard_index, Data.all_clusters(name)})
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
        Data.registry_insert(name, shard, cluster, key, pid, meta, time, mref, node(pid))

        broadcast_to_cluster(state, cluster,
          {:sync_register, cluster, key, pid, meta, time, :register})

        Group.__dispatch__(name, :registered, key, pid, meta, %{
          previous_meta: nil,
          cluster: cluster
        })

        {:reply, :ok, put_monitor(state, pid, mref)}

      {existing_pid, _existing_meta, _time, _mref, _node} when existing_pid == pid ->
        # Same pid re-registering — update metadata
        {_, old_meta, _, old_mref, _} = Data.registry_lookup(name, shard, cluster, key)
        time = System.system_time()
        Data.registry_insert(name, shard, cluster, key, pid, meta, time, old_mref, node(pid))

        broadcast_to_cluster(state, cluster,
          {:sync_register, cluster, key, pid, meta, time, :update})

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
      {pid, meta, _time, _mref, entry_node} when entry_node == node() ->
        Data.registry_delete(name, shard, cluster, key)
        state = maybe_demonitor_pid(state, name, shard, pid)

        broadcast_to_cluster(state, cluster,
          {:sync_unregister, cluster, key, pid, meta, :unregister})

        Group.__dispatch__(name, :unregistered, key, pid, meta, %{
          reason: :unregister,
          cluster: cluster
        })

        {:reply, :ok, state}

      nil ->
        {:reply, {:error, :undefined}, state}

      {_pid, _meta, _time, _mref, _other_node} ->
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
        Data.pg_insert(name, shard, cluster, key, pid, meta, time, mref, node(pid))

        broadcast_to_cluster(state, cluster,
          {:sync_join, cluster, key, pid, meta, time, :join})

        Group.__dispatch__(name, :joined, key, pid, meta, %{
          previous_meta: nil,
          cluster: cluster
        })

        {:reply, :ok, put_monitor(state, pid, mref)}

      {old_meta, _time, old_mref, _node} ->
        # Re-join with updated metadata
        time = System.system_time()
        Data.pg_insert(name, shard, cluster, key, pid, meta, time, old_mref, node(pid))

        broadcast_to_cluster(state, cluster,
          {:sync_join, cluster, key, pid, meta, time, :update})

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

      {meta, _time, _mref, _node} ->
        Data.pg_delete(name, shard, cluster, key, pid)
        state = maybe_demonitor_pid(state, name, shard, pid)

        broadcast_to_cluster(state, cluster,
          {:sync_leave, cluster, key, pid, meta, :leave})

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

  def handle_call({:cluster_connect, cluster}, _from, state) do
    %{name: name, multicast_pid: multicast_pid} = state

    # Broadcast to remote peers to inform them about our cluster membership
    target_nodes = Map.keys(state.remote_shards)

    send(multicast_pid,
      {:broadcast, {:cluster_connect, cluster, self()}, target_nodes, []})

    # Send cluster data for this shard to existing cluster members
    send_cluster_data_to_nodes(state, cluster, Data.cluster_nodes(name, cluster))

    {:reply, :ok, state}
  end

  def handle_call({:cluster_disconnect, cluster}, _from, state) do
    %{name: name, shard_index: shard, multicast_pid: multicast_pid} = state

    # Remove our entries for this cluster
    {purged_reg, purged_pg} = purge_cluster_entries(name, shard, cluster, node())

    # Fire events for purged entries
    dispatch_purged(state, cluster, purged_reg, purged_pg, :cluster_disconnect)

    target_nodes = Map.keys(state.remote_shards)

    send(multicast_pid,
      {:broadcast, {:cluster_disconnect, cluster, self()}, target_nodes, []})

    {:reply, :ok, state}
  end

  # =====================================================================
  # Sync receive (handle_info)
  # =====================================================================

  @impl true
  def handle_info({:sync_register, cluster, key, pid, meta, time, reason}, state) do
    %{name: name, shard_index: shard} = state

    case Data.registry_lookup(name, shard, cluster, key) do
      nil ->
        mref = monitor_pid(state, pid)
        Data.registry_insert(name, shard, cluster, key, pid, meta, time, mref, node(pid))

        previous_meta =
          if reason == :update do
            nil
          else
            nil
          end

        Group.__dispatch__(name, :registered, key, pid, meta, %{
          previous_meta: previous_meta,
          cluster: cluster
        })

        {:noreply, put_monitor(state, pid, mref)}

      {^pid, _old_meta, _old_time, _old_mref, _node} ->
        # Same pid updating — keep mref
        {_, old_meta, _, old_mref, _} = Data.registry_lookup(name, shard, cluster, key)
        Data.registry_insert(name, shard, cluster, key, pid, meta, time, old_mref, node(pid))

        Group.__dispatch__(name, :registered, key, pid, meta, %{
          previous_meta: old_meta,
          cluster: cluster
        })

        {:noreply, state}

      {existing_pid, existing_meta, existing_time, _existing_mref, existing_node}
      when existing_node == node() ->
        # Conflict: incoming remote registration vs local entry
        state = resolve_conflict(state, cluster, key,
          {existing_pid, existing_meta, existing_time},
          {pid, meta, time})
        {:noreply, state}

      {_existing_pid, _existing_meta, existing_time, _existing_mref, _existing_node} ->
        # Both remote — keep the more recent one
        if time > existing_time do
          mref = monitor_pid(state, pid)
          Data.registry_insert(name, shard, cluster, key, pid, meta, time, mref, node(pid))

          Group.__dispatch__(name, :registered, key, pid, meta, %{
            previous_meta: nil,
            cluster: cluster
          })

          {:noreply, put_monitor(state, pid, mref)}
        else
          {:noreply, state}
        end
    end
  end

  def handle_info({:sync_unregister, cluster, key, pid, meta, reason}, state) do
    %{name: name, shard_index: shard} = state

    case Data.registry_lookup(name, shard, cluster, key) do
      {^pid, _meta, _time, _mref, _node} ->
        Data.registry_delete(name, shard, cluster, key)
        state = maybe_demonitor_pid(state, name, shard, pid)

        Group.__dispatch__(name, :unregistered, key, pid, meta, %{
          reason: reason,
          cluster: cluster
        })

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:sync_join, cluster, key, pid, meta, time, reason}, state) do
    %{name: name, shard_index: shard} = state

    case Data.pg_lookup(name, shard, cluster, key, pid) do
      nil ->
        mref = monitor_pid(state, pid)
        Data.pg_insert(name, shard, cluster, key, pid, meta, time, mref, node(pid))

        Group.__dispatch__(name, :joined, key, pid, meta, %{
          previous_meta: nil,
          cluster: cluster
        })

        {:noreply, put_monitor(state, pid, mref)}

      {old_meta, _old_time, old_mref, _node} ->
        Data.pg_insert(name, shard, cluster, key, pid, meta, time, old_mref, node(pid))

        previous_meta = if reason == :update, do: old_meta, else: nil

        Group.__dispatch__(name, :joined, key, pid, meta, %{
          previous_meta: previous_meta,
          cluster: cluster
        })

        {:noreply, state}
    end
  end

  def handle_info({:sync_leave, cluster, key, pid, meta, reason}, state) do
    %{name: name, shard_index: shard} = state

    case Data.pg_lookup(name, shard, cluster, key, pid) do
      nil ->
        {:noreply, state}

      {_meta, _time, _mref, _node} ->
        Data.pg_delete(name, shard, cluster, key, pid)
        state = maybe_demonitor_pid(state, name, shard, pid)

        Group.__dispatch__(name, :left, key, pid, meta, %{
          reason: reason,
          cluster: cluster
        })

        {:noreply, state}
    end
  end

  # =====================================================================
  # Discovery protocol
  # =====================================================================

  def handle_info({:discover, remote_pid, remote_shard_index, _remote_clusters}, state)
      when remote_shard_index == state.shard_index do
    %{name: name, shard_index: shard, multicast_pid: multicast_pid} = state
    remote_node = node(remote_pid)

    if Map.has_key?(state.remote_shards, remote_node) do
      # Already know about this node, send ack with our data
      {reg_data, pg_data} = Data.all_local_data(name, shard)
      clusters = Data.all_clusters(name)

      send(multicast_pid,
        {:send_one, remote_node,
         {:ack_sync, self(), shard, clusters, reg_data, pg_data}})

      {:noreply, state}
    else
      # New node, monitor and exchange data
      Process.monitor(remote_pid)

      {reg_data, pg_data} = Data.all_local_data(name, shard)
      clusters = Data.all_clusters(name)

      send(multicast_pid,
        {:send_one, remote_node,
         {:ack_sync, self(), shard, clusters, reg_data, pg_data}})

      state = %{state | remote_shards: Map.put(state.remote_shards, remote_node, remote_pid)}
      {:noreply, state}
    end
  end

  def handle_info({:discover, _remote_pid, _other_shard, _clusters}, state) do
    # Wrong shard index, ignore
    {:noreply, state}
  end

  def handle_info({:ack_sync, remote_pid, remote_shard_index, _remote_clusters, reg_data, pg_data}, state)
      when remote_shard_index == state.shard_index do
    %{name: name, shard_index: shard} = state
    remote_node = node(remote_pid)

    already_known = Map.has_key?(state.remote_shards, remote_node)

    # Merge remote data into local ETS
    state = merge_remote_data(state, reg_data, pg_data, remote_node)

    state =
      if already_known do
        state
      else
        # New node — monitor and send our data back
        Process.monitor(remote_pid)

        {local_reg, local_pg} = Data.all_local_data(name, shard)
        clusters = Data.all_clusters(name)

        send(state.multicast_pid,
          {:send_one, remote_node,
           {:ack_sync, self(), shard, clusters, local_reg, local_pg}})

        %{state | remote_shards: Map.put(state.remote_shards, remote_node, remote_pid)}
      end

    {:noreply, state}
  end

  def handle_info({:ack_sync, _remote_pid, _other_shard, _clusters, _reg, _pg}, state) do
    {:noreply, state}
  end

  # =====================================================================
  # Cluster connect/disconnect from remote
  # =====================================================================

  def handle_info({:cluster_connect, cluster, remote_pid}, state) do
    %{name: name, shard_index: shard} = state
    remote_node = node(remote_pid)
    current = Data.cluster_nodes(name, cluster)

    if MapSet.member?(current, node()) do
      # We're also in this cluster — add the remote node and exchange data
      Data.put_cluster_nodes(name, cluster, MapSet.put(current, remote_node))

      # Send ack so remote node knows we're in the cluster too
      send(state.multicast_pid,
        {:send_one, remote_node, {:cluster_connect_ack, cluster, self()}})

      # Send our local data for this cluster to the new member
      {reg_data, pg_data} = Data.local_data(name, shard, cluster)

      if length(reg_data) > 0 or length(pg_data) > 0 do
        send(state.multicast_pid,
          {:send_one, remote_node,
           {:cluster_sync, cluster, reg_data, pg_data}})
      end
    end

    {:noreply, state}
  end

  def handle_info({:cluster_connect_ack, cluster, remote_pid}, state) do
    %{name: name} = state
    remote_node = node(remote_pid)

    # The remote node confirmed it's in this cluster — add it to our cluster_nodes
    current = Data.cluster_nodes(name, cluster)
    Data.put_cluster_nodes(name, cluster, MapSet.put(current, remote_node))
    {:noreply, state}
  end

  def handle_info({:cluster_disconnect, cluster, remote_pid}, state) do
    %{name: name, shard_index: shard} = state
    remote_node = node(remote_pid)

    # Purge the disconnecting node's entries for this cluster
    {purged_reg, purged_pg} = purge_cluster_entries(name, shard, cluster, remote_node)
    state = dispatch_purged(state, cluster, purged_reg, purged_pg, :cluster_disconnect)
    {:noreply, state}
  end

  def handle_info({:cluster_sync, cluster, reg_data, pg_data}, state) do
    state = merge_remote_cluster_data(state, cluster, reg_data, pg_data)
    {:noreply, state}
  end

  # =====================================================================
  # Node up/down
  # =====================================================================

  def handle_info({:nodeup, remote_node}, state) do
    %{shard_index: shard, name: name} = state
    shard_registered_name = shard_name(name, shard)

    send({shard_registered_name, remote_node},
      {:discover, self(), shard, Data.all_clusters(name)})

    {:noreply, state}
  end

  def handle_info({:nodedown, dead_node}, state) do
    %{name: name, shard_index: shard} = state

    # Purge all data from the dead node
    {purged_reg, purged_pg} = Data.purge_node(name, shard, dead_node)

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
      # Remote shard process died — purge its node data
      {purged_reg, purged_pg} = Data.purge_node(name, shard, remote_node)

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

      # Filter to entries that belong to this shard
      my_entries =
        Enum.filter(entries, fn
          {:registry, cluster, key, _pid, _meta, _time, _mref, _node} ->
            shard_index_for(cluster, key, num_shards) == shard

          {:pg, cluster, key, _pid, _meta, _time, _mref, _node} ->
            shard_index_for(cluster, key, num_shards) == shard
        end)

      for entry <- my_entries do
        case entry do
          {:registry, cluster, key, ^pid, meta, _time, _mref, _entry_node} ->
            Data.registry_delete(name, shard, cluster, key)

            broadcast_to_cluster(state, cluster,
              {:sync_unregister, cluster, key, pid, meta, reason})

            Group.__dispatch__(name, :unregistered, key, pid, meta, %{
              reason: reason,
              cluster: cluster
            })

          {:pg, cluster, key, ^pid, meta, _time, _mref, _entry_node} ->
            Data.pg_delete(name, shard, cluster, key, pid)

            broadcast_to_cluster(state, cluster,
              {:sync_leave, cluster, key, pid, meta, reason})

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

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # =====================================================================
  # Internal helpers
  # =====================================================================

  defp multicast_loop(shard_name) do
    receive do
      {:broadcast, message, target_nodes, excluded_nodes} ->
        for target_node <- target_nodes, target_node not in excluded_nodes do
          send({shard_name, target_node}, message)
        end

        multicast_loop(shard_name)

      {:send_one, node, message} ->
        send({shard_name, node}, message)
        multicast_loop(shard_name)
    end
  end

  defp broadcast_to_cluster(state, nil = _cluster, message) do
    # Default cluster: broadcast to all known peers
    %{multicast_pid: multicast_pid} = state
    target_nodes = Map.keys(state.remote_shards)

    if target_nodes != [] do
      send(multicast_pid, {:broadcast, message, target_nodes, []})
    end
  end

  defp broadcast_to_cluster(state, cluster, message) do
    %{name: name, multicast_pid: multicast_pid} = state
    cluster_member_nodes = Data.cluster_nodes(name, cluster)

    target_nodes =
      cluster_member_nodes
      |> MapSet.to_list()
      |> Enum.filter(&(&1 != node()))

    if target_nodes != [] do
      send(multicast_pid, {:broadcast, message, target_nodes, []})
    end
  end

  defp monitor_pid(state, pid) do
    %{name: name, shard_index: shard} = state

    case Map.get(state.monitors, pid) do
      nil ->
        # Check ETS for existing monitor (could be from before shard restart)
        case Data.find_monitor_for_pid(name, shard, pid) do
          nil -> Process.monitor(pid)
          existing_mref -> existing_mref
        end

      existing_mref ->
        existing_mref
    end
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

    # Scan all entries in this shard's tables and re-establish monitors
    reg_table = Data.reg_by_pid_table(name, shard)

    monitors =
      try do
        :ets.foldl(
          fn {pid, _cluster, _key, _meta, _time, _mref, entry_node}, acc ->
            if entry_node == node() or Map.has_key?(acc, pid) do
              acc
            else
              mref = Process.monitor(pid)
              Map.put(acc, pid, mref)
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
          fn {{pid, _cluster, _key}, _meta, _time, _mref, entry_node}, acc ->
            if entry_node == node() or Map.has_key?(acc, pid) do
              acc
            else
              mref = Process.monitor(pid)
              Map.put(acc, pid, mref)
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

  defp merge_remote_data(state, reg_data, pg_data, remote_node) do
    %{name: name, shard_index: shard} = state

    state =
      Enum.reduce(reg_data, state, fn {cluster, key, pid, meta, time}, acc ->
        if shard_index_for(cluster, key, state.num_shards) == shard and
             cluster_member?(name, cluster) do
          case Data.registry_lookup(name, shard, cluster, key) do
            nil ->
              mref = monitor_pid(acc, pid)
              Data.registry_insert(name, shard, cluster, key, pid, meta, time, mref, remote_node)
              put_monitor(acc, pid, mref)

            {_existing_pid, _meta, existing_time, _mref, _node} when time > existing_time ->
              mref = monitor_pid(acc, pid)
              Data.registry_insert(name, shard, cluster, key, pid, meta, time, mref, remote_node)
              put_monitor(acc, pid, mref)

            _ ->
              acc
          end
        else
          acc
        end
      end)

    Enum.reduce(pg_data, state, fn {cluster, key, pid, meta, time}, acc ->
      if shard_index_for(cluster, key, state.num_shards) == shard and
           cluster_member?(name, cluster) do
        case Data.pg_lookup(name, shard, cluster, key, pid) do
          nil ->
            mref = monitor_pid(acc, pid)
            Data.pg_insert(name, shard, cluster, key, pid, meta, time, mref, remote_node)
            put_monitor(acc, pid, mref)

          {_meta, existing_time, _mref, _node} when time > existing_time ->
            mref = monitor_pid(acc, pid)
            Data.pg_insert(name, shard, cluster, key, pid, meta, time, mref, remote_node)
            put_monitor(acc, pid, mref)

          _ ->
            acc
        end
      else
        acc
      end
    end)
  end

  # Check if the local node is a member of a cluster.
  # Default cluster (nil) includes all nodes.
  defp cluster_member?(_name, nil), do: true

  defp cluster_member?(name, cluster) do
    case Data.cluster_nodes(name, cluster) do
      nil -> false
      nodes -> MapSet.member?(nodes, node())
    end
  end

  defp merge_remote_cluster_data(state, cluster, reg_data, pg_data) do
    %{name: name, shard_index: shard} = state

    state =
      Enum.reduce(reg_data, state, fn {key, pid, meta, time}, acc ->
        if shard_index_for(cluster, key, state.num_shards) == shard do
          case Data.registry_lookup(name, shard, cluster, key) do
            nil ->
              mref = monitor_pid(acc, pid)
              Data.registry_insert(name, shard, cluster, key, pid, meta, time, mref, node(pid))
              put_monitor(acc, pid, mref)

            _ ->
              acc
          end
        else
          acc
        end
      end)

    Enum.reduce(pg_data, state, fn {key, pid, meta, time}, acc ->
      if shard_index_for(cluster, key, state.num_shards) == shard do
        case Data.pg_lookup(name, shard, cluster, key, pid) do
          nil ->
            mref = monitor_pid(acc, pid)
            Data.pg_insert(name, shard, cluster, key, pid, meta, time, mref, node(pid))
            put_monitor(acc, pid, mref)

          _ ->
            acc
        end
      else
        acc
      end
    end)
  end

  defp resolve_conflict(state, cluster, key, {local_pid, local_meta, local_time}, {remote_pid, remote_meta, remote_time}) do
    %{name: name, shard_index: shard} = state

    config = Group.get_config(name)

    winner_pid =
      case Map.get(config, :resolve_registry_conflict) do
        nil ->
          # Default: keep most recent, kill loser
          default_resolve_conflict(name, cluster, key,
            {local_pid, local_meta, local_time},
            {remote_pid, remote_meta, remote_time})

        {mod, func, extra_args} ->
          apply(mod, func, [name, key,
            {local_pid, local_meta, local_time},
            {remote_pid, remote_meta, remote_time} | extra_args])
      end

    cond do
      winner_pid == remote_pid ->
        # Remote wins — replace local entry
        Data.registry_delete(name, shard, cluster, key)
        mref = monitor_pid(state, remote_pid)
        time = System.system_time()
        Data.registry_insert(name, shard, cluster, key, remote_pid, remote_meta, time, mref, node(remote_pid))
        put_monitor(state, remote_pid, mref)

      winner_pid == local_pid ->
        # Local wins — re-broadcast to override remote
        time = System.system_time()
        {_, _, _, old_mref, _} = Data.registry_lookup(name, shard, cluster, key)
        Data.registry_insert(name, shard, cluster, key, local_pid, local_meta, time, old_mref, node(local_pid))

        broadcast_to_cluster(state, cluster,
          {:sync_register, cluster, key, local_pid, local_meta, time, :resolve_conflict})
        state

      true ->
        # Neither wins — remove both
        Data.registry_delete(name, shard, cluster, key)
        state = maybe_demonitor_pid(state, name, shard, local_pid)

        broadcast_to_cluster(state, cluster,
          {:sync_unregister, cluster, key, local_pid, local_meta, :resolve_conflict})

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

    Process.exit(loser_pid, {:syn_resolve_kill, key, meta2})
    winner_pid
  end

  defp send_cluster_data_to_nodes(state, cluster, target_nodes) do
    %{name: name, shard_index: shard, multicast_pid: multicast_pid} = state

    {reg_data, pg_data} = Data.local_data(name, shard, cluster)

    if length(reg_data) > 0 or length(pg_data) > 0 do
      for target_node <- MapSet.to_list(target_nodes) do
        if target_node != node() do
          send(multicast_pid,
            {:send_one, target_node,
             {:cluster_sync, cluster, reg_data, pg_data}})
        end
      end
    end
  end

  defp purge_cluster_entries(name, shard, cluster, target_node) do
    # Remove entries for a specific cluster and node
    reg_table = Data.reg_by_key_table(name, shard)
    reg_pid_table = Data.reg_by_pid_table(name, shard)

    purged_reg =
      :ets.select(reg_table, [
        {{{cluster, :"$1"}, :"$2", :"$3", :"$4", :_, :"$5"},
         [{:==, :"$5", target_node}],
         [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])
      |> Enum.map(fn {key, pid, meta, time} -> {cluster, key, pid, meta, time} end)

    for {^cluster, key, _pid, _meta, _time} <- purged_reg do
      :ets.delete(reg_table, {cluster, key})
      :ets.match_delete(reg_pid_table, {:_, cluster, key, :_, :_, :_, target_node})
    end

    pg_table = Data.pg_by_key_table(name, shard)
    pg_pid_table = Data.pg_by_pid_table(name, shard)

    purged_pg =
      :ets.select(pg_table, [
        {{{cluster, :"$1", :"$2"}, :"$3", :"$4", :_, :"$5"},
         [{:==, :"$5", target_node}],
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
end
