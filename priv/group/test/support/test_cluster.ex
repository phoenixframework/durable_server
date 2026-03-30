defmodule Group.TestCluster do
  @moduledoc false

  @doc "Start N peer nodes with Group app loaded and ready"
  def start_peers(count, opts \\ []) do
    cookie = Keyword.get(opts, :cookie, Node.get_cookie())
    code_paths = :code.get_path()

    args =
      [~c"-setcookie", ~c"#{cookie}", ~c"-kernel", ~c"prevent_overlapping_partitions", ~c"false"] ++
        Enum.flat_map(code_paths, fn p -> [~c"-pa", p] end)

    for _i <- 1..count do
      name = :"peer#{System.unique_integer([:positive])}"
      {:ok, pid, node} = :peer.start(%{name: name, args: args})
      {:ok, _} = :rpc.call(node, :application, :ensure_all_started, [:elixir])
      {:ok, _} = :rpc.call(node, :application, :ensure_all_started, [:group])
      {pid, node}
    end
  end

  def stop_peers(peers) do
    Enum.each(peers, fn {pid, _node} ->
      if pid do
        try do
          :peer.stop(pid)
        catch
          :exit, _ -> :ok
        end
      end
    end)
  end

  @doc "Call a function on a remote node, raise on badrpc"
  def rpc!(node, mod, fun, args) do
    case :rpc.call(node, mod, fun, args) do
      {:badrpc, reason} -> raise "RPC to #{node} failed: #{inspect(reason)}"
      result -> result
    end
  end

  @doc "Start Group on a remote node"
  def start_group(node, opts) do
    opts = Keyword.put_new(opts, :log, false)

    :erpc.call(node, fn ->
      {:ok, pid} = Group.start_link(opts)
      Process.unlink(pid)
      {:ok, pid}
    end)
  end

  @doc "Start Group on a remote node and immediately connect to a cluster,
  all within a single call. This ensures Group.connect runs before peer
  discovery can complete (no round-trip gap between start and connect)."
  def start_group_and_connect(node, opts, cluster) do
    :erpc.call(node, __MODULE__, :do_start_group_and_connect, [opts, cluster])
  end

  @doc false
  def do_start_group_and_connect(opts, cluster) do
    opts = Keyword.put_new(opts, :log, false)
    {:ok, pid} = Group.start_link(opts)
    Process.unlink(pid)
    name = Keyword.fetch!(opts, :name)
    Group.connect(name, cluster)
    {:ok, pid}
  end

  @doc "Spawn a process on a remote node that registers and sleeps forever.

  Waits for the registration to complete before returning.

  Options:
    - `flush_shards: num_shards` — after registering, flush the target shard
      with `:sys.get_state` to ensure any pending nodedown or replicate messages
      have been processed.
  "
  def spawn_register(node, name, key, meta, opts \\ []) do
    :erpc.call(node, fn ->
      parent = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, key, meta)
          send(parent, {:registered, self()})
          Process.sleep(:infinity)
        end)

      receive do
        {:registered, ^pid} -> :ok
      after
        5000 -> raise "spawn_register timed out"
      end

      if num_shards = opts[:flush_shards] do
        cluster = opts[:cluster]
        shard_index = :erlang.phash2({cluster, key}, num_shards)
        :sys.get_state(:"#{name}_replica_#{shard_index}")
      end

      pid
    end)
  end

  @doc "Spawn a process on a remote node that joins and sleeps forever.
  Waits for the join to complete before returning."
  def spawn_join(node, name, key, meta, opts \\ []) do
    :erpc.call(node, fn ->
      parent = self()

      pid =
        spawn(fn ->
          :ok = Group.join(name, key, meta, opts)
          send(parent, {:joined, self()})
          Process.sleep(:infinity)
        end)

      receive do
        {:joined, ^pid} -> pid
      after
        5000 -> raise "spawn_join timed out"
      end
    end)
  end

  @doc "Spawn a process on a remote node that registers, joins, and sleeps forever.
  Waits for both operations to complete before returning."
  def spawn_register_and_join(node, name, reg_key, reg_meta, join_key, join_meta) do
    :erpc.call(node, fn ->
      parent = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, reg_key, reg_meta)
          :ok = Group.join(name, join_key, join_meta)
          send(parent, {:ready, self()})
          Process.sleep(:infinity)
        end)

      receive do
        {:ready, ^pid} -> pid
      after
        5000 -> raise "spawn_register_and_join timed out"
      end
    end)
  end

  @doc "Spawn a process on a remote node that monitors a pattern and forwards events"
  def spawn_monitor_forwarder(node, name, pattern, target_pid, opts \\ []) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :ok = Group.monitor(name, pattern, opts)
        send(target_pid, {:monitor_ready, self()})
        forward_events(target_pid)
      end)
    end)
  end

  defp forward_events(target_pid) do
    receive do
      {:group, events, _info} ->
        for event <- events, do: send(target_pid, {:got_event, event})
        forward_events(target_pid)
    after
      30_000 -> :ok
    end
  end

  @doc "Like spawn_monitor_forwarder, but preserves batch structure.
  Sends `{:got_batch, events}` per received `{:group, events, info}` message."
  def spawn_batch_forwarder(node, name, pattern, target_pid, opts \\ []) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :ok = Group.monitor(name, pattern, opts)
        send(target_pid, {:monitor_ready, self()})
        forward_batches(target_pid)
      end)
    end)
  end

  defp forward_batches(target_pid) do
    receive do
      {:group, events, _info} ->
        send(target_pid, {:got_batch, events})
        forward_batches(target_pid)
    after
      30_000 -> :ok
    end
  end

  @doc "Disconnect two peer nodes from each other"
  def disconnect_nodes(node_a, node_b) do
    rpc!(node_a, :erlang, :disconnect_node, [node_b])
  end

  @doc "Reconnect two peer nodes"
  def reconnect_nodes(node_a, node_b) do
    rpc!(node_a, Node, :connect, [node_b])
  end

  @doc "Spawn a process that registers and then exits after optional delay"
  def spawn_register_then_kill(node, name, key, meta, delay \\ 0) do
    :erpc.call(node, fn ->
      pid =
        spawn(fn ->
          :ok = Group.register(name, key, meta)
          if delay > 0, do: Process.sleep(delay)
        end)

      pid
    end)
  end

  @doc "Spawn a process that registers, re-registers with new meta, then unregisters"
  def spawn_register_update_unregister(node, name, key, meta1, meta2) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :ok = Group.register(name, key, meta1)
        Process.sleep(10)
        :ok = Group.register(name, key, meta2)
        Process.sleep(10)
        :ok = Group.unregister(name, key)
      end)
    end)
  end

  @doc "Find two keys that hash to different shards for the default cluster"
  def keys_for_different_shards(num_shards) do
    key1 = "shard_test/a"
    shard1 = :erlang.phash2({nil, key1}, num_shards)

    key2_suffix =
      Enum.find(
        Stream.iterate(0, &(&1 + 1)),
        fn i ->
          k = "shard_test/b_#{i}"
          :erlang.phash2({nil, k}, num_shards) != shard1
        end
      )

    {key1, "shard_test/b_#{key2_suffix}"}
  end

  @doc "Spawn a process that registers under one key and joins another, then sleeps.
  Waits for both operations to complete before returning."
  def spawn_register_and_join_keys(node, name, reg_key, reg_meta, join_key, join_meta) do
    :erpc.call(node, fn ->
      parent = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, reg_key, reg_meta)
          :ok = Group.join(name, join_key, join_meta)
          send(parent, {:ready, self()})
          Process.sleep(:infinity)
        end)

      receive do
        {:ready, ^pid} -> pid
      after
        5000 -> raise "spawn_register_and_join_keys timed out"
      end
    end)
  end

  @doc "Kill many remote processes in one RPC so their DOWNs arrive tightly."
  def kill_pids(node, pids, reason \\ :kill) do
    :erpc.call(node, __MODULE__, :do_kill_pids, [pids, reason])
  end

  @doc false
  def do_kill_pids(pids, reason) do
    Enum.each(pids, &Process.exit(&1, reason))
    :ok
  end

  @doc "Spawn a process on a remote node that registers in a named cluster and sleeps.
  Waits for the registration to complete before returning."
  def spawn_register_in_cluster(node, name, key, meta, cluster) do
    :erpc.call(node, fn ->
      parent = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, key, meta, cluster: cluster)
          send(parent, {:registered, self()})
          Process.sleep(:infinity)
        end)

      receive do
        {:registered, ^pid} -> pid
      after
        5000 -> raise "spawn_register_in_cluster timed out"
      end
    end)
  end

  @doc "Monitor nodedown events from a remote node, forwarding to caller"
  def monitor_nodes_on(node, target_pid) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :net_kernel.monitor_nodes(true)
        forward_nodedown(target_pid)
      end)
    end)
  end

  defp forward_nodedown(target_pid) do
    receive do
      {:nodedown, node} ->
        send(target_pid, {:nodedown_on_remote, node})
        forward_nodedown(target_pid)

      {:nodeup, _node} ->
        forward_nodedown(target_pid)
    after
      30_000 -> :ok
    end
  end

  @doc "Spawn a process on a remote node that joins a group and forwards messages to target"
  def spawn_join_forwarder(node, name, key, target_pid, opts) do
    :erpc.call(node, __MODULE__, :do_spawn_join_forwarder, [name, key, target_pid, opts])
  end

  @doc false
  def do_spawn_join_forwarder(name, key, target_pid, opts) do
    parent = self()

    pid =
      spawn(fn ->
        :ok = Group.join(name, key, %{}, opts)
        send(parent, {:joined, self()})
        forward_messages(target_pid)
      end)

    receive do
      {:joined, ^pid} -> pid
    after
      5000 -> raise "spawn_join_forwarder timed out"
    end
  end

  defp forward_messages(target_pid) do
    receive do
      msg ->
        send(target_pid, {:forwarded, msg})
        forward_messages(target_pid)
    after
      30_000 -> :ok
    end
  end

  @doc """
  Synchronously flushes all shard GenServers on a remote node.

  Issues `:sys.get_state` on each shard, which processes all messages
  queued before the call. Use after `assert_eventually` to drain any
  remaining async fan-out or cleanup messages before checking ETS state.
  """
  def flush_shards(node, name) do
    num_shards = rpc!(node, Group, :get_config, [name]).num_shards

    for shard <- 0..(num_shards - 1) do
      rpc!(node, :sys, :get_state, [:"#{name}_replica_#{shard}"])
    end

    :ok
  end

  @doc """
  Asserts that all ETS dual-index tables are in sync for a Group instance.

  Verifies:
  - reg_by_key ↔ reg_by_pid contain the same entries (across all shards)
  - pg_by_key ↔ pg_by_pid contain the same entries (across all shards)
  - cluster_nodes ↔ node_clusters contain the same pairs

  Raises on inconsistency with details about orphaned/missing entries.
  """
  def assert_ets_consistent(name) do
    num_shards = Group.get_config(name).num_shards

    for shard <- 0..(num_shards - 1) do
      # Registry: by_key entries should match by_pid entries
      reg_key_table = Group.Replica.Data.reg_by_key_table(name, shard)
      reg_pid_table = Group.Replica.Data.reg_by_pid_table(name, shard)

      # by_key: {{cluster, key}, pid, meta, time, node}
      key_set =
        :ets.tab2list(reg_key_table)
        |> MapSet.new(fn {{cluster, key}, pid, meta, time, nd} ->
          {cluster, key, pid, meta, time, nd}
        end)

      # by_pid: {{pid, cluster, key}, meta, time, node}
      pid_set =
        :ets.tab2list(reg_pid_table)
        |> MapSet.new(fn {{pid, cluster, key}, meta, time, nd} ->
          {cluster, key, pid, meta, time, nd}
        end)

      if key_set != pid_set do
        orphaned = MapSet.difference(pid_set, key_set) |> MapSet.to_list()
        missing = MapSet.difference(key_set, pid_set) |> MapSet.to_list()

        raise "ETS inconsistency in #{name} shard #{shard} (registry)!\n" <>
                "  Orphaned in reg_by_pid (no matching by_key): #{inspect(orphaned)}\n" <>
                "  Missing from reg_by_pid (in by_key only): #{inspect(missing)}"
      end

      # PG: by_key entries should match by_pid entries
      pg_key_table = Group.Replica.Data.pg_by_key_table(name, shard)
      pg_pid_table = Group.Replica.Data.pg_by_pid_table(name, shard)

      # pg_by_key: {{cluster, key, pid}, meta, time, node}
      pg_key_set =
        :ets.tab2list(pg_key_table)
        |> MapSet.new(fn {{cluster, key, pid}, meta, time, nd} ->
          {cluster, key, pid, meta, time, nd}
        end)

      # pg_by_pid: {{pid, cluster, key}, meta, time, node}
      pg_pid_set =
        :ets.tab2list(pg_pid_table)
        |> MapSet.new(fn {{pid, cluster, key}, meta, time, nd} ->
          {cluster, key, pid, meta, time, nd}
        end)

      if pg_key_set != pg_pid_set do
        orphaned = MapSet.difference(pg_pid_set, pg_key_set) |> MapSet.to_list()
        missing = MapSet.difference(pg_key_set, pg_pid_set) |> MapSet.to_list()

        raise "ETS inconsistency in #{name} shard #{shard} (PG)!\n" <>
                "  Orphaned in pg_by_pid (no matching by_key): #{inspect(orphaned)}\n" <>
                "  Missing from pg_by_pid (in by_key only): #{inspect(missing)}"
      end
    end

    # Cluster membership: cluster_nodes ↔ node_clusters
    cn_table = Group.Replica.Data.cluster_nodes_table(name)
    nc_table = Group.Replica.Data.node_clusters_table(name)

    cn_set = :ets.tab2list(cn_table) |> MapSet.new(fn {cluster, nd} -> {cluster, nd} end)
    nc_set = :ets.tab2list(nc_table) |> MapSet.new(fn {nd, cluster} -> {cluster, nd} end)

    if cn_set != nc_set do
      only_cn = MapSet.difference(cn_set, nc_set) |> MapSet.to_list()
      only_nc = MapSet.difference(nc_set, cn_set) |> MapSet.to_list()

      raise "cluster_nodes / node_clusters inconsistency in #{name}!\n" <>
              "  Only in cluster_nodes: #{inspect(only_cn)}\n" <>
              "  Only in node_clusters: #{inspect(only_nc)}"
    end

    :ok
  end

  @doc "Wait for a condition to become true, with retries"
  def assert_eventually(fun, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 2000)
    interval = Keyword.get(opts, :interval, 50)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_assert_eventually(fun, interval, deadline)
  end

  defp do_assert_eventually(fun, interval, deadline) do
    case fun.() do
      true ->
        true

      false ->
        if System.monotonic_time(:millisecond) >= deadline do
          raise "assert_eventually timed out"
        end

        Process.sleep(interval)
        do_assert_eventually(fun, interval, deadline)
    end
  end
end
