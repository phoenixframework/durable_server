defmodule Group.TestCluster do
  @moduledoc false

  @doc "Start N peer nodes with Group app loaded and ready"
  def start_peers(count, opts \\ []) do
    cookie = Keyword.get(opts, :cookie, Node.get_cookie())
    code_paths = :code.get_path()

    args =
      [~c"-setcookie", ~c"#{cookie}",
       ~c"-kernel", ~c"prevent_overlapping_partitions", ~c"false"] ++
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
      if pid, do: :peer.stop(pid)
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
    :erpc.call(node, fn ->
      {:ok, pid} = Group.start_link(opts)
      Process.unlink(pid)
      {:ok, pid}
    end)
  end

  @doc "Spawn a process on a remote node that registers and sleeps forever"
  def spawn_register(node, name, key, meta) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :ok = Group.register(name, key, meta)
        Process.sleep(:infinity)
      end)
    end)
  end

  @doc "Spawn a process on a remote node that joins and sleeps forever"
  def spawn_join(node, name, key, meta, opts \\ []) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :ok = Group.join(name, key, meta, opts)
        Process.sleep(:infinity)
      end)
    end)
  end

  @doc "Spawn a process on a remote node that registers, joins, and sleeps forever"
  def spawn_register_and_join(node, name, reg_key, reg_meta, join_key, join_meta) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :ok = Group.register(name, reg_key, reg_meta)
        :ok = Group.join(name, join_key, join_meta)
        Process.sleep(:infinity)
      end)
    end)
  end

  @doc "Spawn a process on a remote node that monitors a pattern and forwards events"
  def spawn_monitor_forwarder(node, name, pattern, target_pid) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :ok = Group.monitor(name, pattern)
        forward_events(target_pid)
      end)
    end)
  end

  defp forward_events(target_pid) do
    receive do
      %Group.Event{} = event ->
        send(target_pid, {:got_event, event})
        forward_events(target_pid)
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
      pid = spawn(fn ->
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

  @doc "Spawn a process that registers under one key and joins another, then sleeps"
  def spawn_register_and_join_keys(node, name, reg_key, reg_meta, join_key, join_meta) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :ok = Group.register(name, reg_key, reg_meta)
        :ok = Group.join(name, join_key, join_meta)
        Process.sleep(:infinity)
      end)
    end)
  end

  @doc "Spawn a process on a remote node that registers in a named cluster and sleeps"
  def spawn_register_in_cluster(node, name, key, meta, cluster) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :ok = Group.register(name, key, meta, cluster: cluster)
        Process.sleep(:infinity)
      end)
    end)
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
