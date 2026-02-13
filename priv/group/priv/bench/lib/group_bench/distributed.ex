defmodule GroupBench.Distributed do
  @moduledoc """
  Coordinator for distributed benchmarks.

  Expects replica1@127.0.0.1 and replica2@127.0.0.1 to already be running
  (started by run_distributed.sh). Connects to them, then drives benchmark
  scenarios via :erpc.call with MFA.
  """

  import GroupBench.Helpers

  @name :bench
  @shards System.schedulers_online()
  @replicas [:"replica1@127.0.0.1", :"replica2@127.0.0.1"]

  def run do
    header("Distributed Benchmarks")
    IO.puts("  coordinator: #{node()}")
    IO.puts("  schedulers:  #{System.schedulers_online()}")

    connect_replicas()

    bench_replication_latency(@replicas)
    bench_bulk_sync(@replicas)
    bench_concurrent_cross_node(@replicas)
    bench_named_cluster_replication(@replicas)

    IO.puts("\n  Done.\n")
  end

  # ── Connection ────────────────────────────────────────────────────────

  defp connect_replicas do
    Enum.each(@replicas, fn node ->
      wait_for_connection(node)
    end)

    IO.puts("  All replicas connected.\n")
  end

  defp wait_for_connection(node_name, attempts \\ 50) do
    if attempts <= 0 do
      raise "Failed to connect to #{node_name}"
    end

    case Node.connect(node_name) do
      true ->
        IO.puts("  Connected to #{node_name}")

      _ ->
        Process.sleep(200)
        wait_for_connection(node_name, attempts - 1)
    end
  end

  # ── Group lifecycle helpers (all MFA) ─────────────────────────────────

  defp start_group_on(node, opts \\ []) do
    opts = Keyword.merge([name: @name, shards: @shards], opts)
    :erpc.call(node, GroupBench.Replica, :start_group, [opts])
  end

  defp stop_group_on(node) do
    :erpc.call(node, GroupBench.Replica, :stop_group, [@name])
  catch
    _, _ -> :ok
  end

  defp stop_groups(replicas) do
    Enum.each(replicas, &stop_group_on/1)
    Process.sleep(100)
  end

  defp wait_for_peer_discovery(replicas) do
    expected = MapSet.new(replicas)

    poll_until(fn ->
      Enum.all?(replicas, fn node ->
        nodes = :erpc.call(node, Group, :nodes, [@name])
        node_set = MapSet.new([node | nodes])
        MapSet.equal?(node_set, expected)
      end)
    end)
  end

  defp poll_until(fun, timeout_ms \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_poll(fun, deadline)
  end

  defp do_poll(fun, deadline) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) > deadline do
        raise "poll_until timed out"
      end

      Process.sleep(10)
      do_poll(fun, deadline)
    end
  end

  # ── 1. Replication latency ───────────────────────────────────────────

  defp bench_replication_latency([r1, r2] = replicas) do
    header("1. Replication Latency (nil cluster)")

    start_group_on(r1)
    start_group_on(r2)
    wait_for_peer_discovery(replicas)

    n = 1_000

    samples =
      Enum.map(1..n, fn i ->
        key = "repl-#{i}"

        {us, _} =
          :timer.tc(fn ->
            :erpc.call(r1, GroupBench.Replica, :spawn_register, [@name, key])

            poll_until(fn ->
              :erpc.call(r2, Group, :lookup, [@name, key, []]) != nil
            end)
          end)

        us
      end)
      |> Enum.sort()

    report_latency("register on r1 → visible on r2", samples)

    stop_groups(replicas)
  end

  # ── 2. Bulk sync (new peer catches up) ───────────────────────────────

  defp bench_bulk_sync([r1, r2] = replicas) do
    header("2. Bulk Sync (new peer catches up)")

    for key_count <- [1_000, 10_000] do
      subheader("#{format_number(key_count)} keys")

      start_group_on(r1)

      :erpc.call(r1, GroupBench.Replica, :bulk_register, [@name, key_count, "bulk-"], 60_000)

      {sync_us, _} =
        :timer.tc(fn ->
          start_group_on(r2)

          poll_until(
            fn ->
              count = :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name])
              count >= key_count
            end,
            30_000
          )
        end)

      rate = if sync_us > 0, do: round(key_count * 1_000_000 / sync_us), else: 0

      IO.puts("  sync time:  #{format_number(div(sync_us, 1000))} ms")
      IO.puts("  keys/sec:   #{format_number(rate)}")

      stop_groups(replicas)
    end
  end

  # ── 3. Concurrent cross-node writes ──────────────────────────────────

  defp bench_concurrent_cross_node([r1, r2] = replicas) do
    header("3. Concurrent Cross-Node Writes")

    n = 5_000

    start_group_on(r1)
    start_group_on(r2)
    wait_for_peer_discovery(replicas)

    {wall_us, _} =
      :timer.tc(fn ->
        t1 =
          Task.async(fn ->
            :erpc.call(r1, GroupBench.Replica, :bulk_register, [@name, n, "r1-"], 60_000)
          end)

        t2 =
          Task.async(fn ->
            :erpc.call(r2, GroupBench.Replica, :bulk_register, [@name, n, "r2-"], 60_000)
          end)

        Task.await_many([t1, t2], 60_000)

        total = 2 * n

        poll_until(
          fn ->
            c1 = :erpc.call(r1, GroupBench.Replica, :total_registry_count, [@name])
            c2 = :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name])
            c1 >= total and c2 >= total
          end,
          30_000
        )
      end)

    total = 2 * n
    report_throughput("concurrent writes + convergence", total, wall_us)

    stop_groups(replicas)
  end

  # ── 4. Named cluster replication ─────────────────────────────────────

  defp bench_named_cluster_replication([r1, r2] = replicas) do
    header("4. Named Cluster Replication Latency")

    start_group_on(r1)
    start_group_on(r2)
    wait_for_peer_discovery(replicas)

    :erpc.call(r1, Group, :connect, [@name, "game"])
    :erpc.call(r2, Group, :connect, [@name, "game"])

    poll_until(fn ->
      n1 = :erpc.call(r1, Group, :nodes, [@name, "game"])
      n2 = :erpc.call(r2, Group, :nodes, [@name, "game"])
      length(n1) >= 1 and length(n2) >= 1
    end)

    n = 1_000

    samples =
      Enum.map(1..n, fn i ->
        key = "game-repl-#{i}"

        {us, _} =
          :timer.tc(fn ->
            :erpc.call(r1, GroupBench.Replica, :spawn_register, [
              @name,
              key,
              [cluster: "game"]
            ])

            poll_until(fn ->
              :erpc.call(r2, Group, :lookup, [@name, key, [cluster: "game"]]) != nil
            end)
          end)

        us
      end)
      |> Enum.sort()

    report_latency("register on r1 → visible on r2 (cluster: \"game\")", samples)

    stop_groups(replicas)
  end
end
