defmodule GroupBench.Distributed do
  @moduledoc """
  Coordinator for distributed benchmarks.

  Expects replica1@127.0.0.1 and replica2@127.0.0.1 to already be running
  (started by run_distributed.sh). Connects to them, then drives benchmark
  scenarios via :erpc.call with MFA.
  """

  import GroupBench.Helpers

  @name :bench
  @replicas [:"replica1@127.0.0.1", :"replica2@127.0.0.1"]

  def run(opts \\ []) do
    shards = Keyword.get(opts, :shards, 8)
    Process.put(:bench_shards, shards)

    header("Distributed Benchmarks")
    IO.puts("  coordinator: #{node()}")
    IO.puts("  shards:      #{shards}")
    IO.puts("  schedulers:  #{System.schedulers_online()}")

    connect_replicas()

    bench_replication_latency(@replicas)
    bench_bulk_sync(@replicas)
    bench_concurrent_cross_node(@replicas)
    bench_named_cluster_replication(@replicas)
    bench_death_cleanup(@replicas)
    bench_churn(@replicas)
    bench_join_death_cleanup(@replicas)
    bench_many_clusters(@replicas)
    bench_busy_app(@replicas)

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
    shards = Process.get(:bench_shards, 8)
    opts = Keyword.merge([name: @name, shards: shards, log: false], opts)
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

  # ── 5. Process death cleanup ─────────────────────────────────────

  defp bench_death_cleanup([r1, r2] = replicas) do
    header("5. Process Death Cleanup Replication")

    for n <- [1_000, 5_000] do
      subheader("#{format_number(n)} processes")

      start_group_on(r1)
      start_group_on(r2)
      wait_for_peer_discovery(replicas)

      # Register N on r1
      pids = :erpc.call(r1, GroupBench.Replica, :bulk_register, [@name, n, "death-"], 60_000)

      # Wait for replication to r2
      poll_until(
        fn ->
          :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) >= n
        end,
        30_000
      )

      # Kill all registered processes, measure cleanup convergence
      {cleanup_us, _} =
        :timer.tc(fn ->
          :erpc.call(r1, GroupBench.Replica, :kill_processes, [pids])

          poll_until(
            fn ->
              :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) == 0
            end,
            30_000
          )
        end)

      rate = if cleanup_us > 0, do: round(n * 1_000_000 / cleanup_us), else: 0

      IO.puts("  cleanup:    #{format_number(div(cleanup_us, 1000))} ms")
      IO.puts("  deaths/sec: #{format_number(rate)}")

      stop_groups(replicas)
    end
  end

  # ── 6. Register/die churn ────────────────────────────────────────

  defp bench_churn([r1, r2] = replicas) do
    header("6. Register/Die Churn Throughput")

    waves = 10
    per_wave = 500
    total = waves * per_wave

    start_group_on(r1)
    start_group_on(r2)
    wait_for_peer_discovery(replicas)

    {wall_us, _} =
      :timer.tc(fn ->
        for w <- 1..waves do
          pids =
            :erpc.call(
              r1,
              GroupBench.Replica,
              :bulk_register,
              [@name, per_wave, "churn-w#{w}-"],
              60_000
            )

          :erpc.call(r1, GroupBench.Replica, :kill_processes, [pids])
        end

        # Wait for r2 to converge to 0
        poll_until(
          fn ->
            :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) == 0
          end,
          30_000
        )
      end)

    report_throughput("register+die waves (#{waves}x#{per_wave})", total, wall_us)

    stop_groups(replicas)
  end

  # ── 7. Join/die cleanup ──────────────────────────────────────────

  defp bench_join_death_cleanup([r1, r2] = replicas) do
    header("7. Join/Die Cleanup Replication")

    n = 1_000

    start_group_on(r1)
    start_group_on(r2)
    wait_for_peer_discovery(replicas)

    # Spawn N processes on r1, each joins the same group
    pids = :erpc.call(r1, GroupBench.Replica, :bulk_join, [@name, n, "room"], 60_000)

    # Wait for replication to r2
    poll_until(
      fn ->
        :erpc.call(r2, GroupBench.Replica, :total_pg_count, [@name]) >= n
      end,
      30_000
    )

    # Kill all joined processes, measure cleanup convergence
    {cleanup_us, _} =
      :timer.tc(fn ->
        :erpc.call(r1, GroupBench.Replica, :kill_processes, [pids])

        poll_until(
          fn ->
            :erpc.call(r2, GroupBench.Replica, :total_pg_count, [@name]) == 0
          end,
          30_000
        )
      end)

    rate = if cleanup_us > 0, do: round(n * 1_000_000 / cleanup_us), else: 0

    IO.puts("  cleanup:    #{format_number(div(cleanup_us, 1000))} ms")
    IO.puts("  deaths/sec: #{format_number(rate)}")

    stop_groups(replicas)
  end

  # ── 8. Many clusters ───────────────────────────────────────────

  defp bench_many_clusters([r1, r2] = replicas) do
    header("8. Many Clusters (10,000 clusters)")

    num_clusters = 10_000
    prefix = "c-"

    # -- 8a. Cluster connect throughput --

    subheader("connect #{format_number(num_clusters)} clusters")

    start_group_on(r1)
    start_group_on(r2)
    wait_for_peer_discovery(replicas)

    {connect_us, _} =
      :timer.tc(fn ->
        t1 =
          Task.async(fn ->
            :erpc.call(r1, GroupBench.Replica, :bulk_connect, [@name, num_clusters, prefix], 120_000)
          end)

        t2 =
          Task.async(fn ->
            :erpc.call(r2, GroupBench.Replica, :bulk_connect, [@name, num_clusters, prefix], 120_000)
          end)

        Task.await_many([t1, t2], 120_000)

        # Wait for convergence — both nodes see each other in the last cluster
        poll_until(
          fn ->
            n1 = :erpc.call(r1, Group, :nodes, [@name, "#{prefix}#{num_clusters}"])
            n2 = :erpc.call(r2, Group, :nodes, [@name, "#{prefix}#{num_clusters}"])
            length(n1) >= 1 and length(n2) >= 1
          end,
          60_000
        )
      end)

    IO.puts("  connect:       #{format_number(div(connect_us, 1000))} ms")
    IO.puts("  clusters/sec:  #{format_number(round(num_clusters * 1_000_000 / connect_us))}")

    # -- 8b. Register 1 key per cluster --

    subheader("register across #{format_number(num_clusters)} clusters")

    {reg_us, pids} =
      :timer.tc(fn ->
        pids =
          :erpc.call(
            r1,
            GroupBench.Replica,
            :bulk_register_per_cluster,
            [@name, num_clusters, prefix],
            120_000
          )

        poll_until(
          fn ->
            :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) >= num_clusters
          end,
          60_000
        )

        pids
      end)

    IO.puts("  register+converge: #{format_number(div(reg_us, 1000))} ms")
    IO.puts("  ops/sec:           #{format_number(round(num_clusters * 1_000_000 / reg_us))}")

    # -- 8c. Peer re-discovery with many clusters --

    subheader("peer re-discovery with #{format_number(num_clusters)} clusters")

    # Stop Group on r2 (simulates crash), restart it, reconnect all clusters
    stop_group_on(r2)
    Process.sleep(500)

    {rediscovery_us, _} =
      :timer.tc(fn ->
        start_group_on(r2)
        wait_for_peer_discovery(replicas)

        :erpc.call(r2, GroupBench.Replica, :bulk_connect, [@name, num_clusters, prefix], 120_000)

        poll_until(
          fn ->
            :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) >= num_clusters
          end,
          120_000
        )
      end)

    IO.puts("  re-discovery:  #{format_number(div(rediscovery_us, 1000))} ms")

    # -- 8d. Disconnect cleanup --

    subheader("disconnect #{format_number(num_clusters)} clusters")

    {disconnect_us, _} =
      :timer.tc(fn ->
        :erpc.call(r1, GroupBench.Replica, :bulk_disconnect, [@name, num_clusters, prefix], 120_000)

        # Wait for r2 to see r1's entries cleaned from all clusters
        poll_until(
          fn ->
            :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) == 0
          end,
          60_000
        )
      end)

    IO.puts("  disconnect+cleanup: #{format_number(div(disconnect_us, 1000))} ms")
    IO.puts("  clusters/sec:       #{format_number(round(num_clusters * 1_000_000 / disconnect_us))}")

    # Kill leftover processes
    :erpc.call(r1, GroupBench.Replica, :kill_processes, [pids])
    stop_groups(replicas)
  end

  # ── 9. Busy app simulation ──────────────────────────────────────────

  defp bench_busy_app([r1, r2] = replicas) do
    header("9. Busy App Simulation")

    num_clusters = 50
    workers_per_node = 10
    users_per_worker = 20
    rooms_per_worker = 5
    churn_rounds = 3
    lookups_per_round = 50
    dispatches_per_round = 10

    total_users = num_clusters * workers_per_node * 2 * users_per_worker
    total_rooms = num_clusters * workers_per_node * 2 * rooms_per_worker * div(users_per_worker, rooms_per_worker)

    IO.puts("  clusters:      #{num_clusters}")
    IO.puts("  workers/node:  #{workers_per_node}")
    IO.puts("  users/worker:  #{users_per_worker}")
    IO.puts("  rooms/worker:  #{rooms_per_worker}")
    IO.puts("  churn rounds:  #{churn_rounds}")
    IO.puts("  initial pids:  #{format_number(total_users + total_rooms)} (#{format_number(total_users)} reg + #{format_number(total_rooms)} pg)")
    IO.puts("")

    start_group_on(r1)
    start_group_on(r2)
    wait_for_peer_discovery(replicas)

    # Connect all clusters on both nodes
    clusters = for i <- 1..num_clusters, do: "org/#{i}"

    for node <- replicas, cluster <- clusters do
      :erpc.call(node, Group, :connect, [@name, cluster])
    end

    # Wait for cluster membership to converge
    poll_until(fn ->
      Enum.all?(replicas, fn node ->
        n = :erpc.call(node, Group, :nodes, [@name, List.last(clusters)])
        length(n) >= 1
      end)
    end)

    worker_opts = [
      users: users_per_worker,
      rooms: rooms_per_worker,
      churn_rounds: churn_rounds,
      lookups_per_round: lookups_per_round,
      dispatches_per_round: dispatches_per_round
    ]

    # Run all workers concurrently across both nodes
    {wall_us, all_pids} =
      :timer.tc(fn ->
        tasks =
          for {node, ni} <- Enum.with_index(replicas),
              {cluster, ci} <- Enum.with_index(clusters),
              wi <- 1..workers_per_node do
            worker_id = "n#{ni}_c#{ci}_w#{wi}"

            Task.async(fn ->
              :erpc.call(
                node,
                GroupBench.Replica,
                :run_busy_worker,
                [@name, cluster, worker_id, worker_opts],
                120_000
              )
            end)
          end

        results = Task.await_many(tasks, 120_000)
        List.flatten(results)
      end)

    total_ops =
      num_clusters * workers_per_node * 2 *
        (users_per_worker + div(users_per_worker, rooms_per_worker) * rooms_per_worker +
           churn_rounds * (lookups_per_round + dispatches_per_round + div(users_per_worker, 4) * 2))

    wall_ms = div(wall_us, 1000)
    ops_sec = if wall_us > 0, do: round(total_ops * 1_000_000 / wall_us), else: 0

    IO.puts("  --- results ---")
    IO.puts("  wall time:     #{format_number(wall_ms)} ms")
    IO.puts("  total ops:     ~#{format_number(total_ops)}")
    IO.puts("  ops/sec:       ~#{format_number(ops_sec)}")

    # Convergence check: wait for both nodes to agree on registry count
    subheader("convergence")

    {converge_us, _} =
      :timer.tc(fn ->
        poll_until(
          fn ->
            c1 = :erpc.call(r1, GroupBench.Replica, :total_registry_count, [@name])
            c2 = :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name])
            c1 == c2 and c1 > 0
          end,
          30_000
        )
      end)

    final_r1 = :erpc.call(r1, GroupBench.Replica, :total_registry_count, [@name])
    final_r2 = :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name])
    pg_r1 = :erpc.call(r1, GroupBench.Replica, :total_pg_count, [@name])
    pg_r2 = :erpc.call(r2, GroupBench.Replica, :total_pg_count, [@name])

    IO.puts("  converge:      #{format_number(div(converge_us, 1000))} ms")
    IO.puts("  registry r1:   #{format_number(final_r1)}")
    IO.puts("  registry r2:   #{format_number(final_r2)}")
    IO.puts("  pg r1:         #{format_number(pg_r1)}")
    IO.puts("  pg r2:         #{format_number(pg_r2)}")
    IO.puts("  match:         #{if final_r1 == final_r2 and pg_r1 == pg_r2, do: "YES", else: "NO"}")

    # Cleanup
    for node <- replicas do
      local_pids = Enum.filter(all_pids, fn pid -> node(pid) == node end)
      :erpc.call(node, GroupBench.Replica, :kill_processes, [local_pids])
    end

    stop_groups(replicas)
  end
end
