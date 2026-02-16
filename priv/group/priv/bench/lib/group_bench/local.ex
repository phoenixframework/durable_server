defmodule GroupBench.Local do
  @moduledoc """
  Local (single-node) benchmarks for Group.
  """

  import GroupBench.Helpers

  @name :bench
  @default_shards System.schedulers_online()

  def run do
    header("Local Benchmarks")
    IO.puts("  schedulers_online: #{System.schedulers_online()}")

    bench_lookup()
    bench_members()
    bench_register_shards()
    bench_register_unregister_cycle()
    bench_join_shards()
    bench_monitor_events()

    IO.puts("\n  Done.\n")
  end

  # ── 1. lookup throughput ──────────────────────────────────────────────

  defp bench_lookup do
    header("1. Lookup Throughput (ETS read)")

    for {cluster_label, cluster_opt} <- clusters() do
      subheader("cluster: #{cluster_label}")

      with_group([name: @name, shards: @default_shards], fn ->
        maybe_connect_cluster(cluster_opt)
        key_count = 10_000
        measure_count = 100_000

        # Each process registers itself
        register_from_spawned_processes(key_count, fn i ->
          Group.register(@name, "key-#{i}", %{i: i}, cluster_opts(cluster_opt))
        end)

        # warmup
        warmup(1_000, fn -> Group.lookup(@name, "key-1", cluster_opts(cluster_opt)) end)

        # measure
        samples =
          collect_samples(measure_count, fn ->
            i = :rand.uniform(key_count)
            Group.lookup(@name, "key-#{i}", cluster_opts(cluster_opt))
          end)

        report_latency("Group.lookup/3", samples)
      end)
    end
  end

  # ── 2. members throughput ─────────────────────────────────────────────

  defp bench_members do
    header("2. Members Throughput (ETS read)")

    for {cluster_label, cluster_opt} <- clusters() do
      subheader("cluster: #{cluster_label}")

      with_group([name: @name, shards: @default_shards], fn ->
        maybe_connect_cluster(cluster_opt)
        group_count = 100
        members_per_group = 100
        measure_count = 100_000

        total = group_count * members_per_group

        # Each process joins a group
        register_from_spawned_processes(total, fn i ->
          gi = rem(i - 1, group_count) + 1
          Group.join(@name, "group-#{gi}", %{}, cluster_opts(cluster_opt))
        end)

        warmup(1_000, fn -> Group.members(@name, "group-1", cluster_opts(cluster_opt)) end)

        samples =
          collect_samples(measure_count, fn ->
            gi = :rand.uniform(group_count)
            Group.members(@name, "group-#{gi}", cluster_opts(cluster_opt))
          end)

        report_latency("Group.members/3", samples)
      end)
    end
  end

  # ── 3. register throughput (shard scaling) ────────────────────────────

  defp bench_register_shards do
    header("3. Register Throughput (shard scaling)")

    n = 10_000
    shard_counts = Enum.uniq([1, 2, 4, @default_shards])

    for {cluster_label, cluster_opt} <- clusters() do
      subheader("cluster: #{cluster_label}")

      for shards <- shard_counts do
        with_group([name: @name, shards: shards], fn ->
          maybe_connect_cluster(cluster_opt)

          {wall_us, _} =
            time_us(fn ->
              register_from_spawned_processes(n, fn i ->
                Group.register(@name, "reg-#{i}", %{}, cluster_opts(cluster_opt))
              end)
            end)

          report_throughput("shards=#{shards}", n, wall_us)
        end)
      end
    end
  end

  # ── 4. register/unregister cycle ──────────────────────────────────────

  defp bench_register_unregister_cycle do
    header("4. Register/Unregister Cycle")

    n = 10_000

    for {cluster_label, cluster_opt} <- clusters() do
      subheader("cluster: #{cluster_label}")

      with_group([name: @name, shards: @default_shards], fn ->
        maybe_connect_cluster(cluster_opt)
        opts = cluster_opts(cluster_opt)

        # register/unregister from self — self() is the calling process
        samples =
          collect_samples(n, fn ->
            key = "cycle-#{:erlang.unique_integer([:positive])}"
            :ok = Group.register(@name, key, %{}, opts)
            :ok = Group.unregister(@name, key, opts)
          end)

        report_latency("register+unregister", samples)
      end)
    end
  end

  # ── 5. join throughput (shard scaling) ────────────────────────────────

  defp bench_join_shards do
    header("5. Join Throughput (shard scaling)")

    n = 10_000
    shard_counts = Enum.uniq([1, 2, 4, @default_shards])

    for {cluster_label, cluster_opt} <- clusters() do
      subheader("cluster: #{cluster_label}")

      for shards <- shard_counts do
        with_group([name: @name, shards: shards], fn ->
          maybe_connect_cluster(cluster_opt)

          {wall_us, _} =
            time_us(fn ->
              register_from_spawned_processes(n, fn i ->
                Group.join(@name, "join-group-#{rem(i, 100)}", %{}, cluster_opts(cluster_opt))
              end)
            end)

          report_throughput("shards=#{shards}", n, wall_us)
        end)
      end
    end
  end

  # ── 6. monitor event delivery ─────────────────────────────────────────

  defp bench_monitor_events do
    header("6. Monitor Event Delivery")

    n = 5_000

    for {cluster_label, cluster_opt} <- clusters() do
      subheader("cluster: #{cluster_label}")

      with_group([name: @name, shards: @default_shards], fn ->
        maybe_connect_cluster(cluster_opt)
        :ok = Group.monitor(@name, :all, cluster_opts(cluster_opt))

        {wall_us, _} =
          time_us(fn ->
            register_from_spawned_processes(n, fn i ->
              Group.register(@name, "mon-#{i}", %{}, cluster_opts(cluster_opt))
            end)

            # drain all N events
            drain_events(n)
          end)

        report_throughput("events (register → receive)", n, wall_us)
      end)
    end
  end

  # ── Helpers ───────────────────────────────────────────────────────────

  defp clusters do
    [{"nil (default)", nil}, {"named (\"game\")", "game"}]
  end

  defp maybe_connect_cluster(nil), do: :ok
  defp maybe_connect_cluster(cluster), do: Group.connect(@name, cluster)

  defp cluster_opts(nil), do: []
  defp cluster_opts(cluster), do: [cluster: cluster]

  @doc false
  # Spawns N processes, each of which calls `fun.(index)` where index is 1..n.
  # Processes stay alive after the call. Waits for all to complete.
  defp register_from_spawned_processes(n, fun) do
    parent = self()

    pids =
      Enum.map(1..n, fn i ->
        spawn(fn ->
          result = fun.(i)
          send(parent, {:done, self(), result})

          # Stay alive so the registration/membership persists
          Process.sleep(:infinity)
        end)
      end)

    # Wait for all to complete
    Enum.each(pids, fn pid ->
      receive do
        {:done, ^pid, _result} -> :ok
      after
        10_000 -> raise "Timed out waiting for #{inspect(pid)}"
      end
    end)

    pids
  end

  defp drain_events(0), do: :ok

  defp drain_events(remaining) do
    receive do
      {:group, events, _info} ->
        count = Enum.count(events, &match?(%Group.Event{type: :registered}, &1))
        drain_events(remaining - count)
    after
      5_000 -> IO.puts("    WARNING: timed out waiting for events, #{remaining} remaining")
    end
  end
end
