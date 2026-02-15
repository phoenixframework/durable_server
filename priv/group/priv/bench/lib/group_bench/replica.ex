defmodule GroupBench.Replica do
  @moduledoc """
  Replica role for distributed benchmarks.

  Started as a separate BEAM VM by the coordinator. Also provides helper
  functions that the coordinator calls via :erpc.call MFA to avoid anonymous
  function serialization issues across nodes.
  """

  def start do
    Application.ensure_all_started(:group)
    IO.puts("[replica] #{node()} ready")
    Process.sleep(:infinity)
  end

  @doc """
  Starts a Group instance, unlinks from caller (safe for :erpc).
  """
  def start_group(opts) do
    {:ok, pid} = Group.start_link(opts)
    Process.unlink(pid)
    {:ok, pid}
  end

  @doc """
  Stops a Group instance by supervisor name.
  """
  def stop_group(name) do
    sup_name = :"#{name}_group_sup"

    case Process.whereis(sup_name) do
      nil -> :ok
      pid -> Supervisor.stop(pid)
    end
  end

  @doc """
  Registers N keys from spawned processes. Each process calls Group.register
  on its own behalf (register uses self()). Returns list of pids.
  """
  def bulk_register(name, n, key_prefix, opts \\ []) do
    parent = self()

    pids =
      Enum.map(1..n, fn i ->
        spawn(fn ->
          :ok = Group.register(name, "#{key_prefix}#{i}", %{}, opts)
          send(parent, {:done, self()})
          Process.sleep(:infinity)
        end)
      end)

    Enum.each(pids, fn pid ->
      receive do
        {:done, ^pid} -> :ok
      after
        30_000 -> raise "Timed out waiting for bulk_register"
      end
    end)

    pids
  end

  @doc """
  Counts total registry entries across all shards (all nodes, not just local).
  """
  def total_registry_count(name) do
    num_shards = Group.get_config(name).num_shards

    Enum.reduce(0..(num_shards - 1), 0, fn shard, acc ->
      table = Group.Replica.Data.reg_by_key_table(name, shard)
      acc + :ets.info(table, :size)
    end)
  end

  @doc """
  Registers a single key from a spawned process. Returns after registration.
  """
  def spawn_register(name, key, opts \\ []) do
    parent = self()

    spawn(fn ->
      :ok = Group.register(name, key, %{}, opts)
      send(parent, :registered)
      Process.sleep(:infinity)
    end)

    receive do
      :registered -> :ok
    after
      5_000 -> raise "spawn_register timed out"
    end
  end

  @doc """
  Spawns N processes, each joining the same group key. Returns list of pids.
  """
  def bulk_join(name, n, key, opts \\ []) do
    parent = self()

    pids =
      Enum.map(1..n, fn _i ->
        spawn(fn ->
          :ok = Group.join(name, key, %{}, opts)
          send(parent, {:done, self()})
          Process.sleep(:infinity)
        end)
      end)

    Enum.each(pids, fn pid ->
      receive do
        {:done, ^pid} -> :ok
      after
        30_000 -> raise "Timed out waiting for bulk_join"
      end
    end)

    pids
  end

  @doc """
  Kills a list of processes.
  """
  def kill_processes(pids) do
    Enum.each(pids, &Process.exit(&1, :kill))
    :ok
  end

  @doc """
  Counts total pg entries across all shards.
  """
  def total_pg_count(name) do
    num_shards = Group.get_config(name).num_shards

    Enum.reduce(0..(num_shards - 1), 0, fn shard, acc ->
      table = Group.Replica.Data.pg_by_key_table(name, shard)
      acc + :ets.info(table, :size)
    end)
  end

  @doc """
  Connects N named clusters concurrently (one per caller, like real usage).
  """
  def bulk_connect(name, n, prefix) do
    parent = self()

    Enum.map(1..n, fn i ->
      spawn(fn ->
        :ok = Group.connect(name, "#{prefix}#{i}")
        send(parent, {:done, self()})
      end)
    end)
    |> Enum.each(fn pid ->
      receive do
        {:done, ^pid} -> :ok
      after
        120_000 -> raise "Timed out waiting for bulk_connect"
      end
    end)
  end

  @doc """
  Disconnects N named clusters concurrently (one per caller, like real usage).
  """
  def bulk_disconnect(name, n, prefix) do
    parent = self()

    Enum.map(1..n, fn i ->
      spawn(fn ->
        :ok = Group.disconnect(name, "#{prefix}#{i}")
        send(parent, {:done, self()})
      end)
    end)
    |> Enum.each(fn pid ->
      receive do
        {:done, ^pid} -> :ok
      after
        120_000 -> raise "Timed out waiting for bulk_disconnect"
      end
    end)
  end

  @doc """
  Spawns N processes, each registering key "key" in a distinct cluster.
  Returns list of pids.
  """
  def bulk_register_per_cluster(name, n, prefix) do
    parent = self()

    pids =
      Enum.map(1..n, fn i ->
        spawn(fn ->
          :ok = Group.register(name, "key", %{}, cluster: "#{prefix}#{i}")
          send(parent, {:done, self()})
          Process.sleep(:infinity)
        end)
      end)

    Enum.each(pids, fn pid ->
      receive do
        {:done, ^pid} -> :ok
      after
        60_000 -> raise "Timed out waiting for bulk_register_per_cluster"
      end
    end)

    pids
  end

  @doc """
  Returns the number of clusters this node is a member of (via reverse index).
  """
  def my_cluster_count(name) do
    length(Group.Replica.Data.my_clusters(name))
  end

  @doc """
  Simulates a busy app worker: registers, joins groups, does lookups and
  dispatches, then some processes die and re-register. Returns final pids.

  Each worker operates in its own cluster (like an org scope), performing
  a mix of operations that mirrors real app usage patterns.
  """
  def run_busy_worker(name, cluster, worker_id, opts) do
    num_users = Keyword.get(opts, :users, 20)
    num_rooms = Keyword.get(opts, :rooms, 5)
    churn_rounds = Keyword.get(opts, :churn_rounds, 3)
    lookups_per_round = Keyword.get(opts, :lookups_per_round, 50)
    dispatches_per_round = Keyword.get(opts, :dispatches_per_round, 10)
    parent = self()

    # Phase 1: Register users
    user_pids =
      Enum.map(1..num_users, fn i ->
        spawn(fn ->
          key = "w#{worker_id}/user/#{i}"
          :ok = Group.register(name, key, %{worker: worker_id, i: i}, cluster: cluster)
          send(parent, {:reg_done, self()})
          Process.sleep(:infinity)
        end)
      end)

    Enum.each(user_pids, fn pid ->
      receive do
        {:reg_done, ^pid} -> :ok
      after
        30_000 -> raise "busy_worker register timed out"
      end
    end)

    # Phase 2: Join rooms
    room_pids =
      Enum.map(1..num_rooms, fn room_i ->
        Enum.map(1..div(num_users, num_rooms), fn member_i ->
          spawn(fn ->
            key = "w#{worker_id}/room/#{room_i}"
            :ok = Group.join(name, key, %{seat: member_i}, cluster: cluster)
            send(parent, {:join_done, self()})
            Process.sleep(:infinity)
          end)
        end)
      end)
      |> List.flatten()

    Enum.each(room_pids, fn pid ->
      receive do
        {:join_done, ^pid} -> :ok
      after
        30_000 -> raise "busy_worker join timed out"
      end
    end)

    # Phase 3: Churn — interleave lookups, dispatches, kills, re-registers
    final_user_pids =
      Enum.reduce(1..churn_rounds, user_pids, fn round, current_pids ->
        # Lookups (read pressure)
        for i <- 1..lookups_per_round do
          key = "w#{worker_id}/user/#{rem(i, num_users) + 1}"
          Group.lookup(name, key, cluster: cluster)
        end

        # Dispatches (fan-out pressure)
        for i <- 1..dispatches_per_round do
          key = "w#{worker_id}/room/#{rem(i, num_rooms) + 1}"
          Group.dispatch(name, key, {:ping, round, i}, cluster: cluster)
        end

        # Kill ~25% of user pids
        kill_count = div(length(current_pids), 4)
        {to_kill, to_keep} = Enum.split(current_pids, kill_count)
        Enum.each(to_kill, &Process.exit(&1, :kill))

        # Re-register replacements
        new_pids =
          Enum.map(1..kill_count, fn i ->
            spawn(fn ->
              key = "w#{worker_id}/user/r#{round}_#{i}"
              :ok = Group.register(name, key, %{round: round}, cluster: cluster)
              send(parent, {:rereg_done, self()})
              Process.sleep(:infinity)
            end)
          end)

        Enum.each(new_pids, fn pid ->
          receive do
            {:rereg_done, ^pid} -> :ok
          after
            30_000 -> raise "busy_worker re-register timed out"
          end
        end)

        to_keep ++ new_pids
      end)

    final_user_pids ++ room_pids
  end
end
