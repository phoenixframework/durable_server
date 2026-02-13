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
end
