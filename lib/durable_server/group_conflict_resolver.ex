defmodule DurableServer.GroupConflictResolver do
  @moduledoc false

  _ = """
  DurableServer-specific conflict resolution for group registry conflicts.

  When a network partition heals or race conditions occur, multiple processes may
  have claimed the same key. Since DurableServers use object storage as a distributed
  lock, registry conflicts are extremely rare in practice. When they do occur, we
  kill both processes and let the system restart cleanly — the storage lock ensures
  only one will successfully re-acquire the key.

  This callback runs synchronously inside the Group shard GenServer, so it must
  never block. We intentionally avoid any synchronous work (no GenServer.call to
  the conflicting processes, no storage lookups) and just kill both immediately.

  This module is registered as the `:resolve_registry_conflict` callback for
  Group instances started by DurableServer.Supervisor.
  """

  require Logger

  alias DurableServer.GroupMeta

  def resolve(name, key, {pid1, %GroupMeta{}, _time1}, {pid2, %GroupMeta{}, _time2}) do
    Logger.error(fn ->
      "#{inspect(__MODULE__)}: registry conflict detected: " <>
        "name=#{inspect(name)}, key=#{inspect(key)}, " <>
        "pid1=#{inspect(pid1)}, pid2=#{inspect(pid2)}, killing both for clean restart"
    end)

    DurableServer.fatal_exit!(pid1, :registry_conflict)
    DurableServer.fatal_exit!(pid2, :registry_conflict)

    # Return pid1 as nominal "winner" — both are killed, so Group will briefly
    # keep pid1's entry until its DOWN handler fires and cleans up the key.
    pid1
  end

  # Non-DurableServer keys (e.g. sprite cache entries) — fall back to Group's
  # default behavior: most recent timestamp wins, pid ordering as tiebreaker.
  def resolve(_name, key, {pid1, _meta1, time1}, {pid2, meta2, time2}) do
    {winner_pid, loser_pid} =
      if time2 > time1 or (time2 == time1 and pid2 > pid1), do: {pid2, pid1}, else: {pid1, pid2}

    Logger.error(fn ->
      "#{inspect(__MODULE__)}: registry conflict detected: key=#{inspect(key)}, " <>
        "pid1=#{inspect(pid1)}, pid2=#{inspect(pid2)}, picking #{inspect(winner_pid)} as winner"
    end)

    Process.exit(loser_pid, {:group_registry_conflict, key, meta2})
    winner_pid
  end
end
