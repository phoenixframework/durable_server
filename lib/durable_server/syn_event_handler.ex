defmodule DurableServer.SynEventHandler do
  @moduledoc false

  _ = """
  `:syn` event handler for resolving registry conflicts between DurableServer processes.

  When a network partition heals or race conditions occur, multiple processes may
  have claimed the same key. This handler resolves conflicts by:

  1. Checking which process has the valid (most recent) etag from object storage
  2. Killing both processes to let the system restart cleanly

  This ensures that only the process with the correct view of storage state survives,
  preventing "object updated out from underneath" errors.
  """

  require Logger

  @behaviour :syn_event_handler

  alias DurableServer.UUID

  @impl true
  def resolve_registry_conflict(
        scope,
        key,
        {_pid1, _meta1, _time1} = info1,
        {_pid2, _meta2, _time2} = info2
      ) do
    if String.starts_with?(to_string(scope), "durable_") do
      resolve_durable_conflict(scope, key, info1, info2)
    else
      resolve_any_conflict(scope, key, info1, info2)
    end
  end

  defp resolve_any_conflict(
         scope,
         key,
         {pid1, meta1, time1},
         {pid2, meta2, time2}
       ) do
    {winner_pid, winner_meta, loser_pid} =
      if time2 >= time1, do: {pid2, meta2, pid1}, else: {pid1, meta1, pid2}

    trace_ref = UUID.uuid4()

    log(trace_ref, :error, fn ->
      "registry conflict detected: scope=#{inspect(scope)}, key=#{inspect(key)}, " <>
        "pid1=#{inspect(pid1)}, pid2=#{inspect(pid2)}, picking #{inspect(winner_pid)} as winner"
    end)

    Process.exit(loser_pid, {:syn_resolve_kill, key, winner_meta})
    winner_pid
  end

  defp resolve_durable_conflict(
         scope,
         key,
         {pid1, meta1, _time1},
         {pid2, meta2, _time2}
       ) do
    trace_ref = UUID.uuid4()

    log(trace_ref, :error, fn ->
      "registry conflict detected: scope=#{inspect(scope)}, key=#{inspect(key)}, " <>
        "pid1=#{inspect(pid1)}, pid2=#{inspect(pid2)}"
    end)

    # Extract etags from metadata (if available)
    etag1 = get_etag_from_process(pid1, meta1)
    etag2 = get_etag_from_process(pid2, meta2)

    log(trace_ref, :error, fn ->
      "conflict etags: pid1=#{inspect(pid1)} etag=#{inspect(etag1)}, " <>
        "pid2=#{inspect(pid2)} etag=#{inspect(etag2)}"
    end)

    # Determine which etag is valid by checking object storage
    winner_pid =
      case {etag1, etag2} do
        {nil, nil} ->
          # no etags available, kill both and let the system restart
          log(trace_ref, :error, fn -> "no etags available for either process, killing both" end)

          DurableServer.fatal_exit!(pid1, :registry_conflict)
          DurableServer.fatal_exit!(pid2, :registry_conflict)
          pid1

        {nil, _etag2} ->
          # only pid2 has an etag, prefer it but still kill both
          log(trace_ref, :error, fn -> "only pid2 has etag, killing both" end)
          DurableServer.fatal_exit!(pid1, :registry_conflict)
          DurableServer.fatal_exit!(pid2, :registry_conflict)
          pid2

        {_etag1, nil} ->
          # only pid1 has an etag, prefer it but still kill both
          log(trace_ref, :error, fn -> "only pid1 has etag, killing both" end)
          DurableServer.fatal_exit!(pid1, :registry_conflict)
          DurableServer.fatal_exit!(pid2, :registry_conflict)
          pid1

        {etag1, etag2} ->
          # both have etags, check which one is valid in storage
          resolve_with_storage_check(trace_ref, key, pid1, etag1, pid2, etag2, meta1)
      end

    log(trace_ref, :error, fn ->
      "conflict resolved: keeping pid=#{inspect(winner_pid)} (both killed for clean restart)"
    end)

    winner_pid
  end

  # Try to get etag from process state
  defp get_etag_from_process(pid, meta) do
    # First try to get it from metadata if we stored it there
    case Map.get(meta, :etag) do
      nil ->
        # Try to get it from process state via GenServer call
        try do
          case GenServer.call(pid, :get_etag, 1000) do
            {:ok, etag} -> etag
            _ -> nil
          end
        catch
          :exit, _ -> nil
        end

      etag ->
        etag
    end
  end

  defp resolve_with_storage_check(trace_ref, key, pid1, etag1, pid2, etag2, meta1) do
    # Get storage configuration from metadata
    case fetch_current_storage_state(key, meta1) do
      {:ok, current_etag} ->
        log(trace_ref, :error, fn ->
          "current storage etag: #{inspect(current_etag)}, " <>
            "pid1 etag: #{inspect(etag1)}, pid2 etag: #{inspect(etag2)}"
        end)

        # Kill both processes for clean restart, but return the winner
        # based on which etag matches storage
        cond do
          etag1 == current_etag and etag2 == current_etag ->
            # Both match (shouldn't happen), kill both anyway
            log(trace_ref, :error, fn -> "both etags match storage (unexpected), killing both" end)

            DurableServer.fatal_exit!(pid1, :registry_conflict)
            DurableServer.fatal_exit!(pid2, :registry_conflict)
            pid1

          etag1 == current_etag ->
            # pid1 has the valid etag
            log(trace_ref, :error, fn -> "pid1 has valid etag, killing pid2" end)
            DurableServer.fatal_exit!(pid2, :registry_conflict)
            pid1

          etag2 == current_etag ->
            # pid2 has the valid etag
            log(trace_ref, :error, fn -> "pid2 has valid etag, killing pid1" end)
            DurableServer.fatal_exit!(pid1, :registry_conflict)
            pid2

          true ->
            # neither matches (storage was updated by someone else), kill both
            log(trace_ref, :error, fn ->
              "neither etag matches storage (current: #{inspect(current_etag)}), killing both"
            end)

            DurableServer.fatal_exit!(pid1, :registry_conflict)
            DurableServer.fatal_exit!(pid2, :registry_conflict)
            pid1
        end

      {:error, reason} ->
        # can't check storage, kill both for safety
        log(trace_ref, :error, fn ->
          "failed to fetch storage state: #{inspect(reason)}, killing both"
        end)

        DurableServer.fatal_exit!(pid1, :registry_conflict)
        DurableServer.fatal_exit!(pid2, :registry_conflict)
        pid1
    end
  end

  defp fetch_current_storage_state(_key, meta) do
    # Extract storage configuration from metadata
    storage_key = Map.get(meta, :storage_key)
    supervisor = Map.get(meta, :supervisor)

    cond do
      is_nil(storage_key) ->
        {:error, :no_storage_key}

      is_nil(supervisor) ->
        {:error, :no_supervisor}

      true ->
        DurableServer.__fetch_stored_state_for_conflict_resolution__(supervisor, storage_key)
    end
  end

  defp log(trace_ref, level, msg_func) when is_atom(level) and is_function(msg_func, 0) do
    Logger.log(level, fn ->
      "[#{trace_ref}]: #{inspect(__MODULE__)}: " <> msg_func.()
    end)
  end
end
