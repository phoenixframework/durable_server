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
          case GenServer.call(pid, {:durable, :get_etag}, 1000) do
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

  # Registry callbacks for DurableServer lifecycle events
  # These fire on ALL cluster nodes when a DurableServer registers/unregisters

  @impl true
  def on_process_registered(scope, key, pid, meta, _reason) do
    case parse_scope(scope) do
      {supervisor_name, cluster} when is_binary(key) ->
        DurableServer.Cluster.__broadcast__(supervisor_name, :registered, key, pid, meta, %{
          cluster: cluster
        })

      _ ->
        :ok
    end

    :ok
  end

  @impl true
  def on_process_unregistered(scope, key, pid, meta, reason) do
    case parse_scope(scope) do
      {supervisor_name, cluster} when is_binary(key) ->
        DurableServer.Cluster.__broadcast__(supervisor_name, :unregistered, key, pid, meta, %{
          reason: reason,
          cluster: cluster
        })

      _ ->
        :ok
    end

    :ok
  end

  @impl true
  def on_registry_process_updated(scope, key, pid, meta, _reason) do
    # 5-arity version without previous_meta
    case parse_scope(scope) do
      {supervisor_name, cluster} when is_binary(key) ->
        DurableServer.Cluster.__broadcast__(supervisor_name, :updated, key, pid, meta, %{
          cluster: cluster
        })

      _ ->
        :ok
    end

    :ok
  end

  @impl true
  def on_registry_process_updated(scope, key, pid, previous_meta, meta, _reason) do
    # 6-arity version with previous_meta
    case parse_scope(scope) do
      {supervisor_name, cluster} when is_binary(key) ->
        DurableServer.Cluster.__broadcast__(supervisor_name, :updated, key, pid, meta, %{
          previous_meta: extract_user_meta(previous_meta),
          cluster: cluster
        })

      _ ->
        :ok
    end

    :ok
  end

  # Process group callbacks for joined pids
  # These fire on ALL cluster nodes when a non-DurableServer process joins/leaves a key

  @impl true
  def on_process_joined(scope, group, pid, meta, _reason) do
    # Only dispatch for string keys (actual keys), not atom groups (sup_name, module)
    case parse_scope(scope) do
      {supervisor_name, cluster} when is_binary(group) ->
        DurableServer.Cluster.__broadcast__(supervisor_name, :joined, group, pid, meta, %{
          cluster: cluster
        })

      _ ->
        :ok
    end

    :ok
  end

  @impl true
  def on_process_left(scope, group, pid, meta, reason) do
    # Only dispatch for string keys (actual keys), not atom groups (sup_name, module)
    case parse_scope(scope) do
      {supervisor_name, cluster} when is_binary(group) ->
        DurableServer.Cluster.__broadcast__(supervisor_name, :left, group, pid, meta, %{
          reason: reason,
          cluster: cluster
        })

      _ ->
        :ok
    end

    :ok
  end

  # Parse a syn scope into {supervisor_name, cluster_name}
  # Returns nil for non-durable scopes
  # Uses __cluster__ (double underscore) as delimiter to avoid ambiguity with
  # supervisor names that contain "_cluster_"
  defp parse_scope(scope) do
    scope_str = to_string(scope)

    cond do
      String.contains?(scope_str, "__cluster__") ->
        # Named cluster: "durable_MySup__cluster__game_servers"
        [prefix_and_sup, cluster] = String.split(scope_str, "__cluster__", parts: 2)
        sup = String.trim_leading(prefix_and_sup, "durable_")
        {String.to_atom(sup), String.to_atom(cluster)}

      String.starts_with?(scope_str, "durable_") ->
        # Default cluster: "durable_MySup"
        sup = String.trim_leading(scope_str, "durable_")
        {String.to_atom(sup), nil}

      true ->
        nil
    end
  end

  defp extract_user_meta(%{user_meta: user_meta}), do: user_meta
  defp extract_user_meta(meta) when is_map(meta), do: meta
end
