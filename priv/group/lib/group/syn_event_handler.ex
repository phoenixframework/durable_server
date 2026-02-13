defmodule Group.SynEventHandler do
  @moduledoc false

  _ = """
  `:syn` event handler for resolving registry conflicts and dispatching lifecycle events.

  Conflict resolution is configurable per Group instance via the `:resolve_registry_conflict`
  option passed to `Group.start_link/1`. When no custom resolver is configured, the default
  behavior is to keep the most recent registration and kill the loser.
  """

  require Logger

  @behaviour :syn_event_handler

  @impl true
  def resolve_registry_conflict(scope, key, info1, info2) do
    case get_conflict_resolver_for_scope(scope) do
      nil ->
        resolve_default_conflict(scope, key, info1, info2)

      {mod, func, extra_args} ->
        name = name_from_scope(scope)
        apply(mod, func, [name, key, info1, info2 | extra_args])
    end
  end

  defp resolve_default_conflict(
         scope,
         key,
         {pid1, meta1, time1},
         {pid2, meta2, time2}
       ) do
    {winner_pid, _winner_meta, loser_pid} =
      if time2 >= time1, do: {pid2, meta2, pid1}, else: {pid1, meta1, pid2}

    Logger.error(fn ->
      "#{inspect(__MODULE__)}: registry conflict detected: scope=#{inspect(scope)}, key=#{inspect(key)}, " <>
        "pid1=#{inspect(pid1)}, pid2=#{inspect(pid2)}, picking #{inspect(winner_pid)} as winner"
    end)

    Process.exit(loser_pid, {:syn_resolve_kill, key, meta2})
    winner_pid
  end

  # Registry callbacks for lifecycle events
  # These fire on ALL cluster nodes when a process registers/unregisters

  @impl true
  def on_process_registered(scope, key, pid, meta, _reason) do
    case parse_scope(scope) do
      {name, cluster} ->
        Group.__dispatch__(name, :registered, key, pid, meta, %{
          previous_meta: nil,
          cluster: cluster
        })

      nil ->
        :ok
    end

    :ok
  end

  @impl true
  def on_process_unregistered(scope, key, pid, meta, reason) do
    case parse_scope(scope) do
      {name, cluster} ->
        Group.__dispatch__(name, :unregistered, key, pid, meta, %{
          reason: reason,
          cluster: cluster
        })

      nil ->
        :ok
    end

    :ok
  end

  @impl true
  def on_registry_process_updated(_scope, _key, _pid, _meta, _reason) do
    # no-op: the 6-arity version fires right after with previous_meta included
    :ok
  end

  @impl true
  def on_registry_process_updated(scope, key, pid, previous_meta, meta, _reason) do
    # 6-arity version with previous_meta
    case parse_scope(scope) do
      {name, cluster} ->
        Group.__dispatch__(name, :registered, key, pid, meta, %{
          previous_meta: previous_meta,
          cluster: cluster
        })

      nil ->
        :ok
    end

    :ok
  end

  # Process group callbacks for joined pids
  # These fire on ALL cluster nodes when a process joins/leaves a key

  @impl true
  def on_process_joined(scope, group, pid, meta, _reason) do
    case parse_scope(scope) do
      {name, cluster} ->
        Group.__dispatch__(name, :joined, group, pid, meta, %{
          previous_meta: nil,
          cluster: cluster
        })

      nil ->
        :ok
    end

    :ok
  end

  @impl true
  def on_group_process_updated(_scope, _group, _pid, _meta, _reason) do
    # no-op: the 6-arity version fires right after with previous_meta included
    :ok
  end

  @impl true
  def on_group_process_updated(scope, group, pid, previous_meta, meta, _reason) do
    case parse_scope(scope) do
      {name, cluster} ->
        Group.__dispatch__(name, :joined, group, pid, meta, %{
          previous_meta: previous_meta,
          cluster: cluster
        })

      nil ->
        :ok
    end

    :ok
  end

  @impl true
  def on_process_left(scope, group, pid, meta, reason) do
    case parse_scope(scope) do
      {name, cluster} ->
        Group.__dispatch__(name, :left, group, pid, meta, %{
          reason: reason,
          cluster: cluster
        })

      nil ->
        :ok
    end

    :ok
  end

  # Parse a syn scope into {name, cluster_name}
  # Returns nil for non-group scopes
  # Uses __cluster__ (double underscore) as delimiter to avoid ambiguity with
  # names that contain "_cluster_"
  defp parse_scope(scope) do
    scope_str = to_string(scope)

    cond do
      String.contains?(scope_str, "__cluster__") ->
        # Named cluster: "group_MySup__cluster__game_servers"
        [prefix_and_name, cluster] = String.split(scope_str, "__cluster__", parts: 2)
        name = String.trim_leading(prefix_and_name, "group_")
        {String.to_atom(name), String.to_atom(cluster)}

      String.starts_with?(scope_str, "group_") ->
        # Default cluster: "group_MySup"
        name = String.trim_leading(scope_str, "group_")
        {String.to_atom(name), nil}

      true ->
        nil
    end
  end

  # Look up the conflict resolver MFA for a given scope from persistent_term
  defp get_conflict_resolver_for_scope(scope) do
    case parse_scope(scope) do
      {name, _cluster} ->
        case Group.get_config(name) do
          %{resolve_registry_conflict: mfa} -> mfa
          _ -> nil
        end

      nil ->
        nil
    end
  end

  # Extract the Group name from a scope atom
  defp name_from_scope(scope) do
    case parse_scope(scope) do
      {name, _cluster} -> name
      nil -> nil
    end
  end
end
