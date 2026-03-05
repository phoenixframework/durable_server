defmodule DurableServer.SingleflightGuard do
  @moduledoc false
  use GenServer

  @sweep_interval_ms :timer.minutes(1)
  @stale_entry_ttl_ms :timer.minutes(10)

  def start_link(opts) when is_list(opts) do
    supervisor_name = Keyword.fetch!(opts, :supervisor_name)
    GenServer.start_link(__MODULE__, supervisor_name, name: process_name(supervisor_name))
  end

  def acquire(supervisor_name, singleflight_key, wait_timeout_ms, max_waiters)
      when is_atom(supervisor_name) and is_integer(wait_timeout_ms) and wait_timeout_ms > 0 do
    if is_integer(max_waiters) and max_waiters > 0 do
      table = table_name(supervisor_name)
      guard_key = guard_key(singleflight_key)
      now = System.monotonic_time(:millisecond)

      if cooldown_open?(table, guard_key, now) do
        case maybe_recover_from_stale_counter(
               supervisor_name,
               table,
               guard_key,
               singleflight_key,
               max_waiters,
               now
             ) do
          :ok ->
            acquire_with_counter(
              supervisor_name,
              table,
              guard_key,
              singleflight_key,
              wait_timeout_ms,
              max_waiters,
              now
            )

          {:overloaded, _actual_waiters, _repaired_count} ->
            {:error, :singleflight_overloaded}
        end
      else
        acquire_with_counter(
          supervisor_name,
          table,
          guard_key,
          singleflight_key,
          wait_timeout_ms,
          max_waiters,
          now
        )
      end
    else
      {:ok, nil}
    end
  rescue
    _ -> {:ok, nil}
  end

  def release(nil), do: :ok

  def release({table, guard_key}) when is_atom(table) do
    now = System.monotonic_time(:millisecond)
    _count = decrement_count(table, guard_key, now)

    case :ets.lookup(table, guard_key) do
      [{^guard_key, 0, cooldown_until, _updated_at}] when cooldown_until <= now ->
        :ets.delete(table, guard_key)
        :ok

      [{^guard_key, _, _, _}] ->
        :ets.update_element(table, guard_key, {4, now})
        :ok

      _ ->
        :ok
    end
  rescue
    _ -> :ok
  end

  @impl true
  def init(supervisor_name) when is_atom(supervisor_name) do
    table = table_name(supervisor_name)

    case :ets.whereis(table) do
      :undefined ->
        :ets.new(table, [
          :named_table,
          :set,
          :public,
          read_concurrency: true,
          write_concurrency: true
        ])

      _ ->
        :ok
    end

    schedule_sweep()
    {:ok, %{table: table}}
  end

  @impl true
  def handle_info(:sweep, %{table: table} = state) do
    sweep_table(table)
    schedule_sweep()
    {:noreply, state}
  end

  defp schedule_sweep do
    Process.send_after(self(), :sweep, @sweep_interval_ms)
  end

  defp decrement_count(table, guard_key, now) when is_atom(table) do
    :ets.update_counter(
      table,
      guard_key,
      {2, -1, 0, 0},
      {guard_key, 0, 0, now}
    )
  end

  defp cooldown_open?(table, guard_key, now) when is_atom(table) and is_integer(now) do
    case :ets.lookup(table, guard_key) do
      [{^guard_key, 0, cooldown_until, _updated_at}] when cooldown_until <= now ->
        :ets.delete(table, guard_key)
        false

      [{^guard_key, _count, cooldown_until, _updated_at}] when cooldown_until > now ->
        true

      _ ->
        false
    end
  end

  defp acquire_with_counter(
         supervisor_name,
         table,
         guard_key,
         singleflight_key,
         wait_timeout_ms,
         max_waiters,
         now
       ) do
    count =
      :ets.update_counter(
        table,
        guard_key,
        {2, 1},
        {guard_key, 0, 0, now}
      )

    :ets.update_element(table, guard_key, {4, now})

    if count > max_waiters do
      case maybe_recover_from_stale_counter(
             supervisor_name,
             table,
             guard_key,
             singleflight_key,
             max_waiters,
             now
           ) do
        :ok ->
          {:ok, {table, guard_key}}

        {:overloaded, _actual_waiters, _repaired_count} ->
          :ets.insert(table, {guard_key, count, now + wait_timeout_ms, now})
          decrement_count(table, guard_key, now)
          {:error, :singleflight_overloaded}
      end
    else
      {:ok, {table, guard_key}}
    end
  end

  defp maybe_recover_from_stale_counter(
         supervisor_name,
         table,
         guard_key,
         singleflight_key,
         max_waiters,
         now
       ) do
    actual_waiters = waiter_count(supervisor_name, singleflight_key)

    # Include the current caller's pending waiter slot in the repaired count.
    repaired_count = max(actual_waiters + 1, 1)

    if repaired_count <= max_waiters do
      :ets.insert(table, {guard_key, repaired_count, 0, now})
      :ok
    else
      {:overloaded, actual_waiters, repaired_count}
    end
  end

  defp waiter_count(supervisor_name, singleflight_key) when is_atom(supervisor_name) do
    waiters_registry = waiters_registry_name(supervisor_name)
    Registry.count_match(waiters_registry, singleflight_key, :_)
  rescue
    _ -> 0
  end

  defp sweep_table(table) when is_atom(table) do
    now = System.monotonic_time(:millisecond)
    stale_cutoff = now - @stale_entry_ttl_ms

    # prune fully released entries whose overload cooldown has elapsed
    :ets.select_delete(table, [
      {
        {:"$1", 0, :"$2", :"$3"},
        [{:"=<", :"$2", now}],
        [true]
      }
    ])

    # failsafe cleanup for leaked entries from abruptly-killed waiters
    :ets.select_delete(table, [
      {
        {:"$1", :"$2", :"$3", :"$4"},
        [{:>, :"$2", 0}, {:"=<", :"$4", stale_cutoff}],
        [true]
      }
    ])

    :ok
  rescue
    _ -> :ok
  end

  defp guard_key({:ensure_started_child, key, module}) when is_binary(key) and is_atom(module) do
    {key, module}
  end

  defp guard_key(singleflight_key), do: singleflight_key

  defp table_name(supervisor_name) do
    :"durable_sf_waiter_guard_#{supervisor_name}"
  end

  defp process_name(supervisor_name) do
    :"durable_sf_guard_#{supervisor_name}"
  end

  defp waiters_registry_name(supervisor_name) do
    :"durable_sf_waiters_#{supervisor_name}"
  end
end
