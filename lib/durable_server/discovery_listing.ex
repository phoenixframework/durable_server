defmodule DurableServer.DiscoveryListing do
  @moduledoc false
  use GenServer

  alias DurableServer.{Backends, ObjectStore, StorageBackend}
  alias DurableServer.Supervisor, as: DurableSup

  # A slow or disconnected peer must not hold up recovery. After a failure the
  # rest of this scan reads S3 directly, starting at the same continuation token.
  @peer_timeout 2_000

  def start_link(
        {sup, %StorageBackend{adapter: Backends.ObjectStore, state: store}, prefix, interval,
         true}
      ) do
    GenServer.start_link(__MODULE__, {sup, store, prefix, interval}, name: name(sup))
  end

  def start_link(_), do: :ignore

  defp name(sup), do: :"#{sup}_discovery_listing"

  def stream(
        sup,
        %StorageBackend{adapter: Backends.ObjectStore, state: store} = backend,
        prefix,
        opts
      ) do
    table = name(sup)

    case scanner(sup, table) do
      nil -> StorageBackend.list_all_objects_stream(backend, prefix, opts)
      pid -> shared_pages(pid, table, store, prefix, Keyword.fetch!(opts, :error_handler))
    end
  end

  def stream(_sup, backend, prefix, opts),
    do: StorageBackend.list_all_objects_stream(backend, prefix, opts)

  defp scanner(sup, table) do
    # Old releases and supervisors with independent discovery do not join.
    with tid when tid != :undefined <- :ets.whereis(table),
         [{:group, group}] <- :ets.lookup(tid, :group),
         members when length(members) > 2 <-
           :pg.get_members(DurableSup.presence_pg_scope(sup), group) do
      replicas = members |> Enum.sort_by(&node/1) |> Enum.take(2)
      Enum.find(replicas, &(node(&1) == node())) || Enum.at(replicas, :erlang.phash2(node(), 2))
    else
      _ -> nil
    end
  end

  defp shared_pages(pid, table, store, prefix, on_error) do
    Stream.unfold({pid, nil}, fn
      :done ->
        nil

      {source, token} ->
        case read(source, table, store, prefix, token) do
          {:ok, entries, next, source} ->
            {entries, if(next, do: {source, next}, else: :done)}

          {:error, reason} ->
            on_error.(reason)
            nil
        end
    end)
    |> Stream.flat_map(& &1)
  end

  defp list_page(store, prefix, token) do
    ObjectStore.list_objects(store, prefix, consistent: false, continuation_token: token)
  end

  defp read(:s3, _table, store, prefix, token) do
    with {:ok, %{keys: entries, next_continuation_token: next}} <- list_page(store, prefix, token) do
      {:ok, entries, next, :s3}
    end
  end

  defp read(pid, table, store, prefix, token) do
    # An unchanged page only costs one digest across the distribution link.
    cached = if node(pid) != node(), do: :ets.lookup(table, {:copy, token}), else: []

    digest =
      case cached do
        [{_, _, hash, _}] -> hash
        [] -> nil
      end

    timeout = if node(pid) == node(), do: :infinity, else: @peer_timeout

    result =
      try do
        GenServer.call(pid, {:page, token, digest}, timeout)
      catch
        :exit, _ -> :unavailable
      end

    case result do
      {:same, next} ->
        [{_, _, _, entries}] = cached
        :ets.update_element(table, {:copy, token}, {2, now()})
        {:ok, entries, next, pid}

      {:page, hash, entries, next} ->
        if node(pid) != node(), do: :ets.insert(table, {{:copy, token}, now(), hash, entries})
        {:ok, entries, next, pid}

      {:error, _} = error when node(pid) == node() ->
        error

      _unavailable ->
        read(:s3, table, store, prefix, token)
    end
  end

  @impl true
  def init({sup, store, prefix, interval}) do
    table = :ets.new(name(sup), [:named_table, :public, :set])
    # Only exchange pages with peers that use the same namespace and read options.
    identity = {prefix, interval, Map.drop(store, [:finch, :task_supervisor])}

    group =
      {__MODULE__, 1, :crypto.hash(:sha256, :erlang.term_to_binary(identity, [:deterministic]))}

    :ets.insert(table, {:group, group})
    :ok = :pg.join(DurableSup.presence_pg_scope(sup), group, self())
    Process.send_after(self(), :expire, interval)

    {:ok,
     %{
       store: store,
       prefix: prefix,
       interval: interval,
       table: table,
       tasks: DurableSup.get_task_supervisor(sup),
       pages: %{},
       pending: %{},
       refs: %{}
     }}
  end

  @impl true
  def handle_call({:page, token, digest}, from, state) do
    reader = node(elem(from, 0))
    waiter = {from, digest}

    cond do
      Map.has_key?(state.pending, token) ->
        {:noreply, update_in(state.pending[token], &[waiter | &1])}

      reusable?(state.pages[token], reader, state.interval) ->
        page = state.pages[token]

        {:reply, reply_page(page, digest),
         put_in(state.pages[token].readers, MapSet.put(page.readers, reader))}

      true ->
        %{store: store, prefix: prefix} = state

        task =
          Task.Supervisor.async_nolink(state.tasks, fn -> list_page(store, prefix, token) end)

        {:noreply,
         %{
           state
           | pending: Map.put(state.pending, token, [waiter]),
             refs: Map.put(state.refs, task.ref, {token, now()})
         }}
    end
  end

  # In particular, the same node's back-to-back startup sweeps must get a fresh LIST.
  defp reusable?(nil, _reader, _interval), do: false

  defp reusable?(page, reader, interval) do
    now() - page.at < interval and not MapSet.member?(page.readers, reader)
  end

  @impl true
  def handle_info({ref, result}, state) when is_map_key(state.refs, ref) do
    Process.demonitor(ref, [:flush])
    finish_page(ref, result, state)
  end

  def handle_info({:DOWN, ref, :process, _, reason}, state) when is_map_key(state.refs, ref) do
    finish_page(ref, {:error, reason}, state)
  end

  def handle_info(:expire, state) do
    cutoff = now() - state.interval
    pages = Map.reject(state.pages, fn {_, page} -> page.at <= cutoff end)

    :ets.select_delete(state.table, [
      {{{:copy, :_}, :"$1", :_, :_}, [{:<, :"$1", cutoff - state.interval}], [true]}
    ])

    Process.send_after(self(), :expire, state.interval)
    {:noreply, %{state | pages: pages}}
  end

  defp finish_page(ref, result, state) do
    {{token, at}, refs} = Map.pop!(state.refs, ref)
    {waiters, pending} = Map.pop!(state.pending, token)

    pages =
      case result do
        {:ok, %{keys: keys, next_continuation_token: next}} ->
          entries = Enum.map(keys, &Map.take(&1, [:key, :etag]))
          hash = :crypto.hash(:sha256, :erlang.term_to_iovec(entries, [:deterministic]))
          readers = MapSet.new(waiters, fn {{pid, _}, _} -> node(pid) end)
          page = %{at: at, hash: hash, entries: entries, next: next, readers: readers}
          for {from, digest} <- waiters, do: GenServer.reply(from, reply_page(page, digest))
          Map.put(state.pages, token, page)

        {:error, _} = error ->
          for {from, _} <- waiters, do: GenServer.reply(from, error)
          Map.delete(state.pages, token)
      end

    {:noreply, %{state | pages: pages, refs: refs, pending: pending}}
  end

  defp reply_page(%{hash: hash, next: next}, hash), do: {:same, next}
  defp reply_page(page, _), do: {:page, page.hash, page.entries, page.next}
  defp now, do: System.monotonic_time(:millisecond)
end
