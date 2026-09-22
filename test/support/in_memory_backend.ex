defmodule DurableServer.TestInMemoryBackend do
  @moduledoc """
  Node-local storage shared by backend configuration and placement tests.

  Each instance owns an independent ETS table. Claims are create-only, but
  updates do not enforce compare-and-swap options. This is not distributed
  storage and must not be used to test competing owners across nodes.
  """
  @behaviour DurableServer.StorageBackend

  @impl true
  def init_backend(raw_opts) do
    opts =
      case raw_opts do
        %{} = map -> map
        opts when is_list(opts) -> Map.new(opts)
        other -> %{raw_opts: other}
      end

    {:ok,
     %{
       state: %{
         table: :ets.new(__MODULE__, [:set, :public]),
         name: Map.get(opts, :name)
       },
       defaults: %{
         heartbeat_tracking_mode: :poll,
         discovery_interval_ms: 60_000,
         heartbeat_interval_ms: 10_000,
         heartbeat_reconcile_interval_ms: 10_000
       }
     }}
  end

  @impl true
  def ensure_ready(_state), do: :ok

  @impl true
  def get_object(%{table: table}, key, _opts) do
    case :ets.lookup(table, key) do
      [{^key, object}] -> {:ok, object}
      [] -> {:error, :not_found}
    end
  end

  @impl true
  def put_object(%{table: table}, key, body, _opts) do
    object = %{body: body, etag: etag()}
    true = :ets.insert(table, {key, object})
    {:ok, object}
  end

  @impl true
  def try_claim(%{table: table}, key, body) do
    object = %{body: body, etag: etag()}

    if :ets.insert_new(table, {key, object}),
      do: {:ok, {:claimed, object.etag}},
      else: {:error, :taken}
  end

  @impl true
  def update_object(state, key, fun, opts) do
    with {:ok, object} <- get_object(state, key, opts),
         {:ok, body} <- fun.(object) do
      put_object(state, key, body, opts)
    end
  end

  @impl true
  def delete_object(%{table: table}, key) do
    case :ets.take(table, key) do
      [] -> {:error, :not_found}
      [_object] -> :ok
    end
  end

  @impl true
  def list_all_objects_stream(%{table: table}, prefix, _opts) do
    table
    |> :ets.tab2list()
    |> Stream.filter(fn {key, _object} -> String.starts_with?(key, prefix) end)
    |> Stream.map(fn {key, object} -> %{key: key, etag: object.etag} end)
  end

  @impl true
  def encode(_state, value), do: {:ok, value}

  @impl true
  def decode(_state, value), do: {:ok, value}

  defp etag, do: Integer.to_string(System.unique_integer([:positive, :monotonic]))
end
