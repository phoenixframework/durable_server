defmodule DurableServer.EKVStoreTest do
  use ExUnit.Case, async: true

  alias DurableServer.Backends.EKVStore
  alias DurableServer.StorageBackend

  @table :durable_server_ekv_store_test_state

  defmodule FakeEKVSupervisor do
    def get_config(_name), do: %{cluster_size: 1}
  end

  defmodule FakeEKV do
    @table :durable_server_ekv_store_test_state

    def keys(name, prefix), do: next(name, :keys, [{prefix, {1, :node@fake}}])
    def lookup(name, _key), do: next(name, :lookup, nil)
    def row_state(name, _key, _opts), do: next(name, :row_state, {:ok, :absent})
    def put(name, _key, _value, _opts), do: next(name, :put, {:ok, {2, :node@fake}})
    def update(name, _key, _fun, _opts), do: next(name, :update, {:ok, :updated, {3, :node@fake}})
    def get(name, _key, _opts), do: next(name, :get, :ok)
    def delete(name, _key, _opts), do: next(name, :delete, {:ok, {4, :node@fake}})
    def subscribe(name, _prefix), do: next(name, :subscribe, :ok)
    def unsubscribe(name, _prefix), do: next(name, :unsubscribe, :ok)

    defp next(name, op, default) do
      case :ets.lookup(@table, {name, op}) do
        [{{^name, ^op}, [step | rest]}] ->
          :ets.insert(@table, {{name, op}, rest})
          apply_step(step)

        _ ->
          default
      end
    end

    defp apply_step({:return, value}), do: value
    defp apply_step({:exit, reason}), do: exit(reason)

    defp apply_step({:raise, message}) when is_binary(message) do
      raise RuntimeError, message
    end
  end

  setup_all do
    case :ets.whereis(@table) do
      :undefined -> :ets.new(@table, [:named_table, :public, :set])
      _ -> :ok
    end

    :ok
  end

  setup do
    name = :"ekv_store_test_#{System.unique_integer([:positive, :monotonic])}"

    backend_opts = [
      name: name,
      cas_retries: 2,
      backoff: {0, 0},
      timeout: 50,
      ekv_mod: FakeEKV,
      ekv_supervisor_mod: FakeEKVSupervisor
    ]

    {:ok, backend} = StorageBackend.init_backend(EKVStore, backend_opts)

    on_exit(fn ->
      for op <- [
            :keys,
            :lookup,
            :row_state,
            :put,
            :update,
            :get,
            :delete,
            :subscribe,
            :unsubscribe
          ] do
        :ets.delete(@table, {name, op})
      end
    end)

    {:ok, backend: backend, name: name}
  end

  test "consistent get retries transient exits and consistent read failures", %{
    backend: backend,
    name: name
  } do
    :ets.insert(
      @table,
      {{name, :get},
       [
         {:exit, :timeout},
         {:raise, "EKV: consistent read failed: :quorum_timeout"},
         {:return, :ok}
       ]}
    )

    :ets.insert(@table, {{name, :lookup}, [{:return, {"value", {11, :node@fake}}}]})

    assert {:ok, %{body: "value"}} = StorageBackend.get_object(backend, "key", consistent: true)
  end

  test "put_object retries transient exits on latest-update path", %{backend: backend, name: name} do
    :ets.insert(
      @table,
      {{name, :update},
       [
         {:exit, {:timeout, {GenServer, :call, []}}},
         {:return, {:ok, :ignored, {12, :node@fake}}}
       ]}
    )

    assert {:ok, %{body: "value"}} =
             StorageBackend.put_object(backend, "key", "value", max_retries: 1)
  end

  test "put_object retries transient exits on expected-vsn path", %{backend: backend, name: name} do
    etag =
      {13, :node@fake}
      |> :erlang.term_to_binary()
      |> Base.url_encode64(padding: false)

    :ets.insert(
      @table,
      {{name, :put},
       [
         {:exit, {:shutdown, {:timeout, {GenServer, :call, []}}}},
         {:return, {:ok, {14, :node@fake}}}
       ]}
    )

    assert {:ok, %{body: "value"}} =
             StorageBackend.put_object(backend, "key", "value", etag: etag, max_retries: 1)
  end

  test "put_object accepts etags whose vsn origin is a binary node id", %{
    backend: backend,
    name: name
  } do
    etag =
      {15, "7844ee5a606338"}
      |> :erlang.term_to_binary()
      |> Base.url_encode64(padding: false)

    :ets.insert(@table, {{name, :put}, [{:return, {:ok, {16, "7844ee5a606338"}}}]})

    assert {:ok, %{body: "value"}} =
             StorageBackend.put_object(backend, "key", "value", etag: etag, max_retries: 0)
  end
end
