defmodule DurableServer.TestHelper do
  @moduledoc """
  Test helpers for DurableServer tests.
  """

  import ExUnit.Assertions, only: [flunk: 1]

  alias DurableServer.ObjectStore

  @doc """
  Waits for a predicate to become truthy, failing if it stays false until the deadline.
  """
  def assert_eventually(fun, timeout \\ 5_000, interval \\ 25)
      when is_function(fun, 0) and is_integer(timeout) and timeout >= 0 do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(fun, deadline, interval)
  end

  defp do_assert_eventually(fun, deadline, interval) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("condition was not met within timeout")

      true ->
        Process.sleep(interval)
        do_assert_eventually(fun, deadline, interval)
    end
  end

  @doc """
  Returns LocalStack options with a bucket unique to this suite invocation.

  This only builds configuration; it does not contact storage.
  """
  def test_object_store_opts(opts \\ []) do
    Keyword.merge(
      [
        access_key_id: "test",
        secret_access_key: "test",
        s3_endpoint: "http://localhost:4566",
        iam_endpoint: "http://localhost:4566",
        default_region: "us-east-1",
        bucket: Application.fetch_env!(:durable_server, :test_object_store_bucket)
      ],
      opts
    )
  end

  @doc """
  Creates an ObjectStore configured for testing.
  """
  def test_object_store(opts \\ []) do
    ObjectStore.new(test_object_store_opts(opts))
  end

  @doc false
  def clean_test_object_store! do
    store = test_object_store()

    for object <- ObjectStore.list_all_objects_stream(store, "") do
      :ok = ObjectStore.delete_object(store, object.key)
    end

    :ok = ObjectStore.delete_bucket(store, store.bucket)
  end

  @doc """
  Allocates an isolated data directory inside the worktree and removes it on exit.
  """
  def test_data_dir(label) do
    path = Path.expand(Path.join("tmp", "#{label}-#{DurableServer.UUID.uuid4()}"))
    File.mkdir_p!(path)
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf!(path) end)
    path
  end
end
