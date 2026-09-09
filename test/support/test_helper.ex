defmodule DurableServer.TestHelper do
  @moduledoc """
  Test helpers for DurableServer tests.
  """

  alias DurableServer.ObjectStore

  def init_test_run do
    Application.put_env(:durable_server, :test_bucket, new_test_bucket())
    Application.delete_env(:durable_server, :created_test_store)
    :ok
  end

  def new_test_bucket, do: "durable-test-#{DurableServer.UUID.uuid4()}"

  def ensure_localstack! do
    store = test_object_store()
    :ok = ObjectStore.ensure_bucket_exists(store)
    Application.put_env(:durable_server, :created_test_store, store)
    :ok
  end

  def cleanup_test_run! do
    if store = Application.get_env(:durable_server, :created_test_store) do
      cleanup_bucket!(store)
      Application.delete_env(:durable_server, :created_test_store)
    end

    :ok
  end

  def cleanup_bucket!(%ObjectStore{} = store) do
    for obj <- ObjectStore.list_all_objects_stream(store, "") do
      :ok = ObjectStore.delete_object(store, obj.key)
    end

    :ok = ObjectStore.delete_bucket(store, store.bucket)
  end

  @doc """
  Returns the default object store config for testing as a keyword list.
  """
  def test_object_store_opts(opts \\ []) do
    endpoint = System.get_env("DURABLE_TEST_S3_ENDPOINT", "http://localhost:4566")

    Keyword.merge(
      [
        access_key_id: "test",
        secret_access_key: "test",
        s3_endpoint: endpoint,
        iam_endpoint: endpoint,
        default_region: "us-east-1",
        bucket: Application.fetch_env!(:durable_server, :test_bucket)
      ],
      opts
    )
  end

  @doc """
  Creates an ObjectStore configured for testing.

  Uses environment variables or defaults suitable for LocalStack.
  """
  def test_object_store(opts \\ []) do
    ObjectStore.new(test_object_store_opts(opts))
  end
end
