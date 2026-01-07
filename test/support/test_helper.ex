defmodule DurableServer.TestHelper do
  @moduledoc """
  Test helpers for DurableServer tests.
  """

  alias DurableServer.ObjectStore

  @doc """
  Returns the default object store config for testing as a keyword list.
  """
  def test_object_store_opts(opts \\ []) do
    Keyword.merge(
      [
        access_key_id: "test",
        secret_access_key: "test",
        s3_endpoint: "http://localhost:4566",
        iam_endpoint: "http://localhost:4566",
        default_region: "us-east-1",
        bucket: "durable-test-bucket"
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
