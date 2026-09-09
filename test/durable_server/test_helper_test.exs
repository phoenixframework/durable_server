defmodule DurableServer.TestHelperTest do
  use ExUnit.Case, async: true

  alias DurableServer.TestHelper

  test "a run reuses its namespace without requiring a storage connection" do
    first = TestHelper.test_object_store_opts()
    second = TestHelper.test_object_store_opts()
    assert first[:bucket] == second[:bucket]
    assert first[:bucket] =~ ~r/^durable-test-[a-f0-9-]{36}$/
    refute first[:bucket] == "durable-test-bucket"
  end

  test "independent runs allocate different bucket names" do
    buckets = for _ <- 1..100, do: TestHelper.new_test_bucket()
    assert length(Enum.uniq(buckets)) == 100
  end

  test "explicit storage options override only the requested defaults" do
    opts =
      TestHelper.test_object_store_opts(bucket: "explicit", s3_endpoint: "http://localhost:1")

    assert opts[:bucket] == "explicit"
    assert opts[:s3_endpoint] == "http://localhost:1"
    assert opts[:access_key_id] == "test"
  end
end
