defmodule DurableServer.LocalStackIsolationTest do
  use DurableServer.LocalStackCase, async: true

  alias DurableServer.{ObjectStore, TestHelper}

  test "cleaning one run leaves another run's objects intact" do
    first = TestHelper.test_object_store(bucket: TestHelper.new_test_bucket())
    second = TestHelper.test_object_store(bucket: TestHelper.new_test_bucket())
    :ok = ObjectStore.ensure_bucket_exists(first)
    :ok = ObjectStore.ensure_bucket_exists(second)
    on_exit(fn -> TestHelper.cleanup_bucket!(second) end)

    assert {:ok, _} = ObjectStore.put_object(first, "same-key", "first-run")
    assert {:ok, _} = ObjectStore.put_object(second, "same-key", "second-run")

    assert :ok = TestHelper.cleanup_bucket!(first)
    assert {:ok, %{body: "second-run"}} = ObjectStore.get_object(second, "same-key")
  end
end
