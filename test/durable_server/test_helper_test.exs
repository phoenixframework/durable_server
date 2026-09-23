defmodule DurableServer.TestHelperTest do
  use ExUnit.Case, async: true
  import DurableServer.TestHelper

  test "eventual assertions retry the predicate and return as soon as it succeeds" do
    attempts = make_ref()

    assert :ok =
             assert_eventually(
               fn ->
                 count = Process.get(attempts, 0)
                 Process.put(attempts, count + 1)
                 count == 1
               end,
               5_000,
               0
             )

    assert Process.get(attempts) == 2
  end

  test "eventual assertions fail when the predicate stays false until the deadline" do
    assert_raise ExUnit.AssertionError, ~r/condition was not met within timeout/, fn ->
      assert_eventually(fn -> false end, 0)
    end
  end

  test "object-store configuration uses the suite bucket without connecting to storage" do
    opts = test_object_store_opts()
    assert opts[:bucket] == Application.fetch_env!(:durable_server, :test_object_store_bucket)
    assert opts[:bucket] =~ ~r/^durable-test-[0-9a-f-]{36}$/
    assert test_object_store().bucket == opts[:bucket]
    assert test_object_store_opts(bucket: "explicit-fixture")[:bucket] == "explicit-fixture"
  end

  test "data directories are unique, worktree-local, and cleaned up on exit" do
    # on_exit callbacks run in reverse registration order. Keep the allocated
    # paths alive past this test process so the last callback can verify cleanup.
    {:ok, paths} = Agent.start(fn -> [] end)

    on_exit(fn ->
      allocated = Agent.get(paths, & &1)
      Agent.stop(paths)
      for path <- allocated, do: refute(File.exists?(path))
    end)

    first = test_data_dir("isolation")
    second = test_data_dir("isolation")
    Agent.update(paths, fn _ -> [first, second] end)

    assert first != second
    assert Path.dirname(first) == Path.expand("tmp")
    assert Path.dirname(second) == Path.expand("tmp")
    assert File.dir?(first)
    assert File.dir?(second)
  end
end
