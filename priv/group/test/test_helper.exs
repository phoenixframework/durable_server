# Start distribution if not already running (needed for distributed tests)
unless Node.alive?() do
  {:ok, _} = Node.start(:"test_#{System.unique_integer([:positive])}@127.0.0.1", :longnames)
  Node.set_cookie(:group_test)
end

# Disable :global's partition prevention to allow peer-to-peer disconnects
# in distributed tests without the test node also being disconnected.
:application.set_env(:kernel, :prevent_overlapping_partitions, false)

ExUnit.start()
