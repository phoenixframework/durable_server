defmodule Group.TestConflictResolver do
  @moduledoc false

  # A compiled module for conflict resolution that records calls to an ETS table.
  # Must be in test/support/ so it's compiled to beam and available on peer nodes.

  def resolve(_name, _key, {pid1, _meta1, time1}, {pid2, _meta2, time2}) do
    # Default: keep the one with the higher time (more recent wins)
    if time2 >= time1, do: pid2, else: pid1
  end
end
