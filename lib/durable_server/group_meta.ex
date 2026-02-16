defmodule DurableServer.GroupMeta do
  @moduledoc false

  # The internal metadata stored in Group registry
  defstruct key: nil,
            module: nil,
            storage_key: nil,
            node_ref: nil,
            start_time: nil,
            user_meta: nil,
            etag: nil,
            supervisor: nil
end
