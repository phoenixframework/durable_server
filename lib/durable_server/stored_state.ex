defmodule DurableServer.StoredState do
  @derive {JSON.Encoder,
           only: [
             :vsn,
             :state,
             :meta
           ]}
  defstruct key: nil,
            prefix: nil,
            state: nil,
            meta: nil,
            vsn: nil,
            etag: nil
end
