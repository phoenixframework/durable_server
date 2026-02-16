defmodule Group.Event do
  @moduledoc """
  Struct representing a lifecycle event from `Group`.

  Events are delivered as `{:group, events, info}` tuples to processes
  that called `Group.monitor/2`.

  ## Fields

  - `:type` - Event type: `:registered`, `:unregistered`, `:joined`, or `:left`
  - `:supervisor` - The supervisor name
  - `:cluster` - The cluster name (`nil` for default cluster)
  - `:key` - The key that triggered the event
  - `:pid` - The process involved
  - `:meta` - User-provided metadata (internal keys stripped)
  - `:previous_meta` - Previous metadata on re-register/re-join (`nil` if new)
  - `:reason` - Exit reason on `:unregistered`/`:left` events
  """

  defstruct [:type, :supervisor, :cluster, :key, :pid, :meta, :reason, :previous_meta]
end
