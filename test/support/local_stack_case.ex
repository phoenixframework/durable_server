defmodule DurableServer.LocalStackCase do
  @moduledoc """
  Opt-in LocalStack fixtures. Storage is created only for selected test modules,
  and the suite owns a unique bucket rather than clearing a shared bucket.
  """
  use ExUnit.CaseTemplate

  using do
    quote do
      @moduletag :localstack
    end
  end

  setup_all do
    DurableServer.TestHelper.ensure_localstack!()
  end
end
