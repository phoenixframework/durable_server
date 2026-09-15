defmodule DurableServer.LocalStackCase do
  @moduledoc """
  Opt-in LocalStack tests sharing a bucket unique to the current suite invocation.

  Tests still use unique prefixes within that bucket. Cleanup happens after all
  test supervisors have stopped, never at the beginning of another suite run.
  """
  use ExUnit.CaseTemplate

  using do
    quote do
      @moduletag :localstack
    end
  end

  setup_all do
    store = DurableServer.TestHelper.test_object_store()
    :ok = DurableServer.ObjectStore.ensure_bucket_exists(store)
    Application.put_env(:durable_server, :test_object_store_used, true)
    :ok
  end
end
