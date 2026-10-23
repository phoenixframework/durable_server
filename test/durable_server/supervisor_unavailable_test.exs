defmodule DurableServer.SupervisorUnavailableTest do
  @moduledoc """
  Acquisition against a supervisor that is stopped, shutting down, or has lost
  its config table must return `{:error, :supervisor_unavailable}` instead of
  starting children outside the singleflight or raising from missing ETS.
  """
  use ExUnit.Case, async: true
  import DurableServer.TestHelper

  alias DurableServer.TestCounterServer

  @moduletag :capture_log

  defp start_test_supervisor do
    unique_id = "#{System.system_time(:microsecond)}_#{DurableServer.UUID.uuid4()}"
    name = :"test_supervisor_#{unique_id}"

    opts = [
      name: name,
      prefix: "test_#{unique_id}/",
      graceful_shutdown_timeout_ms: 500,
      object_store: test_object_store_opts()
    ]

    pid =
      start_supervised!(%{
        id: {DurableServer.Supervisor, name},
        start: {DurableServer.Supervisor, :start_link, [opts]},
        type: :supervisor
      })

    {name, pid}
  end

  defp child_spec(key), do: {TestCounterServer, key: key, initial_state: %{count: 0}}

  defp unique_key, do: "unavailable-#{DurableServer.UUID.uuid4()}"

  defp local_children(name) do
    name
    |> DurableServer.Supervisor.get_dynamic_supervisor()
    |> DynamicSupervisor.which_children()
  end

  describe "after the supervisor has stopped" do
    setup do
      {name, _pid} = start_test_supervisor()
      :ok = stop_supervised({DurableServer.Supervisor, name})
      %{name: name}
    end

    test "ensure_started_child returns supervisor_unavailable", %{name: name} do
      assert {:error, :supervisor_unavailable} =
               DurableServer.Supervisor.ensure_started_child(name, child_spec(unique_key()))
    end

    test "start_child returns supervisor_unavailable", %{name: name} do
      assert {:error, :supervisor_unavailable} =
               DurableServer.Supervisor.start_child(name, child_spec(unique_key()))
    end

    test "rehome_child returns supervisor_unavailable", %{name: name} do
      assert {:error, :supervisor_unavailable} =
               DurableServer.Supervisor.rehome_child(name, child_spec(unique_key()))
    end

    test "lookup reports no registered child", %{name: name} do
      assert DurableServer.Supervisor.lookup(name, unique_key()) == nil
    end
  end

  describe "while the supervisor is shutting down" do
    test "ensure_started_child does not bypass the singleflight once its registry is gone" do
      {name, pid} = start_test_supervisor()

      # Shutdown stops children in reverse start order, so the singleflight
      # registries disappear while the config table and DynamicSupervisor remain.
      :ok = Supervisor.terminate_child(pid, :"durable_sf_waiters_#{name}")
      :ok = Supervisor.terminate_child(pid, :"durable_sf_owner_#{name}")

      assert {:error, :supervisor_unavailable} =
               DurableServer.Supervisor.ensure_started_child(name, child_spec(unique_key()))

      assert local_children(name) == []
    end

    test "ensure_started_child returns supervisor_unavailable when the config table disappears after the singleflight claim" do
      {name, _pid} = start_test_supervisor()

      # The config table is owned by the supervisor process, so it vanishes with
      # it while an acquisition that already holds the singleflight continues.
      true = :ets.delete(DurableServer.Supervisor.__ets_table_name__(name))

      assert {:error, :supervisor_unavailable} =
               DurableServer.Supervisor.ensure_started_child(name, child_spec(unique_key()))

      assert local_children(name) == []
    end

    test "start_child returns supervisor_unavailable when the config table disappears" do
      {name, _pid} = start_test_supervisor()
      true = :ets.delete(DurableServer.Supervisor.__ets_table_name__(name))

      assert {:error, :supervisor_unavailable} =
               DurableServer.Supervisor.start_child(name, child_spec(unique_key()))

      assert local_children(name) == []
    end
  end
end
