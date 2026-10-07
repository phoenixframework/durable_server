defmodule DurableServer.BalancedPlacementTest do
  use ExUnit.Case, async: false
  import DurableServer.TestHelper
  alias DurableServer.{Supervisor, TestCounterServer}

  @moduletag :capture_log

  setup_all do
    unless Node.alive?() do
      {:ok, _} = Node.start(:"placement_test_#{System.unique_integer([:positive])}", :shortnames)
    end

    {:ok, peer, remote} =
      :peer.start(%{name: :"placement_peer_#{System.unique_integer([:positive])}"})

    on_exit(fn ->
      if Process.alive?(peer), do: :peer.stop(peer)
    end)

    :ok = :erpc.call(remote, :code, :add_paths, [:code.get_path()])
    {:ok, _} = :erpc.call(remote, Application, :ensure_all_started, [:durable_server])
    {:ok, remote: remote}
  end

  test "new starts balance before local fills and retain local fallback", %{remote: remote} do
    name = :"balanced_supervisor_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      prefix: "balanced_placement_#{System.unique_integer([:positive])}/",
      object_store: test_object_store_opts(),
      max_children: %{TestCounterServer => 100}
    ]

    start_supervised!({Supervisor, opts})

    {:ok, _} =
      :erpc.call(remote, Elixir.Supervisor, :start_child, [
        DurableServer.AppSupervisor,
        {Supervisor, opts}
      ])

    on_exit(fn ->
      :erpc.call(remote, Elixir.Supervisor, :terminate_child, [DurableServer.AppSupervisor, name])
      :erpc.call(remote, Elixir.Supervisor, :delete_child, [DurableServer.AppSupervisor, name])
    end)

    placements =
      for i <- 1..10 do
        # Deterministic heartbeat snapshots isolate the placement decision from
        # gossip timing. Production remote counts remain heartbeat-based.
        refresh_remote_capacity(name, remote)

        assert {:ok, {pid, _}} =
                 Supervisor.start_child(
                   name,
                   {TestCounterServer, key: "balanced-#{i}", initial_state: %{count: 0}},
                   placement_timeout: 0
                 )

        node(pid)
      end

    assert Enum.frequencies(placements) == %{node() => 5, remote => 5}

    # Existing children are returned without routing them to a less busy node.
    assert {:ok, {existing, _}} =
             Supervisor.ensure_started_child(name, {
               TestCounterServer,
               key: "balanced-1", initial_state: %{count: 0}
             })

    assert {:error, {:already_started, {^existing, _}}} =
             Supervisor.start_child(name, {
               TestCounterServer,
               key: "balanced-1", initial_state: %{count: 0}
             })

    for {key, start_opts} <- [
          {"forced-local", [local_only: true]},
          {"targeted-local", [max_placement_retries: 0]}
        ] do
      assert {:ok, {pid, _}} =
               Supervisor.start_child(
                 name,
                 {TestCounterServer, key: key, initial_state: %{count: 0}},
                 start_opts
               )

      assert node(pid) == node()
    end

    refresh_remote_capacity(name, remote)

    assert {:ok, {pid, _}} =
             Supervisor.ensure_started_child(name, {
               TestCounterServer,
               key: "ensure-new", initial_state: %{count: 0}
             })

    assert node(pid) == remote

    # The remote snapshot says it can accept a child, but admission now rejects
    # it. Local must still be tried, even with a one-remote-node retry budget.
    refresh_remote_capacity(name, remote)
    %{ets_table: remote_table} = :erpc.call(remote, Supervisor, :__get_config__, [name])
    :erpc.call(remote, :ets, :insert, [remote_table, {:shutting_down, true}])

    assert {:ok, {fallback, _}} =
             Supervisor.start_child(
               name,
               {TestCounterServer, key: "local-fallback", initial_state: %{count: 0}},
               max_placement_retries: 1,
               placement_timeout: 0
             )

    assert node(fallback) == node()
  end

  defp refresh_remote_capacity(supervisor, remote) do
    capacity = :erpc.call(remote, Supervisor, :current_capacity, [supervisor])
    node_ref = :erpc.call(remote, Supervisor, :node_ref, [supervisor])

    :ets.insert(
      :"durable_server_heartbeats_#{supervisor}",
      {to_string(remote), node_ref, System.system_time(:millisecond), capacity, nil, %{}, %{}}
    )
  end
end
