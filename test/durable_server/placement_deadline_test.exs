defmodule DurableServer.PlacementDeadlineTest do
  use ExUnit.Case, async: false

  alias DurableServer.{LifecycleManager, PlacementTestServer, TestInMemoryBackend}
  alias DurableServer.Supervisor, as: DurableSupervisor

  @moduletag :capture_log

  setup_all do
    unless Node.alive?() do
      {_, 0} = System.cmd("epmd", ["-daemon"])

      {:ok, _} =
        Node.start(:"placement_deadline_test_#{System.pid()}@127.0.0.1", :longnames)

      on_exit(fn -> Node.stop() end)
    end

    :ok
  end

  setup context do
    suffix = "#{System.pid()}_#{System.unique_integer([:positive])}"
    rpc_timeout = Map.get(context, :placement_rpc_timeout_ms, 500)

    {:ok, peer, remote_node} =
      :peer.start_link(%{
        name: :"placement_deadline_peer_#{suffix}",
        host: ~c"127.0.0.1",
        args: [~c"+S", ~c"2:2", ~c"-pa" | :code.get_path()]
      })

    Process.unlink(peer)

    on_exit(fn ->
      if Process.alive?(peer), do: :peer.stop(peer)
    end)

    {:ok, _} = :erpc.call(remote_node, Application, :ensure_all_started, [:durable_server])
    supervisor = :"placement_deadline_sup_#{suffix}"

    opts = [
      name: supervisor,
      prefix: "placement-deadline/#{suffix}/",
      backend: {TestInMemoryBackend, []},
      initial_discovery_delay_ms: 60_000,
      graceful_shutdown_timeout_ms: 500,
      placement_erpc_timeout_same_region_ms: rpc_timeout,
      placement_erpc_timeout_cross_region_ms: rpc_timeout
    ]

    {:ok, _} =
      :erpc.call(remote_node, PlacementTestServer, :start_supervisor, [
        Keyword.put(opts, :max_children, %{total: 10})
      ])

    start_supervised!(
      {DurableSupervisor, Keyword.put(opts, :max_children, %{PlacementTestServer => 0})}
    )

    :ok = DurableSupervisor.wait_until_ready(supervisor)
    :ok = :erpc.call(remote_node, DurableSupervisor, :wait_until_ready, [supervisor])
    advertise_remote(supervisor, remote_node)

    %{supervisor: supervisor, remote_node: remote_node, peer: peer}
  end

  for {rpc_timeout, start_timeout} <- [{500, 250}, {3_000, 2_000}, {8_000, 7_000}] do
    @tag placement_rpc_timeout_ms: rpc_timeout
    test "reserves the expected reply headroom for a #{rpc_timeout}ms RPC", context do
      %{supervisor: supervisor, remote_node: remote_node} = context
      :ok = :erpc.call(remote_node, PlacementTestServer, :trace_start_timeouts, [self()])

      assert {:ok, {pid, _meta}} =
               DurableSupervisor.start_child(
                 supervisor,
                 {PlacementTestServer, key: "budget", initial_state: %{}},
                 timeout: 10_000,
                 max_placement_retries: 1
               )

      assert node(pid) == remote_node
      assert_receive {:placement_start_timeout, unquote(start_timeout)}, 1_000
    end
  end

  test "slow bootstrap does not cool down its reachable node or block another key", context do
    %{supervisor: supervisor, remote_node: remote_node} = context
    observer = self()

    task =
      Task.async(fn ->
        DurableSupervisor.start_child(
          supervisor,
          {PlacementTestServer, key: "slow", initial_state: %{blocked: true, observer: observer}},
          timeout: 500,
          max_placement_retries: 1
        )
      end)

    assert_receive {:bootstrap_started, "slow", bootstrap_pid}, 2_000
    assert node(bootstrap_pid) == remote_node

    try do
      assert {:error, _reason} = Task.await(task, 2_000)

      diagnostics = LifecycleManager.get_discovery_diagnostics(supervisor)
      assert Map.get(diagnostics, :remote_placement_node_cooldown_trip, 0) == 0
      assert Map.get(diagnostics, {:remote_placement_erpc_error, :timeout}, 0) == 0

      assert {:ok, {fast_pid, _meta}} =
               DurableSupervisor.start_child(
                 supervisor,
                 {PlacementTestServer, key: "fast", initial_state: %{}},
                 timeout: 500,
                 max_placement_retries: 1
               )

      assert node(fast_pid) == remote_node
      assert :erpc.call(remote_node, Process, :alive?, [bootstrap_pid])
    after
      send(bootstrap_pid, :finish_bootstrap)
    end

    assert_eventually(fn ->
      case :erpc.call(remote_node, DurableSupervisor, :lookup, [supervisor, "slow"]) do
        {pid, _meta} -> pid == bootstrap_pid
        nil -> false
      end
    end)

    assert {:ok, {^bootstrap_pid, _meta}} =
             DurableSupervisor.ensure_started_child(
               supervisor,
               {PlacementTestServer, key: "slow", initial_state: %{}},
               timeout: 1_000
             )
  end

  test "a genuine transport failure still trips node cooldown", context do
    %{supervisor: supervisor, peer: peer, remote_node: remote_node} = context
    :ok = :peer.stop(peer)
    assert Node.ping(remote_node) == :pang
    advertise_remote(supervisor, remote_node)

    assert {:error, _reason} =
             DurableSupervisor.start_child(
               supervisor,
               {PlacementTestServer, key: "disconnected", initial_state: %{}},
               timeout: 500,
               max_placement_retries: 1
             )

    diagnostics = LifecycleManager.get_discovery_diagnostics(supervisor)
    assert diagnostics.remote_placement_node_cooldown_trip == 1
    assert diagnostics.remote_placement_erpc_error == 1
  end

  test "remote readiness waiting honors a short caller budget" do
    supervisor = :"absent_placement_sup_#{System.unique_integer([:positive])}"
    started_at = System.monotonic_time(:millisecond)

    assert catch_throw(
             DurableSupervisor.__start_child__(
               supervisor,
               {PlacementTestServer, [key: "not-ready", initial_state: %{}], nil},
               max_placement_retries: 0,
               timeout: 30
             )
           ) == {:error, :not_ready}

    assert System.monotonic_time(:millisecond) - started_at < 250
  end

  test "a one-millisecond budget does not dispatch a remote bootstrap", context do
    %{supervisor: supervisor, remote_node: remote_node} = context

    assert {:error, :timeout} =
             DurableSupervisor.start_child(
               supervisor,
               {PlacementTestServer,
                key: "expired", initial_state: %{blocked: true, observer: self()}},
               timeout: 1,
               max_placement_retries: 1
             )

    refute_receive {:bootstrap_started, "expired", _pid}, 50
    assert :erpc.call(remote_node, DurableSupervisor, :lookup, [supervisor, "expired"]) == nil
    diagnostics = LifecycleManager.get_discovery_diagnostics(supervisor)
    assert Map.get(diagnostics, :remote_placement_node_cooldown_trip, 0) == 0
    assert Map.get(diagnostics, :remote_placement_erpc_attempt, 0) == 0
  end

  defp advertise_remote(supervisor, remote_node) do
    :ets.insert(
      :"durable_server_heartbeats_#{supervisor}",
      {Atom.to_string(remote_node), 1, System.system_time(:millisecond),
       %{total: %{current: 0, limit: 10}}, nil, %{}, %{}}
    )
  end

  defp assert_eventually(fun, attempts \\ 100)
  defp assert_eventually(fun, 0), do: assert(fun.())

  defp assert_eventually(fun, attempts) do
    unless fun.() do
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end
end
