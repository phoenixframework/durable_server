defmodule DurableServer.PlacementTestServer do
  @moduledoc false
  use DurableServer, vsn: 1

  # Keep the remote supervisor alive after the short-lived RPC worker exits.
  def start_supervisor(opts) do
    {:ok, pid} = DurableServer.Supervisor.start_link(opts)
    Process.unlink(pid)
    {:ok, pid}
  end

  # Observe the budget actually sent over RPC without sleeping for multi-second
  # deadlines or exposing a production API solely for checking timeout arithmetic.
  # The tracer and trace pattern live only on this test's disposable peer node.
  def trace_start_timeouts(observer) do
    tracer = spawn(fn -> forward_start_timeouts(observer) end)
    :erlang.trace_pattern({DurableServer.Supervisor, :__start_child__, 3}, true, [])
    :erlang.trace(:new, true, [:call, {:tracer, tracer}])
    :ok
  end

  defp forward_start_timeouts(observer) do
    receive do
      {:trace, _pid, :call, {DurableServer.Supervisor, :__start_child__, [_sup, _spec, opts]}} ->
        send(observer, {:placement_start_timeout, Keyword.fetch!(opts, :timeout)})
        forward_start_timeouts(observer)
    end
  end

  @impl true
  def init(state, info) do
    if state[:blocked] do
      send(state.observer, {:bootstrap_started, info.key, self()})

      receive do
        :finish_bootstrap -> :ok
      end
    end

    {:ok, state}
  end

  @impl true
  def dump_state(state), do: state

  @impl true
  def load_state(_vsn, state), do: state
end
