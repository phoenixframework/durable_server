defmodule GroupBench.Helpers do
  @moduledoc """
  Timing, formatting, percentile math, and setup utilities for benchmarks.
  """

  @doc """
  Times a function call in microseconds. Returns {microseconds, result}.
  """
  def time_us(fun) do
    :timer.tc(fun)
  end

  @doc """
  Collects N timing samples by calling `fun` repeatedly.
  Returns a sorted list of microsecond timings.
  """
  def collect_samples(n, fun) do
    Enum.map(1..n, fn _ ->
      {us, _} = :timer.tc(fun)
      us
    end)
    |> Enum.sort()
  end

  @doc """
  Returns the value at the given percentile (0-100) from a sorted list.
  """
  def percentile(sorted, p) when is_list(sorted) and p >= 0 and p <= 100 do
    len = length(sorted)

    if len == 0 do
      0
    else
      index = max(0, min(round(p / 100 * len) - 1, len - 1))
      Enum.at(sorted, index)
    end
  end

  @doc """
  Formats a number with comma separators.
  """
  def format_number(n) when is_integer(n) do
    n
    |> Integer.to_string()
    |> String.reverse()
    |> String.to_charlist()
    |> Enum.chunk_every(3)
    |> Enum.join(",")
    |> String.reverse()
  end

  def format_number(n) when is_float(n) do
    :erlang.float_to_binary(n, decimals: 1)
  end

  @doc """
  Prints a section header.
  """
  def header(text) do
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("  #{text}")
    IO.puts(String.duplicate("=", 60))
  end

  @doc """
  Prints a sub-header.
  """
  def subheader(text) do
    IO.puts("\n  --- #{text} ---")
  end

  @doc """
  Reports throughput and latency stats from a sorted list of microsecond timings.
  """
  def report_latency(label, sorted_us) do
    count = length(sorted_us)
    total_us = Enum.sum(sorted_us)
    ops_sec = if total_us > 0, do: round(count * 1_000_000 / total_us), else: 0

    IO.puts("  #{label}")
    IO.puts("    ops/sec:  #{format_number(ops_sec)}")
    IO.puts("    p50:      #{percentile(sorted_us, 50)} µs")
    IO.puts("    p99:      #{percentile(sorted_us, 99)} µs")
    IO.puts("    max:      #{percentile(sorted_us, 100)} µs")
  end

  @doc """
  Reports throughput from total wall-clock time and operation count.
  """
  def report_throughput(label, count, wall_us) do
    ops_sec = if wall_us > 0, do: round(count * 1_000_000 / wall_us), else: 0

    IO.puts("  #{label}")
    IO.puts("    total:    #{format_number(count)} ops in #{format_number(div(wall_us, 1000))} ms")
    IO.puts("    ops/sec:  #{format_number(ops_sec)}")
  end

  @doc """
  Starts a fresh Group instance with the given options, runs `fun`, then stops it.
  """
  def with_group(opts, fun) do
    opts = Keyword.put_new(opts, :name, :bench)
    old_trap = Process.flag(:trap_exit, true)
    {:ok, sup} = Group.start_link(opts)

    try do
      fun.()
    after
      Process.unlink(sup)

      try do
        Supervisor.stop(sup, :shutdown, 5_000)
      catch
        :exit, _ -> :ok
      end

      # Drain any EXIT messages from the stopped supervisor / children
      drain_exits()

      Process.flag(:trap_exit, old_trap)
      Process.sleep(50)
    end
  end

  defp drain_exits do
    receive do
      {:EXIT, _, _} -> drain_exits()
    after
      0 -> :ok
    end
  end

  @doc """
  Spawns N long-lived processes that stay alive until the caller exits.
  Returns a list of pids.
  """
  def spawn_processes(n) do
    parent = self()

    Enum.map(1..n, fn _ ->
      spawn(fn ->
        ref = Process.monitor(parent)

        receive do
          {:DOWN, ^ref, _, _, _} -> :ok
        end
      end)
    end)
  end

  @doc """
  Runs `fun` as warmup for `n` iterations (discards results).
  """
  def warmup(n, fun) do
    Enum.each(1..n, fn _ -> fun.() end)
  end
end
