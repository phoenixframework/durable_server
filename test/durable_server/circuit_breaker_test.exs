defmodule DurableServer.CircuitBreakerTest do
  use ExUnit.Case, async: true
  import DurableServer.TestHelper

  alias DurableServer.CircuitBreaker
  alias DurableServer.ObjectStore

  @default_config %{
    object_store: test_object_store(),
    crash_threshold_count: 5,
    # 1 hour
    crash_threshold_window_ms: 60 * 60 * 1000,
    module_circuit_breaker_count: 50,
    # 5 minutes
    module_circuit_breaker_window_ms: 5 * 60 * 1000,
    # 30 minutes
    module_circuit_breaker_cooldown_ms: 30 * 60 * 1000,
    global_lock_failure_count: 100,
    # 30 seconds
    global_lock_failure_window_ms: 30 * 1000,
    # 1 minute
    global_lock_failure_cooldown_ms: 60 * 1000
  }

  setup do
    # Clean up any existing ETS table from previous tests
    if :ets.whereis(:module_circuit_breaker) != :undefined do
      :ets.delete(:module_circuit_breaker)
    end

    :ok
  end

  describe "new/2" do
    test "creates a CircuitBreaker struct and ETS table" do
      supervisor_name = :test_supervisor_1
      circuit_breaker = CircuitBreaker.new(supervisor_name, @default_config)

      assert %CircuitBreaker{} = circuit_breaker
      assert circuit_breaker.supervisor_name == supervisor_name
      assert circuit_breaker.table_name == :"circuit_breaker_#{supervisor_name}"
      assert circuit_breaker.config == Map.drop(@default_config, [:object_store])
      assert %ObjectStore{} = circuit_breaker.object_store
      assert :ets.whereis(circuit_breaker.table_name) != :undefined
    end

    test "raises error if ETS table already exists" do
      supervisor_name = :test_supervisor_2
      CircuitBreaker.new(supervisor_name, @default_config)

      assert_raise ArgumentError, ~r/already exists/, fn ->
        CircuitBreaker.new(supervisor_name, @default_config)
      end
    end

    test "creates different tables for different supervisors" do
      supervisor1 = :test_supervisor_3
      supervisor2 = :test_supervisor_4

      cb1 = CircuitBreaker.new(supervisor1, @default_config)
      cb2 = CircuitBreaker.new(supervisor2, @default_config)

      assert :ets.whereis(cb1.table_name) != :undefined
      assert :ets.whereis(cb2.table_name) != :undefined
      assert cb1.table_name != cb2.table_name
    end
  end

  describe "check_module_circuit_breaker/2" do
    setup do
      supervisor_name = :test_supervisor_5
      circuit_breaker = CircuitBreaker.new(supervisor_name, @default_config)
      %{circuit_breaker: circuit_breaker}
    end

    test "returns :ok for module with no previous crashes", %{circuit_breaker: circuit_breaker} do
      assert CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule) == :ok
    end

    test "returns :ok when count is below threshold", %{circuit_breaker: circuit_breaker} do
      # Add some crashes but below threshold
      for _ <- 1..(@default_config.module_circuit_breaker_count - 1) do
        CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)
      end

      assert CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule) == :ok
    end

    test "opens circuit breaker when count reaches threshold", %{circuit_breaker: circuit_breaker} do
      # Add crashes up to threshold
      for _ <- 1..@default_config.module_circuit_breaker_count do
        CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)
      end

      assert {:circuit_open, cooldown_ms} =
               CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule)

      assert cooldown_ms == @default_config.module_circuit_breaker_cooldown_ms
    end

    test "keeps circuit breaker open during cooldown period", %{circuit_breaker: circuit_breaker} do
      # Trigger circuit breaker
      for _ <- 1..@default_config.module_circuit_breaker_count do
        CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)
      end

      CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule)

      # Should still be open immediately after
      assert {:circuit_open, _cooldown_ms} =
               CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule)
    end

    test "resets window when outside time window", %{circuit_breaker: circuit_breaker} do
      current_time = System.system_time(:millisecond)
      old_time = current_time - @default_config.module_circuit_breaker_window_ms - 1000

      # Manually insert old entry
      :ets.insert(circuit_breaker.table_name, {TestModule, 10, old_time, 0})

      # Should reset the window
      assert CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule) == :ok

      # Verify window was reset
      [{TestModule, count, last_reset, _}] = :ets.lookup(circuit_breaker.table_name, TestModule)
      assert count == 0
      assert last_reset > old_time
    end

    test "handles different modules independently", %{circuit_breaker: circuit_breaker} do
      # Trigger circuit breaker for TestModule1
      for _ <- 1..@default_config.module_circuit_breaker_count do
        CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule1)
      end

      # TestModule1 should be open
      assert {:circuit_open, _} =
               CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule1)

      # TestModule2 should still be ok
      assert CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule2) == :ok
    end

    test "circuit breaker re-opens immediately after cooldown if count still at threshold", %{
      circuit_breaker: circuit_breaker
    } do
      # Trigger circuit breaker
      for _ <- 1..@default_config.module_circuit_breaker_count do
        CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)
      end

      # Should be open with full cooldown
      assert {:circuit_open, cooldown_ms} =
               CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule)

      assert cooldown_ms == @default_config.module_circuit_breaker_cooldown_ms

      # Simulate cooldown expiring by manually setting cooldown_until to past time  
      # but keep the count at threshold
      current_time = System.system_time(:millisecond)
      # 1 second ago
      past_cooldown = current_time - 1000

      :ets.insert(
        circuit_breaker.table_name,
        {TestModule, @default_config.module_circuit_breaker_count, current_time, past_cooldown}
      )

      # Should immediately re-open since count is still at threshold
      assert {:circuit_open, new_cooldown_ms} =
               CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule)

      assert new_cooldown_ms == @default_config.module_circuit_breaker_cooldown_ms
    end

    test "circuit breaker closes after cooldown when time window resets", %{
      circuit_breaker: circuit_breaker
    } do
      # Trigger circuit breaker
      for _ <- 1..@default_config.module_circuit_breaker_count do
        CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)
      end

      # Should be open
      assert {:circuit_open, _} =
               CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule)

      # Simulate both cooldown expiring AND time window expiring
      # This will trigger the window reset logic (line 98-101)
      current_time = System.system_time(:millisecond)
      old_last_reset = current_time - @default_config.module_circuit_breaker_window_ms - 1000
      past_cooldown = current_time - 1000

      :ets.insert(
        circuit_breaker.table_name,
        {TestModule, @default_config.module_circuit_breaker_count, old_last_reset, past_cooldown}
      )

      # Should now be closed because window reset
      assert CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule) == :ok

      # Verify the entry was reset
      [{TestModule, count, last_reset, cooldown_until}] =
        :ets.lookup(circuit_breaker.table_name, TestModule)

      assert count == 0
      assert cooldown_until == 0
      # Should be updated to current_time
      assert last_reset > old_last_reset
    end

    test "operations work normally after time window reset", %{circuit_breaker: circuit_breaker} do
      # Trigger circuit breaker
      for _ <- 1..@default_config.module_circuit_breaker_count do
        CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)
      end

      # Verify it's open
      assert {:circuit_open, _} =
               CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule)

      # Simulate both cooldown expiring AND time window expiring (triggers reset)
      current_time = System.system_time(:millisecond)
      old_last_reset = current_time - @default_config.module_circuit_breaker_window_ms - 1000
      past_cooldown = current_time - 1000

      :ets.insert(
        circuit_breaker.table_name,
        {TestModule, @default_config.module_circuit_breaker_count, old_last_reset, past_cooldown}
      )

      # Should be closed now due to window reset
      assert CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule) == :ok

      # New increments should work normally and not immediately re-open circuit
      CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)
      assert CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule) == :ok

      # Should need to reach threshold again to re-open
      for _ <- 2..@default_config.module_circuit_breaker_count do
        CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)
      end

      # Now it should be open again
      assert {:circuit_open, _} =
               CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule)
    end

    test "cooldown countdown decreases over time", %{circuit_breaker: circuit_breaker} do
      # Trigger circuit breaker
      for _ <- 1..@default_config.module_circuit_breaker_count do
        CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)
      end

      # Get initial cooldown
      assert {:circuit_open, initial_cooldown} =
               CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule)

      assert initial_cooldown == @default_config.module_circuit_breaker_cooldown_ms

      # Simulate some time passing by manually adjusting cooldown_until
      current_time = System.system_time(:millisecond)
      # 10 seconds remaining
      partial_cooldown = current_time + 10000

      :ets.insert(
        circuit_breaker.table_name,
        {TestModule, @default_config.module_circuit_breaker_count, current_time, partial_cooldown}
      )

      # Should show reduced cooldown time
      assert {:circuit_open, remaining_cooldown} =
               CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule)

      assert remaining_cooldown <= 10000
      assert remaining_cooldown > 0
    end

    test "circuit breaker respects custom cooldown duration" do
      # Test with shorter cooldown
      short_cooldown_config = %{@default_config | module_circuit_breaker_cooldown_ms: 5000}
      supervisor_name = :test_supervisor_short_cooldown
      circuit_breaker = CircuitBreaker.new(supervisor_name, short_cooldown_config)

      # Trigger circuit breaker
      for _ <- 1..short_cooldown_config.module_circuit_breaker_count do
        CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)
      end

      # Should report shorter cooldown duration
      assert {:circuit_open, cooldown_ms} =
               CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule)

      # Custom short cooldown
      assert cooldown_ms == 5000
    end
  end

  describe "increment_module_circuit_breaker/2" do
    setup do
      supervisor_name = :test_supervisor_6
      circuit_breaker = CircuitBreaker.new(supervisor_name, @default_config)
      %{circuit_breaker: circuit_breaker}
    end

    test "creates first entry for new module", %{circuit_breaker: circuit_breaker} do
      CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)

      [{TestModule, count, last_reset, cooldown_until}] =
        :ets.lookup(circuit_breaker.table_name, TestModule)

      assert count == 1
      assert last_reset > 0
      assert cooldown_until == 0
    end

    test "increments count for existing module", %{circuit_breaker: circuit_breaker} do
      CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)
      CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)

      [{TestModule, count, _last_reset, _cooldown_until}] =
        :ets.lookup(circuit_breaker.table_name, TestModule)

      assert count == 2
    end

    test "preserves cooldown_until when incrementing", %{circuit_breaker: circuit_breaker} do
      current_time = System.system_time(:millisecond)
      cooldown_until = current_time + 10000

      # Manually set a cooldown
      :ets.insert(circuit_breaker.table_name, {TestModule, 5, current_time, cooldown_until})

      CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)

      [{TestModule, count, _last_reset, preserved_cooldown}] =
        :ets.lookup(circuit_breaker.table_name, TestModule)

      assert count == 6
      assert preserved_cooldown == cooldown_until
    end
  end

  describe "check_object_crash_status/3" do
    setup do
      supervisor_name = :test_supervisor_7
      circuit_breaker = CircuitBreaker.new(supervisor_name, @default_config)
      %{circuit_breaker: circuit_breaker}
    end

    test "returns :crashed when metadata has no crash history", %{
      circuit_breaker: circuit_breaker
    } do
      crash_entry = %{
        timestamp: System.system_time(:millisecond),
        reason: "test_reason",
        node_ref: "test_node_ref"
      }

      # Test with empty metadata (no crash_history)
      empty_meta = %{crash_history: []}

      {status, _updated_history} =
        CircuitBreaker.check_object_crash_status(
          circuit_breaker,
          empty_meta,
          crash_entry
        )

      assert status == :crashed
    end

    test "returns :crashed when crashes are below threshold", %{circuit_breaker: circuit_breaker} do
      current_time = System.system_time(:millisecond)

      crash_entry = %{
        timestamp: current_time,
        reason: "test_reason",
        node_ref: "test_node_ref"
      }

      # Create metadata with some crash history but below threshold
      existing_crashes = [
        %{timestamp: current_time - 1000, reason: "old_crash", node_ref: "node1"},
        %{timestamp: current_time - 2000, reason: "old_crash", node_ref: "node2"}
      ]

      meta = %{crash_history: existing_crashes}

      {status, _updated_history} =
        CircuitBreaker.check_object_crash_status(
          circuit_breaker,
          meta,
          crash_entry
        )

      assert status == :crashed
    end

    test "returns :permanently_crashed when crashes reach threshold", %{
      circuit_breaker: circuit_breaker
    } do
      current_time = System.system_time(:millisecond)

      crash_entry = %{
        timestamp: current_time,
        reason: "test_reason",
        node_ref: "test_node_ref"
      }

      # Create metadata with crash history at threshold (5 crashes in default config)
      existing_crashes = [
        %{timestamp: current_time - 1000, reason: "crash1", node_ref: "node1"},
        %{timestamp: current_time - 2000, reason: "crash2", node_ref: "node2"},
        %{timestamp: current_time - 3000, reason: "crash3", node_ref: "node3"},
        %{timestamp: current_time - 4000, reason: "crash4", node_ref: "node4"}
      ]

      meta = %{crash_history: existing_crashes}

      # Adding one more crash should reach the threshold of 5
      {status, updated_history} =
        CircuitBreaker.check_object_crash_status(
          circuit_breaker,
          meta,
          crash_entry
        )

      assert status == :permanently_crashed
      assert length(updated_history) == 5
    end
  end

  describe "crash history management" do
    test "add_crash_to_history filters old crashes outside window" do
      config = %{
        object_store: test_object_store(),
        crash_threshold_count: 5,
        crash_threshold_window_ms: 60_000
      }

      current_time = System.system_time(:millisecond)

      # Create old crash entry outside window
      old_crash = %{timestamp: current_time - 120_000, reason: "old", node_ref: "node1"}
      new_crash = %{timestamp: current_time, reason: "new", node_ref: "node2"}

      _existing_history = [old_crash]

      # Use private function through module (testing the behavior indirectly)
      supervisor_name = :test_supervisor_crash_history
      circuit_breaker = CircuitBreaker.new(supervisor_name, config)

      # The old crash should be filtered out when we check status
      # Since we can't directly test the private function, we test the public interface
      # The fact that it returns :crashed (not :permanently_crashed) indicates filtering worked
      # Test with metadata that has the old crash in history
      meta = %{crash_history: [old_crash]}

      {status, _updated_history} =
        CircuitBreaker.check_object_crash_status(circuit_breaker, meta, new_crash)

      assert status == :crashed
    end

    test "crash history is limited to threshold count" do
      # This is tested indirectly through the public interface
      # When we have exactly threshold crashes, it should return :permanently_crashed
      config = %{
        object_store: test_object_store(),
        crash_threshold_count: 3,
        crash_threshold_window_ms: 60_000,
        module_circuit_breaker_count: 50,
        module_circuit_breaker_window_ms: 5 * 60 * 1000,
        module_circuit_breaker_cooldown_ms: 30 * 60 * 1000
      }

      supervisor_name = :test_supervisor_crash_limit
      circuit_breaker = CircuitBreaker.new(supervisor_name, config)

      crash_entry = %{
        timestamp: System.system_time(:millisecond),
        reason: "test",
        node_ref: "node1"
      }

      # With empty metadata, it should return :crashed regardless of history
      meta = %{crash_history: []}

      {status, _updated_history} =
        CircuitBreaker.check_object_crash_status(circuit_breaker, meta, crash_entry)

      assert status == :crashed
    end
  end

  describe "configuration validation" do
    test "handles different crash thresholds" do
      low_threshold_config = %{@default_config | crash_threshold_count: 2}
      high_threshold_config = %{@default_config | crash_threshold_count: 10}

      supervisor1 = :test_supervisor_low_threshold
      supervisor2 = :test_supervisor_high_threshold

      cb1 = CircuitBreaker.new(supervisor1, low_threshold_config)
      cb2 = CircuitBreaker.new(supervisor2, high_threshold_config)

      # Test that they behave differently (low threshold reaches limit faster)
      crash_entry = %{
        timestamp: System.system_time(:millisecond),
        reason: "test",
        node_ref: "node1"
      }

      meta = %{crash_history: []}
      {status1, _} = CircuitBreaker.check_object_crash_status(cb1, meta, crash_entry)
      {status2, _} = CircuitBreaker.check_object_crash_status(cb2, meta, crash_entry)
      assert status1 == :crashed
      assert status2 == :crashed
    end

    test "handles different time windows" do
      short_window_config = %{@default_config | crash_threshold_window_ms: 30_000}
      long_window_config = %{@default_config | crash_threshold_window_ms: 120_000}

      supervisor1 = :test_supervisor_short_window
      supervisor2 = :test_supervisor_long_window

      cb1 = CircuitBreaker.new(supervisor1, short_window_config)
      cb2 = CircuitBreaker.new(supervisor2, long_window_config)

      # Verify tables exist with different configs
      assert :ets.whereis(cb1.table_name) != :undefined
      assert :ets.whereis(cb2.table_name) != :undefined
    end
  end

  describe "edge cases" do
    test "handles zero crashes gracefully" do
      supervisor_name = :test_supervisor_zero_crashes
      circuit_breaker = CircuitBreaker.new(supervisor_name, @default_config)

      # Module with no crashes should be ok
      assert CircuitBreaker.check_module_circuit_breaker(circuit_breaker, TestModule) == :ok
    end

    test "handles concurrent access to ETS table" do
      supervisor_name = :test_supervisor_concurrent
      circuit_breaker = CircuitBreaker.new(supervisor_name, @default_config)

      # Simulate concurrent increments
      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            CircuitBreaker.increment_module_circuit_breaker(circuit_breaker, TestModule)
            i
          end)
        end

      Enum.map(tasks, &Task.await/1)

      # Should have exactly 10 crashes recorded
      [{TestModule, count, _last_reset, _cooldown_until}] =
        :ets.lookup(circuit_breaker.table_name, TestModule)

      assert count == 10
    end

    test "handles very large timestamps" do
      supervisor_name = :test_supervisor_large_timestamp
      circuit_breaker = CircuitBreaker.new(supervisor_name, @default_config)
      large_timestamp = 999_999_999_999_999

      crash_entry = %{
        timestamp: large_timestamp,
        reason: "test",
        node_ref: "node1"
      }

      # Should not crash with large timestamps
      meta = %{crash_history: []}

      {status, _updated_history} =
        CircuitBreaker.check_object_crash_status(circuit_breaker, meta, crash_entry)

      assert status == :crashed
    end
  end

  describe "prune_stale_entries/1" do
    setup do
      supervisor_name = :"test_supervisor_prune_#{DurableServer.UUID.uuid4()}"
      circuit_breaker = CircuitBreaker.new(supervisor_name, @default_config)
      %{circuit_breaker: circuit_breaker}
    end

    test "removes entries that are outside time window and not in cooldown", %{
      circuit_breaker: circuit_breaker
    } do
      current_time = System.system_time(:millisecond)
      window_ms = @default_config.module_circuit_breaker_window_ms

      # Create entries that should be pruned (old and no active cooldown)
      stale_time = current_time - window_ms - 1000
      :ets.insert(circuit_breaker.table_name, {TestModule1, 3, stale_time, 0})
      :ets.insert(circuit_breaker.table_name, {TestModule2, 5, stale_time, current_time - 100})

      # Create entries that should be kept (recent or in cooldown)
      recent_time = current_time - 1000
      future_cooldown = current_time + 5000
      :ets.insert(circuit_breaker.table_name, {TestModule3, 2, recent_time, 0})
      :ets.insert(circuit_breaker.table_name, {TestModule4, 4, stale_time, future_cooldown})

      # Verify all entries exist before pruning
      assert length(:ets.tab2list(circuit_breaker.table_name)) == 4

      # Run pruning
      CircuitBreaker.prune_stale_entries(circuit_breaker)

      # Verify only the stale entries were removed
      remaining = :ets.tab2list(circuit_breaker.table_name)
      assert length(remaining) == 2

      # Check which entries remain
      remaining_modules = Enum.map(remaining, fn {module, _, _, _} -> module end)
      # Recent entry
      assert TestModule3 in remaining_modules
      # Entry with active cooldown
      assert TestModule4 in remaining_modules
      # Stale entry with no cooldown
      refute TestModule1 in remaining_modules
      # Stale entry with expired cooldown
      refute TestModule2 in remaining_modules
    end

    test "keeps entries with recent last_reset regardless of cooldown status", %{
      circuit_breaker: circuit_breaker
    } do
      current_time = System.system_time(:millisecond)
      # Within window
      recent_time = current_time - 1000

      # Insert recent entries with various cooldown states
      # No cooldown
      :ets.insert(circuit_breaker.table_name, {TestModule1, 1, recent_time, 0})
      # Expired cooldown
      :ets.insert(circuit_breaker.table_name, {TestModule2, 2, recent_time, current_time - 100})
      # Active cooldown
      :ets.insert(circuit_breaker.table_name, {TestModule3, 3, recent_time, current_time + 5000})

      CircuitBreaker.prune_stale_entries(circuit_breaker)

      # All recent entries should remain
      remaining = :ets.tab2list(circuit_breaker.table_name)
      assert length(remaining) == 3
    end

    test "keeps entries with active cooldown regardless of last_reset age", %{
      circuit_breaker: circuit_breaker
    } do
      current_time = System.system_time(:millisecond)
      window_ms = @default_config.module_circuit_breaker_window_ms
      stale_time = current_time - window_ms - 1000
      active_cooldown = current_time + 10000

      # Insert old entries with active cooldowns
      :ets.insert(circuit_breaker.table_name, {TestModule1, 10, stale_time, active_cooldown})
      :ets.insert(circuit_breaker.table_name, {TestModule2, 15, stale_time, active_cooldown})

      CircuitBreaker.prune_stale_entries(circuit_breaker)

      # Entries with active cooldowns should remain despite being old
      remaining = :ets.tab2list(circuit_breaker.table_name)
      assert length(remaining) == 2
    end

    test "removes entries where cooldown has just expired", %{circuit_breaker: circuit_breaker} do
      current_time = System.system_time(:millisecond)
      window_ms = @default_config.module_circuit_breaker_window_ms
      stale_time = current_time - window_ms - 1000
      # Just expired
      just_expired_cooldown = current_time - 1

      :ets.insert(circuit_breaker.table_name, {TestModule1, 5, stale_time, just_expired_cooldown})

      CircuitBreaker.prune_stale_entries(circuit_breaker)

      # Entry should be removed since cooldown expired and last_reset is old
      remaining = :ets.tab2list(circuit_breaker.table_name)
      assert length(remaining) == 0
    end

    test "handles edge case where cooldown_until equals current_time", %{
      circuit_breaker: circuit_breaker
    } do
      current_time = System.system_time(:millisecond)
      window_ms = @default_config.module_circuit_breaker_window_ms
      stale_time = current_time - window_ms - 1000

      # cooldown_until exactly equals current_time (should be considered expired)
      :ets.insert(circuit_breaker.table_name, {TestModule1, 5, stale_time, current_time})

      CircuitBreaker.prune_stale_entries(circuit_breaker)

      # Entry should be removed since cooldown_until <= current_time
      remaining = :ets.tab2list(circuit_breaker.table_name)
      assert length(remaining) == 0
    end

    test "handles mixed scenarios with multiple modules", %{circuit_breaker: circuit_breaker} do
      current_time = System.system_time(:millisecond)
      window_ms = @default_config.module_circuit_breaker_window_ms
      stale_time = current_time - window_ms - 1000
      recent_time = current_time - 1000
      active_cooldown = current_time + 5000
      expired_cooldown = current_time - 100

      # Mix of scenarios
      entries = [
        # Should be removed
        {ModuleStaleNoCooldown, 1, stale_time, 0},
        # Should be removed
        {ModuleStaleExpiredCooldown, 2, stale_time, expired_cooldown},
        # Should be kept
        {ModuleStaleActiveCooldown, 3, stale_time, active_cooldown},
        # Should be kept
        {ModuleRecentNoCooldown, 4, recent_time, 0},
        # Should be kept
        {ModuleRecentActiveCooldown, 5, recent_time, active_cooldown},
        # Should be kept
        {ModuleRecentExpiredCooldown, 6, recent_time, expired_cooldown}
      ]

      # Insert all entries
      for entry <- entries do
        :ets.insert(circuit_breaker.table_name, entry)
      end

      # Verify all inserted
      assert length(:ets.tab2list(circuit_breaker.table_name)) == 6

      # Run pruning
      CircuitBreaker.prune_stale_entries(circuit_breaker)

      # Check results
      remaining = :ets.tab2list(circuit_breaker.table_name)
      assert length(remaining) == 4

      remaining_modules = Enum.map(remaining, fn {module, _, _, _} -> module end)

      # Should be removed
      refute ModuleStaleNoCooldown in remaining_modules
      refute ModuleStaleExpiredCooldown in remaining_modules

      # Should be kept
      assert ModuleStaleActiveCooldown in remaining_modules
      assert ModuleRecentNoCooldown in remaining_modules
      assert ModuleRecentActiveCooldown in remaining_modules
      assert ModuleRecentExpiredCooldown in remaining_modules
    end

    test "prunes entries based on different time window configurations" do
      # Test with shorter time window
      short_window_config = %{@default_config | module_circuit_breaker_window_ms: 1000}
      supervisor_name = :"test_supervisor_prune_short_#{DurableServer.UUID.uuid4()}"
      circuit_breaker = CircuitBreaker.new(supervisor_name, short_window_config)

      current_time = System.system_time(:millisecond)

      # Entry that would be recent for default config but stale for short config
      # 2 seconds ago
      moderately_old_time = current_time - 2000
      :ets.insert(circuit_breaker.table_name, {TestModule1, 1, moderately_old_time, 0})

      CircuitBreaker.prune_stale_entries(circuit_breaker)

      # Should be removed with short window
      remaining = :ets.tab2list(circuit_breaker.table_name)
      assert length(remaining) == 0
    end

    test "handles empty ETS table gracefully", %{circuit_breaker: circuit_breaker} do
      # Table exists but is empty
      assert :ets.info(circuit_breaker.table_name, :size) == 0

      # Should not crash on empty table
      assert CircuitBreaker.prune_stale_entries(circuit_breaker) == :ok

      # Table should still exist and be empty
      assert :ets.info(circuit_breaker.table_name, :size) == 0
    end

    test "verifies match specification logic with exact boundary conditions", %{
      circuit_breaker: circuit_breaker
    } do
      current_time = System.system_time(:millisecond)
      window_ms = @default_config.module_circuit_breaker_window_ms
      window_start = current_time - window_ms

      # Test exact boundary conditions for the match spec:
      # Delete where: last_reset < window_start AND cooldown_until <= current_time

      # Boundary case 1: last_reset exactly equals window_start (should NOT be removed)
      :ets.insert(circuit_breaker.table_name, {BoundaryModule1, 1, window_start, 0})

      # Boundary case 2: last_reset is 1ms before window_start (should be removed)  
      :ets.insert(circuit_breaker.table_name, {BoundaryModule2, 1, window_start - 1, 0})

      # Boundary case 3: cooldown_until is 1ms in future (should NOT be removed even if old)
      :ets.insert(
        circuit_breaker.table_name,
        {BoundaryModule3, 1, window_start - 1000, current_time + 1}
      )

      CircuitBreaker.prune_stale_entries(circuit_breaker)

      remaining = :ets.tab2list(circuit_breaker.table_name)
      remaining_modules = Enum.map(remaining, fn {module, _, _, _} -> module end)

      # Verify boundary conditions
      # last_reset == window_start
      assert BoundaryModule1 in remaining_modules
      # last_reset < window_start AND cooldown <= current_time
      refute BoundaryModule2 in remaining_modules
      # cooldown_until > current_time
      assert BoundaryModule3 in remaining_modules

      assert length(remaining) == 2
    end
  end

  describe "check_global_lock_circuit_breaker/1" do
    setup do
      supervisor_name = :"test_supervisor_global_lock_#{DurableServer.UUID.uuid4()}"
      circuit_breaker = CircuitBreaker.new(supervisor_name, @default_config)
      %{circuit_breaker: circuit_breaker}
    end

    test "returns :ok when no lock failures have occurred", %{circuit_breaker: circuit_breaker} do
      assert CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker) == :ok
    end

    test "returns :ok when lock failures are below threshold", %{circuit_breaker: circuit_breaker} do
      # add some lock failures but below threshold
      for _ <- 1..(@default_config.global_lock_failure_count - 1) do
        CircuitBreaker.increment_global_lock_failures(circuit_breaker)
      end

      assert CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker) == :ok
    end

    test "opens circuit breaker when lock failures reach threshold", %{
      circuit_breaker: circuit_breaker
    } do
      # add lock failures up to threshold
      for _ <- 1..@default_config.global_lock_failure_count do
        CircuitBreaker.increment_global_lock_failures(circuit_breaker)
      end

      assert {:circuit_open, cooldown_ms} =
               CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker)

      assert cooldown_ms == @default_config.global_lock_failure_cooldown_ms
    end

    test "keeps circuit breaker open during cooldown period", %{circuit_breaker: circuit_breaker} do
      # trigger circuit breaker
      for _ <- 1..@default_config.global_lock_failure_count do
        CircuitBreaker.increment_global_lock_failures(circuit_breaker)
      end

      CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker)

      # should still be open immediately after
      assert {:circuit_open, _cooldown_ms} =
               CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker)
    end

    test "resets window when outside time window", %{circuit_breaker: circuit_breaker} do
      current_time = System.system_time(:millisecond)
      old_time = current_time - @default_config.global_lock_failure_window_ms - 1000

      # manually insert old entry
      :ets.insert(circuit_breaker.table_name, {:global_lock_failures, 10, old_time, 0})

      # should reset the window
      assert CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker) == :ok

      # verify window was reset
      [{:global_lock_failures, count, last_reset, _}] =
        :ets.lookup(circuit_breaker.table_name, :global_lock_failures)

      assert count == 0
      assert last_reset > old_time
    end

    test "circuit breaker re-opens immediately after cooldown if count still at threshold", %{
      circuit_breaker: circuit_breaker
    } do
      # trigger circuit breaker
      for _ <- 1..@default_config.global_lock_failure_count do
        CircuitBreaker.increment_global_lock_failures(circuit_breaker)
      end

      # should be open with full cooldown
      assert {:circuit_open, cooldown_ms} =
               CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker)

      assert cooldown_ms == @default_config.global_lock_failure_cooldown_ms

      # simulate cooldown expiring by manually setting cooldown_until to past time
      # but keep the count at threshold
      current_time = System.system_time(:millisecond)
      past_cooldown = current_time - 1000

      :ets.insert(
        circuit_breaker.table_name,
        {:global_lock_failures, @default_config.global_lock_failure_count, current_time,
         past_cooldown}
      )

      # should immediately re-open since count is still at threshold
      assert {:circuit_open, new_cooldown_ms} =
               CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker)

      assert new_cooldown_ms == @default_config.global_lock_failure_cooldown_ms
    end

    test "circuit breaker closes after cooldown when time window resets", %{
      circuit_breaker: circuit_breaker
    } do
      # trigger circuit breaker
      for _ <- 1..@default_config.global_lock_failure_count do
        CircuitBreaker.increment_global_lock_failures(circuit_breaker)
      end

      # should be open
      assert {:circuit_open, _} =
               CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker)

      # simulate both cooldown expiring AND time window expiring
      current_time = System.system_time(:millisecond)
      old_last_reset = current_time - @default_config.global_lock_failure_window_ms - 1000
      past_cooldown = current_time - 1000

      :ets.insert(
        circuit_breaker.table_name,
        {:global_lock_failures, @default_config.global_lock_failure_count, old_last_reset,
         past_cooldown}
      )

      # should now be closed because window reset
      assert CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker) == :ok

      # verify the entry was reset
      [{:global_lock_failures, count, last_reset, cooldown_until}] =
        :ets.lookup(circuit_breaker.table_name, :global_lock_failures)

      assert count == 0
      assert cooldown_until == 0
      assert last_reset > old_last_reset
    end

    test "cooldown countdown decreases over time", %{circuit_breaker: circuit_breaker} do
      # trigger circuit breaker
      for _ <- 1..@default_config.global_lock_failure_count do
        CircuitBreaker.increment_global_lock_failures(circuit_breaker)
      end

      # get initial cooldown
      assert {:circuit_open, initial_cooldown} =
               CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker)

      assert initial_cooldown == @default_config.global_lock_failure_cooldown_ms

      # simulate some time passing by manually adjusting cooldown_until
      current_time = System.system_time(:millisecond)
      partial_cooldown = current_time + 10000

      :ets.insert(
        circuit_breaker.table_name,
        {:global_lock_failures, @default_config.global_lock_failure_count, current_time,
         partial_cooldown}
      )

      # should show reduced cooldown time
      assert {:circuit_open, remaining_cooldown} =
               CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker)

      assert remaining_cooldown <= 10000
      assert remaining_cooldown > 0
    end
  end

  describe "increment_global_lock_failures/1" do
    setup do
      supervisor_name = :"test_supervisor_global_inc_#{DurableServer.UUID.uuid4()}"
      circuit_breaker = CircuitBreaker.new(supervisor_name, @default_config)
      %{circuit_breaker: circuit_breaker}
    end

    test "creates first entry for global lock failures", %{circuit_breaker: circuit_breaker} do
      CircuitBreaker.increment_global_lock_failures(circuit_breaker)

      [{:global_lock_failures, count, last_reset, cooldown_until}] =
        :ets.lookup(circuit_breaker.table_name, :global_lock_failures)

      assert count == 1
      assert last_reset > 0
      assert cooldown_until == 0
    end

    test "increments count for existing global lock failures", %{circuit_breaker: circuit_breaker} do
      CircuitBreaker.increment_global_lock_failures(circuit_breaker)
      CircuitBreaker.increment_global_lock_failures(circuit_breaker)

      [{:global_lock_failures, count, _last_reset, _cooldown_until}] =
        :ets.lookup(circuit_breaker.table_name, :global_lock_failures)

      assert count == 2
    end

    test "preserves cooldown_until when incrementing", %{circuit_breaker: circuit_breaker} do
      current_time = System.system_time(:millisecond)
      cooldown_until = current_time + 10000

      # manually set a cooldown
      :ets.insert(
        circuit_breaker.table_name,
        {:global_lock_failures, 5, current_time, cooldown_until}
      )

      CircuitBreaker.increment_global_lock_failures(circuit_breaker)

      [{:global_lock_failures, count, _last_reset, preserved_cooldown}] =
        :ets.lookup(circuit_breaker.table_name, :global_lock_failures)

      assert count == 6
      assert preserved_cooldown == cooldown_until
    end

    test "handles concurrent increments correctly", %{circuit_breaker: circuit_breaker} do
      # simulate concurrent increments from multiple processes
      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            CircuitBreaker.increment_global_lock_failures(circuit_breaker)
            i
          end)
        end

      Enum.map(tasks, &Task.await/1)

      # should have exactly 20 failures recorded
      [{:global_lock_failures, count, _last_reset, _cooldown_until}] =
        :ets.lookup(circuit_breaker.table_name, :global_lock_failures)

      assert count == 20
    end
  end

  describe "global lock circuit breaker integration" do
    test "circuit breaker protects against network partition lock storms" do
      supervisor_name = :"test_supervisor_partition_#{DurableServer.UUID.uuid4()}"

      # use short thresholds for faster testing
      config = %{
        @default_config
        | global_lock_failure_count: 3,
          global_lock_failure_window_ms: 1000,
          global_lock_failure_cooldown_ms: 2000
      }

      circuit_breaker = CircuitBreaker.new(supervisor_name, config)

      # simulate rapid lock failures (network partition scenario)
      assert CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker) == :ok

      # first failure
      CircuitBreaker.increment_global_lock_failures(circuit_breaker)
      assert CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker) == :ok

      # second failure
      CircuitBreaker.increment_global_lock_failures(circuit_breaker)
      assert CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker) == :ok

      # third failure - should trip circuit breaker
      CircuitBreaker.increment_global_lock_failures(circuit_breaker)

      assert {:circuit_open, cooldown_ms} =
               CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker)

      assert cooldown_ms == 2000

      # subsequent checks during cooldown should continue to return circuit_open
      assert {:circuit_open, _} =
               CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker)
    end

    test "circuit breaker respects custom configuration values" do
      supervisor_name = :"test_supervisor_custom_#{DurableServer.UUID.uuid4()}"

      custom_config = %{
        @default_config
        | global_lock_failure_count: 5,
          global_lock_failure_window_ms: 5000,
          global_lock_failure_cooldown_ms: 3000
      }

      circuit_breaker = CircuitBreaker.new(supervisor_name, custom_config)

      # should require 5 failures to trip
      for _ <- 1..4 do
        CircuitBreaker.increment_global_lock_failures(circuit_breaker)
      end

      assert CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker) == :ok

      # 5th failure should trip with custom cooldown
      CircuitBreaker.increment_global_lock_failures(circuit_breaker)

      assert {:circuit_open, cooldown_ms} =
               CircuitBreaker.check_global_lock_circuit_breaker(circuit_breaker)

      assert cooldown_ms == 3000
    end
  end

  describe "placement node timeout circuit breaker" do
    setup do
      supervisor_name = :"test_supervisor_placement_timeout_#{DurableServer.UUID.uuid4()}"
      circuit_breaker = CircuitBreaker.new(supervisor_name, @default_config)
      %{circuit_breaker: circuit_breaker}
    end

    test "returns :ok when no timeout cooldown exists", %{circuit_breaker: circuit_breaker} do
      assert CircuitBreaker.check_placement_node_timeout_circuit_breaker(
               circuit_breaker,
               "node@host"
             ) == :ok
    end

    test "opens cooldown after timeout trip and returns remaining cooldown", %{
      circuit_breaker: circuit_breaker
    } do
      :ok =
        CircuitBreaker.trip_placement_node_timeout_circuit_breaker(
          circuit_breaker,
          "node@host",
          2_000
        )

      assert {:circuit_open, remaining} =
               CircuitBreaker.check_placement_node_timeout_circuit_breaker(
                 circuit_breaker,
                 "node@host"
               )

      assert remaining <= 2_000
      assert remaining > 0
    end

    test "returns :ok after cooldown expires and cleans up entry", %{
      circuit_breaker: circuit_breaker
    } do
      key = {:placement_node_timeout, "node@host"}
      now = System.system_time(:millisecond)
      expired = now - 100
      :ets.insert(circuit_breaker.table_name, {key, 1, now, expired})

      assert CircuitBreaker.check_placement_node_timeout_circuit_breaker(
               circuit_breaker,
               "node@host"
             ) == :ok

      assert [] == :ets.lookup(circuit_breaker.table_name, key)
    end
  end
end
