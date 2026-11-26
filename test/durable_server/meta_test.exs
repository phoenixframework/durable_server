defmodule DurableServer.MetaTest do
  use ExUnit.Case, async: true

  alias DurableServer.Meta
  alias DurableServer.StoredState

  @context %{key: "server-key", prefix: "tenant/"}

  test "stored metadata can disable automatic restart" do
    for status <- [:running, :crashed, :stopped_graceful] do
      assert Meta.automatic_restart_disabled?(%Meta{permanent: false, status: status})
      refute Meta.automatic_restart_disabled?(%Meta{permanent: true, status: status})
    end

    for status <- [:deleting, :cordoned, :permanently_crashed, :stopped_permanent] do
      assert Meta.automatic_restart_disabled?(%Meta{permanent: true, status: status})
    end
  end

  test "round-trips valid metadata through the hardened decoder" do
    meta = %Meta{
      module: __MODULE__,
      lock_epoch: 42,
      permanent: true,
      pid: self(),
      status: :running,
      supervisor: :meta_test_supervisor,
      task_supervisor: :meta_test_tasks,
      dynamic_supervisor: :meta_test_dynamic,
      node_ref: 123,
      node_str: "node@example",
      last_heartbeat_at: 456,
      crash_history: [100, 200],
      init_from_ref: make_ref(),
      init_from_pid: self()
    }

    decoded = meta |> Meta.encode_to_binary() |> Meta.decode_from_binary(@context)

    assert decoded == %{meta | key: @context.key, prefix: @context.prefix}
  end

  test "defaults metadata written before lock epochs to zero" do
    encoded =
      %Meta{module: __MODULE__, status: :running}
      |> Meta.to_storage_term()
      |> Map.delete(:lock_epoch)
      |> :erlang.term_to_binary()
      |> Base.encode64()

    assert %Meta{lock_epoch: 0} = Meta.decode_from_binary(encoded, @context)
  end

  test "decodes persisted metadata atoms that do not exist in the current VM" do
    atom_name = "Elixir.DurableMetaOldNode#{System.unique_integer([:positive, :monotonic])}"
    assert_raise ArgumentError, fn -> String.to_existing_atom(atom_name) end

    encoded =
      <<131, 116, 2::unsigned-big-32, 119, 6, "status", 119, 7, "running", 119, 6, "module", 119,
        byte_size(atom_name), atom_name::binary>>
      |> Base.encode64()

    decoded = Meta.decode_from_binary(encoded, @context)

    assert decoded.module == String.to_existing_atom(atom_name)
    assert decoded.status == :running
  end

  test "rejects executable and malformed external terms with a controlled error" do
    executable = :erlang.term_to_binary(%{status: :running, payload: fn -> :unsafe end})

    for encoded <- [
          Base.encode64(executable),
          "not base64!",
          Base.encode64(<<131, 80, 0, 0, 0, 1, 0>>)
        ] do
      term = %{"vsn" => 1, "state" => %{}, "meta" => encoded}
      assert {:error, %ArgumentError{}} = StoredState.from_object_store_term(term)
    end
  end

  test "rejects invalid metadata field types" do
    encoded =
      %{status: :running, permanent: "yes"}
      |> :erlang.term_to_binary()
      |> Base.encode64()

    assert_raise ArgumentError, ~r/invalid metadata field :permanent/, fn ->
      Meta.decode_from_binary(encoded, @context)
    end
  end

  test "rejects negative lock epochs" do
    encoded =
      %{module: __MODULE__, status: :running, lock_epoch: -1}
      |> :erlang.term_to_binary()
      |> Base.encode64()

    assert_raise ArgumentError, ~r/invalid metadata field :lock_epoch/, fn ->
      Meta.decode_from_binary(encoded, @context)
    end
  end
end
