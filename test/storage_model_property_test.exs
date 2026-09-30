defmodule DurableServer.StorageModelPropertyTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias DurableServer.{PropertyFixture, StorageBackend}

  @moduletag :property
  @moduletag capture_log: [level: :warning]

  setup do
    root = Path.expand("tmp/storage_property/#{DurableServer.UUID.uuid4()}")
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, root: root}
  end

  property "claims and conditional mutations follow object generations, including delete/recreate",
           %{root: root} do
    check all(commands <- list_of(command(), max_length: 60)) do
      PropertyFixture.with_sample(root, :storage, fn %{backend: backend} ->
        # Give every key a real historical token; no invalid-token stand-ins.
        model =
          Enum.reduce(["a", "b", "c"], %{}, fn key, model ->
            model
            |> step({:claim, key, 0}, backend)
            |> step({:delete, key, 0}, backend)
          end)

        # Guarantee that even an empty/shrunk sequence exercises an old delete
        # against a recreated object, as well as an old write.
        commands =
          [{:claim, "a", 1}, {:delete, "a", 1}, {:write, "a", 2, 1}] ++ commands

        Enum.reduce(commands, model, &step(&2, &1, backend))
      end)
    end
  end

  test "a failed sample is stopped and removed before the next sample", %{root: root} do
    assert_raise RuntimeError, "deliberate sample failure", fn ->
      PropertyFixture.with_sample(root, :storage, fn %{backend: backend} ->
        assert {:ok, _} = StorageBackend.put_object(backend, "leftover", 1)
        raise "deliberate sample failure"
      end)
    end

    assert File.ls!(root) == []

    PropertyFixture.with_sample(root, :storage, fn %{backend: backend} ->
      assert {:error, :not_found} =
               StorageBackend.get_object(backend, "leftover", consistent: true)
    end)

    assert File.ls!(root) == []
  end

  defp command do
    key = member_of(["a", "b", "c"])
    value = integer(-100..100)
    token = integer(0..8)

    frequency([
      {3, tuple({constant(:claim), key, value})},
      {4, tuple({constant(:write), key, value, token})},
      {3, tuple({constant(:delete), key, token})},
      {1, tuple({constant(:read), key})}
    ])
  end

  # The reference state uses logical generations, not EKV versions or ETag
  # equality. ETags are opaque handles used only to invoke the real backend.
  defp step(model, command, backend) do
    key = elem(command, 1)
    object = Map.get(model, key, %{current: nil, history: [], generation: 0})
    object = execute(command, object, backend)
    model = Map.put(model, key, object)

    # Check every key, not only the one just touched (cross-key corruption).
    for {key, expected} <- model do
      actual = StorageBackend.get_object(backend, key, consistent: true)

      case expected.current do
        nil ->
          assert actual == {:error, :not_found}

        %{value: value, etag: etag} ->
          assert actual == {:ok, %{body: value, etag: etag}}
      end
    end

    model
  end

  defp execute({:claim, key, value}, object, backend) do
    result = StorageBackend.try_claim(backend, key, value)

    if object.current do
      assert result == {:error, :already_claimed}
      object
    else
      assert {:ok, {:claimed, etag}} = result
      remember(object, value, etag)
    end
  end

  defp execute({:write, key, value, index}, object, backend) do
    token = Enum.at(object.history, rem(index, length(object.history)))
    result = StorageBackend.put_object(backend, key, value, etag: token.etag, max_retries: 0)

    if object.current && object.current.generation == token.generation do
      assert {:ok, %{body: ^value, etag: etag}} = result
      remember(object, value, etag)
    else
      assert result == {:error, :conflict}
      object
    end
  end

  defp execute({:delete, key, index}, object, backend) do
    token = Enum.at(object.history, rem(index, length(object.history)))
    result = StorageBackend.delete_object(backend, key, etag: token.etag)

    cond do
      object.current == nil ->
        assert result == {:error, :not_found}
        object

      object.current.generation == token.generation ->
        assert result == :ok
        %{object | current: nil}

      true ->
        assert result == {:error, :conflict}
        object
    end
  end

  defp execute({:read, _key}, object, _backend), do: object

  defp remember(object, value, etag) do
    refute Enum.any?(object.history, &(&1.etag == etag)), "an old ownership token was reused"
    next = %{value: value, etag: etag, generation: object.generation + 1}
    %{object | current: next, generation: next.generation, history: [next | object.history]}
  end
end
