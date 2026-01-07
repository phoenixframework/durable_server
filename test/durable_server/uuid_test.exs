defmodule DurableServer.UUIDTest do
  use ExUnit.Case, async: true

  alias DurableServer.UUID

  describe "uuid4/0" do
    test "generates a valid UUID v4 string" do
      uuid = UUID.uuid4()

      assert is_binary(uuid)
      assert byte_size(uuid) == 36

      assert Regex.match?(
               ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
               uuid
             )
    end

    test "generates unique UUIDs" do
      uuids = for _ <- 1..100, do: UUID.uuid4()
      assert length(Enum.uniq(uuids)) == 100
    end

    test "version nibble is always 4" do
      for _ <- 1..10 do
        uuid = UUID.uuid4()
        # Version is the 13th character (after 8-4-)
        assert String.at(uuid, 14) == "4"
      end
    end

    test "variant bits are correct (8, 9, a, or b)" do
      for _ <- 1..10 do
        uuid = UUID.uuid4()
        # Variant is the 17th character (after 8-4-4-)
        variant_char = String.at(uuid, 19)
        assert variant_char in ["8", "9", "a", "b"]
      end
    end
  end
end
