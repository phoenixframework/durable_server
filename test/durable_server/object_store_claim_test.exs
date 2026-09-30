defmodule DurableServer.ObjectStoreClaimTest do
  use ExUnit.Case, async: true

  alias DurableServer.Backends.ObjectStore, as: ObjectStoreBackend
  alias DurableServer.{Meta, ObjectStore, StorageBackend, StoredState}

  @moduletag :capture_log

  describe "initial claim retries" do
    test "retries transient failures with fresh signatures" do
      store =
        object_store([
          %Req.TransportError{reason: :closed},
          %Req.Response{status: 503},
          %Req.Response{status: 200, headers: %{"etag" => ["claimed-etag"]}}
        ])

      assert {:ok, {:claimed, "claimed-etag"}} =
               ObjectStore.try_claim(store, "server/one", "state")

      for _attempt <- 1..3 do
        assert_receive {:request, %Req.Request{method: :put} = request}
        assert Req.Request.get_header(request, "if-none-match") == ["*"]
        assert [authorization] = Req.Request.get_header(request, "authorization")
        refute authorization =~ "SignedHeaders=authorization;"
      end
    end

    test "stops after two retries" do
      store = object_store(List.duplicate(%Req.TransportError{reason: :closed}, 3))

      assert {:error, %Req.TransportError{reason: :closed}} =
               ObjectStore.try_claim(store, "server/one", "state")

      for _attempt <- 1..3 do
        assert_receive {:request, %Req.Request{method: :put}}
      end
    end

    test "does not retry a permanent response" do
      store = object_store([%Req.Response{status: 403}])

      assert {:error, %Req.Response{status: 403}} =
               ObjectStore.try_claim(store, "server/one", "state")
    end

    test "does not retry a claim conflict" do
      store = object_store([%Req.Response{status: 412}])

      assert {:error, :already_claimed} = ObjectStore.try_claim(store, "server/one", "state")
    end
  end

  describe "initial claim recovery" do
    test "adopts the committed claim when a lost response is followed by a conflict" do
      attempted = stored_state(%{value: 1})

      store =
        object_store([
          %Req.TransportError{reason: :closed},
          %Req.Response{status: 412},
          stored_response(attempted)
        ])

      backend = StorageBackend.new(ObjectStoreBackend, store)

      assert {:ok, {:claimed, "committed-etag"}} =
               StorageBackend.try_claim(backend, "server/one", attempted)

      assert_receive {:request, %Req.Request{method: :put}}
      assert_receive {:request, %Req.Request{method: :put}}
      assert_receive {:request, %Req.Request{method: :get} = read}
      assert Req.Request.get_header(read, "x-tigris-consistent") == ["true"]
    end

    test "adopts the committed claim when all PUT responses are lost" do
      attempted = stored_state(%{value: 1})

      store =
        object_store([
          %Req.TransportError{reason: :closed},
          %Req.TransportError{reason: :closed},
          %Req.TransportError{reason: :closed},
          stored_response(attempted)
        ])

      backend = StorageBackend.new(ObjectStoreBackend, store)

      assert {:ok, {:claimed, "committed-etag"}} =
               StorageBackend.try_claim(backend, "server/one", attempted)
    end

    test "does not adopt a claim owned by another boot" do
      attempted = stored_state(%{value: 1})
      persisted = stored_state(%{value: 1}, %{node_ref: 124})
      store = object_store([%Req.Response{status: 412}, stored_response(persisted)])
      backend = StorageBackend.new(ObjectStoreBackend, store)

      assert {:error, :already_claimed} =
               StorageBackend.try_claim(backend, "server/one", attempted)
    end

    test "does not adopt different state owned by the same boot" do
      attempted = stored_state(%{value: 1})
      persisted = stored_state(%{value: 2})
      store = object_store([%Req.Response{status: 412}, stored_response(persisted)])
      backend = StorageBackend.new(ObjectStoreBackend, store)

      assert {:error, :already_claimed} =
               StorageBackend.try_claim(backend, "server/one", attempted)
    end

    for field <- [:pid, :node_ref, :node_str] do
      test "does not adopt a matching claim without #{field}" do
        attempted = stored_state(%{value: 1}, %{unquote(field) => nil})
        store = object_store([%Req.Response{status: 412}, stored_response(attempted)])
        backend = StorageBackend.new(ObjectStoreBackend, store)

        assert {:error, :already_claimed} =
                 StorageBackend.try_claim(backend, "server/one", attempted)
      end
    end

    test "does not adopt generic values after a conflict" do
      store = object_store([%Req.Response{status: 412}])
      backend = StorageBackend.new(ObjectStoreBackend, store)

      assert {:error, :already_claimed} =
               StorageBackend.try_claim(backend, "server/one", %{value: 1})
    end

    test "preserves the transport error when the claim was not stored" do
      store =
        object_store([
          %Req.TransportError{reason: :closed},
          %Req.TransportError{reason: :closed},
          %Req.TransportError{reason: :closed},
          %Req.Response{status: 404}
        ])

      backend = StorageBackend.new(ObjectStoreBackend, store)

      assert {:error, %Req.TransportError{reason: :closed}} =
               StorageBackend.try_claim(backend, "server/one", stored_state(%{value: 1}))
    end

    test "preserves the conflict when reading the claim fails" do
      store = object_store([%Req.Response{status: 412}, %Req.Response{status: 403}])
      backend = StorageBackend.new(ObjectStoreBackend, store)

      assert {:error, :already_claimed} =
               StorageBackend.try_claim(backend, "server/one", stored_state(%{value: 1}))
    end
  end

  defp stored_state(state, owner_overrides \\ %{}) do
    meta =
      struct!(
        Meta,
        Map.merge(
          %{
            module: __MODULE__,
            permanent: false,
            pid: self(),
            status: :running,
            node_ref: 123,
            node_str: "test@node"
          },
          owner_overrides
        )
      )

    %StoredState{vsn: 1, state: state, meta: meta}
  end

  defp stored_response(state) do
    {:ok, encoded} = ObjectStoreBackend.encode(%ObjectStore{}, state)
    %Req.Response{status: 200, body: encoded, headers: %{"etag" => ["committed-etag"]}}
  end

  defp object_store(responses) do
    parent = self()
    responses = start_supervised!({Agent, fn -> responses end})

    adapter = fn request ->
      send(parent, {:request, request})
      response = Agent.get_and_update(responses, fn [response | rest] -> {response, rest} end)
      {request, response}
    end

    ObjectStore.new(
      bucket: "test-bucket",
      access_key_id: "test-access-key",
      secret_access_key: "test-secret-key",
      s3_endpoint: "http://s3.test",
      default_region: "us-east-1",
      req_opts: [adapter: adapter, retry_delay: 0, retry_log_level: false]
    )
  end
end
