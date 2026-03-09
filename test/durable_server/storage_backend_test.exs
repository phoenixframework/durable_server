defmodule DurableServer.StorageBackendTest do
  use ExUnit.Case, async: true

  alias DurableServer.StorageBackend

  defmodule DynamicInitBackend do
    @behaviour StorageBackend

    @impl true
    def init_backend(raw), do: raw

    @impl true
    def ensure_ready(_state), do: :ok

    @impl true
    def get_object(_state, _key, _opts), do: {:error, :not_found}

    @impl true
    def list_all_objects_stream(_state, _prefix, _opts), do: []

    @impl true
    def put_object(_state, _key, _data, _opts), do: {:error, :unsupported}

    @impl true
    def delete_object(_state, _key), do: {:error, :unsupported}

    @impl true
    def try_claim(_state, _key, _body), do: {:error, :unsupported}

    @impl true
    def update_object(_state, _key, _update_fn, _opts), do: {:error, :unsupported}

    @impl true
    def encode(_state, data), do: {:ok, data}

    @impl true
    def decode(_state, data), do: {:ok, data}
  end

  test "accepts valid init result with defaults and features" do
    raw =
      {:ok,
       %{
         state: %{adapter_data: 123},
         defaults: %{
           heartbeat_tracking_mode: :subscribe,
           discovery_interval_ms: 5_000,
           heartbeat_interval_ms: 5_000,
           heartbeat_reconcile_interval_ms: 30_000
         },
         features: %{heartbeat_subscribe?: true}
       }}

    assert {:ok, %StorageBackend{} = backend} =
             StorageBackend.init_backend(DynamicInitBackend, raw)

    assert backend.state == %{adapter_data: 123}
    assert backend.defaults.heartbeat_tracking_mode == :subscribe
    assert backend.features.heartbeat_subscribe? == true
  end

  test "raises when :state is missing" do
    raw = {:ok, %{defaults: %{heartbeat_tracking_mode: :poll}}}

    assert_raise ArgumentError, ~r/missing required key :state/, fn ->
      StorageBackend.init_backend(DynamicInitBackend, raw)
    end
  end

  test "rejects unknown default keys" do
    raw = {:ok, %{state: :ok, defaults: %{typo_interval: 1_000}}}

    assert_raise ArgumentError, ~r/unknown keys: \[:typo_interval\]/, fn ->
      StorageBackend.init_backend(DynamicInitBackend, raw)
    end
  end

  test "rejects invalid default values and invalid features" do
    raw_defaults =
      {:ok,
       %{
         state: :ok,
         defaults: %{heartbeat_tracking_mode: :bad_mode}
       }}

    assert_raise ArgumentError, ~r/heartbeat_tracking_mode must be :poll or :subscribe/, fn ->
      StorageBackend.init_backend(DynamicInitBackend, raw_defaults)
    end

    raw_features =
      {:ok,
       %{
         state: :ok,
         features: %{heartbeat_subscribe?: "yes"}
       }}

    assert_raise ArgumentError, ~r/feature :heartbeat_subscribe\? must be a boolean/, fn ->
      StorageBackend.init_backend(DynamicInitBackend, raw_features)
    end
  end

  test "rejects non tuple callback return value" do
    assert_raise ArgumentError, ~r/must return \{:ok, map\} or \{:error, reason\}/, fn ->
      StorageBackend.init_backend(DynamicInitBackend, %{state: :ok})
    end
  end
end
