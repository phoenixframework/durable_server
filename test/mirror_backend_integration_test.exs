defmodule DurableServer.MirrorBackendIntegrationTest do
  use ExUnit.Case, async: false

  alias DurableServer.StorageBackend
  alias DurableServer.Backends.EKVStore
  alias DurableServer.Backends.MirrorStore

  defmodule RejectingPutBackend do
    @behaviour DurableServer.StorageBackend

    alias DurableServer.StorageBackend

    @impl true
    def init_backend(opts) when is_list(opts) do
      delegate = Keyword.fetch!(opts, :delegate)
      {:ok, %{state: %{delegate: delegate}}}
    end

    @impl true
    def ensure_ready(%{delegate: delegate}), do: StorageBackend.ensure_ready(delegate)

    @impl true
    def get_object(%{delegate: delegate}, key, opts),
      do: StorageBackend.get_object(delegate, key, opts)

    @impl true
    def list_all_objects_stream(%{delegate: delegate}, prefix, opts),
      do: StorageBackend.list_all_objects_stream(delegate, prefix, opts)

    @impl true
    def put_object(_state, _key, _data, _opts), do: {:error, :promotion_write_rejected}

    @impl true
    def delete_object(%{delegate: delegate}, key), do: StorageBackend.delete_object(delegate, key)

    @impl true
    def try_claim(%{delegate: delegate}, key, body),
      do: StorageBackend.try_claim(delegate, key, body)

    @impl true
    def update_object(%{delegate: delegate}, key, update_fn, opts),
      do: StorageBackend.update_object(delegate, key, update_fn, opts)

    @impl true
    def encode(%{delegate: delegate}, data), do: StorageBackend.encode(delegate, data)

    @impl true
    def decode(%{delegate: delegate}, data), do: StorageBackend.decode(delegate, data)

    @impl true
    def subscribe(%{delegate: delegate}, subscriber, prefix, opts),
      do: StorageBackend.subscribe(delegate, subscriber, prefix, opts)

    @impl true
    def unsubscribe(%{delegate: delegate}, subscription_ref),
      do: StorageBackend.unsubscribe(delegate, subscription_ref)
  end

  @moduletag :integration
  @moduletag :capture_log

  setup do
    unique_id = System.unique_integer([:positive, :monotonic])

    primary_name = :"durable_mirror_primary_#{unique_id}"
    secondary_name = :"durable_mirror_secondary_#{unique_id}"

    primary_dir = Path.join(System.tmp_dir!(), "durable_server_mirror_primary_#{unique_id}")

    secondary_dir =
      Path.join(System.tmp_dir!(), "durable_server_mirror_secondary_#{unique_id}")

    File.rm_rf(primary_dir)
    File.rm_rf(secondary_dir)

    start_supervised!(
      {ekv_mod(),
       [
         name: primary_name,
         data_dir: primary_dir,
         cluster_size: 1,
         node_id: 1,
         log: false
       ]}
    )

    start_supervised!(
      {ekv_mod(),
       [
         name: secondary_name,
         data_dir: secondary_dir,
         cluster_size: 1,
         node_id: 1,
         log: false
       ]}
    )

    primary = StorageBackend.new(EKVStore, EKVStore.normalize_opts(name: primary_name))
    secondary = StorageBackend.new(EKVStore, EKVStore.normalize_opts(name: secondary_name))
    mirror = mirror_backend(primary, secondary)

    on_exit(fn ->
      File.rm_rf(primary_dir)
      File.rm_rf(secondary_dir)
    end)

    {:ok, primary: primary, secondary: secondary, mirror: mirror}
  end

  test "fallback read promotes to primary and returns primary CAS etag", %{
    primary: primary,
    secondary: secondary,
    mirror: mirror
  } do
    key = "mirror/promotion"
    initial_body = "value-v1"
    updated_body = "value-v2"

    assert {:ok, _obj} = StorageBackend.put_object(secondary, key, initial_body)
    assert {:error, :not_found} = StorageBackend.get_object(primary, key)

    assert {:ok, %{body: ^initial_body, etag: promoted_etag}} =
             StorageBackend.get_object(mirror, key)

    assert {:ok, %{body: ^initial_body, etag: primary_etag}} =
             StorageBackend.get_object(primary, key)

    assert primary_etag == promoted_etag

    # Returned etag must be from primary so subsequent CAS writes succeed.
    assert {:ok, %{body: ^updated_body}} =
             StorageBackend.put_object(primary, key, updated_body,
               etag: promoted_etag,
               max_retries: 0
             )
  end

  test "fallback read fails when promotion cannot complete", %{
    primary: primary,
    secondary: secondary
  } do
    key = "mirror/promotion_failure"
    body = "value-v1"

    assert {:ok, _obj} = StorageBackend.put_object(secondary, key, body)
    assert {:error, :not_found} = StorageBackend.get_object(primary, key)

    rejecting_primary =
      StorageBackend.new(RejectingPutBackend, %{delegate: primary})

    read_only_mirror =
      mirror_backend(rejecting_primary, secondary,
        read_preference: :primary,
        write_target: :secondary,
        fallback_reads: true,
        promote_on_fallback: true,
        mirror_writes: false
      )

    assert {:error, {:promotion_failed, :promotion_write_rejected}} =
             StorageBackend.get_object(read_only_mirror, key)
  end

  test "mirror writes propagate put and delete to secondary", %{
    primary: primary,
    secondary: secondary,
    mirror: mirror
  } do
    key = "mirror/mirror"
    body = "mirror-body"

    assert {:ok, %{body: ^body}} = StorageBackend.put_object(mirror, key, body)
    assert {:ok, %{body: ^body}} = StorageBackend.get_object(primary, key)
    assert {:ok, %{body: ^body}} = StorageBackend.get_object(secondary, key)

    assert :ok = StorageBackend.delete_object(mirror, key)
    assert {:error, :not_found} = StorageBackend.get_object(primary, key)
    assert {:error, :not_found} = StorageBackend.get_object(secondary, key)
  end

  test "secondary cutover reads and writes mirrored state", %{
    primary: primary,
    secondary: secondary,
    mirror: phase1
  } do
    key = "mirror/cutover"

    assert {:ok, %{body: "cutover-v1"}} = StorageBackend.put_object(phase1, key, "cutover-v1")

    phase2 =
      mirror_backend(primary, secondary,
        read_preference: :secondary,
        write_target: :secondary,
        fallback_reads: false,
        mirror_writes: false
      )

    assert {:ok, %{body: "cutover-v1", etag: etag}} = StorageBackend.get_object(phase2, key)

    assert {:ok, %{body: "cutover-v2"}} =
             StorageBackend.put_object(phase2, key, "cutover-v2", etag: etag, max_retries: 0)

    assert {:ok, %{body: "cutover-v1"}} = StorageBackend.get_object(primary, key)
    assert {:ok, %{body: "cutover-v2"}} = StorageBackend.get_object(secondary, key)
  end

  defp mirror_backend(primary, secondary, opts \\ []) do
    defaults = [
      primary: primary,
      secondary: secondary,
      read_preference: :primary,
      write_target: :primary,
      fallback_reads: true,
      promote_on_fallback: true,
      mirror_writes: true,
      mirror_mode: :required,
      secondary_required: true
    ]

    state =
      defaults
      |> Keyword.merge(opts)
      |> MirrorStore.normalize_opts()

    StorageBackend.new(MirrorStore, state)
  end

  defp ekv_mod, do: :"Elixir.EKV"
end
