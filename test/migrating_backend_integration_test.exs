defmodule DurableServer.MigratingBackendIntegrationTest do
  use ExUnit.Case, async: false

  alias DurableServer.StorageBackend
  alias DurableServer.StorageBackend.EKV
  alias DurableServer.StorageBackend.Migrating

  @moduletag :integration
  @moduletag :capture_log

  setup do
    unique_id = System.unique_integer([:positive, :monotonic])

    primary_name = :"durable_migrating_primary_#{unique_id}"
    secondary_name = :"durable_migrating_secondary_#{unique_id}"

    primary_dir = Path.join(System.tmp_dir!(), "durable_server_migrating_primary_#{unique_id}")

    secondary_dir =
      Path.join(System.tmp_dir!(), "durable_server_migrating_secondary_#{unique_id}")

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

    primary = StorageBackend.new(EKV, EKV.normalize_opts(name: primary_name))
    secondary = StorageBackend.new(EKV, EKV.normalize_opts(name: secondary_name))
    migrating = migrating_backend(primary, secondary)

    on_exit(fn ->
      File.rm_rf(primary_dir)
      File.rm_rf(secondary_dir)
    end)

    {:ok, primary: primary, secondary: secondary, migrating: migrating}
  end

  test "fallback read promotes to primary and returns primary CAS etag", %{
    primary: primary,
    secondary: secondary,
    migrating: migrating
  } do
    key = "migrating/promotion"
    initial_body = "value-v1"
    updated_body = "value-v2"

    assert {:ok, _obj} = StorageBackend.put_object(secondary, key, initial_body)
    assert {:error, :not_found} = StorageBackend.get_object(primary, key)

    assert {:ok, %{body: ^initial_body, etag: promoted_etag}} =
             StorageBackend.get_object(migrating, key)

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

  test "mirror writes propagate put and delete to secondary", %{
    primary: primary,
    secondary: secondary,
    migrating: migrating
  } do
    key = "migrating/mirror"
    body = "mirror-body"

    assert {:ok, %{body: ^body}} = StorageBackend.put_object(migrating, key, body)
    assert {:ok, %{body: ^body}} = StorageBackend.get_object(primary, key)
    assert {:ok, %{body: ^body}} = StorageBackend.get_object(secondary, key)

    assert :ok = StorageBackend.delete_object(migrating, key)
    assert {:error, :not_found} = StorageBackend.get_object(primary, key)
    assert {:error, :not_found} = StorageBackend.get_object(secondary, key)
  end

  test "secondary cutover reads and writes mirrored state", %{
    primary: primary,
    secondary: secondary,
    migrating: phase1
  } do
    key = "migrating/cutover"

    assert {:ok, %{body: "cutover-v1"}} = StorageBackend.put_object(phase1, key, "cutover-v1")

    phase2 =
      migrating_backend(primary, secondary,
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

  defp migrating_backend(primary, secondary, opts \\ []) do
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
      |> Migrating.normalize_opts()

    StorageBackend.new(Migrating, state)
  end

  defp ekv_mod, do: :"Elixir.EKV"
end
