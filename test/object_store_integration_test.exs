defmodule DurableServer.ObjectStoreIntegrationTest do
  use ExUnit.Case, async: true
  alias DurableServer.ObjectStore

  @moduletag :integration
  @moduletag :capture_log

  @required_env_vars [
    "DURABLE_AWS_ACCESS_KEY_ID",
    "DURABLE_AWS_SECRET_ACCESS_KEY",
    "DURABLE_AWS_ENDPOINT_URL_S3",
    "DURABLE_AWS_REGION",
    "DURABLE_BUCKET"
  ]

  setup do
    # Check if all required environment variables are set
    missing_vars =
      @required_env_vars
      |> Enum.reject(&System.get_env/1)

    case missing_vars do
      [] ->
        # All environment variables are set
        # Use separate IAM endpoint if provided, otherwise use Tigris IAM endpoint
        iam_endpoint =
          System.get_env("DURABLE_AWS_ENDPOINT_URL_IAM") || "https://iam.storage.dev"

        store_config = [
          access_key_id: System.get_env("DURABLE_AWS_ACCESS_KEY_ID"),
          secret_access_key: System.get_env("DURABLE_AWS_SECRET_ACCESS_KEY"),
          s3_endpoint: System.get_env("DURABLE_AWS_ENDPOINT_URL_S3"),
          iam_endpoint: iam_endpoint,
          default_region: System.get_env("DURABLE_AWS_REGION"),
          bucket: System.get_env("DURABLE_BUCKET"),
          req_opts: []
        ]

        {:ok, store_config: store_config}

      missing ->
        raise(
          "Required environment variables: #{inspect(@required_env_vars)}, missing: #{inspect(missing)}"
        )
    end
  end

  describe "IAM policy enforcement" do
    setup %{store_config: store_config} do
      test_id = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
      test_bucket_name = "durable-test-#{:os.system_time(:millisecond)}-#{test_id}"
      control_bucket_name = "durable-test-control-#{:os.system_time(:millisecond)}-#{test_id}"
      test_file_name = "test-file-#{test_id}.txt"
      test_file_content = "Test content #{DateTime.utc_now()}"

      admin_client =
        ObjectStore.new(Keyword.merge(store_config, bucket: control_bucket_name))

      case ObjectStore.create_bucket_with_credentials(admin_client, test_bucket_name) do
        {:ok, %ObjectStore{} = restricted_client} ->
          IO.puts("Successfully created bucket: #{restricted_client.bucket}")
          # Verify the returned bucket name matches what we requested
          assert restricted_client.bucket == test_bucket_name
          # Verify credentials structure is correct
          assert restricted_client.bucket == test_bucket_name
          assert is_binary(restricted_client.access_key_id)
          assert is_binary(restricted_client.secret_access_key)

          %{
            test_bucket_name: test_bucket_name,
            control_bucket_name: control_bucket_name,
            test_file_name: test_file_name,
            test_file_content: test_file_content,
            admin_client: admin_client,
            restricted_client: restricted_client
          }

        error ->
          flunk("Setup failed: #{inspect(error)}")
      end
    end

    test "bucket-scoped credentials can write to assigned bucket", context do
      %{
        test_file_name: test_file_name,
        test_file_content: test_file_content,
        restricted_client: restricted_client
      } = context

      {:ok, %{etag: _}} =
        ObjectStore.put_object(restricted_client, test_file_name, test_file_content)
    end

    test "bucket-scoped credentials can read from assigned bucket", context do
      %{
        test_file_name: test_file_name,
        restricted_client: restricted_client
      } = context

      # first, make sure there's an obj to read
      {:ok, %{etag: etag}} =
        ObjectStore.put_object(restricted_client, test_file_name, "#{test_file_name} content")

      # now read it back
      assert {:ok, %{body: body, etag: ^etag}} =
               ObjectStore.get_object(restricted_client, test_file_name)

      assert body == "#{test_file_name} content"
    end

    test "bucket-scoped credentials cannot write to unauthorized bucket", context do
      %{
        control_bucket_name: control_bucket_name,
        restricted_client: restricted_client
      } = context

      malicious_client = ObjectStore.new(restricted_client, bucket: control_bucket_name)

      assert {:error, _} =
               ObjectStore.put_object(
                 malicious_client,
                 "unauthorized-file.txt",
                 "This should fail"
               )
    end

    test "bucket-scoped credentials cannot read from unauthorized bucket", context do
      %{
        control_bucket_name: control_bucket_name,
        restricted_client: restricted_client
      } = context

      malicious_client = ObjectStore.new(restricted_client, bucket: control_bucket_name)

      IO.puts("Testing IAM enforcement: User trying to access unauthorized bucket")
      IO.puts("  User's bucket: #{malicious_client.bucket}")
      IO.puts("  Target bucket: #{control_bucket_name}")
      IO.puts("  Access key: #{malicious_client.access_key_id}")

      assert {:error, :bad_gateway} = ObjectStore.get_object(malicious_client, "some-file.txt")
    end

    test "direct write to control bucket", %{
      test_file_name: file_name,
      store_config: store_config
    } do
      file_content = "Testing direct access to ObjectStore storage at #{DateTime.utc_now()}"

      # Create client from process config
      client = ObjectStore.new(store_config)

      # Try to upload a file
      assert {:ok, _} = ObjectStore.put_object(client, file_name, file_content)
      assert {:ok, response} = ObjectStore.get_object(client, file_name)

      # Verify content matches
      assert response.body == file_content

      # Clean up - delete the file
      :ok = ObjectStore.delete_object(client, file_name)
    end

    test "admin credentials have broader access than bucket-scoped credentials", context do
      %{
        admin_client: admin_client,
        restricted_client: restricted_client
      } = context

      test_file_name =
        "admin-test-#{:crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)}.txt"

      admin_content = "Admin uploaded content"

      # Admin uploads a file to the restricted bucket

      assert {:ok, _} =
               ObjectStore.put_object(
                 ObjectStore.new(admin_client, bucket: restricted_client.bucket),
                 test_file_name,
                 admin_content
               )

      assert {:ok, result} =
               ObjectStore.get_object(restricted_client, test_file_name)

      # Restricted credentials should be able to read the file in their own bucket
      assert result.body == admin_content

      # Clean up with admin credentials
      :ok =
        ObjectStore.delete_object(
          ObjectStore.new(admin_client, bucket: restricted_client.bucket),
          test_file_name
        )
    end

    test "bucket lifecycle operations work correctly", context do
      %{test_bucket_name: bucket_name, store_config: store_config} = context

      lifecycle_test_id = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
      test_file = "lifecycle-test-#{lifecycle_test_id}.txt"
      test_content = "Lifecycle test content"

      # Create client from process config
      admin_client = ObjectStore.new(store_config)

      # Test the complete lifecycle: create bucket, create credentials, use credentials, list objects
      with {:ok, %ObjectStore{} = restricted_client} <-
             ObjectStore.create_bucket_with_credentials(admin_client, bucket_name) do
        assert restricted_client.bucket == bucket_name

        # Upload a file using the bucket-scoped credentials
        {:ok, _} = ObjectStore.put_object(restricted_client, test_file, test_content)

        # List objects using admin credentials
        {:ok, %{keys: keys}} = ObjectStore.list_objects(admin_client, "")

        # Should contain our test file
        assert Enum.any?(keys, fn %{key: key} -> key == test_file end)

        # Read the file back using bucket-scoped credentials
        {:ok, read_response} = ObjectStore.get_object(restricted_client, test_file)
        assert read_response.body == test_content

        # Clean up the file using admin credentials
        :ok = ObjectStore.delete_object(admin_client, test_file)
      end
    end

    test "cross-user IAM enforcement: User A cannot access User B's bucket", %{
      store_config: store_config
    } do
      admin_client = ObjectStore.new(store_config)

      # Generate unique test identifiers
      test_id = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
      user_a_bucket = "durable-test-user-a-bucket-#{test_id}"
      user_b_bucket = "durable-test-user-b-bucket-#{test_id}"

      IO.puts("Testing cross-user IAM enforcement:")
      IO.puts("  User A bucket: #{user_a_bucket}")
      IO.puts("  User B bucket: #{user_b_bucket}")

      # Create User A's bucket and credentials
      IO.puts("Creating User A's bucket and credentials...")

      {:ok, %ObjectStore{} = client_a} =
        ObjectStore.create_bucket_with_credentials(admin_client, user_a_bucket)

      assert client_a.bucket !== admin_client.bucket
      assert client_a.access_key_id !== admin_client.access_key_id

      IO.puts("  User A access key: #{client_a.access_key_id}")

      # Small delay to avoid overwhelming API
      Process.sleep(1000)

      # Create User B's bucket and credentials
      IO.puts("Creating User B's bucket and credentials...")

      {:ok, %ObjectStore{} = client_b} =
        ObjectStore.create_bucket_with_credentials(admin_client, user_b_bucket)

      IO.puts("  User B access key: #{client_b.access_key_id}")

      # Create test content in each user's bucket
      test_content_a = "User A's private content"
      test_content_b = "User B's private content"
      test_file = "test-file.txt"

      # User A writes to their own bucket
      {:ok, _} = ObjectStore.put_object(client_a, test_file, test_content_a)
      {:ok, %{body: ^test_content_a}} = ObjectStore.get_object(client_a, test_file)

      # User B writes to their own bucket
      {:ok, _} = ObjectStore.put_object(client_b, test_file, test_content_b)
      {:ok, %{body: ^test_content_b}} = ObjectStore.get_object(client_b, test_file)

      IO.puts("Both users successfully wrote to their own buckets")

      # Test 1: User A tries to read from User B's bucket (should fail)
      IO.puts("Test 1: User A trying to read User B's bucket...")

      malicious_a = ObjectStore.new(client_a, bucket: user_b_bucket)
      assert malicious_a.bucket == user_b_bucket
      user_a_tries_b_result = ObjectStore.get_object(malicious_a, test_file)

      case user_a_tries_b_result do
        {:error, reason} ->
          IO.puts(
            "IAM enforcement working: User A got error #{inspect(reason)} when trying to access User B's bucket"
          )

        other ->
          flunk(
            "IAM enforcement FAILED: User A successfully read User B's bucket: #{inspect(other)}"
          )
      end

      # Test 2: User B tries to read from User A's bucket (should fail)
      IO.puts("Test 2: User B trying to read User A's bucket...")

      malicious_b = ObjectStore.new(client_b, bucket: user_a_bucket)
      user_b_tries_a_result = ObjectStore.get_object(malicious_b, test_file)

      case user_b_tries_a_result do
        {:error, reason} ->
          IO.puts(
            "IAM enforcement working: User B got error #{inspect(reason)} when trying to access User A's bucket"
          )

        other ->
          flunk(
            "IAM enforcement FAILED: User B successfully read User A's bucket: #{inspect(other)}"
          )
      end

      IO.puts("Cross-user IAM enforcement test completed successfully!")
    end
  end

  describe "ObjectStore operations" do
    setup %{store_config: store_config} do
      test_id = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
      test_bucket_name = "durable-test-#{:os.system_time(:millisecond)}-#{test_id}"
      admin_client = ObjectStore.new(store_config)

      # Create bucket for testing
      case ObjectStore.create_bucket_with_credentials(admin_client, test_bucket_name) do
        {:ok, %ObjectStore{} = cred_client} ->
          IO.puts("Successfully created bucket: #{cred_client.bucket}")

          %{
            test_bucket_name: test_bucket_name,
            admin_client: admin_client,
            cred_client: cred_client
          }

        error ->
          flunk("Setup failed: #{inspect(error)}")
      end
    end

    test "put_object operation works correctly", %{
      admin_client: client,
      test_bucket_name: _bucket
    } do
      test_key =
        "test-objects/put-test-#{:crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)}.txt"

      test_content = "Test content for put_object #{DateTime.utc_now()}"

      result = ObjectStore.put_object(client, test_key, test_content)

      case result do
        {:ok, %{etag: etag}} ->
          assert is_binary(etag)
          IO.puts("put_object succeeded with etag: #{etag}")

        {:error, reason} ->
          flunk("put_object failed: #{inspect(reason)}")
      end
    end

    test "get_object operation works correctly", %{admin_client: client} do
      test_key =
        "test-objects/get-test-#{:crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)}.txt"

      test_content = "Test content for get_object #{DateTime.utc_now()}"

      # First put an object
      {:ok, _} = ObjectStore.put_object(client, test_key, test_content)

      # Then get it back
      result = ObjectStore.get_object(client, test_key)

      case result do
        {:ok, %{body: body, etag: etag}} ->
          assert body == test_content
          assert is_binary(etag)
          IO.puts("get_object succeeded, content matches")

        {:error, reason} ->
          flunk("get_object failed: #{inspect(reason)}")
      end
    end

    test "try_claim operation works correctly", %{admin_client: client} do
      test_key =
        "test-objects/claim-test-#{:crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)}.txt"

      test_content = "Test content for try_claim #{DateTime.utc_now()}"

      # First claim should succeed
      result1 = ObjectStore.try_claim(client, test_key, test_content)

      case result1 do
        {:ok, {:claimed, _etag}} ->
          IO.puts("First try_claim succeeded")

        {:error, reason} ->
          flunk("First try_claim failed: #{inspect(reason)}")
      end

      # Second claim of same key should fail
      result2 = ObjectStore.try_claim(client, test_key, test_content)

      case result2 do
        {:error, :already_claimed} ->
          IO.puts("Second try_claim correctly failed with :already_claimed")

        {:ok, {:claimed, _etag}} ->
          flunk("Second try_claim should have failed but succeeded")

        {:error, reason} ->
          flunk("Second try_claim failed with unexpected reason: #{inspect(reason)}")
      end
    end

    test "list_objects operation works correctly", %{admin_client: client} do
      # Create a few test objects
      test_prefix = "list-test-#{:crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)}"

      for i <- 1..3 do
        key = "#{test_prefix}/file-#{i}.txt"
        content = "Content for file #{i}"
        {:ok, _} = ObjectStore.put_object(client, key, content)
      end

      # List objects with the test prefix
      result = ObjectStore.list_objects(client, test_prefix)

      case result do
        {:ok, %{keys: keys}} ->
          assert length(keys) == 3
          IO.puts("list_objects returned #{length(keys)} objects")

        {:error, reason} ->
          flunk("list_objects failed: #{inspect(reason)}")
      end
    end
  end
end
