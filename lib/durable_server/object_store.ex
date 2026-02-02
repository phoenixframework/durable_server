defmodule DurableServer.ObjectStore do
  @moduledoc """
  Manages Tigris bucket operations for Fly apps.

  Focused solely on bucket management and credential generation.
  For object operations, use Req + ReqS3 functionality directly.

  ## Consistency

  All operations are consistent by default. Consistent operations send then
  `x-tigris-consistent true` header. This guarantees consistency with reads and writes,
  regardless of the client region. Clients can opt into local region request
  with `consistent: false` on a case by case basis, to favor speed and reduced latency
  at the cost of consistency on a base-by-base basis.
  """

  @default_timeout 30_000

  @derive {Inspect, only: []}
  defstruct access_key_id: nil,
            secret_access_key: nil,
            region: nil,
            default_region: nil,
            s3_endpoint: nil,
            iam_endpoint: nil,
            bucket: nil,
            req_opts: nil,
            s3: nil,
            json_codec: nil,
            xml_codec: nil,
            iam: nil,
            headers: [],
            finch: nil,
            task_supervisor: nil

  require Logger
  require ReqS3

  # Import SweetXml for XML parsing and sigils
  import SweetXml

  alias Req

  # Create ObjectStore client with default configuration
  @valid_new_opts [
    :headers,
    :bucket,
    :region,
    :default_region,
    :req_opts,
    :s3_endpoint,
    :iam_endpoint,
    :access_key_id,
    :secret_access_key,
    :finch,
    :task_supervisor
  ]

  def new(%__MODULE__{} = client, opts) when is_list(opts) do
    opts = Keyword.validate!(opts, @valid_new_opts)

    merged_opts =
      Enum.map(@valid_new_opts, fn key ->
        case Keyword.fetch(opts, key) do
          {:ok, value} -> {key, value}
          :error -> {key, Map.get(client, key)}
        end
      end)

    Map.merge(client, new(merged_opts))
  end

  @required_opts [:bucket, :access_key_id, :secret_access_key, :s3_endpoint, :default_region]

  def new(opts) when is_list(opts) do
    opts = Keyword.validate!(opts, @valid_new_opts)

    Enum.each(@required_opts, fn key ->
      unless Keyword.has_key?(opts, key) do
        raise ArgumentError, "DurableServer.ObjectStore.new/1 requires #{inspect(key)}"
      end
    end)

    headers = Keyword.get(opts, :headers, [])
    region = opts[:region] || "auto"

    default_region =
      case opts[:default_region] do
        default when default in [nil, "auto"] ->
          raise ArgumentError,
                "#{inspect(__MODULE__)} :default_region must be set and cannot be \"auto\""

        default when is_binary(default) ->
          default
      end

    %__MODULE__{
      access_key_id: Keyword.fetch!(opts, :access_key_id),
      secret_access_key: Keyword.fetch!(opts, :secret_access_key),
      region: region,
      default_region: default_region,
      s3_endpoint: Keyword.fetch!(opts, :s3_endpoint),
      iam_endpoint: opts[:iam_endpoint],
      bucket: Keyword.fetch!(opts, :bucket),
      req_opts: opts[:req_opts] || [],
      json_codec: JSON,
      xml_codec: SweetXml,
      headers: headers,
      finch: opts[:finch] || DurableServer.Finch,
      task_supervisor: opts[:task_supervisor] || DurableServer.TaskSupervisor
    }
  end

  def ensure_bucket_exists(%__MODULE__{} = client) do
    case create_bucket(client, client.bucket) do
      {:error, %{status: 409}} -> :ok
      {:ok, %__MODULE__{}} -> :ok
    end
  end

  @doc """
  Creates a bucket for a Fly app.
  Returns {:ok, bucket_info} on success, or {:error, reason} if creation fails.

  Note: If the bucket already exists, this function will return an error,
  which can be handled by the caller.
  """
  def create_bucket(%__MODULE__{} = client, bucket_name, opts \\ []) do
    # Setup req with ReqS3 using client credentials
    req = new_req(client, headers: opts[:headers] || [], consistent: true)

    # Create bucket
    case Req.request(req,
           method: :put,
           url: "s3://#{bucket_name}",
           params: %{
             "location-constraint" => "us-east-1"
           },
           retry: :transient
         ) do
      {:ok, %{status: status}} when status >= 200 and status < 300 ->
        {:ok, new(client, bucket: bucket_name)}

      {:error, reason} ->
        {:error, reason}

      {:ok, response} ->
        {:error, response}
    end
  end

  @doc """
  Lists all buckets in the account.
  Returns {:ok, buckets} on success, or {:error, reason} if listing fails.
  """
  def list_buckets(%__MODULE__{} = client) do
    req = new_req(client)

    # List buckets
    case Req.request(req, method: :get, url: "s3://") do
      {:ok, %{status: status, body: body}} when status >= 200 and status < 300 ->
        buckets = parse_list_buckets_response(body)
        {:ok, buckets}

      {:error, reason} ->
        {:error, reason}

      {:ok, response} ->
        {:error, response}
    end
  end

  @doc """
  Deletes a bucket. This will fail if the bucket is not empty.
  Returns :ok on success, or {:error, reason} if deletion fails.
  """
  def delete_bucket(%__MODULE__{} = client, bucket_name) do
    req = new_req(client, consistent: true, headers: [{"tigris-force-delete", "true"}])

    # Delete bucket
    case Req.request(req,
           method: :delete,
           url: "s3://#{bucket_name}"
         ) do
      {:ok, %{status: status}} when status >= 200 and status < 300 ->
        :ok

      {:error, reason} ->
        {:error, reason}

      {:ok, response} ->
        {:error, response}
    end
  end

  @doc """
  Creates a bucket for a Fly app and generates credentials for it in a single operation.
  This is a convenience function that combines create_bucket/1 and generate_bucket_credentials/1.

  Returns:
  - {:ok, %ObjectStore{} = scoped_obj_store} on success
  - {:error, {:bucket_creation_failed, reason}} if bucket creation fails
  - {:error, {:credential_generation_failed, reason}} if credential generation fails
  """
  def create_bucket_with_credentials(%__MODULE__{} = client, bucket_name) do
    # Step 1: Create the bucket
    case create_bucket(client, bucket_name) do
      {:ok, %{bucket: bucket}} ->
        # Step 2: Generate credentials for the bucket
        case generate_bucket_credentials(client, bucket) do
          {:ok, credentials} ->
            %{access_key_id: access_key_id, secret_access_key: secret_access_key} = credentials
            # Return both the bucket name and the credentials
            {:ok,
             new(client,
               bucket: bucket,
               access_key_id: access_key_id,
               secret_access_key: secret_access_key
             )}

          {:error, reason} ->
            # If credential generation fails, we should still return the bucket info
            # but indicate that credentials failed
            {:error, {:credential_generation_failed, reason}}
        end

      {:error, reason} ->
        {:error, {:bucket_creation_failed, reason}}
    end
  end

  @doc """
  Generates bucket-scoped credentials for a Fly app to access its own bucket.

  This implementation uses Tigris IAM API to:
  1. Create an access key using CreateAccessKey
  2. Create a policy using CreatePolicy to restrict access to the specified bucket
  3. Attach the policy to the access key using AttachUserPolicy

  Returns {:ok, credentials} on success, or {:error, reason} if creation fails.
  """
  def generate_bucket_credentials(%__MODULE__{} = client, bucket_name) do
    # Create a unique random ID for the access key name
    random_id = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    user_name = "tid_#{random_id}"

    # Check if we need to create a user first
    # Skip user creation only for Tigris (storage.dev)
    needs_user_creation =
      client.iam_endpoint &&
        !String.contains?(client.iam_endpoint, "storage.dev") &&
        !String.contains?(client.iam_endpoint, "tigris")

    if needs_user_creation do
      Logger.info("Creating IAM user for LocalStack: #{user_name}")

      case create_iam_user(client, user_name) do
        :ok ->
          Logger.info("Successfully created IAM user: #{user_name}")

        {:error, reason} ->
          Logger.warning("Failed to create IAM user (may already exist): #{inspect(reason)}")
      end
    end

    # 1. Create an access key using CreateAccessKey operation
    Logger.info("Creating access key for bucket: #{bucket_name}")
    access_key_result = create_access_key(client, user_name)

    case access_key_result do
      {:ok,
       %{access_key_id: access_key_id, secret_access_key: secret_access_key, user_name: user_name}} ->
        # 2. Create a bucket-specific policy
        policy_name = "bucket-policy-#{bucket_name}-#{random_id}"
        policy_document = create_bucket_policy_document(bucket_name)

        case create_policy(client, policy_name, policy_document) do
          {:ok, %{policy_arn: policy_arn}} ->
            # 3. Attach the policy to the user
            # For Tigris (no user creation), use access_key_id as the username
            attach_user_name = if needs_user_creation, do: user_name, else: access_key_id

            case attach_user_policy(client, attach_user_name, policy_arn) do
              :ok ->
                # Return the credentials if everything succeeded
                {:ok,
                 %{
                   access_key_id: access_key_id,
                   secret_access_key: secret_access_key,
                   bucket: bucket_name
                 }}

              {:error, reason} ->
                # Clean up the access key if policy attachment failed
                _ = delete_access_key(client, access_key_id)
                # Clean up the policy
                _ = delete_policy(client, policy_arn)
                {:error, {:attach_policy_failed, reason}}
            end

          {:error, reason} ->
            # Clean up the access key if policy creation failed
            _ = delete_access_key(client, access_key_id)
            {:error, {:create_policy_failed, reason}}
        end

      {:error, reason} ->
        {:error, {:create_access_key_failed, reason}}
    end
  end

  # Creates a bucket policy JSON document with proper S3 actions
  defp create_bucket_policy_document(bucket_name) do
    policy = %{
      "Version" => "2012-10-17",
      "Statement" => [
        %{
          "Sid" => "ListObjectsInBucket",
          "Effect" => "Allow",
          "Action" => ["s3:ListBucket"],
          "Resource" => ["arn:aws:s3:::#{bucket_name}"]
        },
        %{
          "Sid" => "ManageAllObjectsInBucketWildcard",
          "Effect" => "Allow",
          "Action" => ["s3:*"],
          "Resource" => ["arn:aws:s3:::#{bucket_name}/*"]
        }
      ]
    }

    JSON.encode!(policy)
  end

  # Make signed IAM request using Req with built-in SigV4 authentication
  defp iam_request(client, params) do
    body = URI.encode_query(params)

    case Req.request(
           Keyword.merge(client.req_opts,
             method: :post,
             url: client.iam_endpoint,
             body: body,
             headers: [{"content-type", "application/x-www-form-urlencoded"}],
             retry: :transient,
             aws_sigv4: [
               access_key_id: client.access_key_id,
               secret_access_key: client.secret_access_key,
               service: :iam,
               region: client.region
             ]
           )
         ) do
      {:ok, %{status: status} = response} when status in 200..299 ->
        {:ok, response}

      {:ok, %{status: status, body: body}} ->
        {:error, {:http_error, status, body}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Creates an IAM user (for LocalStack only)
  defp create_iam_user(client, user_name) do
    Logger.info("Creating IAM user: #{user_name}")

    params = %{
      "Action" => "CreateUser",
      "Version" => "2010-05-08",
      "UserName" => user_name
    }

    case iam_request(client, params) do
      {:ok, %{status: status}} when status in 200..299 ->
        :ok

      {:error, {:http_error, _status, body}} ->
        # Check if error is about user already existing
        if String.contains?(body, "EntityAlreadyExists") do
          Logger.info("IAM user already exists: #{user_name}")
          :ok
        else
          {:error, {:create_user_failed, body}}
        end

      {:error, reason} ->
        {:error, {:create_user_failed, reason}}
    end
  end

  # Creates an access key using the CreateAccessKey API operation via Req
  defp create_access_key(client, user_name) do
    Logger.info("Creating access key for user: #{user_name}")

    params = %{
      "Action" => "CreateAccessKey",
      "Version" => "2010-05-08",
      "UserName" => user_name
    }

    case iam_request(client, params) do
      {:ok, %{body: xml_body} = result} ->
        try do
          # Parse the XML response manually
          Logger.debug(fn -> "Received CreateAccessKey response: #{inspect(xml_body)}" end)

          # Extract the access key ID and secret from the XML
          access_key_id = xml_body |> xpath(~x"//AccessKeyId/text()"s)
          secret_access_key = xml_body |> xpath(~x"//SecretAccessKey/text()"s)

          if access_key_id != "" and secret_access_key != "" do
            Logger.info("Successfully created access key with ID: #{access_key_id}")

            {:ok,
             %{
               access_key_id: access_key_id,
               secret_access_key: secret_access_key,
               user_name: user_name
             }}
          else
            {:error, {:invalid_response, "Missing access key information in response"}}
          end
        rescue
          error ->
            Logger.error(
              "Failed to parse CreateAccessKey XML response: #{inspect(error)} #{inspect(result)}"
            )

            {:error, {:xml_parse_error, error}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Creates an IAM policy using the CreatePolicy API operation via Req
  defp create_policy(client, policy_name, policy_document) do
    Logger.info("Creating IAM policy: #{policy_name}")

    params = %{
      "Action" => "CreatePolicy",
      "Version" => "2010-05-08",
      "PolicyName" => policy_name,
      "PolicyDocument" => policy_document
    }

    case iam_request(client, params) do
      {:ok, %{body: xml_body}} when is_binary(xml_body) and xml_body != "" ->
        # Parse the XML response manually
        Logger.debug(
          "Received CreatePolicy response: #{inspect(String.slice(xml_body, 0, 100))}..."
        )

        try do
          # Extract the policy ARN from the XML
          policy_arn = xml_body |> xpath(~x"//Arn/text()"s)

          if policy_arn != "" do
            Logger.info("Successfully created policy with ARN: #{policy_arn}")
            {:ok, %{policy_arn: policy_arn}}
          else
            {:error, {:invalid_response, "Missing policy ARN in response"}}
          end
        catch
          kind, reason ->
            Logger.error(
              "Failed to parse CreatePolicy XML response: #{inspect(kind: kind, reason: reason, body: xml_body)}"
            )

            {:error, {:xml_parse_error, {kind, reason}}}
        end

      {:ok, %{body: xml_body}} ->
        Logger.error("CreatePolicy returned empty or invalid body: #{inspect(xml_body)}")
        {:error, {:invalid_response, "Empty or invalid response body"}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Attaches a policy to a user (access key) using the AttachUserPolicy API operation via Req
  defp attach_user_policy(client, user_name, policy_arn) do
    Logger.info("Attaching policy #{policy_arn} to user: #{user_name}")

    params = %{
      "Action" => "AttachUserPolicy",
      "Version" => "2010-05-08",
      "UserName" => user_name,
      "PolicyArn" => policy_arn
    }

    case iam_request(client, params) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # Deletes an access key using the DeleteAccessKey API operation
  defp delete_access_key(client, access_key_id) do
    Logger.info("Deleting access key: #{access_key_id}")

    params = %{
      "Action" => "DeleteAccessKey",
      "Version" => "2010-05-08",
      "AccessKeyId" => access_key_id,
      # In Tigris, the access key ID is used as the user name
      "UserName" => access_key_id
    }

    case iam_request(client, params) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # Deletes a policy using the DeletePolicy API operation
  defp delete_policy(client, policy_arn) do
    Logger.info("Deleting policy: #{policy_arn}")

    # First detach the policy from the user
    detach_result = detach_user_policy(client, policy_arn)

    # Then delete the policy
    params = %{
      "Action" => "DeletePolicy",
      "Version" => "2010-05-08",
      "PolicyArn" => policy_arn
    }

    case iam_request(client, params) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, {:delete_policy_failed, reason, detach_result}}
    end
  end

  # Detaches a policy from all users that it's attached to
  defp detach_user_policy(_config, policy_arn) do
    Logger.info("Detaching policy: #{policy_arn} from users")

    # We would need to list all users the policy is attached to
    # For simplicity, we'll just return success for now
    # In a real implementation, you would:
    # 1. Use ListEntitiesForPolicy to get all attached users
    # 2. Call DetachUserPolicy for each user
    :ok
  end

  @doc """
  Lists IAM policies that match a given bucket name pattern.

  Uses the ListPolicies API operation to find policies related to a bucket.

  ## Examples

      iex> ObjectStore.list_bucket_policies("my-bucket")
      {:ok, [
        %{arn: "arn:aws:iam::123456789012:policy/bucket-policy-my-bucket-abc123", name: "bucket-policy-my-bucket-abc123"}
      ]}
  """
  def list_bucket_policies(%__MODULE__{} = client, bucket_name) do
    Logger.info("Listing IAM policies for bucket: #{bucket_name}")

    policy_prefix = "bucket-policy-#{bucket_name}"

    params = %{
      "Action" => "ListPolicies",
      "Version" => "2010-05-08",
      "PathPrefix" => "/"
    }

    case iam_request(client, params) do
      {:ok, %{body: xml_body}} ->
        policies =
          xml_body
          |> xpath(~x"//Policies/member"l)
          |> Enum.map(fn policy ->
            %{
              arn: xpath(policy, ~x"./Arn/text()"s),
              name: xpath(policy, ~x"./PolicyName/text()"s)
            }
          end)
          |> Enum.filter(fn policy ->
            String.contains?(policy.name, policy_prefix)
          end)

        {:ok, policies}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Gets detailed information about a specific IAM policy including the policy document.

  Uses the GetPolicy and GetPolicyVersion API operations to retrieve full policy details.

  ## Examples

      iex> get_policy_details("arn:aws:iam::123456789012:policy/bucket-policy-my-bucket-abc123")
      {:ok, %{
        arn: "arn:aws:iam::123456789012:policy/bucket-policy-my-bucket-abc123",
        name: "bucket-policy-my-bucket-abc123",
        document: "{\"Version\":\"2012-10-17\",\"Statement\":[...]}"
      }}
  """
  def get_policy_details(%__MODULE__{} = client, policy_arn) do
    Logger.info("Getting policy details for: #{policy_arn}")

    get_policy_params = %{
      "Action" => "GetPolicy",
      "Version" => "2010-05-08",
      "PolicyArn" => policy_arn
    }

    with {:ok, %{body: policy_xml}} <- iam_request(client, get_policy_params),
         policy_name = policy_xml |> xpath(~x"//PolicyName/text()"s),
         default_version = policy_xml |> xpath(~x"//DefaultVersionId/text()"s),
         get_version_params = %{
           "Action" => "GetPolicyVersion",
           "Version" => "2010-05-08",
           "PolicyArn" => policy_arn,
           "VersionId" => default_version
         },
         {:ok, %{body: version_xml}} <- iam_request(client, get_version_params) do
      encoded_document = version_xml |> xpath(~x"//Document/text()"s)
      document = URI.decode(encoded_document)

      {:ok,
       %{
         arn: policy_arn,
         name: policy_name,
         document: document
       }}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, %{errors: ["Failed to get complete policy details"]}}
    end
  end

  def new_req(%__MODULE__{} = client, opts \\ []) when is_list(opts) do
    opts = Keyword.validate!(opts, [:headers, :consistent])
    initial_caller_headers = Keyword.get(opts, :headers, [])
    # pop `:consistent` from being merged into req_opts
    {consistent, opts} = Keyword.pop(opts, :consistent, true)

    {computed_headers, caller_headers, computed_region} =
      if consistent do
        computed =
          [
            {"x-tigris-consistent", "true"}
          ]

        new_caller_headers =
          Enum.filter(initial_caller_headers, fn {key, _val} ->
            String.downcase(key) not in ["x-tigris-consistent"]
          end)

        {computed, new_caller_headers, client.default_region}
      else
        {[], initial_caller_headers, client.region}
      end

    req_opts =
      opts
      |> Keyword.merge(client.req_opts)
      |> Keyword.drop([:headers])

    config =
      %{
        access_key_id: client.access_key_id,
        secret_access_key: client.secret_access_key,
        region: computed_region
      }

    req =
      Req.new(req_opts)
      |> ReqS3.attach(
        aws_endpoint_url_s3: client.s3_endpoint,
        aws_sigv4: config
      )

    req = Req.merge(req, headers: client.headers ++ caller_headers ++ computed_headers)

    req_opts = [finch: client.finch, receive_timeout: @default_timeout]

    Req.merge(req, Keyword.merge(req_opts, base_url: "s3://#{client.bucket}"))
  end

  @doc """
  Attempts to claim a key in a bucket using a CAS (Compare-And-Swap) operation.
  Uses an if-match header to ensure the key doesn't exist when creating.

  Args:
    - req: A configured %ObjectStore{} client
    - key: The key to claim
    - body: The content to write if claim succeeds

  Returns:
    - {:ok, {:claimed, etag}} if successful
    - {:error, :already_claimed} if key exists
    - {:error, reason} for other failures
  """
  def try_claim(%__MODULE__{} = client, key, body) do
    req = new_req(client, consistent: true, headers: [{"if-none-match", "*"}])

    case Req.request(req,
           method: :put,
           url: key,
           body: body,
           retry: false
         ) do
      {:ok, %{status: status, headers: headers}}
      when status >= 200 and status < 300 ->
        {:ok, {:claimed, parse_etag!(headers)}}

      {:ok, %{status: 412}} ->
        # Precondition Failed - object exists (already claimed)
        {:error, :already_claimed}

      {:ok, response} ->
        # Other unexpected response
        {:error, response}

      {:error, exception} ->
        # Network or other errors
        {:error, exception}
    end
  end

  @doc """
  Gets an object from storage.

  ## Options
    `:consistent` - whether to make a consistent request to the default region. (Default `true`)

  ## Examples

      iex> get_object(ObjectStore.new(), "my-key")
      {:ok, %{body: "my-value", etag: "..."}

  Returns of a map of the form `%{body: body, etag: etag}` or `{:error, reason}`.
  """
  def get_object(%__MODULE__{} = client, key, opts \\ []) when is_list(opts) do
    opts = Keyword.validate!(opts, [:consistent])
    consistent = Keyword.get(opts, :consistent, true)
    req = new_req(client, consistent: consistent)

    case Req.get(req, url: key) do
      {:ok, %{status: 200, body: body, headers: response_headers}} ->
        etag = parse_etag!(response_headers)
        {:ok, %{body: body, etag: etag}}

      {:ok, %{status: 404}} ->
        {:error, :not_found}

      {:ok, response} ->
        Logger.error("Failed to get object: #{inspect(key: key, response: response)}")

        {:error, :bad_gateway}

      {:error, reason} ->
        Logger.error("Failed to get object: #{inspect(key: key, error: reason)}")
        {:error, reason}
    end
  end

  @doc """
  Lists objects in a bucket with optional prefix filtering.

  ## Options
    `:consistent` - whether to make a consistent request to the default region. (Default `true`)
  """
  def list_objects(%__MODULE__{} = client, prefix, opts \\ []) when is_list(opts) do
    opts = validate_opts!(opts)
    consistent = Keyword.get(opts, :consistent, true)
    req = new_req(client, consistent: consistent)
    max_results = Keyword.get(opts, :max_results, 1000)
    continuation_token = Keyword.get(opts, :continuation_token)

    params = %{
      "list-type" => "2",
      "prefix" => prefix,
      "max-keys" => max_results
    }

    params =
      if continuation_token do
        Map.put(params, "continuation-token", continuation_token)
      else
        params
      end

    case Req.get(req, url: "/", params: params, retry: :transient) do
      {:ok, %{status: 200, body: body}} ->
        parse_list_objects_response(body)

      {:ok, response} ->
        Logger.error("Failed to list objects: #{inspect(response: response)}")
        {:error, %{errors: ["list objects failed"]}}

      {:error, reason} ->
        Logger.error("Failed to list objects: #{inspect(error: reason)}")
        {:error, reason}
    end
  end

  @doc """
  Streams all object keys in a bucket with optional prefix filtering.

  *CAUTION*: use this with care as since this will stream over _every_ matching object
  in the bucket. While the stream will efficiently enumerable all objects without loading
  them all into memory at a time, it can still enumerate the entire object space on an
  eager match.

  Returns a Stream that automatically handles pagination using continuation tokens.
  This is memory-efficient for large buckets as it only loads one page at a time.

  ## Options

    * `:error_handler` - Function to handle errors. Receives error reason and should
      return `:halt` to stop the stream or `:continue` to skip the error.
      Defaults to raising the error.

  ## Examples

      # Stream all keys with prefix
      ObjectStore.list_all_objects_stream(store, "my-prefix/")
      |> Stream.take(100)
      |> Enum.map(fn %{key: key, etag: etag} = _obj -> ... end)

      # With custom error handling
      ObjectStore.list_all_objects_stream(store, "prefix/",
        error_handler: fn error_reason ->
          Logger.warning("List error: \#{inspect(error_reason)}")
          :continue
        end)

  """
  def list_all_objects_stream(%__MODULE__{} = client, prefix, opts \\ []) do
    {error_handler, list_opts} =
      Keyword.pop(opts, :error_handler, fn reason -> raise inspect(reason) end)

    Stream.unfold(nil, fn
      :done ->
        nil

      token ->
        # Merge continuation token with other options like max_results
        current_opts =
          if token, do: Keyword.put(list_opts, :continuation_token, token), else: list_opts

        case list_objects(client, prefix, current_opts) do
          {:ok, %{keys: keys, next_continuation_token: next_token}} when next_token != nil ->
            {keys, next_token}

          {:ok, %{keys: keys, next_continuation_token: nil}} ->
            {keys, :done}

          {:ok, %{keys: keys}} ->
            {keys, :done}

          {:error, reason} ->
            case error_handler.(reason) do
              :halt -> nil
              :continue -> {[], :done}
              _ -> nil
            end
        end
    end)
    |> Stream.flat_map(& &1)
  end

  @doc """
  Puts an object to S3.

  ## Options
  - `:max_retries` - The maximum number of times to retry put. Default 0.
  - `:etag` - The existing etag to match. Conflicts return `{:error, :conflict}`
  - `:timeout` - Total time in ms for the operation including retries. If exceeded,
    no further retries will be attempted. Default: no timeout (unlimited retries until max_retries).
  """
  def put_object(%__MODULE__{} = client, key, data, opts \\ []) do
    opts = validate_opts!(opts)
    content_type = Keyword.get(opts, :content_type, "application/octet-stream")
    consistent = Keyword.get(opts, :consistent, true)
    timeout = Keyword.get(opts, :timeout)

    # Handle :infinity and nil as no deadline
    deadline_at =
      case timeout do
        nil -> nil
        :infinity -> nil
        ms when is_integer(ms) -> System.system_time(:millisecond) + ms
      end

    # Add If-Match header for etag verification
    base_headers = [
      {"content-type", content_type}
    ]

    headers =
      case Keyword.fetch(opts, :etag) do
        {:ok, etag} when is_binary(etag) ->
          [{"if-match", etag} | base_headers]

        {:ok, invalid} ->
          raise ArgumentError, "excepted etag to be a string, got: #{inspect(invalid)}"

        :error ->
          base_headers
      end

    req = new_req(client, consistent: consistent, headers: headers)

    req_with_retries =
      case Keyword.fetch(opts, :max_retries) do
        {:ok, retries} when is_integer(retries) and retries >= 0 ->
          Req.merge(req,
            max_retries: retries,
            retry: fn
              # don't retry good response, conflict, or not found
              # 404 on PUT with if-match means object doesn't exist (localstack behavior)
              %Req.Request{}, %Req.Response{status: status}
              when status in 200..299 or status in [404, 409, 412] ->
                false

              # check deadline before retrying transient errors
              %Req.Request{}, _exception ->
                if deadline_at && System.system_time(:millisecond) >= deadline_at do
                  false
                else
                  true
                end
            end
          )

        :error ->
          req
      end

    case Req.put(req_with_retries, url: key, body: data) do
      {:ok, %{status: status} = response} when status in 200..299 ->
        etag = parse_etag!(response.headers)
        {:ok, %{etag: etag, body: data}}

      {:ok, %{status: status}} when status in [409, 412] ->
        # Precondition Failed - etag mismatch
        {:error, :conflict}

      {:ok, response} ->
        Logger.error("Failed to put object with etag: #{inspect(key: key, response: response)}")
        {:error, response}

      {:error, reason} ->
        Logger.error("Failed to put object with etag: #{inspect(key: key, error: reason)}")
        {:error, reason}
    end
  end

  defp parse_etag!(%{} = headers_or_attrs) do
    case headers_or_attrs["etag"] || headers_or_attrs["ETag"] do
      [etag_value] when is_binary(etag_value) -> String.replace(etag_value, "\"", "")
      etag_value when is_binary(etag_value) -> String.replace(etag_value, "\"", "")
      nil -> raise "ETag not found in response: #{inspect(headers_or_attrs)}"
      other -> raise "Unexpected ETag format: #{inspect(other)}"
    end
  end

  defp validate_opts!(opts) do
    Keyword.validate!(opts, [
      :content_type,
      :consistent,
      :headers,
      :backoff_fun,
      :timeout,
      :task_supervisor,
      :max_retries,
      :max_results,
      :continuation_token,
      :prefix,
      :etag
    ])
  end

  @doc """
  Deletes an object from S3.
  """
  def delete_object(%__MODULE__{} = client, key) do
    req = new_req(client, consistent: true)

    case Req.request(req,
           method: :delete,
           url: key,
           retry: false
         ) do
      {:ok, %{status: status}} when status in 200..299 ->
        :ok

      {:ok, %{status: 404}} ->
        {:error, :not_found}

      {:ok, %{status: 412}} ->
        {:error, :not_found}

      {:ok, response} ->
        Logger.error("Failed to delete object: #{inspect(key: key, response: response)}")

        {:error, %{errors: ["delete object failed"]}}

      {:error, reason} ->
        Logger.error("Failed to delete object: #{inspect(key: key, error: reason)}")
        {:error, reason}
    end
  end

  @doc """
  Copies an object within S3 (used for moving to trash).
  """
  def copy_object(%__MODULE__{} = client, source_bucket, source_key, dest_bucket, dest_key) do
    copy_source = "/#{source_bucket}/#{source_key}"

    headers = [
      {"x-amz-copy-source", copy_source}
    ]

    req = new_req(client, consistent: true, headers: headers)

    case Req.put(req, url: "/#{dest_bucket}/#{dest_key}") do
      {:ok, %{status: status}} when status in 200..299 ->
        :ok

      {:ok, response} ->
        Logger.error(
          "Failed to copy object: #{inspect(source: "#{source_bucket}/#{source_key}",
          dest: "#{dest_bucket}/#{dest_key}",
          response: response)}"
        )

        {:error, %{errors: ["copy object failed"]}}

      {:error, reason} ->
        Logger.error(
          "Failed to copy object: #{inspect(source: "#{source_bucket}/#{source_key}",
          dest: "#{dest_bucket}/#{dest_key}",
          error: reason)}"
        )

        {:error, reason}
    end
  end

  @doc """
  Atomically updates an object using etag-based conflict resolution.

  Uses a read-modify-write pattern with etag verification to avoid conflicts.
  If a conflict is detected, it will retry up to the specified maximum retries.

  - `key` – The object key to update
  - `update_fn` - Function that takes current data and returns {:ok, new_data} or {:error, reason}
    - To proceed with write, return `{:ok, new_data}`
    - To abort write, return `{:error, reason}`

  ## Options
  - `:timeout` - Operation timeout (default: :infinity)
  - `:max_retries` - Maximum number of retry attempts (default: 5)
  - `:consistent` - Use consistent reads (default: true)
  - `:content_type` - Content type for the object (default: "application/octet-stream")
  - `:task_supervisor` - Task supervisor for async operations (default: uses client.task_supervisor)

  ## Returns:
  - {:ok, %{etag: etag, body: body}} on successful update
  - {:error, :not_found} if the key doesn't exist
  - {:error, :max_retries_exceeded} if retries are exhausted
  - {:error, reason} for other failures

  ## Examples

      store = ObjectStore.new()
      ObjectStore.update_object(store, "my-key", fn %{body: current_data, etag: current_etag} ->
        updated = current_data <> " - updated"
        {:ok, updated}
      end, timeout: :infinity, max_retries: 5)
  """
  def update_object(%__MODULE__{} = client, key, update_fn, opts \\ [])
      when is_function(update_fn, 1) do
    timeout = Keyword.get(opts, :timeout, :infinity)
    max_retries = Keyword.get(opts, :max_retries, 5)
    task_sup = Keyword.get(opts, :task_supervisor, client.task_supervisor)

    if timeout == :infinity do
      do_update_object(client, key, update_fn, opts, 0, max_retries)
    else
      task =
        Task.Supervisor.async(task_sup, fn ->
          do_update_object(client, key, update_fn, opts, 0, max_retries)
        end)

      case Task.yield(task, timeout) || Task.shutdown(task) do
        {:ok, result} -> result
        nil -> {:error, :timeout}
      end
    end
  end

  defp do_update_object(%__MODULE__{} = client, key, update_fn, opts, attempt, max_retries) do
    if attempt > max_retries do
      {:error, :max_retries_exceeded}
    else
      case get_object(client, key) do
        {:ok, %{body: current_data, etag: current_etag}} ->
          case update_fn.(%{body: current_data, etag: current_etag}) do
            {:ok, new_data} ->
              case put_object(client, key, new_data, Keyword.put(opts, :etag, current_etag)) do
                {:ok, result} ->
                  {:ok, result}

                {:error, :conflict} ->
                  # ETag mismatch, retry with exponential backoff
                  Process.sleep(round(min(100 * :math.pow(2, attempt), 1000)))
                  do_update_object(client, key, update_fn, opts, attempt + 1, max_retries)

                {:error, reason} ->
                  {:error, reason}
              end

            {:error, reason} ->
              {:error, reason}
          end

        {:error, :not_found} ->
          {:error, :not_found}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp parse_list_objects_response(response_map) do
    # Navigate to the ListBucketResult in the map
    bucket_result = response_map["ListBucketResult"] || %{}

    # Extract keys from Contents
    # Contents can be a single map or a list of maps
    contents = bucket_result["Contents"] || []
    contents_list = if is_list(contents), do: contents, else: [contents]

    keys =
      contents_list
      |> Enum.map(fn content ->
        case content do
          %{"Key" => key} = item ->
            # Include LastModified if present
            base = %{key: key, etag: parse_etag!(item), size: item["Size"]}

            case Map.get(item, "LastModified") do
              nil -> base
              last_modified -> Map.put(base, :last_modified, last_modified)
            end

          _ ->
            nil
        end
      end)
      |> Enum.reject(&is_nil/1)

    # Extract next continuation token
    next_token = bucket_result["NextContinuationToken"]

    # Check if response is truncated
    is_truncated =
      case bucket_result["IsTruncated"] do
        "true" -> true
        true -> true
        _ -> false
      end

    result = %{
      keys: keys,
      is_truncated: is_truncated,
      next_continuation_token: next_token
    }

    {:ok, result}
  end

  defp parse_list_buckets_response(response_map) do
    # Navigate to the ListAllMyBucketsResult in the map
    buckets_result = response_map["ListAllMyBucketsResult"] || %{}

    # The "Buckets" section contains a "Bucket" key with the actual list
    buckets_section = buckets_result["Buckets"] || %{}

    # Extract buckets list - it's directly under "Bucket" key
    bucket_list =
      case buckets_section do
        %{"Bucket" => bucket_data} when is_list(bucket_data) -> bucket_data
        %{"Bucket" => bucket_data} when is_map(bucket_data) -> [bucket_data]
        # Fallback if structure is different
        bucket_data when is_list(bucket_data) -> bucket_data
        _ -> []
      end

    buckets =
      bucket_list
      |> Enum.map(fn bucket ->
        case bucket do
          %{"Name" => name, "CreationDate" => creation_date} ->
            %{name: name, creation_date: creation_date}

          %{"Name" => name} ->
            %{name: name, creation_date: nil}

          _ ->
            nil
        end
      end)
      |> Enum.reject(&is_nil/1)

    buckets
  end
end
