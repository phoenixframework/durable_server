defmodule DurableServer.ObjectStoreRetryTest do
  use ExUnit.Case, async: true

  alias DurableServer.ObjectStore

  # Retry outcomes are scripted, so these cases don't impose a wall-clock
  # deadline while other test modules compile. Deadline behavior is covered below.
  test "put retries transient Req failures" do
    adapter =
      adapter([
        %Req.Response{status: 503},
        %Req.TransportError{reason: :timeout},
        %Req.Response{status: 200, headers: %{"etag" => ["retry-etag"]}}
      ])

    assert {:ok, %{etag: "retry-etag", body: "heartbeat"}} =
             ObjectStore.put_object(store(adapter), "__nodes/test@localhost", "heartbeat",
               max_retries: 10,
               timeout: :infinity
             )
  end

  test "put signs each retry from clean generated headers" do
    parent = self()
    responses_key = make_ref()

    Process.put(responses_key, [
      %Req.Response{status: 503},
      %Req.Response{status: 200, headers: %{"etag" => ["retry-etag"]}}
    ])

    adapter = fn request ->
      send(parent, {
        :signed_headers,
        Req.Request.get_header(request, "authorization"),
        Req.Request.get_header(request, "x-amz-content-sha256"),
        Req.Request.get_header(request, "x-amz-date")
      })

      case Process.get(responses_key) do
        [response | rest] ->
          Process.put(responses_key, rest)
          {request, response}
      end
    end

    assert {:ok, %{etag: "retry-etag"}} =
             ObjectStore.put_object(store(adapter), "__nodes/test@localhost", "heartbeat",
               max_retries: 1,
               timeout: :infinity
             )

    for _attempt <- 1..2 do
      assert_receive {:signed_headers, [authorization], [content_sha256], [date]}
      refute authorization =~ "SignedHeaders=authorization;"
      assert byte_size(content_sha256) > 0
      assert byte_size(date) > 0
    end
  end

  test "put does not retry permanent Req responses" do
    responses_key = make_ref()
    Process.put(responses_key, [%Req.Response{status: 400}, :unexpected_retry])

    assert {:error, %Req.Response{status: 400}} =
             ObjectStore.put_object(
               store(adapter(responses_key)),
               "__nodes/test@localhost",
               "heartbeat",
               max_retries: 10,
               timeout: :infinity
             )

    assert Process.get(responses_key) == [:unexpected_retry]
  end

  test "put stops after the configured transient retry limit" do
    responses_key = make_ref()

    Process.put(responses_key, [
      %Req.Response{status: 503},
      %Req.Response{status: 503},
      :unexpected_retry
    ])

    assert {:error, %Req.Response{status: 503}} =
             ObjectStore.put_object(
               store(adapter(responses_key)),
               "__nodes/test@localhost",
               "heartbeat",
               max_retries: 1,
               timeout: :infinity
             )

    assert Process.get(responses_key) == [:unexpected_retry]
  end

  test "an expired operation deadline prevents otherwise retryable failures from retrying" do
    for failure <- [
          %Req.Response{status: 503},
          %Req.TransportError{reason: :timeout},
          %Req.HTTPError{protocol: :http2, reason: :unprocessed}
        ] do
      responses_key = make_ref()
      Process.put(responses_key, [failure, :unexpected_retry])

      assert {:error, ^failure} =
               ObjectStore.put_object(
                 store(adapter(responses_key)),
                 "__nodes/test@localhost",
                 "heartbeat",
                 max_retries: 10,
                 timeout: 0
               )

      assert Process.get(responses_key) == [:unexpected_retry]
    end
  end

  test "finite operation deadlines cap each HTTP receive attempt" do
    parent = self()

    adapter = fn request ->
      send(parent, {:receive_timeout, request.options.receive_timeout})

      case Process.get(:deadline_responses, [
             %Req.TransportError{reason: :timeout},
             %Req.Response{status: 200, headers: %{"etag" => ["retry-etag"]}}
           ]) do
        [response_or_error | rest] ->
          Process.put(:deadline_responses, rest)
          {request, response_or_error}
      end
    end

    assert {:ok, %{etag: "retry-etag"}} =
             ObjectStore.put_object(store(adapter), "__nodes/test@localhost", "heartbeat",
               max_retries: 10,
               timeout: 28_000
             )

    assert_receive {:receive_timeout, 5_000}
    assert_receive {:receive_timeout, 5_000}
  end

  test "an operation deadline below the attempt cap becomes the receive timeout" do
    parent = self()

    adapter = fn request ->
      send(parent, {:receive_timeout, request.options.receive_timeout})
      {request, %Req.Response{status: 200, headers: %{"etag" => ["deadline-etag"]}}}
    end

    assert {:ok, %{etag: "deadline-etag"}} =
             ObjectStore.put_object(store(adapter), "__nodes/test@localhost", "heartbeat",
               max_retries: 0,
               timeout: 750
             )

    assert_receive {:receive_timeout, 750}
  end

  defp adapter(responses) when is_list(responses) do
    responses_key = make_ref()
    Process.put(responses_key, responses)
    adapter(responses_key)
  end

  defp adapter(responses_key) do
    fn request ->
      case Process.get(responses_key) do
        [response_or_error | rest] ->
          Process.put(responses_key, rest)
          {request, response_or_error}

        [] ->
          raise "unexpected request"
      end
    end
  end

  defp store(adapter) do
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
