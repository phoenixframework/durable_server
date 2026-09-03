defmodule DurableServer.ObjectStoreRetryTest do
  use ExUnit.Case, async: true

  alias DurableServer.ObjectStore

  test "put retries transient Req failures within its deadline" do
    adapter =
      adapter([
        %Req.Response{status: 503},
        %Req.TransportError{reason: :timeout},
        %Req.Response{status: 200, headers: %{"etag" => ["retry-etag"]}}
      ])

    assert {:ok, %{etag: "retry-etag", body: "heartbeat"}} =
             ObjectStore.put_object(store(adapter), "__nodes/test@localhost", "heartbeat",
               max_retries: 10,
               timeout: 1_000
             )
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
               timeout: 1_000
             )

    assert Process.get(responses_key) == [:unexpected_retry]
  end

  test "put reports authentication responses as non-retryable" do
    parent = self()
    responses_key = make_ref()
    Process.put(responses_key, [%Req.Response{status: 403}, :unexpected_retry])

    observer = fn _request, response_or_error, retryable? ->
      send(parent, {:attempt, response_or_error, retryable?})
    end

    assert {:error, %Req.Response{status: 403}} =
             ObjectStore.put_object(
               store(adapter(responses_key)),
               "__nodes/test@localhost",
               "heartbeat",
               max_retries: 10,
               timeout: 1_000,
               retry_observer: observer
             )

    assert_receive {:attempt, %Req.Response{status: 403}, false}
    assert Process.get(responses_key) == [:unexpected_retry]
  end

  test "put retries every Req transport error variant within its deadline" do
    adapter =
      adapter([
        %Req.TransportError{reason: :enetunreach},
        %Req.TransportError{reason: {:tls_alert, :unknown_ca}},
        %Req.Response{status: 200, headers: %{"etag" => ["retry-etag"]}}
      ])

    assert {:ok, %{etag: "retry-etag", body: "heartbeat"}} =
             ObjectStore.put_object(store(adapter), "__nodes/test@localhost", "heartbeat",
               max_retries: 10,
               timeout: 1_000
             )
  end

  test "put retry observer receives every classified attempt" do
    parent = self()

    adapter =
      adapter([
        %Req.Response{status: 503},
        %Req.TransportError{reason: :enetunreach},
        %Req.Response{status: 200, headers: %{"etag" => ["retry-etag"]}}
      ])

    observer = fn _request, response_or_error, retryable? ->
      send(parent, {:attempt, response_or_error, retryable?})
    end

    assert {:ok, %{etag: "retry-etag"}} =
             ObjectStore.put_object(store(adapter), "__nodes/test@localhost", "heartbeat",
               max_retries: 10,
               timeout: 1_000,
               retry_observer: observer
             )

    assert_receive {:attempt, %Req.Response{status: 503}, true}
    assert_receive {:attempt, %Req.TransportError{reason: :enetunreach}, true}
    assert_receive {:attempt, %Req.Response{status: 200}, false}
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
               timeout: 1_000
             )

    assert Process.get(responses_key) == [:unexpected_retry]
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
