# Load environment variables from .env file
case File.read(".env") do
  {:ok, content} ->
    content
    |> String.split("\n")
    |> Enum.reject(&(&1 == "" or String.starts_with?(String.trim(&1), "#")))
    |> Enum.each(fn line ->
      with [key, val] <- String.split(line, "=", parts: 2) do
        System.put_env(String.trim(key), String.replace(val, ~r/^[\s"']+|[\s"']+$/, ""))
      end
    end)

  {:error, _} ->
    :noop
end

# Selecting storage-free tests must not contact LocalStack. Each suite invocation
# gets its own bucket; LocalStackCase creates it only when one of its tests runs.
Application.put_env(
  :durable_server,
  :test_object_store_bucket,
  "durable-test-#{DurableServer.UUID.uuid4()}"
)

Application.put_env(:durable_server, :test_object_store_used, false)

ExUnit.start(
  exclude: [:localstack, :ekv, :integration, :stress],
  assert_receive_timeout: 1_000
)

ExUnit.after_suite(fn _results ->
  if Application.fetch_env!(:durable_server, :test_object_store_used) do
    DurableServer.TestHelper.clean_test_object_store!()
  end
end)
