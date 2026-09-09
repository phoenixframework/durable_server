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

# Allocate a namespace without contacting storage. Only LocalStackCase creates it.
DurableServer.TestHelper.init_test_run()
ExUnit.configure(exclude: [:localstack, :integration, :stress])
ExUnit.after_suite(fn _ -> DurableServer.TestHelper.cleanup_test_run!() end)

ExUnit.start()
