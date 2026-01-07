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

# Exclude integration tests by default (they require real credentials)
ExUnit.configure(exclude: [:integration, :stress])

alias DurableServer.ObjectStore
import DurableServer.TestHelper

# Clear object store (local stack) for this run
store = test_object_store()
:ok = ObjectStore.ensure_bucket_exists(store)

for obj <- ObjectStore.list_all_objects_stream(store, "") do
  :ok = ObjectStore.delete_object(store, obj.key)
end

ExUnit.start()
