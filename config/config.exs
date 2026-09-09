import Config

if config_env() == :test do
  config :logger, level: :info

  property_runs = String.to_integer(System.get_env("DURABLE_PROPERTY_RUNS", "100"))
  if property_runs < 1, do: raise("DURABLE_PROPERTY_RUNS must be positive")
  config :stream_data, max_runs: property_runs
end
