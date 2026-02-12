import Config

# Configure syn event handler for DurableServer conflict resolution
config :syn,
  event_handler: Group.SynEventHandler

if config_env() == :test do
  config :logger, level: :info
end
