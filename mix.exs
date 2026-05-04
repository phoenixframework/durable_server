defmodule DurableServer.MixProject do
  use Mix.Project

  def project do
    [
      app: :durable_server,
      version: "0.1.1",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      homepage_url: "https://github.com/phoenixframework/durable_server",
      description: """
      DurableServer provides durable, distributed GenServer processes backed by object storage.
      """
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      maintainers: ["Chris McCord"],
      licenses: ["MIT"],
      links: %{
        GitHub: "https://github.com/phoenixframework/durable_server"
      },
      files: ~w(lib CHANGELOG.md LICENSE.md mix.exs README.md .formatter.exs)
    ]
  end

  def application do
    [
      extra_applications: [:logger, :os_mon],
      mod: {DurableServer.Application, []}
    ]
  end

  defp deps do
    [
      {:group, "~> 0.2.0"},
      {:jason, "~> 1.4"},
      {:req, "~> 0.5"},
      {:req_s3, "~> 0.2"},
      {:finch, "~> 0.18"},
      {:sweet_xml, "~> 0.7"},
      {:ex_doc, "~> 0.30", only: :dev, runtime: false},
      {:ekv, "~> 0.4.0", optional: true}
    ]
  end
end
