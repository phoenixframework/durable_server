defmodule Group.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/chrismccord/group"

  def project do
    [
      app: :group,
      version: @version,
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: docs(),
      name: "Group",
      homepage_url: @source_url,
      description: """
      Distributed process groups, registry, lifecycle monitoring, and isolated subclusters.
      """
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger],
      mod: {Group.Application, []}
    ]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.30", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["Chris McCord"],
      licenses: ["MIT"],
      links: %{GitHub: @source_url},
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE.md CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "Group",
      source_url: @source_url,
      source_ref: "v#{@version}"
    ]
  end
end
