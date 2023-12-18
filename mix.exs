defmodule Pgmq.MixProject do
  use Mix.Project

  @version "0.3.0"

  def project do
    [
      app: :pgmq,
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      description: description(),
      package: package(),
      name: "Pgmq",
      docs: docs(),
      source_url: "https://github.com/v0idpwn/pgmq-elixir"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.0"},
      {:ecto_sql, "~> 3.10"},
      {:postgrex, ">= 0.0.0"},
      {:dialyxir, "~> 1.0", only: :dev},
      {:ex_doc, "~> 0.25", only: :dev}
    ]
  end

  defp docs do
    [
      main: "Pgmq",
      source_ref: "v#{@version}"
    ]
  end

  defp package do
    [
      name: "pgmq",
      files: ~w(lib .formatter.exs mix.exs README.md),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/v0idpwn/pgmq-elixir"}
    ]
  end

  defp description, do: "Wrapper for the pgmq extension"

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
