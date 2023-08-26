# Pgmq
Thin elixir client for the pgmq postgres extension.

## Installation
For instructions on installing the pgmq extension, or getting a docker image
with the extension installed, check the [official pgmq repo](https://github.com/tembo-io/pgmq).

The package can be installed by adding `pgmq` to your list of dependencies in
`mix.exs`:

```elixir
def deps do
  [
    {:pgmq, "~> 0.1.0"}
  ]
end
```
If needed, you can create a migration to create the extension in your database:
```elixir
defmodule MyApp.Repo.Migrations.CreatePgmqExtension do
  use Ecto.Migration

  def change do
    execute("CREATE EXTENSION pgmq CASCADE")
  end
end
```
And to create queues:
```elixir
defmodule MyApp.Repo.Migrations.CreateSomeQueues do
  use Ecto.Migration

  def up do
    Pgmq.create_queue(repo(), "queue_a")
    Pgmq.create_queue(repo(), "queue_b")
    Pgmq.create_queue(repo(), "queue_c")
  end

  def down do
    Pgmq.drop_queue(repo(), "queue_a")
    Pgmq.drop_queue(repo(), "queue_b")
    Pgmq.drop_queue(repo(), "queue_c")
  end
end
```

## Documentation
Check [our documentation in Hexdocs](https://hexdocs.com/pgmq).

## Usage with Broadway
The [OffBroadwayPgmq](https://github.com/v0idpwn/off_broadway_pgmq) package
provides a configurable Broadway adapter that manages reading, acking and
archiving failing messages.

## Stability warning
This package (and pgmq) are both pre-1.0 and might have breaking changes.
