defmodule Pgmq.TestRepo do
  use Ecto.Repo, otp_app: :pgmq, adapter: Ecto.Adapters.Postgres
end
