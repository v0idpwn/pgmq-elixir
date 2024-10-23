import Config

config :pgmq, Pgmq.TestRepo,
  database: "postgres",
  username: "postgres",
  password: "postgres",
  port: 5432

config :logger, level: :info
