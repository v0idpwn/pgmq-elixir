import Config

config :pgmq, Pgmq.TestRepo,
  database: "pgmq_test",
  username: "v0idpwn",
  password: "postgres",
  port: 28815

config :logger, level: :info
