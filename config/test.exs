import Config

config :beeline,
  get_stream_position: fn _producer -> -1 end,
  auto_subscribe?: fn _producer -> true end,
  spawn_health_checkers?: true

config :beeline, Beeline.Fixtures.ExtremeClient,
  db_type: :node,
  host: System.get_env("EVENTSTORE_TCP_HOST") || "localhost",
  port: 1113,
  username: "admin",
  password: "changeit",
  max_attempts: :infinity,
  reconnect_delay: 2_000

config :beeline, Beeline.Fixtures.SpearClient,
  host: System.get_env("EVENTSTORE_GRPC_HOST") || "localhost"
