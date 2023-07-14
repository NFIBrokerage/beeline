defmodule Beeline.HealthChecker.Logger do
  @moduledoc """
  A Task to attach a logger exporter for health-checker telemetry

  Attaches a telemetry handler which writes out how far a producer is behind
  the head of the stream to the logger.

  This task can be started in a supervision tree such as an app's application
  supervision tree:

  ```elixir
  def start(_type, _args) do
    children = [
      Beeline.HealthChecker.Logger,
      MyApp.MyBeelineTopology
    ]
    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
  ```

  The log messages are 'debug' level in a format of the producer name
  concatenated with "is up-to-date." if the producer's current stream position
  matches the latest available stream position (within `acceptable_behind_by`).

  When producer is falling behind and falling behind more then in a previous check,
  it is logged in warn level with "is behind: n events." and "is behind more: n events"
  with `n` being the number of events producer is behind head of stream.

  Once producer is behind the stream less then in a previous check and when it catches up with it,
  it is logged in `info` level as `is behind but catching up: n events` and `is caught up.` respectively.

  The log messages also include metadata fields
  `:event_listener` - the name of the producer - and `delta`: the number
  of events by which the producer is behind.
  """

  use Task
  require Logger

  @doc false
  def start_link(_opts) do
    Task.start_link(__MODULE__, :attach, [])
  end

  @doc false
  def attach do
    :telemetry.attach(
      "beeline-health-checker-logger",
      [:beeline, :health_check, :stop],
      &__MODULE__.handle_event/4,
      :ok
    )
  end

  @doc false
  def handle_event(_event, _measurement, metadata, state) do
    producer = inspect(metadata[:producer])
    delta = metadata[:head_position] - metadata[:current_position]
    log_metadata = [delta: delta, event_listener: producer]

    case metadata[:status] do
      :up_to_date ->
        Logger.debug("#{producer} is up-to-date", log_metadata)

      # coveralls-ignore-start
      :caught_up ->
        Logger.info("#{producer} is caught up.", log_metadata)

      :falling_behind ->
        Logger.warn("#{producer} is behind: #{delta} events.", log_metadata)

      :falling_behind_more ->
        Logger.warn(
          "#{producer} is behind more: #{delta} events.",
          log_metadata
        )

      :catching_up ->
        Logger.info(
          "#{producer} is behind but catching up: #{delta} events.",
          log_metadata
        )

      :stuck ->
        Logger.error(
          "#{producer} is stuck at #{metadata[:current_position]}: behind by #{delta} events.",
          log_metadata
        )

        # coveralls-ignore-stop
    end

    state
  end
end
