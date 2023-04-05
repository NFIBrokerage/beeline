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
  concatenated with "is caught up." if the producer's current stream position
  matches the latest available stream position and a 'warn' level message
  with "is behind: n events." when the producer is behind, with `n` being
  the number of events. The log messages also include metadata fields
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

    if delta == 0 do
      Logger.debug("#{producer} is caught up.", log_metadata)
    else
      # coveralls-ignore-start
      Logger.warn("#{producer} is behind: #{delta} events.", log_metadata)

      # coveralls-ignore-stop
    end

    state
  end
end
