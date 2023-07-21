defmodule Beeline.HealthChecker do
  @moduledoc """
  A GenServer which periodically polls a producer's stream positions and
  process

  This GenServer emits `:telemetry` measurements which serve as an interface
  for exporting this health-check information to an external monitoring
  service.

  `Beeline.HealthChecker.Logger` is included as a reasonable default consumer
  for this telemetry. You may wish to export this telemetry to another system
  such as appsignal or grafana in order to create alerts when processors fall
  behind.

  ## Telemetry

  * `[:beeline, :health_check, :stop]` - dispatched by each HealthChecker
    process after polling the producer's position and process information
      * Measurement: `%{duration: native_time}` - the time taken to perform
        the position and process checks
      * Metadata, a map with the following keys:
          * `:producer` (module) - the producer module being measured
          * `:alive?` (boolean) - whether the producer process is alive
          * `:stream_name` (string) - the EventStoreDB stream name from which
            the producer reads
          * `:hostname` (string) - the hostname of the machine on which
            the health checker process is being run
          * `:interval` (integer) - the milliseconds the health checker process
            has waited (minus drift) since the last poll
          * `:drift` (integer) - the milliseconds used for drifting the interval
            for the last poll
          * `:measurement_time` (UTC datetime) - the time when the poll started
          * `:prior_position` (integer) - the `:current_position` from the last
            poll
          * `:current_position` (integer) - the current stream position of
            the producer
          * `:head_position` (integer) - the stream position of the head
            (the latest event) of the EventStoreDB stream
          * `:auto_subscribe` (boolean) - the value of the producer's `:auto_subscribe?`
            flag at the time of polling
  """

  @behaviour GenServer

  alias Beeline.HealthChecker.Impl

  defstruct [
    :producer,
    :stream_name,
    :interval_fn,
    :drift_fn,
    :get_stream_position,
    :get_head_position,
    :hostname,
    status: :up_to_date,
    interval: 0,
    drift: 0,
    acceptable_behind_by: 5,
    current_position: -1,
    head_position: -1,
    up_to_date?: true,
    auto_subscribe?: false
  ]

  @doc false
  def child_spec({config, key, producer}) do
    %{
      id: {__MODULE__, key},
      start: {__MODULE__, :start_link, [{config, producer}]},
      type: :worker,
      restart: :permanent
    }
  end

  @doc false
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl GenServer
  def init({config, producer}) do
    state =
      %__MODULE__{
        producer: producer.name,
        stream_name: producer.stream_name,
        get_head_position: fn ->
          Beeline.EventStoreDB.latest_event_number(
            producer.adapter,
            producer.connection,
            producer.stream_name
          )
        end,
        get_stream_position:
          wrap_function(config.get_stream_position, producer.name),
        interval_fn: wrap_function(config.health_check_interval),
        drift_fn: wrap_function(config.health_check_drift),
        hostname: hostname(),
        acceptable_behind_by: config.acceptable_behind_by,
        auto_subscribe?: wrap_function(config.auto_subscribe?, producer.name)
      }
      |> schedule_next_poll()

    {:ok, state}
  end

  @impl GenServer
  def handle_info(:poll, state) do
    state =
      state
      |> Impl.poll_producer()
      |> schedule_next_poll()

    {:noreply, state}
  end

  defp schedule_next_poll(state) do
    interval = state.interval_fn.()
    drift = state.drift_fn.()

    Process.send_after(self(), :poll, interval + drift)

    %__MODULE__{
      state
      | drift: drift,
        interval: interval
    }
  end

  defp hostname do
    case :inet.gethostname() do
      {:ok, hostname_charlist} ->
        hostname_charlist |> to_string()

      # coveralls-ignore-start
      _ ->
        nil
    end
  end

  defp wrap_function(function) when is_function(function, 0), do: function

  defp wrap_function({m, f, a}), do: fn -> apply(m, f, a) end

  defp wrap_function(value), do: fn -> value end

  defp wrap_function(function, producer_name) when is_function(function, 1) do
    fn -> function.(producer_name) end
  end

  defp wrap_function({m, f, a}, producer_name) do
    fn -> apply(m, f, [producer_name | a]) end
  end

  # coveralls-ignore-stop
end
