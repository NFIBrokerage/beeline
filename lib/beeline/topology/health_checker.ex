defmodule Beeline.Topology.HealthChecker do
  @moduledoc false

  # a GenServer which periodically polls a producer's stream positions and
  # process

  @behaviour GenServer

  defstruct [
    :producer,
    :interval,
    :get_stream_position,
    :get_head_position,
    :hostname,
    drift: 0,
    current_position: -1
  ]

  def child_spec({config, key, producer}) do
    %{
      id: {__MODULE__, key},
      start: {__MODULE__, :start_link, [{config, producer}]},
      type: :worker,
      restart: :permanent
    }
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl GenServer
  def init({config, producer}) do
    state =
      %__MODULE__{
        producer: producer.name,
        get_head_position: fn ->
          Beeline.EventStoreDB.latest_event_number(
            producer.adapter,
            producer.connection,
            producer.stream_name
          )
        end,
        get_stream_position: config.get_stream_position,
        interval: config.health_check_interval,
        hostname: hostname()
      }
      |> schedule_next_poll()

    {:ok, state}
  end

  @impl GenServer
  def handle_info(:poll, state) do
    state =
      state
      |> poll_producer()
      |> schedule_next_poll()

    {:noreply, state}
  end

  defp schedule_next_poll(state) do
    drift = div(state.interval, 6)
    Process.send_after(self(), :poll, state.interval + drift)

    put_in(state.drift, drift)
  end

  defp poll_producer(state) do
    metadata = %{
      producer: state.producer,
      stream_name: state.stream_name,
      hostname: state.hostname,
      interval: state.interval,
      drift: state.drift,
      measurement_time: DateTime.utc_now(),
      prior_position: state.current_position
    }

    :telemetry.span(
      [:beeline, :health_check],
      metadata,
      fn ->
        current_position = state.get_stream_position.()

        metadata =
          Map.merge(metadata, %{
            current_position: current_position,
            head_position: state.get_head_position.(),
            alive?: alive?(state.producer)
          })

        state = put_in(state.current_position, current_position)

        {state, metadata}
      end
    )
  end

  defp alive?(producer) do
    case GenServer.whereis(producer) do
      nil -> false
      pid -> Process.alive?(pid)
    end
  end

  defp hostname do
    case :inet.gethostname() do
      {:ok, hostname_charlist} ->
        hostname_charlist |> to_string()

      _ ->
        nil
    end
  end
end
