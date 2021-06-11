defmodule Beeline.Fixtures.GoodHandler do
  @moduledoc """
  A good-example fixture which sets up a handler that subscribes to two
  different versions of EventStoreDB

  Note that this handler stores stream positions in GenStage state which
  is not a good strategy for an actual process-manager or read-model.
  Ideally stream positions should be stored somewhere reliable like a
  PostgreSQL table.
  """

  use Beeline

  def get_state do
    GenServer.call(__MODULE__, :get_state)
  end

  def get_stream_position(producer) do
    case GenServer.whereis(__MODULE__) do
      pid when is_pid(pid) ->
        GenServer.call(pid, {:get_stream_position, producer})

      _ ->
        -1
    end
  end

  defstruct [:opts, events: [], stream_positions: %{}]

  def start_link(opts) do
    Beeline.start_link(__MODULE__,
      name: __MODULE__,
      producers: [
        tcp: [
          connection: Beeline.Fixtures.ExtremeClient,
          stream_name: opts[:tcp_stream_name],
          adapter: :kelvin
        ],
        grpc: [
          connection: Beeline.Fixtures.SpearClient,
          stream_name: opts[:grpc_stream_name],
          adapter: :volley
        ]
      ],
      get_stream_position: {__MODULE__, :get_stream_position, []},
      subscribe_after: 0,
      health_check_interval: 1_000,
      health_check_drift: 0,
      context: %__MODULE__{opts: Map.new(opts)}
    )
  end

  @impl GenStage
  def handle_events([subscription_event], _from, state) do
    event = Beeline.decode_event(subscription_event)
    producer = Beeline.producer(subscription_event)
    stream_position = Beeline.stream_position(subscription_event)

    state =
      state
      |> put_in([Access.key(:stream_positions), producer], stream_position)
      |> update_in([Access.key(:events)], &[event | &1])

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_call(:get_state, _from, state), do: {:reply, state, [], state}

  def handle_call({:get_stream_position, producer}, _from, state) do
    {:reply, state.stream_positions[producer] || -1, [], state}
  end
end
