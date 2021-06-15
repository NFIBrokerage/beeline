defmodule Beeline do
  @schema Beeline.Config.schema()
  @producer_schema Beeline.Producer.schema()

  @moduledoc """
  A tool for building in-order GenStage topologies for EventStoreDB

  Beeline provides a Broadway-like experience for building GenStage
  topologies for consuming streams of events from EventStoreDB in-order,
  usually one event at a time. Beeline aims to close over the supervision
  and basic configuration of the producer(s), as well as some of the
  run-of-the-mill procedure done in the `c:GenStage.init/1` callback of
  the consumer such as linking the producer process(es).

  ## The Beeline Topology

  Beeline creates a topology of GenStage, GenServer, and Supervisor processes.
  This topology looks like the following:

  ```text
  Supervisor
  ├── HealthChecker*
  └── StageSupervisor
      ├── Producer*
      └── Consumer
  ```

  Let's break these down from the bottom up:

  * "Consumer" - the GenStage consumer module which invokes
    `Beeline.start_link/2`, handles events, and increments stream
    positions.
  * "Producer*" - one or more GenStage producers which feed the consumer.
    These producers are declared with the `:producers` key and may either
    be `Kelvin.InOrderSubscription`, `Volley.InOrderSubscription`, or
    `Beeline.DummyProducer` producer modules.
  * "StageSupervisor" - a supervisor for the GenStage pipeline. This supervisor
    has a `:transient` restart strategy so that if the GenStage pipeline halts
    on an event it cannot handle, the `StageSupervisor` supervision tree is
    brought down but not the entire supervision tree. This behavior is
    desirable so that the health-checker process can continue reading the
    stream positions and so that an operator can perform any necessary
    manual intervention on the crashed supervision tree (for example,
    skipping the failure event).
  * "HealthChecker*" - a GenServer which periodically polls the stream positions
    of a producer. There is one health checker process per producer.
  * "Supervisor" - a top-level supervisor. This supervisor has a `:permanent`
    restart strategy.

  See the `start_link/2` documentation for a full configuration reference and
  examples.
  """

  defmacro __using__(_opts) do
    quote do
      use GenStage

      @impl GenStage
      def init(config) do
        producers =
          Enum.map(config.producers, fn {_key, producer} ->
            {producer.name,
             min_demand: producer.min_demand, max_demand: producer.max_demand}
          end)

        {:consumer, config.context, subscribe_to: producers}
      end

      defoverridable init: 1
    end
  end

  @doc """
  Starts a Beeline topology

  ## Options

  #{NimbleOptions.docs(@schema)}

  #### Producer options

  #{NimbleOptions.docs(@producer_schema)}

  ## Examples

      defmodule MyEventHandler do
        use Beeline

        def start_link(_opts) do
          Beeline.start_link(MyEventHandler,
            name: MyEventHandler,
            producers: [
              default: [
                name: MyEventHandler.EventListener,
                stream_name: "$ce-BoundedContext.AggregateName",
                connection: MyEventHandler.EventStoreDBConnection,
                adapter: :kelvin
              ]
            ]
          )
        end

        # .. GenStage callbacks

        @impl GenStage
        def handle_events([subscription_event], _from, state) do
          # .. handle the events one-by-one ..

          {:noreply, [], state}
        end
      end
  """
  @doc since: "0.1.0"
  @spec start_link(module :: module(), opts :: Keyword.t()) ::
          Supervisor.on_start()
  def start_link(module, opts) do
    case NimbleOptions.validate(opts, @schema) do
      {:error, reason} ->
        raise ArgumentError,
              "invalid configuration given to Beeline.start_link/2, " <>
                reason.message

      {:ok, opts} ->
        opts
        |> Keyword.put(:module, module)
        |> Beeline.Config.source()
        |> Beeline.Topology.start_link()
    end
  end

  @doc """
  Restarts the supervision tree of GenStages for the given Beeline topology

  This can be useful for manual intervention by a human operator in a remote
  console session, if the GenStage supervision tree crashes and exceeds the
  retry limits.

  ## Examples

      iex> Beeline.restart_stages(MyEventHandler)
      :ok
  """
  @spec restart_stages(module()) :: :ok | {:error, term()}
  def restart_stages(beeline) when is_atom(beeline) do
    beeline
    |> Module.concat(Topology)
    |> GenServer.call(:restart_stages)
  end

  @doc """
  Decodes the body of a subscription event

  This function performs JSON decoding if necessary and converts maps with
  string keys into maps keyed by atoms. This This can potentially lead to
  atom exhaustion, but the allowed atom count is quite high and usually this
  concern is only theoretical.

  ## Examples

      @impl GenStage
      def handle_events([subscription_event], _from, state) do
        event = Beeline.decode_event(subscription_event)
        # ..
  """
  defdelegate decode_event(subscription_event), to: Beeline.EventStoreDB

  @doc """
  Determines the stream position of the subscription event

  This function prefers link stream positions if available. This means that if
  the subscription from which the event is emitted is reading a projected
  stream such as a category stream, the returned stream position will reflect
  the position in the projected stream instead of the origin stream.

  ## Examples

      @impl GenStage
      def handle_events([subscription_event], _from, state) do
        # consume the events

        MyApp.Repo.transaction(fn ->
          # save some state

          producer = Beeline.producer(subscription_event)
          stream_position = Beeline.stream_position(subscription_event)
          MyApp.StreamPosition.persist(producer, stream_position)
        end)
      end
  """
  defdelegate stream_position(subscription_event), to: Beeline.EventStoreDB

  @doc """
  Determines which producer emitted the subscription event

  This can be useful in order to save stream positions when a consumer is
  subscribed to multiple producers. Should be used in tandem with
  `stream_position/1`.
  """
  defdelegate producer(subscription_event), to: Beeline.EventStoreDB

  @doc """
  Wraps an event in a subscription event packet

  This can be useful for building test events to pass through the dummy
  producer.
  """
  @spec as_subscription_event(map(), atom()) :: {atom(), map()}
  def as_subscription_event(event, producer) do
    {producer, %{event: %{data: Jason.encode!(event), event_number: 0}}}
  end

  @doc """
  Gives a set of events to a topology's dummy producer

  This function can be used to test running events through a topology.
  If there are multiple producers, one is picked at random.
  """
  def test_events(events, beeline) when is_atom(beeline) do
    beeline
    |> Module.concat(Topology)
    |> GenServer.call({:test_events, events})
  end
end
