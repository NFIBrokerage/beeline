defmodule Beeline do
  @producer_schema [
    adapter: [
      doc: """
      The adapter module to use for creating the producer. Use `:kelvin` for
      EventStoreDB v3-5, `:volley` for EventStoreDB v20+, and `:dummy` for
      test cases.
      """,
      type: {:in, [:kelvin, :volley, :dummy]},
      required: true
    ],
    stream_name: [
      doc: """
      The name of the EventStoreDB stream to which this producer should
      subscribe for events.
      """,
      type: :string,
      required: true
    ],
    connection: [
      doc: """
      The module to use as a connection to form the subscription. When the
      `:adapter` option is `:kelvin`, this should be an Extreme client module.
      When the adapter is `:volley`, it should be a `Spear.Client` module.
      """,
      type: :atom,
      required: true
    ],
    name: [
      doc: """
      The full GenServer name to use for this producer. When this option is
      not provided, the name will be a formula of the name of the consumer
      and the index in the list of producers.
      """,
      type: :atom
    ]
  ]

  @schema [
    name: [
      doc: """
      The GenServer name for the topology. The topology will build on this
      name, using it as a prefix.
      """,
      type: :atom
    ],
    producers: [
      doc: """
      A list of producers to which the consumer should subscribe. See the
      "producer options" section below for the schema.
      """,
      type: {:list, {:custom, __MODULE__, :validate_schema, [@producer_schema]}}
    ],
    max_demand: [
      doc: """
      The maximum number of events this consumer is allowed to request from
      any producer. This option can be configured to allow batch processing.
      """,
      type: :pos_integer,
      default: 1
    ],
    min_demand: [
      doc: """
      The minimum number of events this consumer can request at a time from
      a producer.
      """,
      type: :pos_integer,
      default: 1
    ],
    get_stream_position: [
      doc: """
      A function to invoke in order to get the stream position for a producer.
      This function should be a 1-arity function (anonymous or capture) where
      the name of the producer is passed as the argument. This option may also
      be passed as an MFA tuple where the producer name will be prepended to
      the argument list. If this option is not provided, a default will be
      fetched with `Application.fetch_env!(:beeline, :get_stream_position)`.
      This configuration can be used to set a blanket function for all
      beelines to use.
      """,
      type: {:or, [:mfa, {:fun, 1}]}
    ],
    context: [
      doc: """
      A user-defined data structure which is used as the initial state of
      the GenStage consumer process.
      """,
      type: :any,
      default: nil
    ]
  ]

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
  ├── HealthChecker
  └── StageSupervisor
      ├── Producer*
      └── Consumer
  ```

  Let's break these down from the bottom up:

  * `Consumer` - the GenStage consumer module which invokes
    `Beeline.start_link/2`, handles events, and increments stream
    positions.
  * `Producer*` - one or more GenStage producers which feed the consumer.
    These producers are declared with the `:producers` key and may either
    be `Kelvin.InOrderSubscription`, `Volley.InOrderSubscription`, or
    `Beeline.DummyProducer` producer modules.
  * `StageSupervisor` - a supervisor for the GenStage pipeline. This supervisor
    has a `:transient` restart strategy so that if the GenStage pipeline halts
    on an event it cannot handle, the `StageSupervisor` supervision tree is
    brought down but not the entire supervision tree. This behavior is
    desirable so that the health-checker process can continue reading the
    stream positions and so that an operator can perform any necessary
    manual intervention on the crashed supervision tree (for example,
    skipping the failure event).
  * `HealthChecker` - a GenServer which periodically polls the stream positions
    of all producers in the topology.
  * `Supervisor` - a top-level supervisor. This supervisor has a `:permanent`
    restart strategy.

  See the `start_link/2` documentation for a full configuration reference and
  examples.
  """

  defmacro __using__(_opts) do
    quote do
      use GenStage

      @impl GenStage
      def init(opts) do
        producers =
          opts
          |> Keyword.fetch!(:producers)
          |> Enum.map(fn {producer, _opts} -> producer end)

        Enum.each(producers, fn producer ->
          producer
          |> GenServer.whereis()
          |> Process.link()
        end)

        {:consumer, opts[:context], subscribe_to: [producers]}
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
              [
                name: MyEventHandler.EventListener,
                stream_name: "$ce-BoundedContext.AggregateName",
                connection: MyEventHandler.EventStoreDBConnection
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
        raise ArgumentError, "invalid configuration given to Beeline.start_link/2," <> reason.message

      {:ok, opts} ->
        opts =
          opts
          |> Keyword.put(:module, module)
          |> add_default_opts()

        Beeline.Topology.start_link(module, opts)
    end
  end

  @doc false
  def add_default_opts(opts) do
    Enum.reduce(opts, [], &add_default_opt(&1, &2, opts))
  end

  @doc false
  def add_default_opt({:get_stream_position, nil}, opts, _all_opts) do
    get_stream_position = Application.fetch_env!(:beeline, :get_stream_position)

    [{:get_stream_position, get_stream_position} | opts]
  end

  def add_default_opt({:auto_subscribe?, nil}, opts, _all_opts) do
    auto_subscribe? = Application.fetch_env!(:beeline, :auto_subscribe?)

    [{:auto_subscribe?, auto_subscribe?} | opts]
  end

  def add_default_opt({:producers, producers}, opts, all_opts) do
    producers =
      producers
      |> Enum.with_index()
      |> Enum.map(fn {producer, index} ->
        Enum.reduce(producer, [], &add_default_producer_opt(&1, &2, index, all_opts))
      end)

    [{:producers, producers} | opts]
  end

  def add_default_opt({k, v}, opts, _all_opts), do: [{k, v} | opts]

  def add_default_producer_opt({:name, nil}, opts, index, all_opts) do
    name = Module.concat(all_opts[:name], "Producer_#{index}")

    [{:name, name} | opts]
  end

  def add_default_producer_opt({k, v}, opts, _index, _all_opts), do: [{k, v} | opts]

  @doc """
  Restarts the GenStage pipeline supervision tree for the given Beeline
  topology

  This can be useful for manual intervention by a human operator in a remote
  console session, if the GenStage pipeline crashes and exceeds the retry
  limits.

  ## Examples

      iex> Beeline.restart_pipeline(MyEventHandler)
      :ok
  """
  @spec restart_pipeline(module()) :: :ok | {:error, term()}
  def restart_pipeline(beeline) do
    GenServer.call(beeline, :restart_pipeline)
  end

  # coveralls-ignore-start
  @doc false
  def validate_schema(opts, schema) do
    with {:error, reason} <- NimbleOptions.validate(opts, schema) do
      {:error, reason.message}
    end
  end

  # coveralls-ignore-stop
end
