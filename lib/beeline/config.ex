defmodule Beeline.Config do
  @moduledoc false

  @producer_schema Beeline.Producer.schema()

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
      type: :keyword_list,
      keys: [
        *: [
          type: :keyword_list,
          keys:
            Enum.map(@producer_schema, fn {k, v} ->
              {k, put_in(v[:doc], false)}
            end)
        ]
      ]
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
    auto_subscribe?: [
      doc: """
      A function to invoke to determine whether each producer should
      subscribe to events as it starts up. The argument passed is the
      GenServer name of the producer. If this option is not provided,
      a default will be fetched with
      `Application.fetch_env!(:beeline, :auto_subscribe?)`.
      """,
      type: {:or, [:mfa, {:fun, 1}]}
    ],
    subscribe_after: [
      doc: """
      A period in msec after initialization when each producer should
      query the `:auto_subscribe?` function.
      """,
      type: {:or, [:mfa, :non_neg_integer]},
      default: {Enum, :random, [3_000..5_000]}
    ],
    spawn_health_checkers?: [
      doc: """
      Controls whether the topology should spawn the HealthChecker children.
      It can be useful to disable this in `Mix.env() in [:dev, :test]` as the
      health checker provides little or no value in those environments and
      can produce many log lines. If this option is left blank, it will be
      gotten from the application environment defaulting to `true` with
      `Application.get_env(:beeline, :spawn_health_checkers?, true)`.
      """,
      type: {:or, [:boolean, {:in, [nil]}]},
      default: nil
    ],
    test_mode?: [
      doc: """
      Controls whether the topology should start up in test mode. In test
      mode, any adapters set in producer specs are switched out with
      the `:dummy` adapter. If this option is left blank, it will be
      gotten from the application environment defaulting to `false` with
      `Application.get_env(:beeline, :test_mode?, false)`.
      """,
      type: {:or, [:boolean, {:in, [nil]}]}
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

  # coveralls-ignore-start
  def schema, do: @schema

  # coveralls-ignore-stop

  defstruct [:module | Keyword.keys(@schema)]

  def source(opts) do
    opts =
      Keyword.keys(@schema)
      |> Enum.map(fn key -> {key, nil} end)
      |> Keyword.merge(opts)
      |> update_in([:producers, Access.all()], fn {key, producer} ->
        producer =
          Keyword.keys(@producer_schema)
          |> Enum.map(fn key -> {key, nil} end)
          |> Keyword.merge(producer)

        {key, producer}
      end)
      |> add_default_opts()
      |> update_in([:producers, Access.all()], fn {key, producer} ->
        {key, struct(Beeline.Producer, producer)}
      end)

    struct(__MODULE__, opts)
  end

  @doc false
  def add_default_opts(opts) do
    Enum.reduce(opts, [], &add_default_opt(&1, &2, opts))
  end

  @doc false
  # coveralls-ignore-start
  def add_default_opt({:get_stream_position, nil}, acc, _all_opts) do
    get_stream_position = Application.fetch_env!(:beeline, :get_stream_position)

    [{:get_stream_position, get_stream_position} | acc]
  end

  # coveralls-ignore-stop

  def add_default_opt({:auto_subscribe?, nil}, acc, _all_opts) do
    auto_subscribe? = Application.fetch_env!(:beeline, :auto_subscribe?)

    [{:auto_subscribe?, auto_subscribe?} | acc]
  end

  def add_default_opt({:spawn_health_checkers?, nil}, acc, _all_opts) do
    spawn_health_checkers? =
      Application.get_env(:beeline, :spawn_health_checkers?, true)

    [{:spawn_health_checkers?, spawn_health_checkers?} | acc]
  end

  def add_default_opt({:test_mode?, nil}, acc, _all_opts) do
    test_mode? = Application.get_env(:beeline, :test_mode?, false)

    [{:test_mode?, test_mode?} | acc]
  end

  def add_default_opt({:producers, producers}, acc, all_opts) do
    producers =
      producers
      |> Enum.map(fn {key, producer} ->
        producer =
          Enum.reduce(
            producer,
            [],
            &add_default_producer_opt(&1, &2, key, all_opts)
          )

        {key, producer}
      end)

    [{:producers, producers} | acc]
  end

  def add_default_opt({k, v}, acc, _all_opts), do: [{k, v} | acc]

  @doc false
  def add_default_producer_opt({:name, nil}, acc, key, all_opts) do
    name = Module.concat(all_opts[:name], "Producer_#{key}")

    [{:name, name} | acc]
  end

  def add_default_producer_opt({k, v}, acc, _key, _all_opts), do: [{k, v} | acc]
end
