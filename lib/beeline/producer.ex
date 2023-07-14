defmodule Beeline.Producer do
  @moduledoc false

  @schema [
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
      and the key in the keyword list of producers.
      """,
      type: :atom,
      default: nil
    ],
    max_demand: [
      doc: """
      The maximum number of events the consumer is allowed to request from
      this producer. This option can be configured to allow batch processing.
      """,
      type: :pos_integer,
      default: 1
    ],
    min_demand: [
      doc: """
      The minimum number of events the consumer can request at a time from
      this producer.
      """,
      type: :non_neg_integer,
      default: 0
    ]
  ]

  # coveralls-ignore-start
  def schema, do: @schema
  # coveralls-ignore-stop

  defstruct Keyword.keys(@schema)
end
