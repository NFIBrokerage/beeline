# Beeline

![Actions CI](https://github.com/NFIBrokerage/beeline/workflows/Actions%20CI/badge.svg)

a tool for building in-order GenStage topologies for EventStoreDB[^honeycomb-beelines]

## Installation

```elixir
def deps do
  [
    {:beeline, "~> 1.0"},
    # these are optional: add whichever adapter your project needs
    {:kelvin, "~> 1.0"},
    {:volley, "~> 1.0"}
  ]
end
```

Check out the docs here: https://hexdocs.pm/beeline

## Usage

Beeline is a [Broadway](https://github.com/dashbitco/broadway)-like library
for building in-order event-processors for EventStoreDB. Writing a Beeline
topology is roughly the same as writing a GenStage consumer but with more
configuration in the `start_link/1` function.

```elixir
defmodule MyApp.MyEventHandler do
  use Beeline

  def start_link(_opts) do
    Beeline.start_link(__MODULE__,
      name: __MODULE__,
      producers: [
        default: [
          adapter: :kelvin,
          stream_name: "$ce-BoundedContext.Aggregate",
          connection: MyApp.ExtremeClient
        ]
      ],
      get_stream_position: fn producer ->
        MyApp.Repo.get(MyApp.StreamPosition, inspect(producer))
      end
    )
  end

  @impl GenStage
  def handle_events(subscription_events, _from, state) do
    # do something with the events

    {:noreply, [], state}
  end
end
```

See the hex documentation for
[`Beeline.start_link/2`](https://hexdocs.pm/beeline/Beeline.html#start_link/2)
for the configuration schema and explanation.

## Why in-order?

Beeline champions in-order procesing because it can vastly simplify the
writing of event processing code. By forcing strictly in-order processing,
Beeline can guarantee either that all events in the stream will be handled
in-order and exactly once or that the processor will halt. This avoids any
potential concurrency dragons and allows an author to make assumptions about
ordering.

In-order subscriptions are also very inexpensive on the EventStoreDB: stream
reads and basic subscriptions cost next to nothing in terms of resources.

## What if I want more complex topologies?

If you want multiple consumers or a pipeline with producer-consumers, you can
write the topology using straight-up GenStage, Kelvin/Volley, and a
Supervisor. Beeline is meant to be a convenience library for gluing together
the most common processes and configuration for a simple in-order subscription.

If you want to handle events out of order with concurrency and batching,
Broadway is an excellent choice when paired with the EventStoreDB persistent
subscriptions feature. Broadway's acknowledgement system fits perfectly with
the persistent subscriptions API.

[^honeycomb-beelines]: not to bee confused with honeycomb beelines
