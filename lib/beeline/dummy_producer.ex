defmodule Beeline.DummyProducer do
  @moduledoc """
  A GenStage producer built for testing beeline topologies

  This producer can be used either by setting the producer option `:adapter`
  to `:dummy` or by setting the `:test_mode?` option to `true`.

  This producer emits events which arrive via `GenServer.cast/2`. Use
  `Beeline.test_events/2` to send events to a topology's dummy producer.
  """

  use GenStage

  @doc false
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts, name: opts[:name])
  end

  @doc false
  @impl GenStage
  def init(_opts) do
    {:producer, nil}
  end

  @doc false
  @impl GenStage
  def handle_cast({:events, events}, state) when is_list(events) do
    {:noreply, events, state}
  end

  @doc false
  @impl GenStage
  def handle_demand(_demand, state), do: {:noreply, [], state}
end
