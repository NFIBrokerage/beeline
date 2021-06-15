defmodule Beeline.EventStoreDB do
  @moduledoc false

  alias __MODULE__.{Extreme, Spear}

  @callback latest_event_number(conn :: atom(), stream :: String.t()) ::
              non_neg_integer() | -1
  @callback stream_position({atom(), map()} | struct()) ::
              non_neg_integer() | -1

  # EventStoreDB interaction functions

  def latest_event_number(:kelvin, conn, stream) do
    Extreme.latest_event_number(conn, stream)
  end

  def latest_event_number(:volley, conn, stream) do
    Spear.latest_event_number(conn, stream)
  end

  # coveralls-ignore-start
  def latest_event_number(_producer, _conn, _stream) do
    -1
  end

  # coveralls-ignore-stop

  # extreme events arrive as two-tuples
  def decode_event({_producer, subscription_event}) do
    Jason.decode!(subscription_event.event.data, keys: :atoms)
  end

  # and spear events as %Spear.Event{} structs
  def decode_event(spear_event) do
    Spear.decode_event(spear_event)
  end

  def stream_position({_producer, subscription_event}) do
    Extreme.stream_position(subscription_event)
  end

  def stream_position(spear_event) do
    Spear.stream_position(spear_event)
  end

  def producer({producer, _subscription_event}), do: producer
  def producer(spear_event), do: spear_event.metadata.producer
end
