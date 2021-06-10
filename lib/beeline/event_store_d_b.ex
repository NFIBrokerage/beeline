defmodule Beeline.EventStoreDB do
  @moduledoc false

  alias __MODULE__.{Extreme, Spear}

  # EventStoreDB interaction functions

  def latest_event_number(:kelvin, conn, stream) do
    Extreme.latest_event_number(conn, stream)
  end

  def latest_event_number(:volley, conn, stream) do
    Spear.latest_event_number(conn, stream)
  end

  def latest_event_number(_producer, _conn, _stream) do
    -1
  end
end
