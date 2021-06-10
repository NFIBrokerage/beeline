defmodule Beeline.EventStoreDB do
  @moduledoc false

  # EventStoreDB interaction functions

  def latest_event_number(:kelvin, conn, stream) do
    # TODO
  end

  def latest_event_number(:volley, conn, stream) do
    Spear.stream!(conn, stream,
      from: :end,
      direction: :backwards,
      chunk_size: 1
    )
    |> Enum.take(1)
    |> case do
      [event] -> Spear.Event.revision(event)
      [] -> -1
    end
  end

  def latest_event_number(:dummy, _conn, _stream) do
    -1
  end
end
