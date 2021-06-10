defmodule Beeline.EventStoreDB.Spear do
  @moduledoc false
  # functions for working with EventStoreDB via Spear

  def latest_event_number(conn, stream) do
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
end
