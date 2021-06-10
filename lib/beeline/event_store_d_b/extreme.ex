defmodule Beeline.EventStoreDB.Extreme do
  @moduledoc false
  # functions for working with EventStoreDB over Extreme

  alias Extreme.Messages.{ReadStreamEvents, ReadStreamEventsCompleted}

  def latest_event_number(conn, stream) do
    read_stream_events_msg =
      ReadStreamEvents.new(
        event_stream_id: stream,
        from_event_number: 1,
        max_count: 1,
        resolve_link_tos: false,
        require_master: false
      )

    case conn.execute(read_stream_events_msg) do
      # coveralls-ignore-start
      {:ok, %ReadStreamEventsCompleted{last_event_number: number}} ->
        number

      # coveralls-ignore-stop

      {:error, :no_stream,
       %ReadStreamEventsCompleted{last_event_number: number}} ->
        number
    end
  end
end
