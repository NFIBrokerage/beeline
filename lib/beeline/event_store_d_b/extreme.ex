import Beeline.Utils, only: [if_extreme: 1]

if_extreme do
  defmodule Beeline.EventStoreDB.Extreme do
    @moduledoc false
    # functions for working with EventStoreDB over Extreme

    @behaviour Beeline.EventStoreDB

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

      case execute(conn, read_stream_events_msg) do
        {:ok, %ReadStreamEventsCompleted{last_event_number: number}} ->
          number

        # coveralls-ignore-start
        {:error, :no_stream,
         %ReadStreamEventsCompleted{last_event_number: number}} ->
          number

          # coveralls-ignore-stop
      end
    end

    def execute(conn, message, timeout \\ 5_000) do
      Extreme.RequestManager.execute(
        conn,
        message,
        Extreme.Tools.generate_uuid(),
        timeout
      )
    end

    # coveralls-ignore-start
    def stream_position(%{link: %{event_number: position}}), do: position
    def stream_position(%{event: %{event_number: position}}), do: position

    # coveralls-ignore-stop
  end
end
