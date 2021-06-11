defmodule Beeline.Fixtures.ExtremeClient do
  @moduledoc """
  A fixture for connecting to EventStoreDB v5 via Extreme
  """

  use Extreme, otp_app: :beeline

  alias Extreme.Messages

  def append!(events, stream_name) do
    events =
      Enum.map(events, fn event ->
        Messages.NewEvent.new(
          event_id: Extreme.Tools.generate_uuid(),
          event_type: "beeline_test",
          data_content_type: 1,
          metadata_content_type: 1,
          data: Jason.encode!(event),
          metadata: "{}"
        )
      end)

    Messages.WriteEvents.new(
      event_stream_id: stream_name,
      expected_version: -2,
      events: events,
      require_master: false
    )
    |> execute()
    |> case do
      {:ok, %Messages.WriteEventsCompleted{result: :success}} -> :ok
    end
  end
end
