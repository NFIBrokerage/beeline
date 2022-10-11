defmodule Beeline.DummyNameFixture do
  @moduledoc """
  A fixture event handler that subscribes to a dummy producer
  """

  use Beeline

  def start_link(%{name: name, proc: test_proc}) do
    Beeline.start_link(__MODULE__,
      name: name,
      producers: [
        default: [
          adapter: :dummy,
          connection: nil,
          stream_name: "dummy-stream"
        ]
      ],
      spawn_health_checkers?: false,
      # these options don't matter in test mode
      auto_subscribe?: fn _producer -> false end,
      get_stream_position: fn _producer -> -1 end,
      context: test_proc
    )
  end

  @impl GenStage
  def handle_events([subscription_event], _from, test_proc) do
    event = Beeline.decode_event(subscription_event)

    if match?(%{poison?: true}, event) do
      raise "inconceivable!"
    end

    send(test_proc, {:event, event})

    {:noreply, [], test_proc}
  end
end
