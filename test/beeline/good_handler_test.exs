defmodule Beeline.GoodHandlerTest do
  @moduledoc """
  A test of the fixture in `test/fixtures/good_handler.ex`
  """

  use ExUnit.Case, async: true

  @moduletag :capture_log

  alias Beeline.Fixtures.{
    ExtremeClient,
    SpearClient,
    GoodHandler
  }

  setup_all do
    start_supervised!(ExtremeClient)
    start_supervised!(SpearClient)

    :ok
  end

  setup do
    [
      grpc_stream_name: "Beeline.Test-#{Spear.Event.uuid_v4()}",
      tcp_stream_name: "Beeline.Test-#{Spear.Event.uuid_v4()}"
    ]
  end

  describe "given events are in some streams for the EventStoreDBs" do
    setup c do
      :ok =
        Stream.repeatedly(fn ->
          Spear.Event.new("beeline_test", %{adapter: "spear"})
        end)
        |> Stream.take(3)
        |> SpearClient.append(c.grpc_stream_name)

      Stream.repeatedly(fn -> %{adapter: "extreme"} end)
      |> Enum.take(3)
      |> ExtremeClient.append!(c.tcp_stream_name)

      :ok
    end

    test "the consumer will store these events in state and increment stream positions",
         c do
      self_pid = self()

      :telemetry.attach(
        "health-checker-stream-position-beeline-tester",
        [:health_checker, :stream_position, :stop],
        fn event, measurements, metadata, _state ->
          send(self_pid, {:health_check, event, measurements, metadata})
        end,
        :ok
      )

      start_supervised!(
        {GoodHandler,
         grpc_stream_name: c.grpc_stream_name,
         tcp_stream_name: c.tcp_stream_name}
      )

      spawn(fn -> check_position(GoodHandler.Producer_grpc, 3, self_pid) end)
      spawn(fn -> check_position(GoodHandler.Producer_tcp, 3, self_pid) end)

      assert_receive {:done, GoodHandler.Producer_grpc}, 5000
      assert_receive {:done, GoodHandler.Producer_tcp}, 5000

      assert GoodHandler.get_stream_position(GoodHandler.Producer_grpc) == 2
      assert GoodHandler.get_stream_position(GoodHandler.Producer_tcp) == 2

      state = GoodHandler.get_state()

      assert length(state.events) == 6

      assert_receive {:health_check, _event, _measurements,
                      %{
                        local_event_number: 2,
                        latest_event_number: 2,
                        event_listener: GoodHandler.Producer_tcp
                      }},
                     15_000

      assert_receive {:health_check, _event, _measurements,
                      %{
                        local_event_number: 2,
                        latest_event_number: 2,
                        event_listener: GoodHandler.Producer_grpc
                      }},
                     15_000
    end
  end

  defp check_position(producer, event_count, test_pid) do
    if GoodHandler.get_stream_position(producer) + 1 >= event_count do
      send(test_pid, {:done, producer})
    else
      Process.sleep(100)
      check_position(producer, event_count, test_pid)
    end
  end
end
