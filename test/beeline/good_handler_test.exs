defmodule Beeline.GoodHandlerTest do
  @moduledoc """
  A test of the fixture in `test/fixtures/good_handler.ex`
  """

  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

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
      tcp_stream_name: "Beeline.Test-#{Spear.Event.uuid_v4()}",
      self: self()
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
      _exporter = start_supervised!(Beeline.HealthChecker.Logger)

      :telemetry.attach(
        "beeline-health-checker-tester",
        [:beeline, :health_check, :stop],
        fn event, measurements, metadata, _state ->
          send(c.self, {:health_check, event, measurements, metadata})
        end,
        :ok
      )

      log =
        capture_log([level: :info], fn ->
          start_supervised!(
            {GoodHandler,
             grpc_stream_name: c.grpc_stream_name,
             tcp_stream_name: c.tcp_stream_name}
          )

          spawn(fn -> check_position(GoodHandler.Producer_grpc, 3, c.self) end)
          spawn(fn -> check_position(GoodHandler.Producer_tcp, 3, c.self) end)

          assert_receive {:done, GoodHandler.Producer_grpc}, 2000
          assert_receive {:done, GoodHandler.Producer_tcp}, 2000

          assert GoodHandler.get_stream_position(GoodHandler.Producer_grpc) == 2
          assert GoodHandler.get_stream_position(GoodHandler.Producer_tcp) == 2

          state = GoodHandler.get_state()

          assert length(state.events) == 6

          assert_receive {:health_check, _event, _measurements,
                          %{
                            current_position: 2,
                            head_position: 2,
                            producer: GoodHandler.Producer_tcp
                          }},
                         2_000

          assert_receive {:health_check, _event, _measurements,
                          %{
                            current_position: 2,
                            head_position: 2,
                            producer: GoodHandler.Producer_grpc
                          }},
                         2_000
        end)

      assert log =~ inspect(GoodHandler.Producer_tcp)
      assert log =~ inspect(GoodHandler.Producer_grpc)
      assert log =~ "caught up"
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
