defmodule Beeline.GlobalProducerFixture do
  @moduledoc """
  A fixture event handler that subscribes to a dummy producer
  """

  use Beeline

  def start_link(test_proc) do
    Beeline.start_link(__MODULE__,
      name: {:global, __MODULE__},
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

defmodule Beeline.GlobalRegistryTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  @producer_id {Beeline.Topology.Producer, :default}
  @fixture Beeline.GlobalProducerFixture

  setup do
    [beeline_pid: start_supervised!({@fixture, self()})]
  end

  test "the dummy handler can handle events" do
    events = [%{foo: "bar"}, %{foo: "bar"}, %{foo: "bar"}]

    :ok = Beeline.test_events(events, {:global, @fixture})

    assert_receive {:event, event_a}
    assert_receive {:event, event_b}
    assert_receive {:event, event_c}

    assert [event_a, event_b, event_c] == events
  end

  test "the dummy producer and handler are both restarted with restart_stages/1" do
    %{
      @producer_id => producer_pid,
      @fixture => consumer_pid
    } = stage_children()

    producer_ref = Process.monitor(producer_pid)
    consumer_ref = Process.monitor(consumer_pid)

    assert Beeline.restart_stages({:global, @fixture}) == :ok

    assert_receive {:DOWN, ^producer_ref, :process, ^producer_pid, :shutdown}
    assert_receive {:DOWN, ^consumer_ref, :process, ^consumer_pid, :shutdown}

    %{
      @producer_id => producer_pid,
      @fixture => consumer_pid
    } = stage_children()

    assert Process.alive?(producer_pid)
    assert Process.alive?(consumer_pid)
  end

  test "when the consumer raises on an event, it kills the producer as well" do
    %{
      @producer_id => producer_pid,
      @fixture => consumer_pid
    } = stage_children()

    producer_ref = Process.monitor(producer_pid)
    consumer_ref = Process.monitor(consumer_pid)

    good_event = %{foo: "bar"}
    bad_event = %{poison?: true}

    :ok = Beeline.test_events([good_event, bad_event], {:global, @fixture})

    assert_receive {:event, ^good_event}
    refute_receive {:event, ^bad_event}

    assert_receive {:DOWN, ^producer_ref, :process, ^producer_pid, :shutdown}
    assert_receive {:DOWN, ^consumer_ref, :process, ^consumer_pid, _error}

    # then the producer and consumer are restarted
    %{
      @producer_id => producer_pid,
      @fixture => consumer_pid
    } = stage_children()

    assert Process.alive?(producer_pid)
    assert Process.alive?(consumer_pid)
  end

  defp stage_children do
    {:global, @fixture}
    |> Beeline.ProcessNaming.name(StageSupervisor)
    |> Supervisor.which_children()
    |> Enum.into(%{}, fn {id, pid, _, _} -> {id, pid} end)
  end
end
