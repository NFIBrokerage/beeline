defmodule Beeline.DummyProducerTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  @producer_id {Beeline.Topology.Producer, :default}
  @fixture Beeline.DummyNameFixture

  setup do
    [
      beeline_pid:
        start_supervised!({@fixture, %{name: @fixture, proc: self()}})
    ]
  end

  test "the dummy handler can handle events" do
    events = [%{foo: "bar"}, %{foo: "bar"}, %{foo: "bar"}]

    :ok = Beeline.test_events(events, @fixture)

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

    assert Beeline.restart_stages(@fixture) == :ok

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

    :ok = Beeline.test_events([good_event, bad_event], @fixture)

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
    @fixture.StageSupervisor
    |> Supervisor.which_children()
    |> Enum.into(%{}, fn {id, pid, _, _} -> {id, pid} end)
  end
end
