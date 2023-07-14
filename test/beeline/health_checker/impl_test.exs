defmodule Beeline.HealthChecker.ImplTest do
  use ExUnit.Case, async: true

  alias Beeline.HealthChecker
  alias Beeline.HealthChecker.Impl
  require Logger

  describe "poll_producer/1" do
    test "when everything was up-to-date but now it's falling behind" do
      {handler_name, ref} = _attach_handlers_to_telemetry()

      new_state =
        %{
          up_to_date?: true,
          current_position: 1,
          get_stream_position: fn -> 2 end,
          get_head_position: fn -> 12 end
        }
        |> _get_state()
        |> Impl.poll_producer()

      assert %HealthChecker{
               status: :falling_behind,
               up_to_date?: false,
               current_position: 2,
               head_position: 12
             } = new_state

      assert_receive {^ref,
                      {:telemetry_event,
                       %{
                         status: :falling_behind,
                         prior_position: _,
                         current_position: 2,
                         head_position: 12
                       }}}

      :telemetry.detach(handler_name)
    end

    test "when listener is stuck within acceptable delay" do
      {handler_name, ref} = _attach_handlers_to_telemetry()

      new_state =
        %{
          up_to_date?: true,
          acceptable_behind_by: 5,
          get_stream_position: fn -> 2 end,
          get_head_position: fn -> 5 end
        }
        |> _get_state()
        |> Impl.poll_producer()

      assert %HealthChecker{
               status: :up_to_date,
               up_to_date?: true,
               current_position: 2,
               head_position: 5
             } = new_state

      assert_receive {^ref,
                      {:telemetry_event,
                       %{
                         status: :up_to_date,
                         prior_position: _,
                         current_position: 2,
                         head_position: 5
                       }}}

      # second time behind and without progress -> not ok
      new_state = Impl.poll_producer(new_state)

      assert %HealthChecker{
               status: :stuck,
               up_to_date?: false,
               current_position: 2,
               head_position: 5
             } = new_state

      assert_receive {^ref,
                      {:telemetry_event,
                       %{
                         status: :stuck,
                         prior_position: _,
                         current_position: 2,
                         head_position: 5
                       }}}

      :telemetry.detach(handler_name)
    end

    test "when listener is progressing within acceptable delay" do
      {handler_name, ref} = _attach_handlers_to_telemetry()

      new_state =
        %{
          up_to_date?: true,
          acceptable_behind_by: 5,
          get_stream_position: fn -> 2 end,
          get_head_position: fn -> 5 end
        }
        |> _get_state()
        |> Impl.poll_producer()

      assert %HealthChecker{
               status: :up_to_date,
               up_to_date?: true,
               current_position: 2,
               head_position: 5
             } = new_state

      assert_receive {^ref,
                      {:telemetry_event,
                       %{
                         status: :up_to_date,
                         prior_position: _,
                         current_position: 2,
                         head_position: 5
                       }}}

      # second time behind within acceptable delay, but with some progress -> ok
      new_state =
        new_state
        |> Map.merge(%{
          get_stream_position: fn -> 3 end,
          get_head_position: fn -> 7 end
        })
        |> Impl.poll_producer()

      assert %HealthChecker{
               status: :up_to_date,
               up_to_date?: true,
               current_position: 3,
               head_position: 7
             } = new_state

      assert_receive {^ref,
                      {:telemetry_event,
                       %{
                         status: :up_to_date,
                         prior_position: 2,
                         current_position: 3,
                         head_position: 7
                       }}}

      :telemetry.detach(handler_name)
    end

    test "when listener wasn't up-to-date but now it is in acceptable delay" do
      {handler_name, ref} = _attach_handlers_to_telemetry()

      new_state =
        %{
          up_to_date?: false,
          acceptable_behind_by: 5,
          get_stream_position: fn -> 2 end,
          get_head_position: fn -> 3 end
        }
        |> _get_state()
        |> Impl.poll_producer()

      assert %HealthChecker{
               status: :caught_up,
               up_to_date?: true,
               current_position: 2,
               head_position: 3
             } = new_state

      assert_receive {^ref,
                      {:telemetry_event,
                       %{
                         status: :caught_up,
                         prior_position: _,
                         current_position: 2,
                         head_position: 3
                       }}}

      :telemetry.detach(handler_name)
    end

    test "when listener was up-to-date but now it's not and had no progress in processing" do
      {handler_name, ref} = _attach_handlers_to_telemetry()

      new_state =
        %{
          up_to_date?: true,
          current_position: 2,
          head_position: 2,
          get_stream_position: fn -> 2 end,
          get_head_position: fn -> 12 end
        }
        |> _get_state()
        |> Impl.poll_producer()

      assert %HealthChecker{
               status: :stuck,
               up_to_date?: false,
               current_position: 2,
               head_position: 12
             } = new_state

      assert_receive {^ref,
                      {:telemetry_event,
                       %{
                         status: :stuck,
                         prior_position: 2,
                         current_position: 2,
                         head_position: 12
                       }}}

      :telemetry.detach(handler_name)
    end

    test "when listener was not and is not still up-to-date and without progress in processing" do
      {handler_name, ref} = _attach_handlers_to_telemetry()

      new_state =
        %{
          up_to_date?: false,
          current_position: 2,
          head_position: 12,
          get_stream_position: fn -> 2 end,
          get_head_position: fn -> 12 end
        }
        |> _get_state()
        |> Impl.poll_producer()

      assert %HealthChecker{
               status: :stuck,
               up_to_date?: false,
               current_position: 2,
               head_position: 12
             } = new_state

      assert_receive {^ref,
                      {:telemetry_event,
                       %{
                         status: :stuck,
                         prior_position: 2,
                         current_position: 2,
                         head_position: 12
                       }}}

      :telemetry.detach(handler_name)
    end

    test "when listener was not and is not still up-to-date with but progressing with the same gap" do
      {handler_name, ref} = _attach_handlers_to_telemetry()

      new_state =
        %{
          up_to_date?: false,
          current_position: 2,
          head_position: 12,
          get_stream_position: fn -> 5 end,
          get_head_position: fn -> 15 end
        }
        |> _get_state()
        |> Impl.poll_producer()

      assert %HealthChecker{
               status: :falling_behind,
               up_to_date?: false,
               current_position: 5,
               head_position: 15
             } = new_state

      assert_receive {^ref,
                      {:telemetry_event,
                       %{
                         status: :falling_behind,
                         prior_position: 2,
                         current_position: 5,
                         head_position: 15
                       }}}

      :telemetry.detach(handler_name)
    end

    test "when listener was not and is not still up-to-date but progressing with greater gap" do
      {handler_name, ref} = _attach_handlers_to_telemetry()

      new_state =
        %{
          up_to_date?: false,
          current_position: 2,
          head_position: 12,
          get_stream_position: fn -> 3 end,
          get_head_position: fn -> 16 end
        }
        |> _get_state()
        |> Impl.poll_producer()

      assert %HealthChecker{
               status: :falling_behind_more,
               up_to_date?: false,
               current_position: 3,
               head_position: 16
             } = new_state

      assert_receive {^ref,
                      {:telemetry_event,
                       %{
                         status: :falling_behind_more,
                         prior_position: 2,
                         current_position: 3,
                         head_position: 16
                       }}}

      :telemetry.detach(handler_name)
    end

    test "when listener was not and is not still up-to-date with less gap" do
      {handler_name, ref} = _attach_handlers_to_telemetry()

      new_state =
        %{
          up_to_date?: false,
          current_position: 2,
          head_position: 12,
          get_stream_position: fn -> 5 end,
          get_head_position: fn -> 12 end
        }
        |> _get_state()
        |> Impl.poll_producer()

      assert %HealthChecker{
               status: :catching_up,
               up_to_date?: false,
               current_position: 5,
               head_position: 12
             } = new_state

      assert_receive {^ref,
                      {:telemetry_event,
                       %{
                         status: :catching_up,
                         prior_position: 2,
                         current_position: 5,
                         head_position: 12
                       }}}

      :telemetry.detach(handler_name)
    end
  end

  defp _get_state(%{} = overrides) do
    %HealthChecker{
      auto_subscribe?: fn -> true end
    }
    |> Map.merge(overrides)
  end

  defp _attach_handlers_to_telemetry() do
    {test_name, _arity} = __ENV__.function
    handler_name = to_string(test_name)
    ref = make_ref()

    parent = self()

    :telemetry.attach_many(
      handler_name,
      [
        [:beeline, :health_check, :stop]
      ],
      __MODULE__.telemetry_handler(parent, ref),
      nil
    )

    {handler_name, ref}
  end

  def telemetry_handler(parent, ref) do
    fn
      [:beeline, :health_check, :stop], _measurements, meta, _config ->
        send(parent, {ref, {:telemetry_event, meta}})

      event, measurements, meta, _config ->
        Logger.error(
          "Unexpected telemetry call: #{inspect([event, measurements, meta])}"
        )
    end
  end
end
