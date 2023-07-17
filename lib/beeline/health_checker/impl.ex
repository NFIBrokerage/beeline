defmodule Beeline.HealthChecker.Impl do
  @moduledoc false
  alias Beeline.HealthChecker

  def poll_producer(%HealthChecker{} = state) do
    metadata = %{
      producer: state.producer,
      stream_name: state.stream_name,
      hostname: state.hostname,
      interval: state.interval,
      drift: state.drift,
      measurement_time: DateTime.utc_now(),
      prior_position: state.current_position,
      auto_subscribe: state.auto_subscribe?.()
    }

    :telemetry.span(
      [:beeline, :health_check],
      metadata,
      fn ->
        situation = _analyse_situation(state)

        metadata =
          Map.merge(metadata, %{
            current_position: situation.current_listener_position,
            head_position: situation.current_stream_position,
            alive?: _alive?(state.producer),
            status: situation.status
          })

        state = %HealthChecker{
          state
          | current_position: situation.current_listener_position,
            head_position: situation.current_stream_position,
            up_to_date?: situation.up_to_date?,
            status: situation.status
        }

        {state, metadata}
      end
    )
  end

  # credo:disable-for-next-line
  defp _analyse_situation(state) do
    previous_listener_position = state.current_position
    current_listener_position = state.get_stream_position.()

    previous_stream_position = state.head_position
    current_stream_position = state.get_head_position.()

    previously_behind_by = previous_stream_position - previous_listener_position
    behind_by = current_stream_position - current_listener_position

    was_up_to_date? = state.up_to_date?

    is_up_to_date? =
      behind_by == 0 ||
        (behind_by < state.acceptable_behind_by &&
           current_listener_position > previous_listener_position)

    had_progress? = current_listener_position != previous_listener_position

    status =
      case {was_up_to_date?, is_up_to_date?, had_progress?,
            behind_by - previously_behind_by} do
        {true, true, _, _} -> :up_to_date
        {_, false, false, _} -> :stuck
        {true, false, _, _} -> :falling_behind
        {false, true, _, _} -> :caught_up
        {false, false, _, 0} -> :falling_behind
        {false, false, _, n} when n < 0 -> :catching_up
        {false, false, _, _} -> :falling_behind_more
      end

    %{
      current_listener_position: current_listener_position,
      current_stream_position: current_stream_position,
      up_to_date?: is_up_to_date?,
      status: status
    }
  end

  defp _alive?(producer) do
    case GenServer.whereis(producer) do
      nil -> false
      pid -> Process.alive?(pid)
    end
  end
end
