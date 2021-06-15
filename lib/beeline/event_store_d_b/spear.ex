import Beeline.Utils, only: [if_spear: 1]

if_spear do
  defmodule Beeline.EventStoreDB.Spear do
    @moduledoc false
    # functions for working with EventStoreDB via Spear

    @behaviour Beeline.EventStoreDB

    def latest_event_number(conn, stream) do
      Spear.stream!(conn, stream,
        from: :end,
        direction: :backwards,
        chunk_size: 1
      )
      |> Enum.take(1)
      |> case do
        [event] ->
          Spear.Event.revision(event)

        # coveralls-ignore-start
        [] ->
          -1
          # coveralls-ignore-stop
      end
    end

    def decode_event(%Spear.Event{} = event) do
      atomify(event.body)
    end

    # coveralls-ignore-start
    defp atomify(map)

    defp atomify(%{__struct__: _some_module} = struct), do: struct

    defp atomify(map) when is_map(map) do
      Enum.into(map, %{}, fn {k, v} ->
        {ensure_is_atom(k), atomify(v)}
      end)
    end

    defp atomify(list) when is_list(list) do
      Enum.map(list, &atomify/1)
    end

    defp atomify(value), do: value

    defp ensure_is_atom(key) when is_binary(key), do: String.to_atom(key)
    defp ensure_is_atom(key) when is_atom(key), do: key
    # coveralls-ignore-stop

    def stream_position(event) do
      Spear.Event.revision(event)
    end
  end
end
