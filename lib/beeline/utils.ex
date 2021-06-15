# coveralls-ignore-start
defmodule Beeline.Utils do
  @moduledoc false

  # utilities for conditionally compiling part of this library depending on
  # whether or not the consuming application/library also includes either of
  # the adapters

  defmacro if_extreme(do: body) do
    case Code.ensure_compiled(Extreme) do
      {:module, Extreme} ->
        body

      _ ->
        quote(do: :ok)
    end
  end

  defmacro if_spear(do: body) do
    case Code.ensure_compiled(Spear) do
      {:module, Spear} ->
        body

      _ ->
        quote(do: :ok)
    end
  end
end

# coveralls-ignore-stop
