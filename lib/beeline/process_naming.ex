defmodule Beeline.ProcessNaming do
  @moduledoc false

  defmodule Guards do
    @moduledoc false
    # coveralls-ignore-start
    defguard is_beeline_name(name)
             when is_atom(name) or
                    (is_tuple(name) and elem(name, 0) == :global and
                       tuple_size(name) == 2) or
                    (is_tuple(name) and elem(name, 0) == :via and
                       elem(name, 1) == Registry and tuple_size(name) == 3)

    # coveralls-ignore-stop
  end

  # Provides common logic for standard OTP server_name types.
  def(name(%Beeline.Config{name: name}, appended_name)) do
    name(name, appended_name)
  end

  def name(base_name, appended_name) when is_atom(base_name) do
    Module.concat(base_name, appended_name)
  end

  def name({:global, base_name}, appended_name) do
    {:global, {base_name, appended_name}}
  end

  def name({:via, Registry, {registry_name, base_name}}, appended_name) do
    {:via, Registry, {registry_name, {base_name, appended_name}}}
  end
end
