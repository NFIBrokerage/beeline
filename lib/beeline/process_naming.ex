defmodule Beeline.ProcessNaming do
  @moduledoc false

  # Provides common logic for standard OTP server_name types.

  def name(%Beeline.Config{name: name}, appended_name) do
    name(name, appended_name)
  end

  def name(base_name, appended_name) when is_atom(base_name) do
    Module.concat(base_name, appended_name)
  end

  def name({:global, base_name}, appended_name) when is_atom(base_name) do
    {:global, Module.concat(base_name, appended_name)}
  end

  def name({:via, registry, base_name}, appended_name)
      when is_binary(base_name) do
    {:via, registry, [base_name: base_name, name: appended_name]}
  end
end
