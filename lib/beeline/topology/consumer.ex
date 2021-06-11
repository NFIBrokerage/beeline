defmodule Beeline.Topology.Consumer do
  @moduledoc false
  # a GenStage consumer
  # this consumer is defined in the same module that defines the start_link/1
  # function

  def start_link(opts) do
    GenStage.start_link(opts.module, opts, name: opts.name)
  end
end
