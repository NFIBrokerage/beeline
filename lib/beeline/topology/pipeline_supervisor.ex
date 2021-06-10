defmodule Beeline.Topology.PipelineSupervisor do
  @moduledoc false
  # a supervisor for the GenStage pipeline

  # Note the restart-temporary and strategy-one-for-all options at play
  # here: if consumer crashes, we want the producer to restart as well.
  # If we don't enforce that relationship and the consumer crashes, the
  # restart strategy of the consumer genserver will attach a new consumer
  # to the producer and the failure event will be effectively skipped.
  # The restart strategy ensures that this supervisor doesn't kill its
  # supervisor when the GenStage pipeline is stuck on an event. This allows
  # the service to stay up-right and allows an operator to perform manual
  # intervention without adjusting the autosubscribe flag.

  use Supervisor,
    restart: :temporary

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: Module.concat(opts[:name], PipelineSupervisor))
  end

  @impl Supervisor
  def init(opts) do
    producer_specs =
      Enum.map(opts[:producers], fn producer ->
        {producer_module(opts[:adapter]), producer_opts(opts, producer)}
      end)

    children = producer_specs ++ [{opts[:module], consumer_opts(opts)}]

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp producer_module(:kelvin), do: Kelvin.InOrderSubscription
  defp producer_module(:volley), do: Volley.InOrderSubscription
  defp producer_module(:dummy), do: Beeline.DummyProducer

  defp producer_opts(opts, producer) do
    [
      name: producer[:name],
      stream_name: producer[:stream_name],
      connection: producer[:connection],
      restore_stream_position!: opts[:get_stream_position],
      subscribe_on_init?: opts[:subscribe_on_init?],
      subscribe_after: opts[:subscribe_after]
    ]
  end

  defp consumer_opts(opts) do
    Keyword.put(opts, :name, Module.concat(opts[:name], Consumer))
  end
end