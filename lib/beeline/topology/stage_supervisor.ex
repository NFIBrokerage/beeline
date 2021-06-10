defmodule Beeline.Topology.StageSupervisor do
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

  alias Beeline.Topology.{Producer, Consumer}

  use Supervisor,
    restart: :temporary

  def name(opts) when is_list(opts) do
    name(opts[:name])
  end

  def name(base_name) when is_atom(base_name) do
    Module.concat(base_name, "StageSupervisor")
  end

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: name(opts))
  end

  @impl Supervisor
  def init(opts) do
    producer_specs =
      Enum.map(opts[:producers], fn {key, producer} ->
        Supervisor.child_spec(
          {producer_module(producer[:adapter]), producer_opts(opts, producer)},
          id: {Producer, key}
        )
      end)

    children = producer_specs ++ [
      %{
        id: opts[:module],
        start: {Consumer, :start_link, [consumer_opts(opts)]},
        restart: :permanent,
        type: :worker
      }
    ]

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
      restore_stream_position!:
        wrap_function(opts[:get_stream_position], producer[:name]),
      subscribe_on_init?:
        wrap_function(opts[:auto_subscribe?], producer[:name]),
      subscribe_after: Enum.random(3_000..5_000)
    ]
  end

  defp wrap_function({m, f, a}, producer_name) do
    fn -> apply(m, f, [producer_name | a]) end
  end

  defp wrap_function(function, producer_name) when is_function(function, 1) do
    fn -> function.(producer_name) end
  end

  defp consumer_opts(opts) do
    Keyword.merge(opts, name: Module.concat(opts[:name], Consumer))
  end
end
