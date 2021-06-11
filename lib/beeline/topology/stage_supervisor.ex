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

  @behaviour Supervisor

  alias Beeline.Topology.{Producer, Consumer}

  def child_spec(config) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [config]},
      restart: :temporary,
      type: :supervisor
    }
  end

  def name(%Beeline.Config{name: name}) do
    name(name)
  end

  def name(base_name) when is_atom(base_name) do
    Module.concat(base_name, "StageSupervisor")
  end

  def start_link(config) do
    Supervisor.start_link(__MODULE__, config, name: name(config))
  end

  @impl Supervisor
  def init(config) do
    producer_specs =
      Enum.map(config.producers, fn {key, producer} ->
        Supervisor.child_spec(
          {producer_module(producer.adapter, config.test_mode?),
           producer_opts(config, producer)},
          id: {Producer, key}
        )
      end)

    children =
      producer_specs ++
        [
          %{
            id: config.module,
            start: {Consumer, :start_link, [config]},
            restart: :permanent,
            type: :worker
          }
        ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp producer_module(_, true = _test_mode?), do: Beeline.DummyProducer
  defp producer_module(:kelvin, _), do: Kelvin.InOrderSubscription
  defp producer_module(:volley, _), do: Volley.InOrderSubscription
  defp producer_module(:dummy, _), do: Beeline.DummyProducer

  defp producer_opts(config, producer) do
    subscribe_after =
      if config.test_mode? do
        0
      else
        subscribe_after(config.subscribe_after)
      end

    [
      name: producer.name,
      stream_name: producer.stream_name,
      connection: producer.connection,
      restore_stream_position!: get_stream_position(config, producer),
      subscribe_on_init?: wrap_function(config.auto_subscribe?, producer.name),
      subscribe_after: subscribe_after
    ]
  end

  # coveralls-ignore-start
  defp wrap_function({m, f, a}, producer_name) do
    fn -> apply(m, f, [producer_name | a]) end
  end

  defp wrap_function(function, producer_name) when is_function(function, 1) do
    fn -> function.(producer_name) end
  end

  defp get_stream_position(config, producer) do
    case config.get_stream_position do
      {m, f, a} ->
        fn ->
          apply(m, f, [producer.name | a])
          |> default_stream_position(producer.adapter)
        end

      function when is_function(function, 1) ->
        fn ->
          function.(producer.name)
          |> default_stream_position(producer.adapter)
        end

      nil ->
        raise ArgumentError,
          message:
            "could not determine the " <>
              "`:get_stream_position` function for Beeline #{inspect(config.name)}"
    end
  end

  # defaults the uninitialized stream position to :start for the :volley
  # adapter, as spear cannot interperet a -1 stream position
  defp default_stream_position(-1, :volley), do: :start
  defp default_stream_position(position, _adapter), do: position

  defp subscribe_after({m, f, a}), do: apply(m, f, a)
  defp subscribe_after(interval) when is_integer(interval), do: interval

  # coveralls-ignore-stop
end
