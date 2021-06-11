defmodule Beeline.Topology do
  @moduledoc false

  @behaviour GenServer

  alias __MODULE__.StageSupervisor

  defstruct [:supervisor_pid, :config]

  def start_link(config) do
    GenServer.start_link(__MODULE__, config,
      name: Module.concat(config.name, "Topology")
    )
  end

  @impl GenServer
  def init(config) do
    {:ok, supervisor_pid} = spawn_supervisor(config)

    {:ok, %__MODULE__{supervisor_pid: supervisor_pid, config: config}}
  end

  @impl GenServer
  def handle_call(:restart_stages, _from, state) do
    parent = state.supervisor_pid
    target = Module.concat(state.config.name, "StageSupervisor")

    spec =
      state.config
      |> StageSupervisor.child_spec()
      |> Supervisor.child_spec(id: StageSupervisor.name(state.config))

    # coveralls-ignore-start
    result =
      with :ok <- Supervisor.terminate_child(parent, target),
           {:ok, _child} <- Supervisor.start_child(parent, spec) do
        :ok
      else
        {:ok, _child, _info} ->
          :ok

        error ->
          error
      end

    # coveralls-ignore-stop

    {:reply, result, state}
  end

  def handle_call({:test_events, events}, _from, state) do
    producer =
      state.config.producers
      |> get_in([Access.all(), Access.elem(1), Access.key(:name)])
      |> Enum.random()

    events = Enum.map(events, &Beeline.as_subscription_event(&1, producer))

    GenServer.cast(producer, {:events, events})

    {:reply, :ok, state}
  end

  def spawn_supervisor(config) do
    health_checkers =
      if config.spawn_health_checkers? do
        config.producers
        |> Enum.map(fn {_key, producer} ->
          {HealthChecker.StreamPosition,
           event_listener: producer.name,
           get_current_stream_position: get_stream_position(config, producer),
           get_latest_stream_position: get_latest_stream_position(producer)}
        end)
      else
        []
      end

    children =
      health_checkers ++
        [
          Supervisor.child_spec({StageSupervisor, config},
            id: StageSupervisor.name(config)
          )
        ]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Module.concat(config.name, Supervisor)
    )
  end

  @spec get_stream_position(Keyword.t(), Keyword.t()) ::
          (() -> non_neg_integer() | -1)
  defp get_stream_position(config, producer) do
    case config.get_stream_position do
      {m, f, a} ->
        fn -> apply(m, f, [producer.name | a]) end

      # coveralls-ignore-start
      function when is_function(function, 1) ->
        fn -> function.(producer.name) end

      nil ->
        raise ArgumentError,
          message:
            "could not determine the " <>
              "`:get_stream_position` function for Beeline #{inspect(config.name)}"

        # coveralls-ignore-stop
    end
  end

  @spec get_latest_stream_position(Keyword.t()) ::
          (() -> non_neg_integer() | -1)
  defp get_latest_stream_position(producer) do
    fn ->
      Beeline.EventStoreDB.latest_event_number(
        producer.adapter,
        producer.connection,
        producer.stream_name
      )
    end
  end
end
