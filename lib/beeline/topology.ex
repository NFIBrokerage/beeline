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
    spec = StageSupervisor.child_spec(state.config)

    # coveralls-ignore-start
    result =
      with :ok <- Supervisor.terminate_child(parent, StageSupervisor),
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
        Enum.map(config.producers, fn {key, producer} ->
          Beeline.HealthChecker.child_spec({config, key, producer})
        end)
      else
        []
      end

    children = health_checkers ++ [StageSupervisor.child_spec(config)]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Module.concat(config.name, Supervisor)
    )
  end
end
