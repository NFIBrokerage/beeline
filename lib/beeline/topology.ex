defmodule Beeline.Topology do
  @moduledoc false

  @behaviour GenServer

  alias __MODULE__.StageSupervisor

  defstruct [:supervisor_pid, :config]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @impl GenServer
  def init(opts) do
    {:ok, supervisor_pid} = spawn_supervisor(opts)

    {:ok, %__MODULE__{supervisor_pid: supervisor_pid, config: opts}}
  end

  @impl GenServer
  def handle_call(:restart_stages, _from, state) do
    parent = state.supervisor_pid
    target = Module.concat(state.config[:name], "StageSupervisor")

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
      state.config
      |> get_in([:producers, Access.all(), Access.elem(1), :name])
      |> Enum.random()

    events = Enum.map(events, &Beeline.as_subscription_event(&1, producer))

    GenServer.cast(producer, {:events, events})

    {:reply, :ok, state}
  end

  def spawn_supervisor(opts) do
    health_checkers =
      if opts[:spawn_health_checkers?] do
        opts[:producers]
        |> Enum.map(fn {_key, producer} ->
          {HealthChecker.StreamPosition,
           event_listener: producer[:name],
           get_current_stream_position: get_stream_position(opts, producer),
           get_latest_stream_position: get_latest_stream_position(producer)}
        end)
      else
        []
      end

    children =
      health_checkers ++
        [
          Supervisor.child_spec({StageSupervisor, opts},
            id: StageSupervisor.name(opts)
          )
        ]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Module.concat(opts[:name], Supervisor)
    )
  end

  @spec get_stream_position(Keyword.t(), Keyword.t()) ::
          (() -> non_neg_integer() | -1)
  defp get_stream_position(opts, producer) do
    case opts[:get_stream_position] do
      {m, f, a} ->
        fn -> apply(m, f, [producer[:name] | a]) end

      # coveralls-ignore-start
      function when is_function(function, 1) ->
        fn -> function.(producer[:name]) end

      nil ->
        raise ArgumentError,
          message:
            "could not determine the " <>
              "`:get_stream_position` function for Beeline #{inspect(opts[:name])}"

        # coveralls-ignore-stop
    end
  end

  @spec get_latest_stream_position(Keyword.t()) ::
          (() -> non_neg_integer() | -1)
  defp get_latest_stream_position(producer) do
    fn ->
      Beeline.EventStoreDB.latest_event_number(
        producer[:adapter],
        producer[:connection],
        producer[:stream_name]
      )
    end
  end
end
