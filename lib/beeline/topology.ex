defmodule Beeline.Topology do
  @moduledoc false

  @behaviour GenServer

  alias __MODULE__.{PipelineSupervisor}

  defstruct [:supervisor_pid, :config]

  def start_link(module, opts) do
    GenServer.start_link(__MODULE__, {module, opts}, Keyword.take(opts, [:name]))
  end

  @impl GenServer
  def init({module, opts}) do
    {:ok, supervisor_pid} = spawn_supervisor(opts)

    {:ok, %__MODULE__{supervisor_pid: supervisor_pid, config: opts}}
  end

  def spawn_supervisor(opts) do
    opts
    |> children()
    |> Supervisor.start_link(strategy: :one_for_one)
  end

  def children(opts) do
    health_checkers =
      Enum.map(opts[:producers], fn producer ->
        {HealthChecker.StreamPosition,
          event_listener: producer[:name],
          get_current_stream_position: get_stream_position(producer),
          get_latest_stream_position: get_latest_stream_position(producer)}
      end)

    health_checkers ++ [{PipelineSupervisor, opts}]
  end

  @spec get_stream_position(Keyword.t()) :: (-> non_neg_integer | -1)
  def get_stream_position(producer) do
    case producer[:get_stream_position] do
      {m, f, a} ->
        fn -> apply(m, f, [producer[:name] | a]) end

      function when is_function(function, 1) ->
        fn -> function.(producer[:name]) end
    end
  end

  @spec get_latest_stream_position(Keyword.t()) :: (-> non_neg_integer | -1)
  def get_latest_stream_position(producer) do
    fn ->
      Beeline.EventStoreDB.latest_event_number(
        producer[:adapter],
        producer[:connection],
        producer[:stream_name]
      )
    end
  end
end
