defmodule BeelineTest do
  use ExUnit.Case, async: true

  test "passing an invalid configuration to start_link/2 raises" do
    assert_raise ArgumentError, fn ->
      Beeline.start_link(__MODULE__, name: "hello")
    end

    assert_raise ArgumentError, fn ->
      Beeline.start_link(__MODULE__, foo: "bar")
    end
  end
end
