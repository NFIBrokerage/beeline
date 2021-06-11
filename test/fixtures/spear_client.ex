defmodule Beeline.Fixtures.SpearClient do
  @moduledoc """
  A fixture for connecting to EventStoreDB v20+ via Spear
  """

  use Spear.Client, otp_app: :beeline
end
