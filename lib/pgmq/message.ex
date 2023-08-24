defmodule Pgmq.Message do
  @enforce_keys [:id, :body]
  defstruct [:id, :body]
  @type t :: %__MODULE__{id: integer, body: term()}
end
