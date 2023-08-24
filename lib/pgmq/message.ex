defmodule Pgmq.Message do
  @enforce_keys [:id, :read_count, :enqueued_at, :visibility_timeout, :body]
  defstruct [:id, :read_count, :enqueued_at, :visibility_timeout, :body]

  @type t :: %__MODULE__{
          id: integer,
          body: binary,
          read_count: integer,
          enqueued_at: Date.t(),
          visibility_timeout: Date.t()
        }

  def from_row([id, read_count, enqueued_at, visibility_timeout, body]) do
    %__MODULE__{
      id: id,
      read_count: read_count,
      enqueued_at: enqueued_at,
      visibility_timeout: visibility_timeout,
      body: body
    }
  end
end
