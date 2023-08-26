defmodule Pgmq do
  @moduledoc """
  Thin wrapper over the pgmq extension

  Provides APIs for sending, reading, archiving and deleting messages.

  ### Use-macros
  You can `use Pgmq` for the convenience of having a standardized repo and less
  convoluted function calls. By defining:
  ```
    # lib/my_app/pgmq.ex
    defmodule MyApp.Pgmq do
      use Pgmq, repo: MyApp.Repo
    end
  ```

  You can then call `MyApp.Pgmq.send_message("myqueue", "hello")`, without passing
  in the `MyApp.Repo`
  """

  alias Pgmq.Message

  @typedoc "Queue name"
  @type queue :: String.t()

  @typedoc "An Ecto repository"
  @type repo :: Ecto.Repo.t()

  @default_max_poll_seconds 5
  @default_poll_interval_ms 250

  defmacro __using__(opts) do
    repo = Keyword.fetch!(opts, :repo)

    quote do
      @spec create_queue(Pgmq.queue()) :: :ok
      def create_queue(queue), do: Pgmq.create_queue(unquote(repo), queue)

      @spec drop_queue(Pgmq.queue()) :: :ok
      def drop_queue(queue), do: Pgmq.drop_queue(unquote(repo), queue)

      @spec send_message(Pgmq.queue(), term()) :: {:ok, integer()} | {:error, term()}
      def send_message(queue, message) do
        Pgmq.send_message(unquote(repo), queue, encoded_message)
      end

      @spec read_message(Pgmq.queue(), integer()) :: Pgmq.Message.t() | nil
      def read_message(queue, visibility_timeout_seconds) do
        Pgmq.read_message(unquote(repo), queue, visibility_timeout_seconds)
      end

      @spec read_messages(
              Pgmq.queue(),
              visibility_timeout_seconds :: integer(),
              count :: integer()
            ) :: [Pgmq.Message.t()]
      def read_messages(queue, visibility_timeout_seconds, count) do
        Pgmq.read_messages(unquote(repo), queue, visibility_timeout_seconds, count)
      end

      @spec read_messages_with_poll(
              Pgmq.queue(),
              visibility_timeout_seconds :: integer(),
              count :: integer(),
              max_poll_seconds :: integer(),
              poll_interval_ms :: integer()
            ) :: [Pgmq.Message.t()]
      def read_messages_with_poll(
            queue,
            count,
            visibility_timeout_seconds,
            max_poll_seconds \\ @default_max_poll_seconds,
            poll_interval_ms \\ @default_poll_interval_ms
          ) do
        Pgmq.read_messages_with_poll(
          unquote(repo),
          queue,
          visibility_timeout_seconds,
          max_poll_seconds,
          poll_interval_ms
        )
      end

      @spec archive_messages(Pgmq.queue(), message :: [Pgmq.Message.t()] | [integer()]) :: :ok
      def archive_message(queue, message) do
        Pgmq.archive_message(unquote(repo), queue, message)
      end

      @spec delete_messages(Pgmq.queue(), messages :: [Pgmq.Message.t()] | [integer()]) :: :ok
      def delete_messages(queue, messages) do
        Pgmq.delete_messages(unquote(repo), queue, message)
      end
    end
  end

  @doc """
  Creates a queue in the database

  Notice that the queue name must:
  - have less than 63 characters
  - start with a letter
  - have only letters, numbers, and `_`
  """
  @spec create_queue(repo, queue) :: :ok | {:error, atom}
  def create_queue(repo, queue) do
    %Postgrex.Result{num_rows: 1} = repo.query!("SELECT FROM pgmq_create($1)", [queue])
    :ok
  end

  @doc """
  Deletes a queue from the database
  """
  @spec drop_queue(repo, queue) :: :ok | {:error, atom}
  def drop_queue(repo, queue) do
    %Postgrex.Result{num_rows: 1} = repo.query!("SELECT FROM pgmq_drop($1)", [queue])
    :ok
  end

  @doc """
  Sends one message to a queue
  """
  @spec send_message(repo, queue, encoded_message :: binary) ::
          {:ok, Message.t()} | {:error, term}
  def send_message(repo, queue, encoded_message) do
    case repo.query!("SELECT * FROM pgmq_send($1, $2)", [queue, encoded_message]) do
      %Postgrex.Result{rows: [[message_id]]} -> {:ok, message_id}
      result -> {:error, {:sending_error, result}}
    end
  end

  @doc """
  Reads one message from a queue

  Returns immediately. If there are no messages in the queue, returns `nil`.

  Messages read through this function are guaranteed not to be read by
  other calls for `visibility_timeout_seconds`.
  """
  @spec read_message(repo, queue, visibility_timeout_seconds :: integer) :: Message.t() | nil
  def read_message(repo, queue, visibility_timeout_seconds) do
    %Postgrex.Result{rows: rows} =
      repo.query!("SELECT * FROM pgmq_read($1, $2, 1)", [queue, visibility_timeout_seconds])

    case rows do
      [] -> nil
      [row] -> Message.from_row(row)
    end
  end

  @doc """
  Reads a batch of messages from a queue

  Messages read through this function are guaranteed not to be read by
  other calls for `visibility_timeout_seconds`.
  """
  @spec read_messages(repo, queue, visibility_timeout_seconds :: integer, count :: integer) :: [
          Message.t()
        ]
  def read_messages(repo, queue, visibility_timeout_seconds, count) do
    %Postgrex.Result{rows: rows} =
      repo.query!("SELECT * FROM pgmq_read($1, $2, $3)", [
        queue,
        visibility_timeout_seconds,
        count
      ])

    Enum.map(rows, &Message.from_row/1)
  end

  @doc """
  Reads a batch of messages from a queue, but waits if no messages are available

  When there are messages available in the queue, returns immediately.
  Otherwise, blocks until at least one message is available, or `max_poll_seconds`
  is reached. The `poll_interval_ms` option dictates the polling interval
  database-side, and can be tuned for lower latency or less database load.

  Notice that this function may put significant burden on the connection pool,
  as it may hold the connection for several seconds if there's no activity in
  the queue.

  Messages read through this function are guaranteed not to be read by
  other calls for `visibility_timeout_seconds`.
  """
  @spec read_messages_with_poll(
          repo,
          queue,
          visibility_timeout_seconds :: integer,
          count :: integer,
          max_poll_seconds :: integer,
          poll_interval_ms :: integer
        ) :: [Message.t()]
  def read_messages_with_poll(
        repo,
        queue,
        visibility_timeout_seconds,
        count,
        max_poll_seconds \\ @default_max_poll_seconds,
        poll_interval_ms \\ @default_poll_interval_ms
      ) do
    %Postgrex.Result{rows: rows} =
      repo.query!("SELECT * FROM pgmq_read_with_poll($1, $2, $3, $4, $5)", [
        queue,
        visibility_timeout_seconds,
        count,
        max_poll_seconds,
        poll_interval_ms
      ])

    Enum.map(rows, &Message.from_row/1)
  end

  @doc """
  Archives a message, removing it from the queue and putting it into the archive

  This function can receive a list of either `Message.t()` or message ids. Mixed
  lists aren't allowed.
  """
  @spec archive_messages(repo, queue, (message_id :: integer) | (message :: Message.t())) :: :ok
  def archive_messages(repo, queue, [%Message{} | _] = messages) do
    message_ids = Enum.map(messages, fn m -> m.id end)
    archive_messages(repo, queue, message_ids)
  end

  def archive_messages(repo, queue, [message_id]) do
    %Postgrex.Result{rows: [[true]]} =
      repo.query!("SELECT * FROM pgmq_archive($1, $2::bigint)", [queue, message_id])

    :ok
  end

  def archive_messages(repo, queue, message_ids) do
    %Postgrex.Result{rows: [[true]]} =
      repo.query!("SELECT * FROM pgmq_archive($1, $2::bigint[])", [queue, message_ids])

    :ok
  end

  @doc """
  Deletes a batch of messages, removing them from the queue

  This function can receive a list of either `Message.t()` or message ids. Mixed
  lists aren't allowed.
  """
  @spec delete_messages(repo, queue, [message_id :: integer] | [Message.t()]) :: :ok
  def delete_messages(repo, queue, [%Message{} | _] = messages) do
    message_ids = Enum.map(messages, fn m -> m.id end)
    delete_messages(repo, queue, message_ids)
  end

  def delete_messages(repo, queue, [message_id]) do
    %Postgrex.Result{rows: [[true]]} =
      repo.query!("SELECT * FROM pgmq_delete($1::text, $2::bigint)", [queue, message_id])

    :ok
  end

  def delete_messages(repo, queue, message_ids) do
    %Postgrex.Result{rows: [[true]]} =
      repo.query!("SELECT * FROM pgmq_delete($1::text, $2::bigint[])", [queue, message_ids])

    :ok
  end
end
