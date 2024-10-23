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
      @spec create_queue(Pgmq.queue(), Keyword.t()) :: :ok
      def create_queue(queue, opts \\ []), do: Pgmq.create_queue(unquote(repo), queue, opts)

      @spec drop_queue(Pgmq.queue()) :: :ok
      def drop_queue(queue), do: Pgmq.drop_queue(unquote(repo), queue)

      @spec send_message(Pgmq.queue(), binary()) :: {:ok, integer()} | {:error, term()}
      def send_message(queue, encoded_message) do
        Pgmq.send_message(unquote(repo), queue, encoded_message)
      end

      @spec send_messages(Pgmq.queue(), [binary()]) :: {:ok, [integer()]} | {:error, term()}
      def send_messages(queue, encoded_messages) do
        Pgmq.send_messages(unquote(repo), queue, encoded_messages)
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
              opts :: Keyword.t()
            ) :: [Pgmq.Message.t()]
      def read_messages_with_poll(
            queue,
            count,
            visibility_timeout_seconds,
            opts \\ []
          ) do
        Pgmq.read_messages_with_poll(
          unquote(repo),
          queue,
          visibility_timeout_seconds,
          opts
        )
      end

      @spec archive_messages(Pgmq.queue(), messages :: [Pgmq.Message.t()] | [integer()]) :: :ok
      def archive_messages(queue, messages) do
        Pgmq.archive_messages(unquote(repo), queue, messages)
      end

      @spec delete_messages(Pgmq.queue(), messages :: [Pgmq.Message.t()] | [integer()]) :: :ok
      def delete_messages(queue, messages) do
        Pgmq.delete_messages(unquote(repo), queue, messages)
      end

      @doc """
      Returns a list of queue names
      """
      @spec list_queues() :: [
              %{
                queue_name: String.t(),
                is_partitioned: boolean(),
                is_unlogged: boolean(),
                created_at: DateTime.t()
              }
            ]
      def list_queues() do
        Pgmq.list_queues(unquote(repo))
      end

      @doc """
      Sets the visibility timeout of a message for X seconds from now

      Accepts either a message or a message id.
      """
      @spec set_message_vt(Pgmq.queue(), Pgmq.Message.t() | integer(), integer()) :: :ok
      def set_message_vt(queue, message, vt) do
        Pgmq.set_message_vt(unquote(repo), queue, message, vt)
      end

      @doc """
      Reads a message and instantly deletes it from the queue

      If there are no messages in the queue, returns `nil`.
      """
      @spec pop_message(Pgmq.queue()) :: Pgmq.Message.t() | nil
      def pop_message(queue) do
        Pgmq.pop_message(unquote(repo), queue)
      end
    end
  end

  @doc """
  Creates a queue in the database

  Notice that the queue name must:
  - have less than 48 characters
  - start with a letter
  - have only letters, numbers, and `_`

  Accepts the following options:
  - `:unlogged`: Boolean indicating if the queue should be unlogged. Unlogged
  queues are faster to write to, but data may be lost in database crashes or
  unclean exits. Can't be used together with `:partitioned`.
  - `:partitioned`: indicates if the queue is partitioned. Defaults to `false`. Requires
  `pg_partman` extension.
  - `:partition_interval:` interval to partition the queue, required if `:partitioned`
  is true.
  - `:retention_interval:` interval for partition retention, required if `:partitioned`
  is true.
  """
  @spec create_queue(repo, queue, opts :: Keyword.t()) :: :ok | {:error, atom}
  def create_queue(repo, queue, opts \\ []) do
    if Keyword.get(opts, :partitioned, false) do
      if Keyword.get(opts, :unlogged), do: raise("Partitioned queues can't be unlogged")
      partition_interval = Keyword.fetch!(opts, :partition_interval)
      retention_interval = Keyword.fetch!(opts, :retention_interval)

      repo.query!("SELECT FROM pgmq.create_partitioned($1, $2, $3)", [
        queue,
        partition_interval,
        retention_interval
      ])
    else
      if Keyword.get(opts, :unlogged) do
        %Postgrex.Result{num_rows: 1} =
          repo.query!("SELECT FROM pgmq.create_unlogged($1)", [queue])
      else
        %Postgrex.Result{num_rows: 1} = repo.query!("SELECT FROM pgmq.create($1)", [queue])
      end
    end

    :ok
  end

  @doc """
  Deletes a queue from the database
  """
  @spec drop_queue(repo, queue) :: :ok | {:error, atom}
  def drop_queue(repo, queue) do
    %Postgrex.Result{num_rows: 1} = repo.query!("SELECT FROM pgmq.drop_queue($1)", [queue])
    :ok
  end

  @doc """
  Sends one message to a queue
  """
  @spec send_message(repo, queue, encoded_message :: binary) ::
          {:ok, Message.t()} | {:error, term}
  def send_message(repo, queue, encoded_message) do
    case repo.query!("SELECT * FROM pgmq.send($1, $2)", [queue, encoded_message]) do
      %Postgrex.Result{rows: [[message_id]]} -> {:ok, message_id}
      result -> {:error, {:sending_error, result}}
    end
  end

  @doc """
  Sends a message batch to a queue
  """
  @spec send_messages(repo, queue, encoded_messages :: [binary]) ::
          {:ok, Message.t()} | {:error, term}
  def send_messages(repo, queue, encoded_messages) do
    case repo.query!("SELECT * FROM pgmq.send_batch($1, $2)", [queue, encoded_messages]) do
      %Postgrex.Result{rows: message_ids} -> {:ok, List.flatten(message_ids)}
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
      repo.query!("SELECT * FROM pgmq.read($1, $2, 1)", [queue, visibility_timeout_seconds])

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
      repo.query!("SELECT * FROM pgmq.read($1, $2, $3)", [
        queue,
        visibility_timeout_seconds,
        count
      ])

    Enum.map(rows, &Message.from_row/1)
  end

  @doc """
  Reads a batch of messages from a queue, but waits if no messages are available

  Accepts two options:
  - `:max_poll_seconds`: the maximum duration of the poll. Defaults to 5.
  - `:poll_interval_ms`: dictates how often the poll is made database
  side. Defaults to 250. Can be tuned for lower latency or less database load,
  depending on your needs.

  When there are messages available in the queue, returns immediately.
  Otherwise, blocks until at least one message is available, or `max_poll_seconds`
  is reached.

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
          opts :: Keyword.t()
        ) :: [Message.t()]
  def read_messages_with_poll(
        repo,
        queue,
        visibility_timeout_seconds,
        count,
        opts \\ []
      ) do
    max_poll_seconds = Keyword.get(opts, :max_poll_seconds, @default_max_poll_seconds)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms)

    %Postgrex.Result{rows: rows} =
      repo.query!("SELECT * FROM pgmq.read_with_poll($1, $2, $3, $4, $5)", [
        queue,
        visibility_timeout_seconds,
        count,
        max_poll_seconds,
        poll_interval_ms
      ])

    Enum.map(rows, &Message.from_row/1)
  end

  @doc """
  Archives list of messages, removing them from the queue and putting
  them into the archive

  This function can receive a list of either `Message.t()` or message ids. Mixed
  lists aren't allowed.
  """
  @spec archive_messages(repo, queue, [message_id :: integer] | [message :: Message.t()]) :: :ok
  def archive_messages(repo, queue, [%Message{} | _] = messages) do
    message_ids = Enum.map(messages, fn m -> m.id end)
    archive_messages(repo, queue, message_ids)
  end

  def archive_messages(repo, queue, [message_id]) do
    %Postgrex.Result{rows: [[true]]} =
      repo.query!("SELECT * FROM pgmq.archive($1, $2::bigint)", [queue, message_id])

    :ok
  end

  def archive_messages(repo, queue, message_ids) do
    %Postgrex.Result{} =
      repo.query!("SELECT * FROM pgmq.archive($1, $2::bigint[])", [queue, message_ids])

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
      repo.query!("SELECT * FROM pgmq.delete($1::text, $2::bigint)", [queue, message_id])

    :ok
  end

  def delete_messages(repo, queue, message_ids) do
    %Postgrex.Result{} =
      repo.query!("SELECT * FROM pgmq.delete($1::text, $2::bigint[])", [queue, message_ids])

    :ok
  end

  @doc """
  Returns a list of queues
  """
  @spec list_queues(repo) :: [
          %{
            queue_name: String.t(),
            is_partitioned: boolean(),
            is_unlogged: boolean(),
            created_at: DateTime.t()
          }
        ]
  def list_queues(repo) do
    %Postgrex.Result{
      columns: ["queue_name", "is_partitioned", "is_unlogged", "created_at"],
      rows: queues
    } = repo.query!("SELECT * FROM pgmq.list_queues()", [])

    Enum.map(queues, fn [queue_name, is_partitioned, is_unlogged, created_at] ->
      %{
        queue_name: queue_name,
        is_partitioned: is_partitioned,
        is_unlogged: is_unlogged,
        created_at: created_at
      }
    end)
  end

  @doc """
  Returns a list of queues with stats
  """
  @spec get_metrics_all(repo) :: [
          %{
            queue_name: String.t(),
            queue_length: pos_integer(),
            newest_msg_age_sec: pos_integer() | nil,
            oldest_msg_age_sec: pos_integer() | nil,
            total_messages: pos_integer(),
            scrape_time: DateTime.t()
          }
        ]
  def get_metrics_all(repo) do
    %Postgrex.Result{rows: queues} = repo.query!("SELECT * FROM pgmq.metrics_all()", [])

    Enum.map(queues, fn [
                          queue_name,
                          queue_length,
                          newest_msg_age_sec,
                          oldest_msg_age_sec,
                          total_messages,
                          scrape_time
                        ] ->
      %{
        queue_name: queue_name,
        queue_length: queue_length,
        newest_msg_age_sec: newest_msg_age_sec,
        oldest_msg_age_sec: oldest_msg_age_sec,
        total_messages: total_messages,
        scrape_time: scrape_time
      }
    end)
  end

  @doc """
  Returns metrics for a single queue
  """
  @spec get_metrics(repo, queue) :: [
          %{
            queue_name: String.t(),
            queue_length: pos_integer(),
            newest_msg_age_sec: pos_integer() | nil,
            oldest_msg_age_sec: pos_integer() | nil,
            total_messages: pos_integer(),
            scrape_time: DateTime.t()
          }
        ]
  def get_metrics(repo, queue) do
    %Postgrex.Result{rows: [result]} = repo.query!("SELECT * FROM pgmq.metrics($1)", [queue])

    [
      queue_name,
      queue_length,
      newest_msg_age_sec,
      oldest_msg_age_sec,
      total_messages,
      scrape_time
    ] = result

    %{
      queue_name: queue_name,
      queue_length: queue_length,
      newest_msg_age_sec: newest_msg_age_sec,
      oldest_msg_age_sec: oldest_msg_age_sec,
      total_messages: total_messages,
      scrape_time: scrape_time
    }
  end

  @doc """
  Sets the visibility timeout of a message for X seconds from now
  """
  @spec set_message_vt(repo, queue, Message.t() | integer(), visibility_timeout :: integer()) ::
          :ok
  def set_message_vt(repo, queue, %Message{id: message_id}, visibility_timeout) do
    set_message_vt(repo, queue, message_id, visibility_timeout)
  end

  def set_message_vt(repo, queue, message_id, visibility_timeout) do
    %Postgrex.Result{rows: [_]} =
      repo.query!("SELECT * FROM pgmq.set_vt($1, $2, $3)", [queue, message_id, visibility_timeout])

    :ok
  end

  @doc """
  Reads a message and instantly deletes it from the queue

  If there are no messages in the queue, returns `nil`.
  """
  @spec pop_message(repo, queue) :: Message.t() | nil
  def pop_message(repo, queue) do
    case repo.query!("SELECT * FROM pgmq.pop($1)", [queue]) do
      %Postgrex.Result{rows: [columns]} ->
        Message.from_row(columns)

      %Postgrex.Result{rows: []} ->
        nil
    end
  end
end
