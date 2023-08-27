defmodule PgmqTest do
  use ExUnit.Case
  doctest Pgmq

  alias Pgmq.TestRepo
  alias Pgmq.Message

  test "regular flow" do
    queue_name = "regular_flow_queue"
    assert :ok = Pgmq.create_queue(TestRepo, queue_name)
    assert {:ok, message_id} = Pgmq.send_message(TestRepo, queue_name, "1")
    assert is_integer(message_id)

    assert %Message{id: ^message_id, body: "1"} =
             message = Pgmq.read_message(TestRepo, queue_name, 2)

    assert is_nil(Pgmq.read_message(TestRepo, queue_name, 2))

    assert :ok = Pgmq.delete_messages(TestRepo, "regular_flow_queue", [message])
  end

  test "list queues" do
    # This is a trick to reset the tables before running this test, as it
    # can fail due to data from others (we aren't using sandbox)
    TestRepo.query!("DROP EXTENSION pgmq")
    TestRepo.query!("CREATE EXTENSION pgmq")

    # Actual test
    q1 = "list_queues_q1"
    q2 = "list_queues_q2"

    assert [] = Pgmq.list_queues(TestRepo)
    Pgmq.create_queue(TestRepo, q1)
    assert [%{queue_name: ^q1}] = Pgmq.list_queues(TestRepo)
    Pgmq.create_queue(TestRepo, q2)
    assert [%{queue_name: ^q1}, %{queue_name: ^q2}] = Pgmq.list_queues(TestRepo)
  end

  test "batches" do
    queue_name = "batches_queue"
    assert :ok = Pgmq.create_queue(TestRepo, queue_name)
    assert {:ok, _m1} = Pgmq.send_message(TestRepo, queue_name, "1")
    assert {:ok, _m2} = Pgmq.send_message(TestRepo, queue_name, "2")
    assert {:ok, m3} = Pgmq.send_message(TestRepo, queue_name, "3")
    assert {:ok, m4} = Pgmq.send_message(TestRepo, queue_name, "4")

    assert [_m1, _m2] = Pgmq.read_messages(TestRepo, queue_name, 0, 2)

    assert [%Message{}, %Message{}, full_m1, full_m2] =
             Pgmq.read_messages(TestRepo, queue_name, 0, 4)

    assert :ok = Pgmq.delete_messages(TestRepo, queue_name, [full_m1, full_m2])
    assert [_, _] = Pgmq.read_messages(TestRepo, queue_name, 0, 4)
    assert :ok = Pgmq.delete_messages(TestRepo, queue_name, [m3, m4])
    assert [] = Pgmq.read_messages(TestRepo, queue_name, 0, 4)
  end

  test "archive" do
    queue_name = "archive_queue"
    assert :ok = Pgmq.create_queue(TestRepo, queue_name)
    assert {:ok, m1} = Pgmq.send_message(TestRepo, queue_name, "1")
    assert {:ok, _m2} = Pgmq.send_message(TestRepo, queue_name, "2")

    assert :ok = Pgmq.archive_messages(TestRepo, queue_name, [m1])
    assert [%Message{} = m2_full] = Pgmq.read_messages(TestRepo, queue_name, 0, 2)

    assert :ok = Pgmq.archive_messages(TestRepo, queue_name, [m2_full])
    assert [] = Pgmq.read_messages(TestRepo, queue_name, 0, 2)
  end

  test "polling" do
    queue_name = "polling_queue"
    assert :ok = Pgmq.create_queue(TestRepo, queue_name)
    test_pid = self()

    inserter_pid =
      spawn(fn ->
        receive do
          :insert -> :ok
        end

        Pgmq.send_message(TestRepo, queue_name, "hello")
      end)

    poller_pid =
      spawn(fn ->
        receive do
          :start_polling -> :ok
        end

        assert [%Message{} = m] = Pgmq.read_messages_with_poll(TestRepo, queue_name, 5, 1)
        send(test_pid, {:got_result, m})
      end)

    send(poller_pid, :start_polling)
    refute_receive {:got_result, _}, 2000
    send(inserter_pid, :insert)
    assert_receive {:got_result, %Message{}}, 1000
  end

  test "pop" do
    queue_name = "pop_queue"
    assert :ok = Pgmq.create_queue(TestRepo, queue_name)
    assert {:ok, _} = Pgmq.send_message(TestRepo, queue_name, "1")

    assert %{body: "1"} = Pgmq.pop_message(TestRepo, queue_name)
    assert is_nil(Pgmq.pop_message(TestRepo, queue_name))
  end

  test "set message vt" do
    queue_name = "set_vt_queue"
    assert :ok = Pgmq.create_queue(TestRepo, queue_name)
    assert {:ok, m1} = Pgmq.send_message(TestRepo, queue_name, "1")
    assert Pgmq.set_message_vt(TestRepo, queue_name, m1, 50)
    assert is_nil(Pgmq.read_message(TestRepo, queue_name, 10))
    assert Pgmq.set_message_vt(TestRepo, queue_name, m1, 0)
    assert %Message{} = Pgmq.read_message(TestRepo, queue_name, 10)
  end
end
