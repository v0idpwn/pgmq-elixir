defmodule PgmqTest do
  use ExUnit.Case
  doctest Pgmq

  test "greets the world" do
    assert Pgmq.hello() == :world
  end
end
