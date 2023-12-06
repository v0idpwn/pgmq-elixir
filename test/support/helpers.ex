defmodule Pgmq.Helpers do
  def queue_size(repo, queue_name) do
    %Postgrex.Result{rows: [[c]]} =
      repo.query!("SELECT COUNT(1) FROM pgmq.q_#{queue_name}")

    c
  end

  def archive_size(repo, queue_name) do
    %Postgrex.Result{rows: [[c]]} =
      repo.query!("SELECT COUNT(1) FROM pgmq.a_#{queue_name}")

    c
  end
end
