-module(avern_queue_sup).
-behaviour(supervisor).

-export([spawn_queue/1]).
-export([start_link/3, init/1]).

spawn_queue(Metric) ->
    supervisor:start_child(?MODULE, [Metric]).

start_link(DiskWindowSize, MemoryWindowSize,  LevelDB) ->
    supervisor:start_link(
        {local, ?MODULE}, ?MODULE, [DiskWindowSize, MemoryWindowSize, LevelDB]).

init(Args) ->
    QueueSpec = {
        avern_queue,
        {avern_queue, start_link, Args},
        temporary, 5000, worker, [avern_queue]
    },
    {ok, {{simple_one_for_one, 10, 10}, [QueueSpec]}}.
