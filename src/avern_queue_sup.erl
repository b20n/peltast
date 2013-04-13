-module(avern_queue_sup).
-behaviour(supervisor).

-export([spawn_queue/1]).
-export([start_link/1, init/1]).

spawn_queue(Metric) ->
    %% TODO: from a config file
    DiskWindowSize = 86400,
    MemoryWindowSize = 0,
    supervisor:start_child(?MODULE, [Metric, DiskWindowSize, MemoryWindowSize]).

start_link(LevelDB) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, LevelDB).

init(LevelDB) ->
    QueueSpec = {
        avern_queue,
        {avern_queue, start_link, [LevelDB]},
        temporary, 5000, worker, [avern_queue]
    },
    {ok, {{simple_one_for_one, 10, 10}, [QueueSpec]}}.
