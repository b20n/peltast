-module(avern_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    % Queues
    DiskWindowSize = case application:get_env(avern, disk_window) of
        {ok, DWS} -> DWS;
        undefined -> 86400
    end,
    MemoryWindowSize = case application:get_env(avern, memory_window) of
        {ok, MWS} -> MWS;
        undefined -> 0
    end,
    % Webmachine
    WebIP = case application:get_env(avern, web_ip) of
        {ok, IP} -> IP;
        undefined -> {0, 0, 0, 0}
    end,
    WebPort = case application:get_env(avern, web_port) of
        {ok, Port} -> Port;
        undefined -> 12068
    end,
    {ok, Dispatch} = file:consult(filename:join([
        filename:dirname(code:which(?MODULE)),
        "..", "priv", "dispatch.conf"
    ])),
    WebConfig = [{ip, WebIP}, {port, WebPort}, {dispatch, Dispatch}],
    % LevelDB
    LevelDBOpts = [
        {create_if_missing, true},
        {write_buffer_size, 60 * 1024 * 1024},
        {max_open_files, 60},
        {block_size, 4096},
        {block_restart_interval, 16},
        {cache_size, 256 * 1024 * 1024},
        {sync, true},
        {verify_checksums, true}
    ],
    UserOpts = case application:get_env(avern, leveldb_opts) of
        {ok, Opts} -> Opts;
        undefined -> []
    end,
    {_, DefaultOpts} = proplists:split(LevelDBOpts, proplists:get_keys(UserOpts)),
    FinalOpts = UserOpts ++ DefaultOpts,
    {OpenOpts, _} = eleveldb:validate_options(open, FinalOpts),
    {ReadOpts, _} = eleveldb:validate_options(read, FinalOpts),
    {WriteOpts, _} = eleveldb:validate_options(write, FinalOpts),
    DataDir = case application:get_env(avern, data_dir) of
        {ok, D} -> D;
        undefined -> "/tmp/avern"
    end,
    case eleveldb:open(DataDir, OpenOpts) of
        {ok, Ref} ->
            LevelDB = [
                {ref, Ref},
                {read_opts, ReadOpts},
                {write_opts, WriteOpts}
            ],
            QueueSupArgs = [DiskWindowSize, MemoryWindowSize, LevelDB],
            {ok, {{one_for_one, 5, 10}, [
                {avern_queue_sup,
                    {avern_queue_sup, start_link, [QueueSupArgs]},
                    permanent, infinity, supervisor, [avern_queue_sup]},
                {avern_encoding,
                    {avern_encoding, start_link, [LevelDB]},
                    permanent, 5000, worker, [avern_encoding]},
                {avern_udp,
                    {avern_udp, start_link, []},
                    permanent, 5000, worker, [avern_udp]},
                {avern_scheduler,
                    {avern_scheduler, start_link, []},
                    permanent, 5000, worker, [avern_scheduler]},
                {webmachine_mochiweb,
                    {webmachine_mochiweb, start, [WebConfig]},
                    permanent, 5000, worker, [webmachine_mochiweb]}
            ]}};
         Else ->
            io:format("Failed to open LevelDB: ~p~n", [Else]),
            {error, Else}
    end.
