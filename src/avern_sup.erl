-module(avern_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
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
            Args = [{ref, Ref}, {read_opts, ReadOpts}, {write_opts, WriteOpts}],
            {ok, {{one_for_one, 5, 10}, [
                {avern_queue_sup,
                    {avern_queue_sup, start_link, [Args]},
                    permanent, infinity, supervisor, [avern_queue_sup]},
                {avern_encoding,
                    {avern_encoding, start_link, [Args]},
                    permanent, 5000, worker, [avern_encoding]},
                {avern_udp,
                    {avern_udp, start_link, []},
                    permanent, 5000, worker, [avern_udp]}
            ]}};
         Else ->
            io:format("Failed to open LevelDB: ~p~n", [Else]),
            {error, Else}
    end.
