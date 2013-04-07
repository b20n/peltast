-module(avern_queue).
-behaviour(gen_server).

-export([
    update/4,
    flush/1,
    read/3
]).

-export([
    start_link/4,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("avern.hrl").

-record(st, {
    metric,
    data,
    latest_flush,
    disk_window_size,
    memory_window_size,
    leveldb
}).

%% TODO: check if queries can be served entirely from RAM before hitting disk

-spec update(binary(), pos_integer(), number(), tags()) -> ok.
update(Metric, Timestamp, Value, Tags) ->
    Pid = case gproc:where({n, l, Metric}) of
        undefined ->
            {ok, P} = avern_queue_sup:spawn_queue(Metric),
            P;
        P -> P
    end,
    gen_server:cast(Pid, {update, Timestamp, Value, Tags}).

-spec flush(binary()) -> {error, any()} | ok.
flush(Metric) ->
    case gproc:where({n, l, Metric}) of
        undefined -> {error, unknown_metric};
        Pid -> gen_server:cast(Pid, flush)
    end.

-spec read(binary(), pos_integer(), pos_integer()) -> {ok, list()}.
read(Metric, From, To) ->
    Pid = case gproc:where({n, l, Metric}) of
        undefined ->
            {ok, P} = avern_queue_sup:spawn_queue(Metric),
            P;
        P -> P
    end,
    gen_server:call(Pid, {read, From, To}).

start_link(LevelDB, Metric, DiskWindowSize, MemoryWindowSize) ->
    gen_server:start_link(
        ?MODULE, {LevelDB, Metric, DiskWindowSize, MemoryWindowSize}, []
    ).

init({LevelDB, Metric, DiskWindowSize, MemoryWindowSize}) ->
    gproc:reg({n, l, Metric}, ignored),
    process_flag(trap_exit, true),
    {ok, Period} = avern_scheduler:schedule(),
    timer:send_after(Period, self(), cleanup),
    timer:send_after(Period, self(), flush),
    Data = gb_sets:new(),
    {ok, #st{
        metric = Metric,
        data = Data,
        latest_flush = 0,
        disk_window_size = DiskWindowSize,
        memory_window_size = MemoryWindowSize,
        leveldb = LevelDB
    }}.

handle_call({read, From, To}, _From, State) ->
    #st{
        metric = Metric,
        data = Data,
        leveldb = LevelDB
    } = State,
    Disk = avern_leveldb:read(Metric, From, To, LevelDB),
    Cache = read_from_cache(Metric, From, To, Data),
    {_, FilteredDisk} = proplists:split(Disk, proplists:get_keys(Cache)),
    Final = lists:sort(FilteredDisk ++ Cache),
    {reply, {ok, Final}, State};
handle_call(Msg, _From, State) ->
    {stop, {unknown_call, Msg}, error, State}.

handle_cast({update, Timestamp, Value, Tags}, #st{data=Data}=State) ->
    Data1 = gb_sets:add({{Timestamp, Tags}, Value}, Data),
    {noreply, State#st{data=Data1}};
handle_cast(Msg, State) ->
    {stop, {unknown_cast, Msg}, State}.

handle_info(cleanup, State) ->
    #st{
        metric = Metric,
        data = Data,
        latest_flush = LF,
        disk_window_size = DWS,
        memory_window_size = MWS,
        leveldb = LevelDB
    } = State,
    Until = avern_util:now() - DWS,
    spawn_link(fun() -> avern_leveldb:delete(Metric, Until, LevelDB) end),
    Data1 = cleanup_memory(MWS, LF, Data),
    {ok, Period} = avern_scheduler:schedule(),
    timer:send_after(Period, self(), cleanup),
    {noreply, State#st{data=Data1}};
handle_info(flush, State) ->
    #st{
        metric = Metric,
        data = Data,
        disk_window_size = DiskWindowSize,
        leveldb = LevelDB
    } = State,
    Points = choose_writable_points(DiskWindowSize, Data),
    case gb_sets:is_empty(Points) of
        true -> ok;
        false ->
            spawn_link(fun() ->
                exit(avern_leveldb:write(Metric, Points, LevelDB)) end
            )
    end,
    {noreply, State};
handle_info({'EXIT', _From, {_, T}}, #st{latest_flush=T0}=State) ->
    {ok, Period} = avern_scheduler:schedule(),
    timer:send_after(Period, self(), flush),
    {noreply, State#st{latest_flush=max(T0, T)}};
handle_info({'EXIT', _From, normal}, State) ->
    {noreply, State};
handle_info(Msg, State) ->
    {stop, {unknown_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

read_from_cache(Metric, From, To, Data) ->
    Filtered = gb_sets:filter(
        fun({{Timestamp, _}, _}) ->
            Timestamp >= From andalso Timestamp =< To
        end,
        Data
    ),
    lists:map(
        fun({{Timestamp, Tags}, Value}) ->
            {{Metric, Timestamp, Tags}, Value}
        end,
        gb_sets:to_list(Filtered)
    ).

-spec choose_writable_points(pos_integer(), gb_set()) -> gb_set().
choose_writable_points(DiskWindowSize, Data) ->
    EarliestTime = avern_util:now() - DiskWindowSize,
    gb_sets:fold(
        fun({{Timestamp, _}, _}=Point, Acc) when Timestamp >= EarliestTime ->
            gb_sets:add(Point, Acc);
           (_, Acc) ->
            Acc
        end,
        gb_sets:new(),
        Data
    ).

-spec cleanup_memory(pos_integer(), pos_integer(), gb_set()) -> gb_set().
cleanup_memory(MemoryWindowSize, LatestFlush, Data) ->
    DeleteUntil = min(LatestFlush, avern_util:now() - MemoryWindowSize),
    gb_sets:filter(
        fun({{Timestamp, _}, _}) ->
            Timestamp >= DeleteUntil
        end,
        Data
    ).
