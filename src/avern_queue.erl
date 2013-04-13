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
    memory,
    disk,
    memory_window_size,
    disk_window_size,
    leveldb
}).

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
    avern_scheduler:register(self()),
    {ok, #st{
        metric = Metric,
        memory = gb_sets:new(),
        disk = gb_sets:new(),
        memory_window_size = MemoryWindowSize,
        disk_window_size = DiskWindowSize,
        leveldb = LevelDB
    }}.

handle_call(get_state, _From, State) ->
    {reply, {ok, State}, State};
handle_call({read, From, To}, _From, State) ->
    #st{
        metric = Metric,
        memory = Mem,
        disk = Disk,
        leveldb = LevelDB
    } = State,
    Cached = read_from_cache(Metric, From, To, gb_sets:union(Mem, Disk)),
    Final = case Cached of
        % TODO: fudge factor on timing
        [{{_, Timestamp, _}, _}|_] when Timestamp > From ->
            Disked = avern_leveldb:read(Metric, From, Timestamp, LevelDB),
            {_, Filtered} = proplists:split(Disked, proplists:get_keys(Cached)),
            lists:sort(Filtered ++ Cached);
        [] ->
            Disked = avern_leveldb:read(Metric, From, To, LevelDB),
            {_, Filtered} = proplists:split(Disked, proplists:get_keys(Cached)),
            lists:sort(Filtered ++ Cached);
        _ ->
            Cached
    end,
    {reply, {ok, Final}, State};
handle_call(Msg, _From, State) ->
    {stop, {unknown_call, Msg}, error, State}.

handle_cast({update, Timestamp, Value, Tags}, #st{disk=Disk}=State) ->
    Disk1 = gb_sets:add({{Timestamp, Tags}, Value}, Disk),
    {noreply, State#st{disk=Disk1}};
handle_cast(Msg, State) ->
    {stop, {unknown_cast, Msg}, State}.

handle_info(flush, State) ->
    #st{
        metric = Metric,
        disk = Disk,
        disk_window_size = DiskWindowSize,
        leveldb = LevelDB
    } = State,
    spawn_link(fun() -> exit(flush(Metric, Disk, DiskWindowSize, LevelDB)) end),
    {noreply, State};
handle_info({'EXIT', _From, {_, LatestFlush}}, State) ->
    #st{
        memory = Mem,
        disk = Disk,
        memory_window_size = MemoryWindowSize
    } = State,
    {Mem1, Disk1} = shuffle_queues(MemoryWindowSize, LatestFlush, Mem, Disk),
    {noreply, State#st{memory=Mem1, disk=Disk1}};
handle_info(Msg, State) ->
    {stop, {unknown_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

read_from_cache(Metric, From, To, Data) ->
    read_from_cache(Metric, From, To, Data, []).

read_from_cache(Metric, From, To, Data, Acc) ->
    try gb_sets:take_smallest(Data) of
        {{{Timestamp, _}, _}, Data1} when Timestamp < From ->
            read_from_cache(Metric, From, To, Data1, Acc);
        {{{Timestamp, _}, _}, _} when Timestamp > To ->
            Acc;
        {{{Timestamp, Tags}, Value}, Data1} ->
            Acc1 = [{{Metric, Timestamp, Tags}, Value}|Acc],
            read_from_cache(Metric, From, To, Data1, Acc1)
    catch
        error:function_clause ->
            Acc
    end.


-spec flush(binary(), gb_set(), pos_integer(), list()) ->
  {ok, pos_integer()} | {error, 0}.
flush(Metric, Data, DiskWindowSize, LevelDB) ->
    KeepUntil = avern_util:now() - DiskWindowSize,
    avern_leveldb:delete(Metric, KeepUntil, LevelDB),
    PointsToWrite = choose_writable_points(DiskWindowSize, Data),
    BinaryPoints = encode_writable_points(Metric, PointsToWrite),
    case avern_leveldb:write(BinaryPoints, LevelDB) of
        ok ->
            {{{LatestTime, _}, _}, _} = gb_sets:take_largest(Data),
            {ok, LatestTime};
        {error, _Reason} ->
            {error, 0}
    end.

% TODO: rewrite this with an iteration via take_largest
-spec choose_writable_points(pos_integer(), gb_set()) -> list().
choose_writable_points(DiskWindowSize, Data) ->
    EarliestTime = avern_util:now() - DiskWindowSize,
    gb_sets:fold(
        fun({{Timestamp, _}, _}=Point, Acc) when Timestamp >= EarliestTime ->
            [Point|Acc];
           (_, Acc) ->
            Acc
        end,
        [],
        Data
    ).

-spec encode_writable_points(binary(), list()) -> list().
encode_writable_points(Metric, Points) ->
    encode_writable_points(Metric, Points, []).

-spec encode_writable_points(binary(), list(), list()) -> list().
encode_writable_points(_, [], Encoded) ->
    Encoded;
encode_writable_points(Metric, [{{Timestamp, Tags}, Value}|Points], Encoded) ->
    Key = avern_encoding:encode_object_key(Metric, Timestamp, Tags),
    EncodedValue = avern_encoding:encode_object_value(Value),
    encode_writable_points(Metric, Points, [{Key, EncodedValue}|Encoded]).

-spec shuffle_queues(integer(), integer(), gb_set(), gb_set()) -> {gb_set(), gb_set()}.
shuffle_queues(MemoryWindowSize, LatestFlush, Mem, Disk) ->
    {Disk1, ToShuffle} = drop_until(LatestFlush, Disk),
    Mem1 = gb_sets:union(Mem, gb_sets:from_list(ToShuffle)),
    {Mem2, _} = drop_until(avern_util:now() - MemoryWindowSize, Mem1),
    {Mem2, Disk1}.

-spec drop_until(pos_integer(), gb_set()) -> {gb_set(), list()}.
drop_until(Timestamp, Set) ->
    drop_until(Timestamp, Set, []).


-spec drop_until(pos_integer(), gb_set(), list()) -> {gb_set(), list()}.
drop_until(Timestamp, Set, Dropped) ->
    try gb_sets:take_smallest(Set) of
        {{{T, _}, _}=Item, Set1} when T =< Timestamp ->
            drop_until(Timestamp, Set1, [Item|Dropped]);
        _ ->
            {Set, Dropped}
    catch
        error:function_clause ->
            {Set, Dropped}
    end.
