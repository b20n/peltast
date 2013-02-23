-module(avern_queue).
-behaviour(gen_server).

-export([
    update/4,
    flush/1,
    read/3
]).

-export([
    start_link/2,
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
    leveldb,
    last_flush
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
        Pid -> gen_server:call(Pid, flush)
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

start_link(LevelDB, Metric) ->
    gen_server:start_link(?MODULE, {LevelDB, Metric}, []).

init({LevelDB, Metric}) ->
    gproc:reg({n, l, Metric}, ignored),
    Data = gb_sets:new(),
    {ok, #st{
        metric = Metric,
        data = Data,
        leveldb = LevelDB,
        last_flush = 0
    }}.

handle_call({read, From, To}, _From, #st{metric=Metric, leveldb=LevelDB}=St) ->
    %% TODO: read from queue cache.
    DiskRows = read_from_disk(Metric, From, To, LevelDB),
    {reply, {ok, DiskRows}, St};
handle_call(flush, _From, #st{data=Data, leveldb=LevelDB}=St) ->
    Operations = format_writes(Data),
    Ref = proplists:get_value(ref, LevelDB),
    WriteOpts = proplists:get_value(write_opts, LevelDB),
    case eleveldb:write(Ref, Operations, WriteOpts) of
        ok ->
            {Megaseconds, Seconds, _} = erlang:now(),
            Epoch = Megaseconds * 1000000 + Seconds,
            {reply, ok, St#st{data=gb_sets:new(), last_flush=Epoch}};
        {error, Reason} ->
            %% TODO: logme
            {reply, {error, Reason}, St#st{data=Data}}
    end;
handle_call(Msg, _From, St) ->
    {stop, {unknown_call, Msg}, error, St}.

handle_cast({update, Timestamp, Value, Tags}, St) ->
    Key = avern_encoding:encode_object_key(St#st.metric, Timestamp, Tags),
    EncodedValue = avern_encoding:encode_object_value(Value),
    Data = gb_sets:add({Key, EncodedValue}, St#st.data),
    {noreply, St#st{data=Data}, 0};
handle_cast(Msg, St) ->
    {stop, {unknown_cast, Msg}, St}.

handle_info(timeout, #st{data=Data, last_flush=LastFlush, metric=Metric}=St) ->
    {Megaseconds, Seconds, _} = erlang:now(),
    Epoch = Megaseconds * 1000000 + Seconds,
    DeltaT = Epoch - LastFlush,
    avern_scheduler:schedule(Metric, gb_trees:size(Data), DeltaT),
    {noreply, St};
handle_info(Msg, St) ->
    {stop, {unknown_info, Msg}, St}.

terminate(_Reason, _St) ->
    ok.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

-spec read_from_disk(binary(), pos_integer(), pos_integer(), any()) -> list().
read_from_disk(Metric, From, To, LevelDB) ->
    Ref = proplists:get_value(ref, LevelDB),
    ReadOpts = proplists:get_value(read_opts, LevelDB),
    ObjectKey = avern_encoding:encode_object_key(Metric, From, []),
    Opts = [{first_key, ObjectKey}|ReadOpts],
    Folder = fun({Key, EncodedValue}, Acc) ->
        case avern_encoding:decode_object_key(Key) of
            {ok, {Metric, Timestamp, Tags}} when Timestamp =< To ->
                Value = avern_encoding:decode_object_value(EncodedValue),
                [{{Metric, Timestamp, Tags}, Value}|Acc];
            {ok, {Metric, _, _}} ->
                throw({break, Acc});
            _ -> Acc
        end
    end,
    try
        eleveldb:fold(Ref, Folder, [], Opts)
    catch
        {break, Acc} -> Acc
    end.

-spec format_writes(gb_set()) -> list().
format_writes(Data) ->
    format_writes(Data, []).

-spec format_writes(gb_set(), list()) -> list().
format_writes({0, nil}, Operations) ->
    Operations;
format_writes(Data, Operations) ->
    {{Key, Value}, Data1} = gb_sets:take_smallest(Data),
    format_writes(Data1, [{put, Key, Value}|Operations]).
