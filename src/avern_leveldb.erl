-module(avern_leveldb).

-export([
    delete/3,
    read/4,
    write/3
]).

-spec delete(binary(), pos_integer, list()) -> any().
delete(Metric, Until, LevelDB) ->
    Ref = proplists:get_value(ref, LevelDB),
    ReadOpts = proplists:get_value(read_opts, LevelDB),
    ObjectKey = avern_encoding:encode_object_key(Metric, 0, []),
    Opts = [{first_key, ObjectKey}|ReadOpts], % TODO: set fillcache = false
    Folder = fun({Key, _}, Acc) ->
        case avern_encoding:decode_object_key(Key) of
            {ok, {_, Timestamp, _}} when Timestamp =< Until ->
                [{delete, Key}|Acc];
            _  ->
                throw({break, Acc})
         end
    end,
    Deletes = try
        eleveldb:fold(Ref, Folder, [], Opts)
    catch
        {break, Acc} -> Acc
    end,
    do_write(Ref, Deletes, proplists:get_value(write_opts, LevelDB)).

-spec read(binary(), pos_integer(), pos_integer(), list()) -> list().
read(Metric, From, To, LevelDB) ->
    Ref = proplists:get_value(ref, LevelDB),
    ReadOpts = proplists:get_value(read_opts, LevelDB),
    ObjectKey = avern_encoding:encode_object_key(Metric, From, []),
    Opts = [{first_key, ObjectKey}|ReadOpts],
    Folder = fun({Key, EncodedValue}, Acc) ->
        case avern_encoding:decode_object_key(Key) of
            {ok, {Metric, Timestamp, Tags}} when Timestamp =< To ->
                Value = avern_encoding:decode_object_value(EncodedValue),
                [{{Metric, Timestamp, Tags}, Value}|Acc];
            _ ->
                throw({break, Acc})
        end
    end,
    try
        eleveldb:fold(Ref, Folder, [], Opts)
    catch
        {break, Acc} -> Acc
    end.

-spec write(binary(), gb_set(), list()) -> {ok, pos_integer()} | {error, any()}.
write(Metric, Data, LevelDB) ->
    Operations = format_writes(Metric, Data),
    Ref = proplists:get_value(ref, LevelDB),
    WriteOpts = proplists:get_value(write_opts, LevelDB),
    case do_write(Ref, Operations, WriteOpts) of
        ok ->
            {{{LatestTime, _}, _}, _} = gb_sets:take_largest(Data),
            {ok, LatestTime};
        {error, _Reason} ->
            {error, 0}
    end.


-spec format_writes(binary(), gb_set()) -> list().
format_writes(Metric, Data) ->
    format_writes(Metric, Data, []).

-spec format_writes(binary(), gb_set(), list()) -> list().
format_writes(Metric, Data, Operations) ->
    case gb_sets:is_empty(Data) of
        true ->
            Operations;
        false ->
            {{{Timestamp, Tags}, Value}, Data1} = gb_sets:take_smallest(Data),
            Key = avern_encoding:encode_object_key(Metric, Timestamp, Tags),
            EncodedValue = avern_encoding:encode_object_value(Value),
            format_writes(Metric, Data1, [{put, Key, EncodedValue}|Operations])
    end.


do_write(_, [], _) ->
    {error, empty};
do_write(Ref, Operations, Opts) ->
    Start = erlang:now(),
    case eleveldb:write(Ref, Operations, Opts) of
        ok ->
            Time = timer:now_diff(erlang:now(), Start),
            Count = length(Operations),
            folsom_metrics:notify([avern, write_size], Count),
            folsom_metrics:notify([avern, write_latency], Time),
            folsom_metrics:notify([avern, successful_write_ops], {inc, 1}),
            ok;
        {error, Reason} ->
            %% TODO: logme
            folsom_metrics:notify([avern, failed_write_ops], {inc, 1}),
            {error, Reason}
    end.
