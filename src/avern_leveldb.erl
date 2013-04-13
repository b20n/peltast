-module(avern_leveldb).

-export([
    delete/3,
    read/4,
    write/2
]).

-spec delete(binary(), number(), list()) -> any().
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
    WriteOpts = proplists:get_value(write_opts, LevelDB),
    do_write(Ref, Deletes, WriteOpts).

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

-spec write(list(), list()) -> ok | {error, any()}.
write(Data, LevelDB) ->
    Operations = format_write_operations(Data),
    Ref = proplists:get_value(ref, LevelDB),
    WriteOpts = proplists:get_value(write_opts, LevelDB),
    do_write(Ref, Operations, WriteOpts).

-spec format_write_operations(list()) -> list().
format_write_operations(Data) ->
    format_write_operations(Data, []).

-spec format_write_operations(list(), list()) -> list().
format_write_operations([], Operations) ->
    Operations;
format_write_operations([{Key, Value}|Data], Operations) ->
    format_write_operations(Data, [{put, Key, Value}|Operations]).

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
