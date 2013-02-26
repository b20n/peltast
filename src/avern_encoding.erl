-module(avern_encoding).
-behaviour(gen_server).

-export([
    encode_object_value/1,
    decode_object_value/1,
    encode_object_key/3,
    decode_object_key/1
]).

-export([
    start_link/1,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("avern.hrl").

-record(st, {
    leveldb
}).


-spec encode_object_value(number()) -> binary().
encode_object_value(Value) ->
    <<Value:64/signed-float>>.

-spec decode_object_value(binary()) -> float().
decode_object_value(EncodedValue) ->
    <<Value:64/signed-float>> = EncodedValue,
    Value.

-spec encode_object_key(binary(), pos_integer(), tags()) -> binary().
encode_object_key(Metric, Timestamp, Tags) ->
    TagIDs = lists:foldl(fun({TagKey, TagValue}, Acc) ->
        EncodedTag = encode_tag(TagKey, TagValue),
        <<EncodedTag/binary, Acc/binary>>
    end, <<>>, Tags),
    {ok, MetricID} = encode(metric, Metric),
    <<
        MetricID:24/unsigned-integer,
        Timestamp:32/unsigned-integer,
        TagIDs/binary
    >>.

-spec decode_object_key(binary()) ->
    {ok, {binary(), pos_integer(), tags()}} | {error, malformatted_key}.
decode_object_key(BinaryKey) ->
    try
        <<
            MetricID:24/unsigned-integer,
            Timestamp:32/unsigned-integer,
            TagIDs/binary
        >> = BinaryKey,
        {ok, Metric} = decode(metric, MetricID),
        Tags = decode_tag_binary(TagIDs),
        {ok, {Metric, Timestamp, Tags}}
    catch
        _Error:_Reason ->
            % TODO: logme
            {error, malformatted_key}
    end.

start_link(LevelDB) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, LevelDB, []).

init(LevelDB) ->
    {ok, #st{
        leveldb = LevelDB
    }}.

handle_call({encode, Type, Value}, _From, #st{leveldb=LevelDB}=St) ->
    Ref = proplists:get_value(ref, LevelDB),
    ReadOpts = proplists:get_value(read_opts, LevelDB),
    WriteOpts = proplists:get_value(write_opts, LevelDB),
    Key = term_to_binary({Type, Value}),
    Id = case eleveldb:get(Ref, Key, ReadOpts) of
        {ok, Id0} -> binary:decode_unsigned(Id0);
        not_found ->
            %% TODO: instrument
            CounterKey = term_to_binary(Type),
            LastId = case eleveldb:get(Ref, CounterKey, ReadOpts) of
                {ok, LastIdBin} -> binary:decode_unsigned(LastIdBin);
                not_found -> 0
            end,
            Id0 = LastId + 1,
            Id0Bin = binary:encode_unsigned(Id0),
            LookupKey = term_to_binary({Type, Id0}),
            Operations = [
                {put, Key, Id0Bin},
                {put, CounterKey, Id0Bin},
                {put, LookupKey, Key}
            ],
            ok = eleveldb:write(Ref, Operations, WriteOpts),
            Id0
    end,
    {reply, {ok, Id}, St};
handle_call({decode, Type, ID}, _From, #st{leveldb=LevelDB}=St) ->
    Ref = proplists:get_value(ref, LevelDB),
    ReadOpts = proplists:get_value(read_opts, LevelDB),
    Key = term_to_binary({Type, ID}),
    case eleveldb:get(Ref, Key, ReadOpts) of
        {ok, ValueBin} ->
            {Type, Value} = binary_to_term(ValueBin),
            {reply, {ok, Value}, St};
        not_found ->
            {reply, {error, not_found}, St}
    end;
handle_call(Msg, _From, St) ->
    {stop, {unknown_call, Msg}, error, St}.

handle_cast(Msg, St) ->
    {stop, {unknown_cast, Msg}, St}.

handle_info(Msg, St) ->
    {stop, {unknown_info, Msg}, St}.

terminate(_Reason, _St) ->
    ok.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

-spec encode(atom(), binary()) -> {atom(), pos_integer()}.
encode(Type, Value) ->
    gen_server:call(?MODULE, {encode, Type, Value}).

-spec decode(atom(), pos_integer()) -> {atom(), binary()}.
decode(Type, Value) ->
    gen_server:call(?MODULE, {decode, Type, Value}).

-spec encode_tag(binary(), binary()) -> binary().
encode_tag(Key, Value) ->
    {ok, KeyID} = encode(tag_key, Key),
    {ok, ValueID} = encode(tag_value, Value),
    <<KeyID:24/unsigned-integer, ValueID:24/unsigned-integer>>.

-spec decode_tag(pos_integer(), pos_integer()) -> {binary(), binary()}.
decode_tag(KeyID, ValueID) ->
    {ok, Key} = decode(tag_key, KeyID),
    {ok, Value} = decode(tag_value, ValueID),
    {Key, Value}.

-spec decode_tag_binary(binary()) -> tags().
decode_tag_binary(TagIDs) ->
    decode_tag_binary([], TagIDs).

-spec decode_tag_binary(tags(), binary()) -> tags().
decode_tag_binary(Acc, <<>>) ->
    lists:reverse(Acc);
decode_tag_binary(Acc, TagIDs) ->
    <<
        KeyID:24/unsigned-integer,
        ValueID:24/unsigned-integer,
        Rest/binary
    >> = TagIDs,
    decode_tag_binary([decode_tag(KeyID, ValueID)|Acc], Rest).
