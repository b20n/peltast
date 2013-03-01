-module(avern_worker).

-export([process/1]).

-spec process([binary()]) -> ok.
process([Packet|Rest]) ->
    Metrics = binary:split(Packet, <<"\n">>, [global, trim]),
    process_metrics(Metrics),
    process(Rest);
process([]) ->
    ok.

-spec process_metrics([binary()]) -> ok.
process_metrics([L|Rest]) ->
    [Metric, TimestampBin, ValueBin|Tags0] = binary:split(L, <<" ">>, [global]),
    Tags = lists:map(
        fun(KV) -> list_to_tuple(binary:split(KV, <<"=">>)) end,
        Tags0
    ),
    Timestamp = list_to_integer(binary_to_list(TimestampBin)),
    Value = binary_to_number(ValueBin),
    avern_queue:update(Metric, Timestamp, Value, Tags),
    folsom_metrics:notify([avern, incoming_metrics], 1),
    process_metrics(Rest);
process_metrics([]) ->
    ok.

-spec binary_to_number(binary()) -> number().
binary_to_number(Bin) ->
    Str = binary_to_list(Bin),
    try list_to_float(Str)
    catch error:badarg -> list_to_integer(Str)
    end.
