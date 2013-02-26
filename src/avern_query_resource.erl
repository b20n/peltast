-module(avern_query_resource).

-export([
    init/1,
    content_types_provided/2,
    allowed_methods/2,
    malformed_request/2,
    to_json/2
]).

-record(st, {
    result,
    metric,
    from,
    to,
    tags
}).

-include_lib("webmachine/include/webmachine.hrl").

init([]) ->
    {ok, #st{}}.

content_types_provided(Req, State) ->
    {[{"application/json", to_json}], Req, State}.

allowed_methods(Req, State) ->
    {['GET'], Req, State}.

malformed_request(Req, State) ->
    Metric = list_to_binary(wrq:path_info(metric, Req)),
    QS = wrq:req_qs(Req),
    From = case proplists:get_value("from", QS) of
        undefined -> 0;
        FromList -> list_to_integer(FromList)
    end,
    To = case proplists:get_value("to", QS) of
        undefined -> avern_util:now();
        ToList -> list_to_integer(ToList)
    end,
    {_, RawTags} = proplists:split(QS, ["from", "to"]),
    Tags = lists:map(
        fun({Key, Value}) -> {list_to_binary(Key), list_to_binary(Value)} end,
        RawTags
    ),
    Result = avern_query:execute(Metric, From, To, Tags),
    {false, Req, State#st{
        result = Result,
        metric = Metric,
        from = From,
        to = To,
        tags = Tags
    }}.

to_json(Req, #st{result=Result}=State) ->
    {jiffy:encode(to_ejson(Result)), Req, State}.

to_ejson([{_, _}|_]=Proplist) ->
    EJSONProps = lists:map(
       fun({Key, Value}) -> {maybe_format_key(Key), to_ejson(Value)} end,
       Proplist
    ),
    {EJSONProps};
to_ejson(NotAProplist) ->
    NotAProplist.

maybe_format_key(Key) when is_integer(Key) ->
    maybe_format_key(integer_to_list(Key));
maybe_format_key(Key) when is_list(Key) ->
    list_to_binary(Key);
maybe_format_key(Key) ->
    Key.
