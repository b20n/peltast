-module(avern_query).
-export([execute/4]).

-include("avern.hrl").

-spec execute(binary(), pos_integer(), pos_integer(), tags()) -> list().
execute(Metric, From, To, Tags) ->
    {ok, RawData} = avern_queue:read(Metric, From, To),
    FilteredGroups = filter_and_group(RawData, Tags),
    FilteredGroups.

-spec filter_and_group(list(), tags()) -> list().
filter_and_group(RawData, QueryTags) ->
    Folder = fun({{_, Timestamp, RowTags}, Value}, Groups) ->
        case choose_tag_group(QueryTags, RowTags) of
            undefined -> Groups;
            Group -> append(Group, Timestamp, Value, Groups)
        end
    end,
    Results = lists:foldl(Folder, orddict:new(), RawData),
    lists:map(
        fun({Group, Rows}) -> {Group, lists:reverse(Rows)} end,
        orddict:to_list(Results)
    ).

-spec append(tags(), pos_integer(), pos_integer(), [{tags(), list()}]) ->
    [{tags(), list()}].
append(Group, Ts, Val, Groups) ->
    orddict:update(Group, fun(O) -> [{Ts, Val}|O] end, [{Ts, Val}], Groups).

-spec choose_tag_group(tags(), tags()) -> tags() | undefined.
choose_tag_group(QueryTags, RowTags) ->
    choose_tag_group(QueryTags, RowTags, []).

-spec choose_tag_group(tags(), tags(), tags()) -> tags() | undefined.
choose_tag_group([], _, Acc) ->
    lists:reverse(Acc);
choose_tag_group(_, [], []) ->
    undefined;
choose_tag_group([{K, <<"*">>}|Query], [{K, V}|Row], Acc) ->
    choose_tag_group(Query, Row, [{K, V}|Acc]);
choose_tag_group([KV|Query], [KV|Row], Acc) ->
    choose_tag_group(Query, Row, [KV|Acc]);
choose_tag_group([{Key, V1}|_], [{Key, V2}|_], _) when V1 =/= V2 ->
    undefined;
choose_tag_group([Q|Query], [R|Row], Acc) when Q < R ->
    choose_tag_group(Query, [R|Row], Acc);
choose_tag_group([Q|Query], [R|Row], Acc) when Q > R ->
    choose_tag_group([Q|Query], Row, Acc).

