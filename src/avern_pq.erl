-module(avern_pq).

-type pq() :: {gb_tree(), dict()}.

-export([
    new/0,
    push/3,
    pop/1,
    delete/2
]).

-spec new() -> pq().
new() ->
    {gb_trees:empty(), dict:new()}.

-spec push(any(), any(), pq()) -> pq().
push(Key, Value, {Tree, Dict}) ->
    Tree1 = case dict:find(Key, Dict) of
        {ok, TreeKey} ->
            gb_trees:delete_any(TreeKey, Tree);
        error ->
            Tree
    end,
    TreeKey1 = {Value, erlang:now()},
    {gb_trees:enter(TreeKey1, Key, Tree1), dict:store(Key, TreeKey1, Dict)}.

-spec pop(pq()) -> {any(), pq()}.
pop({Tree, Dict}) ->
    case gb_trees:is_empty(Tree) of
        true ->
            {undefined, {Tree, Dict}};
        false ->
            {{_, _}, Key, Tree1} = gb_trees:take_largest(Tree),
            {Key, {Tree1, dict:erase(Key, Dict)}}
    end.

-spec delete(any(), pq()) -> pq().
delete(Key, {Tree, Dict}) ->
    case dict:find(Key, Dict) of
        {ok, TreeKey} ->
            {gb_trees:delete_any(TreeKey, Tree), dict:erase(Key, Dict)};
        error ->
            {Tree, Dict}
    end.
