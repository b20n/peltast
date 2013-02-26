-module(avern_util).

-export([now/0]).

now() ->
    {Megaseconds, Seconds, _} = erlang:now(),
    Megaseconds * 1000000 + Seconds.
