-module(avern_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    avern_sup:start_link().

stop(_State) ->
    ok.
