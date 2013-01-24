-module(avern).

-export([start/0]).

start() ->
    application:start(gproc),
    application:start(avern).
