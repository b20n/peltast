-module(avern).

-export([start/0]).

start() ->
    application:start(crypto),
    application:start(inets),
    application:start(mochiweb),
    application:start(webmachine),
    application:start(gproc),
    application:start(folsom),
    application:start(avern).
