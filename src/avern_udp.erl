-module(avern_udp).

-export([start_link/0, init/1]).

-define(SOCKOPTS, [binary, {active, false}, {recbuf, 128 * 1024 * 1024}]).
-define(MAX_LENGTH, 2048).
-define(PROCESSING_BUFSIZE, 100).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Interface = case application:get_env(avern, interface) of
        {ok, I} -> I;
        undefined -> {127, 0, 0, 1}
    end,
    Port = case application:get_env(avern, port) of
        {ok, P} -> P;
        undefined -> 7777
    end,
    case gen_udp:open(Port, [{ip, Interface}|?SOCKOPTS]) of
        {ok, Socket} ->
            {ok, [{recbuf, RecBuf}]} = inet:getopts(Socket, [recbuf]),
            inet:setopts(Socket, [{buffer, RecBuf}]),
            proc_lib:init_ack({ok, self()}),
            loop(Socket, [], 0);
        {error, Reason} ->
            {stop, Reason}
    end.

loop(Socket, Buf, N) when N >= ?PROCESSING_BUFSIZE ->
    spawn(avern_worker, process, [Buf]),
    graf:increment_counter([avern, metrics_received], ?PROCESSING_BUFSIZE),
    loop(Socket, [], 0);
loop(Socket, Buf, N) ->
    case gen_udp:recv(Socket, ?MAX_LENGTH, 1000) of
        {ok, {_Address, _Port, Data}} ->
            loop(Socket, [Data|Buf], N + 1);
        {error, timeout} ->
            loop(Socket, Buf, N)
    end.
