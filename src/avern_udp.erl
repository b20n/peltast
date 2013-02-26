-module(avern_udp).

-export([start_link/0, init/1]).

-define(SOCKOPTS, [binary, {reuseaddr, true}, {active, false}]).
-define(MAX_LENGTH, 1024).
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
            proc_lib:init_ack({ok, self()}),
            loop(Socket, []);
        {error, Reason} ->
            {stop, Reason}
    end.

loop(Socket, Buf) when length(Buf) =:= ?PROCESSING_BUFSIZE ->
    % TODO: instrument me
    spawn(avern_worker, process, [Buf]),
    loop(Socket, []);
loop(Socket, Buf) ->
    case gen_udp:recv(Socket, ?MAX_LENGTH, 10) of
        {ok, {_Address, _Port, Data}} ->
            loop(Socket, [Data|Buf]);
        {error, timeout} ->
            loop(Socket, Buf)
    end.
