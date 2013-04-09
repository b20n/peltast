-module(avern_scheduler).
-behaviour(gen_server).

-export([
    schedule/2,
    set_wps/1
]).
-export([
    start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(st, {
    waiting,
    wps,
    tref
}).

schedule(Pid, Msg) ->
    gen_server:cast(?MODULE, {schedule, Pid, Msg}).

set_wps(N) ->
    gen_server:call(?MODULE, {set_wps, N}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, Tref} = timer:send_interval(1000, self(), execute),
    {ok, #st{
        waiting = queue:new(),
        wps = 20,
        tref = Tref
    }}.

handle_call({set_wps, WPS}, _From, State) ->
    {reply, ok, State#st{wps=WPS}};
handle_call(Msg, _From, State) ->
    {stop, {unknown_call, Msg}, error, State}.

handle_cast({schedule, Pid, Msg}, #st{waiting=W}=State) ->
    %% {{value, {Pid1, Msg1}}, W2} = queue:out(W1),
    %% Pid1 ! Msg1,
    {noreply, State#st{waiting=queue:in({Pid, Msg}, W)}};
handle_cast(Msg, State) ->
    {stop, {unknown_cast, Msg}, State}.

handle_info(execute, #st{waiting=W, wps=WPS}=State) ->
    Gap = trunc(1000 / WPS),
    Times = [Gap * N || N <- lists:seq(1, WPS)],
    {Items, W1} = out_n(WPS, W),
    [timer:send_after(T, P, M) || {P, M} <- Items, T <- Times],
    {noreply, State#st{waiting=W1}};
handle_info(Msg, State) ->
    {stop, {unknown_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

out_n(N, Q) ->
    out_n(N, Q, []).

out_n(0, Q, Acc) ->
    {Acc, Q};
out_n(N, Q, Acc) ->
    case queue:out(Q) of
        {{value, I}, Q1} ->
            out_n(N-1, Q1, [I|Acc]);
        {empty, Q1} ->
            out_n(0, Q1, Acc)
    end.
