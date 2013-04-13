-module(avern_scheduler).
-behaviour(gen_server).

-export([
    register/1,
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
    pids,
    wps,
    tref
}).

register(Pid) ->
    gen_server:cast(?MODULE, {register, Pid}).

set_wps(N) ->
    gen_server:call(?MODULE, {set_wps, N}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    {ok, Tref} = timer:send_interval(1000, self(), schedule),
    {ok, #st{
        pids = queue:new(),
        wps = 5,
        tref = Tref
    }}.

handle_call({set_wps, WPS}, _From, State) ->
    {reply, ok, State#st{wps=WPS}};
handle_call(Msg, _From, State) ->
    {stop, {unknown_call, Msg}, error, State}.

handle_cast({register, Pid}, #st{pids=Pids}=State) ->
    erlang:monitor(process, Pid),
    {noreply, State#st{pids=queue:in(Pid, Pids)}};
handle_cast(Msg, State) ->
    {stop, {unknown_cast, Msg}, State}.

handle_info(schedule, #st{pids=Pids, wps=WPS}=State) ->
    {ToSchedule, Pids1} = cycle(WPS, Pids),
    Gap = trunc(1000 / WPS),
    Count = min(length(ToSchedule), WPS),
    Times = [Gap * N || N <- lists:seq(1, Count)],
    [timer:send_after(T, P, flush) || {T, P} <- lists:zip(Times, ToSchedule)],
    {noreply, State#st{pids=Pids1}};
handle_info({'EXIT', _, _, Pid, _}, #st{pids=Pids}=State) ->
    Pids1 = queue:filter(fun(I) -> I =/= Pid end, Pids),
    {noreply, State#st{pids=Pids1}};
handle_info(Msg, State) ->
    {stop, {unknown_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

cycle(N, Q) ->
    cycle(N, Q, []).

cycle(0, Q, Acc) ->
    {lists:usort(Acc), Q};
cycle(N, Q, Acc) ->
    case queue:out(Q) of
        {{value, I}, Q1} ->
            cycle(N-1, queue:in(I, Q1), [I|Acc]);
        {empty, Q} ->
            cycle(0, Q, Acc)
    end.
