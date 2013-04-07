-module(avern_scheduler).
-behaviour(gen_server).

-export([
    schedule/0,
    update/2
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
    ewma,
    current_period,
    minimum_period,
    tref
}).

-define(ALPHA, 1 - math:exp(-5 / 60)).

-spec schedule() -> {ok, pos_integer()}.
schedule() ->
    gen_server:call(?MODULE, schedule).

-spec update(pos_integer(), pos_integer()) -> ok.
update(WriteCount, Duration) ->
    gen_server:call(?MODULE, {update, WriteCount, Duration}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #st{
        ewma = 0,
        current_period = 1000
    }}.

handle_call(schedule, _From, State) ->
    Period = round(State#st.current_period),
    Period1 = Period + random:uniform(50),
    {reply, {ok, Period1}, State};
handle_call({update, WriteCount, Duration}, _From, State) ->
    #st{
        ewma = EWMA,
        current_period = CP
    } = State,
    Current = WriteCount / Duration,
    EWMA1 = EWMA + (?ALPHA * (Current - EWMA)),
    io:format("Current: ~p, Old: ~p, New: ~p~n", [Current, EWMA, EWMA1]),
    Delta = EWMA1 - EWMA,
    Period = CP + Delta,
    {reply, ok, State#st{ewma=EWMA1, current_period=Period}};
handle_call(Msg, _From, State) ->
    {stop, {unknown_call, Msg}, error, State}.

handle_cast(Msg, State) ->
    {stop, {unknown_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unknown_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
