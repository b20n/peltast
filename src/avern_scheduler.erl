-module(avern_scheduler).
-behaviour(gen_server).

-export([
    schedule/3
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
    concurrency,
    inflight,
    queues
}).

-spec schedule(binary(), integer(), integer()) -> ok.
schedule(Metric, Count, DeltaT) ->
    gen_server:cast(?MODULE, {schedule, Metric, Count, DeltaT}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    Queues = [{by_count, avern_pq:new()}, {by_time, avern_pq:new()}],
    {ok, #st{
        concurrency = 1,
        inflight = 0,
        queues = Queues
    }}.

handle_call(Msg, _From, St) ->
    {stop, {unknown_call, Msg}, error, St}.

handle_cast({schedule, Metric, Count, DeltaT}, #st{queues=Queues}=State) ->
    Queues1 = lists:map(
        fun({Type, Pq}) ->
            case Type of
                by_count -> {Type, avern_pq:push(Metric, Count, Pq)};
                by_time -> {Type, avern_pq:push(Metric, DeltaT, Pq)}
            end
        end,
        Queues
    ),
    {noreply, State#st{queues=Queues1}, 0};
handle_cast(Msg, St) ->
    {stop, {unknown_cast, Msg}, St}.

handle_info(timeout, #st{queues=Q, inflight=I, concurrency=C}=State) ->
    {Q1, I1} = maybe_schedule_flush(Q, I, C),
    {noreply, State#st{queues=Q1, inflight=I1}};
handle_info({'EXIT', _From, _Reason}, #st{inflight=I}=State) ->
    {noreply, State#st{inflight=I-1}, 0};
handle_info(Msg, St) ->
    {stop, {unknown_info, Msg}, St}.

terminate(_Reason, _St) ->
    ok.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

-spec maybe_schedule_flush(list(), integer(), integer()) -> {list(), integer()}.
maybe_schedule_flush(Q, I, C) when I >= C ->
    {Q, I};
maybe_schedule_flush([{Type, Pq}|Rest]=Q, I, C) ->
    case avern_pq:pop(Pq) of
        {undefined, _} ->
            {Q, I};
        {Metric, Pq1} ->
            spawn_link(avern_queue, flush, [Metric]),
            Rest1 = lists:map(
                fun({T, P}) -> {T, avern_pq:delete(Metric, P)} end,
                Rest
            ),
            maybe_schedule_flush(Rest1 ++ [{Type, Pq1}], I + 1, C)
    end.

