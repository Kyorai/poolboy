%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy).
-behaviour(gen_statem).

-export([checkout/1, checkout/2, checkout/3, checkin/2, transaction/2,
         child_spec/2, child_spec/3, start/1, start/2, start_link/1,
         start_link/2, stop/1, status/1]).
-export([init/1, ready/3, overflow/3, full/3,
         callback_mode/0,  terminate/3,
         code_change/4]).
-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{gen_statem, pulse_gen_statem}, %%FIXME: Not sure if pulse already provides customized versions of gen_statem
                                 {gen_server, pulse_gen_server},
                                 {supervisor, pulse_supervisor}]}).
-endif.


-include_lib("eunit/include/eunit.hrl").
-define(TIMEOUT, 5000).

-ifdef(namespaced_types).
-type poolboy_queue() :: queue:queue().
-else.
-type poolboy_queue() :: queue().
-endif.

-record(state, {
    supervisor :: pid() | undefined,
    workers :: poolboy_queue() | undefined,
    waiting :: poolboy_queue(),
    monitors :: ets:tid(),
    size = 5 :: non_neg_integer(),
    overflow = 0 :: non_neg_integer(),
    max_overflow = 10 :: non_neg_integer()
}).

-spec checkout(Pool :: node()) -> pid().
checkout(Pool) ->
    checkout(Pool, true).

-spec checkout(Pool :: node(), Block :: boolean()) -> pid() | full.
checkout(Pool, Block) ->
    checkout(Pool, Block, ?TIMEOUT).

-spec checkout(Pool :: node(), Block :: boolean(), Timeout :: timeout())
    -> pid() | full.
checkout(Pool, Block, Timeout) ->
    gen_statem:call(Pool, {checkout, Block, Timeout},{dirty_timeout, Timeout}).

-spec checkin(Pool :: node(), Worker :: pid()) -> ok.
checkin(Pool, Worker) when is_pid(Worker) ->
    gen_statem:cast(Pool, {checkin, Worker}).

-spec transaction(Pool :: node(), Fun :: fun((Worker :: pid()) -> any()))
    -> any().
transaction(Pool, Fun) ->
    Worker = poolboy:checkout(Pool),
    try
        Fun(Worker)
    after
        ok = poolboy:checkin(Pool, Worker)
    end.

-spec child_spec(Pool :: node(), PoolArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(Pool, PoolArgs) ->
    child_spec(Pool, PoolArgs, []).

-spec child_spec(Pool :: node(),
                 PoolArgs :: proplists:proplist(),
                 WorkerArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(Pool, PoolArgs, WorkerArgs) ->
    {Pool, {poolboy, start_link, [PoolArgs, WorkerArgs]},
     permanent, 5000, worker, [poolboy]}.

-spec start(PoolArgs :: proplists:proplist())
    -> {ok, pid()}.
start(PoolArgs) ->
    start(PoolArgs, PoolArgs).

-spec start(PoolArgs :: proplists:proplist(),
            WorkerArgs:: proplists:proplist())
    -> {ok, pid()}.
start(PoolArgs, WorkerArgs) ->
    start_pool(start, PoolArgs, WorkerArgs).

-spec start_link(PoolArgs :: proplists:proplist())
    -> {ok, pid()}.
start_link(PoolArgs)  ->
    %% for backwards compatability, pass the pool args as the worker args as well
    start_link(PoolArgs, PoolArgs).

-spec start_link(PoolArgs :: proplists:proplist(),
                 WorkerArgs:: proplists:proplist())
    -> {ok, pid()}.
start_link(PoolArgs, WorkerArgs)  ->
    start_pool(start_link, PoolArgs, WorkerArgs).

-spec stop(Pool :: node()) -> ok.
stop(Pool) ->
    gen_statem:call(Pool, stop).

-spec status(Pool :: node()) -> {atom(), integer(), integer(), integer()}.
status(Pool) ->
    gen_statem:call(Pool, status).

init({PoolArgs, WorkerArgs}) ->
    process_flag(trap_exit, true),
    Waiting = queue:new(),
    Monitors = ets:new(monitors, [private]),
    init(PoolArgs, WorkerArgs, #state{waiting=Waiting, monitors=Monitors}).

init([{worker_module, Mod} | Rest], WorkerArgs, State) when is_atom(Mod) ->
    {ok, Sup} = poolboy_sup:start_link(Mod, WorkerArgs),
    init(Rest, WorkerArgs, State#state{supervisor=Sup});
init([{size, Size} | Rest], WorkerArgs, State) when is_integer(Size) ->
    init(Rest, WorkerArgs, State#state{size=Size});
init([{max_overflow, MaxOverflow} | Rest], WorkerArgs, State) when is_integer(MaxOverflow) ->
    init(Rest, WorkerArgs, State#state{max_overflow=MaxOverflow});
init([_ | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State);
init([], _WorkerArgs, #state{size=Size, supervisor=Sup, max_overflow=MaxOverflow}=State) ->
    Workers = prepopulate(Size, Sup),
    StartState = case Size of
        Size when Size < 1, MaxOverflow < 1 -> full;
        Size when Size < 1 -> overflow;
        Size -> ready
    end,
    {ok, StartState, State#state{workers=Workers}}.

callback_mode() ->
    state_functions.

ready(cast, {checkin, Pid}, State) ->
    Monitors = State#state.monitors,
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            Workers = queue:in(Pid, State#state.workers),
            {next_state, ready, State#state{workers=Workers}};
        [] ->
            {next_state, ready, State}
    end;

ready({call, {FromPid, _}=From}, {checkout, Block, Timeout}, State) ->
    #state{supervisor = Sup,
           workers = Workers,
           monitors = Monitors,
           max_overflow = MaxOverflow} = State,
    case queue:out(Workers) of
        {{value, Pid}, Left} ->
            Ref = erlang:monitor(process, FromPid),
            true = ets:insert(Monitors, {Pid, Ref}),
            NextState = case queue:is_empty(Left) of
                true when MaxOverflow < 1 -> full;
                true -> overflow;
                false -> ready
            end,
            {next_state, NextState, State#state{workers=Left}, [{reply, From, Pid}]};
        {empty, Empty} when MaxOverflow > 0 ->
            {Pid, Ref} = new_worker(Sup, FromPid),
            true = ets:insert(Monitors, {Pid, Ref}),
            {next_state, overflow, State#state{workers=Empty, overflow=1}, [{reply, From, Pid}]};
        {empty, Empty} when Block =:= false ->
            {next_state, full, State#state{workers=Empty}, [{reply, From, full}]};
        {empty, Empty} ->
            Waiting = add_waiting(From, Timeout, State#state.waiting),
            {next_state, full, State#state{workers=Empty, waiting=Waiting}}
    end;
ready(EventType, Event, State) ->
    handle_common_event(EventType, Event, ready, State).

overflow(cast, {checkin, Pid}, #state{overflow=0}=State) ->
    Monitors = State#state.monitors,
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            NextState = case State#state.size > 0 of
                true  -> ready;
                false -> overflow
            end,
            Workers = queue:in(Pid, State#state.workers),
            {next_state, NextState, State#state{overflow=0, workers=Workers}};
        [] ->
            {next_state, overflow, State}
    end;
overflow(cast, {checkin, Pid}, State) ->
    #state{supervisor=Sup, monitors=Monitors, overflow=Overflow} = State,
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            ok = dismiss_worker(Sup, Pid),
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            {next_state, overflow, State#state{overflow=Overflow-1}};
        [] ->
            {next_state, overflow, State}
    end;

overflow({call, From}, {checkout, Block, Timeout},
         #state{overflow=Overflow,
                max_overflow=MaxOverflow}=State) when Overflow >= MaxOverflow ->
    case Block of
        true ->
            Waiting = add_waiting(From, Timeout, State#state.waiting),
            {next_state, full, State#state{waiting=Waiting}};
        false ->
            {next_state, full, State, [{reply, From, full}]}
    end;
overflow({call,{FromPid, _}=From}, {checkout, _Block, _Timeout}, State) ->
    #state{supervisor = Sup,
           overflow = Overflow,
           max_overflow = MaxOverflow} = State,
    {Pid, Ref} = new_worker(Sup, FromPid),
    true = ets:insert(State#state.monitors, {Pid, Ref}),
    NewOverflow = Overflow + 1,
    NextState = case NewOverflow >= MaxOverflow of
        true  -> full;
        false -> overflow
    end,
    {next_state, NextState, State#state{overflow=NewOverflow}, [{reply, From, Pid}]};
overflow(EventType, Event, State) ->
    handle_common_event(EventType, Event, overflow, State).

full(cast, {checkin, Pid}, State) ->
    #state{monitors = Monitors} = State,
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            checkin_while_full(Pid, State);
        [] ->
            {next_state, full, State}
    end;

full({call, From}, {checkout, true, Timeout},  State) ->
    Waiting = add_waiting(From, Timeout, State#state.waiting),
    {next_state, full, State#state{waiting=Waiting}};
full({call, From}, {checkout, false, _Timeout},  State) ->
    {next_state, full, State, [{reply, From, full}]};
full(EventType, Event, State) ->
    handle_common_event(EventType, Event, full, State).

handle_common_event(cast, _EventData, StateName, State) ->
    {next_state, StateName, State};

handle_common_event({call, From}, status, StateName, State) ->
    {keep_state_and_data, [{reply, From, {StateName, queue:len(State#state.workers), State#state.overflow,
             ets:info(State#state.monitors, size)}}]};
handle_common_event({call, From}, get_avail_workers, StateName, State) ->
    Workers = State#state.workers,
    WorkerList = queue:to_list(Workers),
    {next_state, StateName, State, [{reply, From, WorkerList}]};

handle_common_event({call, From}, get_all_workers, _StateName, State) ->
    Sup = State#state.supervisor,
    WorkerList = supervisor:which_children(Sup),
    {keep_state_and_data, [{reply, From, WorkerList}]};
handle_common_event({call, From}, get_all_monitors, _StateName, State) ->
    Monitors = ets:tab2list(State#state.monitors),
    {keep_state_and_data, [{reply, From, Monitors}]};
handle_common_event({call, From}, stop, _StateName, State) ->
    Sup = State#state.supervisor,
    true = exit(Sup, shutdown),
    {stop_and_reply, normal, {reply, From, ok}, State};
handle_common_event({call, From}, _Event, _StateName, _State) ->
    Reply = {error, invalid_message},
    {keep_state_and_data, [{reply, From, Reply}]};

handle_common_event(info, {'DOWN', Ref, _, _, _}, StateName, State) ->
    case ets:match(State#state.monitors, {'$1', Ref}) of
        [[Pid]] ->
            Sup = State#state.supervisor,
            ok = supervisor:terminate_child(Sup, Pid),
            %% Don't wait for the EXIT message to come in.
            %% Deal with the worker exit right now to avoid
            %% a race condition with messages waiting in the
            %% mailbox.
            true = ets:delete(State#state.monitors, Pid),
            handle_worker_exit(Pid, StateName, State);
        [] ->
            {next_state, StateName, State}
    end;
handle_common_event(info, {'EXIT', Pid, _Reason}, StateName, State) ->
    #state{supervisor = Sup,
           monitors = Monitors} = State,
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            handle_worker_exit(Pid, StateName, State);
        [] ->
            case queue:member(Pid, State#state.workers) of
                true ->
                    W = queue:filter(fun (P) -> P =/= Pid end, State#state.workers),
                    {next_state, StateName, State#state{workers=queue:in(new_worker(Sup), W)}};
                false ->
                    {next_state, StateName, State}
            end
    end;
handle_common_event(info, _Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

start_pool(StartFun, PoolArgs, WorkerArgs) ->
    case proplists:get_value(name, PoolArgs) of
        undefined ->
            gen_statem:StartFun(?MODULE, {PoolArgs, WorkerArgs}, []);
        Name ->
            gen_statem:StartFun(Name, ?MODULE, {PoolArgs, WorkerArgs}, [])
    end.

new_worker(Sup) ->
    {ok, Pid} = supervisor:start_child(Sup, []),
    true = link(Pid),
    Pid.

new_worker(Sup, FromPid) ->
    Pid = new_worker(Sup),
    Ref = erlang:monitor(process, FromPid),
    {Pid, Ref}.

dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

prepopulate(N, _Sup) when N < 1 ->
    queue:new();
prepopulate(N, Sup) ->
    prepopulate(N, Sup, queue:new()).

prepopulate(0, _Sup, Workers) ->
    Workers;
prepopulate(N, Sup, Workers) ->
    prepopulate(N-1, Sup, queue:in(new_worker(Sup), Workers)).

add_waiting(Pid, Timeout, Queue) ->
    queue:in({Pid, Timeout, os:timestamp()}, Queue).

wait_valid(infinity, _Timeout) ->
    true;
wait_valid(StartTime, Timeout) ->
    Waited = timer:now_diff(os:timestamp(), StartTime),
    (Waited div 1000) < Timeout.

checkin_while_full(Pid, State) ->
    #state{supervisor = Sup,
           waiting = Waiting,
           monitors = Monitors,
           max_overflow = MaxOverflow,
           overflow = Overflow} = State,
    case queue:out(Waiting) of
        {{value, {{FromPid, _}=From, Timeout, StartTime}}, Left} ->
            case wait_valid(StartTime, Timeout) of
                true ->
                    Ref1 = erlang:monitor(process, FromPid),
                    true = ets:insert(Monitors, {Pid, Ref1}),
                    gen_statem:reply(From, Pid),
                    {next_state, full, State#state{waiting=Left}};
                false ->
                    checkin_while_full(Pid, State#state{waiting=Left})
            end;
        {empty, Empty} when MaxOverflow < 1 ->
            Workers = queue:in(Pid, State#state.workers),
            {next_state, ready, State#state{workers=Workers,
                                            waiting=Empty}};
        {empty, Empty} ->
            ok = dismiss_worker(Sup, Pid),
            {next_state, overflow, State#state{waiting=Empty,
                                               overflow=Overflow-1}}
    end.

handle_worker_exit(Pid, StateName, State) ->
    #state{supervisor = Sup,
           overflow = Overflow,
           waiting = Waiting,
           monitors = Monitors,
           max_overflow = MaxOverflow} = State,
    case StateName of
        ready ->
            W = queue:filter(fun (P) -> P =/= Pid end, State#state.workers),
            {next_state, ready, State#state{workers=queue:in(new_worker(Sup), W)}};
        overflow when Overflow =:= 0 ->
            W = queue:filter(fun (P) -> P =/= Pid end, State#state.workers),
            {next_state, ready, State#state{workers=queue:in(new_worker(Sup), W)}};
        overflow ->
            {next_state, overflow, State#state{overflow=Overflow-1}};
        full when MaxOverflow < 1 ->
            case queue:out(Waiting) of
                {{value, {{FromPid, _}=From, Timeout, StartTime}}, LeftWaiting} ->
                    case wait_valid(StartTime, Timeout) of
                        true ->
                            MonitorRef = erlang:monitor(process, FromPid),
                            NewWorker = new_worker(Sup),
                            true = ets:insert(Monitors, {NewWorker, MonitorRef}),
                            gen_statem:reply(From, NewWorker),
                            {next_state, full, State#state{waiting=LeftWaiting}};
                        false ->
                            handle_worker_exit(Pid, StateName, State#state{waiting=LeftWaiting})
                    end;
                {empty, Empty} ->
                    Workers2 = queue:in(new_worker(Sup), State#state.workers),
                    {next_state, ready, State#state{waiting=Empty,
                                                    workers=Workers2}}
            end;
        full when Overflow =< MaxOverflow ->
            case queue:out(Waiting) of
                {{value, {{FromPid, _}=From, Timeout, StartTime}}, LeftWaiting} ->
                    case wait_valid(StartTime, Timeout) of
                        true ->
                            MonitorRef = erlang:monitor(process, FromPid),
                            NewWorker = new_worker(Sup),
                            true = ets:insert(Monitors, {NewWorker, MonitorRef}),
                            gen_statem:reply(From, NewWorker),
                            {next_state, full, State#state{waiting=LeftWaiting}};
                        _ ->
                            handle_worker_exit(Pid, StateName, State#state{waiting=LeftWaiting})
                    end;
                {empty, Empty} ->
                    {next_state, overflow, State#state{overflow=Overflow-1,
                                                       waiting=Empty}}
            end;
        full ->
            {next_state, full, State#state{overflow=Overflow-1}}
    end.
