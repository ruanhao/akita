%%%-------------------------------------------------------------------
%%% @author ruan <ruanhao1116@gmail.com>
%%% @copyright (C) 2013, Ericsson
%%% @doc
%%% An OTP gen_server used hosted on all nodes to
%%% collect information locally.
%%% @end
%%% Created : 14 Aug 2013 by ruan <ruanhao1116@gmail.com>
%%%-------------------------------------------------------------------
-module(akita_collector_local).

-behaviour(gen_server).

%% API
-export([start_link/3, start_collect/0, stop_collect/0, quit/0, pull/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(DETS_FILE, filename:join(home(), "akita.record." ++ atom_to_list(node()))).

%% -include_lib("stdlib/include/ms_transform.hrl").

%% the config name specified in 'state' record must be the same
%% as that specified in 'env' entry in app.src file.
-record(state, {interval = 60000, topn = 10, 
                working = false, smp = true}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(From, Flag, Paras) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [From, Flag, Paras], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
start_collect() ->
    info(start_collect).

stop_collect() ->
    info(stop_collect).

pull(From) ->
    info({pull, From}).

quit() ->
    info(quit).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([From, boot, Paras]) ->
    error_logger:info_msg("start local init on node (~w)~n", [node()]),
    remove_old_dets(),
    %% just create a new dets file here,
    %% so we can append data into it, 
    %% but we close it for now.
    create_new_dets(From, local_init),
    {ok, init_config(Paras)};

init([From, reboot, Paras]) ->
    error_logger:info_msg("restart local init on node (~w)~n", [node()]),
    %% in case of there is no such file,
    %% the possibility is very small.
    create_new_dets(From, local_reboot),
    {ok, init_config(Paras)}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(start_collect, State) ->
    dets:open_file(?MODULE, [{file, ?DETS_FILE}]),
    start_sup(),
    error_logger:info_msg("start to collect info on node (~w)~n", [node()]),
    lazy_do(0, period_collect),
    {noreply, State#state{working = true}};

handle_info(period_collect, #state{interval = Intv, working = true} = State) ->
    akita_insert(generate_entry(State)),
    lazy_do(Intv, period_collect),
    {noreply, State};

handle_info(period_collect, #state{working = false} = State) -> 
    {noreply, State};

handle_info(stop_collect, State) ->
    dets:close(?MODULE),
    stop_sup(),
    error_logger:info_msg("stop collecting info on node (~w)~n", [node()]),
    dump_mailbox(),
    {noreply, State#state{working = false}};

handle_info({pull, From}, #state{working = false} = State) ->
    Basename = filename:basename(?DETS_FILE),
    From ! {pull_ack, self(), Basename},
    {noreply, State};

handle_info({pull_req, {Hostname, Port}}, State) ->
    case gen_tcp:connect(Hostname, Port, [binary, {packet, raw}]) of
        {ok, Sock} ->
            file:sendfile(?DETS_FILE, Sock),
            gen_tcp:close(Sock);
        {error, Reason} ->
            error_logger:error_msg("collector can not send file ~p (~w) ~n", [?DETS_FILE, Reason])
    end,
    {noreply, State};

handle_info(quit, State) ->
    dets:close(?MODULE),
    error_logger:info_msg("collector on node (~w) quits~n", [node()]),
    dump_mailbox(),
    {stop, normal, State#state{working = false}}; % set 'working' to 'false' just because of 'Obsessive Compulsive Disorder'

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    dets:close(?MODULE),                        % in order to save file
    stop_sup(),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================
home() -> 
    {ok, [[HOME]]} = init:get_argument(home),
    HOME.

akita_insert(V) -> 
    dets:insert(?MODULE, V).

epoch() -> 
%%  calendar:datetime_to_gregorian_seconds(calendar:universal_time())-719528*24*3600.
    calendar:local_time().

beam_pid() -> 
    os:getpid().

system(Cmd) ->
    From = self(),
    Timeout = 5000,
    Worker = 
        spawn(fun() ->
                      Res  = os:cmd(Cmd),
                      From ! {ok, Res} 
              end),
    receive
        {ok, Res} ->
            Res
    after
        Timeout ->
            exit(Worker, kill),
            fail
    end.

get_memutil() ->
    VmMemory = erlang:memory(total),
    [SysTotMemory] = [V || {system_total_memory, V} <- memsup:get_system_memory_data()],
    1.0 * VmMemory / SysTotMemory.

sys_info(true, true) ->
    Core = -1,                                  % because of SMP enabled
    CpuUtil = cpu_sup:util(),
    MemUtil = get_memutil(),
    {Core, CpuUtil, MemUtil};

sys_info(true, false) ->
    Shell = io_lib:format("ps -eo psr,pid | grep ~w | tail -n 1 | sed -e 's/^[[:space:]]*//' | awk '{print $1}'", [beam_pid()]),
    Affinity = system(Shell),
    Core = case Affinity of
               fail -> -1;
               Res -> list_to_integer(Res -- "\n")
           end,
    [CpuUtil] = [V || {C, V, _, _} <- cpu_sup:util([per_cpu]), C =:= Core],
    MemUtil = get_memutil(),
    {Core, CpuUtil, MemUtil};

sys_info(_OsSupport, _SmpSupport) ->
    {-1, -1, -1}.

get_proc_attr(Proc, Attr) ->
    case catch process_info(Proc, Attr) of 
        {Attr, V} -> V;
        {'EXIT', _} -> -9999;
        undefined -> -9999
    end.

compare(A, B, Attr) -> 
    compare(A, B, Attr, down).

compare(A, B, Attr, Direction) -> 
    [V1, V2] = [get_proc_attr(P, Attr) || P <- [A, B]],
    case Direction of 
        down -> V1 > V2;
        up -> V1 < V2
    end.

top_procs(Procs, Attr, TopN) -> 
    L = lists:sort(fun(A, B) -> compare(A, B, Attr) end, Procs),
    lists:sublist(L, TopN).

dump_all_proc(Procs) -> 
    [{P, process_info(P)} || P <- Procs].

generate_entry(#state{topn = TopN, smp = SmpSupport}) -> 
    {Core, CpuUtil, MemUtil} = sys_info(check_os(), SmpSupport),
    Epoch = epoch(),
    Procs = processes(),
    ErlangProcsMemTopList = top_procs(Procs, memory, TopN),
    ErlangProcsRedTopList = top_procs(Procs, reductions, TopN),
    ErlangProcsMqToplist = top_procs(Procs, message_queue_len, TopN),
    AllRelatedProcs = lists:usort(ErlangProcsMemTopList ++ ErlangProcsRedTopList ++ ErlangProcsMqToplist),
    ProcsInfoNeeded = dump_all_proc(AllRelatedProcs),
    {{epoch, Epoch},
     {core, Core}, 
     {cpu_util, CpuUtil}, 
     {mem_util, MemUtil}, 
     {mem_toplist, ErlangProcsMemTopList}, 
     {red_toplist, ErlangProcsRedTopList},
     {mq_toplist, ErlangProcsMqToplist}, 
     {procs_info, ProcsInfoNeeded}}.

lazy_do(Latency, Something) ->
    timer:send_after(Latency, Something).

dump_mailbox() ->             % i figure it may be not necessary :<
    receive
        _Any -> ok
    after
        0 -> ok
    end.
            
init_config(Paras) ->
    Intv = get_config(interval, Paras),
    TopN = get_config(topn, Paras),
    Smp = get_config(smp, Paras),
    #state{interval = Intv, topn = TopN, smp = Smp}.

get_config(K, Paras) ->
    [{K, V}] = [{K0, V0} || {K0, V0} <- Paras, K0 =:= K],
    V.

info(Msg) ->
    ?SERVER ! Msg.

remove_old_dets() ->
    IsFile = filelib:is_file(?DETS_FILE),
    if 
        IsFile -> 
            file:delete(?DETS_FILE);            % check if there is such dets file,
                                                % if exists, just delete it.
        true ->
            ok
    end.

create_new_dets(RespondTo, When) ->
    case dets:open_file(?MODULE, [{file, ?DETS_FILE}]) of 
        {ok, ?MODULE} -> 
            dets:close(?MODULE),                
            RespondTo ! {When, {node(), ok}};
        _ -> 
            RespondTo ! {When, {node(), fail}}
    end.

check_os() ->
    case os:type() of
        {unix, linux} -> true;                           
        _ -> false
    end.            
                
start_sup() ->
    cpu_sup:start(),
    memsup:start_link().

stop_sup() ->
    cpu_sup:stop(),
    exit(whereis(memsup), normal).
