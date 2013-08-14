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
-export([start_link/1, start_collect/0, stop_collect/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(INTERVAL, 60).
-define(AKITA_FILE, filename:join(home(), "akita.record." ++ atom_to_list(node()))).
-define(TOP_N, 30).
-include_lib("stdlib/include/ms_transform.hrl").

-record(state, {}).

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
start_link(From) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [From], []).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
start_collect() ->
    gen_server:call(?SERVER, start_collect).

stop_collect() ->
    gen_server:call(?SERVER, stop_collect).

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
init([From]) ->
    error_logger:info_msg("do local init on node (~w)~n", [node()]),
    IsFile = filelib:is_file(?AKITA_FILE),
    if 
        IsFile -> 
            file:delete(?AKITA_FILE);           % check if there is such dets file,
                                                % if exists, just delete it.
        true ->
            ok
    end,
    case dets:open_file(?MODULE, [{file, ?AKITA_FILE}]) of 
        {ok, ?MODULE} -> 
            dets:close(?MODULE),                % just create a new dets file here,
                                                % so we can append data into it, 
                                                % but we close it for now.
            From ! {local_init_res, {node(), ok}};
        _             -> 
            From ! {local_init_res, {node(), fail}}
    end,
    {ok, #state{}}.

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
handle_call(start_collect, From, State) ->
    dets:open_file(?MODULE, [{file, ?AKITA_FILE}]),
    error_logger:info_msg("start to collect info on node (~w)~n", [node()]),
    lazy_do(start_collect),
    From ! {collect_started, node()},
    {reply, ok, State};

handle_call(stop_collect, From, State) ->
    error_logger:info_msg("stop collecting info on node (~w)~n", [node()]),
    dump_mailbox(),
    lazy_do(stop_collect),
    From ! {collect_stopped, node()},
    {reply, ok, State};
    
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
    akita_insert(generate_entry()),
    lazy_do(?INTERVAL, start_collect),
    {noreply, State};

handle_info(stop_collect, State) ->
    {stop, "stop collecting", State};

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

%%%===================================================================
%%% Internal functions
%%%===================================================================





%% ====================================================================
%% Internal functions
%% ====================================================================
home() -> 
    {ok, [[HOME]]} = init:get_argument(home),
    HOME.

akita_insert(V) when is_tuple(V) -> 
    dets:insert(?MODULE, V);
akita_insert(_)                  -> 
    error_logger:error_msg("only tuple can be inserted~n", []).

epoch() -> 
    calendar:datetime_to_gregorian_seconds(calendar:universal_time())-719528*24*3600.

beam_pid() -> 
    os:getpid().

ps_info() -> 
    Cmd = "ps -eo pid,psr,pcpu,pmem | egrep '^\\s*" ++ beam_pid() ++ "\\b'",
    Res = os:cmd(Cmd),
    [_Pid, CoreStr, CpuUtilStr, MemUtilStr] = string:tokens(Res, "\n\s"),
    Core    = list_to_integer(CoreStr),
    CpuUtil = list_to_float(CpuUtilStr),
    MemUtil = list_to_float(MemUtilStr),
    {Core, CpuUtil, MemUtil}.

get_proc_attr(Proc, Attr) when is_pid(Proc) -> 
    case catch process_info(Proc, Attr) of 
        {'EXIT', _} -> -9999;
        {Attr  , V} -> V
    end.

compare(A, B, Attr) -> 
    compare(A, B, Attr, down).
compare(A, B, Attr, Direction) -> 
    [V1, V2] = [ get_proc_attr(P, Attr) || P <- [A, B] ],
    case Direction of 
        down -> V1 > V2;
        up   -> V1 < V2
    end.

top_procs(Procs, Attr) -> 
    L = lists:sort(fun(A, B) -> compare(A, B, Attr) end, Procs),
    lists:sublist(L, ?TOP_N).

dump_all_proc() -> 
    [ {P, process_info(P)} || P <- processes() ].

generate_entry() -> 
    {Core, CpuUtil, MemUtil} = ps_info(),
    Epoch                    = epoch(),
    Procs                    = processes(),
    ErlangProcsMemTopList    = top_procs(Procs, memory),
    ErlangProcsRedTopList    = top_procs(Procs, reductions),
    ErlangProcsMqToplist     = top_procs(Procs, message_queue_len),
    AllProcsInfo             = dump_all_proc(),
    {   {epoch, Epoch}, 
        {core, Core}, 
        {cpu_util, CpuUtil}, 
        {mem_util, MemUtil}, 
        {mem_toplist, ErlangProcsMemTopList}, 
        {red_toplist, ErlangProcsRedTopList},
        {mq_toplist, ErlangProcsMqToplist}, 
        {procs_info, AllProcsInfo}}.

read_all() -> 
    dets:open_file(?MODULE, [{file, ?AKITA_FILE}]),
    Res = dets:select(?MODULE, ets:fun2ms(fun(T) -> T end)),
    dets:close(?MODULE),
    Res.

lazy_do(Something) ->
    timer:send_after(1000, Something).

lazy_do(Latency, Something) ->
    timer:send_after(Latency, Something).

dump_mailbox() ->
    receive
        _Any -> ok
    after
        ?INTERVAL -> ok
    end.
            
