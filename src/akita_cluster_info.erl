%%%----------------------------------------------------------------------
%%% File      : akita_cluster_info.erl
%%% Author    : ryan.ruan@ericsson.com
%%% Purpose   : Akita cluster info collector worker
%%% Created   : Jun 26, 2013
%%%----------------------------------------------------------------------

%%%----------------------------------------------------------------------
%%% Copyright Ericsson AB 1996-2013. All Rights Reserved.
%%%
%%% The contents of this file are subject to the Erlang Public License,
%%% Version 1.1, (the "License"); you may not use this file except in
%%% compliance with the License. You should have received a copy of the
%%% Erlang Public License along with this software. If not, it can be
%%% retrieved online at http://www.erlang.org/.
%%%
%%% Software distributed under the License is distributed on an "AS IS"
%%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%% the License for the specific language governing rights and limitations
%%% under the License.
%%%----------------------------------------------------------------------

-module(akita_cluster_info).
-behaviour(gen_server).

-record(state, {nodes, failed}).

-include_lib("stdlib/include/ms_transform.hrl").
-define(TOP_N, 30).
-define(CENTRAL_NODE, 'hello@hao').
-define(TIMEOUT, 15000).

%% API Function
-export([start_link/0]).

%% Behaviour Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Functions
%% ------------------------------------------------------------------
start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

%% ------------------------------------------------------------------
%% Behaviour Callbacks
%% ------------------------------------------------------------------
init([]) ->
    %% if canine framework is used, there is no need to 'connect_all'
    connect_all(),
    %% in order to call terminate when application stops
    process_flag(trap_exit, true),
    c:nl(akita_collector_local),
    init_det_files(),
    {ok, #state{nodes = get(nodes), failed = 0}, ?TIMEOUT}.

handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, #state{nodes = Failed} = State) -> 
    io:format("no response from theses nodes: ~w~n", [Failed]),
    {stop, no_response, State};
handle_info({init_dets, {Node, Status}}, #state{nodes = Nodes, failed = Failed}) -> 
    io:format("initializaiton on node ~w --- ~w ~n", [Node, Status]),
    NodesLeft = Nodes -- [Node],
    TotalFailedNow = case Status of
        ok   -> Failed;
        fail -> Failed + 1
    end,
    if 
        NodesLeft =:= [] -> 
            if 
                TotalFailedNow > 0 -> 
                    {stop, init_fail, #state{nodes = NodesLeft, failed = TotalFailedNow}};
                true               -> 
                    application:set_env(akita, cluster_info_start, true), 
                    {noreply, #state{nodes = NodesLeft}}
            end;
        true             -> 
            {noreply, #state{nodes = NodesLeft, failed = TotalFailedNow}, ?TIMEOUT}
    end;
handle_info(Info, State) ->
    io:format("receive unexpected info: ~w~n", [Info]),
    {noreply, State}.

terminate(Reason, _State) ->
    mesh_unload(),
    io:format("akita terminated with reason: ~w~n", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Inner Functions
%% ------------------------------------------------------------------

akita_insert(V) when is_tuple(V) -> 
    dets:insert(?MODULE, V);
akita_insert(_)                  -> 
    io:format("only tuple can be inserted~n", []).

period_collect() -> 
    akita_insert(generate_entry()),
    receive
        stop -> dets:close(?MODULE)
    after
        60   -> period_collect()
    end.

akita_read_all() -> 
    dets:select(?MODULE, ets:fun2ms(fun(T) -> T end)). 

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
    {Epoch, Core, CpuUtil, MemUtil, ErlangProcsMemTopList, ErlangProcsRedTopList,
        ErlangProcsMqToplist, AllProcsInfo}.

mesh_unload() -> 
    todo.

%% this method should be improved
connect_all() -> 
    net_kernel:connect_node(?CENTRAL_NODE),
    OtherNodes = rpc:call(?CENTRAL_NODE, erlang, nodes, []),
    [ net_kernel:connect_node(N) || N <- OtherNodes ],
    io:format("connect to all --- ok~n", []),
    put(nodes, [?CENTRAL_NODE | OtherNodes]).

init_det_files() -> 
    {ok, Flag} = application:get_env(akita, cluster_info_start),
    if 
        not Flag -> 
            [ spawn(Node, akita_collector_local, init, []) || Node <- get(nodes) ];
        true     -> 
            ok
    end.
