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

-record(state, {nodes}).

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
    timer:send_after(1000, do_init),
    {ok, #state{nodes = get(nodes)}, ?TIMEOUT}.

handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, #state{nodes = [H | _]}) -> 
    io:format("no response from node: ~w~n", [H]),
    {stop, no_response, #state{}};

handle_info(do_init, #state{nodes = Nodes} = State) -> 
    spawn_init_proc(Nodes),
    {noreply,State, ?TIMEOUT}; 

handle_info({init_dets, {Node, Status}}, #state{nodes = [_ | L]}) -> 
    io:format("initializaiton on node ~w --- ~w ~n", [Node, Status]),
    case Status of 
        fail -> 
            {stop, init_fail, #state{}};
        ok -> 
            if 
                L =:= [] -> 
                    application:set_env(akita, cluster_info_start, true),
                    {noreply, #state{}};
                true     -> 
                    spawn_init_proc(L),
                    {noreply, #state{nodes = L}, ?TIMEOUT}
            end
    end;

handle_info({'DOWN', _Ref, process, Pid, _Info}, State) -> 
    [{Node, _M, Pid}] = [{N, M, P} || {N, M, P} <- get(workers), P =:= Pid ],
    delete_worker(Pid),
    NewPid = proc_lib:spawn(Node, akita_collector_local, start_collect_local, []),
    MonitorRef = erlang:monitor(process, Pid),
    add_worker({Node, MonitorRef, NewPid}),
    {noreply, State};

handle_info(dump_cluster_info, State) -> 
    Filename = filename:join(home(), "dump"),
    {ok, Fd} = file:open(Filename, write),
    Workers = get(workers),
    io:format(Fd, "===============================================~n", []),
    [begin
        io:format(Fd, "cluster info on ~w: ~n",[N]),
        Res = rpc:call(N, akita_collector_local, read_all, []),
        io:format(Fd, "~p~n", [Res]),
        io:format(Fd, "~n~n~n", [])
        end || {N, _M, _P} <- Workers ],
    file:close(Fd),
    {noreply, State};

handle_info(stop_collect, State) -> 
    stop_collect(),
    {noreply, State};

handle_info(start_collect, State) -> 
    do_collect(),
    {noreply, State};

handle_info(Info, State) ->
    io:format("receive unexpected info: ~w~n", [Info]),
    {noreply, State}.

terminate(Reason, _State) ->
    stop_collect(),
    mesh_unload(),
    io:format("akita terminated with reason: ~w~n", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Inner Functions
%% ------------------------------------------------------------------

stop_collect() -> 
    Workers = get(workers),
    [begin
        erlang:demonitor(M),
        timer:sleep(100),
        P ! stop
        end || {_N, M, P} <- Workers ].

mesh_unload() -> 
    Workers = get(workers),
    [begin
        rpc:call(N, code, delete, [akita_collector_local])
        end || {N, _M, _P} <- Workers ].


%% this method should be improved
connect_all() -> 
    net_kernel:connect_node(?CENTRAL_NODE),
    OtherNodes = rpc:call(?CENTRAL_NODE, erlang, nodes, []),
    [ net_kernel:connect_node(N) || N <- OtherNodes ],
    io:format("connect to all --- ok~n", []),
    put(nodes, [?CENTRAL_NODE | OtherNodes]).

spawn_init_proc([H | _]) -> 
    proc_lib:spawn(H, akita_collector_local, init, []).

add_worker(W) -> 
    case get(workers) of 
        undefined -> put(workers, [W]);
        Workers   -> put(workers, [W | Workers])
    end.

delete_worker(Pid) -> 
    Workers = get(workers),
    Worker = [ {N, M, P} || {N, M, P} <- Workers, P =:= Pid ], 
    put(workers, Workers -- Worker).

do_collect() -> 
    Nodes = get(nodes),
    [begin
                Pid = proc_lib:spawn(N, akita_collector_local, start_collect_local, []),
                MonitorRef = erlang:monitor(process, Pid),
                add_worker({N, MonitorRef, Pid})
        end || N <- Nodes].
home() -> 
    {ok, [[HOME]]} = init:get_argument(home),
    HOME.

