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
-define(TIMEOUT, 150000).

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
    io:format("initialization begins~n", []),
    %% if canine framework is used, there is no need to 'connect_all'
    connect_all(),
    %% in order to call terminate when application stops
    process_flag(trap_exit, true),
    load_module(akita_collector_local),
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
    case application:get_env(akita, cluster_info_start) of 
        {ok, false} -> 
            io:format("start to init on local nodes~n", []),
            spawn_init_proc(Nodes), 
            {noreply, State, ?TIMEOUT}; 
        {ok, true}  -> 
            {noreply, State}
    end;

handle_info({init_dets, {Node, Status}}, #state{nodes = [_ | L]}) -> 
    io:format("initializaiton on node ~w ------ ~w ~n", [Node, Status]),
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
    MonitorRef = erlang:monitor(process, NewPid),
    add_worker({Node, MonitorRef, NewPid}),
    {noreply, State};

handle_info(dump_cluster_info, State) -> 
    Filename = filename:join(home(), "akita.dump"),
    {ok, Fd} = file:open(Filename, write),
    Workers = get(workers),
    [begin
        io:format("dumping cluster info on ~w~n", [N]),
        io:format(Fd, "=========== akita cluster info on ~w begins to dump ===============~n",[N]),
        Res = rpc:call(N, akita_collector_local, read_all, []),
        io:format(Fd, "~p~n", [Res]),
        io:format(Fd, "=========== akita cluster info on ~w dumps over ===============~n",[N]),
        io:format(Fd, "~n~n~n", [])
        end || {N, _M, _P} <- Workers ],
    file:close(Fd),
    io:format("dump ok~n", []),
    {noreply, State};

handle_info(stop_collect, State) -> 
    stop_collect(get(workers)),
    {noreply, State};

handle_info(start_collect, State) -> 
    do_collect(),
    {noreply, State};

handle_info(Info, State) ->
    io:format("receive unexpected info: ~w~n", [Info]),
    {noreply, State}.

terminate(Reason, _State) ->
    case get(workers) of 
        undefined -> 
            ok;
        Workers  -> 
            stop_collect(Workers),
            mesh_unload(Workers)
    end,
    io:format("akita terminated with reason: ~w~n", [Reason]),
    application:set_env(akita, cluster_info_start, false),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Inner Functions
%% ------------------------------------------------------------------

stop_collect(Workers) -> 
    [begin
        erlang:demonitor(M),
        timer:sleep(100),
        P ! stop
        end || {_N, M, P} <- Workers ].

mesh_unload(Workers) -> 
    [begin
        rpc:call(N, code, delete, [akita_collector_local])
        end || {N, _M, _P} <- Workers ].


connect_all() -> 
    net_kernel:connect_node(?CENTRAL_NODE),
    OtherNodes = rpc:call(?CENTRAL_NODE, erlang, nodes, []),
    [ net_kernel:connect_node(N) || N <- OtherNodes ],
    ClusterNodes = [?CENTRAL_NODE | OtherNodes],
    io:format("connect to all nodes (~w) ------ ok~n", [ClusterNodes]),
    put(nodes, ClusterNodes).

spawn_init_proc([]) -> 
    io:format("Erlang cluster not available~n", []),
    exit('cluster_not_available');
spawn_init_proc([H | _]) -> 
    proc_lib:spawn(H, akita_collector_local, init, [self()]).

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

load_module(M) -> 
    {M, Bin, Fname} = code:get_object_code(M),
    [ begin
          io:format("loading module ~w on node ~w~n", [M, N]),
          rpc:call(N, code, purge, [M]),
          rpc:call(N, code, load_binary, [M, Fname, Bin])
      end || N <- nodes(connected) ].

