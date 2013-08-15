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

-record(state, {collectors, collecting}). % 'collectors' is the worker processes
                                          % on all local nodes.
                                          % 'collecting' indicates whether they are
                                          % doing job now.

-define(MAX_TRY_TIMES, 3).                      % max times to check cluster state

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
    error_logger:info_msg("initialize akita~n", []),
    %% in order to call terminate when application stops
    process_flag(trap_exit, true),
    lazy_do(check_meshed),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_meshed, State) ->
    check_cluster_meshed(?MAX_TRY_TIMES),
    lazy_do(load_mod),
    {noreply, State};

handle_info(load_mod, State) ->
    load_module(akita_collector_local),
    lazy_do(local_init),
    {noreply, State};

handle_info(local_init, State) -> 
    error_logger:info_msg("start to init on local nodes~n", []),
    servers_prepare(),
    LocalCollectors = start_local_servers(nodes(connected)), 
    {noreply, State#state{collectors = LocalCollectors}}; 

handle_info({'DOWN', Ref, process, _Pid, _Info}, #state{collectors = Collectors, collecting = IsWorking} = State) ->
    [OldCollector] = [{N, R, P} || {N, R, P} <- Collectors, R =:= Ref],
    {DownNode, _OldRef, _OldPid} = OldCollector,
    error_logger:error_msg("collector on node (~w) goes on strike~n", [DownNode]),
    NewCollectors0 = Collectors -- [OldCollector],
    {ok, NewPid} = rpc:call(DownNode, akita_collector_local, start_link, [self(), reboot]),
    case IsWorking of 
        true ->
            timer:sleep(500),
            rpc:call(DownNode, akita_collector_local, start_collect, []);
        _ -> ok
    end,
    NewCollectors = receive
                        {local_reboot, {DownNode, ok}} ->
                            error_logger:info_msg("collector on node (~w) goes back to work~n", [DownNode]),
                            MonitorRef = erlang:monitor(process, NewPid),
                            NewCollectors0 ++ [{DownNode, MonitorRef, NewPid}];
                        {local_reboot, {DownNode, fail}} ->
                            %% if NewPid is not alive, it is to say that DownNode may be unavailable.
                            %% so there is no need to restart the collector on that node.
                            error_logger:error_msg("collector on node (~w) goes home~n", [DownNode]),
                            NewCollectors0
                    after
                        5000 ->
                            error_logger:error_msg("collector on node (~w) goes home (timeout)~n", [DownNode]),
                            NewCollectors0
                    end,
    {noreply, State#state{collectors = NewCollectors}};

handle_info({collect_started, Node}, State) ->
    error_logger:info_msg("collector on node (~w) is working~n", [Node]),
    {noreply, State};

handle_info({collect_stopped, Node}, State) ->
    error_logger:info_msg("collector on node (~w) stops working~n", [Node]),
    {noreply, State};

handle_info(stop_collect, #state{collectors = Collectors} = State) ->
    [begin
         timer:sleep(100),                      % in order to avoid Race Condition
         rpc:call(N, akita_collector_local, stop_collect, [])
     end || {N, _R, _P} <- Collectors],
    {noreply, State#state{collecting = false}};

handle_info(start_collect, #state{collectors = Collectors} = State) ->
    [rpc:call(N, akita_collector_local, start_collect, []) 
     || {N, _R, _P} <- Collectors],
    {noreply, State#state{collecting = true}};

handle_info(Info, State) ->
    error_logger:error_msg("receive unexpected info: ~w~n", [Info]),
    {noreply, State}.

terminate(Reason, #state{collectors = undefined}) ->
    error_logger:error_msg("akita shuts down with reason: ~w~n", [Reason]),
    mesh_unload0(nodes(connected)),
    ok;

terminate(Reason, #state{collectors = Collectors}) ->
    [begin
         erlang:demonitor(R),
         timer:sleep(100),
         rpc:call(N, akita_collector_local, quit, [])
     end || {N, R, _P} <- Collectors],
    timer:sleep(500),           % make sure that all collector is down,
                                % the max waiting time is 5 seconds due
                                % to OTP limitation.
    error_logger:info_msg("akita shuts down with reason: ~w~n", [Reason]),
    mesh_unload1(Collectors),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Inner Functions
%% ------------------------------------------------------------------
mesh_unload0(Nodes) ->
    [begin
         rpc:call(N, code, purge, [akita_collector_local]),
         rpc:call(N, code, delete, [akita_collector_local])
     end || N <- Nodes].

mesh_unload1(Collectors) -> 
    [begin
         rpc:call(N, code, purge, [akita_collector_local]),
         rpc:call(N, code, delete, [akita_collector_local])
     end || {N, _M, _P} <- Collectors].

start_local_servers(Nodes) ->
    start_local_servers(Nodes, []).

start_local_servers([], Collectors) -> 
    error_logger:info_msg("local init on all nodes successfully~n", []),
    Collectors;

start_local_servers([H | T], Collectors) ->
    {ok, Pid} = rpc:call(H, akita_collector_local, start_link, [self(), boot]),
    receive
        {local_init_res, {Node, ok}} ->
            error_logger:info_msg("local init on node (~w) successfully~n", [Node]),
            Ref = erlang:monitor(process, Pid),
            start_local_servers(T, [{H, Ref, Pid} | Collectors]);
        {local_init_res, {Node, fail}} ->
            Msg = io_lib:format("local init on node (" ++ atom_to_list(Node) ++ ") unsuccessfully", []),
            exit(Msg)
    after
        5000 ->
            Msg = io_lib:format("local init on node (" ++ atom_to_list(H) ++ ") unsuccessfully (timeout)", []),
            exit(Msg)
    end.

load_module(M) -> 
    {M, Bin, Fname} = code:get_object_code(M),
    [begin
         error_logger:info_msg("loading module (~w) on node (~w)~n", [M, N]),
         rpc:call(N, code, purge, [M]),
         rpc:call(N, code, load_binary, [M, Fname, Bin])
     end || N <- nodes(connected)].

check_cluster_meshed(0) ->
    Msg = io_lib:format("cluster can not be meshed", []),
    exit(Msg);

check_cluster_meshed(Times) ->
    State = application:get_env(westminster, cluster_meshed),
    case State of
        {ok, true} ->
            ok;
        _ ->                                    % {ok, false} or
                                                % 'undefined'
            error_logger:info_msg("waiting for cluster meshing~n", []),
            timer:sleep(5000),
            check_cluster_meshed(Times - 1)
    end.

lazy_do(Something) ->
    timer:send_after(300, Something).

servers_prepare() ->
    Nodes = nodes(connected),
    [begin
         Residual = rpc:call(N, erlang, whereis, [akita_collector_local]),
         case Residual of
             undefined ->
                 ok;
             Pid ->
                 exit(Pid, kill)
         end
     end || N <- Nodes],
    timer:sleep(1000).
