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

-record(state, {collectors = [], collecting = false}).       % 'collectors' is the worker processes
                                                % on all local nodes.
                                                % 'collecting' indicates whether they are
                                                % doing job now.

-define(MAX_TRY_TIMES, 3).                      % max times to check cluster state
-define(LOCAL_SERVER, akita_collector_local).
-define(LOCAL_SERVER_MODULE, ?LOCAL_SERVER).

%% API Function
-export([start_link/0]).

%% Behaviour Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Functions
%% ------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
    load_module(?LOCAL_SERVER_MODULE),
    lazy_do(local_init),
    {noreply, State};

handle_info(local_init, State) -> 
    error_logger:info_msg("start to init on local nodes~n", []),
    servers_prepare(),
    LocalCollectors = start_local_servers(nodes(connected)), 
    {noreply, State#state{collectors = LocalCollectors}}; 

handle_info({'DOWN', Ref, process, _Pid, _Info}, #state{collectors = Collectors, collecting = IsWorking} = State) ->
    [OldCollector] = [{N, R} || {N, R} <- Collectors, R =:= Ref],
    {DownNode, _OldRef} = OldCollector,
    error_logger:error_msg("collector on node (~w) goes on strike~n", [DownNode]),
    NewCollectors0 = Collectors -- [OldCollector],
    {ok, NewPid} = rpc:call(DownNode, ?LOCAL_SERVER, start_link, [self(), reboot, init_config()]),
    case IsWorking of 
        true ->
            timer:sleep(500),
            rpc:call(DownNode, ?LOCAL_SERVER, start_collect, []);
        _ -> ok
    end,
    NewCollectors = receive
                        {local_reboot, {DownNode, ok}} ->
                            error_logger:info_msg("collector on node (~w) goes back to work~n", [DownNode]),
                            MonitorRef = erlang:monitor(process, NewPid),
                            NewCollectors0 ++ [{DownNode, MonitorRef}];
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
         rpc:call(N, ?LOCAL_SERVER, stop_collect, [])
     end || {N, _R} <- Collectors],
    {noreply, State#state{collecting = false}};

handle_info(start_collect, #state{collectors = Collectors} = State) ->
    [rpc:call(N, ?LOCAL_SERVER, start_collect, []) || {N, _R} <- Collectors],
    {noreply, State#state{collecting = true}};

handle_info(status, #state{collectors = Collectors, collecting = IsWorking} = State) ->
    SplitLine = "--------------------------------------------------",
    WorkingNodes = [atom_to_list(N) ++ "\n" || {N, _} <- Collectors],
    WorkingNodesStr = lists:flatten(WorkingNodes),
    error_logger:info_msg("CURRENT COLLECTORS: ~n~s" 
                          ++ SplitLine ++ "~nIS COLLECTING NOW: ~n~w~n", [WorkingNodesStr, IsWorking]),
    {noreply, State};

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
         rpc:call(N, ?LOCAL_SERVER, quit, [])
     end || {N, R} <- Collectors],
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
         rpc:call(N, code, purge, [?LOCAL_SERVER_MODULE]),
         rpc:call(N, code, delete, [?LOCAL_SERVER_MODULE])
     end || N <- Nodes].

mesh_unload1(Collectors) -> 
    [begin
         rpc:call(N, code, purge, [?LOCAL_SERVER_MODULE]),
         rpc:call(N, code, delete, [?LOCAL_SERVER_MODULE])
     end || {N, _M} <- Collectors].

start_local_servers(Nodes) ->
    start_local_servers(Nodes, []).

start_local_servers([], Collectors) -> 
    error_logger:info_msg("local init on all nodes successfully~n", []),
    Collectors;

start_local_servers([H | T], Collectors) ->
    {ok, Pid} = rpc:call(H, ?LOCAL_SERVER, start_link, [self(), boot, init_config()]),
    receive
        {local_init_res, {Node, ok}} ->
            error_logger:info_msg("local init on node (~w) successfully~n", [Node]),
            Ref = erlang:monitor(process, Pid),
            start_local_servers(T, [{H, Ref} | Collectors]);
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
         Residual = rpc:call(N, erlang, whereis, [?LOCAL_SERVER]),
         case Residual of
             undefined ->
                 ok;
             Pid ->
                 exit(Pid, kill)
         end
     end || N <- Nodes],
    timer:sleep(1000).

init_config() ->
    Interval = get_akita_env(interval, 5 * 60 * 1000),
    TopN = get_akita_env(topn, 30),    
    [{interval, Interval}, {topn, TopN}].
    
get_akita_env(Key, Default) ->
    V0 = application:get_env(akita, Key),
    case V0 of
        {ok, V} -> V;
        undefined -> Default
    end.
