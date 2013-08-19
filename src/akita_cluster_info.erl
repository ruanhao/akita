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

% 'collectors' is the worker processes   
% on all local nodes.                    
% 'collecting' indicates whether they are
% doing job now. 
-record(state, {collectors = [], collecting = false,
                start_clct_time = " undefined~n", end_clct_time = " undefined~n", 
                repo, pulled = 0, 
                offline = false}).      

-define(MAX_TRY_TIMES, 3).                      % max times to check cluster state
-define(LOCAL_SERVER, akita_collector_local).
-define(LOCAL_SERVER_MODULE, ?LOCAL_SERVER).

%% API Function
-export([start_link/0, start_link/1]).

%% Behaviour Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Functions
%% ------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link(offline) ->                          % just for pulling
    gen_server:start_link({local, ?MODULE}, ?MODULE, [offline], []).
%% ------------------------------------------------------------------
%% Behaviour Callbacks
%% ------------------------------------------------------------------
init([offline]) ->
    error_logger:info_msg("initialize akita (offline) ~n", []),
    %% in order to call terminate when application stops
    process_flag(trap_exit, true),
    lazy_do(check_meshed),
    {ok, #state{offline = true}};

init([]) ->
    error_logger:info_msg("initialize akita ~n", []),
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

handle_info(local_init, #state{offline = OffL} = State) -> 
    error_logger:info_msg("start to init on local nodes~n", []),
    servers_prepare(),
    LocalCollectors = start_local_servers(nodes(connected), OffL), 
    {noreply, State#state{collectors = LocalCollectors}}; 

handle_info({'DOWN', Ref, process, _Pid, Info}, #state{collectors = Collectors, 
                                                        collecting = Boolean, offline = OffL} = State) ->
    case Info of
        noconnection ->
            timer:sleep(500),
            check_cluster_meshed(?MAX_TRY_TIMES);
        noproc ->
            ok
    end,
    NewCollectors = update_collectors(Ref, Collectors, Boolean, OffL),
    {noreply, State#state{collectors = NewCollectors}};

handle_info(start_collect, #state{collectors = []} = State) ->
    error_logger:error_msg("there are no collectors at all~n", []),
    {noreply, State};

handle_info(start_collect, #state{offline = true} = State) ->
    error_logger:error_msg("hey, you are in offline mode, don't play on me -_-# ~n", []),
    {noreply, State};

handle_info(start_collect, #state{collecting = true} = State) ->
    error_logger:error_msg("collecting is going~n", []),
    {noreply, State};

handle_info(start_collect, #state{collectors = Collectors} = State) ->
    [rpc:call(N, ?LOCAL_SERVER, start_collect, []) || {N, _R} <- Collectors],
    {noreply, State#state{collecting = true, 
                          start_clct_time = get_current_time(), 
                          end_clct_time = " undefined~n"}};

handle_info(stop_collect, #state{collectors = []} = State) ->
    error_logger:error_msg("there are not collectors at all~n", []),
    {noreply, State};

handle_info(stop_collect, #state{collecting = false} = State) ->
    error_logger:error_msg("collecting is already stopped~n", []),
    {noreply, State};

handle_info(stop_collect, #state{collectors = Collectors} = State) ->
    [begin
         timer:sleep(100),                      % in order to avoid Race Condition
         rpc:call(N, ?LOCAL_SERVER, stop_collect, [])
     end || {N, _R} <- Collectors],
    {noreply, State#state{collecting = false, end_clct_time = get_current_time()}};

handle_info(status, #state{collectors = Collectors, collecting = IsWorking, 
                           start_clct_time = StartTime, end_clct_time = EndTime, 
                           offline = OffL} = State) ->
    SplitLine = "----------------------------~n",
    WorkingNodes = [io_lib:format("~w~n", [N]) || {N, _} <- Collectors],
    WorkingNodesStr = lists:flatten(WorkingNodes),
    Configs = [io_lib:format("~w: ~w~n", [K, V]) || {K, V} <- init_config()],
    ConfigsStr = lists:flatten(Configs),
    IsWorkingStr = io_lib:format("~w~n", [IsWorking]),
    OffLStr = atom_to_list(OffL) ++ "\n",
    error_logger:info_msg("CURRENT COLLECTORS: ~n" ++ WorkingNodesStr ++ SplitLine ++ 
                              "IS COLLECTING NOW: ~n" ++ IsWorkingStr ++ SplitLine ++
                              "START COLLECTING: ~n" ++ StartTime ++ 
                              "STOP COLLECTING: ~n" ++ EndTime ++ SplitLine ++ 
                              "CONFIGS: ~n" ++ ConfigsStr ++ SplitLine ++ 
                              "OFFLINE: ~n" ++ OffLStr, []),
    {noreply, State};

handle_info({From, stop}, #state{collectors = Collectors} = State) ->
    error_logger:info_msg("akita about to stop~n", []),
    [begin
         erlang:demonitor(R),
         timer:sleep(100),
         rpc:call(N, ?LOCAL_SERVER, quit, [])
     end || {N, R} <- Collectors],
    timer:sleep(3000),          % make sure that all collector is down 
                                % before mesh_unload. the max waiting time
                                % is 5 seconds due to OTP supervisor child spec.
    mesh_unload1(Collectors),
    From ! please_stop_akita,
    {noreply, State#state{collectors = useless}};

handle_info(pull, #state{collecting = true} = State) ->
    error_logger:error_msg("collector is working now, don't be so hurry~n", []),
    {noreply, State};

handle_info(pull, #state{collecting = false, collectors = Collectors} = State) ->
    {ok, [[Home]]} = init:get_argument(home),
    {{Y, M, D}, {H, Min, S}} = calendar:local_time(),
    DirName = io_lib:format("doghair_~w_~w_~w_~w_~w_~w", [Y, M, D, H, Min, S]),
    Repo = filename:join(Home, DirName),
    file:make_dir(Repo),
    [rpc:call(N, ?LOCAL_SERVER, pull, [self()]) || {N, _} <- Collectors],
    {noreply, State#state{repo = Repo, pulled = 0}};

handle_info({pull_ack, From, FileName}, #state{repo = Repo} = State) ->
    pull_req(Repo, FileName, From),
    {noreply, State};

handle_info(pulled, #state{pulled = NumOfPulled, collectors = Collectors, repo = Repo} = State) ->
    if
        (NumOfPulled + 1) =:= length(Collectors) ->
            error_logger:info_msg("pulling done, archive at: ~p ~n", [Repo]);
        true ->
            ok
    end,
    {noreply, State#state{pulled = NumOfPulled + 1}};

handle_info({'EXIT', Pid, Reason}, State) ->    % this clause is used to check the status of the proc
                                                % which is sent out to retrieve data on every node.
    error_logger:info_msg("receive EXIT signal from ~w (~w) ~n", [Pid, Reason]),
    {noreply, State};

handle_info(Info, State) ->
    error_logger:error_msg("receive unexpected info: ~p~n", [Info]),
    {noreply, State}.

terminate(shutdown, _State) ->
    error_logger:info_msg("akita stops gracefully ~n", []),
    ok;

terminate(Reason, _State) ->
    error_logger:error_msg("akita crashes with reason: ~p~n", [Reason]),
    mesh_unload0(nodes(connected)),
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

start_local_servers(Nodes, OffL) ->
    start_local_servers(Nodes, [], OffL).

start_local_servers([], Collectors, _) -> 
    error_logger:info_msg("local init on all nodes successfully~n", []),
    Collectors;

start_local_servers([H | T], Collectors, OffL) ->
    {ok, Pid} = rpc:call(H, ?LOCAL_SERVER, start_link, [self(), boot, init_config(), OffL]),
    receive
        {local_init, {Node, ok}} ->
            error_logger:info_msg("local init on node (~w) successfully~n", [Node]),
            Ref = erlang:monitor(process, Pid),
            start_local_servers(T, [{H, Ref} | Collectors], OffL);
        {local_init, {Node, fail}} ->
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
    SmpSupport = get_akita_env(smp, true),
    [{interval, Interval}, {topn, TopN}, {smp, SmpSupport}].
    
get_akita_env(Key, Default) ->
    V0 = application:get_env(akita, Key),
    case V0 of
        {ok, V} -> V;
        undefined -> Default
    end.

get_current_time() ->
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:local_time(),
    io_lib:format("~2B/~2B/~4B ~2B:~2.10.0B:~2.10.0B\n", 
                  [Month, Day, Year, Hour, Min, Sec]).

recv_sock(Sock, IoDevice, Hub, FileName) ->
    case gen_tcp:recv(Sock, 0, 5000) of
        {ok, B} ->
            file:write(IoDevice, B),
            recv_sock(Sock, IoDevice, Hub, FileName);
        {error, closed} ->
            file:close(IoDevice),
            gen_tcp:close(Sock),
            Hub ! pulled,
            error_logger:info_msg("~p pulled ~n", [FileName]);
        {error, Reason} ->
            file:close(IoDevice),
            gen_tcp:close(Sock),
            error_logger:error_msg("~p can not be pulled (~p) ~n", [FileName, Reason])
    end.

pull_req(Repo, FileName, From) ->
    {ok, Hostname} = inet:gethostname(),
    {ok, Listen} = gen_tcp:listen(0, [binary, {packet, raw}, {active, false}]),
    {ok, Port} = inet:port(Listen),
    Hub = self(),
    SaveFile = filename:join(Repo, FileName),
    proc_lib:spawn_link(fun() -> accept_and_recv(Listen, SaveFile, Hub, FileName) end),
    timer:sleep(500),
    From ! {pull_req, {Hostname, Port}},
    ok.

accept_and_recv(Listen, SaveFile, Hub, FileName) ->
    case gen_tcp:accept(Listen, 5000) of
        {ok, Sock} ->
            gen_tcp:close(Listen),
            {ok, IoDevice} = file:open(SaveFile, [write, binary]),
            recv_sock(Sock, IoDevice, Hub, FileName);
        {error, Reason} ->
            error_logger:error_msg("~p can not be pulled (~w) ~n", [FileName, Reason])
    end.

update_collectors(Ref, Collectors, NeedRestartCollect, OffL) ->
    [OldCollector] = [{N, R} || {N, R} <- Collectors, R =:= Ref],
    {DownNode, _OldRef} = OldCollector,
    error_logger:error_msg("collector on node (~w) goes on strike~n", [DownNode]),
    NewCollectors0 = Collectors -- [OldCollector],
    NewPid = 
        case rpc:call(DownNode, ?LOCAL_SERVER, start_link, [self(), reboot, init_config(), OffL]) of
            {ok, P} -> 
                %% check whether i need restart collecting
                case NeedRestartCollect of 
                    true ->
                        timer:sleep(500),
                        rpc:call(DownNode, ?LOCAL_SERVER, start_collect, []);
                    _ -> ok
                end,
                P;
            {badrpc,nodedown} -> 
                null
        end,
    receive
        {local_reboot, {DownNode, ok}} ->
            error_logger:info_msg("collector on node (~w) goes back to work ~n", [DownNode]),
            MonitorRef = erlang:monitor(process, NewPid),
            NewCollectors0 ++ [{DownNode, MonitorRef}];
        {local_reboot, {DownNode, fail}} ->
            %% if NewPid is not alive, it is to say that DownNode may be unavailable.
            %% so there is no need to restart the collector on that node.
            error_logger:error_msg("collector on node (~w) goes home~n", [DownNode]),
            NewCollectors0
    after
        5000 ->
            case NewPid of
                null -> 
                    error_logger:error_msg("collector on node (~w) goes home (node down)~n", [DownNode]);
                _ ->
                    error_logger:error_msg("collector on node (~w) goes home (timeout)~n", [DownNode])
            end,
            NewCollectors0
    end.

    
