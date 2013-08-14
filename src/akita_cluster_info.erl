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

-record(state, {collectors}).                   % 'collectors' is the worker processes
                                                % on all local nodes.

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
    spawn_init_proc(nodes(connected)), 
    {noreply, State}; 

handle_info({'DOWN', Ref, process, _Pid, _Info}, #state{collectors = Collectors} = State) ->
    [OldCollector] = [{N, R, P} || {N, R, P} <- Collectors, R =:= Ref],
    {DownNode, _OldRef, _OldPid} = OldCollector,
    error_logger:error_msg("collector on node (~w) goes on strike~n", [DownNode]),
    NewCollectors0 = Collectors -- [OldCollector],
    NewPid = proc_lib:spawn(DownNode, akita_collector_local, start_collect_local, []),
    NewCollectors = case is_process_alive(NewPid) of
                        true ->
                            error_logger:info_msg("collector on node (~w) goes back to work~n", [DownNode]),
                            MonitorRef = erlang:monitor(process, NewPid),
                            NewCollectors0 ++ [{DownNode, MonitorRef, NewPid}];
                        %% if NewPid is not alive, it is to say that DownNode may be unavailable.
                        %% so there is no need to restart the collector on that node.
                        false ->
                            error_logger:error_msg("collector on node (~w) goes home~n", [DownNode]),
                            NewCollectors0
                    end,
    {noreply, State#state{collectors = NewCollectors}};

handle_info({collect_started, Node}, State) ->
    error_logger:info_msg("collector on node (~w) is working~n", [Node]),
    {noreply, State};

handle_info({collect_stopped, Node}, State) ->
    error_logger:info_msg("collector on node (~w) stops working~n", [Node]),
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
        end || {N, _M, _P} <- Workers],
    file:close(Fd),
    io:format("dump ok~n", []),
    {noreply, State};

handle_info(stop_collect, State) -> 
    stop_collect(get(workers)),
    {noreply, State};

handle_info(start_collect, State) -> 
    Collectors = get_collectors(),
    {noreply, State#state{collectors = Collectors}};

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

spawn_init_proc([]) -> 
    error_logger:info_msg("local init on all nodes successfully~n", []);

spawn_init_proc([H | T]) -> 
    proc_lib:spawn(H, akita_collector_local, start_link, [self()]),
    receive
        {local_init_res, {Node, ok}} ->
            error_logger:info_msg("local init on node (~w) successfully~n", [Node]),
            spawn_init_proc(T);
        {local_init_res, {Node, fail}} ->
            Msg = io_lib:format("local init on node (~w) unsuccessfully", [Node]),
            exit(Msg)
    after
        2000 ->
            Msg = io_lib:format("local init on node (~w) unsuccessfully", [H]),
            exit(Msg)
    end.

get_collectors() -> 
    Nodes = nodes(connected),
    [begin
         Pid = proc_lib:spawn(N, akita_collector_local, start_collect_local, []),
         Ref = erlang:monitor(process, Pid),
         {N, Ref, Pid}
     end || N <- Nodes].

home() -> 
    {ok, [[HOME]]} = init:get_argument(home),
    HOME.

load_module(M) -> 
    {M, Bin, Fname} = code:get_object_code(M),
    [begin
         io:format("loading module ~w on node ~w~n", [M, N]),
         rpc:call(N, code, purge, [M]),
         rpc:call(N, code, load_binary, [M, Fname, Bin])
     end || N <- nodes(connected)].

check_cluster_meshed(0) ->
    Msg = io_lib:format("cluster can not be meshed~n", []),
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
    timer:send_after(1000, Something).
