-module(akita_collector_local).
-behaviour(gen_server).
-define(HUB, akita_cluster_info).
-define(AKITA_FILE, filename:join(home(), "akita.record." ++ atom_to_list(node()))).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start/0]).

%% ====================================================================
%% API functions
%% ====================================================================
start() -> 
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []). 



%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {}).

%% init/1
%% ====================================================================
-spec init(Args :: term()) -> Result when
    Result :: {ok, State}
            | {ok, State, Timeout}
            | {ok, State, hibernate}
            | {stop, Reason :: term()}
            | ignore,
    State :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
init([]) ->
    process_flag(trap_exit, true),
    init_dets_file(),
    {ok, #state{}}.


%% handle_call/3
%% ==================================================================================================
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
    Result :: {reply, Reply, NewState}
            | {reply, Reply, NewState, Timeout}
            | {reply, Reply, NewState, hibernate}
            | {noreply, NewState}
            | {noreply, NewState, Timeout}
            | {noreply, NewState, hibernate}
            | {stop, Reason, Reply, NewState}
            | {stop, Reason, NewState},
    Reply :: term(),
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity,
    Reason :: term().
%% ==================================================================================================
handle_call(Request, From, State) ->
    Reply = ok,
    {reply, Reply, State}.


%% handle_cast/2
%% ====================================================================
-spec handle_cast(Request :: term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
            | {noreply, NewState, Timeout}
            | {noreply, NewState, hibernate}
            | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_cast(Msg, State) ->
    {noreply, State}.


%% handle_info/2
%% =========================================================================
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
            | {noreply, NewState, Timeout}
            | {noreply, NewState, hibernate}
            | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
%% =========================================================================
handle_info(Info, State) ->
    {noreply, State}.


%% terminate/2
%% ====================================================================
-spec terminate(Reason, State :: term()) -> Any :: term() when
    Reason :: normal
            | shutdown
            | {shutdown, term()}
            | term().
%% ====================================================================
terminate(Reason, State) ->
    io:format("terminated with Reason: ~w~n", [Reason]),
    dets:close(?MODULE),
    send_to_hub({collector_close, {node(), Reason}}),
    ok.


%% code_change/3
%% ========================================================================
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term()},
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term().
%% ========================================================================
code_change(OldVsn, State, Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================
send_to_hub(Msg) -> 
   case global:whereis_name(?HUB) of 
       undefined -> exit("Hub not available");
       _         -> ok
   end,
   global:send(?HUB, Msg).

init_dets_file() -> 
    Existed = filelib:is_file(?AKITA_FILE),
    if 
        Existed -> 
            file:delete(?AKITA_FILE);
        true    ->
            ok
    end,
    case dets:open_file(?MODULE, [{file, ?AKITA_FILE}]) of 
        {ok, ?MODULE} -> send_to_hub({init_dets, {node(), ok}});
        _             -> send_to_hub({init_dets, {node(), fail}})
    end.
    
home() -> 
    {ok, [[HOME]]} = init:get_argument(home),
    HOME.
    


