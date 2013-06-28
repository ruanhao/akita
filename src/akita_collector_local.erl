-module(akita_collector_local).
-define(HUB, akita_cluster_info).
-define(AKITA_FILE, filename:join(home(), "akita.record." ++ atom_to_list(node()))).
-export([init/0]).

%% ====================================================================
%% API functions
%% ====================================================================
init() -> 
    io:format("doing init on Node: ~w, dets file: ~p~n", [node(), ?AKITA_FILE]),
    Existed = filelib:is_file(?AKITA_FILE),
    if 
        Existed -> 
            io:format("delete ~p~n", [?AKITA_FILE]),
            file:delete(?AKITA_FILE),
            io:format("can i go here?~n", []);
        true    ->
            ok
    end,
    io:format("go to open file ~p~n", [?AKITA_FILE]),
    case catch dets:open_file(?MODULE, [{file, ?AKITA_FILE}]) of 
        {ok, ?MODULE} -> send_to_hub({init_dets, {node(), ok}});
        _             -> send_to_hub({init_dets, {node(), fail}})
    end.



%% ====================================================================
%% Internal functions
%% ====================================================================
send_to_hub(Msg) -> 
   case global:whereis_name(?HUB) of 
       undefined -> exit("Hub not available");
       _         -> ok
   end,
   io:format("global send~n", []),
   global:send(?HUB, Msg).

    
home() -> 
    {ok, [[HOME]]} = init:get_argument(home),
    HOME.
    


