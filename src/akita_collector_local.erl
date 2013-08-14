-module(akita_collector_local).
-define(HUB, akita_cluster_info).
-define(INTERVAL, 60).
-define(AKITA_FILE, filename:join(home(), "akita.record." ++ atom_to_list(node()))).
-define(TOP_N, 30).
-include_lib("stdlib/include/ms_transform.hrl").
-export([init/1, start_collect_local/0, read_all/0]).

%% ====================================================================
%% API functions
%% ====================================================================
init(From) ->
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
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
home() -> 
    {ok, [[HOME]]} = init:get_argument(home),
    HOME.

akita_insert(V) when is_tuple(V) -> 
    dets:insert(?MODULE, V);
akita_insert(_)                  -> 
    io:format("only tuple can be inserted~n", []).

start_collect_local() -> 
    process_flag(trap_exit, true),
    dets:open_file(?MODULE, [{file, ?AKITA_FILE}]),
    io:format("start to  collect on ~w~n", [node()]),
    period_collect().

period_collect() -> 
    akita_insert(generate_entry()),
    receive
        stop -> 
            io:format("local collector on ~w stopped~n", [node()]),
            dets:close(?MODULE);
        _    -> %% exit signal when stop akita
            io:format("local collector on ~w stopped~n", [node()]),
            dets:close(?MODULE)
    after
        ?INTERVAL -> period_collect()
    end.

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
