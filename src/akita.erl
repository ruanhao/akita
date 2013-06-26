-module(akita).
-compile(export_all).
-include_lib("stdlib/include/ms_transform.hrl").
-define(AKITA_FILE, filename:join(home(), "akita.record." ++ atom_to_list(node()))).
-define(TOP_N, 30).


home() -> 
    {ok, [[HOME]]} = init:get_argument(home),
    HOME.

start() -> 
    Existed = filelib:is_file(?AKITA_FILE),
    if 
        Existed -> 
            io:format("delete old file~n", []),
            file:delete(?AKITA_FILE);
        true    -> 
            ok
    end,
    case dets:open_file(?MODULE, [{file, ?AKITA_FILE}]) of 
        {ok, ?MODULE} -> io:format("create record file~n", []), period_collect();
        _             -> io:format("can not create dets file~n", [])
    end.

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
