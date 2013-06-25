-module(akita).
-compile(export_all).
-include_lib("stdlib/include/ms_transform.hrl").
-define(AKITA_FILE, filename:join(home(), "akita.record")).


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
    akita_insert({hello, world}).

akita_read_all() -> 
    dets:select(?MODULE, ets:fun2ms(fun(T) -> T end)). 



