%%%----------------------------------------------------------------------
%%% File      : akita.erl
%%% Author    : ryan.ruan@ericsson.com
%%% Purpose   : Bootstrap module.
%%% Created   : Apr 3, 2013
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

-module(akita).            

%% API Functions
-export([start/0, 
         start/1,
         stop/0, 
         start_collect/0, 
         stop_collect/0,
         pull/0,
         status/0, 
         log/0, log/1, 
         q/1, 
         process/1]).

-define(HUB, akita_cluster_info).

%% ===================================================================
%% API Functions
%% ===================================================================
start() ->
    ensure_started(crypto),
    ensure_started(sasl),
    ensure_started(westminster),
    application:start(akita).

start(offline) ->
    ensure_started(crypto),
    ensure_started(sasl),
    ensure_started(westminster),
    akita_cluster_info:start_link(offline).

stop() ->
    command({self(), stop}),
    receive
        please_stop_akita ->
            application:stop(akita)
    after
        5000 ->
            application:stop(akita)
    end.

start_collect() -> 
    command(start_collect).

stop_collect() -> 
    command(stop_collect).

pull() ->
    command(pull).

status() ->
    command(status).

log() ->
    log(all).

log(N) ->
    rb:stop(),
    rb:start([{max, N}]),
    rb:list().

q(N) ->
    rb:show(N).

process(Path) ->
    {ok, FileList} = file:list_dir(Path),
    [begin
         AbsName = filename:join(Path, File),
         IsDetsFile = dets:is_dets_file(AbsName),
         case IsDetsFile of
             true ->
                 output(AbsName);
             false ->
                 ok
         end
     end || File <- FileList],
    error_logger:info_msg("process complete~n", []).



%% ===================================================================
%% Inner Functions
%% ===================================================================
output(FileName) ->
    {ok, ?MODULE} = dets:open_file(?MODULE, [{file, FileName}, {access, read}]),
    {ok, TxtFd} = file:open(FileName ++ ".txt", [write]),
    {ok, CsvFd} = file:open(FileName ++ ".csv", [write]),
    Split = "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -~n",
    io:format(CsvFd, "time,cpu_util,mem_util~n", []),
    io:format(TxtFd, "welcome to akita data centre~n", []),
    dets:traverse(?MODULE, fun({{epoch, {_, {H, M, S}}}, _, {cpu_util, CpuUtil}, {mem_util, MemUtil},
                                _, _, _, _} = Entry) ->
                                   Time = io_lib:format("~B:~2.10.0B:~2.10.0B", [H, M, S]),
                                   io:format(CsvFd, Time ++ ",~w,~w~n", [CpuUtil, MemUtil]),
                                   io:format(TxtFd, Split ++ "~p~n", [Entry]),
                                   continue
                           end),
    file:close(CsvFd),
    file:close(TxtFd),
    dets:close(?MODULE).
                              
command(Cmd) ->
    Pid = whereis(?HUB),
    if
        is_pid(Pid) ->
            IsProcAlive = is_process_alive(Pid),
            if
                not IsProcAlive ->
                    error_logger:error_msg("akita crashes unexpectedly~n", []);
                true ->
                    ?HUB ! Cmd
            end;
        true ->
            error_logger:info_msg("akita does not start yet~n", [])
    end,
    ok.

ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.
