%%%----------------------------------------------------------------------
%%% File      : akita_app.erl
%%% Author    : ryan.ruan@ericsson.com
%%% Purpose   : Erlang application entry module.
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

-module(akita_app).

-behaviour(application).

%% API Functions
-export([start/2, stop/1]).

%% ===================================================================
%% API Functions
%% ===================================================================
start(_StartType, _StartArgs) ->
    akita_sup:start_link().

stop(_State) ->
    ok.
