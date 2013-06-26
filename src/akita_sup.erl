%%%----------------------------------------------------------------------
%%% File      : akita_sup.erl
%%% Author    : ryan.ruan@ericsson.com
%%% Purpose   : Erlang application supervisor.
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

-module(akita_sup).

-behaviour(supervisor).

%% Helper Macro For Declaring Children Of Supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% Behaviour Callbacks
-export([start_link/0]).

%% Supervisor Callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor Callbacks
%% ===================================================================
init([]) ->
    AkitaWorker  = ?CHILD(akita_cluster_info, worker),
    Specs        = [AkitaWorker],
    {ok, {{one_for_one, 5, 10}, Specs}}.
