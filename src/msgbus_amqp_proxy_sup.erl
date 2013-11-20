%% Copyright (c) 2013 by Tiger Zhang. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

-module(msgbus_amqp_proxy_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).
-define(PROXYCHILD(Id, Param, Outgoing, Incoming, Tag), {Id,
	{msgbus_amqp_proxy_client, start_link, [{Id,Param, Outgoing, Incoming, Tag}]},
	permanent, 5000, worker, [msgbus_amqp_proxy_client]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Rabbitmqs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Rabbitmqs).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init({Rabbitmqs, OutgoingQueues, IncomingQueues, NodeTag}) ->
	RestartStrategy = {one_for_one, 5, 10},
	ChildSpecs = [ ?PROXYCHILD(AmqpId, AmqpParam, OutgoingQueues, IncomingQueues, NodeTag)
					|| {AmqpId, AmqpParam} <- Rabbitmqs ],

	% io:format("ChildSpecs: ~p~n", [ChildSpecs]),

    {ok, { RestartStrategy, ChildSpecs } }.

