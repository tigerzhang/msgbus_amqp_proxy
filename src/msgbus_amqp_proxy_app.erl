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

-module(msgbus_amqp_proxy_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  ets:new(msgbus_amqp_clients_priority_table, [public, named_table, ordered_set]),
	{ok, Rabbitmqs} = application:get_env(rabbitmqs),
	{ok, OutgoingQueues} = application:get_env(outgoing_queues),
	{ok, IncomingQueues} = application:get_env(incoming_queues),
	{ok, NodeTag} = application:get_env(node_tag),
    msgbus_amqp_proxy_sup:start_link({Rabbitmqs, OutgoingQueues, IncomingQueues, NodeTag}).

stop(_State) ->
  ets:delete(msgbus_amqp_clients_priority_table),
  ok.
