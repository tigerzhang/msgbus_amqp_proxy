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

-module(msgbus_amqp_proxy).
-export([start/0, send_test/0]).

start() ->
	application:start(amqp_client),
	application:start(msgbus_amqp_proxy).

declare_bind(RoutingKey, Queue) ->
	[ gen_server:call(Pid, {declare_bind, RoutingKey, Queue}) || Pid <- pg2:get_members(msgbus_amqp_clients) ],
	ok.

send_test() ->
	RoutingKey = <<"route">>,
	declare_bind(RoutingKey, <<"test_queue">>),
	Pid = pg2:get_closest_pid(msgbus_amqp_clients),
	Message = <<"message">>,
%% 	gen_server:call(Pid, {declare_bind, RoutingKey, <<"test_queue">>}),
	gen_server:call(Pid, {forward_to_amqp, RoutingKey, Message},
		infinity).
