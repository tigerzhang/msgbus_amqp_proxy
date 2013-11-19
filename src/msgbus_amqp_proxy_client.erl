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

-module(msgbus_amqp_proxy_client).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-compile({parse_transform, lager_transform}).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("elog/include/elog.hrl").

-record(state, {
  name,
  level,
  exchange,
  params
}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, start_link/1, test/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

start_link({Id, Params}) ->
  gen_server:start_link({local, Id}, ?MODULE, Params, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Params) ->
  
  % io:format("Params: ~p~n", [Params]),

  Name = config_val(name, Params, ?MODULE),  
  Level = config_val(level, Params, debug),
  Exchange = config_val(exchange, Params, list_to_binary(atom_to_list(?MODULE))),
  
  AmqpParams = #amqp_params_network {
    username       = config_val(amqp_user, Params, <<"guest">>),
    password       = config_val(amqp_pass, Params, <<"guest">>),
    virtual_host   = config_val(amqp_vhost, Params, <<"/">>),
    host           = config_val(amqp_host, Params, "localhost"),
    port           = config_val(amqp_port, Params, 5672)
  },

  io:format("Connecting to: ~p~n", [Name]),
  
  case amqp_channel(AmqpParams) of
    {ok, Channel} ->  
      case amqp_channel:call(Channel,
        #'exchange.declare'{ exchange = Exchange, type = <<"topic">> }) of
        #'exchange.declare_ok'{} ->
          io:format("declare exchange succeeded: ~p~n", [Exchange]);
        Return ->
          io:format("declare exchange failed: ~p~n", [Return])
      end,

	  % Subscribe backend->frentend queue
      Prefix = <<"msgbus_frentend_queue_">>,
	  FrontQueueId = <<"front1">>,
	  ConsumeQueue = <<Prefix/binary, FrontQueueId/binary>>,
	  case amqp_channel:call(Channel, #'queue.declare'{queue = ConsumeQueue}) of
		  #'queue.declare_ok'{} ->
	  		Tag = amqp_channel:subscribe( Channel,
										  #'basic.consume'{queue = ConsumeQueue,
														   no_ack = true},
										  self()),
			?DEBUG("Tag: ~p", [Tag]);
		  Return2 ->
			  io:format("declare queue failed: ~p~n", [Return2])
	  end,
	  
	  RoutingKey = <<"msgbus_frentend_routekey_front1">>,
	  Binding = #'queue.bind'{queue       = ConsumeQueue,
					        exchange    = Exchange,
	              			routing_key = RoutingKey},
	  case amqp_channel:call(Channel, Binding) of
		  #'queue.bind_ok'{} ->
			  ?INFO("Bind succeeded: ~p",
					 [{ConsumeQueue, Exchange, RoutingKey}]);
		  Return3 ->
			  ?ERROR("Bind failed: ~p",
					 [{ConsumeQueue, Exchange, RoutingKey, Return3}])
	  end,
	  
      pg2:join(msgbus_amqp_clients, self());
    Error ->

      io:format("amqp_channel failed. will try again after 10s ~n"),
      % exit the client after 10 seconds, let the supervisor recreate it
      timer:exit_after(timer:seconds(10), "Connect failed"),
      Error
  end,

  {ok, #state{ 
    name = Name, 
    level = Level, 
    exchange = Exchange,
    params = AmqpParams
  }}.

handle_call({forward_to_amqp, RoutingKey, Message}, _From, #state{params = AmqpParams, exchange = Exchange} = State) ->
	case amqp_channel(AmqpParams) of
		{ok, Channel} ->
			amqp_publish(Exchange, RoutingKey, Message, Channel, State);
		_ ->
			State
	end,
	{reply, ok, State};

handle_call({declare_bind, RoutingKey, Queue}, _From, #state{params = AmqpParams, exchange = Exchange} = State) ->
	case amqp_channel(AmqpParams) of
		{ok, Channel} ->
			#'queue.declare_ok'{} = amqp_channel:call(Channel, #'queue.declare'{queue = Queue}),
			Binding = #'queue.bind'{queue       = Queue,
							        exchange    = Exchange,
	                      			routing_key = RoutingKey},
			#'queue.bind_ok'{} = amqp_channel:call(Channel, Binding);
		_ ->
			State
	end,
	{reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, Type, Pid, Info}, State) ->
  io:format("DOWN: ~p~n", [{Ref, Type, Pid, Info}]),

  pg2:leave(msgbus_amqp_clients, self()),

  % exit the client after 10 seconds, let the supervisor recreate it
  timer:exit_after(timer:seconds(10), "Connection closed"),

  {noreply, State};
handle_info(#'basic.consume_ok'{consumer_tag = CTag}, State) ->
    ?INFO("Consumer Tag: ~p", [CTag]),
    {noreply, State};
handle_info({#'basic.deliver'{consumer_tag = CTag,
                              delivery_tag = DeliveryTag,
                              exchange = Exch,
                              routing_key = RK},
             #amqp_msg{payload = Data} = Content}, State) ->
    ?INFO("ConsumerTag: ~p"
         "~nDeliveryTag: ~p"
         "~nExchange: ~p"
         "~nRoutingKey: ~p"
         "~nContent: ~p"
         "~n",
         [CTag, DeliveryTag, Exch, RK, Content]),
    ?INFO("Data: ~p", [Data]),
	{noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

test() ->
  gen_server:call(self(),
    {forward_to_amqp, <<"amqp_proxy_client">>, <<"route">>, <<"message">>},
    infinity).
%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

amqp_publish(Exchange, RoutingKey, Message, Channel, State) ->
    Publish = #'basic.publish'{ exchange = Exchange, routing_key = RoutingKey },
    Props = #'P_basic'{ content_type = <<"application/octet-stream">> },
    Msg = #amqp_msg{ payload = Message, props = Props },
	amqp_channel:cast(Channel, Publish, Msg),

	State.

config_val(C, Params, Default) ->
  case lists:keyfind(C, 1, Params) of
    {C, V} -> V;
    _ ->
      io:format("Default: ~p~n", [Default]),
      Default
  end.

amqp_channel(AmqpParams) ->
  case maybe_new_pid({AmqpParams, connection},
                     fun() -> amqp_connection:start(AmqpParams) end) of
    {ok, Client} ->
      maybe_new_pid({AmqpParams, channel},
                    fun() -> amqp_connection:open_channel(Client) end);
    Error ->
      Error
  end.

maybe_new_pid(Group, StartFun) ->
  case pg2:get_closest_pid(Group) of
    {error, {no_such_group, _}} ->
      pg2:create(Group),
      maybe_new_pid(Group, StartFun);
    {error, {no_process, _}} ->
      case StartFun() of
        {ok, Pid} ->
          pg2:join(Group, Pid),
          erlang:monitor(process, Pid),
          {ok, Pid};
        Error ->
          Error
      end;
    Pid ->
      {ok, Pid}
  end.
