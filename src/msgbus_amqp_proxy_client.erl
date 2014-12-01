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
    tref,
    is_unsubscribe,
    name,
    level,
    connection,
    channel,
    exchange,
    params,
    amqp_package_sent_count,
    amqp_package_recv_count,
    receiver_module,
    receiver_msg_queue_len,
    queue_info
}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, start_link/1, test/0, close/1, unsubscribe/0]).

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

start_link({Id, Params, OutgoingQueues, IncomingQueues, NodeTag}) ->
    gen_server:start_link({local, Id}, ?MODULE, {Params, OutgoingQueues, IncomingQueues, NodeTag}, []).

receiver_module_name(Receiver) ->
    Receiver.

close(Id) ->
    gen_server:call(Id, close).

unsubscribe() ->
    gen_server:call(?MODULE, unsubscribe).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init({Params, OutgoingQueues, IncomingQueues, NodeTag}) ->

    % io:format("Params: ~p~n", [Params]),
    {ok, Receiver} = application:get_env(receiver_module),
    {ok, MsgQueueLen} = application:get_env(receiver_msg_queue_len),

    Name = config_val(name, Params, ?MODULE),
    Level = config_val(level, Params, debug),
    Exchange = config_val(exchange, Params, list_to_binary(atom_to_list(?MODULE))),

    AmqpParams = #amqp_params_network{
        username = config_val(amqp_user, Params, <<"guest">>),
        password = config_val(amqp_pass, Params, <<"guest">>),
        virtual_host = config_val(amqp_vhost, Params, <<"/">>),
        host = config_val(amqp_host, Params, "localhost"),
        port = config_val(amqp_port, Params, 5672)
    },

    ?INFO("Connecting to: ~p", [Name]),

    {Connection2, Channel2, QueueInfo} =
        case amqp_channel(AmqpParams) of
            {ok, Connection, Channel} ->

                % declare exchange
                case amqp_channel:call(Channel,
                    #'exchange.declare'{exchange = Exchange, type = <<"topic">>}) of
                    #'exchange.declare_ok'{} ->
                        ?INFO("declare exchange succeeded: ~p", [Exchange]);
                    Return ->
                        ?ERROR("declare exchange failed: ~p", [Return])
                end,

                % Subscribe incoming queues
                SubscribeInfo = [subscribe_incoming_queues(Key, Queue, Exchange, Channel, NodeTag) || {Key, Queue} <- IncomingQueues],
                ?DEBUG("Subscribe queue Info ~p", [SubscribeInfo]),

                declare_and_bind_outgoing_queues(Channel, Exchange, OutgoingQueues),

                pg2:join(msgbus_amqp_clients, self()),
                {Connection, Channel, SubscribeInfo};
            Error ->
                Interval = 10,
                ?ERROR("amqp_channel failed. will try again after ~p s", [Interval]),
                % exit the client after 10 seconds, let the supervisor recreate it
                timer:exit_after(timer:seconds(Interval), "Connect failed"),
                {undefined, undefined, []}
        end,

    {ok, #state{
        name = Name,
        level = Level,
        connection = Connection2,
        channel = Channel2,
        exchange = Exchange,
        params = AmqpParams,
        amqp_package_sent_count = 0,
        is_unsubscribe = false,
        amqp_package_recv_count = 0,
        receiver_module = receiver_module_name(Receiver),
        receiver_msg_queue_len = MsgQueueLen,
        queue_info = QueueInfo
    }}.

handle_call(unsubscribe, _From,  #state{channel = Channel, queue_info = QueueInfo} = State) ->
    unsubscribe_incomming_queues(Channel, QueueInfo),
    {reply, ok, State};

handle_call({set_msgq_len, Len}, _From, State=#state{receiver_msg_queue_len = MsgQueueLen}) ->
    {reply, ok, State#state{receiver_msg_queue_len = Len}};

handle_call(close, _From, #state{connection = Connection, channel = Channel} = State) ->
    ?DEBUG("~p", [<<"close channel">>]),
    amqp_channel:close(Channel),
    ?DEBUG("~p", [<<"close connection">>]),
    amqp_connection:close(Connection),
    {reply, ok, State};

handle_call({forward_to_amqp, RoutingKey, Message}, _From,
    #state{params = AmqpParams, exchange = Exchange, amqp_package_sent_count = Sent} = State) ->
    State2 = case amqp_channel(AmqpParams) of
                 {ok, Connection, Channel} ->
                     amqp_publish(Exchange, RoutingKey, Message, Channel, State),
                     State#state{amqp_package_sent_count = Sent + 1, connection = Connection, channel = Channel};
                 _ ->
                     State
             end,
    {reply, ok, State2};

handle_call({declare_bind, RoutingKey, Queue}, _From, #state{params = AmqpParams, exchange = Exchange} = State) ->
    case amqp_channel(AmqpParams) of
        {ok, _Connection, Channel} ->
            #'queue.declare_ok'{} = amqp_channel:call(Channel, #'queue.declare'{queue = Queue}),
            Binding = #'queue.bind'{queue = Queue,
                exchange = Exchange,
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
    ?INFO("DOWN: ~p", [{Ref, Type, Pid, Info}]),

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
    #amqp_msg{payload = Data} = Content},
    #state{amqp_package_recv_count = Recv,
        channel = Channel,
        queue_info = QueueInfo,
        receiver_msg_queue_len = MsgQueueLen,
        is_unsubscribe = IsUnsubscribe,
        receiver_module = ReceiverModule} = State) ->
%%   ?INFO("ConsumerTag: ~p"
%%   "~nDeliveryTag: ~p"
%%   "~nExchange: ~p"
%%   "~nRoutingKey: ~p"
%%   "~nContent: ~p"
%%   "~n",
%%     [CTag, DeliveryTag, Exch, RK, Content]),
%%   ?INFO("Data: ~p", [Data]),
    %% fixme

    Pid = whereis(ReceiverModule),
    State3 = case Pid of
                 undefined ->
                     ?WARN("No Pid for ~p", [ReceiverModule]),
                     State;
                 _ ->
                     {message_queue_len, Len} = erlang:process_info(Pid, message_queue_len),
                     ?DEBUG("QueueLen ~p, config ~p", [Len, MsgQueueLen]),
                     State2 = case Len > MsgQueueLen of
                                  true ->
                                      case IsUnsubscribe of
                                          false ->
                                              ?CRITICAL("msgq length ~p > config leng ~p", [Len, MsgQueueLen]),
                                              TRef = erlang:start_timer(5000, self(), triger),
                                              unsubscribe_incomming_queues(Channel, QueueInfo),
                                              ?WARN_MSG("Pause Consumer"),
                                              State#state{tref = TRef, is_unsubscribe = true};
                                          _ ->
                                              State
                                      end;
                                  _ ->
                                      State
                              end,
                     State2
             end,
    gen_server:cast(ReceiverModule, {package_from_mq, Data}),
    {noreply,State3#state{amqp_package_recv_count = Recv + 1}};

handle_info({timeout, _Ref, _}, #state{channel = Channel, queue_info = QueueInfo} = State) ->
    erlang:cancel_timer(State#state.tref),
    NewQueueInfo = [
        {
            ConsumeQueue,
            amqp_channel:subscribe(Channel,
                                   #'basic.consume'{queue = ConsumeQueue,
                                                    consumer_tag = ConsumerTag,
                                                    no_ack = true},
                                   self())
        }  || {ConsumeQueue, {_, ConsumerTag}} <- QueueInfo],
    ?WARN("Resume Consumer \n old info ~p \n new info ~p", [QueueInfo, NewQueueInfo]),
    {noreply, State#state{is_unsubscribe = false, queue_info=NewQueueInfo}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, _State=#state{channel=Channel, queue_info = QueueInfo}) ->
    unsubscribe_incomming_queues(Channel, QueueInfo),
    ?DEBUG("proxy client terminated ~p", [Reason]),
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
    Publish = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    Props = #'P_basic'{content_type = <<"application/octet-stream">>, expiration = <<"8000">>},
    Msg = #amqp_msg{payload = Message, props = Props},
    amqp_channel:cast(Channel, Publish, Msg),

    State.

config_val(C, Params, Default) ->
    case lists:keyfind(C, 1, Params) of
        {C, V} -> V;
        _ ->
            ?INFO("Default: ~p", [Default]),
            Default
    end.

amqp_channel(AmqpParams) ->
    case maybe_new_pid({AmqpParams, connection},
        fun() -> amqp_connection:start(AmqpParams) end) of
        {ok, Client} ->
            case maybe_new_pid({AmqpParams, channel},
                fun() -> amqp_connection:open_channel(Client) end) of
                {ok, Channel} ->
                    {ok, Client, Channel};
                Error2 ->
                    Error2
            end;
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

unsubscribe_incomming_queues(Channel, QueueInfo) ->
    lists:map(fun(Info) ->
        case Info of
            {Queue, {'basic.consume_ok', ConsumerTag}} ->
                Method = #'basic.cancel'{consumer_tag = ConsumerTag},
                Result = amqp_channel:call(Channel, Method),
                ?DEBUG("Unsubscribe queue succee ~p Result ~p", [Queue, Result]);
            {Queue, {Other, _ComsumerTag}} ->
                ?DEBUG("Queue does not consumer ~p, ~p", [Queue, Other])
        end
    end, QueueInfo).

subscribe_incoming_queues(Key, Queue, Exchange, Channel, NodeTag) ->
    ConsumeQueue =
        case binary:last(Queue) of
            $_ ->
                <<Queue/binary, NodeTag/binary>>;
            _ ->
                Queue
        end,

    ConsumerTag = case amqp_channel:call(Channel, #'queue.declare'{queue = ConsumeQueue}) of
        #'queue.declare_ok'{} ->
            Tag = amqp_channel:subscribe(Channel,
                #'basic.consume'{queue = ConsumeQueue,
                    no_ack = true},
                self()),
            ?DEBUG("Tag: ~p", [Tag]),
            Tag;
        Return2 ->
            ?ERROR("declare queue failed: ~p", [Return2]),
            <<"tag-error">>
    end,

    KeyEnd = binary:last(Key),
    RoutingKey =
        case KeyEnd of
            $_ ->
                <<Key/binary, NodeTag/binary>>;
            _ ->
                Key
        end,

    Binding = #'queue.bind'{queue = ConsumeQueue,
        exchange = Exchange,
        routing_key = RoutingKey},
    case amqp_channel:call(Channel, Binding) of
        #'queue.bind_ok'{} ->
            ?INFO("Bind succeeded: ~p",
                [{ConsumeQueue, Exchange, RoutingKey}]);
        Return3 ->
            ?ERROR("Bind failed: ~p",
                [{ConsumeQueue, Exchange, RoutingKey, Return3}])
    end,
    {ConsumeQueue, ConsumerTag}.

declare_and_bind_outgoing_queues(Channel, Exchange, OutgoingQueues) ->
    [
        {#'queue.declare_ok'{} = amqp_channel:call(Channel, #'queue.declare'{queue = Queue}),
            #'queue.bind_ok'{} = amqp_channel:call(Channel, #'queue.bind'{queue = Queue,
                exchange = Exchange,
                routing_key = Key})}
        || {Key, Queue} <- OutgoingQueues].
