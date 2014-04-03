# MsgBus Amqp Proxy

This is a proxy of rabbitmq.

It connects to multiple rabbitmq brokers, and dispatch messages to them randomly
leveraging pg2:get_closest_pid.

This application is expected to be deployed in the same server with the client.

### Usage

Include this proxy into your project using rebar:

    {msgbus_amqp_proxy, ".*", {git, "https://github.com/TigerZhang/msgbus_amqp_proxy.git", "master"}}

### Configuration

You can pass the proxy the following configuration:

    {rabbitmqs, [
        {msgbus_rabbitmq_local, [
            {name, "msgbus_rabbitmq_local"},
            {exchange, <<"msgbus_amqp_proxy">>},
            {amqp_user, <<"guest">>},
            {amqp_pass, <<"guest">>},
            {amqp_vhost, <<"/">>},
            {amqp_host, "localhost"},
            {amqp_port, 5672}
        ]}
    ]},
    {outgoing_queues, [
        {<<"1">>, <<"msgbus_mqtt_command_connect">>},
        {<<"3">>, <<"msgbus_mqtt_command_publish">>},
        {<<"4">>, <<"msgbus_mqtt_command_puback">>},
        {<<"5">>, <<"msgbus_mqtt_command_pubrec">>},
        {<<"6">>, <<"msgbus_mqtt_command_pubrel">>},
        {<<"7">>, <<"msgbus_mqtt_command_pubcomp">>},
        {<<"8">>, <<"msgbus_mqtt_command_subscribe">>},
        {<<"10">>, <<"msgbus_mqtt_command_unsubscribe">>},
        {<<"12">>, <<"msgbus_mqtt_command_pingreq">>},
        {<<"14">>, <<"msgbus_mqtt_command_disconnect">>}
    ]},
    {incoming_queues, [
        {<<"msgbus_frontend_key_">>, <<"msgbus_frontend_queue_">>}
    ]},
    {node_tag, <<"front_tag">>},
    {receiver_module, emqtt_register} %% forward received message to.
    %% 收到的消息通过 gen_server:cast(receiver_module, {package_from_mq, Data}) 转发

### Author

Tiger Zhang

### License

Apache 2.0
