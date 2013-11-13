# MsgBus Amqp Proxy

This is a proxy of rabbitmq.

It connects to multiple rabbitmq brokers, and dispatch messages to them randomly
leveraging pg2:get_closest_pid.

### Usage

Include this proxy into your project using rebar:

    {msgbus_amqp_proxy, ".*", {git, "https://github.com/TigerZhang/msgbus_amqp_proxy.git", "master"}}

### Configuration

You can pass the proxy the following configuration:

	{rabbitmqs, [
  		{msgbus_rabbitmq_local, [
	      {name,        "msgbus_rabbitmq_local"},
	      {exchange,    <<"msgbus_amqp_proxy">>},
	      {amqp_user,   <<"guest">>},
	      {amqp_pass,   <<"guest">>},
	      {amqp_vhost,  <<"/">>},
	      {amqp_host,   "localhost"},
	      {amqp_port,   5672}
	  	]}
	]}

### Author

Tiger Zhang

### License

Apache 2.0
