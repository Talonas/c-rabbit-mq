#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "rabbit-mq.h"

#define LOG printf("%s: %s:%d: ", __FILE__, __FUNCTION__, __LINE__); printf


struct rabbit
{
	amqp_connection_state_t conn;
	amqp_socket_t *socket;
	amqp_bytes_t reply_to_queue;
	uint8_t connections;
};

static struct rabbit *rabbit_create(void);
static int rabbit_connect(struct rabbit *rabbit, const char *hostname,
	int port);
static void dump_amqp_error(amqp_rpc_reply_t status);


struct rabbit *
rabbit_mq_client_create(const char *hostname, int port)
{
	struct rabbit *retval = NULL;
	struct rabbit *rabbit = NULL;
	int ret;


	if (hostname == NULL)
	{
		LOG("ERROR: argument hostname is NULL\n");
		goto done;
	}

	rabbit = rabbit_create();
	if (rabbit == NULL)
	{
		LOG("ERROR: rabbit_create() failed\n");
		goto done;
	}

	ret = rabbit_connect(rabbit, hostname, port);
	if (ret != 0)
	{
		LOG("ERROR: rabbit_connect() failed\n");
		goto done;
	}

	retval = rabbit;
	rabbit = NULL;

done:
	if (rabbit != NULL)
	{
		rabbit_mq_destroy(rabbit);
		rabbit = NULL;
	}

	return retval;
}

int
rabbit_mq_client_publish(struct rabbit *rabbit, const char *exchange,
	const char *routing_key, const void *msg, size_t size)
{
	int retval = -1;
	int ret;
	amqp_basic_properties_t props;
	amqp_bytes_t data;


	if (rabbit == NULL)
	{
		LOG("ERROR: argument rabbit is NULL\n");
		goto done;
	}

	if (exchange == NULL)
	{
		LOG("ERROR: argument exchange is NULL\n");
		goto done;
	}

	if (routing_key == NULL)
	{
		LOG("ERROR: argument routing_key is NULL\n");
		goto done;
	}

	if (msg == NULL)
	{
		LOG("ERROR: argument msg is NULL\n");
		goto done;
	}

	/* set properties */
	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG |
		AMQP_BASIC_DELIVERY_MODE_FLAG;

	props.content_type = amqp_cstring_bytes("text/plain");

	/* persistent delivery mode */
	props.delivery_mode = 2;

	data.len = size;
	data.bytes = (void *)msg;

	/* publish */
	ret = amqp_basic_publish(rabbit->conn, 1,
		amqp_cstring_bytes(exchange), amqp_cstring_bytes(routing_key),
		0, 0, &props, data);
//		0, 0, &props, amqp_cstring_bytes(msg));
	if (ret != 0)
	{
		LOG("ERROR: amqp_basic_publish() failed\n");
		goto done;
	}

	retval = 0;
done:
	return retval;
}

struct rabbit *
rabbit_mq_server_create(const char *hostname,
	int port, const char *exchange, const char *binding_key)
{
	struct rabbit *retval = NULL;
	struct rabbit *rabbit = NULL;
	int ret;
	amqp_queue_declare_ok_t *r = NULL;
	amqp_bytes_t queue_name;
	amqp_rpc_reply_t status;


	queue_name.bytes = NULL;

	if (hostname == NULL)
	{
		LOG("ERROR: argument hostname is NULL\n");
		goto done;
	}

	if (exchange == NULL)
	{
		LOG("ERROR: argument exchange is NULL\n");
		goto done;
	}

	if (binding_key == NULL)
	{
		LOG("ERROR: argument binding_key is NULL\n");
		goto done;
	}

	rabbit = rabbit_create();
	if (rabbit == NULL)
	{
		LOG("ERROR: rabbit_create() failed\n");
		goto done;
	}

	ret = rabbit_connect(rabbit, hostname, port);
	if (ret != 0)
	{
		LOG("ERROR: rabbit_connect() failed\n");
		goto done;
	}

	r = amqp_queue_declare(rabbit->conn, 1, amqp_empty_bytes, 0, 0, 0,
		1, amqp_empty_table);
	if (r == NULL)
	{
		LOG("ERROR: amqp_queue_declare() failed\n");
		goto done;
	}

	status = amqp_get_rpc_reply(rabbit->conn);
	if (status.reply_type != AMQP_RESPONSE_NORMAL)
	{
		LOG("ERROR: amqp_get_rpc_reply() failed\n");
		dump_amqp_error(status);
		goto done;
	}

	queue_name = amqp_bytes_malloc_dup(r->queue);
	if (queue_name.bytes == NULL)
	{
		LOG("ERROR: amqp_bytes_malloc_dup() failed\n");
		goto done;
	}

	amqp_queue_bind(rabbit->conn, 1, queue_name,
		amqp_cstring_bytes(exchange), amqp_cstring_bytes(binding_key),
		amqp_empty_table);

	status = amqp_get_rpc_reply(rabbit->conn);
	if (status.reply_type != AMQP_RESPONSE_NORMAL)
	{
		LOG("ERROR: amqp_get_rpc_reply() failed\n");
		dump_amqp_error(status);
		goto done;
	}

#if 1
	amqp_basic_qos(rabbit->conn, 1, 0, 16, 0);
	status = amqp_get_rpc_reply(rabbit->conn); 
	if (status.reply_type != AMQP_RESPONSE_NORMAL)
	{
		LOG("ERROR: amqp_get_rpc_reply() failed\n");
		dump_amqp_error(status);
		goto done;
	}
#endif

	amqp_basic_consume(rabbit->conn, 1, queue_name, amqp_empty_bytes, 0,
		1, 0, amqp_empty_table);

	status = amqp_get_rpc_reply(rabbit->conn);
	if (status.reply_type != AMQP_RESPONSE_NORMAL)
	{
		LOG("ERROR: amqp_get_rpc_reply() failed\n");
		dump_amqp_error(status);
		goto done;
	}

	retval = rabbit;
	rabbit = NULL;

done:
	if (queue_name.bytes != NULL)
	{
		amqp_bytes_free(queue_name);
	}

	if (rabbit != NULL)
	{
		rabbit_mq_destroy(rabbit);
		rabbit = NULL;
	}

	return retval;
}

int
rabbit_mq_server_receive(struct rabbit *rabbit, void **msg,
	size_t *msg_size)
{
	int retval = -1;
	amqp_rpc_reply_t res;
	amqp_envelope_t envelope;


	if (rabbit == NULL)
	{
		LOG("ERROR: argument rabbit is NULL\n");
		goto done;
	}

	if (msg == NULL)
	{
		LOG("ERROR: argument msg is NULL\n");
		goto done;
	}

	if (msg_size == NULL)
	{
		LOG("ERROR: argument msg_size is NULL\n");
		goto done;
	}

	if (rabbit->conn == NULL)
	{
		LOG("ERROR: rabbit->conn is NULL\n");
		goto done;
	}

	*msg = NULL;
	*msg_size = 0;

	amqp_maybe_release_buffers(rabbit->conn);

	res = amqp_consume_message(rabbit->conn, &envelope, NULL, 0);
	if (res.reply_type != AMQP_RESPONSE_NORMAL)
	{
		LOG("ERROR: amqp_consume_message() failed: %d\n",
			res.reply_type);
		goto done;
	}
#if 0
	printf("Content-type: %zd:%s\n",
		envelope.message.properties.content_type.len,
		(char *)envelope.message.properties.content_type.bytes);
#endif

	*msg_size = envelope.message.body.len;
	*msg = malloc(*msg_size);
	if (*msg == NULL)
	{
		LOG("ERROR: low memory\n");
		goto done;
	}

	memcpy(*msg, envelope.message.body.bytes, *msg_size);

	amqp_destroy_envelope(&envelope);

	retval = 0;
done:
	return retval;

}

void
rabbit_mq_close(struct rabbit *rabbit)
{
	if (rabbit == NULL)
	{
		goto done;
	}

	switch (rabbit->connections)
	{
	case 1:
		amqp_channel_close(rabbit->conn, 1, AMQP_REPLY_SUCCESS);
	case 2:
		amqp_connection_close(rabbit->conn, AMQP_REPLY_SUCCESS);
	case 3:
		if (rabbit->conn == NULL)
		{
			break;
		}

		amqp_destroy_connection(rabbit->conn);
		rabbit->conn = NULL;
		break;
	default:
		LOG("ERROR: invalid rabbit->connections value: %d\n",
			rabbit->connections);
	}

done:
	return;
}

void
rabbit_mq_destroy(struct rabbit *rabbit)
{
	if (rabbit == NULL)
	{
		goto done;
	}

	rabbit_mq_close(rabbit);

	free(rabbit);
	rabbit = NULL;

done:
	return;
}

static struct rabbit *
rabbit_create(void)
{
	struct rabbit *retval = NULL;
	struct rabbit *rabbit = NULL;


	rabbit = malloc(sizeof(*rabbit));
	if (rabbit == NULL)
	{
		LOG("ERROR: low memory\n");
		goto done;
	}

	memset(rabbit, 0, sizeof(*rabbit));

	retval = rabbit;
	rabbit = NULL;

done:
	if (rabbit != NULL)
	{
		rabbit_mq_destroy(rabbit);
		rabbit = NULL;
	}

	return retval;
}

static int
rabbit_connect(struct rabbit *rabbit, const char *hostname,
	int port)
{
	int retval = -1;
	int ret;
	amqp_rpc_reply_t status;


	if (rabbit == NULL)
	{
		LOG("ERROR: argument rabbit is NULL\n");
		goto done;
	}

	if (hostname == NULL)
	{
		LOG("ERROR: argument hostname is NULL\n");
		goto done;
	}

	rabbit->conn = amqp_new_connection();
	if (rabbit->conn == NULL)
	{
		LOG("ERROR: amqp_new_connection() failed\n");
		goto done;
	}

	rabbit->connections = 1;

	rabbit->socket = amqp_tcp_socket_new(rabbit->conn);
	if (rabbit->socket == NULL)
	{
		LOG("ERROR: amqp_tcp_socket_new() failed\n");
		goto done;
	}

	rabbit->connections = 2;

	ret = amqp_socket_open(rabbit->socket, hostname, port);
	if (ret != 0)
	{
		LOG("ERROR: amqp_socket_open() failed\n");
		goto done;
	}

	status = amqp_login(rabbit->conn, "/", 0, 131072, 0,
		AMQP_SASL_METHOD_PLAIN, "guest", "guest");
	if (status.reply_type != AMQP_RESPONSE_NORMAL)
	{
		LOG("ERROR: amqp_login() failed: %d\n", status.reply_type);
		dump_amqp_error(status);
		goto done;
	}

	rabbit->connections = 3;

	amqp_channel_open(rabbit->conn, 1);

	status = amqp_get_rpc_reply(rabbit->conn);
	if (status.reply_type != AMQP_RESPONSE_NORMAL)
	{
		LOG("ERROR: amqp_get_rpc_reply() failed: %d\n",
			status.reply_type);
		dump_amqp_error(status);
		goto done;
	}

	retval = 0;
done:
	return retval;
}

static void
dump_amqp_error(amqp_rpc_reply_t status)
{
	amqp_connection_close_t *con = NULL;
#if 0
	amqp_channel_close_t *ch = NULL;
#endif


	switch (status.reply_type)
	{
	case AMQP_RESPONSE_NORMAL:
		LOG("AMQP_RESPONSE_NORMAL\n");
		break;

	case AMQP_RESPONSE_NONE:
		LOG("missing RPC reply type!\n");
		break;

	case AMQP_RESPONSE_LIBRARY_EXCEPTION:
		LOG("%s\n", amqp_error_string2(status.library_error));
		break;

	case AMQP_RESPONSE_SERVER_EXCEPTION:
		switch (status.reply.id)
		{
		case AMQP_CONNECTION_CLOSE_METHOD:
			con = (amqp_connection_close_t *)status.reply.decoded;
			LOG("server connection error %d, message: %.*s\n",
				con->reply_code, (int)con->reply_text.len,
				(char *)con->reply_text.bytes);
			break;
		}
		break;

#if 0
	case AMQP_CHANNEL_CLOSE_METHOD:
		ch = (amqp_channel_close_t *) status.reply.decoded;
		LOG("server channel error %d, message: %.*s\n",
			ch->reply_code, (int) ch->reply_text.len,
			(char *)ch->reply_text.bytes);
		break;
#endif

	default:
		LOG("unknown server error, method id 0x%08X\n",
			status.reply_type);
	}
}

