#include <stdlib.h>
#include <stdio.h>

#include "rabbit-mq.h"

#define LOG printf("%s: %s:%d: ", __FILE__, __FUNCTION__, __LINE__); printf

#ifndef RABBIT_HOST
#define RABBIT_HOST "localhost"
#endif

#ifndef EXCHANGE
#define EXCHANGE "amq.topic"
#endif

#ifndef RABBIT_PORT
#define RABBIT_PORT 5672
#endif


int
main(void)
{
	struct rabbit *rabbit = NULL;
	const char *routing_key = "test-routing-key";
	int ret;
	size_t msg_size;
	void *msg = NULL;


	rabbit = rabbit_mq_server_create("localhost", RABBIT_PORT,
		EXCHANGE, routing_key);
	if (rabbit == NULL)
	{
		LOG("ERROR: rabbit_mq_server_create() failed\n");
		goto done;
	}

	LOG("waiting for requests...\n");
	while (1)
	{
		ret = rabbit_mq_server_receive(rabbit, &msg, &msg_size);
		if (ret != 0)
		{
			LOG("ERROR: rabbit_mq_server_receive() failed: %d\n", ret);
			continue;
		}

		if (msg == NULL)
		{
			LOG("ERROR: received msg is NULL\n");
			continue;
		}

		if (msg_size == 0)
		{
			LOG("ERROR: received size is 0\n");
			continue;
		}

		LOG("Received: \"%s\"\n", (char *)msg);
	}

done:
	if (rabbit != NULL)
	{
		rabbit_mq_destroy(rabbit);
		rabbit = NULL;
	}

	return 0;
}
