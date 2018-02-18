#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "rabbit-mq.h"

#define LOG printf("%s: %s:%d: ", __FILE__, __FUNCTION__, __LINE__); printf

#ifndef EXCHANGE
#define EXCHANGE "amq.topic"
#endif


int
main(int argc, char **argv)
{
	struct rabbit *rabbit = NULL;
	const char *routing_key = "test-routing-key";
	const char *hostname = "0.0.0.0";
	char *msg = "default message";
	int port = 5672;
	int ret;


	if (argc > 1)
	{
		msg = argv[1];
	}

	if (argc > 2)
	{
		hostname = argv[2];
	}

	if (argc > 3)
	{
		port = atoi(argv[3]);
	}

	rabbit = rabbit_mq_client_create(hostname, port);
	if (rabbit == NULL)
	{
		LOG("ERROR: rabbit_mq_client_create() failed\n");
		goto done;
	}

	ret = rabbit_mq_client_publish(rabbit, EXCHANGE, routing_key,
		msg, strlen(msg) + 1);
	if (ret != 0)
	{
		LOG("ERROR: rabbit_mq_client_publish() failed\n");
		goto done;
	}

done:
	if (rabbit != NULL)
	{
		rabbit_mq_destroy(rabbit);
		rabbit = NULL;
	}

	return 0;
}
