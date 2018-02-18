#ifndef INCLUDE_ZEU0WT767uFuTNqtdlXGCwwKJ7EDCPqB
#define INCLUDE_ZEU0WT767uFuTNqtdlXGCwwKJ7EDCPqB 1

#ifdef __cplusplus
extern "C"
{
#endif

struct rabbit;


struct rabbit *rabbit_mq_client_create(const char *hostname, int port);
int rabbit_mq_client_publish(struct rabbit *rabbit, const char *exchange,
	const char *routing_key, const void *msg, size_t size);

struct rabbit *rabbit_mq_server_create(const char *hostname,
	int port, const char *exchange, const char *binding_key);
int rabbit_mq_server_receive(struct rabbit *rabbit, void **msg,
	size_t *msg_size);

void rabbit_mq_close(struct rabbit *rabbit);

void rabbit_mq_destroy(struct rabbit *rabbit);

#ifdef __cplusplus
}
#endif

#endif /* INCLUDE_ZEU0WT767uFuTNqtdlXGCwwKJ7EDCPqB */
