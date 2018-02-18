CC = gcc -W -Wall -ggdb

CFLAGS = -I../src

LDLIBS = -lrabbitmq

RABBIT_MQ_SEND_OBJ = ../src/rabbit-mq.o rabbitmq-send.o
RABBIT_MQ_RECEIVE_OBJ = ../src/rabbit-mq.o rabbitmq-receive.o

TARGETS = rabbitmq-receive rabbitmq-send

all: $(TARGETS)

rabbitmq-receive: $(RABBIT_MQ_RECEIVE_OBJ)
rabbitmq-send: $(RABBIT_MQ_SEND_OBJ)

clean:
	rm -fr $(TARGETS) \
	$(RABBIT_MQ_SEND_OBJ) \
	$(RABBIT_MQ_RECEIVE_OBJ) \

