#ifndef _SERVER_H
#define _SERVER_H

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <zlib.h>

#define DEFAULT_PORT 2222

#define REQUEST_FLAG_NO_REPLY         (1 << 0)
#define REQUEST_FLAG_GZIP             (1 << 1)

#define RESPONSE_STATUS_SUCCESS       0
#define RESPONSE_STATUS_REDIRECTION   1

struct request {
    uint32_t ver;
    uint32_t magic;
    uint64_t seq;
    uint32_t command;
    uint32_t flags;
    uint32_t payload_size;
    char payload[];
} __attribute__((packed));


struct response_header {
    uint64_t seq;
    uint32_t status;
    uint32_t payload_size;
};

struct host_port {
	char *host;
	uint16_t port;
};

struct server {
    int fd;
    bool is_slave;
    char *datadir;
    struct host_port *redirection;
    bool terminated;
};

extern struct server *server;

int server_init(const char *host, int port);
int server_wait(struct server *server);

#endif
