#include <poll.h>
#include <netinet/tcp.h>
#include <msgpack.h>
#include "server.h"
#include "crabql.h"
#include "command.h"
#include "log.h"
#include "table.h"
#include "utils.h"
#include "error.h"

struct server *server = NULL;

int server_init(const char *host, int port) {
    (void) host;
    
    int ret = 0;

    int server_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (server_fd == -1) {
        perror("socket");
        exit(1);
    }

    int one = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(int)) == -1) {
        perror("setsockopt");
        exit(1);
    }
    
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = INADDR_ANY; 

    if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(struct sockaddr)) == -1) {
        log_error("bind error: %d", errno);
        ret = CRABDB_ERROR_IO;
        goto err;
    }

    if (listen(server_fd, 1000) == -1) {
        log_error("listen error: %d", errno);
        ret = CRABDB_ERROR_IO;
        goto err;
    }

    log_notice("listen on port %d", port);
    
    server = malloc(sizeof(*server));
    if (!server) {
        perror("malloc");
        exit(1);
    }

    memset(server, 0, sizeof(*server));
    server->fd = server_fd;
err:
    return ret;
}

int pending_commands = 0;

static ssize_t readn(int fd, void *vptr, size_t n)
{
    size_t nleft;
    ssize_t nread;
    char *ptr;

    ptr = vptr;
    nleft = n;
    while (nleft > 0)
    {
        if ((nread = read(fd, ptr, nleft)) < 0)
        {
            if (errno == EINTR)
                nread = 0;
            else
                return -1;
        } else if (nread == 0)
        {
            break;
        }
        nleft -= nread;
        ptr += nread;
    }
    return (n - nleft);
}

ssize_t writen(int fd, const void *vptr, size_t n)
{
    size_t nleft;
    ssize_t nwrite;
    const char *ptr;

    ptr = vptr;
    nleft = n;
    while (nleft > 0)
    {
        if ((nwrite = write(fd, ptr, nleft)) < 0)
        {
            if (errno == EINTR)
                nwrite = 0;
            else
                return -1;
        } else if (nwrite == 0)
        {
            break;
        }
        nleft -= nwrite;
        ptr += nwrite;
    }
    return (n - nleft);
}

/*
static ssize_t robust_recv(int sockfd, void *buf, size_t len, int flags) {
    ssize_t ret = 0;
    while (len > 0) {
        ssize_t recvsize = recv(sockfd, buf, len, flags);
        if (recvsize == 0) {
            break;
        }
        if (recvsize < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                continue;
            else
                break;
        }
        ret += recvsize;
        buf += recvsize;
        len -= recvsize;
    }
    return ret;
}
*/


static void *client_thread(void *arg) {

    ssize_t nwritten;

    pthread_detach(pthread_self());

    int client_fd = (int) (long) arg;
    
    int yes = 1, tcp_keepalive_probes = 3, tcp_keepalive_time = 5, tcp_keepalive_intvl = 2;
    int nodelay = 1;

    if (setsockopt(client_fd, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) == -1) {
        log_error("setsockopt failed: %s", strerror(errno));
        close(client_fd);
        return NULL;
    }

    if (setsockopt(client_fd, SOL_TCP, TCP_KEEPCNT, &tcp_keepalive_probes, sizeof(tcp_keepalive_probes)) == -1) {
        log_error("setsockopt failed: %s", strerror(errno));
        close(client_fd);
        return NULL;
    }

    if (setsockopt(client_fd, SOL_TCP, TCP_KEEPIDLE, &tcp_keepalive_time, sizeof(tcp_keepalive_time)) == -1) {
        log_error("setsockopt failed: %s", strerror(errno));
        close(client_fd);
        return NULL;
    }

    if (setsockopt(client_fd, SOL_TCP, TCP_KEEPINTVL, &tcp_keepalive_intvl, sizeof(tcp_keepalive_intvl)) == -1) {
        log_error("setsockopt failed: %s", strerror(errno));
        close(client_fd);
        return NULL;
    }

    if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay)) == -1) {
        log_error("setsockopt failed: %s", strerror(errno));
        close(client_fd);
        return NULL;
    }

    size_t request_size = 8192;
    struct request *request_back = NULL;
    struct request *request = (struct request *) malloc(request_size);
    if (!request) {
        log_error("malloc failed: %s", strerror(errno));
        close(client_fd);
        return NULL;
    }
          
    msgpack_unpacked msg;
    msgpack_unpacked_init(&msg);
    
    bool enable_gzip = false;

    while (1) {
        
        struct response_header response_header;
    
        ssize_t recvsize = readn(client_fd, request, sizeof(struct request));

        if (!recvsize) {
            log_warn("peer closed connection");
            break;
        }

        if (recvsize < (ssize_t) sizeof(struct request)) {
            log_warn("error while receiving header, received %zd", recvsize);
            break;
        }
        
        if (request->payload_size > 256 * 1024 * 1024) {
            log_warn("payload size %"PRIu32" too large", request->payload_size);
            break;
        }
        
        if (sizeof(struct request) + request->payload_size > request_size) {
            request_size = sizeof(struct request) + request->payload_size;
            request_back = request;
            request = realloc(request, request_size);
            if (!request) {
                log_error("realloc failed: %s", strerror(errno));
                free(request_back);
                break;
            }
        }
        
        recvsize = readn(client_fd, request->payload, request->payload_size);

        if (!recvsize) {
            log_warn("peer closed connection");
            break;
        }

        if (recvsize < request->payload_size) {
            log_warn("error while receiving payload, received %zd (should be %"PRIu32")", recvsize, request->payload_size);
            break;
        }
        
        if (request->flags & REQUEST_FLAG_GZIP) {
            enable_gzip = true;
            uLongf destLen = 16;
            Bytef *dest = malloc(destLen);
            if (!dest) {
                log_error("malloc failed: %s", strerror(errno));
                break;
            }

            int ret;
keep_malloc:
            ret = uncompress(dest, &destLen, (Bytef *) request->payload, request->payload_size);
            if (ret == Z_BUF_ERROR) {
                destLen = destLen * 2;
                free(dest);
                dest = malloc(destLen);
                if (!dest) {
                    log_error("malloc failed: %s", strerror(errno));
                    break;
                }
                goto keep_malloc;
            }
            
            if (ret != Z_OK) {
                free(dest);
                break;
            }

            request->flags &= ~REQUEST_FLAG_GZIP;
            if (sizeof(struct request) + destLen > request_size) {
                request_size = sizeof(struct request) + destLen;
                request_back = request;
                request = realloc(request, request_size);
                if (!request) {
                    log_error("realloc failed: %s", strerror(errno));
                    free(request_back);
                    free(dest);
                    break;
                }
            }
            memcpy(request->payload, dest, destLen);
            request->payload_size = destLen;

            free(dest);
        }

        bool success = msgpack_unpack_next(&msg, request->payload, request->payload_size, NULL);
        if (!success) {
            log_warn("error while parsing payload");
            break;
        }
  
        msgpack_sbuffer* buffer = msgpack_sbuffer_new();
        msgpack_packer* pk = msgpack_packer_new(buffer, msgpack_sbuffer_write);
        
        response_header.seq = request->seq;
        
        __sync_add_and_fetch(&pending_commands, 1);
        if (server->terminated) {
            __sync_add_and_fetch(&pending_commands, -1);
            break;
        }

        if (server->redirection) {
            response_header.status = RESPONSE_STATUS_REDIRECTION;
            msgpack_pack_map(pk, 2);
            mp_pack_string(pk, "host");
            mp_pack_string(pk, server->redirection->host);
            mp_pack_string(pk, "port");
            msgpack_pack_uint16(pk, server->redirection->port);
        }
        else {
            response_header.status = execute_command(request, request->command, msg.data, pk);
        }
        __sync_add_and_fetch(&pending_commands, -1);
        
        msgpack_packer_free(pk);
        
        if (!(request->flags & REQUEST_FLAG_NO_REPLY)) {
            if (!(buffer->size > 0)) {
                response_header.payload_size = 0;
                nwritten = writen(client_fd, &response_header, sizeof(response_header));
                if (!nwritten) {
                    log_error("writen failed: %s", strerror(errno));
                    msgpack_sbuffer_free(buffer);
                    break;
                }
            }
            else {
                if (!enable_gzip) {
                    response_header.payload_size = buffer->size;
                    nwritten = writen(client_fd, &response_header, sizeof(response_header));
                    if (!nwritten) {
                        log_error("writen failed: %s", strerror(errno));
                        msgpack_sbuffer_free(buffer);
                        break;
                    }

                    nwritten = writen(client_fd, buffer->data, buffer->size);
                    if (!nwritten) {
                        log_error("writen failed: %s", strerror(errno));
                        msgpack_sbuffer_free(buffer);
                        break;
                    }
                }
                else {
                    uLongf destLen = buffer->size * 1.00101 + 13;
                    Bytef *dest = malloc(destLen);
                    if (!dest) {
                        log_error("malloc failed: %s", strerror(errno));
                        msgpack_sbuffer_free(buffer);
                        break;
                    }
                    int ret = compress(dest, &destLen, (Bytef *) buffer->data, buffer->size);

                    while (ret == Z_BUF_ERROR) {
                        destLen = destLen * 2 + 16;
                        free(dest);
                        dest = malloc(destLen);
                        if (!dest) {
                            log_error("malloc failed: %s", strerror(errno));
                            msgpack_sbuffer_free(buffer);
                            break;
                        }
                        ret = compress(dest, &destLen, (Bytef *) buffer->data, buffer->size);
                    }

                    if (ret != Z_OK) {
                        log_error("error while compressing response: %d", ret);
                        if (dest)
                            free(dest);
                        break;
                    }

                    response_header.payload_size = destLen;
                    nwritten = writen(client_fd, &response_header, sizeof(response_header));
                    if (!nwritten) {
                        log_warn("peer closed connection");
                        msgpack_sbuffer_free(buffer);
                        free(dest);
                        break;
                    }

                    if (nwritten < 0) {
                        log_error("send response header failed: %s", strerror(errno));
                        msgpack_sbuffer_free(buffer);
                        free(dest);
                        break;
                    }

                    if (nwritten < (ssize_t)sizeof(response_header)) {
                        log_warn("error while sending response header, sent %zd (should be %"PRIu64")", nwritten, sizeof(response_header));
                        msgpack_sbuffer_free(buffer);
                        free(dest);
                        break;
                    }

                    nwritten = writen(client_fd, dest, destLen);
                    if (!nwritten) {
                        log_warn("peer closed connection");
                        msgpack_sbuffer_free(buffer);
                        free(dest);
                        break;
                    }

                    if (nwritten < 0) {
                        log_error("send response failed: %s", strerror(errno));
                        msgpack_sbuffer_free(buffer);
                        free(dest);
                        break;
                    }

                    if (nwritten < (ssize_t)destLen) {
                        log_warn("error while sending response, sent %zd (should be %"PRIu64")", nwritten, destLen);
                        msgpack_sbuffer_free(buffer);
                        free(dest);
                        break;
                    }

                    free(dest);
                }
            }
        }
        
        msgpack_sbuffer_free(buffer);
        
        if (request_size > 65536) {
            request_size = 8192;
            request_back = request;
            request = realloc(request, request_size);
            if (!request) {
                log_error("realloc failed: %s", strerror(errno));
                free(request_back);
                break;
            }
        }
    }
    
    close(client_fd);
    
    free(request);
    msgpack_unpacked_destroy(&msg);
    
    return NULL;
}

int server_wait(struct server *server) {

    int ret = 0;
    struct sockaddr_in client_addr;
    socklen_t sin_size = sizeof(struct sockaddr_in);

    log_notice("waiting for connections...");

    struct pollfd pollfd;
    pollfd.fd = server->fd;
    pollfd.events = POLLIN;

    while (1)
    {
        while (server->terminated) {
            freeze = 0;
            int pending = __sync_add_and_fetch(&pending_commands, 0);
            log_notice("waiting for pending commands... %d left", pending);
            if (pending == 0)
                goto err;
            usleep(200000);
        }
        
        if (poll(&pollfd, 1, 100) <= 0)
            continue;

        int client_fd = accept(server->fd, (struct sockaddr *) &client_addr, &sin_size);
        if (client_fd == -1) {
            usleep(1000);
            continue;
        }
            
        pthread_t thread_id;
        int err = pthread_create(&thread_id, NULL, client_thread, (void *) (long) client_fd);
        if (err) {
            log_error("can't create thread: %s", strerror(err));
            close(client_fd);
        }
    }
err:
    return ret;
}
