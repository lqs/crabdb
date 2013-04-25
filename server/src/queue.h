#include <semaphore.h>
#include <sys/time.h>
#include "log.h"

#define DECLARE_QUEUE(TYPE, MAX_LENGTH, NAME)               \
                                                                                     \
struct NAME {                                                                        \
    TYPE data[MAX_LENGTH];                                                           \
    bool full;                                                                       \
    int32_t begin, end;                                                              \
    sem_t sem;                                                                       \
    pthread_mutex_t mutex;                                                           \
};                                                                                   \
                                                                                     \
static struct NAME NAME;                                                             \
                                                                                     \
static void NAME ## _init(struct NAME *queue) {                                      \
    sem_init(&queue->sem, 0, 0);                                                     \
    queue->full = 0;                                                                 \
    queue->begin = queue->end = 0;                                                   \
    pthread_mutex_init(&queue->mutex, NULL);                                         \
}                                                                                    \
                                                                                     \
static bool NAME ## _is_full(struct NAME *queue) {                         \
    bool is_full;                                                                    \
    pthread_mutex_lock(&queue->mutex);                                               \
    is_full = queue->full;                                                           \
    pthread_mutex_unlock(&queue->mutex);                                             \
    return is_full;                                                                  \
}                                                                                    \
                                                                                     \
static TYPE NAME ## _timedpoll(struct NAME *queue, useconds_t usec) {                \
    struct timeval timeval;                                                          \
    struct timespec timespec;                                                        \
    gettimeofday(&timeval, NULL);                                                    \
    timespec.tv_sec = timeval.tv_sec + usec / 1000000;                               \
    timespec.tv_nsec = (timeval.tv_usec + usec % 1000000) * 1000;                    \
                                                                                     \
    timespec.tv_sec += timespec.tv_nsec / 1000000000;                                \
    timespec.tv_nsec %= 1000000000;                                                  \
                                                                                     \
    for (;;) {                                                                       \
        int res = sem_timedwait(&queue->sem, &timespec);                             \
        if (res == -1 && ETIMEDOUT)                                                  \
            return NULL;                                                             \
                                                                                     \
        pthread_mutex_lock(&queue->mutex);                                           \
                                                                                     \
        if (queue->begin != queue->end || queue->full) {                             \
            TYPE res = queue->data[queue->begin];                                    \
            queue->begin = (queue->begin + 1) % (MAX_LENGTH);                        \
            queue->full = false;                                                     \
            pthread_mutex_unlock(&queue->mutex);                                     \
            return res;                                                              \
        }                                                                            \
        pthread_mutex_unlock(&queue->mutex);                                         \
    }                                                                                \
}                                                                                    \
                                                                                     \
static void NAME ## _add(struct NAME *queue, TYPE data) {                            \
again:                                                                               \
    pthread_mutex_lock(&queue->mutex);                                               \
    if (queue->full) {                                                               \
        pthread_mutex_unlock(&queue->mutex);                                         \
        log_warn("queue full, sleeping...");                                         \
        usleep(200000);                                                              \
        goto again;                                                                  \
    }                                                                                \
                                                                                     \
    queue->data[queue->end] = data;                                                  \
                                                                                     \
    queue->end = (queue->end + 1) % (MAX_LENGTH);                                    \
    if (queue->end == queue->begin)                                                  \
        queue->full = true;                                                          \
                                                                                     \
    pthread_mutex_unlock(&queue->mutex);                                             \
    sem_post(&queue->sem);                                                           \
}
