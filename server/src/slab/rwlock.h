/* © Copyright 2011 jingmi. All Rights Reserved.
 *
 * +----------------------------------------------------------------------+
 * | read & write lock                                                    |
 * +----------------------------------------------------------------------+
 * | Author: mi.jing@jiepang.com                                          |
 * +----------------------------------------------------------------------+
 * | Created: 2011-11-04 15:00                                            |
 * +----------------------------------------------------------------------+
 */
#ifndef _RWLOCK_H_
#define _RWLOCK_H_

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>

/** 
 * @brief read-write lock with priority for read.
 */
struct rwlock_t
{
    pthread_mutex_t lock;
    pthread_cond_t condition;
    volatile size_t tobe_write_queue_len;   /* queue length of request to be write */
    volatile size_t write_queue_len;        /* queue length of request is now writing */
    volatile size_t read_queue_len;         /* queue length of request is NOW READING and TO BE READ */
};

struct rwlock_t * rwlock_init(void* (*my_alloc)(size_t));
struct rwlock_t * rwlock_new();
int rwlock_build(struct rwlock_t *rwlock);
int rwlock_rdlock(struct rwlock_t *rwlock);
int rwlock_unrdlock(struct rwlock_t *rwlock);
int rwlock_wrlock(struct rwlock_t *rwlock);
int rwlock_unwrlock(struct rwlock_t *rwlock);
int rwlock_destory(struct rwlock_t *rwlock, void (*my_free)(void *));
int rwlock_free(struct rwlock_t *rwlock);

#endif /* ! _RWLOCK_H_ */

/* vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker: */
