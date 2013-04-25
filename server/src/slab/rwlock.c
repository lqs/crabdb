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
#include "rwlock.h"

/** 
 * @brief Init read-write lock.
 * 
 * @param my_alloc Memory allocator.
 * 
 * @return NULL if failed, otherwise success.
 */
struct rwlock_t *
rwlock_init(void* (*my_alloc)(size_t))
{
    struct rwlock_t *rwlock = my_alloc(sizeof(struct rwlock_t));

    if (rwlock == NULL)
        return NULL;
    rwlock->write_queue_len = 0;
    rwlock->read_queue_len = 0;
    rwlock->tobe_write_queue_len = 0;
    pthread_mutex_init(&(rwlock->lock), NULL);
    pthread_cond_init(&(rwlock->condition), NULL);

    return rwlock;
}

struct rwlock_t *
rwlock_new()
{
    struct rwlock_t *rwlock = malloc(sizeof(struct rwlock_t));

    if (rwlock == NULL)
        return NULL;
    rwlock->write_queue_len = 0;
    rwlock->read_queue_len = 0;
    rwlock->tobe_write_queue_len = 0;
    pthread_mutex_init(&(rwlock->lock), NULL);
    pthread_cond_init(&(rwlock->condition), NULL);

    return rwlock;
}

int
rwlock_build(struct rwlock_t *rwlock)
{
    if (rwlock == NULL)
        return -1;

    rwlock->write_queue_len = 0;
    rwlock->read_queue_len = 0;
    rwlock->tobe_write_queue_len = 0;
    pthread_mutex_init(&(rwlock->lock), NULL);
    pthread_cond_init(&(rwlock->condition), NULL);

    return 0;
}

/** 
 * @brief 
 * 
 * @param rwlock 
 * 
 * @return 
 */
int
rwlock_rdlock(struct rwlock_t *rwlock)
{
    assert(rwlock != NULL);

    pthread_mutex_lock(&rwlock->lock);
    rwlock->read_queue_len ++;
    while (rwlock->write_queue_len > 0)
    {
        pthread_cond_wait(&rwlock->condition, &rwlock->lock);
    }
    pthread_mutex_unlock(&rwlock->lock);

    return 0;
}

int
rwlock_unrdlock(struct rwlock_t *rwlock)
{
    assert(rwlock != NULL);

    pthread_mutex_lock(&rwlock->lock);
    rwlock->read_queue_len --;
    if ((rwlock->read_queue_len == 0) && (rwlock->tobe_write_queue_len > 0))
    {
        pthread_cond_broadcast(&rwlock->condition);
    }
    pthread_mutex_unlock(&rwlock->lock);

    return 0;
}

int
rwlock_wrlock(struct rwlock_t *rwlock)
{
    assert(rwlock != NULL);

    pthread_mutex_lock(&rwlock->lock);
    rwlock->tobe_write_queue_len ++;
    while ((rwlock->read_queue_len > 0) || (rwlock->write_queue_len > 0))
    {
        pthread_cond_wait(&rwlock->condition, &rwlock->lock);
    }
    rwlock->tobe_write_queue_len --;
    rwlock->write_queue_len ++;
    pthread_mutex_unlock(&rwlock->lock);

    return 0;
}

int
rwlock_unwrlock(struct rwlock_t *rwlock)
{
    assert(rwlock != NULL);

    pthread_mutex_lock(&rwlock->lock);
    rwlock->write_queue_len --;
    if ((rwlock->read_queue_len > 0) || (rwlock->tobe_write_queue_len > 0))
    {
        pthread_cond_broadcast(&rwlock->condition);
    }
    pthread_mutex_unlock(&rwlock->lock);

    return 0;
}

int
rwlock_destory(struct rwlock_t *rwlock, void (*my_free)(void *))
{
    assert(rwlock != NULL);

    pthread_mutex_destroy(&rwlock->lock);
    pthread_cond_destroy(&rwlock->condition);

    my_free(rwlock);

    return 0;
}

int
rwlock_free(struct rwlock_t *rwlock)
{
    assert(rwlock != NULL);

    pthread_mutex_destroy(&rwlock->lock);
    pthread_cond_destroy(&rwlock->condition);
    free(rwlock);

    return 0;
}

/* vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker: */
