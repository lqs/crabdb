#ifndef _GLOBAL_H_
#define _GLOBAL_H_
/* © Copyright 2011 jingmi. All Rights Reserved.
 *
 * +----------------------------------------------------------------------+
 * | global var, config etc                                               |
 * +----------------------------------------------------------------------+
 * | Author: mi.jing@jiepang.com                                          |
 * +----------------------------------------------------------------------+
 * | Created: 2011-10-27 23:30                                            |
 * +----------------------------------------------------------------------+
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <errno.h>
#include "log.h"
#include "slab.h"
#include "rwlock.h"

/** 
 * @brief Make sure that our bool type is 1 char.
 */
typedef unsigned char tiny_bool_t;

/** 
 * @brief Amount of tables in buckets while initializing.
 */
extern size_t init_table_cnt;

#ifdef TESTMODE
#define STATIC_MODIFIER
#else
#define STATIC_MODIFIER static
#endif

#ifndef __offset_of
#define __offset_of(type, field)    ((size_t)(&((type *)0)->field))
#endif

#define timeval_sub(vpa, vpb, vpc)                          \
    do{                                                     \
        (vpc)->tv_sec = (vpa)->tv_sec - (vpb)->tv_sec;      \
        (vpc)->tv_usec = (vpa)->tv_usec - (vpb)->tv_usec;   \
        if ((vpc)->tv_usec < 0){                            \
            (vpc)->tv_sec --;                               \
            (vpc)->tv_usec += 1000000;                      \
        }                                                   \
    }while (0)

#define pre_timer() struct timeval start, end, used
#define launch_timer() gettimeofday(&start, NULL);
#define stop_timer() do { gettimeofday(&end, NULL); timeval_sub(&end, &start, &used); } while (0)

#endif /* ! _GLOBAL_H_ */
/* vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker: */
