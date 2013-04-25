/*
 * +-----------------------------------------------------------------------+
 * | Slab memory allocator.                                                |
 * +-----------------------------------------------------------------------+
 * | Author: jingmi@baidu.com                                              |
 * +-----------------------------------------------------------------------+
 *
 * $Id: slab.h,v 1.1 2009/07/16 09:07:30 jingmi Exp $
 */

#ifndef _SLAB_H_
#define _SLAB_H_

#include <sys/signal.h>
#include <sys/resource.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdbool.h>
#include <limits.h>

#define POWER_SMALLEST 1
#define POWER_LARGEST  200
#define POWER_BLOCK (1024*1024*4)
#define CHUNK_ALIGN_BYTES (sizeof(void *))
#define TINY_SIZE    16 /* smallest size of slab */

#ifdef NEED_DEBUG
#define NEED_DEBUG 1 /* 1/2 to enable debug mode, 0 disable */
#else
#define NEED_DEBUG 0
#endif

#define PRINT_TO_FILE 0 /* whether print debug info to file */
#define USE_SPIN_LOCK
#define USE_VALLOC
#define BSEARCH_BLOCK
#define USE_FIXED_BLOCK
#define DETECT_MEMORY_OVERRUN
//#define TRACE_MEM_ALLOC
//#define SINGLE_THREAD
//#define USE_SYSTEM_MALLOC

    /* Init the subsystem. 1st argument is the limit on no. of bytes to allocate,
       0 if no limit. 2nd argument is the growth factor; each slab will use a chunk
       size equal to the previous slab's chunk size times this factor. */
    void slab_init(const size_t limit, const double factor);

    /*
     * Given object size, return id to use when allocating/freeing memory for object
     * 0 means error: can't store such a large object
     */
    unsigned int slab_clsid(const size_t size);

#ifdef TRACE_MEM_ALLOC
    #define slab_alloc(_size) ({void *_ret_ptr_=_slab_alloc(_size); printf("%s [%s:%d]%p=slab_alloc(%u)\n", __func__, __FILE__, __LINE__, _ret_ptr_, (unsigned int)(_size)); _ret_ptr_;})
#else
    #define slab_alloc(_size) ({void *_ret_ptr_=_slab_alloc(_size); _ret_ptr_;})
#endif
    /* Allocate object of given length. 0 on error */
    void *_slab_alloc(size_t size) __attribute__ ((malloc));

#ifdef TRACE_MEM_ALLOC
    #define slab_realloc(_ptr, _size) ({void *_ret_ptr_=_slab_realloc(_ptr, _size); printf("%s [%s:%d]%p=slab_realloc(%p, %u)\n", __func__, __FILE__, __LINE__, _ret_ptr_, _ptr, (unsigned int)(_size)); _ret_ptr_;})
#else
    #define slab_realloc(_ptr, _size) ({void *_ret_ptr_=_slab_realloc(_ptr, _size); _ret_ptr_;})
#endif
    /* Realloc object for new size */
    void *_slab_realloc(void *ptr, const size_t size) __attribute__ ((malloc));

#ifdef TRACE_MEM_ALLOC
    #define slab_free(_ptr) ({_slab_free(_ptr); printf("%s [%s:%d]slab_free(%p)\n", __func__, __FILE__, __LINE__, _ptr);})
#else
    #define slab_free(_ptr) ({_slab_free(_ptr);})
#endif

    /* Free previously allocated object */
    void _slab_free(void *ptr);

    /* Fill buffer with stats */
    void slab_stats(void);

#ifdef TRACE_MEM_ALLOC
    #define slab_calloc(_size) ({void *_ptr=_slab_calloc(_size); printf("%s [%s:%d]%p=slab_calloc(%u)\n", __func__, __FILE__, __LINE__, _ptr, (unsigned int)(_size)); _ptr;})
#else
    #define slab_calloc(_size) ({void *_ptr=_slab_calloc(_size); _ptr;})
#endif
    /** 
     * @brief slab_calloc() allocates memory for an array of size bytes and returns
     *        a pointer to the allocated memory.  The memory is set to zero.
     * 
     * @param size Size you want.
     * 
     * @return NULL if failed, otherwise successful.
     */
    static inline void * _slab_calloc(size_t size) __attribute__ ((always_inline));
    static inline void * _slab_calloc(size_t size)
    {
        void *ptr;

        if ((ptr=_slab_alloc(size)) == NULL)
            return NULL;
        memset(ptr, 0, size);

        return ptr;
    }

#ifndef USE_SYSTEM_MALLOC
    #define SLAB_ALLOC
    #define SLAB_REALLOC
    #define SLAB_FREE
    #define SLAB_CALLOC
#endif /* ! USE_SYSTEM_MALLOC */

#endif /* ! _SLAB_H_ */

/* vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker: */
