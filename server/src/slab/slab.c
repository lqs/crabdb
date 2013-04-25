/*
 * +-----------------------------------------------------------------------+
 * | Slabs memory allocation, based on powers-of-N. Slabs are up to 1MB in |
 * | size and are divided into chunks.                                     |
 * +-----------------------------------------------------------------------+
 * | Author: jingmi@baidu.com                                              |
 * +-----------------------------------------------------------------------+
 *
 * $Id: slab.cpp,v 1.2 2009/09/11 08:19:40 jingmi Exp $
 */

#include "slab.h"

#ifdef USE_SPIN_LOCK

#define slab_lock_t pthread_spinlock_t
#define slab_lock_init(lockptr) pthread_spin_init(lockptr, PTHREAD_PROCESS_PRIVATE)
#define slab_lock   pthread_spin_lock
#define slab_unlock pthread_spin_unlock

#else /* USE MUTEX LOCK */

#define slab_lock_t pthread_mutex_t
#define slab_lock_init(lockptr) pthread_mutex_init(lockptr, NULL)
#define slab_lock   pthread_mutex_lock
#define slab_unlock pthread_mutex_unlock

#endif /* ! USE_SPIN_LOCK */

#ifdef SINGLE_THREAD

#undef slab_lock_init
#define slab_lock_init(lockptr) ((void)0)
#undef slab_lock
#define slab_lock(ptr) ((void)0)
#undef slab_unlock
#define slab_unlock(ptr) ((void)0)

#endif /* ! SINGLE_THREAD */

/* powers-of-N allocation structures */
typedef struct
{
    unsigned int size;      /* sizes of items */
    unsigned int perslab;   /* how many items per slab */

    void **slots;           /* list of free item ptrs */
    unsigned int sl_total;  /* size of previous array */
    unsigned int sl_curr;   /* first free slot */

    void *end_page_ptr;         /* pointer to next free item at end of page, or 0 */
    unsigned int end_page_free; /* number of items remaining at end of last alloced page */

    unsigned int slabs;     /* how many slabs were allocated for this class */

    void **slab_list;       /* array of slab pointers */
    unsigned int list_size; /* size of prev array */

    slab_lock_t lock;
} slabclass_t;

typedef struct
{
    bool is_malloc;
    uint32_t size;
#if NEED_DEBUG >= 1
    uint32_t magic_head;
    uint32_t color; /* do NOT remove this line */
#endif /* ! NEED_DEBUG */
    void* mem[];
} slabhead_t;

/*
 * Global static variables.
 */
static slabclass_t slabclass[POWER_LARGEST + 1];
static size_t mem_limit = 0;
static size_t mem_malloced = 0;
static int power_largest;
/*
 *static void **mem_pool = NULL;
 *static int *free_block_array = NULL;
 *static *used_block_array = NULL;
 */

#if NEED_DEBUG >= 1
static uint64_t alloc_times = 0;
static uint64_t free_times = 0;
static uint64_t realloc_times = 0;
#endif /* ! NEED_DEBUG */

/*
 * Forward Declarations
 */
static int do_slabs_newslab(const unsigned int id);

/** 
 * @brief Figures out which slab class (chunk size) is required to store an item of
 *        a given size.
 *        Given object size, return id to use when allocating/freeing memory for object.
 *        0 means error: can't store such a large object
 *
 * @param size Size of memory will use.
 * 
 * @return Proper slab class id, or 0, or power_largest+1.
 */
unsigned int
slab_clsid(const size_t size)
{
    if (size == 0)
    {
        return 0;
    }

#ifdef BSEARCH_BLOCK

    int res;
    int left, right;

    left = 1;
    right = power_largest;
    res = (left + right) / 2;

    while (left <= right)
    {
        if (size < slabclass[res].size)
        {
            if (size > slabclass[res-1].size)
                return res;
            else
            {
                right = res - 1;
                res = (left + right) / 2;
                continue;
            }
        }
        else if (size > slabclass[res].size)
        {
            left = res + 1;
            res = (left + right) / 2;
            continue;
        }
        return res;
    }

    /* didn't find suitable block */
    return (power_largest + 1);

#else

    int res = POWER_SMALLEST;

    while (size > slabclass[res].size)
    {
        if (res++ == power_largest)
        {
            return (power_largest + 1);
        }
    }
    return res;

#endif /* ! BSEARCH_BLOCK */
}

/** 
 * @brief Determines the chunk sizes and initializes the slab class descriptors
 *        accordingly.
 * 
 * @param limit The limits of memory will be used by the slab allocator, 0 is 
 *              no limit.
 * @param factor The factor of growth every time.
 */
void
slab_init(const size_t limit, const double factor)
{
    int i = POWER_SMALLEST - 1;
    unsigned int size = TINY_SIZE;

    assert(factor > 1.0);

    mem_limit = limit;
    memset(slabclass, 0, sizeof(slabclass));

    while (++i < POWER_LARGEST && size <= POWER_BLOCK / 2)
    {
        /* Make sure items are always n-byte aligned */
        if (size % CHUNK_ALIGN_BYTES)
        {
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        }

        slabclass[i].size = size;
        slabclass[i].perslab = POWER_BLOCK / slabclass[i].size;
        slab_lock_init(&(slabclass[i].lock));
        size = (int)((double)size * factor);
#if NEED_DEBUG >= 2
            fprintf(stdout, "slab class %3d: chunk size %6u perslab %5u\n",
                    i, slabclass[i].size, slabclass[i].perslab);
#endif /* ! NEED_DEBUG */
    }

    power_largest = i;
    slabclass[power_largest].size = POWER_BLOCK;
    slabclass[power_largest].perslab = 1;
    slab_lock_init(&(slabclass[power_largest].lock));
}

static int
grow_slab_list (const unsigned int id)
{
    slabclass_t *p = &slabclass[id];
    if (p->slabs == p->list_size)
    {
        size_t new_size =  (p->list_size != 0) ? p->list_size * 2 : 16;
        void *new_list = realloc(p->slab_list, new_size * sizeof(void *));
        if (new_list == 0)
        {
            return 0;
        }
        p->list_size = new_size;
        p->slab_list = (void **)new_list;
    }
    return 1;
}

static int
do_slabs_newslab(const unsigned int id)
{
    slabclass_t *p = &slabclass[id];
#ifdef USE_FIXED_BLOCK
    int len = POWER_BLOCK;
#else
    int len = p->size * p->perslab;
#endif
    char *ptr;

    if (mem_limit && mem_malloced + len > mem_limit && p->slabs > 0)
        return 0;

    if (grow_slab_list(id) == 0) return 0;

#ifdef USE_VALLOC
    ptr = (char *)valloc((size_t)len);
#else   /* USE malloc(3) */
    ptr = (char *)malloc((size_t)len);
#endif /* VALLOC */
    if (ptr == 0) return 0;
#if NEED_DEBUG >= 1
    memset(ptr, 0xCC, (size_t)len);
#endif /* ! NEED_DEBUG */

    p->end_page_ptr = ptr;
    p->end_page_free = p->perslab;

    p->slab_list[p->slabs++] = ptr;
    mem_malloced += len;

    return 1;
}

/** 
 * @brief Use slab allocator to allocates size bytes of memory.
 *        _slab_alloc() will not initialize the memory.
 * 
 * @param size Size you need.
 * 
 * @return Pointer to allocated memory, NULL if failed.
 */
void *
_slab_alloc(size_t size)
{
#ifdef USE_SYSTEM_MALLOC

    if (mem_limit && mem_malloced + size > mem_limit)
        return 0;
    mem_malloced += size;
    return malloc(size);

#else   /* ! DONOT USE_SYSTEM_MALLOC, USE SLAB INSTEAD */

#if NEED_DEBUG >= 1
    alloc_times ++;
#endif /* NEED_DEBUG */
    slabclass_t *p;
    slabhead_t *head;
    int id;

    size += sizeof(slabhead_t);
    id = slab_clsid(size);
    head = NULL;
    if (id < POWER_SMALLEST)
    {
        return NULL;
    }
    else if (id > power_largest)
    {
        head = (slabhead_t *)malloc(size);
        if (head != NULL)
        {
            head->is_malloc = true;
            head->size = size;
#if NEED_DEBUG >= 1
            head->magic_head = 0xfeedbeef;
#endif /* ! NEED_DEBUG */
#if NEED_DEBUG >= 2
            fprintf(stdout, "malloc(%zu)\n", size);
#endif /* ! NEED_DEBUG */
        }
        else
        {
            return NULL;
        }

        return (void *)(head->mem);
    }

    p = &slabclass[id];
#if NEED_DEBUG >= 2
    fprintf(stdout, "slab_alloc(%zu), class size=%zu\n", size, p->size);
#endif /* NEED_DEBUG */
    slab_lock(&(p->lock));

    /* fail unless we have space at the end of a recently allocated page,
       we have something on our freelist, or we could allocate a new page */
    if (! (p->end_page_ptr != 0 || p->sl_curr != 0 || do_slabs_newslab(id) != 0))
    {
        slab_unlock(&(p->lock));
        return NULL;
    }

    /* return off our freelist, if we have one */
    if (p->sl_curr != 0)
    {
        head = (slabhead_t *)p->slots[--p->sl_curr];
        head->is_malloc = false;
        head->size = size;
#if NEED_DEBUG >= 1
        head->magic_head = 0xfeedbeef;
#endif /* ! NEED_DEBUG */
        slab_unlock(&(p->lock));

        return (void *)(head->mem);
    }

    /* if we recently allocated a whole page, return from that */
    if (p->end_page_ptr)
    {
        head = (slabhead_t *)p->end_page_ptr;
        head->is_malloc = false;
        head->size = size;
#if NEED_DEBUG >= 1
        head->magic_head = 0xfeedbeef;
#endif /* ! NEED_DEBUG */
        if (--p->end_page_free != 0)
        {
            p->end_page_ptr = (void *)((intptr_t)p->end_page_ptr + p->size);
        }
        else
        {
            p->end_page_ptr = NULL;
        }
        slab_unlock(&(p->lock));
        return (void *)(head->mem);
    }

    slab_unlock(&(p->lock));
    return NULL;  /* shouldn't ever get here */

#endif /* ! USE_SYSTEM_MALLOC */
}

/** 
 * @brief The slab_free() causes the allocated memory referenced by ptr to be
 *        made available for future allocations. If ptr is NULL, no action
 *        occurs.
 * 
 * @param ptr Pointer to the memory to be free.
 */
void
_slab_free(void *ptr)
{
    if (ptr == NULL)
    {
        return;
    }

#if NEED_DEBUG >= 1
    free_times ++;
#endif /* ! NEED_DEBUG */

    slabhead_t *head;
    size_t size;

    head = (slabhead_t *)((intptr_t)ptr - sizeof(slabhead_t));
    size = head->size;

#if NEED_DEBUG >= 2
    fprintf(stdout, "slab_free(), size=%zu\n", size);
#endif /* NEED_DEBUG */

#ifdef USE_SYSTEM_MALLOC

    mem_malloced -= size;
    free(ptr);
    return;

#else   /* ! DONOT USE_SYSTEM_MALLOC, USE SLAB INSTEAD */

    unsigned char id;
    slabclass_t *p;

#if NEED_DEBUG >= 1
    assert(head->magic_head == 0xfeedbeef);
#endif /* ! NEED_DEBUG */

    /* if allocated by malloc(3) */
    if (head->is_malloc)
    {
#if NEED_DEBUG >= 2
        fprintf(stdout, "malloc_free(%d)\n", head->size);
#endif /* ! NEED_DEBUG */
        free(head);
        return;
    }
    id = slab_clsid(size);
    if (id < POWER_SMALLEST)
    {
        return;
    }

    p = &slabclass[id];
    slab_lock(&(p->lock));

    if (p->sl_curr == p->sl_total)   /* need more space on the free list */
    {
        int new_size = (p->sl_total != 0) ? p->sl_total * 2 : 1024;  /* 1024 is arbitrary */
        void **new_slots = (void **)realloc(p->slots, new_size * sizeof(void *));
        if (new_slots == 0)
        {
            slab_unlock(&(p->lock));
            return;
        }
        p->slots = new_slots;
        p->sl_total = new_size;
    }
    p->slots[p->sl_curr++] = head;
#if NEED_DEBUG >= 1
    memset(head, 0xCC, sizeof(slabhead_t));
#endif /* ! NEED_DEBUG */
    slab_unlock(&(p->lock));
    return;

#endif /* ! USE_SYSTEM_MALLOC */
}

/** 
 * @brief Print slab stat info to terminal or file.
 */
void
slab_stats(void)
{
#if NEED_DEBUG >= 1

    int i;

    if (power_largest == 0)
    {
        fprintf(stderr, "didn't init!!!\n");
        return ;
    }
    for (i = POWER_SMALLEST; i <= power_largest; i++)
    {
        slabclass_t *p = &slabclass[i];
        if (p->slabs != 0)
        {
#if PRINT_TO_FILE == 1
#else
            printf("size: %d, slab_cnt: %d, count: %d, free: %d\n", p->size, p->slabs, (p->perslab)*(p->slabs), p->sl_curr);
#endif
#if NEED_DEBUG >= 2

            slabhead_t *slab_header;
            void *header;
            unsigned int perslab, slabs;
            for (slabs=0; slabs<p->slabs; slabs++)
            {
#if PRINT_TO_FILE == 1
#else
                printf("slab [%d]\n", slabs);
#endif
                header = p->slab_list[slabs];
                for (perslab=0; perslab<p->perslab; perslab++, header+=p->size)
                {
                    slab_header = header;
                    if (slab_header->magic_head == 0xfeedbeef)
#if PRINT_TO_FILE == 1
#else
                        printf("X");
#endif
                    else
#if PRINT_TO_FILE == 1
#else
                        printf(".");
#endif
                }
#if PRINT_TO_FILE == 1
#else
                printf("\n");
#endif
            }
#endif /* ! NEED_DEBUG */
        }
    }

#ifdef __FreeBSD__
    printf("static: mem_malloced: %zd, alloc_times: %lu, free_times: %lu, realloc_times: %lu\n", mem_malloced, alloc_times, free_times, realloc_times);
#elif linux
#if __x86_64__
    printf("static: mem_malloced: %zd, alloc_times: %lu, free_times: %lu, realloc_times: %lu\n", mem_malloced, alloc_times, free_times, realloc_times);
#else
    printf("static: mem_malloced: %zd, alloc_times: %llu, free_times: %llu, realloc_times: %llu\n", mem_malloced, alloc_times, free_times, realloc_times);
#endif /* ! linux */
#else
#error "OS unknown"
#endif /* __FreeBSD__ */

    return;
#endif /* ! NEED_DEBUG */
}

/** 
 * @brief slab_realloc() changes the size of the memory block pointed to by ptr
 *        to size bytes. The contents will be unchanged to  the  minimum  of
 *        the  old  and  new  sizes; newly allocated memory will be
 *        uninitialized.
 *        If ptr is NULL, the call is equivalent to slab_alloc(size).
 *        If size is zero, it will return NULL directly.
 *        If size is less than new size, then truncates.
 * 
 * @param ptr The pointer to the memory you want to realloc.
 * @param size New size you need.
 * 
 * @return Null if failed, otherwise successful.
 */
void *
_slab_realloc(void *ptr, const size_t size)
{
#if NEED_DEBUG >= 1
    realloc_times ++;
#endif /* ! NEED_DEBUG */
    void *p;
    slabhead_t *head;
    size_t new_size;

    if (size == 0)
    {
        return NULL;
    }
    if (ptr == NULL)
    {
        return slab_alloc(size);
    }
    else
    {
        if ((p=slab_alloc(size)) == NULL)
            return NULL;
        head = (slabhead_t *)((intptr_t)ptr - sizeof(slabhead_t));
#if NEED_DEBUG >= 1
        assert(head->magic_head == 0xfeedbeef);
#endif /* ! NEED_DEBUG */
        new_size = size < head->size ? size : head->size;
        memcpy(p, head->mem, new_size);
        slab_free(head->mem);
        return (void*)p;
    }

    return NULL;
}

#if NEED_DEBUG >= 1
static void debug_flag(void) __attribute__ ((unused));
static void debug_flag(void) {};
#endif

/* vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker: */
