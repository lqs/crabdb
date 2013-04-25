#ifndef _STORAGE_C_
#define _STORAGE_C_
/* © Copyright 2011 jingmi. All Rights Reserved.
 *
 * +----------------------------------------------------------------------+
 * | header file for data block store                                     |
 * +----------------------------------------------------------------------+
 * | Author: jingmi@gmail.com                                             |
 * +----------------------------------------------------------------------+
 * | Created: 2011-11-04 12:53                                            |
 * +----------------------------------------------------------------------+
 */

#include <libgen.h>
#include "global.h"
#include "bdblib.h"
#include "crc32.h"

#define MAX_FREE_SLOTS (100 * 1000)
#define KEY_LEN (sizeof(uint64_t))

#define DATA_SECTION_POS (sizeof(struct block_file_t) - __offset_of(struct block_file_t, block_size))
#define SLOTS_SECTION_POS (__offset_of(struct block_file_t, free_slots_offset) - __offset_of(struct block_file_t, block_size))

#if DB_NOTFOUND == -1
#error "DB_NOTFOUND should not equal error ret value"
#endif

typedef struct bucket_storage_t
{
    struct bdb_t *indexdb;
    struct rwlock_t *index_lock;
} bucket_storage_t;

/* TODO pack this structure */
typedef struct block_file_t
{
    int fd;
    struct rwlock_t lock;
    size_t block_size; /* don't move first three items since I use block_size to locate */
    size_t block_cnt;
    size_t free_slots_cnt;
    off_t free_slots_offset[MAX_FREE_SLOTS];
} block_file_t;

typedef union index_key_t
{
    unsigned char key[KEY_LEN];
    uint64_t key_num;
} index_key_t;

typedef struct index_value_t
{
    uint32_t crc32;
    uint32_t block_id;
    size_t data_size;
    off_t  offset;
} index_value_t;

typedef struct index_item_t
{
    union index_key_t key;
    struct index_value_t value;
} index_item_t;

extern int open_storage(const char *dir_path, size_t largest_size, struct bucket_storage_t **bsp);
extern int close_storage(struct bucket_storage_t *bsp);

extern int find_block(struct bucket_storage_t *bsp, void *key, size_t key_size, void **value_ptr, size_t *value_len);
extern int delete_block(struct bucket_storage_t *bsp, void *key, size_t key_size);
extern int update_block(struct bucket_storage_t *bsp, void *key, size_t key_len, void *data, size_t data_size);

#endif /* ! _STORAGE_C_ */
/* vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker: */
