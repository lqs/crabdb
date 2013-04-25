#ifndef _TABLE_H
#define _TABLE_H

#include "schema.h"
#include "bucket.h"
#include <pthread.h>
#include "uthash.h"

#define TABLE_INSERT_FLAG_OVERWRITE     (1 << 0)

#define TABLE_INSERT_MODIFIED           (1 << 0)
#define TABLE_INSERT_LENGTH_INCREASED   (1 << 1)

struct table {
    pthread_rwlock_t rwlock;
    int64_t id;
    uint32_t len;
    uint32_t cap;
    bool writing;
    struct schema *schema;
    struct bucket *bucket;
    void *data;
    UT_hash_handle hh;
};

struct table_data {
    int64_t table_id;
    uint64_t schema_hash;
    uint32_t crc32;
    uint32_t timestamp;
    uint32_t size;
    char data[];
};

extern int freeze;
extern int freeze_done;
extern int64_t last_write_nonce;

extern struct rwlock_t *io_rwlock;

struct table *table_create_and_read(struct bucket *bucket, int64_t id, int flags);
void table_destroy(struct table *table);
int table_find(struct table *table, int64_t pk);
int table_insert_nolock(struct table *table, struct schema *schema, void *data, int flags);
void table_write(struct table *table);
void table_writer_init();
void table_check_schema(struct table *table);

#endif
