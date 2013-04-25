#ifndef BUCKET_H
#define BUCKET_H

#include "table.h"
#include "uthash.h"

struct nonexistent_table {
    int64_t id;
    UT_hash_handle hh;
};



struct bucket {
    pthread_rwlock_t rwlock;
    char name[64];
    struct schema *schema;
    struct table *tables;
    struct nonexistent_table *nonexistent_tables;
    struct bucket_storage_t *bsp;
    UT_hash_handle hh;
};

#define CAN_RETURN_NULL 1

struct bucket *bucket_get(const char *name, int flags);
struct table *bucket_get_table(struct bucket *bucket, int64_t id, int flags);
void bucket_set_table(struct bucket *bucket, int64_t id, struct table *table);
void bucket_drop(const char *name);
char *bucket_list();
void bucket_set_schema(struct bucket *bucket, struct schema *schema);

#endif
