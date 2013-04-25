#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include "table.h"
#include "storage/storage.h"
#include "slab.h"
#include "crc32.h"
#include "queue.h"
#include "rwlock.h"
#include "utils.h"
#include "bucket.h"
#include "stats.h"

static pthread_t table_writer_thread;

DECLARE_QUEUE(struct table *, 65536, table_writer_queue);

struct rwlock_t *io_rwlock = NULL;

extern int pending_commands;

void table_write_real(struct table *table) {
    pthread_rwlock_wrlock(&table->rwlock);
    table->writing = 0;
    pthread_rwlock_unlock(&table->rwlock);
    
    pthread_rwlock_rdlock(&table->rwlock);
    
    struct schema *schema = table->schema;
    
    size_t data_size = ceildiv(schema->nbits, 8) * table->len;
    
    size_t total_size = sizeof(struct table_data) + data_size;
    
    struct table_data *table_data = slab_alloc(total_size);
    if (!table_data) {
        log_error("table write can't alloc table_data");
        return;
    }

    memset(table_data, 0, sizeof(struct table_data));
    
    table_data->schema_hash = schema->hash;
    table_data->table_id = table->id;
    table_data->size = data_size;
    table_data->timestamp = time(0);
    
    memcpy(table_data->data, table->data, data_size);
    int64_t table_id = table->id;
    struct bucket *bucket = table->bucket;
    pthread_rwlock_unlock(&table->rwlock);
    
    table_data->crc32 = crc32_calc(table_data->data, data_size);
    
    /*
    if (table_data->schema_hash == 0) {
        log_error("bug!");
        exit(0);
    }
    */
    assert(table_data->schema_hash != 0);
    
    stat_add(NUM_DISK_WRITES, 1);
    rwlock_wrlock(io_rwlock);
    int res = update_block(bucket->bsp, &table_id, 8, table_data, total_size);
    rwlock_unwrlock(io_rwlock);
    
    if (res != 0) {
        log_error("update_block error %d", res);
    }
    
    slab_free(table_data);
}


int freeze = 0;
int freeze_done = 0;
int64_t last_write_nonce = 0;

void *table_writer_thread_func(void *arg) {
    (void) arg;
    for (;;) {
        if (freeze) {
            if (table_writer_queue_is_full(&table_writer_queue)) {
                log_warn("auto unfreeze because table_writer_queue is full.");
                freeze = 0;
            }
            else {
                freeze_done = 1;
                sleep(1);
                continue;
            }
        }
        freeze_done = 0;

        struct table *table = table_writer_queue_timedpoll(&table_writer_queue, 1000000);
        if (table) {
            last_write_nonce = randu64();
            table_write_real(table);
            __sync_add_and_fetch(&pending_commands, -1);
        }
    }
}


void table_writer_init() {
    io_rwlock = rwlock_new();
    if (!io_rwlock) {
        log_error("can't create io_rwlock");
        return;
    }

    table_writer_queue_init(&table_writer_queue);
    int res = pthread_create(&table_writer_thread, NULL, table_writer_thread_func, NULL);
    if (res != 0) {
        log_error("thread creation failed: %d", res);
    }
}

int table_find(struct table *table, int64_t pk) {
    /* XXX: refer to the XXX below */
    if ((!table) || (pk < 0))
        goto err;

    uint32_t l = 0, r = table->len;
    struct schema *schema = table->schema;
    struct field *pk_field = &schema->fields[schema->primary_key_index];
    while (l < r) {
        int m = (l + r) / 2;
        void *data = table->data + m * ceildiv(schema->nbits, 8);
            
        int64_t record_pk = field_data_get(pk_field, data);
        
        if (pk > record_pk)
            l = m + 1;
        else if (pk < record_pk)
            r = m;
        else
            return m;
    }
    /* XXX: why -1 indicates a fault, while it's a int64_t too */
err:
    return -1;
}

static struct table *table_create(struct bucket *bucket, int64_t id) {
    struct table *table = slab_alloc(sizeof(struct table));
    if (!table) {
        log_error("can't alloc table: %"PRId64, id);
        return NULL;
    }

    table->id = id;
    table->len = 0;
    table->bucket = bucket;
    table->schema = bucket->schema;
    table->cap = 0;
    table->data = NULL;
    table->writing = 0;
    pthread_rwlock_init(&table->rwlock, NULL);
    
    return table;
}

struct table *table_create_and_read(struct bucket *bucket, int64_t id, int flags) {
    struct table *table;
    struct table_data *table_data;
    size_t total_size;

    if (!bucket)
        return NULL;

    table = NULL;
    table_data = NULL;
    total_size = 0;

    if (!(flags & CAN_RETURN_NULL)) {
        table = table_create(bucket, id);
        if (!table) {
            log_error("can't create table: %"PRId64, id);
            return NULL;
        }
    }

    stat_add(NUM_DISK_READS, 1);

    rwlock_rdlock(io_rwlock);
    int res = find_block(bucket->bsp, &id, 8, (void **) &table_data, &total_size);
    rwlock_unrdlock(io_rwlock);

    if (res != 0)
        return table;

    /*
    if (bucket->schema && bucket->schema->hash == 0) {
        log_error("bug 1");
        exit(0);
    }
    */

    assert(!(bucket->schema && bucket->schema->hash == 0));

    if (table_data->table_id != id) {
        log_error("table_id doesn't match: %ld %ld", table_data->table_id, id);
        goto fail;
    }

    if (table_data->size + sizeof(struct table_data) != total_size) {
        log_error("size doesn't match: table_data->size = %d, sizeof(struct table_data) = %d, total_size = %d in %s[%"PRId64"]",
            (int) table_data->size, (int) sizeof(struct table_data), (int) total_size, bucket->name, id);
        goto fail;
    }
    
    if (table_data->crc32 != crc32_calc(table_data->data, table_data->size)) {
        log_error("crc32 doesn't match");
        goto fail;
    }
    
    struct schema *schema = schema_get_from_hash(table_data->schema_hash, bucket->name);
    
    /* XXX: why return table even schema is NULL */
    if (schema == NULL) {
        log_error("schema %"PRIx64" not found", table_data->schema_hash);
        goto fail;
    }

    if (table == NULL) {
        table = table_create(bucket, id);
        if (!table) {
            log_error("can't create table: %"PRId64, id);
            goto fail;
        }
    }
    
    table->schema = schema;
    table->len = table_data->size / ceildiv(schema->nbits, 8);
    table->cap = table->len;
    table->data = slab_alloc(table->cap * ceildiv(schema->nbits, 8));
    if (!table->data) {
        log_error("can't alloc table data: %"PRId64, id);
        goto fail;
    }
    
    memcpy(table->data, table_data->data, table_data->size);

fail:
    if (table_data) {
        slab_free(table_data);
        table_data = NULL;
    }
    return table;
}

void table_destroy(struct table *table) {
    pthread_rwlock_destroy(&table->rwlock);
    slab_free(table);
    log_notice("table %p destroyed", table);
}

int table_insert_nolock(struct table *table, struct schema *schema, void *data, int flags) {

    int ret = 0;
    void *back = NULL;

    size_t record_size = ceildiv(schema->nbits, 8);
    
    int64_t pk = field_data_get(&schema->fields[schema->primary_key_index], data);
    
    int pos = table_find(table, pk);
    if (pos != -1) {
        if (flags & TABLE_INSERT_FLAG_OVERWRITE) {
            if (memcmp(table->data + pos * record_size, data, record_size) != 0) {
                memcpy(table->data + pos * record_size, data, record_size);
                ret |= TABLE_INSERT_MODIFIED;
            }
        }
        return ret;
    }

    ret |= TABLE_INSERT_MODIFIED | TABLE_INSERT_LENGTH_INCREASED;
    
    if (table->len == table->cap) {
        if (table->cap > 0)
            table->cap *= 2;
        else
            table->cap = 2;

        back = table->data;
        table->data = slab_realloc(table->data, table->cap * record_size);
        if (!table->data) {
            log_error("can't realloc table data");
            table->data = back;
            return 0;
        }
    }
    
    size_t q = 0;
    while (q < table->len && schema_compare_record(table->schema, table->data + q * record_size, data) < 0)
        q++;
    memmove(table->data + (q + 1) * record_size,
            table->data + q * record_size,
            (table->len - q) * record_size);
    memcpy(table->data + q * record_size, data, record_size);
    table->len++;
    
    return ret;
}

void table_write(struct table *table) {
    int will_write = 1;
    pthread_rwlock_wrlock(&table->rwlock);
    if (table->writing)
        will_write = 0;
    else
        table->writing = 1;
    pthread_rwlock_unlock(&table->rwlock);

    if (will_write) {
        __sync_add_and_fetch(&pending_commands, 1);
        table_writer_queue_add(&table_writer_queue, table);
    }
}

static void table_upgrade_schema(struct table *table, struct schema *new_schema) {

    struct schema *old_schema = table->schema;
    log_notice("upgrade schema: %p (%"PRIx64") -> %p (%"PRIx64")", old_schema, old_schema->hash, new_schema, new_schema->hash);
    
    if (new_schema->hash == old_schema->hash) {
        log_notice("schema %p and %p have the same hash: %"PRIx64", skip upgrading.", old_schema, new_schema, new_schema->hash);
        table->schema = new_schema;
        return;
    }
    
    size_t oldrecsize = ceildiv(old_schema->nbits, 8);
    size_t newrecsize = ceildiv(new_schema->nbits, 8);
    size_t newsize = table->len * newrecsize;
    
    log_notice("convert table %s[%"PRId64"]: %zu to %zu", table->bucket->name, table->id, oldrecsize, newrecsize);
    char *buf = slab_alloc(newsize);
    if (!buf) {
        log_error("can't alloc table data with new schema");
        return;
    }

    memset(buf, 0, newsize);
    
    char *oldp = table->data, *newp = buf;
    
    for (uint32_t i = 0; i < table->len; i++) {
        for (size_t j = 0; j < new_schema->nfields; j++) {
            // struct field *old_field = ;
            struct field *new_field = &new_schema->fields[j];
            if (new_field->name[0]) {
                struct field *old_field = schema_field_get(old_schema, new_field->name);
                if (old_field) {
                    int64_t data = field_data_get(old_field, oldp);
                    field_data_set(new_field, newp, data);
                }
            }
        }
        oldp += oldrecsize;
        newp += newrecsize;
    }
    
    slab_free(table->data);
    table->cap = table->len;
    table->data = buf;
    table->schema = new_schema;
}

void table_check_schema(struct table *table) {
    pthread_rwlock_wrlock(&table->rwlock);
    pthread_rwlock_rdlock(&table->bucket->rwlock);

    if (table->schema != table->bucket->schema) {
        if (table->schema == NULL)
            table->schema = table->bucket->schema;
        else {
            table_upgrade_schema(table, table->bucket->schema);
        }
    }

    pthread_rwlock_unlock(&table->bucket->rwlock);
    pthread_rwlock_unlock(&table->rwlock);
}
