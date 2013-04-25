#include <string.h>
#include <stdio.h>
#include <pthread.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <dirent.h>
#include "uthash.h"
#include "storage/storage.h"
#include "table.h"
#include "bucket.h"
#include "schema.h"
#include "utils.h"
#include "slab.h"

static pthread_rwlock_t rwlock = PTHREAD_RWLOCK_INITIALIZER;

static struct bucket *buckets = NULL;

/*
 * get a bucket with the specified name,
 * if it does not exits, create it implicitly,
 * returns a pointer to the bucket on success,
 * returns NULL if it fails
 */
struct bucket *bucket_get(const char *name, int flags) {
    if (!is_valid_name(name)) {
        log_warn("invalid bucket name: [%s]", name);
        return NULL;
    }
    
    struct bucket *bucket;
    pthread_rwlock_rdlock(&rwlock);
    int wrlocked = 0;
again:
    HASH_FIND_STR(buckets, name, bucket);
    if (bucket == NULL) {
        if (wrlocked == 0) {
            pthread_rwlock_unlock(&rwlock);
            pthread_rwlock_wrlock(&rwlock);
            wrlocked = 1;
            goto again;
        }
        
        // find schema before malloc
        struct schema *schema = schema_get_latest(name);
        if (schema == NULL && (flags & CAN_RETURN_NULL))
            goto done;
        
        if (mkdir(name, 0755) == -1) {
            log_error("mkdir failed: %s", strerror(errno));
            if (errno != EEXIST)
                goto done;
        }

        bucket = (struct bucket *)slab_alloc(sizeof(struct bucket));
        /* XXX: free the schema that no one can be sure if it's referenced by others */
        if (!bucket) {
            log_error("can't alloc bucket %s", name);
            goto done;
        }

        memset(bucket, 0, sizeof(struct bucket));
        pthread_rwlock_init(&bucket->rwlock, NULL);

        size_t namelen = strlen(name);
        char filename[namelen + 4];
        memcpy(filename, name, namelen);
        memcpy(filename + namelen, ".db", 4);

        if (open_storage(filename, 100 * 1024 * 1024, &bucket->bsp) != 0) {
            log_error("open_storage failed");
            pthread_rwlock_destroy(&bucket->rwlock);
            slab_free(bucket);
            pthread_rwlock_unlock(&rwlock);
            return NULL;
        }

        strncpy(bucket->name, name, sizeof(bucket->name));
        bucket->schema = schema;
        bucket->tables = NULL;
        HASH_ADD_STR(buckets, name, bucket);

        if (schema)
            schema_save_to_file(schema, bucket->name);
    }
done:
    pthread_rwlock_unlock(&rwlock);
    
    return bucket;
}

/*
 * drop a bucket with a specified name,
 * no return value, check log when you find something wrong
 */
void bucket_drop(const char *name) {

    if (!is_valid_name(name))
        return;

    struct bucket *bucket;
    pthread_rwlock_wrlock(&rwlock);
    HASH_FIND_STR(buckets, name, bucket);
    if (bucket) {
        /* TODO: find and delete all data on hard disk belongs to this bucket */
        /* TODO: find and delete all request in write queue belongs to this bucket */
        while (bucket->tables) {
            HASH_DEL(bucket->tables, bucket->tables);
        }
        HASH_DEL(buckets, bucket);
    }
    // close_storage(bucket->bsp);
    char link_from[1024];

    snprintf(link_from, sizeof(link_from), "%s.schema", name);
    if (unlink(link_from) != 0)
        log_error("unlink %s failed: %s", link_from, strerror(errno));

    snprintf(link_from, sizeof(link_from), "%s/schema", name);
    if (unlink(link_from) != 0)
        log_error("unlink %s failed: %s", link_from, strerror(errno));

    pthread_rwlock_unlock(&rwlock);
}

/*
 * returns a string containing all bucket name
 * returns NULL if it fails
 */
char *bucket_list(void) {
    /* load buckets from filesystem */
    int size;
    int pos = 0;
    char *result;
    struct bucket *bucket;
    int len;

    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *dirent;
        while ((dirent = readdir(dir))) {
            if ((dirent->d_type & DT_DIR) && is_valid_name(dirent->d_name))
                bucket_get(dirent->d_name, CAN_RETURN_NULL);
        }
        closedir(dir);
    }


    pthread_rwlock_rdlock(&rwlock);
    
    size = len = 0;
    for (bucket = buckets; bucket; bucket = (struct bucket *)bucket->hh.next) {
        len = strlen(bucket->name) + 1;
        size += len;
    }

    result = slab_alloc(size);
    if (!result)
        return NULL;

    size = len = 0;
    for (bucket = buckets; bucket; bucket = (struct bucket *)bucket->hh.next) {
        len = strlen(bucket->name) + 1;
        size += len;
        memcpy(result + pos, bucket->name, len);
        pos = size;
    }

    pthread_rwlock_unlock(&rwlock);
    result[pos] = '\0';
    return result;
}

/*
 * returns a pointer to a table, or NULL if fails
 */
struct table *bucket_get_table(struct bucket *bucket, int64_t id, int flags) {
    struct table *table;
    int wrlocked;

    if (bucket == NULL)
        return NULL;
        
    wrlocked = 0;
    pthread_rwlock_rdlock(&bucket->rwlock);

again:
    HASH_FIND_INT64(bucket->tables, &id, table);
    
    if (table == NULL) {
        if (wrlocked == 0) {
            pthread_rwlock_unlock(&bucket->rwlock);
            pthread_rwlock_wrlock(&bucket->rwlock);
            wrlocked = 1;
            goto again;
        }
        table = table_create_and_read(bucket, id, flags);
        if (table)
            HASH_ADD_INT64(bucket->tables, id, table);
    }

    pthread_rwlock_unlock(&bucket->rwlock);
    return table;
}

/*
 * set a bucket's schema
 * no return value
 * check log when you find something wrong
 */
void bucket_set_schema(struct bucket *bucket, struct schema *schema) {
    pthread_rwlock_wrlock(&bucket->rwlock);
    if (schema) {
        char link_from[1024], link_to[1024], link_from_temp[1024];

        snprintf(link_from_temp, sizeof(link_from_temp), "%s/schema.tmp.%"PRIx64, bucket->name, randu64());
        snprintf(link_from, sizeof(link_from), "%s/schema", bucket->name);
        snprintf(link_to, sizeof(link_to), "schemas/%016"PRIx64, schema->hash);

        log_warn("should symlink: %s %s", link_from_temp, link_to);

        int res = unlink(link_from_temp);
        if (res != 0 && errno != ENOENT) {
            log_error("unlink failed %d", res);
            goto fail;
        }

        res = symlink(link_to, link_from_temp);
        if (res != 0) {
            log_error("symlink failed");
            goto fail;
        }

        res = rename(link_from_temp, link_from);
        if (res != 0) {
            log_error("rename failed");
            goto fail;
        }

    }
    bucket->schema = schema;
fail:
    pthread_rwlock_unlock(&bucket->rwlock);
}
