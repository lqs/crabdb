/* © Copyright 2011 jingmi. All Rights Reserved.
 *
 * +----------------------------------------------------------------------+
 * | implement bdb transaction functions                                  |
 * +----------------------------------------------------------------------+
 * | Author: mi.jing@jiepang.com                                          |
 * +----------------------------------------------------------------------+
 * | Created: 2011-11-02 16:13                                            |
 * +----------------------------------------------------------------------+
 */

#include "bdblib.h"
#include "log.h"
#include "slab.h"

#define BUILD_DB_PREFIX char log_prefix[BUFSIZ];        \
do {                                    \
    char lineno[10] = {0};              \
    snprintf(lineno, sizeof lineno, "%d", __LINE__);    \
    log_prefix[0] = '[';                \
    get_time_string(&log_prefix[1]);    \
    strcat(log_prefix, "] [BDB] ");     \
    strcat(log_prefix, "(");            \
    strcat(log_prefix, __FILE__);       \
    strcat(log_prefix, ":");            \
    strcat(log_prefix, lineno);         \
    strcat(log_prefix, ")");            \
    log_prefix[BUFSIZ-1] = '\0';        \
    bdbp->dbp->set_errpfx(bdbp->dbp, log_prefix);       \
} while (0)

#define REPORT_ERR(fmt, ...) do {       \
    log_warn(fmt, ##__VA_ARGS__);       \
    bdbp->error_no = ret;               \
    goto err;                           \
} while (0)

#define DEFAULT_PAGE_SIZE (1024 * 4)

int
insert_record(struct bdb_t *bdbp, void *key_ptr, size_t key_len, void *value_ptr, size_t value_len)
{
    int ret;
    DBT key_ref, value_ref;

    bzero(&key_ref, sizeof key_ref);
    bzero(&value_ref, sizeof value_ref);
    bdbp->error_no = 0;

    key_ref.data = key_ptr;
    key_ref.size = key_len;
    value_ref.data = value_ptr;
    value_ref.size = value_len;

    BUILD_DB_PREFIX;
    if ((ret = bdbp->dbp->put(bdbp->dbp, NULL, &key_ref, &value_ref, 0)) != 0)
    {
        REPORT_ERR("bdb insert failed: %d - %s", ret, db_strerror(ret));
    }

err:
    return ret;
}

#define FIND_RECORD_ALLOC_DBT_PTR (0x1)
#define FIND_RECORD_ALLOC_VAL_PTR (0x2)

int
find_record(struct bdb_t *bdbp, DBT **dbt_ptr, void *key_ptr, size_t key_len, size_t *value_len)
{
    assert(dbt_ptr != NULL);
    assert(key_ptr != NULL);
    assert(value_len != NULL);

    void *value_ptr = NULL;
    int ret;
    DBT query_cond;
    int alloc_flag;

    alloc_flag = 0;

    if (*dbt_ptr == NULL)
    {
        if ((*dbt_ptr = slab_alloc(sizeof **dbt_ptr)) == NULL)
        {
            ret = -1;
            REPORT_ERR("memory is not enough");
        }
        alloc_flag = alloc_flag | FIND_RECORD_ALLOC_DBT_PTR;
    }
    if ((value_ptr = slab_alloc(*value_len)) == NULL)
    {
        ret = -1;
        REPORT_ERR("memory is not enough");
    }
    alloc_flag = alloc_flag | FIND_RECORD_ALLOC_VAL_PTR;

    memset(&query_cond, 0, sizeof query_cond);
    memset(*dbt_ptr, 0, sizeof **dbt_ptr);
    memset(value_ptr, 0, *value_len);

    query_cond.data = key_ptr;
    query_cond.size = key_len;
    (*dbt_ptr)->data = value_ptr;
    (*dbt_ptr)->ulen = *value_len;
    (*dbt_ptr)->flags = DB_DBT_USERMEM;

    BUILD_DB_PREFIX;
    if ((ret = bdbp->dbp->get(bdbp->dbp, NULL, &query_cond, *dbt_ptr, 0)) != 0)
    {
        if (ret != DB_NOTFOUND)
            REPORT_ERR("bdb query failed: %s", db_strerror(ret));
    }

err:
    if (ret != 0)
    {
        if ((alloc_flag & FIND_RECORD_ALLOC_DBT_PTR) && (*dbt_ptr != NULL))
        {
            slab_free(*dbt_ptr);
            *dbt_ptr = NULL;
        }
        if ((alloc_flag & FIND_RECORD_ALLOC_VAL_PTR) && (value_ptr != NULL))
        {
            slab_free(value_ptr);
            if (*dbt_ptr != NULL)
                (*dbt_ptr)->data = NULL;
        }
    }
    return ret;
}

int
del_record(struct bdb_t *bdbp, void *key, size_t key_len)
{
    DBT dbt;

    memset(&dbt, 0, sizeof dbt);
    dbt.data = key;
    dbt.size = key_len;

    return bdbp->dbp->del(bdbp->dbp, NULL, &dbt, 0);
}

int
list_keys(struct bdb_t *bdbp, void *start_key, size_t key_len, void **key_ptr, size_t *key_count)
{
    (void) start_key;
    (void) key_len;

    DBC *cursor;
    int ret;
    if ((ret = bdbp->dbp->cursor(bdbp->dbp, NULL, &cursor, DB_CURSOR_BULK)) != 0)
        return ret;
    
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));
    key.flags = DB_DBT_MALLOC;
    key.data = start_key;
    if (start_key)
        key.size = key_len;

    // printf("start_key, key_len = %p %zu\n", start_key, key_len);

    value.flags = DB_DBT_REALLOC;

    size_t count = 0;

    
    for (ret = cursor->get(cursor, &key, &value, DB_SET_RANGE); ret != DB_NOTFOUND; ret = cursor->get(cursor, &key, &value, DB_NEXT)) {

        if (ret != 0) {
            for (size_t i = 0; i < count; i++)
                free(key_ptr[i]);
            goto err;
        }

        if (count >= *key_count) {
            free(key.data);
            break;
        }

        if (key.size == key_len) {
            if (start_key && memcmp(start_key, key.data, key_len) == 0) {
                free(key.data);
                continue;
            }
            key_ptr[count++] = key.data;
        }
        else {
            REPORT_ERR("invalid key size %"PRIu32", should be %zu", key.size, key_len);
        }
    }
    ret = 0;

    *key_count = count;

err:
    cursor->close(cursor);

    free(value.data);

    return ret;
}

struct bdb_t *
open_transaction_bdb(const char *db_dir, const char *db_filename, size_t cache_size, size_t page_size, FILE *log_fp)
{
    struct bdb_t *bdbp;
    int ret;
    u_int32_t db_flags, env_flags;
    u_int32_t gbytes, bytes;
    DB *dbp;
    DB_ENV *envp;

    if ((bdbp = calloc(1, sizeof(struct bdb_t))) == NULL)
    {
        goto err;
    }

    dbp = NULL;
    envp = NULL;

    // env_flags = DB_CREATE | DB_INIT_TXN | DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL;
    env_flags = DB_CREATE |  DB_INIT_LOG | DB_INIT_MPOOL;
    if ((ret = db_env_create(&envp, 0)) != 0)
    {
        bdbp->error_no = ret;
        goto err;
    }
    bdbp->envp = envp;

    if (cache_size != 0)
    {
        if ((ret = bdbp->envp->set_cache_max(bdbp->envp, 1, 0))
                != 0)
        {
            bdbp->error_no = ret;
            goto err;
        }
    }

    if ((ret = envp->open(envp, db_dir, env_flags, 0)) != 0)
    {
        bdbp->error_no = ret;
        goto err;
    }

    if (cache_size != 0)
    {
        gbytes = cache_size / (1024 * 1024 * 1024);
        bytes  = cache_size % (1024 * 1024 * 1024);
        if ((ret = bdbp->envp->set_cachesize(bdbp->envp, gbytes, bytes, 0))
                != 0)
        {
            bdbp->error_no = ret;
            goto err;
        }
    }

    // db_flags = DB_CREATE | DB_AUTO_COMMIT;
    db_flags = DB_CREATE;
    if ((ret = db_create(&dbp, envp, 0)) != 0)
    {
        bdbp->error_no = ret;
        goto err;
    }
    bdbp->dbp = dbp;
    if (log_fp == NULL)
    {
        if (log_fd != -1) {
            FILE *fp = fdopen(log_fd, "a+");
            bdbp->dbp->set_errfile(bdbp->dbp, fp);
        }
    }
    else
    {
        bdbp->dbp->set_errfile(bdbp->dbp, log_fp);
    }

    if (page_size == 0)
        page_size = DEFAULT_PAGE_SIZE;
    if ((ret = bdbp->dbp->set_pagesize(bdbp->dbp, page_size)) != 0)
    {
        bdbp->error_no = ret;
        goto err;
    }

    if ((ret = dbp->open(dbp, NULL, db_filename, NULL, DB_BTREE, db_flags, 0)) != 0)
    {
        bdbp->error_no = ret;
        goto err;
    }

err:
    if (bdbp == NULL)
        return NULL;

    if (ret != 0)
    {
        if (bdbp->dbp != NULL)
        {
            ret = bdbp->dbp->close(bdbp->dbp, 0);
            assert(ret == 0);
        }
        if (bdbp->envp != NULL)
        {
            ret = bdbp->envp->close(bdbp->envp, 0);
            assert(ret == 0);
        }
        free(bdbp);
        bdbp = NULL;
    }

    return bdbp;
}

int
close_bdb(struct bdb_t *bdbp)
{
    int ret;

    if ((ret=bdbp->dbp->close(bdbp->dbp, 0)) != 0)
        return ret;
    if ((ret=bdbp->envp->close(bdbp->envp, 0)) != 0)
        return ret;

    free(bdbp);

    return 0;
}

/* vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker: */
