#ifndef _BDBLIB_H_
#define _BDBLIB_H_
/* © Copyright 2011 jingmi. All Rights Reserved.
 *
 * +----------------------------------------------------------------------+
 * | encapsulated bdb transaction functions                               |
 * +----------------------------------------------------------------------+
 * | Author: mi.jing@jiepang.com                                          |
 * +----------------------------------------------------------------------+
 * | Created: 2011-11-02 16:14                                            |
 * +----------------------------------------------------------------------+
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <db.h>

typedef struct bdb_t
{
    DB *dbp;
    DB_ENV *envp;
    int error_no;
} bdb_t;

extern struct bdb_t *open_transaction_bdb(const char *db_dir, const char *db_filename, size_t cache_size, size_t page_size, FILE *log_fp);
extern int insert_record(struct bdb_t *bdbp, void *key_ptr, size_t key_len, void *value_ptr, size_t value_len);
extern int find_record(struct bdb_t *bdbp, DBT **dbt_ptr, void *key_ptr, size_t key_len, size_t *value_len);
extern int del_record(struct bdb_t *bdbp, void *key, size_t key_len);
extern int list_keys(struct bdb_t *bdbp, void *start_key, size_t key_len, void **key_ptr, size_t *key_count);
extern int close_bdb(struct bdb_t *bdbp);

#endif /* ! _BDBLIB_H_ */
/* vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker: */
