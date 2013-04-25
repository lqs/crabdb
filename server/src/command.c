#include <msgpack.h>
#include <sys/mman.h>
#include "schema.h"
#include "crabql.h"
#include "bucket.h"
#include "utils.h"
#include "table.h"
#include "log.h"
#include "utils.h"
#include "server.h"
#include "binlog.h"
#include "table.h"
#include "schema.h"
#include "command_multifind.h"
#include "stats.h"
#include "bdblib.h"
#include "storage.h"
#include "error.h"
#include "slab.h"

typedef int (*command_func_t) (struct request *request, msgpack_object o, msgpack_packer* response);

int check_and_return_error(struct eval_context *context, msgpack_packer* response) {
    int res = context->error;
    if (res) {
        int len = strlen(context->error_message);
        msgpack_pack_raw(response, len);
        msgpack_pack_raw_body(response, context->error_message, len);
    }
    return res;
}

int command_eval(struct request *request, msgpack_object o, msgpack_packer* response) {
    (void) request;
    struct eval_context context;
    eval_context_init(&context);
    struct crabql crabql;
    crabql_init(&crabql, &o, NULL, DATA_MODE_NONE);
    int64_t val = crabql_exec(&crabql, &context);
    crabql_finish(&crabql);
    for (int res = check_and_return_error(&context, response); res;)
        return res;
    
    msgpack_pack_long(response, val);
    
    return context.error;
};

struct schema *schema_from_input(msgpack_object *fields_o) {

    if (fields_o->type != MSGPACK_OBJECT_ARRAY)
        return NULL;

    struct schema *schema = schema_create();
    if (!schema)
        return NULL;

    msgpack_object* p;
    for (p = fields_o->via.array.ptr; p < fields_o->via.array.ptr + fields_o->via.array.size; p++) {
        
        char name[64] = {0};
        int nbits = 0;
        bool is_signed = 0;
        bool is_primary_key = 0;
        
        if (p->type == MSGPACK_OBJECT_MAP) {
            msgpack_object_kv *kv;
            for (kv = p->via.map.ptr; kv < p->via.map.ptr + p->via.map.size; kv++) {
                if (mp_raw_eq(kv->key, "name"))
                    mp_raw_strcpy(name, kv->val, sizeof(name));
                else if (mp_raw_eq(kv->key, "nbits"))
                    nbits = kv->val.via.u64;
                else if (mp_raw_eq(kv->key, "is_signed") || mp_raw_eq(kv->key, "signed"))
                    is_signed = kv->val.via.boolean;
                else if (mp_raw_eq(kv->key, "is_primary_key") || mp_raw_eq(kv->key, "primary_key"))
                    is_primary_key = kv->val.via.boolean;
            }
        }
        
        int ret = schema_field_add(schema, name, is_signed, nbits, is_primary_key);
        
        if (ret) {
            slab_free(schema);
            return NULL;
        }
    }
    
    if (schema->primary_key_index == -1) {
        slab_free(schema);
        return NULL;
    }
    
    schema_calc_hash(schema);
    return schema;
}

int command_create_bucket(struct request *request, msgpack_object o, msgpack_packer* response) {
    (void) response;
    
    if (request && server->is_slave && binlog->position != 0)
        return CRABDB_ERROR_NOT_MASTER;

    int ret = 0;
    
    char bucket_name[64] = {0};
    msgpack_object *fields_o = NULL;
    
    if (o.type != MSGPACK_OBJECT_MAP) {
        ret = CRABDB_ERROR_INVALID_ARGUMENT;
        goto err;
    }

    mp_for_each_in_kv(p, o) {
        if (mp_raw_eq(p->key, "bucket"))
            mp_raw_strcpy(bucket_name, p->val, sizeof(bucket_name));
        else if (mp_raw_eq(p->key, "fields"))
            fields_o = &p->val;
    }
    
    if (fields_o == NULL) {
        ret = CRABDB_ERROR_INVALID_ARGUMENT;
        goto err;
    }

    struct schema *schema = schema_from_input(fields_o);

    if (schema == NULL) {
        ret = CRABDB_ERROR_INVALID_ARGUMENT;
        goto err;
    }

    struct bucket *bucket = bucket_get(bucket_name, 0);
    if (!bucket) {
        log_error("can't create bucket %s", bucket_name);
        /* TODO: ret = CRABDB_ERROR_CREATE_BUCKET; */
        goto err;
    }

    schema_save_to_file(schema, bucket_name);

    if (!schema_is_equal(schema, bucket->schema)) {
        if (!schema_can_upgrade(bucket->schema, schema)) {
            ret = CRABDB_ERROR_INVALID_ARGUMENT;
            goto err;
        }
        binlog_save_request(request);
        bucket_set_schema(bucket, schema);
    }
    
err:
    return ret;
}

static int compare(int64_t *cmpcache, size_t nsortrules, size_t idx1, size_t idx2) {
    int res = 0;
    for (size_t i = 0; i < nsortrules; i++) {
        int64_t val1 = cmpcache[idx1 * nsortrules + i];
        int64_t val2 = cmpcache[idx2 * nsortrules + i];
        if (val1 < val2) {
            res = -1;
            break;
        }
        if (val1 > val2) {
            res = 1;
            break;
        }
    }
    return res;
}

static void heap_down(uint32_t *heap, uint32_t *heap_inverse, size_t heaplen, size_t cur, int64_t *cmpcache, size_t nsortrules) {
    for (;;) {
        size_t lc = cur * 2 + 1;
        size_t rc = lc + 1;
        
        size_t maxone = cur;
        if (lc < heaplen && compare(cmpcache, nsortrules, heap[lc], heap[cur]) > 0)
            maxone = lc;
        if (rc < heaplen && compare(cmpcache, nsortrules, heap[rc], heap[maxone]) > 0)
            maxone = rc;
        if (maxone == cur)
            break;
        
        swap(&heap_inverse[heap[cur]], &heap_inverse[heap[maxone]]);
        swap(&heap[cur], &heap[maxone]);
        cur = maxone;
    }
}

static void heap_up(uint32_t *heap, uint32_t *heap_inverse, size_t cur, int64_t *cmpcache, size_t nsortrules) {
    while (cur > 0) {
        size_t par = (cur - 1) / 2;
        
        if (compare(cmpcache, nsortrules, heap[cur], heap[par]) <= 0)
            break;
        
        swap(&heap_inverse[heap[cur]], &heap_inverse[heap[par]]);
        swap(&heap[cur], &heap[par]);
        cur = par;
    }
}

struct group {
    uint64_t value;
    uint32_t count;
    uint32_t index;
    UT_hash_handle hh;
};

int command_query(struct request *request, msgpack_object o, msgpack_packer* response) {
    (void) request;
    struct eval_context context;
    eval_context_init(&context);
    
    char bucket_name[64] = {0};
    int64_t table_id = 0;
    msgpack_object *query_o = NULL;
    msgpack_object *filter_o = NULL;
    msgpack_object *update_o = NULL;
    msgpack_object *sort_o = NULL;
    msgpack_object *group_o = NULL;
    
    struct crabql query;
    struct crabql filter;
    struct crabql update;
    struct crabql group;
    
    int64_t pk = 0;
    int8_t has_pk = 0;
    
    size_t num_items = 0;
    size_t offset = 0;
    uint64_t limit = -1;
    
    struct crabql *sort_rules = NULL;
    size_t nsortrules = 0;

    int8_t has_more = 0;

    bool modified = false;
    
    if (o.type == MSGPACK_OBJECT_MAP) {
        mp_for_each_in_kv(p, o) {
            if (mp_raw_eq(p->key, "bucket"))
                mp_raw_strcpy(bucket_name, p->val, sizeof(bucket_name));
            else if (mp_raw_eq(p->key, "table"))
                table_id = p->val.via.i64;
            else if (mp_raw_eq(p->key, "offset"))
                offset = p->val.via.i64;
            else if (mp_raw_eq(p->key, "limit"))
                limit = p->val.via.i64;
            else if (mp_raw_eq(p->key, "query"))
                query_o = &p->val;
            else if (mp_raw_eq(p->key, "filter"))
                filter_o = &p->val;
            else if (mp_raw_eq(p->key, "pk")) {
                pk = p->val.via.i64;
                has_pk = 1;
                limit = 1;
            }
            else if (mp_raw_eq(p->key, "sort")) {
                sort_o = &p->val;
                // ['key1', 'key2', '-key3']   // type 1
                // '-key1'
                // ['$add', 'key1', 'key2']    // type 2
            }
            else if (mp_raw_eq(p->key, "group")) {
                // 'key'
                group_o = &p->val;
            }
            else if (mp_raw_eq(p->key, "update")) {
                update_o = &p->val;
            }
            // else if (mp_raw_eq(p->key, "remove")) {
                // update_o = &p->val;
            // }
        }
        
        int flags = 0;
        if (update_o == NULL) {
            flags = 1;
        }
        
        // initialize the heap for sorting
        uint32_t *heap = NULL;
        uint32_t *heap_inverse = NULL;
        uint32_t heaplen = 0;
        int64_t *cmpcache = NULL;
        int64_t *groupcache = NULL;
        struct group *groups = NULL;
        
        struct bucket *bucket = NULL;
        struct table *table = NULL;
        struct schema *schema = NULL;
        size_t recordsize = 0;
        
        bucket = bucket_get(bucket_name, flags);
        if (bucket == NULL)
            goto done;
            
        table = bucket_get_table(bucket, table_id, flags);
        /*XXX: what about the bucket above */
        if (table == NULL)
            goto done;
            
        table_check_schema(table);
        
        if (update_o)
            pthread_rwlock_wrlock(&table->rwlock);
        else
            pthread_rwlock_rdlock(&table->rwlock);
            
        uint32_t tablelen = table->len;
        
        schema = table->schema;
        if (!schema)
            goto done;
        
        if (sort_o) {
            if (sort_o->type != MSGPACK_OBJECT_ARRAY || (sort_o->type == MSGPACK_OBJECT_ARRAY &&
                                                       sort_o->via.array.size > 0 &&
                                                       sort_o->via.array.ptr[0].via.raw.size > 0 &&
                                                       sort_o->via.array.ptr[0].via.raw.ptr[0] == '$')) {
                // single expression
                
                nsortrules = 1;
                sort_rules = (struct crabql *) slab_alloc(sizeof(sort_rules[0]));
                if (!sort_rules) {
                    log_error("can't alloc sort_rules");
                    goto done;
                }

                crabql_init(sort_rules, sort_o, schema, DATA_MODE_READ);
            }
            else {
                // multiple expression
                
                
                nsortrules = sort_o->via.array.size;
                sort_rules = (struct crabql *) slab_alloc(nsortrules * sizeof(sort_rules[0]));
                if (!sort_rules) {
                    log_error("can't alloc sort_rules");
                    goto done;
                }

                for (size_t i = 0; i < nsortrules; i++) {
                    crabql_init(&sort_rules[i], &sort_o->via.array.ptr[i], schema, DATA_MODE_READ);
                }
            }
        }

        

        

        if (limit != (uint64_t) -1)
            limit += offset;
        
        if (limit > tablelen)
            limit = tablelen;
        
        recordsize = ceildiv(schema->nbits, 8);

        
        if (tablelen == 0) {
            goto done;
        }
        
        heap_inverse = (uint32_t *) slab_alloc(table->len * sizeof(uint32_t));
        if (!heap_inverse) {
            log_error("can't alloc heap_inverse");
            goto done;
        }

        memset(heap_inverse, -1, table->len * sizeof(uint32_t));

        if (has_pk) {
            int pos = table_find(table, pk);
            if (pos >= 0) { // found
                heap = slab_alloc(4);
                heap[0] = pos;
                heap_inverse[pos] = 0;
                heaplen = 1;
            }
            goto done;
        }
        
        crabql_init(&query, query_o, schema, DATA_MODE_READ);
        crabql_init(&filter, filter_o, schema, DATA_MODE_READ);
        crabql_init(&update, update_o, schema, DATA_MODE_WRITE);
        crabql_init(&group, group_o, schema, DATA_MODE_READ);
        
        heap = (uint32_t *) slab_alloc((limit + 1) * sizeof(uint32_t));
        if (!heap) {
            log_error("can't alloc heap");
            goto done;
        }

        if (sort_rules) {
            cmpcache = slab_alloc(sizeof(cmpcache[0]) * nsortrules * tablelen);
            if (!cmpcache) {
                log_error("can't alloc cmpcache");
                goto done;
            }
        }
        
        if (group_o) {
            groupcache = slab_alloc(sizeof(groupcache[0]) * tablelen);
            if (!groupcache) {
                log_error("can't alloc groupcache");
                goto done;
            }
        }
        
        crabql_func_t query_func = crabql_get_func(&query);
        
        struct timeval tv1, tv2;
        gettimeofday(&tv1, NULL);
        
        context.table = table_id;
        
        stat_add(NUM_SCANS, tablelen);
        
        for (uint32_t i = 0; i < tablelen; i++) {
            void *data = table->data + i * recordsize;
            context.data = data;
            int64_t val = 1;
            
            // printf("dump: ");
            // for (uint32_t j = 0; j < recordsize; j++) {
                // uint8_t x = * (uint8_t *) (data + j);
                // for (int k = 0; k < 8; k++) {
                    // printf("%d", (int) x % 2);
                    // x /= 2;
                // }
                // putchar(' ');
            // }
            // putchar(10);
            
            if (query_o && query_func) {
                val = query_func(&context);
            }
            if (val) {
            
                // do update...
                if (update_o) {
                    crabql_exec(&update, &context);
                    if (context.modified) {
                        binlog_save_data(table, data);
                        modified = true;
                        context.modified = false;
                    }
                }
                
                num_items++;
                if (filter_o) {
                    val = crabql_exec(&filter, &context);
                }
            }
            if (val) {
            

                if (sort_rules) {
                    for (uint32_t j = 0; j < nsortrules; j++) {
                        cmpcache[i * nsortrules + j] = crabql_exec(&sort_rules[j], &context);
                    }
                }
                
                
                if (group_o) {
                    int64_t group_val = crabql_exec(&group, &context);
                    struct group *grp;
                    HASH_FIND_INT64(groups, &group_val, grp);

                    if (grp == NULL) {
                        grp = (struct group *) slab_alloc(sizeof(struct group));
                        if (!grp) {
                            log_error("can't alloc group");
                            goto done;
                        }
                        grp->value = group_val;
                        grp->count = 0;
                        grp->index = i;
                        HASH_ADD_INT64(groups, value, grp);
                    }
                    else {
                        num_items--;
                    }
                    grp->count++;
                    
                    groupcache[i] = group_val;
                    
                    bool add = 1;

                    uint32_t last_group_pos = heap_inverse[grp->index];

                    /*
                        heap里是带返回的记录。如果某一个的group值和现在的一样，则保留comprare后较小的。
                    */

                    if (last_group_pos != (uint32_t) -1) {
                        int cmpres = compare(cmpcache, nsortrules, i, heap[last_group_pos]);
                        
                        if (cmpres < 0) {
                            heap_inverse[heap[last_group_pos]] = -1;
                            heap[last_group_pos] = i;
                            heap_inverse[i] = last_group_pos;
                            heap_down(heap, heap_inverse, heaplen, last_group_pos, cmpcache, nsortrules);
                            grp->index = i;
                        }
                        add = 0;
                    }

                    if (add) {
                        grp->index = i;
                        heap[heaplen] = i;
                        heap_inverse[i] = heaplen;
                        heaplen++;
                    }
                }
                else if (limit > 0) {
                    heap[heaplen] = i;
                    heap_inverse[i] = heaplen;
                    heaplen++;
                }
                else {
                    has_more = 1;
                }
                
                if (sort_rules) {
                    /*
                        需要维护heap性质
                    */
                    if (heaplen > 0)
                        heap_up(heap, heap_inverse, heaplen - 1, cmpcache, nsortrules);
                    
                    if (heaplen > limit) {
                        heaplen--;
                        heap_inverse[heap[0]] = -1;
                        heap[0] = heap[heaplen];
                        heap_inverse[heap[heaplen]] = 0;
                        heap_down(heap, heap_inverse, heaplen, 0, cmpcache, nsortrules);
                        has_more = 1;
                    }
                }
                else {
                    if (heaplen > limit) {
                        heaplen--;
                        heap_inverse[heap[heaplen]] = -1;
                        has_more = 1;
                    }
                }
            }
        }
        
        gettimeofday(&tv2, NULL);
        
        // log_notice("iter %"PRIu32" records in %.6lf seconds", tablelen, (double) ((tv2.tv_sec + tv2.tv_usec / 1000000.0) - (tv1.tv_sec + tv1.tv_usec / 1000000.0)));
        
        
        crabql_finish(&query);
        crabql_finish(&filter);
        crabql_finish(&update);
        crabql_finish(&group);
        
        if (nsortrules > 0) {
            // printf("heaplen = %d\n", (int) heaplen);
            
            
            // printf("final:\n");
            // for (int i=0; i<heaplen; i++) printf("%d ", (int) heap[i]);putchar(10);
                        
            for (size_t i = 1; i < heaplen; i++) {
                swap(&heap_inverse[heap[0]], &heap_inverse[heap[heaplen - i]]);
                swap(&heap[0], &heap[heaplen - i]);
                heap_down(heap, heap_inverse, heaplen - i, 0, cmpcache, nsortrules);
            }
            // for (int i=0; i<heaplen; i++) printf("%d ", (int) heap[i]);putchar(10);
        }
        
        for (size_t i = 0; i < nsortrules; i++)
            crabql_finish(&sort_rules[i]);
        
        
done:
        if (sort_rules)
            slab_free(sort_rules);
        if (cmpcache)
            slab_free(cmpcache);
        
        for (int res = check_and_return_error(&context, response); res;) {
            if (table) {
                pthread_rwlock_unlock(&table->rwlock);
            }
            goto cleanup;
        }
        
        msgpack_pack_map(response, 3);
        mp_pack_string(response, "num_items");
        msgpack_pack_long(response, num_items);
        mp_pack_string(response, "has_more");
        mp_pack_bool(response, has_more);
        mp_pack_string(response, "items");
        
        if (heaplen > offset) {
            msgpack_pack_array(response, heaplen - offset);
            
            for (size_t i = offset; i < heaplen; i++) {
                if (group_o) {
                    msgpack_pack_map(response, schema->nfields + 1);
                    
                    mp_pack_string(response, "_count");
                    
                    struct group *grp = 0;
                    HASH_FIND_INT64(groups, &groupcache[heap[i]], grp);
                    if (grp)
                        msgpack_pack_long(response, grp->count);
                    else
                        msgpack_pack_long(response, 0);
                }
                else
                    msgpack_pack_map(response, schema->nfields);
                    
                for (size_t j = 0; j < schema->nfields; j++) {
                    struct field *field = &schema->fields[j];
                    
                    mp_pack_string(response, field->name);
                    
                    msgpack_pack_long(response, field_data_get(field, table->data + heap[i] * recordsize));
                    
                }
            }
        }
        else {
            msgpack_pack_array(response, 0);
        }
        
cleanup:
        // table may be NULL if the flag CAN_RETURN_NULL is set and the table doesn't exist
        
        if (table)
            pthread_rwlock_unlock(&table->rwlock);
        
        struct group *current_group, *tmp;
        HASH_ITER(hh, groups, current_group, tmp) {
            HASH_DEL(groups, current_group);
            if (current_group)
                slab_free(current_group);
        }
        
        if (groupcache)
            slab_free(groupcache);
        if (heap)
            slab_free(heap);
        if (heap_inverse)
            slab_free(heap_inverse);
        
        if (modified)
            table_write(table);
        
        for (int res = check_and_return_error(&context, response); res;)
            return res;
    }
    return 0;
};

int command_remove(struct request *request, msgpack_object o, msgpack_packer *response) {

    (void)response;

    if (request && server->is_slave)
        return CRABDB_ERROR_NOT_MASTER;

    struct eval_context context;
    eval_context_init(&context);

    char bucket_name[64] = {0};
    int64_t table_id = 0;
    msgpack_object *query_o = NULL;
    int err = 0;

    if (o.type == MSGPACK_OBJECT_MAP) {
        for (msgpack_object_kv* p = o.via.map.ptr; p < o.via.map.ptr + o.via.map.size; p++) {
            if (mp_raw_eq(p->key, "bucket"))
                mp_raw_strcpy(bucket_name, p->val, sizeof(bucket_name));
            else if (mp_raw_eq(p->key, "table"))
                table_id = p->val.via.i64;
            else if (mp_raw_eq(p->key, "query"))
                query_o = &p->val;
        }
        
        struct bucket *bucket = bucket_get(bucket_name, CAN_RETURN_NULL);
        struct table *table = bucket_get_table(bucket, table_id, CAN_RETURN_NULL);
        
        if (table) {
            pthread_rwlock_wrlock(&table->rwlock);
            
            struct schema *schema = table->schema;
            struct crabql query;
            crabql_init(&query, query_o, schema, DATA_MODE_READ);
            
            if (query.error) {
                err = 904;
                goto done;
            }

            binlog_save_request(request);
            
            context.table = table_id;
            
            size_t datasize = ceildiv(schema->nbits, 8);
            uint32_t newlen = 0;
            void *newdata = slab_alloc(table->len * datasize);
            if (!newdata) {
                log_error("can't alloc new data");
                goto done;
            }
            
            for (uint32_t i = 0; i < table->len; i++) {
                context.data = table->data + i * datasize;
                int64_t val = crabql_exec(&query, &context);
                if (!val) {
                    // append to new list
                    memcpy(newdata + newlen * datasize, context.data, datasize);
                    newlen++;
                }
            }
            memcpy(table->data, newdata, newlen * datasize);
            slab_free(newdata);
            table->len = newlen;

done:
            crabql_finish(&query);
            
            pthread_rwlock_unlock(&table->rwlock);
            
            table_write(table);
        }
    }

    return err;
};

static int64_t to_int64(msgpack_object *o, int64_t *res) {
    switch (o->type) {
        case MSGPACK_OBJECT_NIL:
            *res = 0;
            return 0;
        case MSGPACK_OBJECT_POSITIVE_INTEGER:
        case MSGPACK_OBJECT_NEGATIVE_INTEGER:
            *res = o->via.i64;
            return 0;
        case MSGPACK_OBJECT_BOOLEAN:
            *res = o->via.boolean ? -1 : 0;
            return 0;
        default:
            return -1;
    }
}

static int plain_data(msgpack_object *o, struct schema *schema, void *data) {
    if (o->type == MSGPACK_OBJECT_MAP) {
        msgpack_object_kv *kv;
        for (kv = o->via.map.ptr; kv < o->via.map.ptr + o->via.map.size; kv++) {
            int64_t val;
            if (kv->key.type == MSGPACK_OBJECT_RAW && to_int64(&kv->val, &val) == 0) {
                char *field_name = strndup(kv->key.via.raw.ptr, kv->key.via.raw.size);
                struct field *field = schema_field_get(schema, field_name);
                free(field_name);
                if (field) {
                    field_data_set(field, data, val);
                }
                else {
                    return -2;
                }
            }
            else {
                return -1;
            }
        }
        return 0;
    }
    else {
        return -3;
    }
}


void mp_log_dump(UT_string *s, msgpack_object o)
{
    switch(o.type) {
    case MSGPACK_OBJECT_NIL:
        uts_printf_concat(s, "null");
        break;

    case MSGPACK_OBJECT_BOOLEAN:
        uts_printf_concat(s, (o.via.boolean ? "true" : "false"));
        break;

    case MSGPACK_OBJECT_POSITIVE_INTEGER:
        uts_printf_concat(s, "%"PRIu64, o.via.u64);
        break;

    case MSGPACK_OBJECT_NEGATIVE_INTEGER:
        uts_printf_concat(s, "%"PRIi64, o.via.i64);
        break;

    case MSGPACK_OBJECT_DOUBLE:
        uts_printf_concat(s, "%f", o.via.dec);
        break;

    case MSGPACK_OBJECT_RAW:
        uts_printf_concat(s, "\"");
        utstring_bincpy(s, o.via.raw.ptr, o.via.raw.size);
        uts_printf_concat(s, "\"");
        break;

    case MSGPACK_OBJECT_ARRAY:
        uts_printf_concat(s, "[");
        if(o.via.array.size != 0) {
            msgpack_object* p = o.via.array.ptr;
            mp_log_dump(s, *p);
            ++p;
            msgpack_object* const pend = o.via.array.ptr + o.via.array.size;
            for(; p < pend; ++p) {
                uts_printf_concat(s, ",");
                mp_log_dump(s, *p);
            }
        }
        uts_printf_concat(s, "]");
        break;

    case MSGPACK_OBJECT_MAP:
        uts_printf_concat(s, "{");
        if(o.via.map.size != 0) {
            msgpack_object_kv* p = o.via.map.ptr;
            mp_log_dump(s, p->key);
            uts_printf_concat(s, ":");
            mp_log_dump(s, p->val);
            ++p;
            msgpack_object_kv* const pend = o.via.map.ptr + o.via.map.size;
            for(; p < pend; ++p) {
                uts_printf_concat(s, ",");
                mp_log_dump(s, p->key);
                uts_printf_concat(s, ":");
                mp_log_dump(s, p->val);
            }
        }
        uts_printf_concat(s, "}");
        break;

    default:
        // FIXME
        uts_printf_concat(s, "#<UNKNOWN %i %"PRIu64">", o.type, o.via.u64);
    }
}

int command_insert(struct request *request, msgpack_object o, msgpack_packer *response) {
    (void) request;
    
    char bucket_name[64] = {0};
    int64_t table_id = 0;
    msgpack_object *data_o = NULL;
    msgpack_object *query_o = NULL;
    bool overwrite = false;
    
    if (o.type == MSGPACK_OBJECT_MAP) {
        msgpack_object_kv* p;
        for (p = o.via.map.ptr; p < o.via.map.ptr + o.via.map.size; p++) {
            if (mp_raw_eq(p->key, "bucket"))
                mp_raw_strcpy(bucket_name, p->val, sizeof(bucket_name));
            else if (mp_raw_eq(p->key, "table"))
                table_id = p->val.via.i64;
            else if (mp_raw_eq(p->key, "data"))
                data_o = &p->val;
            else if (mp_raw_eq(p->key, "overwrite"))
                overwrite = p->val.via.boolean;
            else if (mp_raw_eq(p->key, "query"))
                query_o = &p->val;
        }
        
        if (!data_o) {
            /* TODO: return CRABDB_ERROR_INSERT_TABLE; */
            return -2;
        }
        
        struct bucket *bucket = bucket_get(bucket_name, CAN_RETURN_NULL);
        
        if (!bucket) {
            /* TODO: return CRABDB_ERROR_INSERT_TABLE; */
            return 13;
        }
        
        struct table *table = bucket_get_table(bucket, table_id, 0);
        table_check_schema(table);
        
        pthread_rwlock_wrlock(&table->rwlock);
        
        if (table->schema == NULL) {
            pthread_rwlock_unlock(&table->rwlock);
            /* TODO: return CRABDB_ERROR_INSERT_TABLE; */
            return 12;
        }
        
        size_t datasize = ceildiv(table->schema->nbits, 8);
        uint32_t tablelen = table->len;

        bool found = 0;
        bool modified = 0;

        if (query_o) {
            struct eval_context context;
            eval_context_init(&context);
            context.table = table_id;

            struct crabql query;
            crabql_init(&query, query_o, table->schema, DATA_MODE_READ);
            
            stat_add(NUM_SCANS, tablelen);
            
            for (uint32_t i = 0; i < tablelen; i++) {
                context.data = table->data + i * datasize;
                int64_t res = crabql_exec(&query, &context);
                if (res) {
                    found = 1;
                    break;
                }
            }

            crabql_finish(&query);
        }

        if (!found) {
            // insert a new item

            char data[datasize];
            memset(data, 0, datasize);
            
            int res = plain_data(data_o, table->schema, data);
            if (res != 0) {
                log_warn("plain_data failed, fallback...");
                UT_string *t;
                utstring_new(t);
                mp_log_dump(t, *data_o);
                log_warn("data: %s", utstring_body(t));
                utstring_free(t);

                // fail here...
                pthread_rwlock_unlock(&table->rwlock);
                /* TODO: return CRABDB_ERROR_INSERT_TABLE; */
                return 727;
            }

            res = table_insert_nolock(table, table->schema, data, overwrite ? TABLE_INSERT_FLAG_OVERWRITE : 0);
            if (!res) {
                /* TODO: ret = CRABDB_ERROR_INSERT_TABLE; */
                log_error("insert table failed");
            }
            else if (res & TABLE_INSERT_MODIFIED) {
                modified = 1;
                binlog_save_data(table, data);
            }
        }

        pthread_rwlock_unlock(&table->rwlock);
        
        if (modified)
            table_write(table);
            
        mp_pack_bool(response, modified);
    }

    return 0;
}

struct command {
    const char *name;
    command_func_t func;
};

int command_drop_bucket(struct request *request, msgpack_object o, msgpack_packer *response) {
    (void) request;
    (void) response;
    
    char bucket_name[64] = {0};
    
    if (o.type == MSGPACK_OBJECT_MAP) {
        msgpack_object_kv* p;
        for (p = o.via.map.ptr; p < o.via.map.ptr + o.via.map.size; p++) {
            if (mp_raw_eq(p->key, "bucket"))
                mp_raw_strcpy(bucket_name, p->val, sizeof(bucket_name));
        }
        
        bucket_drop(bucket_name);
    }
    return 0;
}

int command_list_buckets(struct request *request, msgpack_object o, msgpack_packer *response) {
    (void) request;
    (void) o;
    
    char *buckets = bucket_list();
    
    char *p = buckets;
    int count = 0;
    while (p && *p) {
        while (*p) p++;
        p++;
        count++;
    }
    
    msgpack_pack_array(response, count);
    p = buckets;
    while (*p) {
        p += mp_pack_string(response, p) + 1;
    }
    slab_free(buckets);
    return 0;    
}

int command_list_tables(struct request *request, msgpack_object o, msgpack_packer *response) {
    (void) request;
    (void) response;

    char bucket_name[64] = {0};
    int64_t from_table_id = 0;
    int has_from_table_id = 0;
    int ret = 0;
    
    /* XXX: why other commands don't have this check */
    if (o.type != MSGPACK_OBJECT_MAP) {
        ret = CRABDB_ERROR_INVALID_ARGUMENT;
        goto err;
    }

    mp_for_each_in_kv(p, o) {
        if (mp_raw_eq(p->key, "bucket"))
            mp_raw_strcpy(bucket_name, p->val, sizeof(bucket_name));
        if (mp_raw_eq(p->key, "from_table_id")) {
            if (p->val.type == MSGPACK_OBJECT_NEGATIVE_INTEGER || p->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                from_table_id = p->val.via.i64;
                has_from_table_id = 1;
            }
        }
    }
        
    struct bucket *bucket = bucket_get(bucket_name, CAN_RETURN_NULL);
    
    if (bucket == NULL)
        return -1;

    void *keys[1024];
    size_t key_count = sizeof(keys) / sizeof(keys[0]);
    // printf("%016"PRIx64"\n", from_table_id);
    rwlock_rdlock(io_rwlock);
    pthread_rwlock_rdlock(&bucket->rwlock);
    ret = list_keys(bucket->bsp->indexdb, has_from_table_id ? &from_table_id : NULL, 8, keys, &key_count);
    pthread_rwlock_unlock(&bucket->rwlock);
    rwlock_unrdlock(io_rwlock);

    if (ret != 0)
        goto err;


    msgpack_pack_map(response, 1);
    mp_pack_string(response, "items");
    msgpack_pack_array(response, key_count);

    for (size_t i = 0; i < key_count; i++) {
        int64_t table_id = *(int64_t *) keys[i];
        free(keys[i]);
        msgpack_pack_long(response, table_id);
    }

err:
    return ret;
}

int command_describe_bucket(struct request *request, msgpack_object o, msgpack_packer *response) {
    (void) request;

    char bucket_name[64] = {0};
    
    int ret = 0;

    if (o.type != MSGPACK_OBJECT_MAP) {
        ret = CRABDB_ERROR_INVALID_ARGUMENT;
        goto err;
    }

    mp_for_each_in_kv(p, o) {
        if (mp_raw_eq(p->key, "bucket"))
            mp_raw_strcpy(bucket_name, p->val, sizeof(bucket_name));
    }
        
    struct bucket *bucket = bucket_get(bucket_name, CAN_RETURN_NULL);
    if (bucket == NULL)
        return -1;

    struct schema *schema = bucket->schema;
    if (schema == NULL)
        return -1;
    
    
    msgpack_pack_array(response, schema->nfields);
    for (size_t i = 0; i < schema->nfields; i++) {
        struct field *field = &schema->fields[i];
        msgpack_pack_map(response, 4);
        mp_pack_string(response, "name");
        mp_pack_string(response, field->name);
        mp_pack_string(response, "nbits");
        msgpack_pack_int(response, field->nbits);
        mp_pack_string(response, "is_signed");
        mp_pack_bool(response, field->is_signed);
        mp_pack_string(response, "is_primary_key");
        mp_pack_bool(response, field->is_primary_key);
    }
        
err:
    return ret;
}

int command_stats(struct request *request, msgpack_object o, msgpack_packer *response) {
    (void) request;
    (void) o;
    
    msgpack_pack_map(response, STAT_MAX);
    for (size_t i = 0; i < STAT_MAX; i++) {
        mp_pack_string(response, stat_name[i]);
        msgpack_pack_long(response, stats[i]);
    }
        
    return 0;
}

int command_freeze(struct request *request, msgpack_object o, msgpack_packer *response) {
    (void) request;
    
    int do_freeze = 0;

    if (o.type == MSGPACK_OBJECT_MAP) {
        msgpack_object_kv* p;
        for (p = o.via.map.ptr; p < o.via.map.ptr + o.via.map.size; p++) {
            if (mp_raw_eq(p->key, "freeze"))
                do_freeze = !!p->val.via.i64;
        }

        if (do_freeze) {
            freeze_done = 0;
            freeze = 1;
            while (freeze && !freeze_done)
                usleep(100000);
        }
        else {
            freeze = 0;
        }


        msgpack_pack_map(response, 1);
        mp_pack_string(response, "last_write_nonce");
        msgpack_pack_long(response, last_write_nonce);
    }

    return 0;
}

int command_get_raw_table(struct request *request, msgpack_object o, msgpack_packer *response) {
    (void) request;
    (void) response;
    (void) o;

    char bucket_name[64] = {0};
    int64_t table_id = 0;
    
    if (o.type != MSGPACK_OBJECT_MAP)
        return CRABDB_ERROR_INVALID_ARGUMENT;

    for (msgpack_object_kv *p = o.via.map.ptr; p < o.via.map.ptr + o.via.map.size; p++) {
        if (mp_raw_eq(p->key, "bucket"))
            mp_raw_strcpy(bucket_name, p->val, sizeof(bucket_name));
        else if (mp_raw_eq(p->key, "table"))
            table_id = p->val.via.i64;
        else
            return CRABDB_ERROR_INVALID_ARGUMENT;
    }
    
    struct bucket *bucket = bucket_get(bucket_name, CAN_RETURN_NULL);
    
    if (bucket == NULL)
        return CRABDB_ERROR_NO_SUCH_BUCKET;
    
    struct table *table = bucket_get_table(bucket, table_id, CAN_RETURN_NULL);
    if (table == NULL) {
        msgpack_pack_map(response, 1);

        mp_pack_string(response, "data");
        msgpack_pack_raw(response, 0);
    }
    else {
        table_check_schema(table);
        
        pthread_rwlock_rdlock(&table->rwlock);

        size_t data_size = ceildiv(table->schema->nbits, 8) * table->len;

        msgpack_pack_map(response, 1);

        mp_pack_string(response, "data");
        msgpack_pack_raw(response, data_size);
        msgpack_pack_raw_body(response, table->data, data_size);

        pthread_rwlock_unlock(&table->rwlock);
    }

    return 0;
}

int command_set_raw_table(struct request *request, msgpack_object o, msgpack_packer *response) {
    (void) request;
    (void) response;
    (void) o;

    if (request && server->is_slave && binlog->position != 0)
        return CRABDB_ERROR_NOT_MASTER;

    char bucket_name[64] = {0};
    int64_t table_id = 0;
    size_t data_size = 0;
    const char *data = NULL;

    if (o.type != MSGPACK_OBJECT_MAP)
        return CRABDB_ERROR_INVALID_ARGUMENT;

    for (msgpack_object_kv *p = o.via.map.ptr; p < o.via.map.ptr + o.via.map.size; p++) {
        if (mp_raw_eq(p->key, "bucket"))
            mp_raw_strcpy(bucket_name, p->val, sizeof(bucket_name));
        else if (mp_raw_eq(p->key, "table"))
            table_id = p->val.via.i64;
        else if (mp_raw_eq(p->key, "data")) {
            data = p->val.via.raw.ptr;
            data_size = p->val.via.raw.size;
        }
    }
    
    struct bucket *bucket = bucket_get(bucket_name, CAN_RETURN_NULL);
    if (bucket == NULL)
        return CRABDB_ERROR_NO_SUCH_BUCKET;
    
    struct table *table = bucket_get_table(bucket, table_id, 0);
    table_check_schema(table);
    
    pthread_rwlock_wrlock(&table->rwlock);

    size_t record_size = ceildiv(table->schema->nbits, 8);
    table->len = data_size / record_size;
    table->cap = table->len;

    table->data = slab_realloc(table->data, data_size);
    memcpy(table->data, data, data_size);

    pthread_rwlock_unlock(&table->rwlock);

    table_write(table);

    return 0;
}


struct command command_table[] = {
    /* 0 */
    {"eval", command_eval},
    {"query", command_query},
    {"remove", command_remove},
    {"alter_bucket", command_create_bucket},
    {"insert", command_insert},
    /* 5 */
    {"drop_bucket", command_drop_bucket},
    {"list_buckets", command_list_buckets},
    {"terminate", (command_func_t) exit},
    {"describe", command_describe_bucket},
    {"multifind", command_multifind},
    /* 10 */
    {"stats", command_stats},
    {"freeze", command_freeze},
    {"get_binlog", command_get_binlog},
    {"replay_binlog", command_replay_binlog},
    {"list_tables", command_list_tables},
    /* 15 */
    {"binlog_stats", command_binlog_stats},
    {"get_raw_table", command_get_raw_table},
    {"set_raw_table", command_set_raw_table},
    {"command_set_binlog_position", command_set_binlog_position},
};

int execute_command(struct request *request, uint32_t command, msgpack_object o, msgpack_packer* response) {
    if (command >= sizeof(command_table) / sizeof(command_table[0]))
        return CRABDB_ERROR_INVALID_ARGUMENT;
    
    static long cmdseq = 0;
    long seq = __sync_add_and_fetch(&cmdseq, 1);
    
    stat_add(NUM_COMMANDS, 1);
    
    // log_notice("executing command #%ld: [%s]", seq, command_table[command].name);
    struct timeval tv1, tv2;
    gettimeofday(&tv1, NULL);
    int res = command_table[command].func(request, o, response);
    gettimeofday(&tv2, NULL);
    
    double cost = (double) ((tv2.tv_sec + tv2.tv_usec / 1000000.0) - (tv1.tv_sec + tv1.tv_usec / 1000000.0));
    if (cost >= 0.2) {
        log_warn("command #%ld [%s] completed in %.6lf seconds.", seq, command_table[command].name, cost);

        UT_string *t;
        utstring_new(t);
        mp_log_dump(t, o);
        log_warn("command #%ld [%s] dump: %s", seq, command_table[command].name, utstring_body(t));
        utstring_free(t);
    }
    
    return res;
}
