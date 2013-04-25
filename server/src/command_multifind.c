#include <stdint.h>
#include "utils.h"
#include "server.h"
#include "crabql.h"
#include "schema.h"
#include "table.h"
#include "stats.h"
#include "slab.h"

struct heap_record {
    int64_t pk;
    int64_t table_id;
    void *data;
};

static void heap_up(struct heap_record *heap, uint32_t cur) {
    while (cur > 0) {
        uint32_t par = (cur - 1) / 2;
        
        if (heap[cur].pk < heap[par].pk)
            swap(&heap[cur], &heap[par]);
        cur = par;
    }
}

static void heap_down(struct heap_record *heap, uint32_t heaplen) {
    uint32_t cur = 0;
    for (;;) {
        uint32_t lc = cur * 2 + 1, rc = lc + 1;
        
        uint32_t minone = cur;
        if (lc < heaplen && heap[lc].pk < heap[cur].pk)
            minone = lc;
        if (rc < heaplen && heap[rc].pk < heap[minone].pk)
            minone = rc;
        if (minone == cur)
            break;
        
        swap(&heap[cur], &heap[minone]);
        cur = minone;
    }
}

static void heap_insert(struct heap_record *heap, uint32_t heaplen, struct heap_record value) {
    heap[heaplen] = value;
    heap_up(heap, heaplen);
}

static void heap_extract(struct heap_record *heap, uint32_t heaplen) {
    heap[0] = heap[heaplen - 1];
    heap_down(heap, heaplen - 1);
}

static void heap_sort(struct heap_record *heap, uint32_t heaplen) {
    while (heaplen) {
        swap(&heap[0], &heap[heaplen - 1]);
        heaplen--;
        heap_down(heap, heaplen);
    }
}

void *memdup(const void *src, size_t n) {
    void *dst = slab_alloc(n);
    memcpy(dst, src, n);
    return dst;
}

int command_multifind(struct request *request, msgpack_object o, msgpack_packer* response) {
    (void) request;
    struct eval_context context;
    eval_context_init(&context);
    
    char bucket_name[64] = {0};
    msgpack_object *query_o = NULL;
    msgpack_object *tables_o = NULL;
    
    struct crabql query;
    crabql_func_t query_func = NULL;
    
    uint64_t num_items = 0;
    bool has_more = 0;
    uint32_t offset = 0;
    uint32_t limit = -1;
    
    // initialize the heap for sorting
    struct heap_record *heap = NULL;
    uint32_t heaplen = 0;
    
    struct field *pk_field = NULL;
    struct schema *schema = NULL;
    
    if (o.type != MSGPACK_OBJECT_MAP)
        goto done;
        
    msgpack_object_kv* p;
    for (p = o.via.map.ptr; p < o.via.map.ptr + o.via.map.size; p++) {
        if (mp_raw_eq(p->key, "bucket"))
            mp_raw_strcpy(bucket_name, p->val, sizeof(bucket_name));
        else if (mp_raw_eq(p->key, "tables"))
            tables_o = &p->val;
        else if (mp_raw_eq(p->key, "offset"))
            offset = p->val.via.i64;
        else if (mp_raw_eq(p->key, "limit"))
            limit = p->val.via.i64;
        else if (mp_raw_eq(p->key, "query"))
            query_o = &p->val;
    }
    
    if (tables_o == NULL)
        goto done;

    if (limit != (uint32_t) -1)
        limit += offset;
    if (limit > 100000)
        limit = 100000;
    
    heap = (struct heap_record *) slab_alloc((limit + 1) * sizeof(struct heap_record));
    
    struct bucket *bucket = bucket_get(bucket_name, CAN_RETURN_NULL);
    if (bucket == NULL)
        goto done;
        
    pthread_rwlock_rdlock(&bucket->rwlock);
    schema = bucket->schema;
    pthread_rwlock_unlock(&bucket->rwlock);
    
    if (schema == NULL)
        goto done;
    
    size_t recordsize = ceildiv(schema->nbits, 8);
    
    pk_field = schema_field_get_primary(schema);
    if (pk_field == NULL)
        goto done;
    
    crabql_init(&query, query_o, schema, DATA_MODE_READ);
    query_func = crabql_get_func(&query);
    
    uint64_t num_scans = 0;
    
    msgpack_object* const pend = tables_o->via.array.ptr + tables_o->via.array.size;
    for (msgpack_object *p = tables_o->via.array.ptr; p < pend; ++p) {
        int64_t table_id = p->via.i64;
        struct table *table = bucket_get_table(bucket, table_id, CAN_RETURN_NULL);
        if (table == NULL)
            continue;
            
        uint32_t tablelen = table->len;
        if (tablelen == 0)
            continue;
        
        num_items += tablelen;
        
        table_check_schema(table);
        
        pthread_rwlock_rdlock(&table->rwlock);
        
        if (table->schema != schema)
            goto table_cleanup;
        
        context.table = table_id;
        
        for (uint32_t i = tablelen - 1; i != (uint32_t) -1; i--) {
            num_scans++;
            void *data = table->data + i * recordsize;
            context.data = data;
            
            int64_t val = 1;
            
            if (query_func) {
                val = query_func(&context);
            }
            if (val) {
                int64_t pk = field_data_get(pk_field, data);
                
                if (heaplen >= limit && pk < heap[0].pk) {
                    has_more = 1;
                    goto table_cleanup;
                }
                
                struct heap_record record = {
                    .pk = pk,
                    .table_id = table_id,
                    .data = memdup(data, recordsize),
                };

                heap_insert(heap, heaplen++, record);
                if (heaplen > limit) {
                    has_more = 1;
                    slab_free(heap[0].data);
                    heap_extract(heap, heaplen--);
                }
            }
        }
table_cleanup:
        pthread_rwlock_unlock(&table->rwlock);
    }
    
    stat_add(NUM_SCANS, num_scans);
    
    crabql_finish(&query);

done:
    msgpack_pack_map(response, 3);
    mp_pack_string(response, "num_items");
    msgpack_pack_long(response, num_items);
    mp_pack_string(response, "has_more");
    mp_pack_bool(response, has_more);
    mp_pack_string(response, "items");
    
    if (heaplen > offset) {
        heap_sort(heap, heaplen);
        msgpack_pack_array(response, heaplen - offset);
        
        for (uint32_t i = offset; i < heaplen; i++) {
            msgpack_pack_map(response, 1 + schema->nfields);
            mp_pack_string(response, "_table");
            msgpack_pack_long(response, heap[i].table_id);
            
            for (size_t j = 0; j < schema->nfields; j++) {
                struct field *field = &schema->fields[j];
                mp_pack_string(response, field->name);
                msgpack_pack_long(response, field_data_get(field, heap[i].data));
            }
        }
    }
    else {
        msgpack_pack_array(response, 0);
    }
    
    for (uint32_t i = 0; i < heaplen; i++)
        slab_free(heap[i].data);
    
    if (heap)
        slab_free(heap);
    return 0;
}
