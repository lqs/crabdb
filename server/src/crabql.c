#include <sys/mman.h>
#include "crabql.h"
#include "utils.h"
#include "bucket.h"
#include "table.h"
#include "log.h"
#include <stdarg.h>
#include <libtcc.h>
#include "utstring.h"
#include "geo.h"
#include "error.h"
#include "slab.h"

#define ERROR_ASSERT(cond) do { \
    if (!(cond)) { \
        crabql->error = 1; \
        snprintf(crabql->error_message, sizeof(crabql->error_message), "Assertion: %s at %s:%d", #cond, __FILE__, __LINE__); \
        crabql->error_message[sizeof(crabql->error_message) - 1] = '\0'; \
        return; \
    } \
} while (0)


#define ERROR_CHECK() do { if (crabql->error) return; } while (0)



static void crabql_generate_code(struct crabql *crabql, msgpack_object *o, UT_string *s);

static void EXPORT_if(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) {
    if (nargs == 3) {
        uts_printf_concat(s, "(");
        crabql_generate_code(crabql, &o[1], s);
        uts_printf_concat(s, " ? ");
        crabql_generate_code(crabql, &o[2], s);
        uts_printf_concat(s, " : 0)");
    }
    if (nargs == 4) {
        uts_printf_concat(s, "(");
        crabql_generate_code(crabql, &o[1], s);
        uts_printf_concat(s, " ? ");
        crabql_generate_code(crabql, &o[2], s);
        uts_printf_concat(s, " : ");
        crabql_generate_code(crabql, &o[3], s);
        uts_printf_concat(s, ")");
    }
}


#define DEFINE_2ARY_OP(name, op) \
static void EXPORT_##name(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) { \
    ERROR_ASSERT(nargs == 3); \
    uts_printf_concat(s, "("); \
    crabql_generate_code(crabql, &o[1], s); \
    uts_printf_concat(s, " " #op " "); \
    crabql_generate_code(crabql, &o[2], s); \
    uts_printf_concat(s, ")"); \
}

#define DEFINE_1ARY_OP(name, op) \
static void EXPORT_##name(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) { \
    ERROR_ASSERT(nargs == 2); \
    uts_printf_concat(s, #op); \
    crabql_generate_code(crabql, &o[1], s); \
}

#define DEFINE_2ARY_OP_BOOL(name, op) \
static void EXPORT_##name(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) { \
    ERROR_ASSERT(nargs == 3); \
    uts_printf_concat(s, "(("); \
    crabql_generate_code(crabql, &o[1], s); \
    uts_printf_concat(s, " " #op " "); \
    crabql_generate_code(crabql, &o[2], s); \
    uts_printf_concat(s, ") ? -1 : 0)"); \
}

#define DEFINE_MULTIARY_OP(name, op) \
static void EXPORT_##name(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) { \
    ERROR_ASSERT(nargs >= 2); \
    uts_printf_concat(s, "("); \
    crabql_generate_code(crabql, &o[1], s); \
    for (int i = 2; i < nargs; i++) { \
        uts_printf_concat(s, " " #op " "); \
        crabql_generate_code(crabql, &o[i], s); \
    } \
    uts_printf_concat(s, ")"); \
}

#define DEFINE_C_FUNC(name, func) \
static void EXPORT_##name(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) { \
    ERROR_ASSERT(nargs >= 2); \
    uts_printf_concat(s, #func "("); \
    crabql_generate_code(crabql, &o[1], s); \
    for (int i = 2; i < nargs; i++) { \
        uts_printf_concat(s, ", "); \
        crabql_generate_code(crabql, &o[i], s); \
    } \
    uts_printf_concat(s, ")"); \
}

static int64_t pospow(int64_t x, int64_t y) {
    if (y < 0)
        return 0;
    if (y == 0)
        return 1;
    int64_t res = 1;
    if (y % 2 == 1)
        res = x;
    int64_t num = pospow(x, y / 2);
    return num * num * res;
}

static void EXPORT_pow(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) {
    ERROR_ASSERT(nargs == 3);
    uts_printf_concat(s, "pospow(");
    crabql_generate_code(crabql, &o[1], s);
    uts_printf_concat(s, ", ");
    crabql_generate_code(crabql, &o[2], s);
    uts_printf_concat(s, ")");
}

static void EXPORT_div(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) {
    ERROR_ASSERT(nargs == 3);
    uts_printf_concat(s, "(");
    crabql_generate_code(crabql, &o[2], s);
    uts_printf_concat(s, " ? ");
    crabql_generate_code(crabql, &o[1], s);
    uts_printf_concat(s, " / ");
    crabql_generate_code(crabql, &o[2], s);
    uts_printf_concat(s, " : ");
    uts_printf_concat(s, " 0)");
}

static void EXPORT_mod(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) {
    ERROR_ASSERT(nargs == 3);
    uts_printf_concat(s, "(");
    crabql_generate_code(crabql, &o[2], s);
    uts_printf_concat(s, " ? ");
    crabql_generate_code(crabql, &o[1], s);
    uts_printf_concat(s, " %% ");
    crabql_generate_code(crabql, &o[2], s);
    uts_printf_concat(s, " : ");
    uts_printf_concat(s, " 0)");
}

static void EXPORT_dist(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) {
    ERROR_ASSERT(nargs == 3);
    uts_printf_concat(s, "geo_dist(");
    crabql_generate_code(crabql, &o[1], s);
    uts_printf_concat(s, ", ");
    crabql_generate_code(crabql, &o[2], s);
    uts_printf_concat(s, ")");
}

static void EXPORT_geo_from_latlon(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) {
    ERROR_ASSERT(nargs == 3);
    uts_printf_concat(s, "geo_from_latlon(");
    crabql_generate_code(crabql, &o[1], s);
    uts_printf_concat(s, ", ");
    crabql_generate_code(crabql, &o[2], s);
    uts_printf_concat(s, ")");
}

#define DEFINE_LOGICAL_OP(name, op, break_if) \
static void EXPORT_##name(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) { \
    ERROR_ASSERT(nargs >= 2); \
    size_t nrp = 0; \
    for (int i = 1; i < nargs; i++) { \
        if (i + 1 < nargs) { \
            uint32_t cvar = crabql->nvar; \
            crabql->nvar++; \
            uts_printf_concat(s, "((v%"PRIu32" = ", cvar); \
            crabql_generate_code(crabql, &o[i], s); \
            uts_printf_concat(s, ") == " #break_if ") ? v%"PRIu32" : (v%"PRIu32" " #op " ", cvar, cvar); \
            nrp++; \
        } \
        else { \
            crabql_generate_code(crabql, &o[i], s); \
        } \
    } \
    char right_parentheses[nrp + 1]; \
    memset(right_parentheses, ')', nrp); \
    right_parentheses[nrp] = '\0'; \
    uts_printf_concat(s, right_parentheses); \
}

DEFINE_1ARY_OP(not, ~)
DEFINE_1ARY_OP(neg, -)

DEFINE_2ARY_OP_BOOL(lt, <)
DEFINE_2ARY_OP_BOOL(lte, <=)
DEFINE_2ARY_OP_BOOL(gt, >)
DEFINE_2ARY_OP_BOOL(gte, >=)
DEFINE_2ARY_OP_BOOL(eq, ==)
DEFINE_2ARY_OP_BOOL(ne, !=)

DEFINE_MULTIARY_OP(add, +)
DEFINE_MULTIARY_OP(sub, -)
DEFINE_MULTIARY_OP(mul, *)
DEFINE_MULTIARY_OP(xor, ^)

DEFINE_LOGICAL_OP(or, |, -1)
DEFINE_LOGICAL_OP(and, &, 0)

DEFINE_2ARY_OP(shl, <<)
DEFINE_2ARY_OP(shr, >>)

DEFINE_C_FUNC(sin, sin)
DEFINE_C_FUNC(cos, cos)
DEFINE_C_FUNC(tan, tan)
DEFINE_C_FUNC(asin, asin)
DEFINE_C_FUNC(acos, acos)
DEFINE_C_FUNC(atan, atan)
DEFINE_C_FUNC(atan2, atan2)
DEFINE_C_FUNC(sqrt, sqrt)

static int parse_bucket_and_table(const char *s, size_t len, char **bucket_name, int64_t *table_id) {

    const char *lb = NULL, *rb = NULL;
    for (const char *p = s, *pend = s + len; p < pend; p++) {
        if (*p == '[')
            lb = p;
        if (*p == ']')
            rb = p;
    }
    
    /*
        "bucket_name[123]"
                    ^   ^
                    lb  rb
    */
    
    if (lb == NULL || rb == NULL || lb > rb || rb + 1 != s + len)
        return -1;
    
    *table_id = 0;
    int sign = 1;
    for (const char *p = lb + 1; p < rb; p++) {
        if (p == lb + 1 && *p == '-') {
            sign = -1;
            continue;
        }
        if (*p < '0' || *p > '9')
            return -1;
        *table_id *= 10;
        *table_id += *p - '0';
    }
    *table_id *= sign;
    *bucket_name = strndup(s, lb - s);
    return 0;
}

static int64_t bs64(int64_t x, int64_t *sorted, size_t count) {
    size_t l = 0, r = count;
    int64_t res = 0;
    while (l < r) {
        size_t m = (l + r) / 2;
        int64_t v2 = sorted[m];
        if (x < v2)
            r = m;
        else if (x > v2)
            l = m + 1;
        else {
            res = -1;
            break;
        }
    }
    return res;
}

static void make_field_data_get(struct field *field, UT_string *s) {

    // int16_t offset;
    // uint16_t nbits;
    
    if (field->offset % 8 == 0) {
        uts_printf_concat(s, "(((struct { %s data:%"PRIu16"; } __attribute__ ((packed)) *) (context->data + %"PRIu64") /* %s */)->data)",
            field->is_signed ? "int64_t" : "uint64_t",
            (uint16_t) field->nbits,
            (uint16_t) field->offset / 8,
            field->name
        );
    }
    else {
        uts_printf_concat(s, "(((struct { int64_t offset:%"PRIu16"; %s data:%"PRIu16"; } __attribute__ ((packed)) *) (context->data + %"PRIu64") /* %s */)->data)",
            (uint16_t) field->offset % 8,
            field->is_signed ? "int64_t" : "uint64_t",
            (uint16_t) field->nbits,
            (uint16_t) field->offset / 8,
            field->name
        );
    }
}

static void *bsintableget(int64_t x, void *table_data, struct field *field, size_t datasize, size_t table_len) {
    size_t l = 0, r = table_len;
    while (l < r) {
        size_t m = (l + r) / 2;
        void *data = table_data + m * datasize;
        int64_t v2 = field_data_get(field, data);
            
        if (x < v2)
            r = m;
        else if (x > v2)
            l = m + 1;
        else {
            return data;
        }
    }
    return NULL;
}

static int64_t bsintable(int64_t x, void *table_data, struct field *field, size_t datasize, size_t table_len) {
    return bsintableget(x, table_data, field, datasize, table_len) ? -1 : 0;
}

static int cmp64(const void * a, const void * b)
{
    int64_t a64 = *(int64_t *) a;
    int64_t b64 = *(int64_t *) b;
    
    if (a64 < b64)
        return -1;
    else if (a64 > b64)
        return 1;
    return 0;
}

static void lock_table(struct crabql *crabql, struct table *table) {
    int to_lock = 1;
    for (uint32_t i = 0; i < crabql->nlocked; i++) {
        if (crabql->locked[i] == table) {
            to_lock = 0;
            break;
        }
    }
    if (to_lock) {
        if (crabql->nlocked > 120) {
            log_error("too many locked tables");
            return;
        }
        crabql->locked[crabql->nlocked++] = table;
        pthread_rwlock_rdlock(&table->rwlock);
    }
}

static void EXPORT_get(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) {
    //    0          1             2             3          4
    // ['$get', 'source_key', 'bucket_name', table_id, 'target_field']
    
    ERROR_ASSERT(nargs == 5);
    
    ERROR_ASSERT(o[2].type == MSGPACK_OBJECT_RAW);
    ERROR_ASSERT(o[3].type == MSGPACK_OBJECT_POSITIVE_INTEGER || o[3].type == MSGPACK_OBJECT_NEGATIVE_INTEGER);
    ERROR_ASSERT(o[4].type == MSGPACK_OBJECT_RAW);
    
    char bucket_name[32];
    int64_t table_id;
    char field_name[32];
    
    mp_raw_strcpy(bucket_name, o[2], sizeof(bucket_name));
    table_id = o->via.i64;
    mp_raw_strcpy(field_name, o[4], sizeof(field_name));
    
    struct bucket *bucket = bucket_get(bucket_name, CAN_RETURN_NULL);
    struct table *table = bucket_get_table(bucket, table_id, CAN_RETURN_NULL);
    if (table) {
        lock_table(crabql, table);
        
        struct schema *schema = table->schema;
        struct field *field = schema_field_get(schema, field_name);
        
        // bsintableget
        
        (void) field;
        (void) s;
    }
}

static int64_t op_year(int64_t ts) {
    time_t t = ts;
    struct tm tm;
    gmtime_r(&t, &tm);
    return tm.tm_year + 1900;
}

static int64_t op_month(int64_t ts) {
    time_t t = ts;
    struct tm tm;
    gmtime_r(&t, &tm);
    return tm.tm_mon + 1;
}

static int64_t op_day(int64_t ts) {
    time_t t = ts;
    struct tm tm;
    gmtime_r(&t, &tm);
    return tm.tm_mday;
}

DEFINE_C_FUNC(year, op_year)
DEFINE_C_FUNC(month, op_month)
DEFINE_C_FUNC(day, op_day)

static char *dump_binstrescape(const char *mem, size_t size) {
    char *temp = slab_alloc(size * 4 + 1); // \xde\xad
    char *q = temp;
    for (size_t i = 0; i < size; i++) {
        uint8_t x = *(uint8_t *) &mem[i];
        *q++ = '\\';
        *q++ = 'x';
        *q++ = HEX[x / 16];
        *q++ = HEX[x % 16];
    }
    *q = '\0';
    return temp;
}

static void EXPORT_in_polygon(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) {
    ERROR_ASSERT(nargs == 3);

    ERROR_ASSERT(o[2].type == MSGPACK_OBJECT_ARRAY);

    msgpack_object *p = o[2].via.array.ptr;
    size_t len = o[2].via.array.size;
    uint64_t *vertexes = slab_alloc(len * sizeof(vertexes[0]));
    for (size_t i = 0; i < len; i++) {
        ERROR_ASSERT(p[i].type == MSGPACK_OBJECT_POSITIVE_INTEGER);
        vertexes[i] = p[i].via.u64;
    }

    struct polygon *polygon = polygon_make((struct point *) vertexes, len);
    char *binstr = dump_binstrescape((const char *) polygon, polygon_size(polygon));

    uts_printf_concat(s, "cb_point_in_polygon((uint64_t) ");
    crabql_generate_code(crabql, &o[1], s);
    uts_printf_concat(s, ", \"");
    utstring_bincpy(s, binstr, strlen(binstr));
    uts_printf_concat(s, "\")");

    slab_free(vertexes);
    slab_free(polygon);
    slab_free(binstr);
}

static void EXPORT_in_circle(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) {
    ERROR_ASSERT(nargs == 3);
    ERROR_ASSERT(o[2].type == MSGPACK_OBJECT_ARRAY);
    ERROR_ASSERT(o[2].via.array.size == 2);
    ERROR_ASSERT(o[2].via.array.ptr[0].type == MSGPACK_OBJECT_POSITIVE_INTEGER);
    ERROR_ASSERT(o[2].via.array.ptr[1].type == MSGPACK_OBJECT_POSITIVE_INTEGER);

    struct circle *circle = circle_make(*(struct point *) &o[2].via.array.ptr[0].via.u64, o[2].via.array.ptr[1].via.u64);
    char *binstr = dump_binstrescape((const char *) circle, sizeof(struct circle));

    uts_printf_concat(s, "cb_point_in_circle((uint64_t) ");
    crabql_generate_code(crabql, &o[1], s);
    uts_printf_concat(s, ", \"");
    utstring_bincpy(s, binstr, strlen(binstr));
    uts_printf_concat(s, "\")");

    slab_free(circle);
    slab_free(binstr);
}

static void EXPORT_in(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) {
    ERROR_ASSERT(nargs == 3);
    
    if (o[2].type == MSGPACK_OBJECT_ARRAY) {
        
        msgpack_object *p = o[2].via.array.ptr;
        uts_printf_concat(s, "bs64((int64_t) ");
        crabql_generate_code(crabql, &o[1], s);
        uts_printf_concat(s, ", \"");
        size_t len = o[2].via.array.size;
        int64_t *nums = slab_alloc(len * 8);
        for (size_t i = 0; i < len; i++) {
            ERROR_ASSERT(p[i].type == MSGPACK_OBJECT_POSITIVE_INTEGER || p[i].type == MSGPACK_OBJECT_NEGATIVE_INTEGER);
            if (p[i].type == MSGPACK_OBJECT_POSITIVE_INTEGER)
                ERROR_ASSERT(p[i].via.u64 < INT64_MAX);
            
            nums[i] = p[i].via.i64;
        }
        
        qsort(nums, len, 8, cmp64);
        
        //   \x01\x00\x00\x00\x00\x00\x00\x00
        
        char *binstr = dump_binstrescape((const char *) nums, len * sizeof(nums[0]));
        utstring_bincpy(s, binstr, strlen(binstr));
        slab_free(nums);
        slab_free(binstr);
        uts_printf_concat(s, "\", (size_t) %zu)", o[2].via.array.size);
    }
    else if (o[2].type == MSGPACK_OBJECT_RAW) {
        char *bucket_name;
        int64_t table_id;
        if (parse_bucket_and_table(o[2].via.raw.ptr, o[2].via.raw.size, &bucket_name, &table_id) == 0) {
            struct bucket *bucket = bucket_get(bucket_name, CAN_RETURN_NULL);
            free(bucket_name);
            
            if (bucket) {
                struct table *table = bucket_get_table(bucket, table_id, CAN_RETURN_NULL);
                if (table) {
                    lock_table(crabql, table);
                    struct schema *schema = table->schema;
                    if (schema) {
                        struct field *field = schema_field_get_primary(schema);
                        
                        size_t datasize = ceildiv(schema->nbits, 8);
                        
                        uts_printf_concat(s, "bsintable(");
                        crabql_generate_code(crabql, &o[1], s);
                        uts_printf_concat(s, ", (size_t) %pULL, (size_t) %pULL, (size_t) %zu, (size_t) %zu)", table->data, field, datasize, table->len);
                    }
                    else {
                        log_warn("schema is NULL, return 0.");
                        uts_printf_concat(s, "0");
                    }
                }
                else {
                    uts_printf_concat(s, "0");
                }
            }
            else {
                log_warn("bucket is NULL, return 0.");
                uts_printf_concat(s, "0");
            }
        }
        else {
            log_warn("cannot parse bucket and table");
            crabql->error = 34;
        }
    }
};

static void EXPORT_popcount(struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs) {
    ERROR_ASSERT(nargs == 2);
    uts_printf_concat(s, "popcountl(");
    crabql_generate_code(crabql, &o[1], s);
    uts_printf_concat(s, ")");
}

struct op_def ops[] = {
#include "crabql.export.ops.inc"
};

static struct field *eval_field(struct crabql *crabql, msgpack_object *o) {
    
    char field_name[o->via.raw.size + 1];
    memcpy(field_name, o->via.raw.ptr, o->via.raw.size);
    field_name[o->via.raw.size] = '\0';
    
    struct field *field = schema_field_get(crabql->schema, field_name);
    if (field == NULL) {
        crabql->error = 1;
        log_warn("field %s not exist", field_name);
    }
    
    return field;
}

static void data_write(struct eval_context *context, struct field *field, int64_t val) {
    int64_t oldval = bitfield_set(context->data, field->is_signed, field->offset, field->nbits, val);
    if (oldval != val)
        context->modified = 1;
}

/* generate c code from crabql */
static void crabql_generate_code(struct crabql *crabql, msgpack_object *o, UT_string *s) {
    struct field *field;
    if (o == NULL) {
        uts_printf_concat(s, "-1LL");
        return;
    }
    switch (o->type) {
        case MSGPACK_OBJECT_NIL:
            uts_printf_concat(s, "0");
            break;
        case MSGPACK_OBJECT_BOOLEAN:
            uts_printf_concat(s, o->via.boolean ? "-1" : "0");
            break;
        case MSGPACK_OBJECT_POSITIVE_INTEGER:
            uts_printf_concat(s, "%"PRIu64"LL", o->via.u64);
            break;
        case MSGPACK_OBJECT_NEGATIVE_INTEGER:
            uts_printf_concat(s, "%"PRId64"LL", o->via.i64);
            break;
        case MSGPACK_OBJECT_DOUBLE:
            uts_printf_concat(s, "%.18lf", o->via.dec);
            break;
        case MSGPACK_OBJECT_RAW:
            if (mp_raw_eq(*o, "_table")) {
                uts_printf_concat(s, "(context->table)");
                break;
            }
            
            field = eval_field(crabql, o);
            ERROR_CHECK();
            make_field_data_get(field, s);
            break;
        case MSGPACK_OBJECT_ARRAY:
            if (o->via.array.size == 0) {
                uts_printf_concat(s, "0");
            }
            else {
                msgpack_object* p = o->via.array.ptr;
                
                if (p->type == MSGPACK_OBJECT_RAW && p->via.raw.size > 0 && p->via.raw.ptr[0] == '$') { // operator
                    for (size_t i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
                        if (mp_raw_eq(*p, ops[i].name)) {
                            ops[i].func(crabql, p, s, o->via.raw.size);
                            return;
                        }
                    }
                    for (size_t i = 0; i < p->via.raw.size; i++)
                        putchar(p->via.raw.ptr[i]);
                    putchar(10);
                    ERROR_ASSERT(!"unknown operator");
                }
                else { // list
                    uts_printf_concat(s, "(0");
                    msgpack_object* const pend = o->via.array.ptr + o->via.array.size;
                    for(; p < pend; ++p) {
                        uts_printf_concat(s, ", ");
                        crabql_generate_code(crabql, p, s);
                    }
                    uts_printf_concat(s, ")");
                    return;
                }
            }
        case MSGPACK_OBJECT_MAP:
            ERROR_ASSERT(crabql->data_mode != DATA_MODE_NONE);

            msgpack_object_kv* p = o->via.map.ptr;
            msgpack_object_kv* const pend = o->via.map.ptr + o->via.map.size;
            
            uts_printf_concat(s, "(1");
            for(; p < pend; ++p) {
                ERROR_ASSERT(p->key.type == MSGPACK_OBJECT_RAW);
                
                ERROR_ASSERT(crabql->data_mode != DATA_MODE_NONE);
                ERROR_ASSERT(crabql->schema);
                field = eval_field(crabql, &p->key);
                ERROR_ASSERT(field);
                ERROR_CHECK();
                
                if (crabql->data_mode == DATA_MODE_READ) {
                    uts_printf_concat(s, " && (");
                    make_field_data_get(field, s);
                    uts_printf_concat(s, " == ");
                    crabql_generate_code(crabql, &p->val, s);
                    uts_printf_concat(s, ")");
                }
                else if (crabql->data_mode == DATA_MODE_WRITE) { // WRITE
                    uts_printf_concat(s, ", data_write(context, %p, ", field);
                    crabql_generate_code(crabql, &p->val, s);
                    uts_printf_concat(s, ")");
                }
            }
            uts_printf_concat(s, ")");
            return;
        default:
            // FIXME
            ERROR_ASSERT(0);
    }
}

static int64_t popcountl(long x) {
    return __builtin_popcountl(x);
}

const char *code_template = "\
typedef long long int64_t;\n\
typedef unsigned long long uint64_t;\n\
typedef int int32_t;\n\
typedef unsigned int uint32_t;\n\
typedef unsigned long size_t;\n\
%s\
double sin(double x);\n\
double cos(double x);\n\
double tan(double x);\n\
double asin(double x);\n\
double acos(double x);\n\
double atan(double x);\n\
double atan2(double y, double x);\n\
double sqrt(double x);\n\
uint64_t geo_from_latlon(double lat, double lon);\n\
uint32_t geo_dist(uint64_t geo1, uint64_t geo2);\n\
int64_t op_year(int64_t ts);\n\
int64_t op_month(int64_t ts);\n\
int64_t op_day(int64_t ts);\n\
struct eval_context { void *data; int64_t table; };\n\
int64_t crabql_func(struct eval_context *context) { return %s; }";

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

struct symbol {
    const char *name;
    void *func;
};

#define ARRAY_COUNT(array) (sizeof(array) / sizeof((array)[0]))

struct symbol symbols_to_add[] = {
    {"popcountl", popcountl},
    {"pospow", pospow},
    {"bsintable", bsintable},
    {"bs64", bs64},
    {"data_write", data_write},
    {"geo_from_latlon", geo_from_latlon},
    {"geo_dist", geo_dist},
    {"cb_point_in_polygon", cb_point_in_polygon},
    {"cb_point_in_circle", cb_point_in_circle},
    {"op_year", op_year},
    {"op_month", op_month},
    {"op_day", op_day},
};

int crabql_self_test() {
    TCCState *tccs = tcc_new();

    int ret = 0;

    if (tccs == NULL) {
        log_error("cannot create TCCState");
        ret = CRABDB_ERROR_COMPILER_FAILURE;
        goto err;
    }

    tcc_set_output_type(tccs, TCC_OUTPUT_MEMORY);
    if (tcc_compile_string(tccs, "int self_test() { return ~-43; }") != 0) {
        log_error("tcc_compile_string failed");
        ret = CRABDB_ERROR_COMPILER_FAILURE;
        goto err_tcc_delete;
    }

        
    int size = tcc_relocate(tccs, NULL);

    if (size <= 0) {
        log_error("tcc_relocate failed");
        ret = CRABDB_ERROR_COMPILER_FAILURE;
        goto err_tcc_delete;
    }

    void *mem = mmap(NULL, size, PROT_READ | PROT_WRITE | PROT_EXEC, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

    if (mem == MAP_FAILED) {
        log_error("mmap failed");
        ret = CRABDB_ERROR_COMPILER_FAILURE;
        goto err_tcc_delete;
    }

    tcc_relocate(tccs, mem);

    int64_t (*func)() = tcc_get_symbol(tccs, "self_test");

    if (func == NULL) {
        log_error("tcc_get_symbol failed");
        ret = CRABDB_ERROR_COMPILER_FAILURE;
        goto err_munmap;
    }

    if (func() != 42) {
        log_error("program output is not 42");
        ret = CRABDB_ERROR_COMPILER_FAILURE;
        goto err_munmap;
    }

err_munmap:
    munmap(mem, size);

err_tcc_delete:
    tcc_delete(tccs);

err:
    return ret;
}

void crabql_init(struct crabql *crabql, msgpack_object *o, struct schema *schema, enum eval_data_mode data_mode) {
    memset(crabql, 0, sizeof(struct crabql));
    crabql->data_mode = data_mode;
    UT_string *s;
    utstring_new(s);
    crabql->schema = schema;
    crabql->mem = NULL;
    crabql->memsize = 0;
    
    struct timeval tv1, tv2;
    
    if (o) {
        gettimeofday(&tv1, NULL);
        crabql_generate_code(crabql, o, s);
        gettimeofday(&tv2, NULL);

        if (crabql->error == 0) {
            UT_string *code;
            utstring_new(code);
            
            UT_string *vars;
            utstring_new(vars);
            for (uint32_t i = 0; i < crabql->nvar; i++) {
                uts_printf_concat(vars, "uint64_t v%"PRIu32";\n", i);
            }
            
            utstring_printf(code, code_template, utstring_body(vars), utstring_body(s));
            
            utstring_free(vars);
            
            pthread_mutex_lock(&mutex);
            
            gettimeofday(&tv1, NULL);
            
            TCCState *tccs = tcc_new();
            if (tccs) {
                tcc_set_output_type(tccs, TCC_OUTPUT_MEMORY);
                if (tcc_compile_string(tccs, utstring_body(code)) == 0) {
                    for (size_t i = 0; i < ARRAY_COUNT(symbols_to_add); i++)
                        tcc_add_symbol(tccs, symbols_to_add[i].name, symbols_to_add[i].func);
                    
                    int size = tcc_relocate(tccs, NULL);
                    if (size > 0) {
                        crabql->mem = mmap(NULL, size, PROT_READ | PROT_WRITE | PROT_EXEC, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
                        if (crabql->mem != MAP_FAILED) {
                            crabql->memsize = size;
                            tcc_relocate(tccs, crabql->mem);
                            crabql->func = tcc_get_symbol(tccs, "crabql_func");
                            if (crabql->func == NULL) {
                                crabql->error = 4;
                                log_error("tcc_get_symbol failed");
                            }
                        }
                        else {
                            crabql->error = 5;
                            log_error("mmap failed");
                        }
                    }
                    else {
                        crabql->error = 3;
                        log_error("tcc_relocate failed");
                    }
                }
                else {
                    crabql->error = 2;
                    log_error("cannot compile code: [%s]", utstring_body(s));
                }
                tcc_delete(tccs);
            }
            else {
                crabql->error = 1;
                log_error("cannot create TCCState");
            }
            
            gettimeofday(&tv2, NULL);
            
            pthread_mutex_unlock(&mutex);
            utstring_free(code);
        }
        else {
            crabql->func = NULL;
        }
    }
    else {
        crabql->error = 0;
        crabql->func = NULL;
    }
    
    utstring_free(s);
}

void *crabql_get_func(struct crabql *crabql) {
    if (crabql->error == 0 && crabql->func) {
        return crabql->func;
    }
    return NULL;
}

int64_t crabql_exec(struct crabql *crabql, struct eval_context *context) {
    int64_t (*func)(struct eval_context *context) = crabql_get_func(crabql);
    if (func)
        return func(context);
    return -1;
}

void crabql_finish(struct crabql *crabql) {
    if (crabql->mem)
        munmap(crabql->mem, crabql->memsize);
    for (uint32_t i = 0; i < crabql->nlocked; i++)
        pthread_rwlock_unlock(&crabql->locked[i]->rwlock);
}
