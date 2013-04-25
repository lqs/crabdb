#ifndef _UTILS_H
#define _UTILS_H

#include <msgpack.h>
#include "uthash.h"
#include "utstring.h"
#define max(a, b) ((a) > (b) ? (a) : (b))
#define min(a, b) ((a) < (b) ? (a) : (b))
#define ceildiv(a, b) ((((a) + (b) - 1) / (b)))
#define HEX "0123456789abcdef"

#define _swap(x, y, t) do { \
    t temp; \
    (temp) = (x); \
    (x) = (y); \
    (y) = (temp); \
} while (0)

#define swap(x, y) _swap(*(x), *(y), typeof(*(x)))

static inline char *strncpynul(char *dest, const char *src, size_t n) {
    strncpy(dest, src, n);
    if (n > 0)
        dest[n - 1] = '\0';
    return dest;
}

static inline int mp_raw_cmp(msgpack_object obj, const char *s) {
    ssize_t slen = strlen(s);
    int cmpres = memcmp(obj.via.raw.ptr, s, obj.via.raw.size);
    if (cmpres == 0)
        return obj.via.raw.size - slen;
    return cmpres;
}

static inline int mp_raw_eq(msgpack_object obj, const char *s) {
    size_t slen = strlen(s);
    return slen == obj.via.raw.size && memcmp(s, obj.via.raw.ptr, obj.via.raw.size) == 0;
}

static inline void mp_raw_strcpy(char *dst, msgpack_object obj, size_t dstsize) {
    if (dstsize == 0)
        return;
    size_t size = min(dstsize - 1, obj.via.raw.size);
    memcpy(dst, obj.via.raw.ptr, size);
    dst[size] = '\0';
}

static inline size_t mp_pack_string(msgpack_packer *response, const char *s) {
    size_t len = strlen(s);
    msgpack_pack_raw(response, len);
    msgpack_pack_raw_body(response, s, len);
    return len;
}

static inline void mp_pack_bool(msgpack_packer *response, bool val) {
    if (val)
        msgpack_pack_true(response);
    else
        msgpack_pack_false(response);
}

#define mp_for_each_in_kv(p, o) for (msgpack_object_kv *(p) = (o).via.map.ptr; (p) < (o).via.map.ptr + (o).via.map.size; (p)++)

static inline void uts_printf_concat(UT_string *s, const char *fmt, ...) {
    UT_string *t;
    utstring_new(t);
    
    va_list ap;
    va_start(ap,fmt);
    utstring_printf_va(t, fmt, ap);
    va_end(ap);

    utstring_concat(s, t);
    utstring_free(t);
}

#define RETRY_FOREVER_IF(expr) do {                                        \
    while (expr) {                                                      \
        log_error("retrying [%s]", #expr);                                 \
        sleep(1);                                                          \
    }                                                                      \
} while (0)

// static inline void __mp_kv_if_k_eq_raw_strcpy(msgpack_object_kv *kv, const char *k, char *dst, int dstsize) {
    // if (mp_raw_eq(kv->key, k))
        // mp_raw_strcpy(dst, kv->val, dstsize);
// }

// static inline void __mp_kv_if_k_eq_int_set(msgpack_object_kv *kv, const char *k, long *dst) {
    // if (mp_raw_eq(kv->key, k))
        // *dst = kv-val.via.i64;
// }


// #define mp_kv_if_k_eq_raw_strcpy(kv, dst) __mp_kv_if_k_eq_raw_strcpy(kv, #dst, dst, sizeof(dst))
// #define mp_kv_if_k_eq_int_set(kv, dst) __mp_kv_if_k_eq_int_set(kv, #dst, &dst)

uint64_t randu64();
bool is_valid_name(const char *name);

#define HASH_FIND_INT64(head,findint,out)                                          \
    HASH_FIND(hh,head,findint,8,out)
#define HASH_ADD_INT64(head,intfield,add)                                          \
    HASH_ADD(hh,head,intfield,8,add)

#endif
