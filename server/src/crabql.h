#ifndef _CRABQL_H
#define _CRABQL_H

#include "schema.h"
#include "bitfield.h"
#include <stdarg.h>
#include "utstring.h"

#include <msgpack.h>
#include <dlfcn.h>
#include <libtcc.h>

#ifndef _MSC_VER
#include <inttypes.h>
#else
#ifndef PRIu64
#define PRIu64 "I64u"
#endif
#ifndef PRIi64
#define PRIi64 "I64d"
#endif
#endif

enum eval_data_mode {
    DATA_MODE_NONE = 0,
    DATA_MODE_READ,
    DATA_MODE_WRITE,
};

struct eval_context {
    void *data;
    int64_t table;
    int error;
    char error_message[128];
    bool modified;
};

struct crabql {
    int64_t (*func)(struct eval_context *context);
    void *mem;
    size_t memsize;
    struct table *locked[128];
    uint32_t nlocked;
    uint32_t nvar;
    enum eval_data_mode data_mode;
    struct schema *schema;
    TCCState *tccs;
    int error;
    char error_message[128];
};

#define eval_context_init(context) memset(context, 0, sizeof(struct eval_context))

typedef void (*op_func_t) (struct crabql *crabql, msgpack_object *o, UT_string *s, int nargs);
// struct field *eval_field(struct schema *schema, msgpack_object *o);

typedef int64_t (*crabql_func_t)(struct eval_context *context);

void crabql_init(struct crabql *crabql, msgpack_object *o, struct schema *schema, enum eval_data_mode data_mode);
void *crabql_get_func(struct crabql *crabql);
int64_t crabql_exec(struct crabql *crabql, struct eval_context *context);
void crabql_finish(struct crabql *crabql);
int crabql_self_test();

struct op_def {
    char *name;
    op_func_t func;
};

#endif
