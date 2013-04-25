#ifndef SCHEMA_H
#define SCHEMA_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include "uthash.h"

#include "bitfield.h"

struct field {
    char name[30];
    bool is_signed;
    bool is_primary_key;
    int16_t offset;
    uint16_t nbits;
};

struct schema {
    uint64_t hash;
    uint64_t upgrade_to;
    uint16_t nbits;
    uint16_t nfields;
    int16_t primary_key_index;
    struct field fields[64];
    UT_hash_handle hh;
};

struct schema *schema_create();
struct field *schema_field_get(struct schema *schema, const char *name);
int schema_field_add(struct schema *schema, const char *name, bool is_signed, uint8_t nbits, bool is_primary_key);
struct field *schema_field_get_primary(struct schema *schema);


static inline int64_t field_data_get(struct field *field, void *data) {
    return bitfield_get(data, field->is_signed, field->offset, field->nbits);
}

static inline int64_t field_data_set(struct field *field, void *data, int64_t val) {
    return bitfield_set(data, field->is_signed, field->offset, field->nbits, val);
}

static inline int schema_compare_record(struct schema *schema, void *left, void *right) {
    struct field *field = schema_field_get_primary(schema);
    int64_t v1 = field_data_get(field, left);
    int64_t v2 = field_data_get(field, right);
    if (v1 < v2)
        return -1;
    if (v1 > v2)
        return 1;
    return 0;
}
void record_dump(struct schema *schema, void *data);
void schema_save_to_file(struct schema *schema, const char *bucket_name);
void schema_calc_hash(struct schema *schema);
struct schema *schema_get_latest(const char *bucket_name);
struct schema *schema_get_from_hash(uint64_t hash, const char *bucket_name);
bool schema_is_equal(struct schema *schema1, struct schema *schema2);
bool schema_can_upgrade(struct schema *old_schema, struct schema *new_schema);

#endif
