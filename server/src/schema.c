#include <stdio.h>
#include "schema.h"
#include <string.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>
#include <fcntl.h>
#include "log.h"
#include "utils.h"
#include "slab.h"

struct schema *schema_create(void) {
    struct schema *schema = slab_alloc(sizeof(struct schema));
    if (!schema) {
        log_error("can't alloc schema");
        return NULL;
    }

    memset(schema, 0, sizeof(struct schema));
    schema->primary_key_index = -1;
    log_notice("created a new schema: %p", schema);
    return schema;
}

static struct schema *schemas = NULL;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static char *next_token(char **saveptr) {
    return strtok_r(NULL, " \n\r\t", saveptr);
}

/*
 * returns a pointer to a schema, or NULL if fails
 */
static struct schema *schema_create_from_hash(uint64_t hash, const char *bucket_name) {
    struct schema *schema = NULL;
    char buf[1024];
    snprintf(buf, sizeof(buf), "%s/schemas/%016"PRIx64, bucket_name, hash);
    log_notice("read schema from file [%s]", buf);
    FILE *f = fopen(buf, "r");
    if (f == NULL) {
        // legacy file path
        snprintf(buf, sizeof(buf), "schemas/%016"PRIx64, hash);        
        log_notice("read schema from file [%s]", buf);
        f = fopen(buf, "r");
    }

    if (f == NULL) {
        log_notice("read schema failed");
        return NULL;
    }
    
    schema = schema_create();
    if (!schema) {
        log_error("can't create schema");
        goto fail;
    }

    while (fgets(buf, sizeof(buf), f)) {
        char *saveptr;
        char *token = strtok_r(buf, " \n\r\t", &saveptr);
        
        uint8_t is_primary_key = 0;
        uint8_t is_signed = 0;
        char *name = NULL;
        uint16_t nbits = 0;
                
        if (strcmp(token, "upgrade_to") == 0) {
            token = next_token(&saveptr);
            if (token == NULL)
                goto fail;
            if (sscanf(token, "%"SCNx64, &schema->upgrade_to) != 1)
                goto fail;
            if (schema->upgrade_to == 0)
                goto fail;
        }
        else {

            /* backward compat */
            if (strcmp(token, "field") == 0)
                token = next_token(&saveptr);

            while (token) {
                if (token[0] >= '1' && token[1] <= '9')
                    nbits = atoi(token);
                else if (strcmp(token, "primary_key") == 0)
                    is_primary_key = 1;
                else if (strcmp(token, "signed") == 0)
                    is_signed = 1;
                else if (token[0] == '[') { // field name
                    size_t len = strlen(token);
                    if (token[len - 1] == ']') {
                        token[len - 1] = '\0';
                        name = &token[1];
                        if (!is_valid_name(name))
                            goto fail;
                    }
                    else
                        goto fail;
                }
                else
                    goto fail;
                
                token = next_token(&saveptr);
            }

            int res = schema_field_add(schema, name, is_signed, nbits, is_primary_key);
            if (res != 0)
                goto fail;
        }
    }
    fclose(f);
    schema->hash = hash;
    return schema;

fail:
    fclose(f);
    if (schema)
        slab_free(schema);
    log_error("schema file %016"PRIx64" is invalid", hash);
    return NULL;
}

struct schema *schema_get_from_hash(uint64_t hash, const char *bucket_name) {
    struct schema *schema;
    pthread_mutex_lock(&mutex);
    HASH_FIND_INT64(schemas, &hash, schema);
    if (schema == NULL) {
        schema = schema_create_from_hash(hash, bucket_name);
        if (schema)
            HASH_ADD_INT64(schemas, hash, schema);
    }
    pthread_mutex_unlock(&mutex);
    return schema;
}

/*
 * returns a pointer to a schema by a bucket name
 * returns NULL if fails
 */
struct schema *schema_get_latest(const char *bucket_name) {
    char buf[1024];
    char linkto[1024];
    snprintf(buf, sizeof(buf), "%s/schema", bucket_name);
    ssize_t res = readlink(buf, linkto, sizeof(linkto) - 1);
    if (res == -1) {
        /* fallback */
        snprintf(buf, sizeof(buf), "%s.schema", bucket_name);
        res = readlink(buf, linkto, sizeof(linkto) - 1);
    }
    if (res == -1) {
        log_warn("unable to read schema file for bucket %s", bucket_name);
        return NULL;
    }
    linkto[res] = '\0';
    
    // schemas/a0794652a8348ac8
    
    uint64_t hash;
    if (res != 24 || sscanf(linkto, "schemas/%"SCNx64, &hash) != 1) {
        log_error("schema symlink is invalid");
        return NULL;
    }

    return schema_get_from_hash(hash, bucket_name);
}

/*
 * save schema info to file
 * check log when you fucked up
 */
void schema_save_to_file(struct schema *schema, const char *bucket_name) {
    char buf[512], buf_temp[512];
    
    snprintf(buf, sizeof(buf), "%s/schemas", bucket_name);
    if (mkdir(buf, 0755) != 0) {
        log_error("mkdir %s failed: %s", buf, strerror(errno));
        return;
    }

    snprintf(buf, sizeof(buf), "%s/schemas/%016"PRIx64, bucket_name, schema->hash);
    snprintf(buf_temp, sizeof(buf_temp), "%s/schemas/%016"PRIx64".writing", bucket_name, schema->hash);

    FILE *f = fopen(buf_temp, "w");
    if (!f) {
        /* TODO: what about the directory that just mkdired? */
        log_error("fopen %s failed: %s", buf_temp, strerror(errno));
        return;
    }
    
    if (schema->upgrade_to)
        fprintf(f, "upgrade_to %016"PRIx64"\n", schema->upgrade_to);

    for (size_t i = 0; i < schema->nfields; i++) {
        struct field *field = &schema->fields[i];
        fprintf(f, "field %2"PRIu16, field->nbits);

        if (field->name[0])
            fprintf(f, " [%s]", field->name);
        if (field->is_signed)
            fputs(" signed", f);
        if (field->is_primary_key)
            fputs(" primary_key", f);
        fputc('\n', f);
    }
    
    fclose(f);

    if (rename(buf_temp, buf) != 0) {
        log_error("rename %s to %s failed: %s", buf_temp, buf, strerror(errno));
    }
}

bool schema_is_equal(struct schema *schema1, struct schema *schema2) {
    if (schema1 == NULL && schema2 == NULL)
        return 1;

    if (schema1 == NULL || schema2 == NULL)
        return 0;

    if (schema1->nbits != schema2->nbits || schema1->nfields > schema2->nfields)
        return 0;
        
    if (schema1->primary_key_index != schema2->primary_key_index)
        return 0;
    
    if (memcmp(schema1->fields, schema2->fields, sizeof(schema1->fields)) != 0)
        return 0;

    return 1;
}

bool schema_can_upgrade(struct schema *old_schema, struct schema *new_schema) {
    if (new_schema == NULL)
        return 0;

    if (old_schema == NULL)
        return 1;

    if (old_schema->nfields > new_schema->nfields)
        return 0;

    for (uint16_t i = 0; i < old_schema->nfields; i++) {
        struct field *field = &old_schema->fields[i];
        if (schema_field_get(new_schema, field->name) == NULL)
            return 0;
    }

    struct field *pk_old = &old_schema->fields[old_schema->primary_key_index];
    struct field *pk_new = &new_schema->fields[new_schema->primary_key_index];

    if (strcmp(pk_old->name, pk_new->name) != 0)
        return 0;

    if (pk_old->nbits > pk_new->nbits)
        return 0;

    if (pk_old->is_signed && !pk_new->is_signed)
        return 0;

    if (!pk_old->is_signed && pk_new->is_signed && pk_old->nbits == pk_new->nbits)
        return 0;

    return 1;
}

struct field *schema_field_get(struct schema *schema, const char *name) {
    if (schema) {
        for (int i = 0; i < schema->nfields; i++) {
            if (strcmp(schema->fields[i].name, name) == 0) {
                return &schema->fields[i];
            }
        }
    }
    return NULL;
}

struct field *schema_field_get_primary(struct schema *schema) {
    if (schema->primary_key_index == -1)
        return NULL;
    return &schema->fields[schema->primary_key_index];
}

void schema_calc_hash(struct schema *schema) {
    // TODO: calculate hash
    schema->hash = randu64();
}

int schema_field_add(struct schema *schema, const char *name, bool is_signed, uint8_t nbits, bool is_primary_key) {
    if (schema->nfields == sizeof(schema->fields) / sizeof(schema->fields[0]))
        return -1;
        
    if (nbits > 64)
        return -5;
        
    if (nbits == 64 && is_signed)
        return -6;
        
    if (schema_field_get(schema, name))
        return -2;
    
    if (!is_valid_name(name))
        return -8;
    
    size_t name_len = strlen(name);
    struct field *field = &schema->fields[schema->nfields];
    
    if (name_len + 1 > sizeof(field->name))
        return -3;
    
    strcpy(field->name, name);
    field->is_signed = is_signed;
    field->offset = schema->nbits;
    field->nbits = nbits;
    
    if (is_primary_key && schema->primary_key_index != -1)
        return -7;
    
    field->is_primary_key = is_primary_key;
    if (is_primary_key)
        schema->primary_key_index = schema->nfields;
    
    schema->nbits += nbits;
    schema->nfields++;
    
    return 0;
}

void record_dump(struct schema *schema, void *data) {
    printf("{\n");
    for (size_t i = 0; i < schema->nfields; i++)
        printf("    %s: %ld\n", schema->fields[i].name, field_data_get(&schema->fields[i], data));
    printf("}\n");
}
