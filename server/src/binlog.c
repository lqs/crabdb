#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <msgpack.h>
#include <inttypes.h>
#include "table.h"
#include "schema.h"
#include "bucket.h"
#include "log.h"
#include "server.h"
#include "binlog.h"
#include "command.h"
#include "utils.h"
#include "error.h"

struct binlog *binlog = NULL;
static int binlog_fsync_interval = 2;

static char zeros[BINLOG_ALIGN] = {0};

const char binlog_file_name[] = "binlog/latest";

static const struct binlog_file_header binlog_file_header = {
    .zero = 0,
    .magic = 0,
    .version = 1,
    .fill_this_with_zero = {0},
};

static void __binlog_filename(char *path, uint32_t file_id) {
    sprintf(path, "binlog/%08"PRIx32, file_id);
}

int __open_binlog_file(uint32_t file_id) {
    char path[32];
    __binlog_filename(path, file_id);
    return open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
}

static int binlog_open_file(struct binlog *binlog, uint32_t file_id) {

    int ret = 0;

    if (binlog->file_id != file_id && binlog->fd >= 0) {
        close(binlog->fd);
        binlog->fd = -1;
    }

    if (binlog->fd < 0) {
        binlog->file_id = file_id;
        binlog->fd = __open_binlog_file(file_id);
        if (binlog->fd < 0) {
            ret = CRABDB_ERROR_IO;
            goto err;
        }
    }

err:
    return ret;
}

int binlog_flush(struct binlog *binlog) {
    pthread_mutex_lock(&binlog->mutex);
    int fd = binlog->fd;
    uint64_t position = binlog->position;
    if (fd >= 0)
        fd = dup(fd);
    pthread_mutex_unlock(&binlog->mutex);

    if (fd >= 0) {
        fsync(fd);
        close(fd);
        binlog_save_position(binlog, position);
    }
    return 0;
}

static void *binlog_flush_thread(void *arg) {
    (void) arg;
    int counter = 0;
    for (;;) {
        if (++counter >= binlog_fsync_interval * 10) {
            counter = 0;
            log_notice("flush binlog");
            struct timeval tv1, tv2;
            
            gettimeofday(&tv1, NULL);
            binlog_flush(binlog);
            gettimeofday(&tv2, NULL);

            // log_notice("flush binlog %.6lf seconds", (double) ((tv2.tv_sec + tv2.tv_usec / 1000000.0) - (tv1.tv_sec + tv1.tv_usec / 1000000.0)));
        }
        if (server->terminated)
            break;
        usleep(100000);
    }
    binlog_flush(binlog);
    return NULL;
}

int binlog_open() {
    unlink("binlog");
    mkdir("binlog", 0755);
    binlog = (struct binlog *) malloc(sizeof(struct binlog));
    memset(binlog, 0, sizeof(*binlog));

    pthread_mutex_init(&binlog->mutex, NULL);


    char buf[20] = {0};
    

    int ret = 0;
    binlog->position = 0;
    binlog->synced_position = 0;
    binlog->fd = -1;

    int fd = open(binlog_file_name, O_RDONLY | O_CLOEXEC);
    if (fd < 0 && errno != ENOENT) {
        ret = CRABDB_ERROR_IO;
        goto err;
    }

    if (fd >= 0 && read(fd, buf, 16) != 16) {
        ret = CRABDB_ERROR_DATA_CORRUPTION;
        goto err;
    }

    if (fd >= 0 && sscanf(buf, "%"SCNx64, &binlog->synced_position) != 1) {
        ret = CRABDB_ERROR_DATA_CORRUPTION;
        goto err;
    }

    // TODO: replay binlog from binlog->synced_position
    binlog->position = binlog->synced_position;

    if (pthread_create(&binlog->flush_thread, NULL, binlog_flush_thread, NULL) != 0) {
        log_error("pthread_create error: %d", errno);
        ret = CRABDB_ERROR_UNKNOWN;
        goto err;
    }

err:
    close(fd);
    return ret;
}

int binlog_close(struct binlog *binlog) {
    log_notice("waiting for binlog thread");
    pthread_join(binlog->flush_thread, NULL);
    log_notice("done");
    return 0;
}

static int binlog_raw_write(struct binlog *binlog, uint64_t position, const char *data, size_t data_size) {

    int ret = 0;

    if (!server->is_slave) {
        log_warn("this is not slave.");
        ret = CRABDB_ERROR_INVALID_ARGUMENT;
        goto err;
    }

    uint32_t file_id = position / BINLOG_FILE_SIZE;
    off_t file_offset = position % BINLOG_FILE_SIZE;

    if (file_offset + data_size > BINLOG_FILE_SIZE) {
        log_error("binlog data beyond BINLOG_FILE_SIZE");
        ret = CRABDB_ERROR_INVALID_ARGUMENT;
        goto err;
    }

    if (binlog->fd >= 0) {
        for (uint32_t span_file_id = binlog->file_id; span_file_id < file_id; span_file_id++) {
            log_notice("truncate file %"PRIu32" to BINLOG_FILE_SIZE", span_file_id);
            int fd = __open_binlog_file(span_file_id);
            if (fd < 0) {
                log_error("open span_file_id span_file_id failed: %d", errno);
                ret = CRABDB_ERROR_IO;
                goto err;
            }
            if (ftruncate(fd, BINLOG_FILE_SIZE) != 0) {
                log_error("ftruncate error: %d", errno);
                ret = CRABDB_ERROR_IO;
                goto err;
            }
            close(fd);
        }
    }

    if ((ret = binlog_open_file(binlog, file_id)) != 0)
        goto err;

    if (pwrite(binlog->fd, data, data_size, file_offset) != (ssize_t) data_size) {
        log_error("pwrite error: %d", errno);
        ret = CRABDB_ERROR_IO;
        goto err;
    }
    
    if (ftruncate(binlog->fd, file_offset + data_size) != 0) {
        log_error("ftruncate error: %d", errno);
        ret = CRABDB_ERROR_IO;
        goto err;
    }

err:
    return ret;
}

int binlog_raw_read(uint64_t *position, uint64_t *next_position, char **data, size_t *data_size) {

    struct binlog_record record;
    ssize_t size;

    uint64_t file_id;
    off_t file_offset;

open_file:
    file_id = *position / BINLOG_FILE_SIZE;

    int fd = __open_binlog_file(file_id);
    int res = 0;
again:
    file_offset = *position % BINLOG_FILE_SIZE;

    if (fd == -1) {
        return CRABDB_ERROR_IO;
    }

    size = pread(fd, &record, sizeof(record), file_offset);

    if (size >= 1) {
        if (record.type == 0) {
            *position += BINLOG_ALIGN - file_offset % BINLOG_ALIGN;

            if (*position / BINLOG_FILE_SIZE != file_id) {
                close(fd);
                goto open_file;
            }

            goto again;
        }
        else if (size != sizeof(record)) {
            log_error("pread header size less than sizeof(record)");
            goto corrupted;
        }
        else {
            if (record.body_size > 512 * 1024) {
                log_error("binlog record body_size %"PRId32" is corrupted.", record.body_size);
                goto corrupted;
            }

            *next_position = *position + sizeof(record) + record.body_size;
            *data_size = sizeof(record) + record.body_size;
            *data = malloc(*data_size);
            memcpy(*data, &record, sizeof(record));
            if (pread(fd, *data + sizeof(record), record.body_size, file_offset + sizeof(record)) != record.body_size) {
                res = 102;
                free(*data);
                goto done;
            }
            goto done;
        }
    }
    else {
        res = 100;
        goto done;
    }

corrupted:
    res = 101;
    log_error("oplog is corrupted at offset %ju.", (uintmax_t) file_offset);
    
done:
    close(fd);
    return res;
}

int binlog_clear(struct binlog *binlog, uint64_t position) {
    // clear binlog files before this
    (void) binlog;
    (void) position;
    log_warn("TODO: binlog_clear()");
    return 0;
}

int binlog_save_position(struct binlog *binlog, uint64_t position) {
    (void) binlog;
    char buf[20];
    const char temp_symlink_name[] = "binlog/latest.temp";
    const char symlink_name[] = "binlog/latest";

    int ret = 0;

    unlink(temp_symlink_name);
    sprintf(buf, "%016"PRIx64"\n", position);

    int fd = open(temp_symlink_name, O_WRONLY | O_CREAT, 0600);
    if (fd < 0) {
        log_error("open error: %d", errno);
        ret = CRABDB_ERROR_IO;
        goto err;
    }

    if (write(fd, buf, 17) != 17) {
        ret = CRABDB_ERROR_IO;
        goto err_close_fd;
    }

    if (rename(temp_symlink_name, symlink_name) != 0) {
        log_error("rename error: %d", errno);
        ret = CRABDB_ERROR_IO;
        goto err_close_fd;
    }

err_close_fd:
    close(fd);
err:
    return ret;
}

int binlog_prepare_file(struct binlog *binlog, size_t record_size) {

    int ret = 0;

    /* open file */
    uint32_t file_id = binlog->position / BINLOG_FILE_SIZE;
    if ((ret = binlog_open_file(binlog, file_id)) != 0)
        goto err;

    off_t file_position = binlog->position % BINLOG_FILE_SIZE;

    /* exceed file size */
    if (file_position + record_size > BINLOG_FILE_SIZE) {
        if (ftruncate(binlog->fd, BINLOG_FILE_SIZE) != 0) {
            log_error("ftruncate error: %d\n", errno);
            ret = CRABDB_ERROR_IO;
            goto err;
        }
        fsync(binlog->fd);
        close(binlog->fd);

        binlog->position += BINLOG_FILE_SIZE - binlog->position % BINLOG_FILE_SIZE;

        uint32_t file_id = binlog->position / BINLOG_FILE_SIZE;

        if ((ret = binlog_open_file(binlog, file_id)) != 0)
            goto err;

        if (ftruncate(binlog->fd, 0) < 0) {
            ret = CRABDB_ERROR_IO;
            goto err;
        }

        file_position = 0;
    }

    /* use first block as header */
    if (file_position < BINLOG_ALIGN) {
        if (pwrite(binlog->fd, &binlog_file_header, sizeof(binlog_file_header), 0) != sizeof(binlog_file_header)) {
            ret = CRABDB_ERROR_IO;
            goto err;
        }
        binlog->position += BINLOG_ALIGN - file_position;
        file_position = BINLOG_ALIGN;
    }

    /* cross block */
    if (record_size <= BINLOG_ALIGN && file_position / BINLOG_ALIGN != (off_t) (file_position + record_size - 1) / BINLOG_ALIGN) {
        ssize_t bytes_left = BINLOG_ALIGN - file_position % BINLOG_ALIGN;
        if (pwrite(binlog->fd, zeros, bytes_left, file_position) != bytes_left) {
            ret = CRABDB_ERROR_IO;
            goto err;
        }
        file_position += bytes_left;
        binlog->position += bytes_left;
    }

err:
    return ret;
}

void binlog_append_data(struct binlog *binlog, struct binlog_record *record, size_t record_size) {

    int ret = 0;

    pthread_mutex_lock(&binlog->mutex);

    if ((ret = binlog_prepare_file(binlog, record_size)) != 0)
        goto err;

    off_t file_position = binlog->position % BINLOG_FILE_SIZE;

    log_notice("binlog %d will write to %d, size %zu", binlog->fd, (int) file_position, record_size);
    if (pwrite(binlog->fd, record, record_size, file_position) != (ssize_t) record_size) {
        log_error("pwrite error %s", strerror(errno));
        ret = CRABDB_ERROR_IO;
        goto err;
    }

    binlog->position += record_size;

err:
    pthread_mutex_unlock(&binlog->mutex);

    if (ret)
        printf("ret = %d\n", ret);
    
}

void binlog_save_request(struct request *request) {

    if (!binlog || !request || server->is_slave)
        return;

    size_t size = sizeof(struct binlog_record) + sizeof(struct binlog_type_1) + request->payload_size;
    struct binlog_record *record = malloc(size);
    memset(record, 0, size);
    log_notice("write binlog type 1, size = %zu", size);
    
    
    record->type = 1;
    record->body_size = sizeof(struct binlog_type_1) + request->payload_size;
    record->body->type_1.command = request->command;
    // record->body->type_1.payload_size = request->payload_size;
    memcpy(record->body->type_1.payload, request->payload, request->payload_size);
    
    binlog_append_data(binlog, record, size);
    
    free(record);    
}

void binlog_save_data(struct table *table, void *data) {

    if (!binlog || !table || !data || server->is_slave)
        return;
        
    struct bucket *bucket = table->bucket;
    struct schema *schema = bucket->schema;
        
    size_t size = sizeof(struct binlog_record) + sizeof(struct binlog_type_2) + ceildiv(schema->nbits, 8);
    
    struct binlog_record *record = malloc(size);
    memset(record, 0, size);
    log_notice("write binlog type 2, size = %zu", size);
    
    record->type = 2;
    record->body_size = size - sizeof(struct binlog_record);
    
    strncpy(record->body->type_2.bucket_name, bucket->name, sizeof(record->body->type_2.bucket_name));
    record->body->type_2.table_id = table->id;
    memcpy(record->body->type_2.data, data, ceildiv(schema->nbits, 8));
    
    binlog_append_data(binlog, record, size);

    free(record);    
}


int binlog_record_replay_1(uint16_t command, void *payload, size_t payload_size) {
    
    int ret = 0;

    msgpack_unpacked msg;
    msgpack_unpacked_init(&msg);

    bool success = msgpack_unpack_next(&msg, payload, payload_size, NULL);
    if (success) {
        msgpack_sbuffer* buffer = msgpack_sbuffer_new();
        msgpack_packer* pk = msgpack_packer_new(buffer, msgpack_sbuffer_write);
        
        ret = execute_command(NULL, command, msg.data, pk);
        
        msgpack_unpacked_destroy(&msg);
        msgpack_packer_free(pk);
        
        msgpack_sbuffer_free(buffer);
    }
    else {
        log_warn("invalid binlog record");
    }
    return ret;
}

int binlog_record_replay_2(const char *bucket_name, int64_t table_id, void *data) {
    
    int ret = 0;

    struct bucket *bucket = bucket_get(bucket_name, 0);

    struct schema *schema = bucket->schema;

    if (schema == NULL)
        return CRABDB_ERROR_DATA_CORRUPTION;

    struct table *table = bucket_get_table(bucket, table_id, 0);
    
    // record_dump(schema, data);
    
    table_insert_nolock(table, schema, data, 1);

    table_write(table);

    return ret;
}

int binlog_replay(const void *data, size_t data_size) {
    struct binlog_record *record = (struct binlog_record *) data;
    if (data_size < sizeof(struct binlog_record))
        return CRABDB_ERROR_INVALID_ARGUMENT;
    if (data_size != sizeof(struct binlog_record) + record->body_size)
        return CRABDB_ERROR_INVALID_ARGUMENT;
    
    switch (record->type) {
        case 1:
            return binlog_record_replay_1(record->body->type_1.command, record->body->type_1.payload, record->body_size - sizeof(struct binlog_type_1));
        case 2:
            return binlog_record_replay_2(record->body->type_2.bucket_name, record->body->type_2.table_id, record->body->type_2.data);
        default:
            log_warn("unknown binlog record type %d", record->type);
            return CRABDB_ERROR_DATA_CORRUPTION;
    }
}


void binlog_replay_all(const char *path) {
    (void) path;
    
    /* TODO: refactor this */
}

static int binlog_truncate_after(struct binlog *binlog) {

    binlog_prepare_file(binlog, 0);

    int ret = 0;
    off_t file_offset = binlog->position % BINLOG_FILE_SIZE;
    if (ftruncate(binlog->fd, file_offset) != 0) {
        ret = CRABDB_ERROR_IO;
        goto err;
    }

    uint32_t file_id = binlog->file_id;
    while (1) {
        char path[32];
        __binlog_filename(path, ++file_id);
        if (unlink(path) != 0) {
            if (errno == ENOENT)
                break;
            log_error("unlink error: %d", errno);
            ret = CRABDB_ERROR_IO;
            goto err;
        }
    }

err:
    return ret;
}


struct binlog_item {
    uint64_t position;
    char *data;
    size_t data_size;
};

int command_get_binlog(struct request *request, msgpack_object o, msgpack_packer *response) {

    (void) request;
    (void) o;
    (void) response;

    uint64_t position = 0;

    if (o.type == MSGPACK_OBJECT_MAP) {
        mp_for_each_in_kv(p, o) {
            if (mp_raw_eq(p->key, "position"))
                position = p->val.via.u64;
        }
    }

    struct binlog_item binlog_item[512];
    size_t binlog_item_count = 0;
    uint64_t next_position = position;
    int ret = 0;

    while (binlog_item_count < sizeof(binlog_item) / sizeof(binlog_item[0])) {
        struct binlog_item *current = &binlog_item[binlog_item_count];
        current->position = next_position;
        ret = binlog_raw_read(&current->position, &next_position, &current->data, &current->data_size);
        if (ret != 0) {
            if (binlog_item_count > 0) {
                ret = 0;
                break;
            }
            goto err;
        }
        binlog_item_count++;
    }

    msgpack_pack_map(response, 2);

    mp_pack_string(response, "items");
    msgpack_pack_array(response, binlog_item_count);

    for (size_t i = 0; i < binlog_item_count; i++) {
        msgpack_pack_map(response, 2);

        mp_pack_string(response, "position");
        msgpack_pack_long(response, binlog_item[i].position);

        mp_pack_string(response, "data");
        msgpack_pack_raw(response, binlog_item[i].data_size);
        msgpack_pack_raw_body(response, binlog_item[i].data, binlog_item[i].data_size);
    }

    mp_pack_string(response, "next_position");
    msgpack_pack_long(response, next_position);



err:
    for (size_t i = 0; i < binlog_item_count; i++)
        free(binlog_item[i].data);

    return ret;
}

int command_replay_binlog(struct request *request, msgpack_object o, msgpack_packer *response) {
    (void) request;
    (void) response;

    uint64_t position = -1;
    const void *data = NULL;
    size_t data_size = 0;

    int ret = 0;

    if (o.type != MSGPACK_OBJECT_MAP) {
        ret = CRABDB_ERROR_INVALID_ARGUMENT;
        goto err;
    }

    mp_for_each_in_kv(p, o) {
        if (mp_raw_eq(p->key, "position"))
            position = p->val.via.u64;
        if (mp_raw_eq(p->key, "data")) {
            data = p->val.via.raw.ptr;
            data_size = p->val.via.raw.size;
        }
    }

    if (data == NULL || position == (uint64_t) -1)
        return CRABDB_ERROR_INVALID_ARGUMENT;

    pthread_mutex_lock(&binlog->mutex);

    if ((ret = binlog_raw_write(binlog, position, data, data_size)) != 0)
        goto err_unlock;
    if ((ret = binlog_replay(data, data_size)) != 0)
        goto err_unlock;

    binlog->position = position + data_size;

    binlog_truncate_after(binlog);

err_unlock:
    pthread_mutex_unlock(&binlog->mutex);

err:
    return ret;
}

int command_binlog_stats(struct request *request, msgpack_object o, msgpack_packer *response) {
    (void) request;
    (void) response;
    (void) o;

    msgpack_pack_map(response, 2);

    pthread_mutex_lock(&binlog->mutex);

    mp_pack_string(response, "position");
    msgpack_pack_long(response, binlog->position);
    mp_pack_string(response, "synced_position");
    msgpack_pack_long(response, binlog->synced_position);

    pthread_mutex_unlock(&binlog->mutex);

    return 0;
}

int command_set_binlog_position(struct request *request, msgpack_object o, msgpack_packer *response) {
    (void) request;
    (void) response;

    int ret = 0;

    uint64_t position = 0;

    if (o.type != MSGPACK_OBJECT_MAP) {
        ret = CRABDB_ERROR_INVALID_ARGUMENT;
        goto err;
    }

    mp_for_each_in_kv(p, o) {
        if (mp_raw_eq(p->key, "position"))
            position = p->val.via.u64;
    }

    pthread_mutex_lock(&binlog->mutex);

    if (binlog->position == 0)
        binlog->position = position;

    pthread_mutex_unlock(&binlog->mutex);
err:
    return ret;
}
