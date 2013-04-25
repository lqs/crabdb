#ifndef _BINLOG_H
#define _BINLOG_H

#include "table.h"
#include <msgpack.h>

#define BINLOG_ALIGN 4096
#define BINLOG_FILE_SIZE (16 * 1024 * 1024)

struct binlog {
    uint64_t position;
    uint64_t synced_position;
    uint32_t file_id;
    int fd;
    pthread_t flush_thread;
    pthread_mutex_t mutex;
};

struct binlog_file_header {
    uint64_t zero;
    uint64_t magic;
    uint32_t version;
    uint8_t fill_this_with_zero[BINLOG_ALIGN - 20];
} __attribute__ ((packed));

struct binlog_type_1 {
    uint16_t command;
    char payload[];
} __attribute__ ((packed));

struct binlog_type_2 {
    char bucket_name[64];
    int64_t table_id;
    char data[];
} __attribute__ ((packed));

struct binlog_type_3 {
    uint8_t bucket_name_length;
    int64_t table_id;
    char data[];
} __attribute__ ((packed));

struct binlog_record {
    uint8_t type; // 0:skip this BINLOG_ALIGN 1:command 2:insert/update
    uint8_t reserved;
    uint32_t body_size;
    union {
        struct binlog_type_1 type_1;
        struct binlog_type_2 type_2;
    
    } body[];
} __attribute__ ((packed));

extern struct binlog *binlog;

int binlog_open();
int binlog_replay(const void *data, size_t data_size);
void binlog_save_insert_update(struct binlog *binlog, struct table *table, void *data);
void binlog_save_request(struct request *request);
void binlog_save_data(struct table *table, void *data);
int binlog_raw_read(uint64_t *position, uint64_t *next_position, char **data, size_t *data_size);
int binlog_save_position(struct binlog *binlog, uint64_t position);
int binlog_close(struct binlog *binlog);

int command_get_binlog(struct request *request, msgpack_object o, msgpack_packer *response);
int command_replay_binlog(struct request *request, msgpack_object o, msgpack_packer *response);
int command_binlog_stats(struct request *request, msgpack_object o, msgpack_packer *response);
int command_set_binlog_position(struct request *request, msgpack_object o, msgpack_packer *response);

#endif
