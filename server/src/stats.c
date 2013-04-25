#include <stdint.h>
#include "stats.h"

uint64_t stats[STAT_MAX] = {0};

const char *stat_name[STAT_MAX] = {
    "num_commands",
    "num_scans",
    "num_disk_reads",
    "num_disk_writes",
};

void stat_add(int key, uint64_t value) {
    __sync_add_and_fetch(&stats[key], value);
}
