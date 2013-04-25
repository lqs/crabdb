#ifndef _STATS_H
#define _STATS_H
enum stats_key {
    NUM_COMMANDS,
    NUM_SCANS,
    NUM_DISK_READS,
    NUM_DISK_WRITES,
    STAT_MAX,
};

extern const char *stat_name[STAT_MAX];

extern uint64_t stats[STAT_MAX];

void stat_add(int key, uint64_t value);
#endif
