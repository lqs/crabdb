#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <inttypes.h>
#include "table.h"

int main(int argc, char *argv[]) {
    size_t block_size = 128;
    
    int fd = open(argv[1], O_RDONLY);
    off_t offset = lseek(fd, 800024, SEEK_SET);
    
    struct table_data *table_data = malloc(block_size);
    while (memset(table_data, 0, block_size), read(fd, table_data, block_size) == block_size) {
    
    
    // int64_t table_id;
    // uint64_t schema_hash;
    // uint32_t crc32;
    // uint32_t timestamp;
    // uint32_t size;
    
        printf("--- OFFSET = %zu\n", offset);
        printf("table_id = %"PRId64" schema_hash=%"PRIx64"\n", table_data->table_id, table_data->schema_hash);
        printf("-------\n");
        
        offset += block_size;
    }
}
