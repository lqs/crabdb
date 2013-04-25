#include "binlog.h"
#include <msgpack.h>

int main() {
    binlog_open("binlog");
    
    struct binlog_record *record = malloc(10000000);
    
    printf("%d\n", binlog->fd);
    
    size_t pos = 0;
    
    ssize_t r;

    while (r = read(binlog->fd, record, sizeof(struct binlog_record)), r > 0) {
        
        pos += read(binlog->fd, record->payload, record->size);
        
        printf("#%llu: ", record->seq);
        msgpack_unpacked msg;
        msgpack_unpacked_init(&msg);

        bool success = msgpack_unpack_next(&msg, record->payload, record->size, NULL);
        
        msgpack_object_print(stdout, msg.data);
        putchar('\n');
        
    }
}
