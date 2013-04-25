#include <stdbool.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

bool is_valid_name(const char *name) {
    int len = 0;
    while (*name) {
        char ch = *name;
        if (++len < 32 && ((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch == '_'))) {
            name++;
            continue;
        }
        return false;
    }
    return len > 0 && name[0] != '_';
}

uint64_t randu64() {
    uint64_t result;
    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    ssize_t readsize = read(fd, &result, sizeof(result));
    (void) readsize;
    close(fd);
    return result;
}
