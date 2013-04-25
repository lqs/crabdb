#include <sys/syscall.h>
#include "log.h"

static pthread_mutex_t lock;
int log_fd = -1;

int
log_open(const char *path)
{
    struct stat sb;

    pthread_mutex_init(&lock, NULL);
    if (stat(path, &sb) == 0)
    {
        if (S_ISREG(sb.st_mode) || S_ISCHR(sb.st_mode))
            log_fd = open(path, O_APPEND|O_RDWR|O_CLOEXEC);
        else
            return -1;
    }
    else
    {
        log_fd = open(path, O_RDWR|O_CREAT|O_APPEND|O_CLOEXEC, S_IRUSR|S_IWUSR|S_IROTH|S_IRGRP);
    }

    if (log_fd >= 0)
        return 0;
    else
        return -1;
}

int
log_mesg(const char *codefile, const char *func, int linenum, uint32_t mesg_level, const char *fmt, ...)
{
    char line[BUFSIZ];
    va_list ap;
    char *level;
    time_t now;
    struct tm t;
    char time_string[30];
    int printed_len;
    int process_id;

    switch (mesg_level)
    {
    case LOG_LEVEL_DEBUG:
        return 0;
        level = "debug";
        break;
    case LOG_LEVEL_WARN:
        level = "warn ";
        break;
    case LOG_LEVEL_ERROR:
        level = "error";
        break;
    case LOG_LEVEL_NOTICE:
        level = "info ";
        break;
    default:
        level = "what's this";
        break;
    }
#ifdef __NR_gettid
    process_id = (int)syscall(__NR_gettid);
#else
    process_id = (int)getpid();
#endif
    now = time(NULL);
    localtime_r(&now, &t);
    snprintf(time_string, sizeof time_string, "%d-%02d-%02d %02d:%02d:%02d", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
    va_start(ap, fmt);
    printed_len = snprintf(line, sizeof(line), "[%s] [%s] (#%d %s %s:%d) ", time_string, level, process_id, func, codefile, linenum);
    printed_len += vsnprintf(line + printed_len, sizeof(line) - printed_len, fmt, ap); /* TODO: buffer may be not enough */
    va_end(ap);
    if (printed_len > BUFSIZ - 2)
        printed_len = BUFSIZ - 2;
    line[ printed_len ] = '\n';
    line[ printed_len + 1 ] = '\0';
    pthread_mutex_lock(&lock);
    ssize_t writesize = write(log_fd, line, strlen(line));
    (void) writesize;
    pthread_mutex_unlock(&lock);

    return 0;
}

int
log_close()
{
    pthread_mutex_destroy(&lock);
    close(log_fd);
    log_fd = -1;
    return 0;
}

FILE*
log_file()
{
    return fdopen(log_fd, "w+");
}

const char *
get_time_string(char *string)
{
    time_t now;
    struct tm t;

    now = time(NULL);
    localtime_r(&now, &t);
    snprintf(string, 20, "%d-%02d-%02d %02d:%02d:%02d", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
    string[19] = '\0';

    return string;
}

/* vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker: */
