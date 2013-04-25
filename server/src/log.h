#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#ifndef _LOG_H_
#define _LOG_H_

#define LOG_LEVEL_DEBUG   0x1
#define LOG_LEVEL_WARN    0x2
#define LOG_LEVEL_ERROR   0x4
#define LOG_LEVEL_NOTICE  0x8

#define log_debug(fmt, ...) log_mesg(__FILE__, __func__, __LINE__, LOG_LEVEL_DEBUG, fmt, ##__VA_ARGS__)
#define log_notice(fmt, ...) log_mesg(__FILE__, __func__, __LINE__, LOG_LEVEL_NOTICE, fmt, ##__VA_ARGS__)
#define log_warn(fmt, ...)  log_mesg(__FILE__, __func__, __LINE__, LOG_LEVEL_WARN, fmt, ##__VA_ARGS__)
#define log_error(fmt, ...) log_mesg(__FILE__, __func__, __LINE__, LOG_LEVEL_ERROR, fmt, ##__VA_ARGS__)

int log_open(const char *path);
int log_mesg(const char *codefile, const char *func, int linenum, uint32_t mesg_level, const char *fmt, ...) __attribute__ ((format (printf, 5, 6)));
int log_close();
FILE* log_file();
const char *get_time_string(char *string); /* string buffer should be >= 20 */
extern int log_fd;

#endif /* ! _LOG_H_ */
