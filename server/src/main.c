#include "schema.h"
#include "table.h"
#include <stdio.h>
#include <dlfcn.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pwd.h>
#include "bucket.h"
#include "server.h"
#include "log.h"
#include "storage.h"
#include "table.h"
#include "binlog.h"
#include "slab.h"
#include "queue.h"
#include "geo.h"
#include "error.h"
#include "crabql.h"

#define VERSION "0.2~beta1"

static int port = DEFAULT_PORT;
static char *logfile = "/dev/stderr";
static char *pidfile = NULL;
static char *datadir = NULL;
static char *username = NULL;

static struct host_port *redirection = NULL;

static void print_help(const char *exename) {
    fprintf(stderr, "CrabDB version %s\nUsage: %s [options...]\n"
                    "  -h, --help                         show this help messages\n"
                    "      --self-test                    do a self-test and exit\n"
                    "  -d, --daemon                       run in the background (as a daemon)\n"
                    "  -p, --port=PORT                    specify the port to listen\n"
                    "  -l, --log=FILENAME                 output log files to FILENAME\n"
                    "  -P, --pidfile=PIDFILE              save pid to PIDFILE\n"
            /*1*/   "      --datadir=DIRECTORY            store all data in DIRECTORY\n"
                    "  -u, --uid=UID                      run as this user\n"
            /*2*/   "      --redirect=NEWHOST[:NEWPORT]   redirect all requests to NEWHOST:NEWPORT.\n"
            /*3*/   "      --slave                        run as a slave.\n"
    , VERSION, exename);
    exit(0);
}

static int do_help = 0;
static int do_daemon = 0;

static bool is_slave = false;
static bool self_test_only = false;

static struct host_port *parse_host_port(char *arg)
{
    printf("will parse: [%s]\n", arg);
    char *p = arg;

    char *host = NULL;
    uint16_t port = DEFAULT_PORT;

    int state = 0;
    while (1) {
        if (state == 0) {
            if (*p == '\0') {
                host = strdup(arg);
                break;
            }
            else if (*p == ':') {
                *p = '\0';
                host = strdup(arg);
                *p = ':';
                port = 0;
                state = 1;
            }
        }
        else if (state == 1) {
            if (*p == '\0') {
                break;
            }
            if (*p >= '0' && *p <= '9') {
                if (port > UINT16_MAX / 10)
                    goto fail;
                port *= 10;
                if (port > UINT16_MAX - (*p - '0')) // overflow
                    goto fail;
                port += *p - '0';
            }
            else {
                goto fail;
            }
        }
        p++;
    }

    if (host == NULL || port == 0)
        goto fail;

    struct host_port *host_port = malloc(sizeof(struct host_port));
    host_port->host = host;
    host_port->port = port;
    return host_port;

fail:
    free(host);
    return NULL;
}


static void parse_opt(int argc, char *argv[])
{
    int c;

    while (1) {
        static struct option long_options[] = {
            {"help",     no_argument,        0, 'h'},
            {"self-test",no_argument,        0, 4},
            {"daemon",   no_argument,        0, 'd'},
            {"port",     required_argument,  0, 'p'},
            {"log",      required_argument,  0, 'l'},
            {"pidfile",  required_argument,  0, 'P'},
            {"datadir",  required_argument,  0, 1},
            {"uid",      required_argument,  0, 'u'},
            {"redirect", required_argument,  0, 2},
            {"slave",    no_argument,        0, 3},
            {0, 0, 0, 0}
        };
        /* getopt_long stores the option index here. */
        int option_index = 0;

        c = getopt_long(argc, argv, "hdp:l:P:", long_options, &option_index);

        /* Detect the end of the options. */
        if (c == -1)
            break;

        switch (c) {
            case 1:
                datadir = strdup(optarg);
                break;
            case 2:
                redirection = parse_host_port(optarg);
                if (redirection == NULL) {
                    fprintf(stderr, "invalid redirection target.\n");
                    exit(1);
                }
                break;
            case 3:
                is_slave = true;
                break;
            case 4:
                self_test_only = true;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'l':
                logfile = strdup(optarg);
                break;
            case 'P':
                pidfile = strdup(optarg);
                break;
            case 'd':
                do_daemon = 1;
                break;
            case 'u':
                username = strdup(optarg);
                break;
            default:
                print_help(argv[0]);
                exit(1);
        }
    }
}

static void signal_handler(int sig) {
    log_notice("received signal %d", sig);
    switch (sig) {
        case SIGSEGV:
            printf("SIGSEGV\n");
            break;
        case SIGHUP:
            // restart = 1;
            // the log file can't be open after setuid()
        case SIGINT:
        case SIGTERM:
        default:
            server->terminated = 1;
    }
    
}

#if 0
static void crash_handler(int sig) {
    log_notice("received signal %d", sig);
    char buf[64];

    /* use the first process as the target */
    snprintf(buf, sizeof(buf), "%d", (int) getpid());
    char *token[] = {
        "gcore",
        buf,
        NULL,
    };
    
    pid_t pid = fork();
    if (pid == 0) {
        sleep(1);
        execv("/usr/bin/gcore", token);
    }
    else {
        waitpid(pid, NULL, 0);
    }
    _exit(1);
}
#endif

#define WEAK __attribute__((weak))
#define __NR_set_mempolicy 238
#define MPOL_INTERLEAVE  3

long WEAK set_mempolicy(int mode, const unsigned long *nmask,
                                   unsigned long maxnode)
{
    long i;
    i = syscall(__NR_set_mempolicy,mode,nmask,maxnode);
    return i;
}


int main(int argc, char *argv[]) {

    int ret = 0;

    unsigned long nodemask = -1;


    set_mempolicy(MPOL_INTERLEAVE, &nodemask, sizeof(nodemask) * 8);
    
    sindeg_table_init();

    struct rlimit rlim = {262144, 262144};
    setrlimit(RLIMIT_NOFILE, &rlim);
    
    // signal(SIGSEGV, crash_handler);
    
    parse_opt(argc, argv);
    
    if (do_help) {
        print_help(argv[0]);
        goto err;
    }
    
    if ((ret = log_open(logfile)) != 0)
        goto err;
    
    if ((ret = crabql_self_test()) != 0)
        goto err;

    if (self_test_only)
        goto err;

    if ((ret = server_init("0.0.0.0", port)) != 0)
        goto err;

    signal(SIGHUP, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGINT, signal_handler);
    signal(SIGPIPE, SIG_IGN);
    
    log_notice("CrabDB started");

    if (is_slave) {
        log_notice("slave mode ON.");
        server->is_slave = is_slave;
    }
    if (redirection) {
        log_notice("redirection mode ON, redirecting to host=%s, port=%"PRIu16, redirection->host, redirection->port);
        server->redirection = redirection;
    }
    else {
        
        if (datadir == NULL)
            datadir = "data";
            
        if (chdir(datadir) == -1) {
            log_error("unable to chdir to \"%s\"", datadir);
            exit(1);
        }
        
        slab_init(0, 1.125);
        
        binlog_open("binlog");
        // binlog_replay_all();

    }
    FILE *pf = NULL;
    
    if (pidfile) {
        pf = fopen(pidfile, "w");
        if (!pf) {
            log_error("unable to open %s\n", pidfile);
            exit(1);
        }
    }
    
    if (do_daemon) {
        int res = daemon(1, 0);
        (void) res;
    }
    
    if (pf) {
        fprintf(pf, "%d", getpid());
        fclose(pf);    
    }
    
    table_writer_init();
    
    if (username) {
        struct passwd *passwd = getpwnam(username);
        if (passwd) {
            log_notice("setgid to %d", (int) passwd->pw_gid);
            if (setgid(passwd->pw_gid) != 0) {
                log_error("setgid error: %d", errno);
                ret = CRABDB_ERROR_UNKNOWN;
                perror("setgid");
                goto err;
            }
            log_notice("setuid to %d", (int) passwd->pw_uid);
            if (setuid(passwd->pw_uid) != 0) {
                log_error("setgid error: %d", errno);
                ret = CRABDB_ERROR_UNKNOWN;
                goto err;
            }
        }
        else {
            log_error("unable to get user %s", username);
            ret = CRABDB_ERROR_INVALID_ARGUMENT;
            goto err;
        }
    }
    
    if ((ret = server_wait(server)) != 0)
        goto err;


    binlog_close(binlog);

err:
    return ret;
}
