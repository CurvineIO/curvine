#define _GNU_SOURCE

#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/xattr.h>
#include <time.h>
#include <unistd.h>

static long elapsed_ms(const struct timespec *start)
{
    struct timespec now;

    clock_gettime(CLOCK_MONOTONIC, &now);
    return (now.tv_sec - start->tv_sec) * 1000L +
           (now.tv_nsec - start->tv_nsec) / 1000000L;
}

static int run_child(const char *path)
{
    static const char data[] = "curvine xattr hang reproducer\n";
    static const char value[] = "value";
    int fd;

    fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) {
        perror("open");
        return 2;
    }

    if (write(fd, data, sizeof(data) - 1) != (ssize_t)(sizeof(data) - 1)) {
        perror("write");
        close(fd);
        return 2;
    }

    /* close() queues FUSE RELEASE; SETXATTR follows immediately. */
    if (close(fd) < 0) {
        perror("close");
        return 2;
    }

    fprintf(stderr, "child: calling setxattr(%s)\n", path);
    if (setxattr(path, "user.curvine_repro", value, sizeof(value) - 1, 0) < 0) {
        perror("setxattr");
        return 1;
    }

    fprintf(stderr, "child: setxattr returned successfully\n");
    return 0;
}

int main(int argc, char **argv)
{
    const char *mountpoint;
    int timeout_seconds = 10;
    struct timespec start;
    char path[4096];
    pid_t child;
    int status;

    if (argc < 2 || argc > 3) {
        fprintf(stderr, "Usage: %s MOUNTPOINT [TIMEOUT_SECONDS]\n", argv[0]);
        return 2;
    }

    mountpoint = argv[1];
    if (argc == 3) {
        timeout_seconds = atoi(argv[2]);
        if (timeout_seconds <= 0) {
            fprintf(stderr, "TIMEOUT_SECONDS must be positive\n");
            return 2;
        }
    }

    if (snprintf(path, sizeof(path), "%s/xattr-hang-repro-%ld",
                 mountpoint, (long)getpid()) >= (int)sizeof(path)) {
        fprintf(stderr, "reproduction path is too long\n");
        return 2;
    }

    child = fork();
    if (child < 0) {
        perror("fork");
        return 2;
    }
    if (child == 0)
        _exit(run_child(path));

    clock_gettime(CLOCK_MONOTONIC, &start);
    for (;;) {
        pid_t result = waitpid(child, &status, WNOHANG);

        if (result == child)
            break;
        if (result < 0) {
            perror("waitpid");
            return 2;
        }
        if (elapsed_ms(&start) >= timeout_seconds * 1000L) {
            fprintf(stderr,
                    "REPRODUCED: setxattr did not return within %d seconds "
                    "(child pid %ld)\n",
                    timeout_seconds, (long)child);
            kill(child, SIGKILL);
            waitpid(child, NULL, 0);
            return 124;
        }

        usleep(10000);
    }

    if (WIFEXITED(status)) {
        int rc = WEXITSTATUS(status);

        if (rc == 0)
            unlink(path);
        return rc;
    }

    fprintf(stderr, "child terminated abnormally\n");
    return 2;
}
