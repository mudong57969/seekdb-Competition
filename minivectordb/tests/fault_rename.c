/* Test-only fault injection. Stop just after a successful filesystem rename. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int rename(const char *source, const char *destination)
{
    int (*next_rename)(const char *, const char *) = dlsym(RTLD_NEXT, "rename");
    if (next_rename == NULL) {
        abort();
    }
    int result = next_rename(source, destination);
    if (result != 0) {
        return result;
    }
    const char *phase = getenv("MINIVECTORDB_FAULT");
    if (phase != NULL &&
        ((strcmp(phase, "publish") == 0 && strcmp(destination, "/data/committed") == 0) ||
         (strcmp(phase, "install") == 0 && strncmp(source, "/data/committed/table-", 22) == 0))) {
        fprintf(stderr, "fault: %s\n", phase);
        fflush(stderr);
        for (;;) {
            pause();
        }
    }
    return result;
}
