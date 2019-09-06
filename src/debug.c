#include "debug.h"

pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;

uint64_t epoch;

uint64_t timestamp(struct timespec * ts) {
    uint64_t ns = ts->tv_sec;
    ns *= 1000000000ULL;
    ns += ts->tv_nsec;
    return ns;
}

long double elapsed(const struct timespec *start, const struct timespec *end) {
    const long double s = ((long double) start->tv_sec) + ((long double) start->tv_nsec) / 1000000000ULL;
    const long double e = ((long double) end->tv_sec)   + ((long double) end->tv_nsec)   / 1000000000ULL;
    return e - s;
}
