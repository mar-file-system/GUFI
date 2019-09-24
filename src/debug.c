#include "debug.h"

pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;

uint64_t epoch = 0;

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

int print_debug(struct OutputBuffers * obufs, const size_t id, const char * str, const size_t str_len) {
    const size_t capacity = obufs->buffers[id].capacity;

    /* if the row can fit within an empty buffer, try to add the new row to the buffer */
    if (str_len <= capacity) {
        /* if there's not enough space in the buffer to fit the new row, flush it first */
        if ((obufs->buffers[id].filled + str_len) > capacity) {
            OutputBuffer_flush(&obufs->mutex, &obufs->buffers[id], stderr);
        }

        char * buf = obufs->buffers[id].buf;
        size_t filled = obufs->buffers[id].filled;

        memcpy(&buf[filled], str, str_len);
        filled += str_len;

        obufs->buffers[id].filled = filled;
        obufs->buffers[id].count++;
    }
    else {
        /* if the row does not fit the buffer, output immediately instead of buffering */
        fwrite(str, sizeof(char), str_len, stderr);
    }

    return 0;
}
