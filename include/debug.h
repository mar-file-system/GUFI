#ifndef GUFI_DEBUG_H
#define GUFI_DEBUG_H

#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <time.h>

extern pthread_mutex_t print_mutex;
extern uint64_t epoch;

// get a timespec's value in nanoseconds
uint64_t timestamp(struct timespec * ts);

// Get number of seconds between two events recorded in struct timespecs
long double elapsed(const struct timespec *start, const struct timespec *end);

#endif
