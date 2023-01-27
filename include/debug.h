/*
This file is part of GUFI, which is part of MarFS, which is released
under the BSD license.


Copyright (c) 2017, Los Alamos National Security (LANS), LLC
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


From Los Alamos National Security, LLC:
LA-CC-15-039

Copyright (c) 2017, Los Alamos National Security, LLC All rights reserved.
Copyright 2017. Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use,
reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
modified to produce derivative works, such modified software should be
clearly marked, so as not to confuse it with the version available from
LANL.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/



#ifndef GUFI_DEBUG_H
#define GUFI_DEBUG_H

#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "OutputBuffers.h"

extern pthread_mutex_t print_mutex;
extern uint64_t epoch;

struct start_end {
    struct timespec start;
    struct timespec end;
};

#define timestamp_create_raw(name)                                      \
    struct start_end name;

#define timestamp_create_zero_raw(name, zero)                           \
    timestamp_create_raw(name);                                         \
    memcpy(&name.start, zero, sizeof(struct timespec));                 \
    memcpy(&name.end,   zero, sizeof(struct timespec))

#define timestamp_set_start_raw(name)                                   \
    clock_gettime(CLOCK_MONOTONIC, &((name).start));

#define timestamp_create_start_raw(name)                                \
    timestamp_create_raw(name);                                         \
    timestamp_set_start_raw(name)

#define timestamp_set_end_raw(name)                                     \
    clock_gettime(CLOCK_MONOTONIC, &((name).end));

#define timestamp_print_raw(obs, id, str, name)                         \
    print_timer(obs, id, ts_buf, sizeof(ts_buf), str, &name)

/* nanoseconds since an unspecified epoch */
uint64_t since_epoch(struct timespec *ts);

/* Get number of nanoseconds between two events recorded in struct timespecs */
uint64_t nsec(struct start_end *se);

long double sec(uint64_t ns);

int print_timer(struct OutputBuffers *obufs, const size_t id,
                const char *name, struct start_end *se);

#ifdef DEBUG

#define timestamp_get_name(name)                                        \
    name##_ts

#define timestamp_create(name)                                          \
    timestamp_create_raw(timestamp_get_name(name))

#define timestamp_create_zero(name, zero)                               \
    timestamp_create_zero_raw(timestamp_get_name(name), &(zero))

#define timestamp_set_start(name)                                       \
    timestamp_set_start_raw(timestamp_get_name(name))

#define timestamp_set_end(name)                                         \
    timestamp_set_end_raw(timestamp_get_name(name))

#define timestamp_elapsed(name)                                         \
    nsec(&timestamp_get_name(name))

#define timestamp_create_start(name)                                    \
    timestamp_create(name);                                             \
    timestamp_set_start(name)

#else

#define timestamp_get_name(name)
#define timestamp_create(name)
#define timestamp_create_zero(name, zero)
#define timestamp_set_start(name)
#define timestamp_set_end(name)
#define timestamp_elapsed(name)
#define timestamp_create_start(name)

#endif /* DEBUG */

#if defined(DEBUG) && defined(PER_THREAD_STATS)

#define timestamp_print_init(obs, count, capacity, mutex_ptr)           \
    struct OutputBuffers obs##_stack;                                   \
    struct OutputBuffers * obs = &obs##_stack;                          \
    pthread_mutex_t obs##_static_mutex = PTHREAD_MUTEX_INITIALIZER;     \
    pthread_mutex_t *obs##_mutex_ptr = (mutex_ptr);                     \
    if (!obs##_mutex_ptr) {                                             \
        obs##_mutex_ptr = &obs##_static_mutex;                          \
    }                                                                   \
    (void) obs##_static_mutex;                                          \
    if (!OutputBuffers_init(obs, count, capacity, obs##_mutex_ptr)) {   \
        fprintf(stderr, "Error: Could not initialize OutputBuffers\n"); \
        return -1;                                                      \
    }

#define timestamp_print(obs, id, str, name)                             \
    print_timer(obs, id, str, &timestamp_get_name(name))

#define timestamp_end_print(obs, id, str, name)                         \
    timestamp_set_end(name);                                            \
    timestamp_print(obs, id, str, name)

#define timestamp_print_destroy(obs)                                    \
    OutputBuffers_flush_to_single(obs, stderr);                         \
    OutputBuffers_destroy(obs)

#else

#define timestamp_print_init(obs, count, capacity, mutex_ptr)
#define timestamp_print(obs, id, str, name)
#define timestamp_end_print(obs, id, str, name)
#define timestamp_print_destroy(obs)

#endif /* PER_THREAD_STATS */

#endif /* GUFI_DEBUG_H */
