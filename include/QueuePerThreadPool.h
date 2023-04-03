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



#ifndef QUEUE_PER_THREAD_POOL_H
#define QUEUE_PER_THREAD_POOL_H

#include <inttypes.h>
#include <pthread.h>

#if defined(DEBUG) && defined(PER_THREAD_STATS)
#include "OutputBuffers.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct QPTPoolThreadData QPTPoolThreadData_t;

/*
 * User defined function to select the thread to pass new work to.
 * If NULL is passed into QPTPool_init, round robin is used.
 *
 * @param id       the id of this thread
 * @param prev     the previous thread id that this thread pushed work to
 * @param threads  the total number of threads in this pool
 * @param data     the data the function is operating on
 * @param args     next_args from QPTPool_init
 * @return work queue to push to next in the range [0, threads)
 */
typedef size_t (*QPTPoolNextFunc_t)(const size_t id, const size_t prev, const size_t threads,
                                    void *data, void *args);

/* The Queue Per Thread Pool context */
typedef struct QPTPool {
    QPTPoolThreadData_t *data;
    size_t nthreads;
    uint64_t queue_limit;

    void *args;

    QPTPoolNextFunc_t next;
    void *next_args;

    pthread_mutex_t mutex;
    int running;
    uint64_t incomplete;

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    struct OutputBuffers *buffers;
    #endif
} QPTPool_t;

/* initialize a QPTPool context and start threads */
QPTPool_t *QPTPool_init(const size_t nthreads,
                        void *args,
                        QPTPoolNextFunc_t next,
                        void *next_args,
                        const uint64_t queue_limit
                        #if defined(DEBUG) && defined(PER_THREAD_STATS)
                        , struct OutputBuffers *buffers
                        #endif
        );

/* User defined function to pass into QPTPool_enqueue
 *
 * @param ctx      the pool context the function is running in
 * @param id       the id of the thread enqueuing work
 * @param data     the new data that is being enqueued
 * @param args     any extra data to make accessible to all functions that this thread pool runs
 * @return 0 if successful, non-zero if not
 */
typedef int (*QPTPoolFunc_t)(QPTPool_t *ctx, const size_t id, void *data, void *args);

/* enqueue data and a function to process the data */
/* id will push to the thread's next scheduled queue, rather than directly onto queue[id] */
void QPTPool_enqueue(QPTPool_t *ctx, const size_t id, QPTPoolFunc_t func, void *new_work);

/*
 * wait for all work to be processed and join threads
 *
 * this is separate from QPTPool_destroy to allow for
 * collecting of stats before destroying context
 */
void QPTPool_wait(QPTPool_t *ctx);

/* clean up QPTPool context data */
void QPTPool_destroy(QPTPool_t *ctx);

/* utility functions */

/* get the number of threads that were started by the QPTPool */
size_t QPTPool_threads_started(QPTPool_t *ctx);

/* get the number of started threads that completed successfully */
size_t QPTPool_threads_completed(QPTPool_t *ctx);

#ifdef __cplusplus
}
#endif

#endif
