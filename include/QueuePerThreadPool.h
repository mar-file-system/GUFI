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

#ifdef __cplusplus
extern "C" {
#endif

/* The Queue Per Thread Pool context */
typedef struct QPTPool QPTPool_t;

/* initialize a QPTPool context - call QPTPool_start to start threads */
QPTPool_t *QPTPool_init(const size_t nthreads, void *args);

/*
 * User defined function to select the thread to pass new work to.
 *
 * @param id       the id of this thread
 * @param prev     the previous thread id that this thread pushed work to
 * @param threads  the total number of threads in this pool
 * @param data     the data the function is operating on
 * @param args     external value available for the next function to use
 * @return work queue to push to next in the range [0, threads)
 */
typedef size_t (*QPTPoolNextFunc_t)(const size_t id, const size_t prev, const size_t threads,
                                    void *data, void *args);

/*
 * Set QPTPool context properties
 * Call these functions before calling QPTPool_start
 *
 * @return 0 if successful, non-zero if not
 */
int QPTPool_set_next(QPTPool_t *ctx, QPTPoolNextFunc_t func, void *args);
int QPTPool_set_queue_limit(QPTPool_t *ctx, const uint64_t queue_limit); /* count; same for all threads */
int QPTPool_set_swap_prefix(QPTPool_t *ctx, const char *swap_prefix);
int QPTPool_set_steal(QPTPool_t *ctx, const uint64_t num, const uint64_t denom);

/* Get QPTPool context properties */
int QPTPool_get_nthreads(QPTPool_t *ctx, size_t *nthreads);
int QPTPool_get_args(QPTPool_t *ctx, void **args);
int QPTPool_get_next(QPTPool_t *ctx, QPTPoolNextFunc_t *func, void **args);
int QPTPool_get_queue_limit(QPTPool_t *ctx, uint64_t *queue_limit);
int QPTPool_get_swap_prefix(QPTPool_t *ctx, const char **swap_prefix);
int QPTPool_get_steal(QPTPool_t *ctx, uint64_t *num, uint64_t *denom);

/* calls QPTPool_init and QPTPool_set_* functions */
QPTPool_t *QPTPool_init_with_props(const size_t nthreads, void *args,
                                   QPTPoolNextFunc_t next_func, void *next_args,
                                   const uint64_t queue_limit, const char *swap_prefix,
                                   const uint64_t steal_num, const uint64_t steal_denom);

/*
 * QPTPool_init only allocates memory - call this to start threads
 *
 * @return 0 if successful, non-zero if not
 */
int QPTPool_start(QPTPool_t *ctx);

/*
 * User defined function to pass into QPTPool_enqueue
 *
 * @param ctx      the pool context the function is running in
 * @param id       the id of the thread enqueuing work
 * @param data     the new data that is being enqueued
 * @param args     any extra data to make accessible to all functions that this thread pool runs
 * @return 0 if successful, non-zero if not
 */
typedef int (*QPTPool_f)(QPTPool_t *ctx, const size_t id, void *data, void *args);

/* which queue a new work item will be placed in */
typedef enum QPTPool_enqueue_dst {
    QPTPool_enqueue_ERROR,
    QPTPool_enqueue_WAIT,
    QPTPool_enqueue_SWAP,
} QPTPool_enqueue_dst_t;

/* function signatures for serializing and deserializing data to swap */
typedef int (*QPTPool_serialize_and_free_f)(const int fd, QPTPool_f func, void *work, size_t *size);
typedef int (*QPTPool_alloc_and_deserialize_f)(const int fd, QPTPool_f *func, void **work);

int QPTPool_generic_serialize_and_free(const int fd, QPTPool_f func, void *work, const size_t len,
                                       size_t *size);
int QPTPool_generic_alloc_and_deserialize(const int fd, QPTPool_f *func, void **work);

/*
 * Enqueue data and a function to process the data. Pushes to
 * thread[id]->next_queue, rather than directly to thread[id]. The
 * queue_limit is ignored and work items are not swapped.
 *
 * This function can be called before starting the thread pool.
 *
 * @param ctx      the pool context the function is running in
 * @param id       the id of the thread used to select where the work item should be enqueued
 * @param func     the function to run
 * @param new_work the work to be processed by func
 */
QPTPool_enqueue_dst_t QPTPool_enqueue(QPTPool_t *ctx, const size_t id,
                                      QPTPool_f func, void *new_work);

/*
 * Enqueue data and a function to process the data. Pushes to
 * thread[id]->next_queue, rather than directly to thread[id]. If
 * target thread's queue size is greater than or equal to the
 * queue_limit, the work item is swapped and freed from memory.
 *
 * This function can be called before starting the thread pool.
 * However, note that if the queue limit or swap prefix is set after
 * pushing to the swap queue, all data in the swap files will be lost.
 *
 * @param ctx         the pool context the function is running in
 * @param id          the id of the thread used to select where the work item should be enqueued
 * @param func        the function to run
 * @param new_work    the work to be processed by func
 * @param serialize   function to serialize and if necessary, free new_work
 * @param deserialize function to allocate space and fill in new_work
 */
QPTPool_enqueue_dst_t QPTPool_enqueue_swappable(QPTPool_t *ctx, const size_t id,
                                                QPTPool_f func, void *new_work,
                                                QPTPool_serialize_and_free_f serialize,
                                                QPTPool_alloc_and_deserialize_f deserialze);
/*
 * Enqueue data and a function to process the data. Pushes directly to
 * thread[id] instead of thread[id]->next_queue. The queue_limit is
 * ignored. If the work item is swapped, it is also freed from memory.
 *
 * This function can be called before starting the thread pool.
 * However, note that if the queue limit or swap prefix is set after
 * pushing to the swap queue, all data in the swap files will be lost.
 *
 * @param ctx         the pool context the function is running in
 * @param id          the id of the thread that will process this work
 * @param queue       queue to push to: wait or swap
 * @param func        the function to run
 * @param new_work    the work to be processed by func
 * @param serialize   function to serialize and if necessary, free new_work
 * @param deserialize function to allocate space and fill in new_work
 */
QPTPool_enqueue_dst_t QPTPool_enqueue_here(QPTPool_t *ctx, const size_t id, QPTPool_enqueue_dst_t queue,
                                           QPTPool_f func, void *new_work,
                                           QPTPool_serialize_and_free_f serialize,
                                           QPTPool_alloc_and_deserialize_f deserialize);

/* Wait for all in-memory work to be processed but do not clean up threads */
uint64_t QPTPool_wait_mem(QPTPool_t *ctx);

/* Wait for all but count in-memory work items to be processed and do not clean up threads */
uint64_t QPTPool_wait_mem_lte(QPTPool_t *ctx, const uint64_t count);

/* Wait for all in-memory and swapped work to be processed and do not clean up threads */
void QPTPool_wait(QPTPool_t *ctx);

/*
 * Join threads after all threads have stopped
 *
 * If QPTPool_start is called QPTPool_stop must be
 * called before QPTPool_destroy to clean up properly.
 * QPTPool_wait does not have to be called before
 * QPTPool_stop.
 *
 * This is separate from QPTPool_destroy to allow for
 * collecting of stats before destroying context.
 */
void QPTPool_stop(QPTPool_t *ctx);

/* utility functions */

/* get the number of threads that were started by the QPTPool */
uint64_t QPTPool_threads_started(QPTPool_t *ctx);

/* get the number of started threads that completed successfully */
uint64_t QPTPool_threads_completed(QPTPool_t *ctx);

/* number of work items that were swapped out */
uint64_t QPTPool_work_swapped_count(QPTPool_t *ctx);

/* total size of work items that were swapped out (function pointer + arg size + arg contents) */
size_t QPTPool_work_swapped_size(QPTPool_t *ctx);

/* clean up QPTPool context data */
void QPTPool_destroy(QPTPool_t *ctx);

#ifdef __cplusplus
}
#endif

#endif
