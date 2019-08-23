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

-----
NOTE:
-----

GUFI uses the C-Thread-Pool library.  The original version, written by
Johan Hanssen Seferidis, is found at
https://github.com/Pithikos/C-Thread-Pool/blob/master/LICENSE, and is
released under the MIT License.  LANS, LLC added functionality to the
original work.  The original work, plus LANS, LLC added functionality is
found at https://github.com/jti-lanl/C-Thread-Pool, also under the MIT
License.  The MIT License can be found at
https://opensource.org/licenses/MIT.


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


#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "QueuePerThreadPool.h"
#include "QueuePerThreadPoolPrivate.h"
#include "debug.h"

#include <sched.h>
#include <stdlib.h>

struct QPTPool * QPTPool_init(const size_t threads) {
    if (!threads) {
        return NULL;
    }

    struct QPTPool * ctx = calloc(1, sizeof(struct QPTPool));
    if (!ctx) {
        return NULL;
    }

    if (!(ctx->data = calloc(threads, sizeof(struct QPTPoolData)))) {
        free(ctx);
        return NULL;
    }
    ctx->size = threads;

    for(size_t i = 0; i < threads; i++) {
        ctx->data[i].id = i;
        sll_init(&ctx->data[i].queue);
        pthread_mutex_init(&ctx->data[i].mutex, NULL);
        pthread_cond_init(&ctx->data[i].cv, NULL);
        ctx->data[i].next_queue = i;
        ctx->data[i].thread = 0;
        ctx->data[i].threads_started = 0;
        ctx->data[i].threads_successful = 0;
    }

    pthread_mutex_init(&ctx->mutex, NULL);
    ctx->incomplete = 0;

    return ctx;
}

void QPTPool_enqueue(struct QPTPool * ctx, const size_t id, void * new_work) {
    pthread_mutex_lock(&ctx->data[ctx->data[id].next_queue].mutex);
    sll_push(&ctx->data[ctx->data[id].next_queue].queue, new_work);
    pthread_mutex_unlock(&ctx->data[ctx->data[id].next_queue].mutex);

    pthread_mutex_lock(&ctx->mutex);
    ctx->incomplete++;
    pthread_mutex_unlock(&ctx->mutex);

    pthread_cond_broadcast(&ctx->data[ctx->data[id].next_queue].cv);

    ctx->data[id].next_queue = (ctx->data[id].next_queue + 1) % ctx->size;
}

struct worker_function_args {
    struct QPTPool * ctx;
    size_t id;
    QPTPoolFunc_t func;
    int pinned;
    void * args;
};

#if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
#include <stdio.h>
static pthread_mutex_t count_mutex = PTHREAD_MUTEX_INITIALIZER;

extern uint64_t epoch;
#endif

static void * worker_function(void *args) {
    #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
    struct timespec wf_start;
    clock_gettime(CLOCK_MONOTONIC, &wf_start);
    #endif

    if (!args) {
        struct timespec wf_end;
        clock_gettime(CLOCK_MONOTONIC, &wf_end);
        return NULL;
    }

    struct worker_function_args * wf_args = (struct worker_function_args *) args;
    struct QPTPool * ctx = wf_args->ctx;

    if (!ctx) {
        free(args);
        struct timespec wf_end;
        clock_gettime(CLOCK_MONOTONIC, &wf_end);
        return NULL;
    }

    if (wf_args->pinned) {
        cpu_set_t cpus;
        CPU_ZERO(&cpus);
        CPU_SET(wf_args->id, &cpus);

        if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpus) != 0) {
            return NULL;
        }
    }

    struct QPTPoolData * tw = &wf_args->ctx->data[wf_args->id];

    while (1) {
        #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
        struct timespec wf_sll_init_start;
        struct timespec wf_sll_init_end;
        struct timespec wf_tw_mutex_lock_start;
        struct timespec wf_tw_mutex_lock_end;
        struct timespec wf_ctx_mutex_lock_start;
        struct timespec wf_ctx_mutex_lock_end;
        struct timespec wf_wait_start;
        struct timespec wf_wait_end;
        struct timespec wf_move_queue_start;
        struct timespec wf_move_queue_end;

        clock_gettime(CLOCK_MONOTONIC, &wf_sll_init_start);
        #endif
        struct sll dirs;
        sll_init(&dirs);
        #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
        clock_gettime(CLOCK_MONOTONIC, &wf_sll_init_end);
        #endif
        {
            #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
            clock_gettime(CLOCK_MONOTONIC, &wf_tw_mutex_lock_start);
            #endif
            pthread_mutex_lock(&tw->mutex);
            #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
            clock_gettime(CLOCK_MONOTONIC, &wf_tw_mutex_lock_end);
            #endif

            #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
            clock_gettime(CLOCK_MONOTONIC, &wf_ctx_mutex_lock_start);
            #endif
            pthread_mutex_lock(&ctx->mutex);
            #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
            clock_gettime(CLOCK_MONOTONIC, &wf_ctx_mutex_lock_end);
            #endif

            // wait for work
            #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
            clock_gettime(CLOCK_MONOTONIC, &wf_wait_start);
            #endif
            while (ctx->incomplete && !tw->queue.head) {
                pthread_mutex_unlock(&ctx->mutex);
                pthread_cond_wait(&tw->cv, &tw->mutex);
                pthread_mutex_lock(&ctx->mutex);
            }
            #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
            clock_gettime(CLOCK_MONOTONIC, &wf_wait_end);
            #endif

            if (!ctx->incomplete && !tw->queue.head) {
                pthread_mutex_unlock(&ctx->mutex);
                pthread_mutex_unlock(&tw->mutex);
                break;
            }

            #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
            clock_gettime(CLOCK_MONOTONIC, &wf_move_queue_start);
            #endif
            pthread_mutex_unlock(&ctx->mutex);

            // moves queue into dirs and clears out queue
            sll_move(&dirs, &tw->queue);

            /* #if defined(DEBUG) */
            /* pthread_mutex_lock(&count_mutex); */
            /* tw->queue.size = dirs.size; */

            /* struct timespec now; */
            /* clock_gettime(CLOCK_MONOTONIC, &now); */
            /* uint64_t ns = now.tv_sec; */
            /* ns *= 1000000000ULL; */
            /* ns += now.tv_nsec; */
            /* ns -= epoch; */
            /* size_t sum = 0; */

            /* fprintf(stderr, "%" PRIu64 " ", ns); */
            /* for(size_t i = 0; i < wf_args->ctx->size; i++) { */
            /*     fprintf(stderr, "%zu ", wf_args->ctx->data[i].queue.size); */
            /*     sum += wf_args->ctx->data[i].queue.size; */
            /* } */
            /* fprintf(stderr, "%zu\n", sum); */
            /* tw->queue.size = 0; */
            /* pthread_mutex_unlock(&count_mutex); */
            /* #endif */

            pthread_mutex_unlock(&tw->mutex);

            #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
            clock_gettime(CLOCK_MONOTONIC, &wf_move_queue_end);
            #endif
        }

        #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
        struct timespec wf_process_queue_start;
        clock_gettime(CLOCK_MONOTONIC, &wf_process_queue_start);
        #endif
        // process all work
        size_t work_count = 0;
        for(struct node * dir = sll_head_node(&dirs); dir; dir = sll_next_node(dir)) {
            tw->threads_successful += !wf_args->func(ctx, sll_node_data(dir), wf_args->id, wf_args->args);
            work_count++;
        }
        #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
        struct timespec wf_process_queue_end;
        clock_gettime(CLOCK_MONOTONIC, &wf_process_queue_end);
        #endif

        #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
        struct timespec wf_cleanup_start;
        clock_gettime(CLOCK_MONOTONIC, &wf_cleanup_start);
        #endif
        sll_destroy(&dirs);
        tw->threads_started += work_count;

        pthread_mutex_lock(&ctx->mutex);
        ctx->incomplete -= work_count;
        pthread_mutex_unlock(&ctx->mutex);
        #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
        struct timespec wf_cleanup_end;
        clock_gettime(CLOCK_MONOTONIC, &wf_cleanup_end);
        #endif

        #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
        pthread_mutex_lock(&count_mutex);
        fprintf(stderr, "%zu ", wf_args->id);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_sll_init_start) - epoch);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_sll_init_end) - epoch);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_tw_mutex_lock_start) - epoch);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_tw_mutex_lock_end) - epoch);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_ctx_mutex_lock_start) - epoch);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_ctx_mutex_lock_end) - epoch);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_wait_start) - epoch);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_wait_end) - epoch);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_move_queue_start) - epoch);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_move_queue_end) - epoch);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_process_queue_start) - epoch);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_process_queue_end) - epoch);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_cleanup_start) - epoch);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_cleanup_end) - epoch);
        /* fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_broadcast_start) - epoch); */
        /* fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_broadcast_end) - epoch); */
        fprintf(stderr, "\n");
        pthread_mutex_unlock(&count_mutex);
        #endif
    }

    #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
    struct timespec wf_broadcast_start;
    clock_gettime(CLOCK_MONOTONIC, &wf_broadcast_start);
    #endif
    for(size_t i = 0; i < ctx->size; i++) {
        pthread_cond_broadcast(&ctx->data[i].cv);
    }
    #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
    struct timespec wf_broadcast_end;
    clock_gettime(CLOCK_MONOTONIC, &wf_broadcast_end);
    #endif

    free(args);

    #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS)
    struct timespec wf_end;
    clock_gettime(CLOCK_MONOTONIC, &wf_end);
    #endif

    /* #if defined(DEBUG) && defined(QPTPOOL_TIMESTAMPS) */
    /* pthread_mutex_lock(&count_mutex); */
    /* fprintf(stderr, "%zu ", wf_args->id); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&wf_end) - epoch); */
    /* fprintf(stderr, "\n"); */
    /* pthread_mutex_unlock(&count_mutex); */
    /* #endif */

    return NULL;
}

size_t QPTPool_start(struct QPTPool * ctx, const int pinned, QPTPoolFunc_t func, void * args) {
    if (!ctx || !func) {
        return 0;
    }

    size_t started = 0;
    for(size_t i = 0; i < ctx->size; i++) {
        struct worker_function_args * wf_args = calloc(1, sizeof(struct worker_function_args));
        wf_args->ctx = ctx;
        wf_args->id = i;
        wf_args->pinned = pinned;
        wf_args->func = func;
        wf_args->args = args;
        started += !pthread_create(&ctx->data[i].thread, NULL, worker_function, wf_args);
    }

    return started;
}

void QPTPool_wait(struct QPTPool * ctx) {
    if (!ctx) {
        return;
    }

    for(size_t i = 0; i < ctx->size; i++) {
        pthread_join(ctx->data[i].thread, NULL);
    }
}

void QPTPool_destroy(struct QPTPool * ctx) {
    if (ctx) {
        for(size_t i = 0; i < ctx->size; i++) {
            ctx->data[i].threads_successful = 0;
            ctx->data[i].threads_started = 0;
            ctx->data[i].thread = 0;
            pthread_cond_destroy(&ctx->data[i].cv);
            pthread_mutex_destroy(&ctx->data[i].mutex);
            sll_destroy(&ctx->data[i].queue);
        }

        free(ctx->data);
        free(ctx);
    }
}

// utility functions
size_t QPTPool_get_index(struct QPTPool * ctx, const pthread_t id) {
    for(size_t i = 0; i < ctx->size; i++) {
        if (id == ctx->data[i].thread) {
            return i;
        }
    }
    return (size_t) -1;
}

size_t QPTPool_threads_started(struct QPTPool * ctx) {
    size_t sum = 0;
    for(size_t i = 0; i < ctx->size; i++) {
        sum += ctx->data[i].threads_started;
    }
    return sum;
}

size_t QPTPool_threads_completed(struct QPTPool * ctx) {
    size_t sum = 0;
    for(size_t i = 0; i < ctx->size; i++) {
        sum += ctx->data[i].threads_successful;
    }
    return sum;
}
