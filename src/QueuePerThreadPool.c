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


#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "QueuePerThreadPool.h"
#include "QueuePerThreadPoolPrivate.h"

#ifdef DEBUG
#include "debug.h"
#include <time.h>
#endif

#include <stdlib.h>

/* struct to pass into pthread_create */
struct worker_function_args {
    struct QPTPool * ctx;
    size_t id;
    void * args;
};

#if defined(DEBUG) && defined(PER_THREAD_STATS)
#include <stdio.h>
#include <string.h>
#include "OutputBuffers.h"
struct OutputBuffers debug_output_buffers = {};
#endif

/* struct that is created when something is enqueued */
struct queue_item {
    QPTPoolFunc_t func;
    void * work;
};

static void * worker_function(void * args) {
    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    char buf[4096];
    const size_t size = 4096;
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
        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        if (debug_output_buffers.buffers) {
            print_debug(&debug_output_buffers, wf_args->id, buf, size, "wf", &wf_start, &wf_end);
        }
        #endif
        return NULL;
    }

    struct QPTPoolData * tw = &wf_args->ctx->data[wf_args->id];

    while (1) {
        #if defined(DEBUG) && defined(PER_THREAD_STATS)
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
        struct sll work; /* don't bother initializing */
        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        clock_gettime(CLOCK_MONOTONIC, &wf_sll_init_end);
        #endif

        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        clock_gettime(CLOCK_MONOTONIC, &wf_tw_mutex_lock_start);
        #endif
        pthread_mutex_lock(&tw->mutex);
        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        clock_gettime(CLOCK_MONOTONIC, &wf_tw_mutex_lock_end);
        #endif

        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        clock_gettime(CLOCK_MONOTONIC, &wf_ctx_mutex_lock_start);
        #endif
        pthread_mutex_lock(&ctx->mutex);
        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        clock_gettime(CLOCK_MONOTONIC, &wf_ctx_mutex_lock_end);
        #endif

        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        clock_gettime(CLOCK_MONOTONIC, &wf_wait_start);
        #endif
        /* wait for work */
        while ((ctx->running && (!ctx->incomplete || !tw->queue.head)) ||
               (!ctx->running && (ctx->incomplete && !tw->queue.head))) {
            pthread_mutex_unlock(&ctx->mutex);
            pthread_cond_wait(&tw->cv, &tw->mutex);
            pthread_mutex_lock(&ctx->mutex);
        }
        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        clock_gettime(CLOCK_MONOTONIC, &wf_wait_end);
        #endif

        if (!ctx->running && !ctx->incomplete && !tw->queue.head) {
            pthread_mutex_unlock(&ctx->mutex);
            pthread_mutex_unlock(&tw->mutex);
            break;
        }

        pthread_mutex_unlock(&ctx->mutex);

        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        clock_gettime(CLOCK_MONOTONIC, &wf_move_queue_start);
        #endif
        /* moves entire queue into work and clears out queue */
        sll_move(&work, &tw->queue);
        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        clock_gettime(CLOCK_MONOTONIC, &wf_move_queue_end);
        #endif

        #if defined(DEBUG) && defined (QPTPOOL_QUEUE_SIZE)
        pthread_mutex_lock(&print_mutex);
        tw->queue.size = work.size;

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        fprintf(stderr, "%" PRIu64 " ", timestamp(&now) - epoch);

        size_t sum = 0;
        for(size_t i = 0; i < wf_args->ctx->size; i++) {
            fprintf(stderr, "%zu ", wf_args->ctx->data[i].queue.size);
            sum += wf_args->ctx->data[i].queue.size;
        }
        fprintf(stderr, "%zu\n", sum);
        tw->queue.size = 0;
        pthread_mutex_unlock(&print_mutex);
        #endif

        pthread_mutex_unlock(&tw->mutex);

        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        struct timespec wf_process_queue_start;
        clock_gettime(CLOCK_MONOTONIC, &wf_process_queue_start);
        #endif
        /* process all work */
        size_t work_count = 0;

        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        struct timespec wf_get_queue_head_start;
        clock_gettime(CLOCK_MONOTONIC, &wf_get_queue_head_start);
        #endif

        struct node * w = sll_head_node(&work);

        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        struct timespec wf_get_queue_head_end;
        clock_gettime(CLOCK_MONOTONIC, &wf_get_queue_head_end);
        print_debug(&debug_output_buffers, wf_args->id, buf, size, "wf_get_queue_head", &wf_get_queue_head_start, &wf_get_queue_head_end);
        #endif

        while (w) {
            #if defined(DEBUG) && defined(PER_THREAD_STATS)
            struct timespec wf_process_work_start;
            clock_gettime(CLOCK_MONOTONIC, &wf_process_work_start);
            #endif
            struct queue_item * qi = sll_node_data(w);

            tw->threads_successful += !qi->func(ctx, wf_args->id, qi->work, wf_args->args);

            #if defined(DEBUG) && defined(PER_THREAD_STATS)
            struct timespec wf_process_work_end;
            clock_gettime(CLOCK_MONOTONIC, &wf_process_work_end);

            print_debug(&debug_output_buffers, wf_args->id, buf, size, "wf_process_work", &wf_process_work_start, &wf_process_work_end);
            #endif

            #if defined(DEBUG) && defined(PER_THREAD_STATS)
            struct timespec wf_next_work_start;
            clock_gettime(CLOCK_MONOTONIC, &wf_next_work_start);
            #endif
            w = sll_next_node(w);
            #if defined(DEBUG) && defined(PER_THREAD_STATS)
            struct timespec wf_next_work_end;
            clock_gettime(CLOCK_MONOTONIC, &wf_next_work_end);
            print_debug(&debug_output_buffers, wf_args->id, buf, size, "wf_next_work", &wf_next_work_start, &wf_next_work_end);
            #endif

            work_count++;
        }

        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        struct timespec wf_process_queue_end;
        clock_gettime(CLOCK_MONOTONIC, &wf_process_queue_end);
        #endif

        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        struct timespec wf_cleanup_start;
        clock_gettime(CLOCK_MONOTONIC, &wf_cleanup_start);
        #endif
        sll_destroy(&work, 1);
        tw->threads_started += work_count;

        pthread_mutex_lock(&ctx->mutex);
        ctx->incomplete -= work_count;
        pthread_mutex_unlock(&ctx->mutex);
        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        struct timespec wf_cleanup_end;
        clock_gettime(CLOCK_MONOTONIC, &wf_cleanup_end);
        #endif

        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        if (debug_output_buffers.buffers) {
            print_debug(&debug_output_buffers, wf_args->id, buf, size, "wf_sll_init",       &wf_sll_init_start, &wf_sll_init_end);
            print_debug(&debug_output_buffers, wf_args->id, buf, size, "wf_tw_mutex_lock",  &wf_tw_mutex_lock_start, &wf_tw_mutex_lock_end);
            print_debug(&debug_output_buffers, wf_args->id, buf, size, "wf_ctx_mutex_lock", &wf_ctx_mutex_lock_start, &wf_ctx_mutex_lock_end);
            print_debug(&debug_output_buffers, wf_args->id, buf, size, "wf_wait",           &wf_wait_start, &wf_wait_end);
            print_debug(&debug_output_buffers, wf_args->id, buf, size, "wf_move",           &wf_move_queue_start, &wf_move_queue_end);
            print_debug(&debug_output_buffers, wf_args->id, buf, size, "wf_process_queue",  &wf_process_queue_start, &wf_process_queue_end);
            print_debug(&debug_output_buffers, wf_args->id, buf, size, "wf_cleanup",        &wf_cleanup_start, &wf_cleanup_end);
        }
        #endif
    }

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    struct timespec wf_broadcast_start;
    clock_gettime(CLOCK_MONOTONIC, &wf_broadcast_start);
    #endif
    for(size_t i = 0; i < ctx->size; i++) {
        pthread_cond_broadcast(&ctx->data[i].cv);
    }
    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    struct timespec wf_broadcast_end;
    clock_gettime(CLOCK_MONOTONIC, &wf_broadcast_end);
    if (debug_output_buffers.buffers) {
        print_debug(&debug_output_buffers, wf_args->id, buf, size, "wf_broadcast", &wf_broadcast_start, &wf_broadcast_end);
    }
    #endif

    free(args);

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    struct timespec wf_end;
    clock_gettime(CLOCK_MONOTONIC, &wf_end);

    if (debug_output_buffers.buffers) {
        print_debug(&debug_output_buffers, wf_args->id, buf, size, "wf", &wf_start, &wf_end);
    }
    #endif

    return NULL;
}

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
    pthread_mutex_init(&ctx->mutex, NULL);
    ctx->running = 1;
    ctx->incomplete = 0;

    for(size_t i = 0; i < threads; i++) {
        sll_init(&ctx->data[i].queue);
        pthread_mutex_init(&ctx->data[i].mutex, NULL);
        pthread_cond_init(&ctx->data[i].cv, NULL);
        ctx->data[i].next_queue = i;
        ctx->data[i].thread = 0;
        ctx->data[i].threads_started = 0;
        ctx->data[i].threads_successful = 0;
    }

    return ctx;
}

size_t QPTPool_start(struct QPTPool * ctx, void * args) {
    if (!ctx) {
        return 0;
    }

    size_t started = 0;
    for(size_t i = 0; i < ctx->size; i++) {
        struct worker_function_args * wf_args = calloc(1, sizeof(struct worker_function_args));
        wf_args->ctx = ctx;
        wf_args->id = i;
        wf_args->args = args;
        started += !pthread_create(&ctx->data[i].thread, NULL, worker_function, wf_args);
    }

    if (started != ctx->size) {
        QPTPool_wait(ctx);
    }

    return started;
}

/* id selects the next_queue variable to use, not where the work will be placed */
void QPTPool_enqueue(struct QPTPool * ctx, const size_t id, QPTPoolFunc_t func, void * new_work) {
    /* skip argument checking */
    /* if (ctx) { */
        struct queue_item * qi = malloc(sizeof(struct queue_item));
        qi->func = func; /* if no function is provided, the thread will segfault when it processes this item*/
        qi->work = new_work;

        pthread_mutex_lock(&ctx->data[ctx->data[id].next_queue].mutex);
        sll_push(&ctx->data[ctx->data[id].next_queue].queue, qi);
        pthread_mutex_unlock(&ctx->data[ctx->data[id].next_queue].mutex);

        pthread_mutex_lock(&ctx->mutex);
        ctx->incomplete++;
        pthread_mutex_unlock(&ctx->mutex);

        pthread_cond_broadcast(&ctx->data[ctx->data[id].next_queue].cv);

        ctx->data[id].next_queue = (ctx->data[id].next_queue + 1) % ctx->size;
    /* } */
}

void QPTPool_wait(struct QPTPool * ctx) {
    if (!ctx) {
        return;
    }

    pthread_mutex_lock(&ctx->mutex);
    ctx->running = 0;
    pthread_mutex_unlock(&ctx->mutex);
    for(size_t i = 0; i < ctx->size; i++) {
        pthread_mutex_lock(&ctx->data[i].mutex);
        pthread_cond_broadcast(&ctx->data[i].cv);
        pthread_mutex_unlock(&ctx->data[i].mutex);
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
            sll_destroy(&ctx->data[i].queue, 0);
        }

        free(ctx->data);
        free(ctx);
    }
}

/* utility functions */
size_t QPTPool_get_index(struct QPTPool * ctx, const pthread_t id) {
    /* skip argument checking */
    /* if (!ctx) { */
    /*     return (size_t) -1; */
    /* } */
    for(size_t i = 0; i < ctx->size; i++) {
        if (id == ctx->data[i].thread) {
            return i;
        }
    }
    return (size_t) -1;
}

size_t QPTPool_threads_started(struct QPTPool * ctx) {
    /* skip argument checking */
    /* if (!ctx) { */
    /*     return (size_t) -1; */
    /* } */
    size_t sum = 0;
    for(size_t i = 0; i < ctx->size; i++) {
        sum += ctx->data[i].threads_started;
    }
    return sum;
}

size_t QPTPool_threads_completed(struct QPTPool * ctx) {
    /* skip argument checking */
    /* if (!ctx) { */
    /*     return (size_t) -1; */
    /* } */
    size_t sum = 0;
    for(size_t i = 0; i < ctx->size; i++) {
        sum += ctx->data[i].threads_successful;
    }
    return sum;
}
