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

#include <stdlib.h>

#include "debug.h"
#include "QueuePerThreadPool.h"
#include "QueuePerThreadPoolPrivate.h"

/* struct to pass into pthread_create */
struct worker_function_args {
    struct QPTPool * ctx;
    size_t id;
    void * args;
};

/* struct that is created when something is enqueued */
struct queue_item {
    QPTPoolFunc_t func;
    void * work;
};

static void * worker_function(void * args) {
    timestamp_create_buffer(4096);
    timestamp_start(wf);

    struct worker_function_args * wf_args = (struct worker_function_args *) args;
    struct QPTPool * ctx = wf_args->ctx;

    if (!args) {
        timestamp_end(ctx->buffers, wf_args->id, "wf", wf);
        return NULL;
    }

    if (!ctx) {
        free(args);
        timestamp_end(ctx->buffers, wf_args->id, "wf", wf);
        return NULL;
    }

    struct QPTPoolData * tw = &wf_args->ctx->data[wf_args->id];

    while (1) {
        timestamp_start(wf_sll_init);
        struct sll work; /* don't bother initializing */
        timestamp_set_end(wf_sll_init);

        timestamp_start(wf_tw_mutex_lock);
        pthread_mutex_lock(&tw->mutex);
        timestamp_set_end(wf_tw_mutex_lock);

        timestamp_start(wf_ctx_mutex_lock);
        pthread_mutex_lock(&ctx->mutex);
        timestamp_set_end(wf_ctx_mutex_lock);

        /* wait for work */
        timestamp_start(wf_wait);
        while ((ctx->running && (!ctx->incomplete || !tw->queue.head)) ||
               (!ctx->running && (ctx->incomplete && !tw->queue.head))) {
            pthread_mutex_unlock(&ctx->mutex);
            pthread_cond_wait(&tw->cv, &tw->mutex);
            pthread_mutex_lock(&ctx->mutex);
        }
        timestamp_set_end(wf_wait);

        if (!ctx->running && !ctx->incomplete && !tw->queue.head) {
            pthread_mutex_unlock(&ctx->mutex);
            pthread_mutex_unlock(&tw->mutex);
            break;
        }

        pthread_mutex_unlock(&ctx->mutex);

        /* moves entire queue into work and clears out queue */
        timestamp_start(wf_move_queue);
        sll_move(&work, &tw->queue);
        timestamp_set_end(wf_move_queue);

        #if defined(DEBUG) && defined (QPTPOOL_QUEUE_SIZE)
        pthread_mutex_lock(&print_mutex);
        tw->queue.size = work.size;

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        fprintf(stderr, "qptpool_size %" PRIu64 " ", since_epoch(&now) - epoch);

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

        timestamp_start(wf_process_queue);

        /* process all work */
        size_t work_count = 0;

        timestamp_start(wf_get_queue_head);
        struct node * w = sll_head_node(&work);
        timestamp_end(ctx->buffers, wf_args->id, "wf_get_queue_head", wf_get_queue_head);

        while (w) {
            timestamp_start(wf_process_work);
            struct queue_item * qi = sll_node_data(w);

            tw->threads_successful += !qi->func(ctx, wf_args->id, qi->work, wf_args->args);
            timestamp_end(ctx->buffers, wf_args->id, "wf_process_work", wf_process_work);

            timestamp_start(wf_next_work);
            w = sll_next_node(w);
            timestamp_end(ctx->buffers, wf_args->id, "wf_next_work", wf_next_work);

            work_count++;
        }

        timestamp_set_end(wf_process_queue);

        timestamp_start(wf_cleanup);
        sll_destroy(&work, free);
        tw->threads_started += work_count;

        pthread_mutex_lock(&ctx->mutex);
        ctx->incomplete -= work_count;
        pthread_mutex_unlock(&ctx->mutex);
        timestamp_set_end(wf_cleanup);

        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        timestamp_print(ctx->buffers, wf_args->id, "wf_sll_init",       wf_sll_init);
        timestamp_print(ctx->buffers, wf_args->id, "wf_tw_mutex_lock",  wf_tw_mutex_lock);
        timestamp_print(ctx->buffers, wf_args->id, "wf_ctx_mutex_lock", wf_ctx_mutex_lock);
        timestamp_print(ctx->buffers, wf_args->id, "wf_wait",           wf_wait);
        timestamp_print(ctx->buffers, wf_args->id, "wf_move",           wf_move_queue);
        timestamp_print(ctx->buffers, wf_args->id, "wf_process_queue",  wf_process_queue);
        timestamp_print(ctx->buffers, wf_args->id, "wf_cleanup",        wf_cleanup);
        #endif
    }

    timestamp_start(wf_broadcast);
    for(size_t i = 0; i < ctx->size; i++) {
        pthread_cond_broadcast(&ctx->data[i].cv);
    }
    timestamp_end(ctx->buffers, wf_args->id, "wf_broadcast", wf_broadcast);

    free(args);

    timestamp_end(ctx->buffers, wf_args->id, "wf", wf);

    return NULL;
}

struct QPTPool * QPTPool_init(const size_t threads
                              #if defined(DEBUG) && defined(PER_THREAD_STATS)
                              , struct OutputBuffers * buffers
                              #endif
    ) {
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

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    ctx->buffers = buffers;
    #endif

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
            sll_destroy(&ctx->data[i].queue, NULL);
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
