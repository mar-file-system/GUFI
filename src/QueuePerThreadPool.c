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

#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "debug.h"

/* The context for a single thread in QPTPool */
struct QPTPoolThreadData {
    sll_t queue;        /* generally push into this queue */
    sll_t deferred;     /* push into here if queue is too big; pop when queue is empty */
    pthread_mutex_t mutex;
    pthread_cond_t cv;
    size_t next_queue;
    pthread_t thread;
    uint64_t threads_started;
    uint64_t threads_successful;
};

/* struct to pass into pthread_create */
struct worker_function_args {
    QPTPool_t *ctx;
    size_t id;
};

/* struct that is created when something is enqueued */
struct queue_item {
    QPTPoolFunc_t func;
    void *work;
};

static void *worker_function(void *args) {
    timestamp_create_start(wf);

    struct worker_function_args *wf_args = (struct worker_function_args *) args;
    /* Not checking arguments */

    QPTPool_t *ctx = wf_args->ctx;

    const size_t id = wf_args->id;

    QPTPoolThreadData_t *tw = &ctx->data[id];

    while (1) {
        timestamp_create_start(wf_sll_init);
        sll_t work; /* don't bother initializing */
        timestamp_set_end(wf_sll_init);

        timestamp_create_start(wf_tw_mutex_lock);
        pthread_mutex_lock(&tw->mutex);
        timestamp_set_end(wf_tw_mutex_lock);

        timestamp_create_start(wf_ctx_mutex_lock);
        pthread_mutex_lock(&ctx->mutex);
        timestamp_set_end(wf_ctx_mutex_lock);

        /* wait for work */
        timestamp_create_start(wf_wait);
        while (
            /* running, but no work in pool */
            (ctx->running && !ctx->incomplete) ||
            /*
             * not running and still have work in
             * other threads, just not this one
             */
            (!ctx->running && ctx->incomplete &&
             !tw->queue.size && !tw->deferred.size)
            ) {
            pthread_mutex_unlock(&ctx->mutex);
            pthread_cond_wait(&tw->cv, &tw->mutex);
            pthread_mutex_lock(&ctx->mutex);
        }
        timestamp_set_end(wf_wait);

        if (!ctx->running && !ctx->incomplete && !tw->queue.head && !tw->deferred.size) {
            pthread_mutex_unlock(&ctx->mutex);
            pthread_mutex_unlock(&tw->mutex);
            break;
        }

        /* moves entire queue into work and clears out queue */
        timestamp_create_start(wf_move_queue);
        if (tw->queue.size) {
            pthread_mutex_unlock(&ctx->mutex);
            sll_move(&work, &tw->queue);
        }
        else {
            sll_move(&work, &tw->deferred);
            pthread_mutex_unlock(&ctx->mutex);
        }
        timestamp_set_end(wf_move_queue);

        #if defined(DEBUG) && defined (QPTPOOL_QUEUE_SIZE)
        pthread_mutex_lock(&ctx->mutex);
        pthread_mutex_lock(&print_mutex);
        tw->queue.size = work.size;

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        fprintf(stderr, "qptpool_size %" PRIu64 " ", since_epoch(&now) - epoch);

        size_t sum = 0;
        for(size_t i = 0; i < ctx->nthreads; i++) {
            fprintf(stderr, "%zu ", ctx->data[i].queue.size);
            sum += ctx->data[i].queue.size;
            fprintf(stderr, "%zu ", ctx->data[i].deferred.size);
            sum += ctx->data[i].deferred.size;
        }
        fprintf(stderr, "%zu\n", sum);
        tw->queue.size = 0;
        pthread_mutex_unlock(&print_mutex);
        pthread_mutex_unlock(&ctx->mutex);
        #endif

        pthread_mutex_unlock(&tw->mutex);

        timestamp_create_start(wf_process_queue);

        /* process all work */
        size_t work_count = 0;

        timestamp_create_start(wf_get_queue_head);
        sll_node_t *w = sll_head_node(&work);
        timestamp_end_print(ctx->buffers, id, "wf_get_queue_head", wf_get_queue_head);

        while (w) {
            timestamp_create_start(wf_process_work);
            struct queue_item *qi = sll_node_data(w);

            tw->threads_successful += !qi->func(ctx, id, qi->work, ctx->args);
            timestamp_end_print(ctx->buffers, id, "wf_process_work", wf_process_work);

            timestamp_create_start(wf_next_work);
            w = sll_next_node(w);
            timestamp_end_print(ctx->buffers, id, "wf_next_work", wf_next_work);

            work_count++;
        }

        timestamp_set_end(wf_process_queue);

        timestamp_create_start(wf_cleanup);
        sll_destroy(&work, free);
        tw->threads_started += work_count;

        pthread_mutex_lock(&ctx->mutex);
        ctx->incomplete -= work_count;
        pthread_mutex_unlock(&ctx->mutex);
        timestamp_set_end(wf_cleanup);

        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        timestamp_print(ctx->buffers, id, "wf_sll_init",       wf_sll_init);
        timestamp_print(ctx->buffers, id, "wf_tw_mutex_lock",  wf_tw_mutex_lock);
        timestamp_print(ctx->buffers, id, "wf_ctx_mutex_lock", wf_ctx_mutex_lock);
        timestamp_print(ctx->buffers, id, "wf_wait",           wf_wait);
        timestamp_print(ctx->buffers, id, "wf_move",           wf_move_queue);
        timestamp_print(ctx->buffers, id, "wf_process_queue",  wf_process_queue);
        timestamp_print(ctx->buffers, id, "wf_cleanup",        wf_cleanup);
        #endif
    }

    timestamp_create_start(wf_broadcast);
    for(size_t i = 0; i < ctx->nthreads; i++) {
        pthread_cond_broadcast(&ctx->data[i].cv);
    }
    timestamp_end_print(ctx->buffers, id, "wf_broadcast", wf_broadcast);

    free(wf_args);

    timestamp_end_print(ctx->buffers, id, "wf", wf);

    return NULL;
}

static size_t QPTPool_round_robin(const size_t id, const size_t prev, const size_t threads,
                                  void *data, void *args) {
    (void) id; (void) data; (void) args;
    return (prev + 1) % threads;
}

QPTPool_t *QPTPool_init(const size_t nthreads,
                        void *args,
                        QPTPoolNextFunc_t next,
                        void *next_args,
                        const uint64_t queue_limit
                        #if defined(DEBUG) && defined(PER_THREAD_STATS)
                        , struct OutputBuffers *buffers
                        #endif
    ) {
    if (!nthreads) {
        return NULL;
    }

    QPTPool_t *ctx = malloc(sizeof(QPTPool_t));

    ctx->data = calloc(nthreads, sizeof(QPTPoolThreadData_t));

    ctx->nthreads = nthreads;
    ctx->queue_limit = queue_limit;
    ctx->args = args;
    ctx->next = QPTPool_round_robin;
    ctx->next_args = NULL;
    if (next) {
        ctx->next = next;
        ctx->next_args = next_args;
    }
    pthread_mutex_init(&ctx->mutex, NULL);
    ctx->running = 1;
    ctx->incomplete = 0;

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    ctx->buffers = buffers;
    #endif

    size_t started = 0;
    for(size_t i = 0; i < nthreads; i++) {
        QPTPoolThreadData_t *data = &ctx->data[i];
        sll_init(&data->queue);
        sll_init(&data->deferred);
        pthread_mutex_init(&data->mutex, NULL);
        pthread_cond_init(&data->cv, NULL);
        data->next_queue = i;
        data->thread = 0;
        data->threads_started = 0;
        data->threads_successful = 0;

        struct worker_function_args *wf_args = calloc(1, sizeof(struct worker_function_args));
        wf_args->ctx = ctx;
        wf_args->id = i;
        started += !pthread_create(&data->thread, NULL, worker_function, wf_args);
    }

    if (started != ctx->nthreads) {
        QPTPool_wait(ctx);
        QPTPool_destroy(ctx);
        ctx = NULL;
    }

    return ctx;
}

/* id selects the next_queue variable to use, not where the work will be placed */
void QPTPool_enqueue(QPTPool_t *ctx, const size_t id, QPTPoolFunc_t func, void *new_work) {
    /* Not checking arguments */

    struct queue_item *qi = malloc(sizeof(struct queue_item));
    qi->func = func; /* if no function is provided, the thread will segfault when it processes this item */
    qi->work = new_work;

    QPTPoolThreadData_t *data = &ctx->data[id];
    QPTPoolThreadData_t *next = &ctx->data[data->next_queue];

    pthread_mutex_lock(&next->mutex);
    if (!ctx->queue_limit || (next->queue.size > ctx->queue_limit)) {
        sll_push(&next->deferred, qi);
    }
    else {
        sll_push(&next->queue, qi);
    }
    pthread_mutex_unlock(&next->mutex);

    pthread_mutex_lock(&ctx->mutex);
    ctx->incomplete++;
    pthread_mutex_unlock(&ctx->mutex);

    pthread_cond_broadcast(&ctx->data[data->next_queue].cv);

    data->next_queue = ctx->next(id, data->next_queue, ctx->nthreads,
                                 new_work, ctx->next_args);
}

void QPTPool_wait(QPTPool_t *ctx) {
    /* Not checking arguments */

    pthread_mutex_lock(&ctx->mutex);
    ctx->running = 0;
    pthread_mutex_unlock(&ctx->mutex);
    for(size_t i = 0; i < ctx->nthreads; i++) {
        QPTPoolThreadData_t *data = &ctx->data[i];
        pthread_mutex_lock(&data->mutex);
        pthread_cond_broadcast(&data->cv);
        pthread_mutex_unlock(&data->mutex);
    }

    for(size_t i = 0; i < ctx->nthreads; i++) {
        pthread_join(ctx->data[i].thread, NULL);
    }
}

void QPTPool_destroy(QPTPool_t *ctx) {
    /* Not checking arguments */

    for(size_t i = 0; i < ctx->nthreads; i++) {
        QPTPoolThreadData_t *data = &ctx->data[i];
        data->threads_successful = 0;
        data->threads_started = 0;
        data->thread = 0;
        pthread_cond_destroy(&data->cv);
        pthread_mutex_destroy(&data->mutex);
        sll_destroy(&data->deferred, NULL);
        sll_destroy(&data->queue, NULL);
    }

    pthread_mutex_destroy(&ctx->mutex);
    free(ctx->data);
    free(ctx);
}

uint64_t QPTPool_threads_started(QPTPool_t *ctx) {
    /* Not checking arguments */

    uint64_t sum = 0;
    for(size_t i = 0; i < ctx->nthreads; i++) {
        sum += ctx->data[i].threads_started;
    }
    return sum;
}

uint64_t QPTPool_threads_completed(QPTPool_t *ctx) {
    /* Not checking arguments */

    uint64_t sum = 0;
    for(size_t i = 0; i < ctx->nthreads; i++) {
        sum += ctx->data[i].threads_successful;
    }
    return sum;
}
