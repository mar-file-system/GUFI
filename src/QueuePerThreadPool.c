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



#include <pthread.h>
#include <stdlib.h>

#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "debug.h"
#include "utils.h"

typedef enum {
    INITIALIZED,
    RUNNING,
    STOPPING,
} QPTPoolState_t;

/* The context for a single thread in QPTPool */
typedef struct QPTPoolThreadData {
    sll_t claimed;          /* work items that have already been claimed by this thread */
    pthread_mutex_t claimed_mutex;

    sll_t waiting;         /* generally push into this queue */
    sll_t deferred;        /* push into here if waiting queue is too big; pop when waiting queue is empty */
    pthread_mutex_t mutex;
    pthread_cond_t cv;
    size_t next_queue;     /* push to this thread when enqueuing */
    size_t steal_from;     /* start search at this thread */
    pthread_t thread;
    uint64_t threads_started;
    uint64_t threads_successful;
} QPTPoolThreadData_t;

/* The Queue Per Thread Pool context */
struct QPTPool {
    size_t nthreads;
    uint64_t queue_limit;    /* if the waiting queue reaches this size, push to the deferred queue instead */
    struct {
        uint64_t num;        /* if there is work to steal from a queue, take */
        uint64_t denom;      /* (queue.size * num / denom) items from the front */
    } steal;

    void *args;

    struct {
        QPTPoolNextFunc_t func;
        void *args;
    } next;

    pthread_mutex_t mutex;
    pthread_cond_t cv;
    QPTPoolState_t state;
    uint64_t incomplete;

    QPTPoolThreadData_t *data;

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    struct OutputBuffers *debug_buffers;
    #endif
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

/*
 * Loop through neighbors, looking for work items.
 * If there are work items, take at least one.
 */
#define steal(ctx, id, start, end, mutex_name, queue_name, rc)                     \
    QPTPoolThreadData_t *tw = &ctx->data[id];                                      \
                                                                                   \
    for(size_t i = start; i < end; i++) {                                          \
        if (id == i) {                                                             \
            continue;                                                              \
        }                                                                          \
                                                                                   \
        QPTPoolThreadData_t *target = &ctx->data[i];                               \
                                                                                   \
        if (pthread_mutex_trylock(&target->mutex_name) == 0) {                     \
            if (target->queue_name.size) {                                         \
                /* always take at least 1 */                                       \
                const uint64_t take = max(                                         \
                    target->queue_name.size * ctx->steal.num / ctx->steal.denom,   \
                    1);                                                            \
                sll_move_first(&tw->waiting, &target->queue_name, take);           \
                pthread_mutex_unlock(&target->mutex_name);                         \
                rc = tw->waiting.size;                                             \
                break;                                                             \
            }                                                                      \
                                                                                   \
            pthread_mutex_unlock(&target->mutex_name);                             \
        }                                                                          \
    }

static uint64_t steal_waiting(QPTPool_t *ctx, const size_t id,
                              const size_t start, const size_t end) {
    uint64_t rc = 0;
    steal(ctx, id, start, end, mutex, waiting, rc);
    return rc;
}

static uint64_t steal_deferred(QPTPool_t *ctx, const size_t id,
                               const size_t start, const size_t end) {
    uint64_t rc = 0;
    steal(ctx, id, start, end, mutex, deferred, rc);
    return rc;
}

/*
 * QueuePerThreadPool threads normally claim their entire waiting or
 * deferred queue at once instead of popping work items off a queue
 * one at a time to reduce contention on the selected queue. However,
 * this scheme makes it possible to cause starvation when a work item
 * takes a long time to complete and has more work items queued up
 * after it.
 *
 * Stealing claimed work attempts to prevent starvation, reproducing
 * the effect of threads popping off individual work items. This only
 * happens after all waiting and deferred queues have been checked and
 * found to be empty, and so should not be called frequently.
 */
static uint64_t steal_claimed(QPTPool_t *ctx, const size_t id,
                              const size_t start, const size_t end) {
    uint64_t rc = 0;
    steal(ctx, id, start, end, claimed_mutex, claimed, rc);
    return rc;
}

/*
 * If there's nothing waiting in this thread's work queues but
 * there is still work to complete, try to steal some work
 * from other threads.
 *
 * Assumes ctx->mutex and tw->mutex are locked.
 */
static void maybe_steal_work(QPTPool_t *ctx, QPTPoolThreadData_t *tw, size_t id) {
    if (!ctx->steal.num || (ctx->nthreads < 2) ||
        tw->waiting.size || tw->deferred.size ||
        !ctx->incomplete) {
        return;
    }

    /*
     * search [steal_from, nthreads) and then [0, steal_from)
     * increment steal_from at the end
     *
     * use short circuiting to stop searching
     *
     * do this to always start searching from different locations
     */
    (void) (
        (steal_waiting( ctx, id, tw->steal_from, ctx->nthreads)  == 0) &&
        (steal_waiting( ctx, id, 0,              tw->steal_from) == 0) &&
        (steal_deferred(ctx, id, tw->steal_from, ctx->nthreads)  == 0) &&
        (steal_deferred(ctx, id, 0,              tw->steal_from) == 0) &&
        /*
         * if still can't find anything, try the claimed queue
         *
         * this should only be called if there is some work that
         * is taking so long that the rest of the threads have run
         * out of work, so this should not happen too often
         */
        (steal_claimed( ctx, id, tw->steal_from, ctx->nthreads)  == 0) &&
        (steal_claimed( ctx, id, 0,              tw->steal_from) == 0)
    );

    tw->steal_from = (tw->steal_from + 1) % ctx->nthreads;
}

/*
 * wait_for_work() -
 *    Assumes ctx->mutex is locked.
 */
static void wait_for_work(QPTPool_t *ctx, QPTPoolThreadData_t *tw) {
    while (
        /* running, but no work in pool */
        ((ctx->state == RUNNING) && !ctx->incomplete) ||
        /*
         * not running and still have work in
         * other threads, just not this one
         */
        ((ctx->state == STOPPING) && ctx->incomplete &&
         !tw->waiting.size && !tw->deferred.size)
        ) {
        pthread_mutex_unlock(&ctx->mutex);
        pthread_cond_wait(&tw->cv, &tw->mutex);
        pthread_mutex_lock(&ctx->mutex);
    }
}

/*
 * claim_work() -
 *    Assumes tw->mutex is locked.
 */
static void claim_work(QPTPoolThreadData_t *tw) {
    /* move entire queue into work and clear out queue */
    pthread_mutex_lock(&tw->claimed_mutex);
    if (tw->waiting.size) {
        sll_move(&tw->claimed, &tw->waiting);
    }
    else {
        sll_move(&tw->claimed, &tw->deferred);
    }
    pthread_mutex_unlock(&tw->claimed_mutex);
}

#if defined(DEBUG) && defined (QPTPOOL_QUEUE_SIZE)
static void dump_queue_size_stats(QPTPool_t *ctx, QPTPoolThreadData_t *tw) {
    pthread_mutex_lock(&ctx->mutex);
    pthread_mutex_lock(&print_mutex);
    tw->waiting.size = tw->claimed.size;

    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    fprintf(stderr, "qptpool_size %" PRIu64 " ", since_epoch(&now) - epoch);

    size_t sum = 0;
    for(size_t i = 0; i < ctx->nthreads; i++) {
        fprintf(stderr, "%zu ", ctx->data[i].waiting.size);
        sum += ctx->data[i].waiting.size;
        fprintf(stderr, "%zu ", ctx->data[i].deferred.size);
        sum += ctx->data[i].deferred.size;
    }
    fprintf(stderr, "%zu\n", sum);
    tw->waiting.size = 0;
    pthread_mutex_unlock(&print_mutex);
    pthread_mutex_unlock(&ctx->mutex);
}
#endif

/*
 * process_work() -
 *    Returns number of work items handled.
 */
static size_t process_work(QPTPool_t *ctx, QPTPoolThreadData_t *tw, size_t id) {
    /* process all work */
    size_t work_count = 0;

    /*
     * pop work item off before it is processed so that if another
     * thread steals from the claimed queue, the current claimed
     * work will not be re-run
     *
     * this has the side effect of moving 2 frees into the loop
     * instead of batching all of them after processing the work
     *
     * tradeoffs:
     *     more locking
     *     delayed work
     *     lower memory utilization
     */
    timestamp_create_start(wf_get_queue_head);
    pthread_mutex_lock(&tw->claimed_mutex);
    struct queue_item *qi = (struct queue_item *) sll_pop(&tw->claimed);
    pthread_mutex_unlock(&tw->claimed_mutex);
    timestamp_end_print(ctx->debug_buffers, id, "wf_get_queue_head", wf_get_queue_head);

    while (qi) {
        timestamp_create_start(wf_process_work);
        tw->threads_successful += !qi->func(ctx, id, qi->work, ctx->args);
        free(qi);
        timestamp_end_print(ctx->debug_buffers, id, "wf_process_work", wf_process_work);

        timestamp_create_start(wf_next_work);
        pthread_mutex_lock(&tw->claimed_mutex);
        qi = (struct queue_item *) sll_pop(&tw->claimed);
        pthread_mutex_unlock(&tw->claimed_mutex);
        timestamp_end_print(ctx->debug_buffers, id, "wf_next_work", wf_next_work);

        work_count++;
    }

    return work_count;
}

static void *worker_function(void *args) {
    timestamp_create_start(wf);

    struct worker_function_args *wf_args = (struct worker_function_args *) args;
    /* Not checking arguments */

    QPTPool_t *ctx = wf_args->ctx;

    const size_t id = wf_args->id;

    QPTPoolThreadData_t *tw = &ctx->data[id];

    while (1) {
        timestamp_create_start(wf_tw_mutex_lock);
        pthread_mutex_lock(&tw->mutex);
        timestamp_set_end(wf_tw_mutex_lock);

        timestamp_create_start(wf_ctx_mutex_lock);
        pthread_mutex_lock(&ctx->mutex);
        timestamp_set_end(wf_ctx_mutex_lock);

        maybe_steal_work(ctx, tw, id);

        timestamp_create_start(wf_wait);
        wait_for_work(ctx, tw);
        timestamp_set_end(wf_wait);

        if ((ctx->state == STOPPING) && !ctx->incomplete &&
            !tw->waiting.head && !tw->deferred.size) {
            pthread_mutex_unlock(&ctx->mutex);
            pthread_mutex_unlock(&tw->mutex);
            break;
        }

        pthread_mutex_unlock(&ctx->mutex);

        timestamp_create_start(wf_move_queue);
        claim_work(tw);
        timestamp_set_end(wf_move_queue);

        #if defined(DEBUG) && defined (QPTPOOL_QUEUE_SIZE)
        dump_queue_size_stats(ctx, tw);
        #endif

        pthread_mutex_unlock(&tw->mutex);

        timestamp_create_start(wf_process_queue);
        const size_t work_count = process_work(ctx, tw, id);
        timestamp_set_end(wf_process_queue);

        timestamp_create_start(wf_cleanup);
        tw->threads_started += work_count;

        pthread_mutex_lock(&ctx->mutex);
        ctx->incomplete -= work_count;
        pthread_cond_broadcast(&ctx->cv);
        pthread_mutex_unlock(&ctx->mutex);
        timestamp_set_end(wf_cleanup);

        #if defined(DEBUG) && defined(PER_THREAD_STATS)
        timestamp_print(ctx->debug_buffers, id, "wf_tw_mutex_lock",  wf_tw_mutex_lock);
        timestamp_print(ctx->debug_buffers, id, "wf_ctx_mutex_lock", wf_ctx_mutex_lock);
        timestamp_print(ctx->debug_buffers, id, "wf_wait",           wf_wait);
        timestamp_print(ctx->debug_buffers, id, "wf_move",           wf_move_queue);
        timestamp_print(ctx->debug_buffers, id, "wf_process_queue",  wf_process_queue);
        timestamp_print(ctx->debug_buffers, id, "wf_cleanup",        wf_cleanup);
        #endif
    }

    /* signal all threads to loop, just in case */
    timestamp_create_start(wf_broadcast);
    for(size_t i = 0; i < ctx->nthreads; i++) {
        QPTPoolThreadData_t *data = &ctx->data[i];
        pthread_mutex_lock(&data->mutex);
        pthread_cond_broadcast(&data->cv);
        pthread_mutex_unlock(&data->mutex);
    }
    timestamp_end_print(ctx->debug_buffers, id, "wf_broadcast", wf_broadcast);

    free(wf_args);

    timestamp_end_print(ctx->debug_buffers, id, "wf", wf);

    return NULL;
}

static size_t QPTPool_round_robin(const size_t id, const size_t prev, const size_t threads,
                                  void *data, void *args) {
    (void) id; (void) data; (void) args;
    return (prev + 1) % threads;
}

QPTPool_t *QPTPool_init(const size_t nthreads, void *args) {
    return QPTPool_init_with_props(nthreads, args, NULL, NULL, 0, 0, 0
                                   #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                   , NULL
                                   #endif
    );
}

int QPTPool_set_next(QPTPool_t *ctx, QPTPoolNextFunc_t func, void *args) {
    if (!ctx || !func) {
        return 1;
    }

    pthread_mutex_lock(&ctx->mutex);
    if (ctx->state != INITIALIZED) {
        pthread_mutex_unlock(&ctx->mutex);
        return 1;
    }

    ctx->next.func = func;
    ctx->next.args = args;
    pthread_mutex_unlock(&ctx->mutex);
    return 0;
}

int QPTPool_set_queue_limit(QPTPool_t *ctx, const uint64_t queue_limit) {
    if (!ctx) {
        return 1;
    }

    pthread_mutex_lock(&ctx->mutex);
    if (ctx->state != INITIALIZED) {
        pthread_mutex_unlock(&ctx->mutex);
        return 1;
    }

    ctx->queue_limit = queue_limit;
    pthread_mutex_unlock(&ctx->mutex);
    return 0;
}

int QPTPool_set_steal(QPTPool_t *ctx, const uint64_t num, const uint64_t denom) {
    if (!ctx) {
        return 1;
    }

    pthread_mutex_lock(&ctx->mutex);
    if (ctx->state != INITIALIZED) {
        pthread_mutex_unlock(&ctx->mutex);
        return 1;
    }

    ctx->steal.num = num;
    ctx->steal.denom = denom;
    pthread_mutex_unlock(&ctx->mutex);
    return 0;
}

#if defined(DEBUG) && defined(PER_THREAD_STATS)
int QPTPool_set_debug_buffers(QPTPool_t *ctx, struct OutputBuffers *debug_buffers) {
    if (!ctx) {
        return 1;
    }

    pthread_mutex_lock(&ctx->mutex);
    if (ctx->state != INITIALIZED) {
        pthread_mutex_unlock(&ctx->mutex);
        return 1;
    }

    ctx->debug_buffers = debug_buffers;
    pthread_mutex_unlock(&ctx->mutex);
    return 0;
}
#endif

int QPTPool_get_nthreads(QPTPool_t *ctx, size_t *nthreads) {
    if (!ctx) {
        return 1;
    }

    if (nthreads) {
        *nthreads = ctx->nthreads;
    }

    return 0;
}

int QPTPool_get_args(QPTPool_t *ctx, void **args) {
    if (!ctx) {
        return 1;
    }

    if (args) {
        *args = ctx->args;
    }

    return 0;
}

int QPTPool_get_next(QPTPool_t *ctx, QPTPoolNextFunc_t *func, void **args) {
    if (!ctx) {
        return 1;
    }

    if (func) {
        *func = ctx->next.func;
    }

    if (args) {
        *args = ctx->next.args;
    }

    return 0;
}

int QPTPool_get_queue_limit(QPTPool_t *ctx, uint64_t *queue_limit) {
    if (!ctx) {
        return 1;
    }

    if (queue_limit) {
        *queue_limit = ctx->queue_limit;
    }

    return 0;
}

int QPTPool_get_steal(QPTPool_t *ctx, uint64_t *num, uint64_t *denom) {
    if (!ctx) {
        return 1;
    }

    if (num) {
        *num = ctx->steal.num;
    }

    if (denom) {
        *denom = ctx->steal.denom;
    }

    return 0;
}

#if defined(DEBUG) && defined(PER_THREAD_STATS)
int QPTPool_get_debug_buffers(QPTPool_t *ctx, struct OutputBuffers **debug_buffers) {
    if (!ctx) {
        return 1;
    }

    if (debug_buffers) {
        *debug_buffers = ctx->debug_buffers;
    }

    return 0;
}
#endif

QPTPool_t *QPTPool_init_with_props(const size_t nthreads,
                                   void *args,
                                   QPTPoolNextFunc_t next_func,
                                   void *next_args,
                                   const uint64_t queue_limit,
                                   const uint64_t steal_num,
                                   const uint64_t steal_denom
                                   #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                   , struct OutputBuffers *debug_buffers
                                   #endif
    ) {
    if (!nthreads) {
        return NULL;
    }

    QPTPool_t *ctx = malloc(sizeof(QPTPool_t));
    if (!ctx) {
        return NULL;
    }

    ctx->nthreads = nthreads;
    ctx->queue_limit = queue_limit;
    ctx->steal.num = steal_num;
    ctx->steal.denom = steal_denom;
    ctx->args = args;
    ctx->next.func = next_func?next_func:QPTPool_round_robin;
    ctx->next.args = next_args;
    pthread_mutex_init(&ctx->mutex, NULL);
    pthread_cond_init(&ctx->cv, NULL);
    ctx->state = INITIALIZED;
    ctx->incomplete = 0;

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    ctx->debug_buffers = debug_buffers;
    #endif

    ctx->data = calloc(nthreads, sizeof(QPTPoolThreadData_t));
    if (!ctx->data) {
        free(ctx);
        return NULL;
    }

    /* set up thread data, but not threads */
    for(size_t i = 0; i < nthreads; i++) {
        QPTPoolThreadData_t *data = &ctx->data[i];
        sll_init(&data->claimed);
        pthread_mutex_init(&data->claimed_mutex, NULL);
        sll_init(&data->waiting);
        sll_init(&data->deferred);
        pthread_mutex_init(&data->mutex, NULL);
        pthread_cond_init(&data->cv, NULL);
        data->next_queue = i;
        data->steal_from = i;
        data->threads_started = 0;
        data->threads_successful = 0;
    }

    return ctx;
}

int QPTPool_start(QPTPool_t *ctx) {
    if (!ctx) {
        return 1;
    }

    pthread_mutex_lock(&ctx->mutex);
    if (ctx->state == RUNNING) {
        pthread_mutex_unlock(&ctx->mutex);
        return 1;
    }

    ctx->state = RUNNING;

    size_t started = 0;
    for(size_t i = 0; i < ctx->nthreads; i++) {
        QPTPoolThreadData_t *data = &ctx->data[i];

        struct worker_function_args *wf_args = malloc(sizeof(struct worker_function_args));
        wf_args->ctx = ctx;
        wf_args->id = i;
        started += !pthread_create(&data->thread, NULL, worker_function, wf_args);
    }
    pthread_mutex_unlock(&ctx->mutex);

    if (started != ctx->nthreads) {
        QPTPool_stop(ctx);
        return 1;
    }

    return 0;
}

/* id selects the next_queue variable to use, not where the work will be placed */
QPTPool_enqueue_dst_t QPTPool_enqueue(QPTPool_t *ctx, const size_t id, QPTPoolFunc_t func, void *new_work) {
    /* Not checking arguments */

    struct queue_item *qi = malloc(sizeof(struct queue_item));
    qi->func = func; /* if no function is provided, the thread will segfault when it processes this item */
    qi->work = new_work;

    QPTPoolThreadData_t *data = &ctx->data[id];
    QPTPoolThreadData_t *next = &ctx->data[data->next_queue];

    /* have to calculate next_queue before new_work is modified */
    data->next_queue = ctx->next.func(id, data->next_queue, ctx->nthreads,
                                      new_work, ctx->next.args);

    QPTPool_enqueue_dst_t ret = QPTPool_enqueue_ERROR;

    pthread_mutex_lock(&next->mutex);
    if (!ctx->queue_limit || (next->waiting.size < ctx->queue_limit)) {
        sll_push(&next->waiting, qi);
        ret = QPTPool_enqueue_WAIT;
    }
    else {
        sll_push(&next->deferred, qi);
        ret = QPTPool_enqueue_DEFERRED;
    }

    pthread_mutex_lock(&ctx->mutex);
    ctx->incomplete++;
    pthread_mutex_unlock(&ctx->mutex);

    pthread_cond_broadcast(&next->cv);
    pthread_mutex_unlock(&next->mutex);

    return ret;
}

QPTPool_enqueue_dst_t QPTPool_enqueue_here(QPTPool_t *ctx, const size_t id, QPTPool_enqueue_dst_t queue,
                                           QPTPoolFunc_t func, void *new_work) {
    /* Not checking other arguments */

    if ((queue != QPTPool_enqueue_WAIT) &&
        (queue != QPTPool_enqueue_DEFERRED)) {
        return QPTPool_enqueue_ERROR;
    }

    struct queue_item *qi = malloc(sizeof(struct queue_item));
    qi->func = func; /* if no function is provided, the thread will segfault when it processes this item */
    qi->work = new_work;

    QPTPoolThreadData_t *data = &ctx->data[id];

    pthread_mutex_lock(&data->mutex);
    if (queue == QPTPool_enqueue_WAIT) {
        sll_push(&data->waiting, qi);
    }
    else if (queue == QPTPool_enqueue_DEFERRED) {
        sll_push(&data->deferred, qi);
    }

    pthread_mutex_lock(&ctx->mutex);
    ctx->incomplete++;
    pthread_mutex_unlock(&ctx->mutex);

    pthread_cond_broadcast(&data->cv);
    pthread_mutex_unlock(&data->mutex);

    return queue;
}

uint64_t QPTPool_wait(QPTPool_t *ctx) {
    return QPTPool_wait_lte(ctx, 0);
}

uint64_t QPTPool_wait_lte(QPTPool_t *ctx, const uint64_t count) {
    if (!ctx) {
        return 0;
    }

    pthread_mutex_lock(&ctx->mutex);

    while ((ctx->state == RUNNING) &&
           (ctx->incomplete > count)) {
        pthread_cond_wait(&ctx->cv, &ctx->mutex);
    }

    const uint64_t incomplete = ctx->incomplete;

    pthread_mutex_unlock(&ctx->mutex);

    /*
     * if returned value is larger than count,
     * then this was called before QPTPool_start
     */
    return incomplete;
}

void QPTPool_stop(QPTPool_t *ctx) {
    if (!ctx) {
        return;
    }

    /* no need to call QPTPool_wait - just wait for threads to stop */

    pthread_mutex_lock(&ctx->mutex);
    const QPTPoolState_t prev = ctx->state;
    ctx->state = STOPPING;
    pthread_mutex_unlock(&ctx->mutex);

    if (prev == RUNNING) {
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
}

uint64_t QPTPool_threads_started(QPTPool_t *ctx) {
    if (!ctx) {
        return 0;
    }

    uint64_t sum = 0;
    for(size_t i = 0; i < ctx->nthreads; i++) {
        sum += ctx->data[i].threads_started;
    }
    return sum;
}

uint64_t QPTPool_threads_completed(QPTPool_t *ctx) {
    if (!ctx) {
        return 0;
    }

    uint64_t sum = 0;
    for(size_t i = 0; i < ctx->nthreads; i++) {
        sum += ctx->data[i].threads_successful;
    }
    return sum;
}

void QPTPool_destroy(QPTPool_t *ctx) {
    if (!ctx) {
        return;
    }

    for(size_t i = 0; i < ctx->nthreads; i++) {
        QPTPoolThreadData_t *data = &ctx->data[i];
        data->threads_successful = 0;
        data->threads_started = 0;
        data->thread = 0;
        pthread_cond_destroy(&data->cv);
        pthread_mutex_destroy(&data->mutex);
        /*
         * If QPTPool_start is called, queues will be empty at this point
         *
         * If QPTPool_start is not called, queues might not be empty since
         * enqueuing work without starting the worker threads is allowed,
         * so free() is called to clear out any unprocessed work items
         */
        sll_destroy(&data->deferred, free);
        sll_destroy(&data->waiting, free);
        pthread_mutex_destroy(&data->claimed_mutex);
        sll_destroy(&data->claimed, free);
    }

    pthread_cond_destroy(&ctx->cv);
    pthread_mutex_destroy(&ctx->mutex);
    free(ctx->data);
    free(ctx);
}
