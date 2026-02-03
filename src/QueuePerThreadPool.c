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
#include <stdio.h>
#include <stdlib.h>

#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#ifdef QPTPOOL_SWAP
#include "swap.h"
#endif
#include "utils.h"

typedef enum {
    INITIALIZED,
    RUNNING,
    STOPPING,
} QPTPoolState_t;

/* The context for a single thread in QPTPool */
typedef struct QPTPoolThreadData {
    sll_t claimed;           /* work items that have already been claimed by this thread */
    pthread_mutex_t claimed_mutex;

    sll_t waiting;           /* generally push into this queue */
    #ifdef QPTPOOL_SWAP
    struct Swap swap;        /* swap for this thread */
    #endif
    pthread_mutex_t mutex;
    pthread_cond_t cv;
    size_t next_queue;       /* push to this thread when enqueuing */
    size_t steal_from;       /* start search at this thread */
    pthread_t thread;
    uint64_t threads_started;
    uint64_t threads_successful;
} QPTPoolThreadData_t;

/* the actual Queue Per Thread Pool context */
typedef struct QPTPool {
    size_t nthreads;
    #ifdef QPTPOOL_SWAP
    uint64_t queue_limit;    /* if the waiting queue reaches this size, write to swap instead */
    const char *swap_prefix; /* only used if queue_limit > 0 */
    #endif
    struct {
        uint64_t num;        /* if there is work to steal from a queue, take */
        uint64_t denom;      /* max(queue.size * num / denom, 1) items from the front */
    } steal;

    void *args;

    struct {
        QPTPoolNextFunc_t func;
        void *args;
    } next;

    pthread_mutex_t mutex;
    pthread_cond_t cv;
    QPTPoolState_t state;

    /* counters */
    uint64_t incomplete;
    uint64_t swapped;

    QPTPoolThreadData_t *data;

    /*
     * arrays for threads to reference into
     * a bit of a layer violation, but makes work stealing more convenient
     */
    sll_t **claimed;
    pthread_mutex_t **claimed_mutex;
    sll_t **waiting;
    pthread_mutex_t **thread_mutex; /* &data[i].mutex */
} QPTPool_t;

/* struct that is returned to users and used by worker functions */
struct QPTPool_ctx {
    QPTPool_t *pool;
    size_t id;
};

/* struct that is created when something is enqueued */
struct queue_item {
    QPTPool_f func;
    void *work;
};

/*
 * Loop through neighbors, looking for work items.
 * If there are work items, take at least one.
 */
static inline uint64_t steal(QPTPool_t *pool, const size_t id, const size_t start,
                             sll_t **queue, pthread_mutex_t **mutexes) {
    size_t rc = 0;
    QPTPoolThreadData_t *tw = &pool->data[id];

    for(size_t i = 0; i < pool->nthreads; i++) {
        const size_t curr = (start + i) % pool->nthreads;
        if (curr == id) {
            continue;
        }

        /*
         * instead of accessing each pool->data[curr], which requires
         * variable name pasting, index into the work queue and mutex
         * arrays
         */
        pthread_mutex_t *mutex = mutexes[curr];
        sll_t *neighbor_work = queue[curr];

        if (pthread_mutex_trylock(mutex) == 0) {
            if (neighbor_work->size) {
                /* always take at least 1 */
                const uint64_t take = max(
                    neighbor_work->size * pool->steal.num / pool->steal.denom,
                    1);
                sll_move_append_first(&tw->waiting, neighbor_work, take);
                pthread_mutex_unlock(mutex);
                rc = tw->waiting.size;
                break;
            }

            pthread_mutex_unlock(mutex);
        }
    }

    return rc;
}

static inline uint64_t steal_waiting(QPTPool_t *pool, const size_t id, const size_t start) {
    return steal(pool, id, start, pool->waiting, pool->thread_mutex);
}

/*
 * QueuePerThreadPool normally claims an entire work queue at once
 * instead of popping work items off one at a time. This reduces the
 * contention on the work queues. However, this scheme makes it
 * possible to cause starvation when a work item takes a long time to
 * complete and has more work items queued up after it, all of which
 * have already been claimed and are not in the waiting queue.
 *
 * This function attempts to prevent starvation by stealing work that
 * has already been claimed, reproducing the effect of threads popping
 * off individual work items. This function is only called after the
 * all waiting queues have been checked and found to be empty, and so
 * should not be called frequently.
 */
static inline uint64_t steal_claimed(QPTPool_t *pool, const size_t id, const size_t start) {
    return steal(pool, id, start, pool->claimed, pool->claimed_mutex);
}

/*
 * If there's nothing waiting in this thread's work queues but
 * there is still work to complete, try to steal some work
 * from other threads.
 *
 * Assumes pool->mutex and tw->mutex are locked.
 */
static inline void maybe_steal_work(QPTPool_t *pool, QPTPoolThreadData_t *tw, size_t id) {
    if (!pool->steal.num || (pool->nthreads < 2) ||
        tw->waiting.size ||
        !pool->incomplete) {
        return;
    }

    /*
     * search other threads' queues
     *
     * use short circuiting to stop searching
     */
    (void) (
        (steal_waiting(pool, id, tw->steal_from) == 0) &&
        /* not stealing swapped work */

        /*
         * if still can't find anything, try the claimed queue
         *
         * this should only be called if there is some work that
         * is taking so long that the rest of the threads have run
         * out of work, so this should not happen too often
         */
        (steal_claimed(pool, id, tw->steal_from) == 0)
    );

    /* always start searching from different locations */
    tw->steal_from = (tw->steal_from + 1) % pool->nthreads;
}

/*
 * wait_for_work() -
 *     Assumes pool->mutex and tw->mutex are locked.
 */
static void wait_for_work(QPTPool_t *pool, QPTPoolThreadData_t *tw) {
    while (
        /* running, but no work in pool or current thread */
        ((pool->state == RUNNING) && ((!pool->incomplete && !pool->swapped) ||
                                     !tw->waiting.size)) ||
        /*
         * not running and still have work in
         * other threads, just not this one
         */
        ((pool->state == STOPPING) && (pool->incomplete || pool->swapped) &&
         !tw->waiting.size)
        ) {
        pthread_mutex_unlock(&pool->mutex);
        pthread_cond_wait(&tw->cv, &tw->mutex);
        pthread_mutex_lock(&pool->mutex);
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
        sll_move_append(&tw->claimed, &tw->waiting);
    }
    pthread_mutex_unlock(&tw->claimed_mutex);
}

/*
 * process_work() -
 *    Returns number of work items handled.
 */
static size_t process_work(QPTPool_ctx_t *ctx, QPTPoolThreadData_t *tw) {
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
    pthread_mutex_lock(&tw->claimed_mutex);
    struct queue_item *qi = (struct queue_item *) sll_pop_front(&tw->claimed);
    pthread_mutex_unlock(&tw->claimed_mutex);

    while (qi) {
        tw->threads_successful += !qi->func(ctx, qi->work);
        free(qi);

        pthread_mutex_lock(&tw->claimed_mutex);
        qi = (struct queue_item *) sll_pop_front(&tw->claimed);
        pthread_mutex_unlock(&tw->claimed_mutex);

        work_count++; /* increment for previous work item */
    }

    return work_count;
}

static void *worker_function(void *args) {
    /* Not checking arguments */

    struct QPTPool_ctx *ctx = (struct QPTPool_ctx *) args;

    QPTPool_t *pool = ctx->pool;
    QPTPoolThreadData_t *tw = &pool->data[ctx->id];

    while (1) {
        pthread_mutex_lock(&tw->mutex);
        pthread_mutex_lock(&pool->mutex);

        maybe_steal_work(pool, tw, ctx->id);
        wait_for_work(pool, tw);

        /* if stopping and entire thread pool is empty, this thread can exit */
        if ((pool->state == STOPPING) && !pool->incomplete && !pool->swapped) {
            pthread_mutex_unlock(&pool->mutex);
            pthread_mutex_unlock(&tw->mutex);
            break;
        }

        pthread_mutex_unlock(&pool->mutex);
        /* tw->mutex still locked */

        claim_work(tw);

        pthread_mutex_unlock(&tw->mutex);
        /* tw->waiting is now empty and can be pushed to */

        const size_t work_count = process_work(ctx, tw);

        /*
         * no need to lock - thread is modifying its own counter and
         * no other threads will modify it
         *
         * read by QPTPool_threads_started, but while there are still
         * work items waiting to be processed, the instantaneous value
         * doesn't matter
         */
        tw->threads_started += work_count;

        pthread_mutex_lock(&pool->mutex);
        pool->incomplete -= work_count;
        pthread_cond_broadcast(&pool->cv);
        pthread_mutex_unlock(&pool->mutex);
    }

    /* signal all threads to loop, just in case */
    for(size_t i = 0; i < pool->nthreads; i++) {
        QPTPoolThreadData_t *data = &pool->data[i];
        pthread_mutex_lock(&data->mutex);
        pthread_cond_broadcast(&data->cv);
        pthread_mutex_unlock(&data->mutex);
    }

    return NULL;
}

QPTPool_ctx_t *QPTPool_init(const size_t nthreads, void *args) {
    return QPTPool_init_with_props(nthreads, args, NULL, NULL, 0, "", 0, 0);
}

/* default function for selecting next thread to push to */
static size_t QPTPool_round_robin(const size_t id, const size_t prev, const size_t threads,
                                  void *data, void *args) {
    (void) id; (void) data; (void) args;
    const size_t next = prev + 1;
    return (next == threads)?0:next; /* avoid divmod */
}

QPTPool_ctx_t *QPTPool_init_with_props(const size_t nthreads, void *args,
                                       QPTPoolNextFunc_t next_func, void *next_args,
                                       const uint64_t queue_limit, const char *swap_prefix,
                                       const uint64_t steal_num, const uint64_t steal_denom) {
    #ifndef QPTPOOL_SWAP
    (void) queue_limit;
    (void) swap_prefix;
    #endif

    if (!nthreads) {
        return NULL;
    }

    QPTPool_t *pool = malloc(sizeof(QPTPool_t));
    pool->nthreads = nthreads;
    #ifdef QPTPOOL_SWAP
    pool->queue_limit = queue_limit;
    pool->swap_prefix = swap_prefix;
    #endif
    pool->steal.num = steal_num;
    pool->steal.denom = steal_denom;
    pool->args = args;
    pool->next.func = next_func?next_func:QPTPool_round_robin;
    pool->next.args = next_args;
    pthread_mutex_init(&pool->mutex, NULL);
    pthread_cond_init(&pool->cv, NULL);
    pool->state = INITIALIZED;
    pool->incomplete = 0;
    pool->swapped = 0;

    /* this can fail since nthreads is user input */
    pool->data          = calloc(nthreads, sizeof(QPTPoolThreadData_t));
    pool->claimed       = calloc(nthreads, sizeof(sll_t));
    pool->claimed_mutex = calloc(nthreads, sizeof(pthread_mutex_t));
    pool->waiting       = calloc(nthreads, sizeof(sll_t));
    pool->thread_mutex  = calloc(nthreads, sizeof(pthread_mutex_t));
    QPTPool_ctx_t *ctx  = calloc(nthreads, sizeof(QPTPool_ctx_t));
    if (!pool->data || !pool->claimed || !pool->claimed_mutex ||
        !pool->waiting || !pool->thread_mutex || !ctx) {
        free(pool->thread_mutex);
        free(pool->waiting);
        free(pool->claimed_mutex);
        free(pool->claimed);
        free(pool->data);
        free(pool);
        free(ctx);
        return NULL;
    }

    /* set up thread data, but not threads */
    int bad = 0;
    for(size_t i = 0; i < nthreads; i++) {
        QPTPoolThreadData_t *data = &pool->data[i];

        /* set references to pool first */
        pool->claimed[i]       = &data->claimed;
        pool->claimed_mutex[i] = &data->claimed_mutex;
        pool->waiting[i]       = &data->waiting;
        pool->thread_mutex[i]  = &data->mutex;

        sll_init(&data->claimed);
        pthread_mutex_init(&data->claimed_mutex, NULL);
        sll_init(&data->waiting);
        #ifdef QPTPOOL_SWAP
        swap_init(&data->swap);
        if (pool->queue_limit) {
            if (swap_start(&data->swap, pool->swap_prefix, i) != 0) {
                bad = 1; /* allow all threads to complete even if there are errors */
            }
        }
        #endif
        pthread_mutex_init(&data->mutex, NULL);
        pthread_cond_init(&data->cv, NULL);
        data->next_queue = i;
        data->steal_from = i;
        data->threads_started = 0;
        data->threads_successful = 0;

        ctx[i].pool = pool;
        ctx[i].id = i;
    }

    if (bad) {
        QPTPool_destroy(ctx);
        return NULL;
    }

     /*
      * the ctx array is returned instead of just ctx[0]
      *     - equivalent to ctx[0]
      *     - used to free entire array
      */
    return ctx;
}

int QPTPool_set_next(QPTPool_ctx_t *ctx, QPTPoolNextFunc_t func, void *args) {
    if (!ctx || !func) {
        return 1;
    }
    QPTPool_t *pool = ctx->pool;

    pthread_mutex_lock(&pool->mutex);
    if (pool->state != INITIALIZED) {
        pthread_mutex_unlock(&pool->mutex);
        return 1;
    }

    pool->next.func = func;
    pool->next.args = args;
    pthread_mutex_unlock(&pool->mutex);
    return 0;
}

#ifdef QPTPOOL_SWAP
int QPTPool_set_queue_limit(QPTPool_ctx_t *ctx, const uint64_t queue_limit) {
    if (!ctx) {
        return 1;
    }
    QPTPool_t *pool = ctx->pool;

    pthread_mutex_lock(&pool->mutex);
    if (pool->state != INITIALIZED) {
        pthread_mutex_unlock(&pool->mutex);
        return 1;
    }

    /* drop old swap data no matter what the new queue limit is */
    for(size_t i = 0; i < pool->nthreads; i++) {
        QPTPoolThreadData_t *data = &pool->data[i];
        swap_stop(&data->swap);
    }

    pool->swapped = 0;
    pool->queue_limit = queue_limit;

    /* create new swap space */
    int bad = 0;
    if (pool->queue_limit) {
        for(size_t i = 0; i < pool->nthreads; i++) {
            QPTPoolThreadData_t *data = &pool->data[i];
            if (swap_restart(&data->swap, pool->swap_prefix, i) != 0) {
                bad = 1;
            }
        }
    }

    pthread_mutex_unlock(&pool->mutex);
    return bad;
}

int QPTPool_set_swap_prefix(QPTPool_ctx_t *ctx, const char *swap_prefix) {
    if (!ctx) {
        return 1;
    }

    QPTPool_t *pool = ctx->pool;

    pthread_mutex_lock(&pool->mutex);
    if (pool->state != INITIALIZED) {
        pthread_mutex_unlock(&pool->mutex);
        return 1;
    }

    pool->swapped = 0;
    pool->swap_prefix = swap_prefix;

    int bad = 0;
    if (pool->queue_limit) {
        for(size_t i = 0; i < pool->nthreads; i++) {
            QPTPoolThreadData_t *data = &pool->data[i];
            if (swap_restart(&data->swap, pool->swap_prefix, i) != 0) {
                bad = 1;
            }
        }
    }

    pthread_mutex_unlock(&pool->mutex);
    return bad;
}
#endif

int QPTPool_set_steal(QPTPool_ctx_t *ctx, const uint64_t num, const uint64_t denom) {
    if (!ctx) {
        return 1;
    }

    QPTPool_t *pool = ctx->pool;

    pthread_mutex_lock(&pool->mutex);
    if (pool->state != INITIALIZED) {
        pthread_mutex_unlock(&pool->mutex);
        return 1;
    }

    pool->steal.num = num;
    pool->steal.denom = denom;
    pthread_mutex_unlock(&pool->mutex);
    return 0;
}

int QPTPool_get_nthreads(QPTPool_ctx_t *ctx, size_t *nthreads) {
    if (!ctx) {
        return 1;
    }

    if (nthreads) {
        *nthreads = ctx->pool->nthreads;
    }

    return 0;
}

int QPTPool_get_args(QPTPool_ctx_t *ctx, void **args) {
    if (!ctx) {
        return 1;
    }

    if (args) {
        *args = QPTPool_get_args_internal(ctx);
    }

    return 0;
}

int QPTPool_get_next(QPTPool_ctx_t *ctx, QPTPoolNextFunc_t *func, void **args) {
    if (!ctx) {
        return 1;
    }

    if (func) {
        *func = ctx->pool->next.func;
    }

    if (args) {
        *args = ctx->pool->next.args;
    }

    return 0;
}

#ifdef QPTPOOL_SWAP
int QPTPool_get_queue_limit(QPTPool_ctx_t *ctx, uint64_t *queue_limit) {
    if (!ctx) {
        return 1;
    }

    if (queue_limit) {
        *queue_limit = ctx->pool->queue_limit;
    }

    return 0;
}

int QPTPool_get_swap_prefix(QPTPool_ctx_t *ctx, const char **swap_prefix) {
    if (!ctx) {
        return 1;
    }

    if (swap_prefix) {
        *swap_prefix = ctx->pool->swap_prefix;
    }

    return 0;
}
#endif

int QPTPool_get_steal(QPTPool_ctx_t *ctx, uint64_t *num, uint64_t *denom) {
    if (!ctx) {
        return 1;
    }

    if (num) {
        *num = ctx->pool->steal.num;
    }

    if (denom) {
        *denom = ctx->pool->steal.denom;
    }

    return 0;
}

int QPTPool_start(QPTPool_ctx_t *ctx) {
    if (!ctx) {
        return 1;
    }

    QPTPool_t *pool = ctx->pool;

    pthread_mutex_lock(&pool->mutex);
    if (pool->state == RUNNING) {
        pthread_mutex_unlock(&pool->mutex);
        return 1;
    }

    pool->state = RUNNING;

    size_t started = 0;
    for(size_t i = 0; i < pool->nthreads; i++) {
        QPTPoolThreadData_t *data = &pool->data[i];

        started += !pthread_create(&data->thread, NULL, worker_function, &ctx[i]);
    }
    pthread_mutex_unlock(&pool->mutex);

    if (started != pool->nthreads) {
        QPTPool_stop(ctx);
        return 1;
    }

    return 0;
}

size_t QPTPool_get_id(QPTPool_ctx_t *ctx) {
    /* this function should only be called within a QPTPool_f, so ctx should be valid */
    return ctx->id;
}

void *QPTPool_get_args_internal(QPTPool_ctx_t *ctx) {
    /* this function should only be called within a QPTPool_f, so ctx should be valid */
    return ctx->pool->args;
}

#if QPTPOOL_SWAP
int QPTPool_generic_serialize_and_free(const int fd, QPTPool_f func, void *work, const size_t len, size_t *size) {
    if ((write_size(fd, &func, sizeof(func)) != sizeof(func)) ||
        (write_size(fd, &len, sizeof(len)) != sizeof(len)) ||
        (write_size(fd, work, len) != (ssize_t) len)) {
        return 1;
    }

    *size += sizeof(func) + sizeof(len) + len;

    free(work);

    return 0;
}

int QPTPool_generic_alloc_and_deserialize(const int fd, QPTPool_f *func, void **work) {
    size_t len = 0;
    if ((read_size(fd, func, sizeof(*func)) != sizeof(*func)) ||
        (read_size(fd, &len, sizeof(len)) != sizeof(len))) {
        return 1;
    }

    void *data = malloc(len);
    if (read_size(fd, data, len) != (ssize_t) len) {
        free(data);
        return 1;
    }

    *work = data;

    return 0;
}
#endif

/* id selects the next_queue variable to use, not where the work will be placed */
QPTPool_enqueue_dst_t QPTPool_enqueue(QPTPool_ctx_t *ctx, QPTPool_f func, void *new_work) {
    /* Not checking arguments */

    struct queue_item *qi = malloc(sizeof(struct queue_item));
    qi->func = func; /* if no function is provided, the thread will segfault when it processes this item */
    qi->work = new_work;

    QPTPool_t *pool = ctx->pool;
    const size_t id = ctx->id;

    QPTPoolThreadData_t *data = &pool->data[id];
    pthread_mutex_lock(&data->mutex);
    QPTPoolThreadData_t *next = &pool->data[data->next_queue];

    /* have to calculate next_queue before new_work is modified */
    data->next_queue = pool->next.func(id, data->next_queue, pool->nthreads,
                                      new_work, pool->next.args);
    pthread_mutex_unlock(&data->mutex);

    QPTPool_enqueue_dst_t ret = QPTPool_enqueue_ERROR;

    /* always push into wait queue no matter what the queue_limit is */
    pthread_mutex_lock(&next->mutex);
    sll_push_back(&next->waiting, qi);
    ret = QPTPool_enqueue_WAIT;
    pthread_mutex_lock(&pool->mutex);
    pool->incomplete++;
    pthread_mutex_unlock(&pool->mutex);

    pthread_cond_broadcast(&next->cv);
    pthread_mutex_unlock(&next->mutex);

    return ret;
}

#ifdef QPTPOOL_SWAP
static int write_swap(struct Swap *swap,
                      QPTPool_serialize_and_free_f serialize,
                      QPTPool_alloc_and_deserialize_f deserialize,
                      QPTPool_f func, void *work) {
    /* Not checking arguments */

    const int fd = swap->write.fd;
    const off_t start = lseek(fd, 0, SEEK_CUR);

    size_t user_size = 0;
    if ((write_size(fd, &serialize,   sizeof(serialize))   != sizeof(serialize))   ||
        (write_size(fd, &deserialize, sizeof(deserialize)) != sizeof(deserialize)) ||
        (serialize(fd, func, work, &user_size)             != 0)) {
        lseek(fd, start, SEEK_SET); /* check for errors? */
        return 1;
    }

    const size_t written = sizeof(serialize) + sizeof(deserialize) + user_size;

    swap->write.count++;
    swap->write.size += written;
    swap->total_count++;
    swap->total_size += written;

    return 0;
}

static int read_swap(struct Swap *swap,
                     QPTPool_serialize_and_free_f *serialize,
                     QPTPool_alloc_and_deserialize_f *deserialize,
                     QPTPool_f *func, void **work) {
    /* Not checking arguments */

    const int fd = swap->read.fd;
    const off_t start = lseek(fd, 0, SEEK_CUR);

    if ((read_size(fd, serialize,   sizeof(*serialize))   != sizeof(*serialize))   ||
        (read_size(fd, deserialize, sizeof(*deserialize)) != sizeof(*deserialize)) ||
        ((*deserialize)(fd, func, work)                   != 0)) {
        lseek(fd, start, SEEK_SET); /* check for errors? */
        return 1;
    }

    return 0;
}

QPTPool_enqueue_dst_t QPTPool_enqueue_swappable(QPTPool_ctx_t *ctx, QPTPool_f func, void *new_work,
                                                QPTPool_serialize_and_free_f serialize,
                                                QPTPool_alloc_and_deserialize_f deserialize) {
    /* Not checking arguments */

    QPTPool_t *pool = ctx->pool;
    QPTPoolThreadData_t *data = &pool->data[ctx->id];

    pthread_mutex_lock(&data->mutex);
    QPTPoolThreadData_t *next = &pool->data[data->next_queue];
    /* have to calculate next_queue before new_work is modified */
    data->next_queue = pool->next.func(ctx->id, data->next_queue, pool->nthreads,
                                      new_work, pool->next.args);
    pthread_mutex_unlock(&data->mutex);

    QPTPool_enqueue_dst_t ret = QPTPool_enqueue_ERROR;

    pthread_mutex_lock(&next->mutex);
    if (!pool->queue_limit || (next->waiting.size < pool->queue_limit)) {
        struct queue_item *qi = malloc(sizeof(struct queue_item));
        qi->func = func; /* if no function is provided, the thread will segfault when it processes this item */
        qi->work = new_work;

        sll_push_back(&next->waiting, qi);
        ret = QPTPool_enqueue_WAIT;

        pthread_mutex_lock(&pool->mutex);
        pool->incomplete++;
        pthread_mutex_unlock(&pool->mutex);
    }
    else {
        if (write_swap(&next->swap, serialize, deserialize, func, new_work) == 0) {
            ret = QPTPool_enqueue_SWAP;

            pthread_mutex_lock(&pool->mutex);
            pool->swapped++;
            pthread_mutex_unlock(&pool->mutex);
        }
    }

    pthread_cond_broadcast(&next->cv);
    pthread_mutex_unlock(&next->mutex);

    return ret;
}
#endif

QPTPool_enqueue_dst_t QPTPool_enqueue_here(QPTPool_ctx_t *ctx, const size_t id, QPTPool_enqueue_dst_t queue,
                                           QPTPool_f func, void *new_work
                                           #ifdef QPTPOOL_SWAP
                                           , QPTPool_serialize_and_free_f serialize
                                           , QPTPool_alloc_and_deserialize_f deserialize
                                           #endif
    ) {
    /* Not checking other arguments */

    QPTPool_t *pool = ctx->pool;

    if ((queue != QPTPool_enqueue_WAIT)
        #ifdef QPTPOOL_SWAP
        && (queue != QPTPool_enqueue_SWAP)
        #endif
        ) {
        return QPTPool_enqueue_ERROR;
    }

    QPTPool_enqueue_dst_t ret = queue;

    QPTPoolThreadData_t *data = &pool->data[id];
    pthread_mutex_lock(&data->mutex);
    #ifdef QPTPOOL_SWAP
    if (queue == QPTPool_enqueue_WAIT) {
    #endif
        struct queue_item *qi = malloc(sizeof(struct queue_item));
        qi->func = func; /* if no function is provided, the thread will segfault when it processes this item */
        qi->work = new_work;

        sll_push_back(&data->waiting, qi);

        pthread_mutex_lock(&pool->mutex);
        pool->incomplete++;
        pthread_mutex_unlock(&pool->mutex);
    #ifdef QPTPOOL_SWAP
    }
    else if (queue == QPTPool_enqueue_SWAP) {
        if (write_swap(&data->swap, serialize, deserialize, func, new_work) == 0) {
            pthread_mutex_lock(&pool->mutex);
            pool->swapped++;
            pthread_mutex_unlock(&pool->mutex);
        }
        else {
            ret = QPTPool_enqueue_ERROR;
        }
    }
    #endif

    pthread_cond_broadcast(&data->cv);
    pthread_mutex_unlock(&data->mutex);

    return ret;
}

uint64_t QPTPool_wait_mem(QPTPool_ctx_t *ctx) {
    return QPTPool_wait_mem_lte(ctx, 0);
}

uint64_t QPTPool_wait_mem_lte(QPTPool_ctx_t *ctx, const uint64_t count) {
    if (!ctx) {
        return 0;
    }

    QPTPool_t *pool = ctx->pool;

    pthread_mutex_lock(&pool->mutex);

    while ((pool->state == RUNNING) &&
           (pool->incomplete > count)) {
        pthread_cond_wait(&pool->cv, &pool->mutex);
    }

    const uint64_t incomplete = pool->incomplete;

    pthread_mutex_unlock(&pool->mutex);

    /*
     * if returned value is larger than count,
     * then this was called before QPTPool_start
     */
    return incomplete;
}

#ifdef QPTPOOL_SWAP
/*
 * Read everything from a swap file back into memory. Simply running
 * this once for ecach thread is not enough because the previously
 * swapped work items might be swapped again or might create new work
 * that is swapped.
 *
 * Additionally, the swap files are changed to new files in order to
 * prevent racing of swap items being generated while they are being
 * read.
 */
static int QPTPool_wait_swapped_once(QPTPool_ctx_t *ctx, void *data) {
    (void) data;
    QPTPool_t *pool = ctx->pool;

    /* read swap files */
    QPTPoolThreadData_t *tw = &pool->data[ctx->id];
    struct Swap *swap = &tw->swap;

    /*
     * Reset the swap file's offset to 0, stash the file
     * descriptor, and open a new swap file for writing
     * to while reading the old one.
     *
     * If this is not done, the new work will write to the
     * original swap file while it is being read, throwing
     * off the internal offset.
     *
     * This has the additional benefit of reducing storage
     * space utilization once the old swap file is closed.
     */
    pthread_mutex_lock(&tw->mutex);
    /* if there's nothing, just return */
    if (swap_read_prep(swap) != 0) {
        pthread_mutex_unlock(&tw->mutex);
        return 0;
    }
    pthread_mutex_unlock(&tw->mutex);

    /* thread mutex does not need locking any more */

    size_t count = 0;

    QPTPool_serialize_and_free_f serialize = NULL;
    QPTPool_alloc_and_deserialize_f deserialize = NULL;
    QPTPool_f func = NULL;
    void *work = NULL;

    /* read everything out of swap and enqueue */
    while (read_swap(swap, &serialize, &deserialize, &func, &work) == 0) {
        /* can be swapped again immediately */
        QPTPool_enqueue_swappable(ctx, func, work, serialize, deserialize);
        count++;
    }

    pthread_mutex_lock(&pool->mutex);
    pool->swapped -= count;
    pthread_mutex_unlock(&pool->mutex);

    swap_read_done(swap);

    return 0;
}
#endif

void QPTPool_wait(QPTPool_ctx_t *ctx) {
    if (!ctx) {
        return;
    }

    QPTPool_t *pool = ctx->pool;

    pthread_mutex_lock(&pool->mutex);
    const QPTPoolState_t state = pool->state;
    pthread_mutex_unlock(&pool->mutex);

    if (state != RUNNING) {
        return;
    }

    /*
     * wait for all in-memory work to clear
     * out so that pool->swapped is stable
     */
    QPTPool_wait_mem(ctx);

    #ifdef QPTPOOL_SWAP
    pthread_mutex_lock(&pool->mutex);
    while (pool->swapped) {
        pthread_mutex_unlock(&pool->mutex);

        /* read swap in parallel */
        for(size_t i = 0; i < pool->nthreads; i++) {
            /* not swappable */
            QPTPool_enqueue_here(ctx, i, QPTPool_enqueue_WAIT,
                                 QPTPool_wait_swapped_once, NULL,
                                 NULL, NULL);
        }

        /* wait for all QPTPool_wait_swapped_once to clear out */
        QPTPool_wait_mem(ctx);

        /* pool->swapped is now stable */

        pthread_mutex_lock(&pool->mutex);
    }
    pthread_mutex_unlock(&pool->mutex);
    #endif
}

void QPTPool_stop(QPTPool_ctx_t *ctx) {
    if (!ctx) {
        return;
    }

    QPTPool_t *pool = ctx->pool;

    pthread_mutex_lock(&pool->mutex);
    const QPTPoolState_t state = pool->state;
    pthread_mutex_unlock(&pool->mutex);

    if (state != RUNNING) {
        return;
    }

    /* have to wait to clear out swapped work */
    QPTPool_wait(ctx);

    pthread_mutex_lock(&pool->mutex);
    pool->state = STOPPING;
    pthread_mutex_unlock(&pool->mutex);

    /* force all threads out of waiting loop if they are in it */
    for(size_t i = 0; i < pool->nthreads; i++) {
        QPTPoolThreadData_t *data = &pool->data[i];
        pthread_mutex_lock(&data->mutex);
        pthread_cond_broadcast(&data->cv);
        pthread_mutex_unlock(&data->mutex);
    }

    for(size_t i = 0; i < pool->nthreads; i++) {
        pthread_join(pool->data[i].thread, NULL);
    }
}

uint64_t QPTPool_threads_started(QPTPool_ctx_t *ctx) {
    if (!ctx) {
        return 0;
    }

    uint64_t sum = 0;
    for(size_t i = 0; i < ctx->pool->nthreads; i++) {
        sum += ctx->pool->data[i].threads_started;
    }
    return sum;
}

uint64_t QPTPool_threads_completed(QPTPool_ctx_t *ctx) {
    if (!ctx) {
        return 0;
    }

    uint64_t sum = 0;
    for(size_t i = 0; i < ctx->pool->nthreads; i++) {
        sum += ctx->pool->data[i].threads_successful;
    }
    return sum;
}

#ifdef QPTPOOL_SWAP
uint64_t QPTPool_work_swapped_count(QPTPool_ctx_t *ctx) {
    if (!ctx) {
        return 0;
    }

    uint64_t sum = 0;
    for(size_t i = 0; i < ctx->pool->nthreads; i++) {
        sum += ctx->pool->data[i].swap.total_count;
    }
    return sum;
}

size_t QPTPool_work_swapped_size(QPTPool_ctx_t *ctx) {
    if (!ctx) {
        return 0;
    }

    size_t sum = 0;
    for(size_t i = 0; i < ctx->pool->nthreads; i++) {
        sum += ctx->pool->data[i].swap.total_size;
    }
    return sum;
}
#endif

void QPTPool_destroy(QPTPool_ctx_t *ctx) {
    if (!ctx) {
        return;
    }

    QPTPool_t *pool = ctx->pool;

    for(size_t i = 0; i < pool->nthreads; i++) {
        QPTPoolThreadData_t *data = &pool->data[i];
        data->threads_successful = 0;
        data->threads_started = 0;
        data->thread = 0;
        pthread_cond_destroy(&data->cv);
        pthread_mutex_destroy(&data->mutex);
        #ifdef QPTPOOL_SWAP
        swap_stop(&data->swap);
        swap_destroy(&data->swap);
        #endif
        /*
         * If QPTPool_start was called, queues will be empty at this point
         *
         * If QPTPool_start was not called, queues might not be empty since
         * enqueuing work without starting the worker threads is allowed,
         * so free() is called to clear out any unprocessed work items
         */
        sll_destroy(&data->waiting, free);
        pthread_mutex_destroy(&data->claimed_mutex);
        sll_destroy(&data->claimed, free);
    }

    pthread_cond_destroy(&pool->cv);
    pthread_mutex_destroy(&pool->mutex);
    free(pool->thread_mutex);
    free(pool->waiting);
    free(pool->claimed_mutex);
    free(pool->claimed);
    free(pool->data);
    free(pool);
    free(ctx);
}
