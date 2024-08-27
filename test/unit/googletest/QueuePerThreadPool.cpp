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



#include <pthread.h> // macOS doesn't seem to like std::mutex

#include <gtest/gtest.h>

#include "QueuePerThreadPool.h"

static const     std::size_t THREADS = 5;
static constexpr std::size_t WORK_COUNT = 2 * THREADS + 1;

#define setup_pool(nthreads, args)                                      \
    QPTPool_t *pool = QPTPool_init(nthreads, args);                     \
    ASSERT_NE(pool, nullptr);                                           \
    /* don't bother cleaning up */                                      \
    ASSERT_EQ(QPTPool_start(pool), 0)

static int return_data(QPTPool_t *, const std::size_t, void *data, void *) {
    return (int) (uintptr_t) data;
}

static int increment_counter(QPTPool_t *, const std::size_t, void *data, void *) {
    std::size_t *counter = static_cast<std::size_t *>(data);
    (*counter)++;
    return 0;
}

TEST(QueuePerThreadPool, null) {
    QPTPool_t *pool = QPTPool_init(0, nullptr);
    EXPECT_EQ(pool, nullptr);
    EXPECT_NO_THROW(QPTPool_start(pool));
    EXPECT_NO_THROW(QPTPool_stop(pool));
    EXPECT_EQ(QPTPool_threads_started(pool), (uint64_t) 0);
    EXPECT_EQ(QPTPool_threads_completed(pool), (uint64_t) 0);
    EXPECT_NO_THROW(QPTPool_destroy(pool));
}

// QPTPool_init followed by QPTPool_destroy - threads are not started
// should not leak memory
TEST(QueuePerThreadPool, no_start_stop) {
    QPTPool_t *pool = QPTPool_init(1, nullptr);
    EXPECT_NE(pool, nullptr);
    QPTPool_destroy(pool);
}

/* C Standard 6.10.3/C++ Standard 16.3 Macro replacement */
static void *qptpool_init_with_props() {
    return QPTPool_init_with_props(-1, nullptr, nullptr, nullptr, 0, 0, 0
                                   #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                   , nullptr
                                   #endif
        );
}

TEST(QueuePreThreadPool, bad_init) {
    EXPECT_EQ(QPTPool_init(-1, nullptr), nullptr);
    EXPECT_EQ(qptpool_init_with_props(), nullptr);
}

TEST(QueuePerThreadPool, bad_start) {
    // already started
    setup_pool(THREADS, nullptr);
    EXPECT_EQ(QPTPool_start(pool), 1);
    QPTPool_stop(pool);
    QPTPool_destroy(pool);
}

// not starting up QPTPool even though there
// is work queued should not leak memory
TEST(QueuePerThreadPool, no_start) {
    void *work = ::operator new(1);

    // not calling QPTPool_stop
    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);

        // the QPTPool should not leak memory
        EXPECT_EQ(QPTPool_enqueue(pool, 0, nullptr, nullptr), QPTPool_enqueue_WAIT);

        // the QPTPool should not leak memory, but the allocation will leak
        EXPECT_EQ(QPTPool_enqueue(pool, 0, nullptr, work), QPTPool_enqueue_WAIT);

        QPTPool_destroy(pool);
    }

    // calling QPTPool_stop
    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);

        // the QPTPool should not leak memory
        EXPECT_EQ(QPTPool_enqueue(pool, 0, nullptr, nullptr), QPTPool_enqueue_WAIT);

        // the QPTPool should not leak memory, but the allocation will leak
        EXPECT_EQ(QPTPool_enqueue(pool, 0, nullptr, work), QPTPool_enqueue_WAIT);

        // wait even though start was not called
        // should not cause invalid thread joins
        QPTPool_stop(pool);

        // calling wait more times does not hurt
        QPTPool_stop(pool);

        QPTPool_destroy(pool);
    }

    ::operator delete(work);
}

// start threads without work
TEST(QueuePerThreadPool, no_work) {
    {
        setup_pool(THREADS, nullptr);
        QPTPool_stop(pool);
        QPTPool_destroy(pool);
    }

    // calling wait more times does not hurt
    {
        setup_pool(THREADS, nullptr);
        QPTPool_stop(pool);
        QPTPool_stop(pool);
        QPTPool_destroy(pool);
    }
}

TEST(QueuePerThreadPool, restart) {
    std::size_t counter = 0;

    QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
    ASSERT_NE(pool, nullptr);

    for(std::size_t i = 0; i < 3; i++) {
        ASSERT_EQ(QPTPool_start(pool), 0);
        EXPECT_EQ(QPTPool_enqueue(pool, 0, increment_counter, &counter), QPTPool_enqueue_WAIT);
        QPTPool_stop(pool);
        EXPECT_EQ(counter, (i + 1));
    }

    QPTPool_destroy(pool);
}

// push work onto queues from the outside
TEST(QueuePerThreadPool, enqueue_external) {
    std::size_t *values = new std::size_t[WORK_COUNT]();

    struct test_work {
        std::size_t *value;
        std::size_t counter;
    };

    setup_pool(THREADS, nullptr);

    for(std::size_t i = 0; i < WORK_COUNT; i++) {
        struct test_work *work = (struct test_work *) calloc(1, sizeof(struct test_work));
        work->value = &values[i];
        work->counter = i;

        QPTPool_enqueue(pool, i % THREADS,
                        [](QPTPool_t *, const std::size_t, void *data, void *) -> int {
                            struct test_work *work = (struct test_work *) data;
                            *(work->value) = work->counter;
                            free(work);
                            return 0;
                        }, work);
    }

    QPTPool_stop(pool);

    for(std::size_t i = 0; i < WORK_COUNT; i++) {
        EXPECT_EQ(values[i], i);
    }
    EXPECT_EQ(QPTPool_threads_completed(pool), WORK_COUNT);

    delete [] values;
    QPTPool_destroy(pool);
}

struct test_work {
    std::size_t index;
    std::size_t *values;
};

static int recursive(QPTPool_t *pool, const std::size_t id, void *data, void *args) {
    struct test_work *work = (struct test_work *) data;
    const std::size_t work_count = * (std::size_t *) args;

    if (work->index >= work_count) {
        free(work);
        return 1;
    }

    work->values[work->index] = work->index;

    struct test_work *next_work = (struct test_work *) calloc(1, sizeof(struct test_work));
    next_work->index = work->index + 1;
    next_work->values = work->values;

    QPTPool_enqueue(pool, id, recursive, next_work);
    free(work);

    return 0;
}

// push work onto queues from within the queue function
TEST(QueuePerThreadPool, enqueue_internal) {
    std::size_t *values = new std::size_t[WORK_COUNT]();

    setup_pool(THREADS, (void *) &WORK_COUNT);

    // push only the first piece of work onto the queue
    // the worker function will push more work
    struct test_work *zero = (struct test_work *) calloc(1, sizeof(struct test_work));
    zero->index = 0;
    zero->values = values;

    QPTPool_enqueue(pool, 0, recursive, zero);
    QPTPool_stop(pool);

    for(std::size_t i = 0; i < WORK_COUNT; i++) {
        EXPECT_EQ(values[i], i);
    }
    EXPECT_EQ(QPTPool_threads_completed(pool), WORK_COUNT);

    delete [] values;
    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, enqueue_deferred) {
    QPTPool_t *pool = QPTPool_init(1, nullptr);
    ASSERT_NE(pool, nullptr);
    ASSERT_EQ(QPTPool_set_queue_limit(pool, 1), 0);

    EXPECT_EQ(QPTPool_enqueue(pool, 0, nullptr, nullptr), QPTPool_enqueue_WAIT);
    EXPECT_EQ(QPTPool_enqueue(pool, 0, nullptr, nullptr), QPTPool_enqueue_DEFERRED);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, enqueue_here) {
    setup_pool(THREADS, nullptr);

    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_ERROR,    return_data, nullptr),
              QPTPool_enqueue_ERROR);

    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_WAIT,     return_data, nullptr),
              QPTPool_enqueue_WAIT);

    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_DEFERRED, return_data, nullptr),
              QPTPool_enqueue_DEFERRED);

    QPTPool_stop(pool);

    EXPECT_EQ(QPTPool_threads_started(pool),   (uint64_t) 2);
    EXPECT_EQ(QPTPool_threads_completed(pool), (uint64_t) 2);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, wait) {
    // macOS doesn't seem to like std::mutex
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

    QPTPool_t *pool = QPTPool_init(THREADS, &mutex);
    ASSERT_NE(pool, nullptr);

    const uint64_t PUSH = 10;
    const uint64_t WAIT = 5;

    std::size_t counter = 0;
    for(uint64_t i = 0; i < PUSH; i++) {
        QPTPool_enqueue(pool, 0,
                        [](QPTPool_t *ctx, const std::size_t id, void *data, void *args) -> int {
                            pthread_mutex_t *mutex = static_cast<pthread_mutex_t *>(args);
                            pthread_mutex_lock(mutex);
                            increment_counter(ctx, id, data, args);
                            pthread_mutex_unlock(mutex);
                            return 0;
                    }, &counter);
    }

    /* not running yet, so will return all work enqueued/remaining in the pool */
    EXPECT_EQ(QPTPool_wait_lte(pool, WAIT), PUSH);

    ASSERT_EQ(QPTPool_start(pool), 0);

    /* wait for pool to have at most WAIT items left */
    EXPECT_LE(QPTPool_wait_lte(pool, WAIT), WAIT);

    /* wait for all work to complete */
    EXPECT_EQ(QPTPool_wait(pool), (uint64_t) 0);

    QPTPool_stop(pool);
    QPTPool_destroy(pool);

    EXPECT_EQ(QPTPool_wait(nullptr), (uint64_t) 0);
}

TEST(QueuePerThreadPool, threads_started) {
    setup_pool(THREADS, nullptr);

    for(std::size_t i = 0; i < WORK_COUNT; i++) {
        QPTPool_enqueue(pool, i % THREADS, return_data, (void *) (uintptr_t) 1);
    }

    QPTPool_stop(pool);

    EXPECT_EQ(QPTPool_threads_started(pool),   WORK_COUNT);
    EXPECT_EQ(QPTPool_threads_completed(pool), (uint64_t) 0);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, threads_completed) {
    setup_pool(THREADS, nullptr);

    for(std::size_t i = 0; i < WORK_COUNT; i++) {
        QPTPool_enqueue(pool, i % THREADS, return_data, nullptr);
    }

    QPTPool_stop(pool);

    EXPECT_EQ(QPTPool_threads_started(pool),   WORK_COUNT);
    EXPECT_EQ(QPTPool_threads_completed(pool), WORK_COUNT);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, custom_next) {
    std::size_t *values = new std::size_t[WORK_COUNT]();

    QPTPool_t *pool = QPTPool_init(THREADS, values);
    ASSERT_NE(pool, nullptr);
    ASSERT_EQ(QPTPool_set_next(pool,
                               [](const std::size_t, const std::size_t prev, const std::size_t threads,
                                  void *, void *) -> std::size_t {
                                   return (prev?prev:threads) - 1;
                               }, nullptr), 0);
    ASSERT_EQ(QPTPool_start(pool), 0);

    for(std::size_t i = 0; i < WORK_COUNT; i++) {
        struct test_work *work = (struct test_work *) calloc(1, sizeof(struct test_work));
        work->index = i;
        work->values = nullptr;

        QPTPool_enqueue(pool, i % THREADS,
                        [](QPTPool_t *, const std::size_t id, void *data, void *args) -> int {
                            std::size_t *values = (std::size_t *) args;
                            struct test_work *work = (struct test_work *) data;
                            values[work->index] = id;
                            free(work);
                            return 0;
                        },
                        work);
    }

    QPTPool_stop(pool);

    EXPECT_EQ(QPTPool_threads_started(pool), WORK_COUNT);

    // first set of work was pushed to the original threads
    for(std::size_t i = 0; i < THREADS; i++) {
        EXPECT_EQ(values[i], i);
    }

    // second set of work was pushed to previous threads
    for(std::size_t i = THREADS; i < (2 * THREADS); i++) {
        EXPECT_EQ(values[i], (i - 1) % THREADS);
    }

    // last work was pushed to (0 - 2) % THREADS
    EXPECT_EQ(values[WORK_COUNT - 1], THREADS - 2);

    QPTPool_destroy(pool);
    delete [] values;
}

static void test_steal(const QPTPool_enqueue_dst queue, const bool find,
                       const int src, const int dst) {
    std::size_t counter = 0;

    // macOS doesn't seem to like std::mutex
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

    QPTPool_t *pool = QPTPool_init(2, &mutex);
    ASSERT_NE(pool, nullptr);
    if (queue == QPTPool_enqueue_DEFERRED) {
        ASSERT_EQ(QPTPool_set_queue_limit(pool, 1), 0);
    }
    ASSERT_EQ(QPTPool_set_steal(pool, 1, 1), 0);
    ASSERT_EQ(QPTPool_start(pool), 0);

    // prevent thread src from completing
    pthread_mutex_lock(&mutex);
    EXPECT_EQ(QPTPool_enqueue(pool, src,
                              [](QPTPool_t *ctx, const std::size_t id, void *data, void *args) -> int {
                                  increment_counter(ctx, id, data, args);

                                  pthread_mutex_t *mutex = static_cast<pthread_mutex_t *>(args);
                                  pthread_mutex_lock(mutex);
                                  pthread_mutex_unlock(mutex);
                                  return 0;
                              }, &counter), QPTPool_enqueue_WAIT);

    // need to make sure only work item got popped off
    // no racing here because only 1 thread runs
    while (counter < 1) {
        sched_yield();
    }

    uint64_t expected = 1;

    // no racing here because even though work is placed into different
    // threads, they end up getting stolen and run serially in dst
    if (find) {
        // thread src; put a work item in the designated queue for stealing
        EXPECT_EQ(QPTPool_enqueue_here(pool, src, queue, increment_counter, &counter),
                  queue);

        // thread dst; gets 1 work item and triggers stealing 1 work item from src
        EXPECT_EQ(QPTPool_enqueue_here(pool, dst, QPTPool_enqueue_WAIT, increment_counter, &counter),
                  QPTPool_enqueue_WAIT);

        expected += 2;
    }
    else {
        // thread dst will look in both queues and not find anything to steal
    }

    // wait for thread dst to run its work (and stolen work item)
    while (counter < expected) {
        sched_yield();
    }

    // unlock thread src so that it can complete
    pthread_mutex_unlock(&mutex);

    QPTPool_stop(pool);

    EXPECT_EQ(counter, expected);
    EXPECT_EQ(QPTPool_threads_started(pool),   expected);
    EXPECT_EQ(QPTPool_threads_completed(pool), expected);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, steal_nothing) {
    // same test
    test_steal(QPTPool_enqueue_WAIT,     false, 0, 1);
    test_steal(QPTPool_enqueue_DEFERRED, false, 0, 1);

    // same test
    test_steal(QPTPool_enqueue_WAIT,     false, 1, 0);
    test_steal(QPTPool_enqueue_DEFERRED, false, 1, 0);
}

TEST(QueuePerThreadPool, steal_waiting) {
    test_steal(QPTPool_enqueue_WAIT,     true, 0, 1);
    test_steal(QPTPool_enqueue_WAIT,     true, 1, 0);
}

TEST(QueuePerThreadPool, steal_deferred) {
    test_steal(QPTPool_enqueue_DEFERRED, true, 0, 1);
    test_steal(QPTPool_enqueue_DEFERRED, true, 1, 0);
}

TEST(QueuePerThreadPool, steal_active) {
    std::size_t counter = 0;

    // macOS doesn't seem to like std::mutex
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

    QPTPool_t *pool = QPTPool_init(2, &mutex);
    ASSERT_NE(pool, nullptr);
    ASSERT_EQ(QPTPool_set_steal(pool, 1, 2), 0);

    // prevent thread 0 from completing
    pthread_mutex_lock(&mutex);

    // this work item does not complete until the end
    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_WAIT,
                                   [](QPTPool_t *ctx, const std::size_t id, void *data, void *args) -> int {
                                       pthread_mutex_t *mutex = static_cast<pthread_mutex_t *>(args);

                                       increment_counter(ctx, id, data, args);

                                       pthread_mutex_lock(mutex);
                                       pthread_mutex_unlock(mutex);
                                       return 0;
                                   }, &counter), QPTPool_enqueue_WAIT);

    // trap this work item after the first work item
    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_WAIT, increment_counter,  &counter),
                                   QPTPool_enqueue_WAIT);

    // start the thread pool, causing thread 0 to take all work items, but get stuck on the
    // first work item, not letting the second work items run
    ASSERT_EQ(QPTPool_start(pool), 0);

    // need to make sure the first work item started
    while (counter < 1) {
        sched_yield();
    }

    // push to thread 1, so after it completes, it steals the first item from thread 0's active queue
    EXPECT_EQ(QPTPool_enqueue_here(pool, 1, QPTPool_enqueue_WAIT, increment_counter, &counter),
              QPTPool_enqueue_WAIT);

    // wait for thread 1's original work item and the stolen work item to finish
    while (counter < 3) {
        sched_yield();
    }

    // let thread 0 finish
    pthread_mutex_unlock(&mutex);

    QPTPool_stop(pool);

    const uint64_t expected = 3;
    EXPECT_EQ(counter, expected);
    EXPECT_EQ(QPTPool_threads_started(pool),   expected);
    EXPECT_EQ(QPTPool_threads_completed(pool), expected);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, prop_nthreads) {
    QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
    ASSERT_NE(pool, nullptr);

    std::size_t nthreads = 0;
    EXPECT_EQ(QPTPool_get_nthreads(pool, &nthreads), 0);
    EXPECT_EQ(nthreads, THREADS);

    EXPECT_EQ(QPTPool_get_nthreads(nullptr, &nthreads), 1);
    EXPECT_EQ(QPTPool_get_nthreads(pool, nullptr), 0);

    QPTPool_destroy(pool);
}

static void check_get_args(void *ptr) {
    QPTPool_t *pool = QPTPool_init(THREADS, ptr);
    ASSERT_NE(pool, nullptr);

    void *args = nullptr;
    EXPECT_EQ(QPTPool_get_args(pool, &args), 0);
    EXPECT_EQ(args, ptr);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, prop_args) {
    check_get_args(nullptr);
    check_get_args((void *) check_get_args);

    EXPECT_EQ(QPTPool_get_args(nullptr, nullptr), 1);
    EXPECT_EQ(QPTPool_get_args((QPTPool_t *) check_get_args, nullptr), 0);
}

static std::size_t test_next_func(const std::size_t, const std::size_t,
                                  const std::size_t, void *, void *) {
    return 0;
}

TEST(QueuePerThreadPool, prop_next) {
    // bad state
    {
        setup_pool(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_next(pool, test_next_func, nullptr), 1);
        QPTPool_stop(pool);
        QPTPool_destroy(pool);
    }

    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_next(nullptr, test_next_func, nullptr), 1);
        EXPECT_EQ(QPTPool_set_next(pool,    nullptr,        nullptr), 1);
        EXPECT_EQ(QPTPool_set_next(pool,    test_next_func, nullptr), 0);

        QPTPoolNextFunc_t get_next_func = nullptr;
        void *get_next_func_args = nullptr;
        EXPECT_EQ(QPTPool_get_next(nullptr, &get_next_func, &get_next_func_args), 1);
        EXPECT_EQ(QPTPool_get_next(pool,    nullptr,        &get_next_func_args), 0);
        EXPECT_EQ(QPTPool_get_next(pool,    &get_next_func, nullptr),             0);
        EXPECT_EQ(QPTPool_get_next(pool,    &get_next_func, &get_next_func_args), 0);
        EXPECT_EQ(get_next_func, test_next_func);
        EXPECT_EQ(get_next_func_args, nullptr);
        QPTPool_destroy(pool);
    }
}

TEST(QueuePerThreadPool, prop_queue_limit) {
    const uint64_t queue_limit = THREADS;

    // bad state
    {
        setup_pool(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_queue_limit(pool,    queue_limit), 1);
        QPTPool_stop(pool);
        QPTPool_destroy(pool);
    }

    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_queue_limit(nullptr, queue_limit), 1);
        EXPECT_EQ(QPTPool_set_queue_limit(pool,    queue_limit), 0);

        uint64_t get_queue_limit = 0;
        EXPECT_EQ(QPTPool_get_queue_limit(nullptr, &get_queue_limit), 1);
        EXPECT_EQ(QPTPool_get_queue_limit(pool,    nullptr),          0);
        EXPECT_EQ(QPTPool_get_queue_limit(pool,    &get_queue_limit), 0);
        EXPECT_EQ(get_queue_limit, static_cast<std::size_t>(THREADS));
        QPTPool_destroy(pool);
    }
}

TEST(QueuePerThreadPool, prop_steal) {
    const uint64_t num = 1;
    const uint64_t denom = 2;

    // bad state
    {
        setup_pool(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_steal(pool,    num, denom), 1);
        QPTPool_stop(pool);
        QPTPool_destroy(pool);
    }

    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_steal(nullptr, num, denom), 1);
        EXPECT_EQ(QPTPool_set_steal(pool,    num, denom), 0);

        uint64_t get_num = 0;
        uint64_t get_denom = 0;
        EXPECT_EQ(QPTPool_get_steal(nullptr, &get_num, &get_denom), 1);
        EXPECT_EQ(QPTPool_get_steal(pool,    nullptr,  &get_denom), 0);
        EXPECT_EQ(QPTPool_get_steal(pool,    &get_num, nullptr),    0);
        EXPECT_EQ(QPTPool_get_steal(pool,    &get_num, &get_denom), 0);
        EXPECT_EQ(get_num, num);
        EXPECT_EQ(get_denom, denom);
        QPTPool_destroy(pool);
    }
}

#if defined(DEBUG) && defined(PER_THREAD_STATS)
TEST(QueuePerThreadPool, prop_debug_buffers) {
    // bad state
    {
        setup_pool(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_debug_buffers(pool, nullptr), 1);
        QPTPool_stop(pool);
        QPTPool_destroy(pool);
    }

    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
        struct OutputBuffers *set_debug_buffers = reinterpret_cast<struct OutputBuffers *>(1);
        EXPECT_EQ(QPTPool_set_debug_buffers(nullptr, set_debug_buffers), 1);
        EXPECT_EQ(QPTPool_set_debug_buffers(pool,    set_debug_buffers), 0);

        struct OutputBuffers *get_debug_buffers = nullptr;
        EXPECT_EQ(QPTPool_get_debug_buffers(nullptr, &get_debug_buffers), 1);
        EXPECT_EQ(QPTPool_get_debug_buffers(pool,    nullptr),            0);
        EXPECT_EQ(QPTPool_get_debug_buffers(pool,    &get_debug_buffers), 0);
        EXPECT_EQ(get_debug_buffers, set_debug_buffers);
        QPTPool_destroy(pool);
    }
}
#endif
