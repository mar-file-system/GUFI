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

static int increment_counter(QPTPool_t *, const std::size_t, void *data, void *) {
    std::size_t *counter = static_cast<std::size_t *>(data);
    (*counter)++;
    return 0;
}

// QPTPool_init followed by QPTPool_destroy - threads are not started
// should not leak memory
TEST(QueuePerThreadPool, no_start_stop) {
    QPTPool_t *pool = QPTPool_init(1, nullptr);
    EXPECT_NE(pool, nullptr);
    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, zero_threads) {
    EXPECT_EQ(QPTPool_init(0, nullptr), nullptr);
}

TEST(QueuePerThreadPool, bad_start) {
    // bad context
    EXPECT_EQ(QPTPool_start(nullptr), 1);

    // already started
    setup_pool(THREADS, nullptr);
    EXPECT_EQ(QPTPool_start(pool), 1);
    QPTPool_wait(pool);
    QPTPool_destroy(pool);
}

// not starting up QPTPool even though there
// is work queued should not leak memory
TEST(QueuePerThreadPool, no_start) {
    void *work = ::operator new(1);

    // not calling QPTPool_wait
    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);

        // the QPTPool should not leak memory
        EXPECT_EQ(QPTPool_enqueue(pool, 0, nullptr, nullptr), QPTPool_enqueue_WAIT);

        // the QPTPool should not leak memory, but the allocation will leak
        EXPECT_EQ(QPTPool_enqueue(pool, 0, nullptr, work), QPTPool_enqueue_WAIT);

        QPTPool_destroy(pool);
    }

    // calling QPTPool_wait
    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);

        // the QPTPool should not leak memory
        EXPECT_EQ(QPTPool_enqueue(pool, 0, nullptr, nullptr), QPTPool_enqueue_WAIT);

        // the QPTPool should not leak memory, but the allocation will leak
        EXPECT_EQ(QPTPool_enqueue(pool, 0, nullptr, work), QPTPool_enqueue_WAIT);

        // wait even though start was not called
        // should not cause invalid thread joins
        QPTPool_wait(pool);

        // calling wait more times does not hurt
        QPTPool_wait(pool);

        QPTPool_destroy(pool);
    }

    ::operator delete(work);
}

// start threads without work
TEST(QueuePerThreadPool, no_work) {
    {
        setup_pool(THREADS, nullptr);
        QPTPool_wait(pool);
        QPTPool_destroy(pool);
    }

    // calling wait more times does not hurt
    {
        setup_pool(THREADS, nullptr);
        QPTPool_wait(pool);
        QPTPool_wait(pool);
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
        QPTPool_wait(pool);
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

    QPTPool_wait(pool);

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
};

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
    QPTPool_wait(pool);

    for(std::size_t i = 0; i < WORK_COUNT; i++) {
        EXPECT_EQ(values[i], i);
    }
    EXPECT_EQ(QPTPool_threads_completed(pool), WORK_COUNT);

    delete [] values;
    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, threads_started) {
    setup_pool(THREADS, nullptr);

    for(std::size_t i = 0; i < WORK_COUNT; i++) {
        QPTPool_enqueue(pool, i % THREADS,
                        [](QPTPool_t *, const std::size_t, void *, void *) -> int {
                            return 0;
                        },
                        nullptr);
    }

    QPTPool_wait(pool);

    EXPECT_EQ(QPTPool_threads_started(pool), WORK_COUNT);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, threads_completed) {
    setup_pool(THREADS, nullptr);

    for(std::size_t i = 0; i < WORK_COUNT; i++) {
        QPTPool_enqueue(pool, i % THREADS,
                        [](QPTPool_t *, const std::size_t, void *, void *) -> int {
                            return 0;
                        }, nullptr);
    }

    QPTPool_wait(pool);

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

    QPTPool_wait(pool);

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

TEST(QueuePerThreadPool, deferred) {
    QPTPool_t *pool = QPTPool_init(1, nullptr);
    ASSERT_NE(pool, nullptr);
    ASSERT_EQ(QPTPool_set_queue_limit(pool, 1), 0);

    EXPECT_EQ(QPTPool_enqueue(pool, 0, nullptr, nullptr), QPTPool_enqueue_WAIT);
    EXPECT_EQ(QPTPool_enqueue(pool, 0, nullptr, nullptr), QPTPool_enqueue_DEFERRED);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, steal_waiting) {
    std::size_t counter = 0;

    // macOS doesn't seem to like std::mutex
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

    QPTPool_t *pool = QPTPool_init(2, &mutex);
    ASSERT_NE(pool, nullptr);
    ASSERT_EQ(QPTPool_set_steal(pool, 1, 2), 0);
    ASSERT_EQ(QPTPool_start(pool), 0);

    // prevent thread 0 from completing
    pthread_mutex_lock(&mutex);
    EXPECT_EQ(QPTPool_enqueue(pool, 0,
                              [](QPTPool_t *, const std::size_t, void *data, void *args) -> int {
                                  std::size_t *counter = static_cast<std::size_t *>(data);
                                  (*counter)++;

                                  pthread_mutex_t *mutex = static_cast<pthread_mutex_t *>(args);
                                  pthread_mutex_lock(mutex);
                                  pthread_mutex_unlock(mutex);
                                  return 0;
                              }, &counter), QPTPool_enqueue_WAIT);

    // need to make sure only this work item got popped off
    while (!counter) {
        sched_yield();
    }

    counter = 0;

    // i == 0 thread 1
    // i == 1 thread 0
    // i == 2 thread 1
    // i == 3 thread 0
    for(std::size_t i = 0; i < 4; i++) {
        EXPECT_EQ(QPTPool_enqueue(pool, 0, increment_counter, &counter), QPTPool_enqueue_WAIT);
    }

    // wait for thread 1 to run its work and at least one stolen work item
    while (counter < (std::size_t) 3) {
        sched_yield();
    }

    // unlock thread 0 so that it can complete
    pthread_mutex_unlock(&mutex);

    QPTPool_wait(pool);

    EXPECT_EQ(counter, (std::size_t) 4);
    EXPECT_EQ(QPTPool_threads_started(pool),   (uint64_t) 5);
    EXPECT_EQ(QPTPool_threads_completed(pool), (uint64_t) 5);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, steal_deferred) {
    std::size_t counter = 0;

    // macOS doesn't seem to like std::mutex
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

    QPTPool_t *pool = QPTPool_init(2, &mutex);
    ASSERT_NE(pool, nullptr);
    ASSERT_EQ(QPTPool_set_queue_limit(pool, 1), 0);
    ASSERT_EQ(QPTPool_set_steal(pool, 1, 1), 0);
    ASSERT_EQ(QPTPool_start(pool), 0);

    // prevent thread 0 from completing
    pthread_mutex_lock(&mutex);
    EXPECT_EQ(QPTPool_enqueue(pool, 0,
                              [](QPTPool_t *, const std::size_t, void *data, void *args) -> int {
                                  std::size_t *counter = static_cast<std::size_t *>(data);
                                  (*counter)++;

                                  pthread_mutex_t *mutex = static_cast<pthread_mutex_t *>(args);
                                  pthread_mutex_lock(mutex);
                                  pthread_mutex_unlock(mutex);
                                  return 0;
                              }, &counter), QPTPool_enqueue_WAIT);

    // need to make sure only this work item got popped off
    while (!counter) {
        sched_yield();
    }

    counter = 0;

    EXPECT_EQ(QPTPool_enqueue(pool, 0, increment_counter, &counter), QPTPool_enqueue_WAIT);  // thread 1
    EXPECT_EQ(QPTPool_enqueue(pool, 0, increment_counter, &counter), QPTPool_enqueue_WAIT);  // thread 0
    EXPECT_NE(QPTPool_enqueue(pool, 0, increment_counter, &counter), QPTPool_enqueue_ERROR); // thread 1
    EXPECT_NE(QPTPool_enqueue(pool, 0, increment_counter, &counter), QPTPool_enqueue_ERROR); // thread 0

    // wait for thread 1 to run its work and at least one stolen work item
    while (counter < (std::size_t) 4) {
        sched_yield();
    }

    // unlock thread 0 so that it can complete
    pthread_mutex_unlock(&mutex);

    QPTPool_wait(pool);

    EXPECT_EQ(counter, (std::size_t) 4);
    EXPECT_EQ(QPTPool_threads_started(pool),   (uint64_t) 5);
    EXPECT_EQ(QPTPool_threads_completed(pool), (uint64_t) 5);

    QPTPool_destroy(pool);
}

static std::size_t test_next_func(const std::size_t, const std::size_t,
                                  const std::size_t, void *, void *) {
    return 0;
}

TEST(QueuePerThreadPool, prop_next) {
    // bad function
    EXPECT_EQ(QPTPool_set_next(nullptr, nullptr, nullptr), 1);

    // bad state
    {
        setup_pool(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_next(pool, test_next_func, nullptr), 1);
        QPTPool_wait(pool);
        QPTPool_destroy(pool);
    }

    // good
    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_next(pool, test_next_func, nullptr), 0);

        QPTPoolNextFunc_t get_next_func = nullptr;
        void *get_next_func_args = nullptr;
        EXPECT_EQ(QPTPool_get_next(pool, &get_next_func, &get_next_func_args), 0);
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
        EXPECT_EQ(QPTPool_set_queue_limit(pool, queue_limit), 1);
        QPTPool_wait(pool);
        QPTPool_destroy(pool);
    }

    // good
    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_queue_limit(pool, queue_limit), 0);
        uint64_t get_queue_limit = 0;
        EXPECT_EQ(QPTPool_get_queue_limit(pool, &get_queue_limit), 0);
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
        EXPECT_EQ(QPTPool_set_steal(pool, num, denom), 1);
        QPTPool_wait(pool);
        QPTPool_destroy(pool);
    }

    // good
    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_steal(pool, num, denom), 0);
        uint64_t get_num = 0;
        uint64_t get_denom = 0;
        EXPECT_EQ(QPTPool_get_steal(pool, &get_num, &get_denom), 0);
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
        QPTPool_wait(pool);
        QPTPool_destroy(pool);
    }

    // good
    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
        struct OutputBuffers *set_debug_buffers = reinterpret_cast<struct OutputBuffers *>(1);
        EXPECT_EQ(QPTPool_set_debug_buffers(pool, set_debug_buffers), 0);
        struct OutputBuffers *get_debug_buffers = nullptr;
        EXPECT_EQ(QPTPool_get_debug_buffers(pool, &get_debug_buffers), 0);
        EXPECT_EQ(get_debug_buffers, set_debug_buffers);
        QPTPool_destroy(pool);
    }
}
#endif
