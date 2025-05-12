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



#include <inttypes.h>
#include <pthread.h> // macOS doesn't seem to like std::mutex

#include <gtest/gtest.h>

#include "QueuePerThreadPool.h"
#include "utils.h"

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

TEST(QueuePerThreadPool, zero_threads) {
    QPTPool_t *pool = QPTPool_init(0, nullptr);
    EXPECT_EQ(pool, nullptr);
    EXPECT_NO_THROW(QPTPool_start(pool));
    EXPECT_NO_THROW(QPTPool_stop(pool));
    EXPECT_EQ(QPTPool_threads_started(pool),    (uint64_t) 0);
    EXPECT_EQ(QPTPool_threads_completed(pool),  (uint64_t) 0);
    #ifdef QPTPOOL_SWAP
    EXPECT_EQ(QPTPool_work_swapped_count(pool), (uint64_t) 0);
    EXPECT_EQ(QPTPool_work_swapped_size(pool),  (std::size_t) 0);
    #endif
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
static void *qptpool_init_with_props(const size_t threads = -1,
                                     const uint64_t queue_limit = 0) {
    return QPTPool_init_with_props(threads, nullptr, nullptr, nullptr, queue_limit, "", 0, 0);
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

#ifdef QPTPOOL_SWAP
TEST(QueuePerThreadPool, generic_serialize_and_free_error) {
    std::size_t size = 0;

    // bad fd
    EXPECT_EQ(QPTPool_generic_serialize_and_free(-1, nullptr, nullptr, 0, &size), 1);
    EXPECT_EQ(size, (std::size_t) 0);

    char temp[] = "XXXXXX";
    const int fd = mkstemp(temp);
    ASSERT_GE(fd, -1);

    // bad work
    EXPECT_EQ(QPTPool_generic_serialize_and_free(fd, nullptr, nullptr, 1, &size), 1);
    EXPECT_EQ(size, (std::size_t) 0);

    EXPECT_EQ(close(fd), 0);
    EXPECT_EQ(remove(temp), 0);
}

TEST(QueuePerThreadPool, generic_alloc_and_deserialize_error) {
    int *src = (int *) malloc(sizeof(int));
    *src = 1234;

    std::size_t size = 0;

    char temp[] = "XXXXXX";
    const int fd = mkstemp(temp);
    ASSERT_GE(fd, -1);

    // good serialize
    ASSERT_EQ(QPTPool_generic_serialize_and_free(fd, nullptr, src, sizeof(*src), &size), 0);
    ASSERT_EQ(size, sizeof(nullptr) + sizeof(sizeof(*src)) + sizeof(*src));

    QPTPool_f func = nullptr;

    // bad fd
    EXPECT_EQ(QPTPool_generic_alloc_and_deserialize(-1, &func, nullptr), 1);
    EXPECT_EQ(func, nullptr);

    // corrupt swap data
    ASSERT_EQ(lseek(fd, sizeof(func), SEEK_SET), (off_t) sizeof(func));
    const std::size_t BAD_SIZE = -2; // can't use -1 because that's the error value
    ASSERT_EQ(write(fd, &BAD_SIZE, sizeof(BAD_SIZE)), (ssize_t) sizeof(BAD_SIZE));

    // reset to start of swap
    ASSERT_EQ(lseek(fd, 0, SEEK_SET), (off_t) 0);

    // bad alloc
    EXPECT_EQ(QPTPool_generic_alloc_and_deserialize(fd, &func, nullptr), 1);
    EXPECT_EQ(func, nullptr);

    EXPECT_EQ(close(fd), 0);
    EXPECT_EQ(remove(temp), 0);
}

static int test_serialize_address(const int fd, QPTPool_f func, void *work, size_t *size) {
    if (write_size(fd, (void *) (uintptr_t) &func, sizeof(func)) != sizeof(func)) {
        return 1;
    }

    if (write_size(fd, &work, sizeof(work)) != sizeof(work)) {
        return 1;
    }

    *size += sizeof(func) + sizeof(work);

    return 0;
}

static int test_deserialize_address(const int fd, QPTPool_f *func, void **work) {
    if (read_size(fd, (void *) (uintptr_t) func, sizeof(*func)) != sizeof(*func)) {
        return 1;
    }

    if (read_size(fd, work, sizeof(*work)) != sizeof(*work)) {
        return 1;
    }

    return 0;
}
#endif

TEST(QueuePerThreadPool, enqueue_here) {
    QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
    ASSERT_NE(pool, nullptr);

    // not enqueued
    #ifdef QPTPOOL_SWAP
    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_ERROR, return_data, nullptr,
                                   test_serialize_address, test_deserialize_address),
              QPTPool_enqueue_ERROR);
    #else
    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_ERROR, return_data, nullptr),
              QPTPool_enqueue_ERROR);
    #endif

    // 1 item
    #ifdef QPTPOOL_SWAP
    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_WAIT,  return_data, nullptr,
                                   test_serialize_address, test_deserialize_address),
              QPTPool_enqueue_WAIT);
    #else
    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_WAIT,  return_data, nullptr),
              QPTPool_enqueue_WAIT);
    #endif

    #ifdef QPTPOOL_SWAP
    // need to set queue_limit first
    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_SWAP,  return_data, nullptr,
                                   test_serialize_address, test_deserialize_address),
              QPTPool_enqueue_ERROR);

    EXPECT_EQ(QPTPool_set_queue_limit(pool, 1), 0);

    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_SWAP,  return_data, nullptr,
                                   test_serialize_address, test_deserialize_address),
              QPTPool_enqueue_SWAP);
    #endif

    EXPECT_EQ(QPTPool_start(pool), 0);
    QPTPool_stop(pool);

    uint64_t expected = 1;
    #ifdef QPTPOOL_SWAP
    expected += THREADS + 1; // ran swapped cleanup once
    #endif

    EXPECT_EQ(QPTPool_threads_started(pool),   expected);
    EXPECT_EQ(QPTPool_threads_completed(pool), expected);

    QPTPool_destroy(pool);
}

#ifdef QPTPOOL_SWAP
TEST(QueuePerThreadPool, enqueue_swappable) {
    QPTPool_t *pool = QPTPool_init(1, nullptr);
    ASSERT_NE(pool, nullptr);
    EXPECT_EQ(QPTPool_set_queue_limit(pool, 1), 0);

    EXPECT_EQ(QPTPool_enqueue_swappable(pool, 0, return_data, nullptr,
                                        test_serialize_address, test_deserialize_address),
              QPTPool_enqueue_WAIT);

    // args that is not freed
    EXPECT_EQ(QPTPool_enqueue_swappable(pool, 0, return_data, nullptr,
                                        test_serialize_address, test_deserialize_address),
              QPTPool_enqueue_SWAP);

    // freed args
    EXPECT_EQ(QPTPool_enqueue_swappable(pool, 0, return_data, nullptr,
                                        test_serialize_address, test_deserialize_address),
              QPTPool_enqueue_SWAP);

    EXPECT_EQ(QPTPool_start(pool), 0);
    QPTPool_stop(pool);

    EXPECT_EQ(QPTPool_threads_started(pool),   (uint64_t) 2 + 3); // ran swapped cleanup twice
    EXPECT_EQ(QPTPool_threads_completed(pool), (uint64_t) 2 + 3); // ran swapped cleanup twice

    QPTPool_destroy(pool);
}
#endif

TEST(QueuePerThreadPool, wait_mem) {
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
    EXPECT_EQ(QPTPool_wait_mem_lte(pool, WAIT), PUSH);

    ASSERT_EQ(QPTPool_start(pool), 0);

    /* wait for pool to have at most WAIT items left */
    EXPECT_LE(QPTPool_wait_mem_lte(pool, WAIT), WAIT);

    /* wait for all work to complete */
    EXPECT_EQ(QPTPool_wait_mem(pool), (uint64_t) 0);

    QPTPool_stop(pool);
    QPTPool_destroy(pool);

    EXPECT_EQ(QPTPool_wait_mem(nullptr), (uint64_t) 0);
}

TEST(QueuePerThreadPool, wait) {
    QPTPool_wait(nullptr);

    // not running, so nothing to wait for
    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
        EXPECT_NE(pool, nullptr);
        QPTPool_wait(pool);
        QPTPool_destroy(pool);
    }

    // wait on no work
    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
        EXPECT_NE(pool, nullptr);
        EXPECT_EQ(QPTPool_start(pool), 0);
        QPTPool_wait(pool);
        QPTPool_stop(pool);
        QPTPool_destroy(pool);
    }

    #ifdef QPTPOOL_SWAP
    // wait on enqueued work
    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
        EXPECT_NE(pool, nullptr);
        EXPECT_EQ(QPTPool_set_queue_limit(pool, 1), 0);
        EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_SWAP, return_data, nullptr,
                                        test_serialize_address, test_deserialize_address),
                  QPTPool_enqueue_SWAP);
        EXPECT_EQ(QPTPool_start(pool), 0);
        QPTPool_wait(pool);
        QPTPool_stop(pool);
        QPTPool_destroy(pool);
    }
    #endif
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
                       const int src) {
    struct work_item {
        work_item()
            : tid(0),
              counter(0)
            {}

        std::size_t tid; // which thread is processing this work item
        std::size_t counter;
    };

    work_item w{};

    // macOS doesn't seem to like std::mutex
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

    QPTPool_t *pool = QPTPool_init(2, &mutex);
    ASSERT_NE(pool, nullptr);
    ASSERT_EQ(QPTPool_set_steal(pool, 1, 1), 0);
    ASSERT_EQ(QPTPool_start(pool), 0);

    // the first work item will be placed in src, but can be processed by any thread
    pthread_mutex_lock(&mutex);
    EXPECT_EQ(QPTPool_enqueue(pool, src,
                              [](QPTPool_t *ctx, const std::size_t id, void *data, void *args) -> int {
                                  work_item *w = static_cast<work_item *>(data);
                                  w->tid = id;
                                  increment_counter(ctx, id, &w->counter, args);

                                  // first work item is prevented from completing, blocking the thread
                                  pthread_mutex_t *mutex = static_cast<pthread_mutex_t *>(args);
                                  pthread_mutex_lock(mutex);
                                  pthread_mutex_unlock(mutex);
                                  return 0;
                              }, &w), QPTPool_enqueue_WAIT);

    // need to make sure only 1 work item got popped off
    // no racing here because only 1 thread runs
    while (w.counter < 1) {
        sched_yield();
    }

    uint64_t expected = 1;

    // no racing here because even though work is placed into
    // different threads, they end up getting stolen and run
    // serially by the thread that is not blocked
    if (find) {
        // place a work item behind the first work item for stealing
        #ifdef QPTPOOL_SWAP
        EXPECT_EQ(QPTPool_enqueue_here(pool, w.tid, queue, increment_counter, &w.counter,
                                       nullptr, nullptr),
                  queue);
        #else
        EXPECT_EQ(QPTPool_enqueue_here(pool, w.tid, queue, increment_counter, &w.counter),
                  queue);
        #endif

        // place a work item on the other thread, which triggers stealing the previous work item
        #ifdef QPTPOOL_SWAP
        EXPECT_EQ(QPTPool_enqueue_here(pool, !w.tid, QPTPool_enqueue_WAIT, increment_counter, &w.counter,
                                       nullptr, nullptr),
                  QPTPool_enqueue_WAIT);
        #else
        EXPECT_EQ(QPTPool_enqueue_here(pool, !w.tid, QPTPool_enqueue_WAIT, increment_counter, &w.counter),
                  QPTPool_enqueue_WAIT);
        #endif

        expected += 2;
    }
    else {
        // thread that is not blocked will look in both queues and not find anything to steal
    }

    // wait for thread that is not blocked to run its work (and stolen work item)
    while (w.counter < expected) {
        sched_yield();
    }

    // unlock thread src so that it can complete
    pthread_mutex_unlock(&mutex);

    QPTPool_stop(pool);

    EXPECT_EQ(w.counter, expected);
    EXPECT_EQ(QPTPool_threads_started(pool),   expected);
    EXPECT_EQ(QPTPool_threads_completed(pool), expected);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, steal_nothing) {
    test_steal(QPTPool_enqueue_WAIT, false, 0);
    test_steal(QPTPool_enqueue_WAIT, false, 1);
}

TEST(QueuePerThreadPool, steal_waiting) {
    test_steal(QPTPool_enqueue_WAIT, true, 0);
    test_steal(QPTPool_enqueue_WAIT, true, 1);
}

TEST(QueuePerThreadPool, steal_claimed) {
    struct work_item {
        work_item(std::size_t &counter)
            :  mutex(PTHREAD_MUTEX_INITIALIZER),
               tid(0),
               counter(counter)
            {}

        // macOS doesn't seem to like std::mutex
        pthread_mutex_t mutex;
        std::size_t tid;
        std::size_t &counter;
    };

    std::size_t counter = 0;
    work_item wi[2] = {counter, counter};

    QPTPool_t *pool = QPTPool_init(2, nullptr);
    ASSERT_NE(pool, nullptr);
    ASSERT_EQ(QPTPool_set_steal(pool, 1, 2), 0);

    QPTPool_f func = [](QPTPool_t *ctx, const std::size_t id, void *data, void *) -> int {
        work_item *w = static_cast<work_item *>(data);
        w->tid = id;
        increment_counter(ctx, id, &w->counter, nullptr);

        pthread_mutex_lock(&w->mutex);
        pthread_mutex_unlock(&w->mutex);
        return 0;
    };

    for(std::size_t i = 0; i < 2; i++) {
        work_item &w = wi[i];

        // prevent first work item in each thread from completing
        pthread_mutex_lock(&w.mutex);

        // each work item should be run by the target thread because
        // both threads start having work and both will block, so
        // nothing should be stolen
        #ifdef QPTPOOL_SWAP
        EXPECT_EQ(QPTPool_enqueue_here(pool, i, QPTPool_enqueue_WAIT, func, &w,
                                       nullptr, nullptr),
                  QPTPool_enqueue_WAIT);
        #else
        EXPECT_EQ(QPTPool_enqueue_here(pool, i, QPTPool_enqueue_WAIT, func, &w),
                  QPTPool_enqueue_WAIT);
        #endif
    }

    // trap this work item after the first work item
    #ifdef QPTPOOL_SWAP
    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_WAIT, increment_counter, &counter,
                                   nullptr, nullptr),
              QPTPool_enqueue_WAIT);
    #else
    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_WAIT, increment_counter, &counter),
              QPTPool_enqueue_WAIT);
    #endif

    ASSERT_EQ(QPTPool_start(pool), 0);

    // need to make sure the first work item in each thread started
    while (counter < 2) {
        sched_yield();
    }

    for(std::size_t i = 0; i < 2; i++) {
        EXPECT_EQ(wi[i].tid, i);
    }

    // let the work item in thread 1 complete so that thread 1
    // steals the first item from thread 0's claimed queue
    pthread_mutex_unlock(&wi[1].mutex);

    // wait for the stolen work item to finish
    while (counter < 3) {
        sched_yield();
    }

    // let thread 0 finish
    pthread_mutex_unlock(&wi[0].mutex);

    QPTPool_stop(pool);

    const uint64_t expected = 3;
    EXPECT_EQ(counter, expected);
    EXPECT_EQ(QPTPool_threads_started(pool),   expected);
    EXPECT_EQ(QPTPool_threads_completed(pool), expected);

    QPTPool_destroy(pool);
}

#ifdef QPTPOOL_SWAP
const char SWAPPED[] = "swapped";

/*
 * this function has to exist after the work item has
 * been swapped so it cannot be a temporary lambda
 */
static int swapped_function(QPTPool *, const size_t, void *data, void *) {
    char *buf = (char *) data;
    memcpy(buf, SWAPPED, sizeof(SWAPPED));
    return 0;
}

TEST(QueuePerThreadPool, swap) {
    char buf[1024] = {0};
    char *ptr = buf;
    EXPECT_STRNE(buf, SWAPPED);

    QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
    ASSERT_NE(pool, nullptr);

    // swap has been zero-ed, but not started
    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_SWAP, swapped_function, ptr,
                                   test_serialize_address, test_deserialize_address),
              QPTPool_enqueue_ERROR);

    EXPECT_EQ(QPTPool_set_queue_limit(pool, 1), 0);

    // swap has been started
    EXPECT_EQ(QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_SWAP, swapped_function, ptr,
                                   test_serialize_address, test_deserialize_address),
              QPTPool_enqueue_SWAP);

    ASSERT_EQ(QPTPool_start(pool), 0);

    QPTPool_stop(pool);

    EXPECT_STREQ(buf, SWAPPED);
    EXPECT_EQ(QPTPool_threads_started(pool),    (uint64_t)    THREADS + 1); // ran swapped cleanup once
    EXPECT_EQ(QPTPool_threads_completed(pool),  (uint64_t)    THREADS + 1); // ran swapped cleanup once
    EXPECT_EQ(QPTPool_work_swapped_count(pool), (uint64_t)    1);
    EXPECT_GT(QPTPool_work_swapped_size(pool),  (uint64_t)    0);

    QPTPool_destroy(pool);
}
#endif

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

#ifdef QPTPOOL_SWAP
TEST(QueuePerThreadPool, prop_queue_limit) {
    const uint64_t queue_limit = THREADS;

    char dir[1024];
    snprintf(dir, sizeof(dir), "%jd.swap.%" PRIu64,
             (intmax_t) getpid(), queue_limit / 2);

    // bad state
    {
        setup_pool(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_queue_limit(pool,    queue_limit),      1);
        QPTPool_stop(pool);
        QPTPool_destroy(pool);
    }

    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_queue_limit(nullptr, queue_limit),      1);
        EXPECT_EQ(QPTPool_set_queue_limit(pool,    queue_limit),      0);

        uint64_t get_queue_limit = 0;
        EXPECT_EQ(QPTPool_get_queue_limit(nullptr, &get_queue_limit), 1);
        EXPECT_EQ(QPTPool_get_queue_limit(pool,    nullptr),          0);
        EXPECT_EQ(QPTPool_get_queue_limit(pool,    &get_queue_limit), 0);
        EXPECT_EQ(get_queue_limit, static_cast<std::size_t>(THREADS));

        // swap path exists and is a directory
        EXPECT_EQ(mkdir(dir, S_IRWXU), 0);
        EXPECT_EQ(QPTPool_set_queue_limit(pool,    queue_limit),      1);
        EXPECT_EQ(rmdir(dir), 0);

        // reset to 0 to destroy swap and not recreate it
        EXPECT_EQ(QPTPool_set_queue_limit(pool,    0),                0);

        QPTPool_destroy(pool);
    }

    // QPTPool_init_with_props instead of QPTPool_init
    {
        // swap path exists and is a directory
        EXPECT_EQ(mkdir(dir, S_IRWXU), 0);
        EXPECT_EQ(qptpool_init_with_props(THREADS, queue_limit), nullptr);
        EXPECT_EQ(rmdir(dir), 0);

        // good
        QPTPool_t *pool = QPTPool_init_with_props(THREADS, nullptr, nullptr, nullptr, 1, "", 0, 0);
        EXPECT_NE(pool, nullptr);
        QPTPool_destroy(pool);
    }
}

TEST(QueuePerThreadPool, prop_swap_prefix) {
    const char swap_prefix[] = "swap_prefix";

    // bad state
    {
        setup_pool(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_swap_prefix(pool,    swap_prefix), 1);
        QPTPool_stop(pool);
        QPTPool_destroy(pool);
    }

    {
        QPTPool_t *pool = QPTPool_init(THREADS, nullptr);
        EXPECT_EQ(QPTPool_set_swap_prefix(nullptr, swap_prefix), 1);

        // set, but since queue_limit is 0, nothing happens
        EXPECT_EQ(QPTPool_set_swap_prefix(pool,    swap_prefix), 0);

        const char *get_swap_prefix = nullptr;
        EXPECT_EQ(QPTPool_get_swap_prefix(nullptr, &get_swap_prefix), 1);
        EXPECT_EQ(QPTPool_get_swap_prefix(pool,    nullptr),          0);
        EXPECT_EQ(QPTPool_get_swap_prefix(pool,    &get_swap_prefix), 0);
        EXPECT_STREQ(get_swap_prefix, swap_prefix);

        // queue_limit > 0, so swap is started
        EXPECT_EQ(QPTPool_set_queue_limit(pool,    THREADS),     0);
        EXPECT_EQ(QPTPool_set_swap_prefix(pool,    swap_prefix), 0);

        // change swap prefix but swap path exists and is a directory
        char dir[1024];
        snprintf(dir, sizeof(dir), "%jd.swap.%zu",
                 (intmax_t) getpid(), THREADS / 2);
        EXPECT_EQ(mkdir(dir, S_IRWXU), 0);
        EXPECT_EQ(QPTPool_set_swap_prefix(pool, ""), 1);
        EXPECT_EQ(rmdir(dir), 0);

        QPTPool_destroy(pool);
    }
}
#endif

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
