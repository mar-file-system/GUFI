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



#include <gtest/gtest.h>

#include "QueuePerThreadPool.h"

#if defined(DEBUG) && defined(PER_THREAD_STATS)
#define INIT_QPTPOOL(nthreads, args) QPTPool_t *pool = QPTPool_init((nthreads), (args), nullptr, nullptr, 0, nullptr)
#else
#define INIT_QPTPOOL(nthreads, args) QPTPool_t *pool = QPTPool_init((nthreads), (args), nullptr, nullptr, 0)
#endif

static const     size_t THREADS = 5;
static constexpr size_t WORK_COUNT = 2 * THREADS + 1;

TEST(QueuePerThreadPool, zero_threads) {
    INIT_QPTPOOL(0, nullptr);
    EXPECT_EQ(pool, nullptr);
}

// start threads without work
TEST(QueuePerThreadPool, no_work) {
    INIT_QPTPOOL(THREADS, nullptr);
    ASSERT_NE(pool, nullptr);

    QPTPool_wait(pool);
    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, provide_function) {
    int vals[] = {0, 0, 0};
    const int default_value  = 1234;
    const int function_value = 5678;

    INIT_QPTPOOL(THREADS, vals);
    ASSERT_NE(pool, nullptr);

    // this thread should set vals[0] = default_value
    QPTPool_enqueue(pool,
                    0,
                    [](QPTPool_t *, const size_t id, void *, void *args) -> int {
                        ((int *) args)[id] = default_value;
                        return 0;
                    },
                    nullptr);

    // this thread should set vals[1] = function_value
    QPTPool_enqueue(pool,
                    1,
                    [](QPTPool_t *, const size_t id, void *, void *args) -> int {
                        ((int *) args)[id] = function_value;
                        return 0;
                    },
                    nullptr);

    // this thread should set vals[2] = default_value
    QPTPool_enqueue(pool,
                    2,
                    [](QPTPool_t *, const size_t id, void *, void *args) -> int {
                        ((int *) args)[id] = default_value;
                        return 0;
                    },
                    nullptr);

    QPTPool_wait(pool);

    EXPECT_EQ(vals[0], default_value);
    EXPECT_EQ(vals[1], function_value);
    EXPECT_EQ(vals[2], default_value);

    QPTPool_destroy(pool);
}

// push work onto queues from the outside
TEST(QueuePerThreadPool, enqueue_external) {
    size_t *values = new size_t[WORK_COUNT]();

    struct test_work {
        size_t *value;
        size_t counter;
    };

    INIT_QPTPOOL(THREADS, nullptr);
    ASSERT_NE(pool, nullptr);

    for(size_t i = 0; i < WORK_COUNT; i++) {
        struct test_work *work = (struct test_work *) calloc(1, sizeof(struct test_work));
        work->value = &values[i];
        work->counter = i;

        QPTPool_enqueue(pool, i % THREADS,
                        [](QPTPool_t *, const size_t, void *data, void *) -> int {
                            struct test_work *work = (struct test_work *) data;
                            * (work->value) = work->counter;
                            free(work);
                            return 0;
                        }, work);
    }

    QPTPool_wait(pool);

    for(size_t i = 0; i < WORK_COUNT; i++) {
        EXPECT_EQ(values[i], i);
    }
    EXPECT_EQ(QPTPool_threads_completed(pool), WORK_COUNT);

    delete [] values;
    QPTPool_destroy(pool);
}

struct test_work {
    size_t index;
    size_t *values;
};

static int recursive(QPTPool_t *pool, const size_t id, void *data, void *args) {
    struct test_work *work = (struct test_work *) data;
    const size_t work_count = * (size_t *) args;

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
    size_t *values = new size_t[WORK_COUNT]();

    INIT_QPTPOOL(THREADS, (void *) &WORK_COUNT);
    ASSERT_NE(pool, nullptr);

    // push only the first piece of work onto the queue
    // the worker function will push more work
    struct test_work *zero = (struct test_work *) calloc(1, sizeof(struct test_work));
    zero->index = 0;
    zero->values = values;

    QPTPool_enqueue(pool, 0, recursive, zero);
    QPTPool_wait(pool);

    for(size_t i = 0; i < WORK_COUNT; i++) {
        EXPECT_EQ(values[i], i);
    }
    EXPECT_EQ(QPTPool_threads_completed(pool), WORK_COUNT);

    delete [] values;
    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, threads_started) {
    INIT_QPTPOOL(THREADS, nullptr);
    ASSERT_NE(pool, nullptr);

    for(size_t i = 0; i < WORK_COUNT; i++) {
        QPTPool_enqueue(pool, i % THREADS,
                        [](QPTPool_t *, const size_t, void *data, void *) -> int {
                            struct test_work *work = (struct test_work *) data;
                            free(work);
                            return 0;
                        },
                        nullptr);
    }

    QPTPool_wait(pool);

    EXPECT_EQ(QPTPool_threads_started(pool), WORK_COUNT);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, threads_completed) {
    INIT_QPTPOOL(THREADS, nullptr);
    ASSERT_NE(pool, nullptr);

    for(size_t i = 0; i < WORK_COUNT; i++) {
        QPTPool_enqueue(pool, i % THREADS,
                        [](QPTPool_t *, const size_t, void *data, void *) -> int {
                            struct test_work *work = (struct test_work *) data;
                            free(work);
                            return 0;
                        }, nullptr);
    }

    QPTPool_wait(pool);

    EXPECT_EQ(QPTPool_threads_completed(pool), WORK_COUNT);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, custom_next) {
    size_t *values = new size_t[WORK_COUNT]();
    QPTPool_t *pool = QPTPool_init(THREADS,
                                        values,
                                        [](const size_t, const size_t prev, const size_t threads,
                                           void *, void *) -> size_t {
                                            return (prev?prev:threads) - 1;
                                        }, nullptr,
                                        0
                                        #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                        , nullptr
                                        #endif
                                        );
    ASSERT_NE(pool, nullptr);

    for(size_t i = 0; i < WORK_COUNT; i++) {
        struct test_work *work = (struct test_work *) calloc(1, sizeof(struct test_work));
        work->index = i;
        work->values = nullptr;

        QPTPool_enqueue(pool, i % THREADS,
                        [](QPTPool_t *, const size_t id, void *data, void *args) -> int {
                            size_t *values = (size_t *) args;
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
    for(size_t i = 0; i < THREADS; i++) {
        EXPECT_EQ(values[i], i);
    }

    // second set of work was pushed to previous threads
    for(size_t i = THREADS; i < (2 * THREADS); i++) {
        EXPECT_EQ(values[i], (i - 1) % THREADS);
    }

    // last work was pushed to (0 - 2) % THREADS
    EXPECT_EQ(values[WORK_COUNT - 1], THREADS - 2);

    QPTPool_destroy(pool);
    delete [] values;
}
