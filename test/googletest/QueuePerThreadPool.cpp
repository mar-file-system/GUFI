#include <gtest/gtest.h>

#include "QueuePerThreadPool.h"
#include "QueuePerThreadPoolPrivate.h"

TEST(QueuePerThreadPool, init_destroy) {
    EXPECT_EQ(QPTPool_init(0), nullptr);

    const size_t threads = 10;
    struct QPTPool * pool = QPTPool_init(threads);
    ASSERT_NE(pool, nullptr);

    for(size_t i = 0; i < threads; i++) {
        EXPECT_EQ(pool->data[i].threads_started, 0UL);
        EXPECT_EQ(pool->data[i].threads_successful, 0UL);
    }
    EXPECT_EQ(pool->incomplete, 0UL);

    QPTPool_destroy(pool);
}

// start threads without work
TEST(QueuePerThreadPool, no_work) {
    const size_t threads = 5;

    struct QPTPool * pool = QPTPool_init(threads);
    ASSERT_NE(pool, nullptr);

    EXPECT_EQ(QPTPool_start(pool,
                            [](struct QPTPool *, void *, const size_t, void *) -> int {
                                return 0;
                            },
                            nullptr), threads);
    QPTPool_wait(pool);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, provide_function) {
    const size_t threads = 5;

    struct QPTPool * pool = QPTPool_init(threads);
    ASSERT_NE(pool, nullptr);

    int vals[] = {0, 0, 0};
    const int default_value  = 1234;
    const int function_value = 5678;

    EXPECT_EQ(QPTPool_start(pool,
                            [](struct QPTPool *, void *, const size_t id, void * args) -> int {
                                ((int *) args)[id] = default_value;
                                return 0;
                            },
                            vals), threads);

    // this thread should set vals[0] = default_value
    QPTPool_enqueue(pool,
                    0,
                    nullptr,
                    nullptr);

    // this thread should set vals[1] = function_value
    QPTPool_enqueue(pool,
                    1,
                    nullptr,
                    [](struct QPTPool *, void *, const size_t id, void * args) -> int {
                        ((int *) args)[id] = function_value;
                        return 0;
                    });

    // this thread should set vals[2] = default_value
    QPTPool_enqueue(pool,
                    2,
                    nullptr,
                    nullptr);

    QPTPool_wait(pool);

    EXPECT_EQ(vals[0], default_value);
    EXPECT_EQ(vals[1], function_value);
    EXPECT_EQ(vals[2], default_value);

    QPTPool_destroy(pool);
}

// push work onto queues from the outside
TEST(QueuePerThreadPool, enqueue_external) {
    const size_t threads = 5;
    const size_t work_count = 11;

    struct QPTPool * pool = QPTPool_init(threads);
    ASSERT_NE(pool, nullptr);

    size_t * values = new size_t[work_count]();

    struct test_work {
        size_t * value;
        size_t counter;
    };

    EXPECT_EQ(QPTPool_start(pool,
                            [](struct QPTPool *, void * data, const size_t id, void *) -> int {
                                struct test_work * work = (struct test_work *) data;
                                *(work->value) = work->counter;
                                free(work);
                                return 0;
                            },
                            nullptr), threads);

    for(size_t i = 0; i < work_count; i++) {
        struct test_work * work = (struct test_work *) calloc(1, sizeof(struct test_work));
        work->value = &values[i];
        work->counter = i;

        QPTPool_enqueue(pool, i % threads, work, nullptr);
    }

    QPTPool_wait(pool);

    for(size_t i = 0; i < work_count; i++) {
        EXPECT_EQ(values[i], i);
    }
    EXPECT_EQ(QPTPool_threads_completed(pool), work_count);

    delete [] values;
    QPTPool_destroy(pool);
}

// push work onto queues from within the queue function
TEST(QueuePerThreadPool, enqueue_internal) {
    const size_t threads = 5;
    const size_t work_count = 11;

    struct QPTPool * pool = QPTPool_init(threads);
    ASSERT_NE(pool, nullptr);

    size_t * values = new size_t[work_count]();

    struct test_work {
        size_t index;
        size_t * values;
    };

    EXPECT_EQ(QPTPool_start(pool,
                            [](struct QPTPool * ctx, void * data, const size_t id, void * args) -> int {
                                struct test_work * work = (struct test_work *) data;
                                const size_t work_count = * (size_t *) args;

                                if (work->index >= work_count) {
                                    free(work);
                                    return 1;
                                }

                                work->values[work->index] = work->index;

                                struct test_work * next_work = (struct test_work *) calloc(1, sizeof(struct test_work));
                                next_work->index = work->index + 1;
                                next_work->values = work->values;

                                QPTPool_enqueue(ctx, id, next_work, nullptr);
                                free(work);

                                return 0;
                            },
                            (void *) &work_count), threads);

    // push only the first piece of work onto the queue
    // the worker function will push more work
    struct test_work * zero = (struct test_work *) calloc(1, sizeof(struct test_work));
    zero->index = 0;
    zero->values = values;

    QPTPool_enqueue(pool, 0, zero, nullptr);
    QPTPool_wait(pool);

    for(size_t i = 0; i < work_count; i++) {
        EXPECT_EQ(values[i], i);
    }
    EXPECT_EQ(QPTPool_threads_completed(pool), work_count);

    delete [] values;
    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, get_index) {
    const size_t threads = 5;
    const size_t work_count = 11;

    struct QPTPool * pool = QPTPool_init(threads);
    ASSERT_NE(pool, nullptr);

    EXPECT_EQ(QPTPool_start(pool,
                            [](struct QPTPool *, void * data, const size_t id, void *) -> int {
                                struct test_work * work = (struct test_work *) data;
                                free(work);
                                return 0;
                            },
                            nullptr), threads);

    for(size_t i = 0; i < work_count; i++) {
        QPTPool_enqueue(pool, i % threads, nullptr, nullptr);
    }

    QPTPool_wait(pool);

    for(size_t i = 0; i < threads; i++) {
        EXPECT_EQ(QPTPool_get_index(pool, pool->data[i].thread), i);
    }
    EXPECT_EQ(QPTPool_threads_completed(pool), work_count);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, threads_started) {
    const size_t threads = 5;
    const size_t work_count = 11;

    struct QPTPool * pool = QPTPool_init(threads);
    ASSERT_NE(pool, nullptr);

    EXPECT_EQ(QPTPool_start(pool,
                            [](struct QPTPool *, void * data, const size_t id, void *) -> int {
                                struct test_work * work = (struct test_work *) data;
                                free(work);
                                return 0;
                            },
                            nullptr), threads);

    for(size_t i = 0; i < work_count; i++) {
        QPTPool_enqueue(pool, i % threads, nullptr, nullptr);
    }

    QPTPool_wait(pool);

    EXPECT_EQ(QPTPool_threads_started(pool), work_count);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, threads_completed) {
    const size_t threads = 5;
    const size_t work_count = 11;

    struct QPTPool * pool = QPTPool_init(threads);
    ASSERT_NE(pool, nullptr);

    EXPECT_EQ(QPTPool_start(pool,
                            [](struct QPTPool *, void * data, const size_t id, void *) -> int {
                                struct test_work * work = (struct test_work *) data;
                                free(work);
                                return 0;
                            },
                            nullptr), threads);

    for(size_t i = 0; i < work_count; i++) {
        QPTPool_enqueue(pool, i % threads, nullptr, nullptr);
    }

    QPTPool_wait(pool);

    EXPECT_EQ(QPTPool_threads_completed(pool), work_count);

    QPTPool_destroy(pool);
}
