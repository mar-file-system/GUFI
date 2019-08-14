#include <gtest/gtest.h>

extern "C" {
#include "QueuePerThreadPool.h"
#include "QueuePerThreadPoolPrivate.h"
}

TEST(QueuePerThreadPool, init_destroy) {
    EXPECT_EQ(QPTPool_init(0), nullptr);

    const size_t threads = 10;
    struct QPTPool * pool = QPTPool_init(threads);
    ASSERT_NE(pool, nullptr);

    for(size_t i = 0; i < threads; i++) {
        EXPECT_EQ(pool->data[i].id, i);
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
                            [](struct QPTPool *, void *, const size_t, size_t *, void *) -> int {
                                return 0;
                            },
                            nullptr), threads);
    QPTPool_wait(pool);

    QPTPool_destroy(pool);
}

// push work onto queues from the outside with QPTPool_enqueue_external
TEST(QueuePerThreadPool, external_work) {
    const size_t threads = 5;
    const size_t work_count = 11;

    struct QPTPool * pool = QPTPool_init(threads);
    ASSERT_NE(pool, nullptr);

    size_t * values = new size_t[work_count]();

    struct test_work {
        size_t * value;
        size_t counter;
    };

    for(size_t i = 0; i < work_count; i++) {
        struct test_work * work = (struct test_work *) calloc(1, sizeof(struct test_work));
        work->value = &values[i];
        work->counter = i;

        QPTPool_enqueue_external(pool, work);
    }

    EXPECT_EQ(QPTPool_start(pool,
                            [](struct QPTPool *, void * data, const size_t id, size_t *, void *) -> int {
                                struct test_work * work = (struct test_work *) data;
                                *(work->value) = work->counter;
                                free(work);
                                return 0;
                            },
                            nullptr), threads);
    QPTPool_wait(pool);

    for(size_t i = 0; i < work_count; i++) {
        EXPECT_EQ(values[i], i);
    }
    EXPECT_EQ(QPTPool_threads_completed(pool), work_count);

    delete [] values;
    QPTPool_destroy(pool);
}

// push work onto queues from the outside with QPTPool_enqueue_internal
TEST(QueuePerThreadPool, external_work_using_internal_enqueue) {
    const size_t threads = 5;
    const size_t work_count = 11;

    struct QPTPool * pool = QPTPool_init(threads);
    ASSERT_NE(pool, nullptr);

    size_t * values = new size_t[work_count]();

    struct test_work {
        size_t * value;
        size_t counter;
    };

    size_t next_queue = 0;
    for(size_t i = 0; i < work_count; i++) {
        struct test_work * work = (struct test_work *) calloc(1, sizeof(struct test_work));
        work->value = &values[i];
        work->counter = i;

        QPTPool_enqueue_internal(pool, work, &next_queue);
    }

    EXPECT_EQ(QPTPool_start(pool,
                            [](struct QPTPool *, void * data, const size_t id, size_t *, void *) -> int {
                                struct test_work * work = (struct test_work *) data;
                                *(work->value) = work->counter;
                                free(work);
                                return 0;
                            },
                            nullptr), threads);
    QPTPool_wait(pool);

    EXPECT_EQ(next_queue, work_count % threads);
    for(size_t i = 0; i < work_count; i++) {
        EXPECT_EQ(values[i], i);
    }
    EXPECT_EQ(QPTPool_threads_completed(pool), work_count);

    delete [] values;
    QPTPool_destroy(pool);
}

// push work onto queues with QPTPool_enqueue_internal
TEST(QueuePerThreadPool, internal_work) {
    const size_t threads = 5;
    const size_t work_count = 11;

    struct QPTPool * pool = QPTPool_init(threads);
    ASSERT_NE(pool, nullptr);

    size_t * values = new size_t[work_count]();

    struct test_work {
        size_t index;
        size_t * values;
    };

    // push only the first piece of work onto the queue
    // the worker function will push more work
    struct test_work * zero = (struct test_work *) calloc(1, sizeof(struct test_work));
    zero->index = 0;
    zero->values = values;
    QPTPool_enqueue_external(pool, zero);

    EXPECT_EQ(QPTPool_start(pool,
                            [](struct QPTPool * ctx, void * data, const size_t id, size_t * next_queue, void * args) -> int {
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

                                QPTPool_enqueue_internal(ctx, next_work, next_queue);
                                free(work);

                                return 0;
                            },
                            (void *) &work_count), threads);
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

    for(size_t i = 0; i < work_count; i++) {
        QPTPool_enqueue_external(pool, nullptr);
    }

    EXPECT_EQ(QPTPool_start(pool,
                            [](struct QPTPool *, void * data, const size_t id, size_t *, void *) -> int {
                                struct test_work * work = (struct test_work *) data;
                                free(work);
                                return 0;
                            },
                            nullptr), threads);

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

    for(size_t i = 0; i < work_count; i++) {
        QPTPool_enqueue_external(pool, nullptr);
    }

    EXPECT_EQ(QPTPool_start(pool,
                            [](struct QPTPool *, void * data, const size_t id, size_t *, void *) -> int {
                                struct test_work * work = (struct test_work *) data;
                                free(work);
                                return 0;
                            },
                            nullptr), threads);

    QPTPool_wait(pool);

    EXPECT_EQ(QPTPool_threads_started(pool), work_count);

    QPTPool_destroy(pool);
}

TEST(QueuePerThreadPool, threads_completed) {
    const size_t threads = 5;
    const size_t work_count = 11;

    struct QPTPool * pool = QPTPool_init(threads);
    ASSERT_NE(pool, nullptr);

    for(size_t i = 0; i < work_count; i++) {
        QPTPool_enqueue_external(pool, nullptr);
    }

    EXPECT_EQ(QPTPool_start(pool,
                            [](struct QPTPool *, void * data, const size_t id, size_t *, void *) -> int {
                                struct test_work * work = (struct test_work *) data;
                                free(work);
                                return 0;
                            },
                            nullptr), threads);

    QPTPool_wait(pool);

    EXPECT_EQ(QPTPool_threads_completed(pool), work_count);

    QPTPool_destroy(pool);
}
