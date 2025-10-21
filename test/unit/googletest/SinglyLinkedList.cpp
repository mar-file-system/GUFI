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

#include "SinglyLinkedList.h"

TEST(SinglyLinkedList, init_destroy) {
    sll_t sll;
    EXPECT_EQ(&sll, sll_init(&sll));
    EXPECT_EQ(sll.head, nullptr);
    EXPECT_EQ(sll.tail, nullptr);

    sll_destroy(&sll, nullptr);
}

TEST(SinglyLinkedList, push) {
    sll_t sll;
    EXPECT_EQ(&sll, sll_init(&sll));
    EXPECT_EQ(sll.head, nullptr);
    EXPECT_EQ(sll.tail, nullptr);

    // push first item onto sll
    sll_push(&sll, nullptr);

    // head and tail are no longer NULL
    EXPECT_NE(sll.head, nullptr);
    EXPECT_NE(sll.tail, nullptr);

    // the head and tail nodes should be the same
    EXPECT_EQ(sll.head, sll.tail);

    // push second item onto sll
    sll_push(&sll, nullptr);

    // head and tail are still not NULL
    EXPECT_NE(sll.head, nullptr);
    EXPECT_NE(sll.tail, nullptr);

    // the head and tail nodes should be different now
    EXPECT_NE(sll.head, sll.tail);

    sll_destroy(&sll, nullptr);
}

TEST(SinglyLinkedList, pop) {
    const std::size_t count = 5;

    sll_t sll;
    EXPECT_EQ(&sll, sll_init(&sll));
    EXPECT_EQ(sll.head, nullptr);
    EXPECT_EQ(sll.tail, nullptr);

    void *data = &sll;

    for(size_t items = 1; items <= count; items++)  {
        // push items
        for(size_t i = 1; i <= items; i++) {
            sll_push(&sll, data);
        }
        EXPECT_EQ(sll.size, items);

        // pop all but the last item
        for(size_t i = 1; i < items; i++) {
            void *popped = sll_pop(&sll);
            EXPECT_EQ(popped, data);

            EXPECT_NE(sll.head, nullptr);
            EXPECT_NE(sll.tail, nullptr);
            EXPECT_EQ(sll.size, items - i);
        }

        EXPECT_EQ(sll.head, sll.tail);
        EXPECT_EQ(sll.size, (size_t) 1);

        // pop last item
        void *popped = sll_pop(&sll);
        EXPECT_EQ(popped, data);

        // head and tail are NULL
        EXPECT_EQ(sll.head, nullptr);
        EXPECT_EQ(sll.tail, nullptr);
        EXPECT_EQ(sll.size, (size_t) 0);
    }

    // pop from empty list
    EXPECT_EQ(sll_pop(&sll), nullptr);

    sll_destroy(&sll, nullptr);
}

TEST(SinglyLinkedList, move_append_first) {
    static const std::size_t src_start = 10;
    static const std::size_t src_stop = 20;
    static const std::size_t src_count = src_stop - src_start;
    static const std::size_t src_half = src_count / 2;

    sll_t dst;
    sll_t src;

    // empty dst
    {
        sll_init(&dst);
        sll_init(&src);

        // empty src
        sll_move_append_first(&dst, &src, src_count);
        EXPECT_EQ(dst.head, nullptr);
        EXPECT_EQ(dst.tail, nullptr);
        EXPECT_EQ(dst.size, (uint64_t) 0);
        EXPECT_EQ(src.head, nullptr);
        EXPECT_EQ(src.tail, nullptr);
        EXPECT_EQ(src.size, (uint64_t) 0);

        // src with items
        {
            for(std::size_t i = 0; i < src_count; i++) {
                sll_push(&src, (void *) (uintptr_t) (i + src_start));
            }

            // move some of src
            sll_move_append_first(&dst, &src, src_half);
            EXPECT_NE(dst.head, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.head), (uint64_t) src_start);
            EXPECT_NE(dst.tail, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.tail), (uint64_t) src_start + src_half - 1);
            EXPECT_EQ(dst.size, src_half);
            EXPECT_NE(src.head, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(src.head), (uint64_t) src_start + src_half);
            EXPECT_NE(src.tail, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(src.tail), (uint64_t) src_start + src_count - 1);
            EXPECT_EQ(src.size, src_count - src_half);

            // move all of src
            sll_move_append_first(&dst, &src, src_count);
            EXPECT_NE(dst.head, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.head), (uint64_t) src_start);
            EXPECT_NE(dst.tail, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.tail), (uint64_t) src_start + src_count - 1);
            EXPECT_EQ(dst.size, src_count);
            EXPECT_EQ(src.head, nullptr);
            EXPECT_EQ(src.tail, nullptr);
            EXPECT_EQ(src.size, (uint64_t) 0);
        }

        sll_destroy(&src, nullptr); // not strictly necessary
        sll_destroy(&dst, nullptr);
    }

    // dst with items
    {
        sll_init(&dst);
        sll_init(&src);

        static const std::size_t dst_start = 0;
        static const std::size_t dst_stop = 10;
        static const std::size_t dst_count = dst_stop - dst_start;

        for(std::size_t i = 0; i < dst_count; i++) {
            sll_push(&dst, (void *) (uintptr_t) (i + dst_start));
        }

        // empty src
        {
            sll_move_append_first(&dst, &src, src_count);
            EXPECT_NE(dst.head, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.head), (uint64_t) dst_start);
            EXPECT_NE(dst.tail, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.tail), (uint64_t) dst_stop - 1);
            EXPECT_EQ(dst.size, dst_count);
            EXPECT_EQ(src.head, nullptr);
            EXPECT_EQ(src.tail, nullptr);
            EXPECT_EQ(src.size, (uint64_t) 0);
        }

        // src with items
        {
            for(std::size_t i = 0; i < src_count; i++) {
                sll_push(&src, (void *) (uintptr_t) (i + src_start));
            }

            // move some of src
            sll_move_append_first(&dst, &src, src_half);
            EXPECT_NE(dst.head, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.head), (uint64_t) dst_start);
            EXPECT_NE(dst.tail, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.tail), (uint64_t) src_start + src_half - 1);
            EXPECT_EQ(dst.size, dst_count + src_half);
            EXPECT_NE(src.head, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(src.head), (uint64_t) src_start + src_half);
            EXPECT_NE(src.tail, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(src.tail), (uint64_t) src_start + src_count - 1);
            EXPECT_EQ(src.size, src_count - src_half);

            // move all of src
            sll_move_append_first(&dst, &src, src_count);
            EXPECT_NE(dst.head, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.head), (uint64_t) dst_start);
            EXPECT_NE(dst.tail, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.tail), (uint64_t) src_start + src_count - 1);
            EXPECT_EQ(dst.size, dst_count + src_count);
            EXPECT_EQ(src.head, nullptr);
            EXPECT_EQ(src.tail, nullptr);
            EXPECT_EQ(src.size, (uint64_t) 0);
        }

        sll_destroy(&src, nullptr); // not strictly necessary
        sll_destroy(&dst, nullptr);
    }
}

TEST(SinglyLinkedList, move_append) {
    static const std::size_t src_start = 10;
    static const std::size_t src_stop = 20;
    static const std::size_t src_count = src_stop - src_start;

    sll_t dst;
    sll_t src;

    // empty dst
    {
        sll_init(&dst);
        sll_init(&src);

        // empty src
        sll_move_append(&dst, &src);
        EXPECT_EQ(dst.head, nullptr);
        EXPECT_EQ(dst.tail, nullptr);
        EXPECT_EQ(dst.size, (uint64_t) 0);
        EXPECT_EQ(src.head, nullptr);
        EXPECT_EQ(src.tail, nullptr);
        EXPECT_EQ(src.size, (uint64_t) 0);

        // src with items
        {
            for(std::size_t i = 0; i < src_count; i++) {
                sll_push(&src, (void *) (uintptr_t) (i + src_start));
            }

            // move all of src
            sll_move_append(&dst, &src);
            EXPECT_NE(dst.head, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.head), (uint64_t) src_start);
            EXPECT_NE(dst.tail, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.tail), (uint64_t) src_start + src_count - 1);
            EXPECT_EQ(dst.size, src_count);
            EXPECT_EQ(src.head, nullptr);
            EXPECT_EQ(src.tail, nullptr);
            EXPECT_EQ(src.size, (uint64_t) 0);
        }

        sll_destroy(&src, nullptr); // not strictly necessary
        sll_destroy(&dst, nullptr);
    }

    // dst with items
    {
        sll_init(&dst);
        sll_init(&src);

        static const std::size_t dst_start = 0;
        static const std::size_t dst_stop = 10;
        static const std::size_t dst_count = dst_stop - dst_start;

        for(std::size_t i = 0; i < dst_count; i++) {
            sll_push(&dst, (void *) (uintptr_t) (i + dst_start));
        }

        // empty src
        {
            sll_move_append(&dst, &src);
            EXPECT_NE(dst.head, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.head), (uint64_t) dst_start);
            EXPECT_NE(dst.tail, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.tail), (uint64_t) dst_stop - 1);
            EXPECT_EQ(dst.size, dst_count);
            EXPECT_EQ(src.head, nullptr);
            EXPECT_EQ(src.tail, nullptr);
            EXPECT_EQ(src.size, (uint64_t) 0);
        }

        // src with items
        {
            for(std::size_t i = 0; i < src_count; i++) {
                sll_push(&src, (void *) (uintptr_t) (i + src_start));
            }

            // move all of src
            sll_move_append(&dst, &src);
            EXPECT_NE(dst.head, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.head), (uint64_t) dst_start);
            EXPECT_NE(dst.tail, nullptr);
            EXPECT_EQ((uint64_t) (uintptr_t) sll_node_data(dst.tail), (uint64_t) src_start + src_count - 1);
            EXPECT_EQ(dst.size, dst_count + src_count);
            EXPECT_EQ(src.head, nullptr);
            EXPECT_EQ(src.tail, nullptr);
            EXPECT_EQ(src.size, (uint64_t) 0);
        }

        sll_destroy(&src, nullptr); // not strictly necessary
        sll_destroy(&dst, nullptr);
    }
}

TEST(SinglyLinkedList, head_node) {
    sll_t sll;
    EXPECT_EQ(&sll, sll_init(&sll));

    // empty sll
    EXPECT_EQ(sll.head, nullptr);
    EXPECT_EQ(sll.head, sll_head_node(&sll));

    sll_push(&sll, nullptr);
    sll_push(&sll, nullptr);

    // non-null head node
    EXPECT_NE(sll.head, nullptr);
    EXPECT_EQ(sll.head, sll_head_node(&sll));
    EXPECT_NE(sll.tail, sll_head_node(&sll));

    sll_destroy(&sll, nullptr);
}

TEST(SinglyLinkedList, next_node) {
    // create sll with 2 items
    sll_t sll;
    EXPECT_EQ(&sll, sll_init(&sll));

    // push first node
    sll_push(&sll, nullptr);
    sll_node_t *head = sll_head_node(&sll);
    EXPECT_NE(head, nullptr);
    EXPECT_EQ(sll_next_node(head), nullptr);

    // push second node
    sll_push(&sll, nullptr);
    sll_node_t *second = sll.tail;
    EXPECT_EQ(second, sll_next_node(head));

    // push third node
    sll_push(&sll, nullptr);
    sll_node_t *third = sll.tail;
    EXPECT_EQ(third, sll_next_node(second));

    sll_destroy(&sll, nullptr);
}

TEST(SinglyLinkedList, tail_node) {
    sll_t sll;
    EXPECT_EQ(&sll, sll_init(&sll));

    // empty sll
    EXPECT_EQ(sll.tail, nullptr);
    EXPECT_EQ(sll.tail, sll_tail_node(&sll));

    sll_push(&sll, nullptr);
    sll_push(&sll, nullptr);

    // non-null tail node
    EXPECT_NE(sll.tail, nullptr);
    EXPECT_EQ(sll.tail, sll_tail_node(&sll));
    EXPECT_NE(sll.head, sll_tail_node(&sll));

    sll_destroy(&sll, nullptr);
}

TEST(SinglyLinkedList, node_data) {
    int value = 1234;
    sll_t sll;
    EXPECT_EQ(&sll, sll_init(&sll));
    sll_push(&sll, &value);

    void *data = sll_node_data(sll_head_node(&sll));
    EXPECT_EQ(&value, data);
    EXPECT_EQ(value, * (int *) data);

    sll_destroy(&sll, nullptr);
}

TEST(SinglyLinkedList, loop) {
    const uint64_t count = 10;
    int *values = new int[count]();

    // fill up queue with values
    sll_t sll;
    EXPECT_EQ(&sll, sll_init(&sll));
    for(uint64_t i = 0; i < count; i++) {
        values[i] = i;
        sll_push(&sll, &values[i]);
    }

    // check values
    std::size_t i = 0;
    for(sll_node_t *node = sll_head_node(&sll); node; node = sll_next_node(node)) {
        EXPECT_EQ(values[i], * (int *) sll_node_data(node));
        i++;
    }

    sll_destroy(&sll, nullptr);
    delete [] values;
}

TEST(SinglyLinkedList, destroy_func) {
    sll_t sll;
    EXPECT_EQ(&sll, sll_init(&sll));

    void *ptr = malloc(10);
    ASSERT_NE(ptr, nullptr);
    sll_push(&sll, ptr);

    // use valgrind to check for leaks
    sll_destroy(&sll, free);
}
