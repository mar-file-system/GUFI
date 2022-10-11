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
    EXPECT_EQ(&sll, sll_push(&sll, nullptr));

    // head and tail are no longer NULL
    EXPECT_NE(sll.head, nullptr);
    EXPECT_NE(sll.tail, nullptr);

    // the head and tail nodes should be the same
    EXPECT_EQ(sll.head, sll.tail);

    // push second item onto sll
    EXPECT_EQ(&sll, sll_push(&sll, nullptr));

    // head and tail are still not NULL
    EXPECT_NE(sll.head, nullptr);
    EXPECT_NE(sll.tail, nullptr);

    // the head and tail nodes should be different now
    EXPECT_NE(sll.head, sll.tail);

    sll_destroy(&sll, nullptr);
}

TEST(SinglyLinkedList, move) {
    // create sll with 2 items
    sll_t sll_src;
    EXPECT_EQ(&sll_src, sll_init(&sll_src));
    EXPECT_EQ(sll_src.head, nullptr);
    EXPECT_EQ(sll_src.tail, nullptr);
    EXPECT_EQ(&sll_src, sll_push(&sll_src, nullptr));
    EXPECT_EQ(&sll_src, sll_push(&sll_src, nullptr));

    sll_node_t *head = sll_src.head;
    sll_node_t *tail = sll_src.tail;

    // empty sll
    sll_t sll_dst;
    EXPECT_EQ(&sll_dst, sll_init(&sll_dst));
    EXPECT_EQ(sll_dst.head, nullptr);
    EXPECT_EQ(sll_dst.tail, nullptr);

    // dst has old info
    EXPECT_EQ(&sll_dst, sll_move(&sll_dst, &sll_src));
    EXPECT_EQ(sll_dst.head, head);
    EXPECT_EQ(sll_dst.tail, tail);

    // src is empty
    EXPECT_EQ(sll_src.head, nullptr);
    EXPECT_EQ(sll_src.tail, nullptr);

    sll_destroy(&sll_dst, nullptr);
    sll_destroy(&sll_src, nullptr);
}

TEST(SinglyLinkedList, move_append) {
    // dst is empty
    {
        // move one item
        {
            sll_t dst;
            EXPECT_EQ(&dst, sll_init(&dst));

            sll_t src;
            EXPECT_EQ(&src, sll_init(&src));
            EXPECT_EQ(&src, sll_push(&src, nullptr));

            sll_node_t *src_head = src.head;
            sll_node_t *src_tail = src.tail;
            EXPECT_EQ(src_head, src_tail);

            EXPECT_EQ(&dst, sll_move_append(&dst, &src));
            EXPECT_EQ(dst.head, src_head);
            EXPECT_EQ(dst.tail, src_tail);
            EXPECT_EQ(src.head, nullptr);
            EXPECT_EQ(src.tail, nullptr);

            sll_destroy(&src, 0);
            sll_destroy(&dst, 0);
        }

        // move two items
        {
            sll_t dst;
            EXPECT_EQ(&dst, sll_init(&dst));

            sll_t src;
            EXPECT_EQ(&src, sll_init(&src));
            EXPECT_EQ(&src, sll_push(&src, nullptr));
            EXPECT_EQ(&src, sll_push(&src, nullptr));

            sll_node_t *src_head = src.head;
            sll_node_t *src_tail = src.tail;
            EXPECT_NE(src_head, src_tail);

            EXPECT_EQ(&dst, sll_move_append(&dst, &src));
            EXPECT_EQ(dst.head, src_head);
            EXPECT_EQ(dst.tail, src_tail);
            EXPECT_EQ(src.head, nullptr);
            EXPECT_EQ(src.tail, nullptr);

            sll_destroy(&src, 0);
            sll_destroy(&dst, 0);
        }
    }

    // src is empty
    {
        sll_t dst;
        EXPECT_EQ(&dst, sll_init(&dst));
        EXPECT_EQ(&dst, sll_push(&dst, nullptr));
        EXPECT_EQ(&dst, sll_push(&dst, nullptr));

        sll_node_t *dst_head = dst.head;
        sll_node_t *dst_tail = dst.tail;
        EXPECT_NE(dst_head, dst_tail);

        sll_t src;
        EXPECT_EQ(&src, sll_init(&src));

        // nothing in src
        EXPECT_EQ(src.head, nullptr);
        EXPECT_EQ(src.tail, nullptr);

        EXPECT_EQ(&dst, sll_move_append(&dst, &src));

        // dst not changed
        EXPECT_EQ(dst.head, dst_head);
        EXPECT_EQ(dst.tail, dst_tail);

        // still nothing in src
        EXPECT_EQ(src.head, nullptr);
        EXPECT_EQ(src.tail, nullptr);

        sll_destroy(&src, 0);
        sll_destroy(&dst, 0);
    }

    // neither empty
    {
        // move one item
        {
            sll_t dst;
            EXPECT_EQ(&dst, sll_init(&dst));
            EXPECT_EQ(&dst, sll_push(&dst, nullptr));
            EXPECT_EQ(&dst, sll_push(&dst, nullptr));

            sll_node_t *dst_head = dst.head;
            sll_node_t *dst_tail = dst.tail;
            EXPECT_NE(dst_head, dst_tail);

            sll_t src;
            EXPECT_EQ(&src, sll_init(&src));
            EXPECT_EQ(&src, sll_push(&src, nullptr));

            sll_node_t *src_head = src.head;
            sll_node_t *src_tail = src.tail;
            EXPECT_EQ(src_head, src_tail);

            EXPECT_EQ(&dst, sll_move_append(&dst, &src));

            EXPECT_EQ(dst.head, dst_head); // same head
            EXPECT_EQ(dst.tail, src_tail); // different tail

            // src empty
            EXPECT_EQ(src.head, nullptr);
            EXPECT_EQ(src.tail, nullptr);

            sll_destroy(&src, 0);
            sll_destroy(&dst, 0);
        }

        // move two items
        {
            sll_t dst;
            EXPECT_EQ(&dst, sll_init(&dst));
            EXPECT_EQ(&dst, sll_push(&dst, nullptr));
            EXPECT_EQ(&dst, sll_push(&dst, nullptr));

            sll_node_t *dst_head = dst.head;
            sll_node_t *dst_tail = dst.tail;
            EXPECT_NE(dst_head, dst_tail);

            sll_t src;
            EXPECT_EQ(&src, sll_init(&src));
            EXPECT_EQ(&src, sll_push(&src, nullptr));
            EXPECT_EQ(&src, sll_push(&src, nullptr));

            sll_node_t *src_head = src.head;
            sll_node_t *src_tail = src.tail;
            EXPECT_NE(src_head, src_tail);

            EXPECT_EQ(&dst, sll_move_append(&dst, &src));

            EXPECT_EQ(dst.head, dst_head); // same head
            EXPECT_EQ(dst.tail, src_tail); // different tail

            // src empty
            EXPECT_EQ(src.head, nullptr);
            EXPECT_EQ(src.tail, nullptr);

            sll_destroy(&src, 0);
            sll_destroy(&dst, 0);
        }
    }
}

TEST(SinglyLinkedList, head_node) {
    sll_t sll;
    EXPECT_EQ(&sll, sll_init(&sll));

    // empty sll
    EXPECT_EQ(sll.head, sll_head_node(&sll));

    EXPECT_EQ(&sll, sll_push(&sll, nullptr));
    EXPECT_EQ(&sll, sll_push(&sll, nullptr));

    // non-null head node
    EXPECT_EQ(sll.head, sll_head_node(&sll));
    EXPECT_NE(sll.tail, sll_head_node(&sll));

    sll_destroy(&sll, nullptr);
}

TEST(SinglyLinkedList, next_node) {
    // create sll with 2 items
    sll_t sll;
    EXPECT_EQ(&sll, sll_init(&sll));

    // push first node
    EXPECT_EQ(&sll, sll_push(&sll, nullptr));
    sll_node_t *head = sll_head_node(&sll);
    EXPECT_NE(head, nullptr);
    EXPECT_EQ(sll_next_node(head), nullptr);

    // push second node
    EXPECT_EQ(&sll, sll_push(&sll, nullptr));
    sll_node_t *second = sll.tail;
    EXPECT_EQ(second, sll_next_node(head));

    // push third node
    EXPECT_EQ(&sll, sll_push(&sll, nullptr));
    sll_node_t *third = sll.tail;
    EXPECT_EQ(third, sll_next_node(second));

    sll_destroy(&sll, nullptr);
}

TEST(SinglyLinkedList, node_data) {
    int value = 1234;
    sll_t sll;
    EXPECT_EQ(&sll, sll_init(&sll));
    EXPECT_EQ(&sll, sll_push(&sll, &value));

    void *data = sll_node_data(sll_head_node(&sll));
    EXPECT_EQ(&value, data);
    EXPECT_EQ(value, * (int *) data);

    sll_destroy(&sll, nullptr);
}

TEST(SinglyLinkedList, loop) {
    const size_t count = 10;
    int *values = new int[count]();

    // fill up queue with values
    sll_t sll;
    EXPECT_EQ(&sll, sll_init(&sll));
    for(size_t i = 0; i < count; i++) {
        values[i] = i;
        EXPECT_EQ(&sll, sll_push(&sll, &values[i]));
    }

    // check values
    size_t i = 0;
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
    ASSERT_EQ(&sll, sll_push(&sll, ptr));

    // use valgrind to check for leaks
    sll_destroy(&sll, free);
}
