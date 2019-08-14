#include <gtest/gtest.h>

extern "C" {
#include "QueuePerThreadPoolPrivate.h"
}

TEST(SinglyLinkedList, init_destroy) {
    struct sll sll;
    EXPECT_EQ(&sll, sll_init(&sll));
    EXPECT_EQ(sll.head, nullptr);
    EXPECT_EQ(sll.tail, nullptr);

    sll_destroy(&sll);
}

TEST(SinglyLinkedList, push) {
    struct sll sll;
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

    sll_destroy(&sll);
}

TEST(SinglyLinkedList, move) {
    // create sll with 2 items
    struct sll sll_src;
    EXPECT_EQ(&sll_src, sll_init(&sll_src));
    EXPECT_EQ(sll_src.head, nullptr);
    EXPECT_EQ(sll_src.tail, nullptr);
    EXPECT_EQ(&sll_src, sll_push(&sll_src, nullptr));
    EXPECT_EQ(&sll_src, sll_push(&sll_src, nullptr));

    struct node * head = sll_src.head;
    struct node * tail = sll_src.tail;

    // empty sll
    struct sll sll_dst;
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

    sll_destroy(&sll_dst);
    sll_destroy(&sll_src);
}

TEST(SinglyLinkedList, head_node) {
    struct sll sll;
    EXPECT_EQ(&sll, sll_init(&sll));

    // empty sll
    EXPECT_EQ(sll.head, sll_head_node(&sll));

    EXPECT_EQ(&sll, sll_push(&sll, nullptr));
    EXPECT_EQ(&sll, sll_push(&sll, nullptr));

    // non-null head node
    EXPECT_EQ(sll.head, sll_head_node(&sll));
    EXPECT_NE(sll.tail, sll_head_node(&sll));

    sll_destroy(&sll);
}

TEST(SinglyLinkedList, next_node) {
    // create sll with 2 items
    struct sll sll;
    EXPECT_EQ(&sll, sll_init(&sll));

    // push first node
    EXPECT_EQ(&sll, sll_push(&sll, nullptr));
    struct node * head = sll_head_node(&sll);
    EXPECT_NE(head, nullptr);
    EXPECT_EQ(sll_next_node(head), nullptr);

    // push second node
    EXPECT_EQ(&sll, sll_push(&sll, nullptr));
    struct node * second = sll.tail;
    EXPECT_EQ(second, sll_next_node(head));

    // push third node
    EXPECT_EQ(&sll, sll_push(&sll, nullptr));
    struct node * third = sll.tail;
    EXPECT_EQ(third, sll_next_node(second));

    sll_destroy(&sll);
}

TEST(SinglyLinkedList, node_data) {
    int value = 1234;
    struct sll sll;
    EXPECT_EQ(&sll, sll_init(&sll));
    EXPECT_EQ(&sll, sll_push(&sll, &value));

    void * data = sll_node_data(sll_head_node(&sll));
    EXPECT_EQ(&value, data);
    EXPECT_EQ(value, * (int *) data);

    sll_destroy(&sll);
}

TEST(SinglyLinkedList, loop) {
    const size_t count = 10;
    int * values = new int[count]();

    // fill up queue with values
    struct sll sll;
    EXPECT_EQ(&sll, sll_init(&sll));
    for(size_t i = 0; i < count; i++) {
        values[i] = i;
        EXPECT_EQ(&sll, sll_push(&sll, &values[i]));
    }

    // check values
    size_t i = 0;
    for(struct node * node = sll_head_node(&sll); node; node = sll_next_node(node)) {
        EXPECT_EQ(values[i], * (int *) sll_node_data(node));
        i++;
    }

    sll_destroy(&sll);
    delete [] values;
}
