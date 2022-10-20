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

extern "C" {

#include "bf.h"
#include "trie.h"

}

TEST(trie, alloc_free) {
    trie_t *root = trie_alloc();
    ASSERT_NE(root, nullptr);

    trie_free(root);
}

TEST(trie, insert_nullptr) {
    trie_t *root = trie_alloc();
    ASSERT_NE(root, nullptr);

    trie_insert(root, nullptr, 0);

    EXPECT_EQ(trie_search(root, nullptr, 0), 0);

    trie_free(root);
}

TEST(trie, insert_empty) {
    char buf[MAXPATH] = "";

    trie_t *root = trie_alloc();
    ASSERT_NE(root, nullptr);

    trie_insert(root, buf, 0);

    EXPECT_EQ(trie_search(root, buf, 0), 1);

    trie_free(root);
}

TEST(trie, search) {
    char buf[MAXPATH] = "0123";

    trie_t *root = trie_alloc();
    ASSERT_NE(root, nullptr);

    trie_insert(root, buf, 4);

    EXPECT_EQ(trie_search(root, buf, 4),  1);

    trie_free(root);
}

TEST(trie, have_children) {
    char buf[MAXPATH] = "0123";

    trie_t *root = trie_alloc();
    ASSERT_NE(root, nullptr);

    EXPECT_EQ(trie_have_children(root), 0);

    trie_insert(root, buf, 4);

    EXPECT_EQ(trie_have_children(root), 1);

    trie_free(root);
}

TEST(trie, delete) {
    char sub[MAXPATH] = "01";
    size_t sub_len = strlen(sub);
    char str[MAXPATH] = "0123";
    size_t str_len = strlen(str);

    trie_t *root = trie_alloc();
    ASSERT_NE(root, nullptr);

    {
        /* insert long string and sub string */
        trie_insert(root, str, str_len);
        trie_insert(root, sub, sub_len);
        EXPECT_EQ(trie_search(root, str, str_len), 1);
        EXPECT_EQ(trie_search(root, sub, sub_len), 1);

        /* longer string is deleted, but the shorter string remains */
        EXPECT_EQ(trie_delete(root, str, str_len), 0);
        EXPECT_NE(root, nullptr);
        EXPECT_EQ(trie_search(root, str, str_len), 0);
        EXPECT_EQ(trie_search(root, sub, sub_len), 1);
    }

    {
        /* insert long string and sub string */
        trie_insert(root, str, str_len);
        trie_insert(root, sub, sub_len);
        EXPECT_EQ(trie_search(root, str, str_len), 1);
        EXPECT_EQ(trie_search(root, sub, sub_len), 1);

        /* shorter string is deleted, but the longer string remains */
        EXPECT_EQ(trie_delete(root, sub, sub_len), 0);
        EXPECT_NE(root, nullptr);
        EXPECT_EQ(trie_search(root, str, str_len), 1);
        EXPECT_EQ(trie_search(root, sub, sub_len), 0);
    }

    /* deleteing the remaining long string deletes the entire chain */
    EXPECT_EQ(trie_search(root, str, str_len), 1);
    EXPECT_EQ(trie_delete(root, str, str_len), 1);
    EXPECT_NE(root, nullptr);
    EXPECT_EQ(trie_search(root, str, str_len), 0);

    trie_free(root);
}

TEST(trie, substring) {
    char sub[MAXPATH] = "01";
    size_t sub_len = strlen(sub);
    char str[MAXPATH] = "0123";
    size_t str_len = strlen(str);

    trie_t *root = trie_alloc();
    ASSERT_NE(root, nullptr);

    trie_insert(root, sub, sub_len);
    EXPECT_EQ(trie_search(root, sub, sub_len), 1);
    EXPECT_EQ(trie_search(root, str, str_len), 0);

    trie_insert(root, str, str_len);
    EXPECT_EQ(trie_search(root, sub, sub_len), 1);
    EXPECT_EQ(trie_search(root, str, str_len), 1);

    trie_free(root);

    root = trie_alloc();
    ASSERT_NE(root, nullptr);

    trie_insert(root, str, str_len);
    EXPECT_EQ(trie_search(root, sub, sub_len), 0);
    EXPECT_EQ(trie_search(root, str, str_len), 1);

    trie_insert(root, sub, sub_len);
    EXPECT_EQ(trie_search(root, sub, sub_len), 1);
    EXPECT_EQ(trie_search(root, str, str_len), 1);

    trie_free(root);
}
