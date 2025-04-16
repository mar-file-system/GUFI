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



#include <cstdio>
#include <cstring>
#include <sqlite3.h>
#include <string>

#include <gtest/gtest.h>

#include "SinglyLinkedList.h"
#include "gufi_query/query_replacement.h"
#include "gufi_query/query_user_strs.h"
#include "trie.h"

static const char   FORMAT[]        = "A{A}B{BC}C{DEF}D";
static const size_t FORMAT_START[]  = {1, 5, 10};
static const size_t FORMAT_END[]    = {3, 8, 14};

TEST(query_user_str, no_replace) {
    refstr_t sql;
    sql.data = "SQL"; /* nothing to replace */
    sql.len = strlen(sql.data);

    sll_t fmts;
    EXPECT_EQ(save_replacements(&sql, &fmts, nullptr), (std::size_t) 0);

    trie_t *user_strs = trie_alloc();

    char *buf = nullptr;
    ASSERT_EQ(replace_sql(&sql, &fmts, nullptr, nullptr, user_strs, &buf), 0);
    EXPECT_EQ(buf, sql.data);
    EXPECT_EQ(strlen(buf), sql.len);

    trie_free(user_strs);
    cleanup_replacements(&fmts);
}

TEST(query_user_str, even) {
    refstr_t sql;
    sql.data = FORMAT;
    sql.len = sizeof(FORMAT) - 2; // ends on user string

    sll_t fmts;
    EXPECT_EQ(save_replacements(&sql, &fmts, nullptr), (std::size_t) 3);

    std::size_t i = 0;
    sll_loop(&fmts, node) {
        const usk_t *usk = (usk_t *) sll_node_data(node);
        EXPECT_EQ(usk->start, FORMAT_START[i]);
        EXPECT_EQ(usk->end,   FORMAT_END[i]);
        i++;
    }

    /* insert some user strings */
    trie_t *user_strs = trie_alloc();
    str_t A = {
        (char *) "a",
        1,
    };
    trie_insert(user_strs, "A", 1, &A, nullptr);
    str_t BC = {
        (char *) "bc",
        2,
    };
    trie_insert(user_strs, "BC", 2, &BC, nullptr);
    str_t DEF = {
        (char *) "def",
        3,
    };
    trie_insert(user_strs, "DEF", 3, &DEF, nullptr);

    ASSERT_EQ(trie_search(user_strs, "A",   1, nullptr), 1);
    ASSERT_EQ(trie_search(user_strs, "BC",  2, nullptr), 1);
    ASSERT_EQ(trie_search(user_strs, "DEF", 3, nullptr), 1);

    const char EXPECTED[] = "AaBbcCdef";

    char *buf = nullptr;
    ASSERT_EQ(replace_sql(&sql, &fmts, nullptr, nullptr, user_strs, &buf), 0);
    EXPECT_STREQ(buf, EXPECTED);
    EXPECT_EQ(strlen(buf), sizeof(EXPECTED) - 1);

    free(buf);

    trie_free(user_strs);
    cleanup_replacements(&fmts);
}

TEST(query_user_str, odd) {
    refstr_t sql;
    sql.data = FORMAT;
    sql.len = sizeof(FORMAT) - 1; // ends on non-format

    sll_t fmts;
    EXPECT_EQ(save_replacements(&sql, &fmts, nullptr), (std::size_t) 3);

    std::size_t i = 0;
    sll_loop(&fmts, node) {
        const usk_t *usk = (usk_t *) sll_node_data(node);
        EXPECT_EQ(usk->start, FORMAT_START[i]);
        EXPECT_EQ(usk->end,   FORMAT_END[i]);
        i++;
    }

    /* insert some user strings */
    trie_t *user_strs = trie_alloc();
    str_t A = {
        (char *) "a",
        1,
    };
    trie_insert(user_strs, "A", 1, &A, nullptr);
    str_t BC = {
        (char *) "bc",
        2,
    };
    trie_insert(user_strs, "BC", 2, &BC, nullptr);
    str_t DEF = {
        (char *) "def",
        3,
    };
    trie_insert(user_strs, "DEF", 3, &DEF, nullptr);

    ASSERT_EQ(trie_search(user_strs, "A",   1, nullptr), 1);
    ASSERT_EQ(trie_search(user_strs, "BC",  2, nullptr), 1);
    ASSERT_EQ(trie_search(user_strs, "DEF", 3, nullptr), 1);

    const char EXPECTED[] = "AaBbcCdefD";

    char *buf = nullptr;
    ASSERT_EQ(replace_sql(&sql, &fmts, nullptr, nullptr, user_strs, &buf), 0);
    EXPECT_STREQ(buf, EXPECTED);
    EXPECT_EQ(strlen(buf), sizeof(EXPECTED) - 1);

    free(buf);

    trie_free(user_strs);
    cleanup_replacements(&fmts);
}
