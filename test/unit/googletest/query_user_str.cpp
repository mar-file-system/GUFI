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
#include <string>

#include <gtest/gtest.h>
#include <sqlite3.h>

#include "SinglyLinkedList.h"
#include "trie.h"

#include "gufi_query/query_replacement.h"
#include "gufi_query/query_user_strs.h"

static const char        FORMAT[]        = "A{A}B{BC}C{DEF}D";
static const std::size_t FORMAT_START[]  = {1, 5, 10};
static const std::size_t FORMAT_END[]    = {3, 8, 14};

TEST(query_user_str, no_replace) {
    str_t sql = REFSTR("SQL", 3); // nothing to replace

    sll_t fmts;
    EXPECT_EQ(save_replacements(&sql, &fmts), (std::size_t) 0);

    trie_t *user_strs = trie_alloc();

    char *buf = nullptr;
    ASSERT_EQ(replace_sql(&sql, &fmts, user_strs, &buf), 0);
    EXPECT_EQ(buf, sql.data);
    EXPECT_EQ(strlen(buf), sql.len);

    trie_free(user_strs);
    cleanup_replacements(&fmts);
}

TEST(query_user_str, even) {
    str_t sql = REFSTR(FORMAT, sizeof(FORMAT) - 2); // ends on user string

    sll_t fmts;
    EXPECT_EQ(save_replacements(&sql, &fmts), (std::size_t) 3);

    std::size_t i = 0;
    sll_loop(&fmts, node) {
        const usk_t *usk = (usk_t *) sll_node_data(node);
        EXPECT_EQ(usk->start, FORMAT_START[i]);
        EXPECT_EQ(usk->end,   FORMAT_END[i]);
        i++;
    }

    /* insert some user strings */
    trie_t *user_strs = trie_alloc();
    str_t A = REFSTR("a", 1);
    trie_insert(user_strs, "A", 1, &A, nullptr);
    str_t BC = REFSTR("bc", 2);
    trie_insert(user_strs, "BC", 2, &BC, nullptr);
    str_t DEF = REFSTR("def", 3);
    trie_insert(user_strs, "DEF", 3, &DEF, nullptr);

    ASSERT_EQ(trie_search(user_strs, "A",   1, nullptr), 1);
    ASSERT_EQ(trie_search(user_strs, "BC",  2, nullptr), 1);
    ASSERT_EQ(trie_search(user_strs, "DEF", 3, nullptr), 1);

    const char EXPECTED[] = "AaBbcCdef";

    char *buf = nullptr;
    ASSERT_EQ(replace_sql(&sql, &fmts, user_strs, &buf), 0);
    EXPECT_STREQ(buf, EXPECTED);
    EXPECT_EQ(strlen(buf), sizeof(EXPECTED) - 1);

    free(buf);

    trie_free(user_strs);
    cleanup_replacements(&fmts);
}

TEST(query_user_str, odd) {
    str_t sql = REFSTR(FORMAT, sizeof(FORMAT) - 1); // ends on non-format

    sll_t fmts;
    EXPECT_EQ(save_replacements(&sql, &fmts), (std::size_t) 3);

    std::size_t i = 0;
    sll_loop(&fmts, node) {
        const usk_t *usk = (usk_t *) sll_node_data(node);
        EXPECT_EQ(usk->start, FORMAT_START[i]);
        EXPECT_EQ(usk->end,   FORMAT_END[i]);
        i++;
    }

    /* insert some user strings */
    trie_t *user_strs = trie_alloc();
    str_t A = REFSTR("a", 1);
    trie_insert(user_strs, "A", 1, &A, nullptr);
    str_t BC = REFSTR("bc", 2);
    trie_insert(user_strs, "BC", 2, &BC, nullptr);
    str_t DEF = REFSTR("def", 3);
    trie_insert(user_strs, "DEF", 3, &DEF, nullptr);

    ASSERT_EQ(trie_search(user_strs, "A",   1, nullptr), 1);
    ASSERT_EQ(trie_search(user_strs, "BC",  2, nullptr), 1);
    ASSERT_EQ(trie_search(user_strs, "DEF", 3, nullptr), 1);

    const char EXPECTED[] = "AaBbcCdefD";

    char *buf = nullptr;
    ASSERT_EQ(replace_sql(&sql, &fmts, user_strs, &buf), 0);
    EXPECT_STREQ(buf, EXPECTED);
    EXPECT_EQ(strlen(buf), sizeof(EXPECTED) - 1);

    free(buf);

    trie_free(user_strs);
    cleanup_replacements(&fmts);
}

TEST(replace_user_str, enomem) {
    str_t sql = REFSTR(FORMAT, sizeof(FORMAT) - 1); // ends on non-format

    /* insert some user strings */
    trie_t *user_strs = trie_alloc();
    str_t A = REFSTR("a", 1);
    trie_insert(user_strs, "A", 1, &A, nullptr);
    str_t BC = REFSTR("bc", 2);
    trie_insert(user_strs, "BC", 2, &BC, nullptr);
    str_t DEF = REFSTR("def", 3);
    trie_insert(user_strs, "DEF", 3, &DEF, nullptr);

    ASSERT_EQ(trie_search(user_strs, "A",   1, nullptr), 1);
    ASSERT_EQ(trie_search(user_strs, "BC",  2, nullptr), 1);
    ASSERT_EQ(trie_search(user_strs, "DEF", 3, nullptr), 1);

    std::size_t src_start = 0;
    usk_t pos = {1, 3};
    str_t replaced = {nullptr, ((std::size_t) -1) / 4, nullptr}; // reallocating this should not succeed
    std::size_t allocd = 0;

    EXPECT_EQ(replace_user_str(&sql, &src_start, &pos, &replaced, &allocd, user_strs), -1);

    trie_free(user_strs);
}
