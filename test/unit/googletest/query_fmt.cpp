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

static const char   FORMAT[]        = "A%nB%iC%sD";
static const size_t FORMAT_POS[]    = {1, 4, 7};
static const char   SEARCH[]        = "search";
static const char   BASENAME[]      = "prefix";
static const char   SOURCE_PREFIX[] = "prefix";    // replaces SEARCH/BASENAME when using %s

TEST(query_fmt, no_replace) {
    refstr_t sql;
    sql.data = "SQL"; /* nothing to replace */
    sql.len = strlen(sql.data);

    sll_t fmts;

    refstr_t source_prefix;
    source_prefix.data = nullptr;
    source_prefix.len = 0;

    EXPECT_EQ(save_replacements(&sql, &fmts, &source_prefix), (std::size_t) 0);

    struct work *work = new_work_with_name(SEARCH, sizeof(SEARCH) - 1, BASENAME, sizeof(BASENAME) - 1);
    work->root_parent.len = sizeof(SEARCH) - 1;
    work->root_basename_len = sizeof(BASENAME) - 1;

    char *buf = nullptr;
    ASSERT_EQ(replace_sql(&sql, &fmts, &source_prefix, work, nullptr, &buf), 0);
    EXPECT_EQ(buf, sql.data);
    EXPECT_EQ(strlen(buf), sql.len);

    free(work);

    cleanup_replacements(&fmts);
}

TEST(query_fmt, even) {
    refstr_t sql;
    sql.data = FORMAT;
    sql.len = sizeof(FORMAT) - 2; // ends on format

    sll_t fmts;

    refstr_t source_prefix;
    source_prefix.data = SOURCE_PREFIX;
    source_prefix.len = sizeof(SOURCE_PREFIX) - 1;

    EXPECT_EQ(save_replacements(&sql, &fmts, &source_prefix), (std::size_t) 3);

    std::size_t i = 0;
    sll_loop(&fmts, node) {
        const std::size_t pos = * (std::size_t *) sll_node_data(node);
        EXPECT_EQ(pos, FORMAT_POS[i++]);
    }

    struct work *work = new_work_with_name(SEARCH, sizeof(SEARCH) - 1, BASENAME, sizeof(BASENAME) - 1);
    work->root_parent.len = sizeof(SEARCH) - 1 + 1; // +1 to count trailing slash
    work->root_basename_len = sizeof(BASENAME) - 1;

    const size_t expected_len = 1 +
        (sizeof(BASENAME) - 1) +
        1 +
        (sizeof(SEARCH) - 1) + 1 + (sizeof(BASENAME) - 1) +
        1 +
        (sizeof(SOURCE_PREFIX) - 1);
    char *expected = (char *) malloc(expected_len + 1);
    snprintf(expected, expected_len + 1, "A%sB%s/%sC%s",
             BASENAME,
             SEARCH, BASENAME,
             SOURCE_PREFIX);

    char *buf = nullptr;
    ASSERT_EQ(replace_sql(&sql, &fmts, &source_prefix, work, nullptr, &buf), 0);
    EXPECT_STREQ(buf, expected);
    EXPECT_EQ(strlen(buf), expected_len);

    free(buf);
    free(expected);
    free(work);

    cleanup_replacements(&fmts);
}

TEST(query_fmt, odd) {
    refstr_t sql;
    sql.data = FORMAT;
    sql.len = sizeof(FORMAT) - 1; // ends on non-format

    sll_t fmts;

    refstr_t source_prefix;
    source_prefix.data = SOURCE_PREFIX;
    source_prefix.len = sizeof(SOURCE_PREFIX) - 1;

    EXPECT_EQ(save_replacements(&sql, &fmts, &source_prefix), (std::size_t) 3);

    std::size_t i = 0;
    sll_loop(&fmts, node) {
        const std::size_t pos = * (std::size_t *) sll_node_data(node);
        EXPECT_EQ(pos, FORMAT_POS[i++]);
    }

    struct work *work = new_work_with_name(SEARCH, sizeof(SEARCH) - 1, BASENAME, sizeof(BASENAME) - 1);
    work->root_parent.len = sizeof(SEARCH) - 1 + 1; // +1 to count trailing slash
    work->root_basename_len = sizeof(BASENAME) - 1;

    const size_t expected_len = 1 +
        (sizeof(BASENAME) - 1) +
        1 +
        (sizeof(SEARCH) - 1) + 1 + (sizeof(BASENAME) - 1) +
        1 +
        (sizeof(SOURCE_PREFIX) - 1) +
        1;
    char *expected = (char *) malloc(expected_len + 1);
    snprintf(expected, expected_len + 1, "A%sB%s/%sC%sD",
             BASENAME,
             SEARCH, BASENAME,
             SOURCE_PREFIX);

    char *buf = nullptr;
    ASSERT_EQ(replace_sql(&sql, &fmts, &source_prefix, work, nullptr, &buf), 0);
    EXPECT_STREQ(buf, expected);
    EXPECT_EQ(strlen(buf), expected_len);

    free(buf);
    free(expected);
    free(work);

    cleanup_replacements(&fmts);
}
