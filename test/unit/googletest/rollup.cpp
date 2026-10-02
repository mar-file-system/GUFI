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



#include <cstdlib>

#include <gtest/gtest.h>
#include <sqlite3.h>

#include "dbutils.h"
#include "rollup.h"
#include "str.h"

TEST(get_permissions, bad) {
    EXPECT_EQ(get_permissions(nullptr, "test", nullptr), -1);
}

TEST(rollup_init, bad) {
    str_t path = REFSTR("test", 4);
    EXPECT_EQ(rollup_init(nullptr, &path), 1);
}

TEST(rollup_child_attach, bad) {
    str_t child_path = REFSTR("test", 4);
    EXPECT_EQ(rollup_child_attach(nullptr, &child_path, SQLITE_OPEN_READONLY), nullptr);
}

TEST(rollup_child_process, bad) {
    rexa_t rexa;
    rexa.parent = (char *) "parent";
    rexa.child = (char *) "child";
    EXPECT_EQ(rollup_child_process(nullptr, nullptr, &rexa), 1);
}

TEST(get_isrolledup, bad) {
    EXPECT_EQ(get_isrolledup(nullptr, nullptr), 1);

    sqlite3 *db = nullptr;
    EXPECT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);
    EXPECT_EQ(sqlite3_exec(db, "CREATE TABLE " SUMMARY "(canrollup, isrolledup, isroot);",
                           nullptr, nullptr, nullptr), SQLITE_OK);

    int isrolledup = -1;

    // bad canrollup
    EXPECT_EQ(sqlite3_exec(db, "INSERT INTO " SUMMARY " VALUES ('-', 1, 1);",
                           nullptr, nullptr, nullptr), SQLITE_OK);
    EXPECT_EQ(get_isrolledup(db, &isrolledup), 1);
    EXPECT_EQ(isrolledup, -1);

    EXPECT_EQ(sqlite3_exec(db, "DELETE FROM " SUMMARY ";",
                           nullptr, nullptr, nullptr), SQLITE_OK);

    // bad isrolledup
    EXPECT_EQ(sqlite3_exec(db, "INSERT INTO " SUMMARY " VALUES (1, '-', 1);",
                           nullptr, nullptr, nullptr), SQLITE_OK);

    sqlite3_close(db);
}

TEST(bottomup_collect_treesummary, nullptr) {
    // fail at top
    EXPECT_EQ(bottomup_collect_treesummary(nullptr, "", nullptr, ISROLLEDUP_KNOWN_YES, 0, 0, nullptr), 1);

    char dirname[] = "XXXXXX";
    ASSERT_EQ(mkdtemp(dirname), dirname);

    sll_t sll;
    sll_init(&sll);
    sll_push_back(&sll, dirname);    // fail to open
    sll_push_back(&sll, (char *) ":memory:");

    EXPECT_EQ(bottomup_collect_treesummary(nullptr, "", &sll, ISROLLEDUP_KNOWN_YES, 0, 0, nullptr), 1);

    sll_destroy(&sll, nullptr);
    EXPECT_EQ(rmdir(dirname), 0);

    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open_v2(SQLITE_MEMORY, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, nullptr), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    // break at deleting old treesummary rows becase the table does not exist
    EXPECT_EQ(bottomup_collect_treesummary(db, "", &sll, ISROLLEDUP_KNOWN_YES, 0, 0, nullptr), 1);

    // create tables with a bad schemas that will run the delete
    EXPECT_EQ(sqlite3_exec(db, "CREATE TABLE summary (inode, isroot);", nullptr, nullptr, nullptr), SQLITE_OK);
    EXPECT_EQ(sqlite3_exec(db, "CREATE TABLE treesummary (inode);",     nullptr, nullptr, nullptr), SQLITE_OK);

    // isrolledup=1 fail to insert
    EXPECT_EQ(bottomup_collect_treesummary(db, "", &sll, ISROLLEDUP_KNOWN_YES, 0, 0, nullptr), 1);

    // isrolledup=0 fail querytsdb
    EXPECT_EQ(bottomup_collect_treesummary(db, "", &sll, ISROLLEDUP_KNOWN_NO, 0, 0, nullptr), 1);

    sqlite3_close(db);
}
