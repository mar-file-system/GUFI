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

#include <ctime>
#include <grp.h>
#include <pwd.h>
#include <sqlite3.h>
#include <sys/types.h>
#include <unistd.h>

extern "C" {

#include "dbutils.h"

}

int str_output(void * args, int, char ** data, char **) {
    char * output = static_cast <char *> (args);
    memcpy(output, data[0], strlen(data[0]));
    return 0;
}

TEST(addqueryfuncs, path) {
    // index was placed into basename
    const char index[MAXPATH] = "/prefix0/prefix1/basename";
    SNPRINTF(gps[0].gpath, MAXPATH, "%s", index);

    sqlite3 * db = nullptr;
    ASSERT_EQ(sqlite3_open(":memory:", &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);
    ASSERT_EQ(addqueryfuncs(db, 0, 0, nullptr), 0);

    // pass in an empty string into path
    {
        char query[MAXSQL] = {};
        SNPRINTF(query, MAXSQL, "SELECT path('')");

        // the path returned by the query is the index prefix with the original basename
        char output[MAXPATH] = {};
        EXPECT_EQ(sqlite3_exec(db, query, str_output, output, nullptr), SQLITE_OK);

        EXPECT_STREQ(output, "/prefix0/prefix1/basename");
    }

    // call path with contents
    {
        // original tree was indexed at root
        const char src[MAXPATH] = "root/level1/level2";

        // use value obtained from summary table
        char query[MAXSQL] = {};
        SNPRINTF(query, MAXSQL, "SELECT path('%s')", src);

        // the path returned by the query is the index prefix with the original basename
        char output[MAXPATH] = {};
        EXPECT_EQ(sqlite3_exec(db, query, str_output, output, nullptr), SQLITE_OK);

        EXPECT_STREQ(output, "/prefix0/prefix1/basename/level1/level2");
    }

    sqlite3_close(db);
}

TEST(addqueryfuncs, fpath) {
    const char fpath[MAXPATH] = "fpath";
    SNFORMAT_S(gps[0].gfpath, MAXPATH, 1, fpath, strlen(fpath));

    sqlite3 * db = nullptr;
    ASSERT_EQ(sqlite3_open(":memory:", &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db, 0, 0, nullptr), 0);

    char output[MAXPATH] = {};
    ASSERT_EQ(sqlite3_exec(db, "SELECT fpath()", str_output, output, NULL), SQLITE_OK);

    EXPECT_STREQ(output, fpath);

    sqlite3_close(db);
}

TEST(addqueryfuncs, epath) {
    const char epath[MAXPATH] = "epath";
    SNFORMAT_S(gps[0].gepath, MAXPATH, 1, epath, strlen(epath));

    sqlite3 * db = nullptr;
    ASSERT_EQ(sqlite3_open(":memory:", &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db, 0, 0, nullptr), 0);

    char output[MAXPATH] = {};
    ASSERT_EQ(sqlite3_exec(db, "SELECT epath()", str_output, output, NULL), SQLITE_OK);

    EXPECT_STREQ(output, epath);

    sqlite3_close(db);
}

TEST(addqueryfuncs, uidtouser) {
    // user caller's uid
    const uid_t uid = getuid();

    struct passwd pwd;
    struct passwd * result = NULL;

    char buf[MAXPATH] = {};
    ASSERT_EQ(getpwuid_r(uid, &pwd, buf, MAXPATH, &result), 0);
    ASSERT_EQ(result, &pwd);

    sqlite3 * db = nullptr;
    ASSERT_EQ(sqlite3_open(":memory:", &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db, 0, 0, nullptr), 0);

    // use value obtained from summary table
    char query[MAXSQL] = {};
    SNPRINTF(query, MAXSQL, "SELECT uidtouser('%d', 0)", uid);

    char output[MAXPATH] = {};
    ASSERT_EQ(sqlite3_exec(db, query, str_output, output, NULL), SQLITE_OK);

    EXPECT_STREQ(output, pwd.pw_name);

    sqlite3_close(db);
}

TEST(addqueryfuncs, gidtogroup) {
    // user caller's gid
    const gid_t gid = getgid();

    struct group grp;
    struct group * result = NULL;

    char buf[MAXPATH] = {};
    ASSERT_EQ(getgrgid_r(gid, &grp, buf, MAXPATH, &result), 0);
    ASSERT_EQ(result, &grp);

    sqlite3 * db = nullptr;
    ASSERT_EQ(sqlite3_open(":memory:", &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db, 0, 0, nullptr), 0);

    // use value obtained from summary table
    char query[MAXSQL] = {};
    SNPRINTF(query, MAXSQL, "SELECT gidtogroup('%d', 0)", gid);

    char output[MAXPATH] = {};
    ASSERT_EQ(sqlite3_exec(db, query, str_output, output, NULL), SQLITE_OK);

    EXPECT_STREQ(output, grp.gr_name);

    sqlite3_close(db);
}

TEST(addqueryfuncs, modetotxt) {
    sqlite3 * db = nullptr;
    ASSERT_EQ(sqlite3_open(":memory:", &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db, 0, 0, nullptr), 0);

    for(mode_t perm = 0; perm < 01000; perm++) {
        char query[MAXSQL] = {};
        char output[11] = {};
        char expected[11] = {};

        // file
        SNPRINTF(query, MAXSQL, "SELECT modetotxt(%zu)", perm);
        ASSERT_EQ(sqlite3_exec(db, query, str_output, output, NULL), SQLITE_OK);
        EXPECT_STREQ(output, modetostr(expected, 11, perm));

        // directory
        SNPRINTF(query, MAXSQL, "SELECT modetotxt(%zu)", perm | S_IFDIR);
        ASSERT_EQ(sqlite3_exec(db, query, str_output, output, NULL), SQLITE_OK);
        EXPECT_STREQ(output, modetostr(expected, 11, perm | S_IFDIR));
    }

    sqlite3_close(db);
}

TEST(addqueryfuncs, strftime) {
    sqlite3 * db = nullptr;
    ASSERT_EQ(sqlite3_open(":memory:", &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db, 0, 0, nullptr), 0);

    const char fmt[] = "%a %A %b %B %c %C %d %D %e %F %g %G %h %H %I %j %m %M %n %p %r %R %S %t %T %u %U %V %w %W %x %X %y %Y %z %Z %%";

    const time_t now = time(nullptr);

    char query[MAXSQL] = {};
    SNPRINTF(query, MAXSQL, "SELECT strftime('%s', %d)", fmt, now);

    char output[MAXPATH] = {};
    ASSERT_EQ(sqlite3_exec(db, query, str_output, output, NULL), SQLITE_OK);

    char expected[MAXSQL] = {};
    struct tm tm;
    ASSERT_NE(localtime_r(&now, &tm), nullptr);
    EXPECT_NE(strftime(expected, MAXSQL, fmt, &tm), (size_t) 0);

    EXPECT_STREQ(output, expected);

    sqlite3_close(db);
}

static const char SIZE[] = {'K',    // 10
                            'M',    // 20
                            'G',    // 30
                            'T',    // 40
                            'P',    // 50
                            'E',    // 60
                            // 'Z', // 70
                            // 'Y'  // 80
};

TEST(addqueryfuncs, blocksize) {
    sqlite3 * db = nullptr;
    ASSERT_EQ(sqlite3_open(":memory:", &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db, 0, 0, nullptr), 0);

    const char expecteds[] = {'1', '1', '2'};

    size_t Bsize = 1;
    size_t iBsize = 1;
    for(size_t i = 0; i < sizeof(SIZE); i++) {
        Bsize  *= 1000;
        const size_t Binputs[] = {
            Bsize - 1,
            Bsize,
            Bsize + 1
        };

        iBsize *= 1024;
        const size_t iBinputs[] = {
            iBsize - 1,
            iBsize,
            iBsize + 1
        };

        for(size_t j = 0; j < 3; j++) {
            char query[MAXSQL] = {};
            char output[MAXPATH] = {};
            char expected[MAXPATH] = {};

            SNPRINTF(query, MAXSQL, "SELECT blocksize(%zu, '%c', 0)", iBinputs[j], SIZE[i]);
            ASSERT_EQ(sqlite3_exec(db, query, str_output, output, NULL), SQLITE_OK);
            SNPRINTF(expected, MAXPATH, "%c%c", expecteds[j], SIZE[i]);
            EXPECT_STREQ(output, expected);

            SNPRINTF(query, MAXSQL, "SELECT blocksize(%zu, '%cB', 0)", Binputs[j], SIZE[i]);
            ASSERT_EQ(sqlite3_exec(db, query, str_output, output, NULL), SQLITE_OK);
            SNPRINTF(expected, MAXPATH, "%c%cB", expecteds[j], SIZE[i]);
            EXPECT_STREQ(output, expected);

            SNPRINTF(query, MAXSQL, "SELECT blocksize(%zu, '%ciB', 0)", iBinputs[j], SIZE[i]);
            ASSERT_EQ(sqlite3_exec(db, query, str_output, output, NULL), SQLITE_OK);
            SNPRINTF(expected, MAXPATH, "%c%ciB", expecteds[j], SIZE[i]);
            EXPECT_STREQ(output, expected);
        }
    }

    sqlite3_close(db);
}

TEST(addqueryfuncs, human_readable_size) {
    sqlite3 * db = nullptr;
    ASSERT_EQ(sqlite3_open(":memory:", &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db, 0, 0, nullptr), 0);

    char query[MAXSQL] = {};
    char output[MAXPATH] = {};

    size_t size = 1;

    SNPRINTF(query, MAXSQL, "SELECT human_readable_size(%zu, 0)", size);
    ASSERT_EQ(sqlite3_exec(db, query, str_output, output, NULL), SQLITE_OK);
    EXPECT_STREQ(output, "1.0");

    // greater than 1K - has unit suffix
    for(size_t i = 0; i < sizeof(SIZE); i++) {
        char expected[MAXPATH] = {};

        size *= 1024;

        SNPRINTF(query, MAXSQL, "SELECT human_readable_size(%zu, 0)", size + (size / 10));
        ASSERT_EQ(sqlite3_exec(db, query, str_output, output, NULL), SQLITE_OK);
        SNPRINTF(expected, MAXPATH, "1.1%c", SIZE[i]);
        EXPECT_STREQ(output, expected);

    }

    sqlite3_close(db);
}

TEST(addqueryfuncs, level) {
    sqlite3 * db = nullptr;
    ASSERT_EQ(sqlite3_open(":memory:", &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    for(size_t level = 0; level < 10; level++) {
        ASSERT_EQ(addqueryfuncs(db, 0, level, nullptr), 0);

        char output[MAXPATH] = {};
        ASSERT_EQ(sqlite3_exec(db, "SELECT level()", str_output, output, NULL), SQLITE_OK);

        char expected[MAXPATH] = {};
        SNPRINTF(expected, MAXPATH, "%zu", level);

        EXPECT_STREQ(output, expected);
    }

    sqlite3_close(db);
}
