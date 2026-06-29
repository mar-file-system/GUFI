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



#include <cmath>
#include <ctime>
#include <fstream>
#include <grp.h>
#include <pwd.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "addqueryfuncs.h"
#include "bf.h"
#include "dbutils.h"

static void test_str(const std::vector<std::pair <const char *, const char *> > &testcases,
                     void (*other_tests)(sqlite3 *db)) {
    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);

    for(std::pair <const char *, const char *> const &testcase : testcases) {
        char *output = nullptr;
        EXPECT_EQ(sqlite3_exec(db, testcase.first,
                               copy_columns_callback, &output, nullptr), SQLITE_OK);
        EXPECT_STREQ(output, testcase.second);
        free(output);
    }

    if (other_tests) {
        other_tests(db);
    }

    sqlite3_close(db);
}

TEST(addqueryfuncs, uidtouser) {
    // user's uid
    const uid_t uid = getuid();

    // no need for reentrant call
    struct passwd *pwd = getpwuid(uid);
    ASSERT_NE(pwd, nullptr);

    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);

    char query[MAXSQL] = {};
    char *output = nullptr;

    /* convert good uid */
    SNPRINTF(query, sizeof(query), "SELECT uidtouser(%" STAT_uid ");", uid);
    EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);
    EXPECT_STREQ(output, pwd->pw_name);
    free(output);
    output = nullptr;

    /* convert bad uid */
    SNPRINTF(query, sizeof(query), "SELECT uidtouser('abcd');");
    EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);
    EXPECT_EQ(output, nullptr);

    /* convert NULL */
    SNPRINTF(query, sizeof(query), "SELECT uidtouser(NULL);");
    EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);
    EXPECT_EQ(output, nullptr);

    /* convert large number */
    SNPRINTF(query, sizeof(query), "SELECT uidtouser(98765432100123456789);");
    EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_ERROR);

    sqlite3_close(db);
}

TEST(addqueryfuncs, gidtogroup) {
    // user's gid
    const gid_t gid = getgid();

    // no need for reentrant call
    struct group *grp = getgrgid(gid);
    ASSERT_NE(grp, nullptr);

    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);

    char query[MAXSQL] = {};
    char *output = nullptr;

    /* convert good gid */
    SNPRINTF(query, sizeof(query), "SELECT gidtogroup(%" STAT_gid ");", gid);
    EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);
    EXPECT_STREQ(output, grp->gr_name);
    free(output);
    output = nullptr;

    /* convert bad gid */
    SNPRINTF(query, sizeof(query), "SELECT gidtogroup('abcd');");
    EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);
    EXPECT_EQ(output, nullptr);

    /* convert NULL */
    SNPRINTF(query, sizeof(query), "SELECT gidtogroup(NULL);");
    EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);
    EXPECT_EQ(output, nullptr);

    /* convert large number */
    SNPRINTF(query, sizeof(query), "SELECT gidtogroup(98765432100123456789);");
    EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_ERROR);

    sqlite3_close(db);
}

TEST(addqueryfuncs, modetotxt) {
    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);

    for(mode_t perm = 0; perm < 01000; perm++) {
        char query[MAXSQL] = {};
        char *output = nullptr;
        char expected[11] = {};

        // file
        SNPRINTF(query, sizeof(query), "SELECT modetotxt(%zu);", (size_t) perm);
        EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);
        EXPECT_STREQ(output, modetostr(expected, 11, perm));
        free(output);
        output = nullptr;

        // directory
        SNPRINTF(query, sizeof(query), "SELECT modetotxt(%zu);", perm | S_IFDIR);
        EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);
        EXPECT_STREQ(output, modetostr(expected, 11, perm | S_IFDIR));
        free(output);
        output = nullptr;
    }

    sqlite3_close(db);
}

TEST(addqueryfuncs, strftime) {
    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);

    const char fmt[] = "%a %A %b %B %c %C %d %D %e %F %g %G %h %H %I %j %m %M %n %p %r %R %S %t %T %u %U %V %w %W %x %X %y %Y %z %Z %%";

    const time_t now = time(nullptr);

    char query[MAXSQL] = {};
    SNPRINTF(query, sizeof(query), "SELECT strftime('%s', %d);", fmt, (int) now);

    char *output = nullptr;
    EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);

    char expected[MAXSQL] = {};
    struct tm tm;
    ASSERT_NE(localtime_r(&now, &tm), nullptr);
    EXPECT_NE(strftime(expected, MAXSQL, fmt, &tm), (size_t) 0);

    EXPECT_STREQ(output, expected);
    free(output);
    output = nullptr;

    sqlite3_close(db);
}

TEST(addqueryfuncs, intcat) {
    const std::vector<std::pair <const char *, const char *> > tests = {
        std::make_pair("SELECT intcat(0);",                "Zero"),
        std::make_pair("SELECT intcat(pow(1000, 0) + 1);", "O"),
        std::make_pair("SELECT intcat(pow(1000, 1) + 1);", "K"),
        std::make_pair("SELECT intcat(pow(1000, 2) + 1);", "M"),
        std::make_pair("SELECT intcat(pow(1000, 3) + 1);", "B"),
        std::make_pair("SELECT intcat(pow(1000, 4) + 1);", "T"),
        std::make_pair("SELECT intcat(pow(1000, 5) + 1);", "q"),
        std::make_pair("SELECT intcat(pow(1000, 6) + 1);", "Q"),
    };

    test_str(tests,
             [](sqlite3 *db) -> void {
                 EXPECT_NE(sqlite3_exec(db, "SELECT intcat('abc');",
                                        copy_columns_callback, nullptr, nullptr), SQLITE_OK);

                 EXPECT_NE(sqlite3_exec(db, "SELECT intcat(-1);",
                                        copy_columns_callback, nullptr, nullptr), SQLITE_OK);
             });
}

TEST(addqueryfuncs, bytecat) {
    /* use integer powers of 1024 to not deal with numerical instability due to +1 */
    const std::vector <std::pair <const char *, const char *> > tests = {
        std::make_pair("SELECT bytecat(0);",                       "Zero"),
        std::make_pair("SELECT bytecat(1);",                       "B"),
        std::make_pair("SELECT bytecat(1024 + 1);",                "K"),
        std::make_pair("SELECT bytecat(1048576 + 1);",             "M"),
        std::make_pair("SELECT bytecat(1073741824 + 1);",          "G"),
        std::make_pair("SELECT bytecat(1099511627776 + 1);",       "T"),
        std::make_pair("SELECT bytecat(1125899906842624 + 1);",    "P"),
        std::make_pair("SELECT bytecat(1152921504606846976 + 1);", "E"),
    };

    test_str(tests,
             [](sqlite3 *db) -> void {
                 EXPECT_NE(sqlite3_exec(db, "SELECT bytecat('abc');",
                                        copy_columns_callback, nullptr, nullptr), SQLITE_OK);

                 EXPECT_NE(sqlite3_exec(db, "SELECT bytecat(-1);",
                                        copy_columns_callback, nullptr, nullptr), SQLITE_OK);
             });
}

TEST(addqueryfuncs, agecat) {
    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);

    char *output = nullptr;

    EXPECT_NE(sqlite3_exec(db, "SELECT agecat('abc');",
                           copy_columns_callback, &output, nullptr), SQLITE_OK);

    char sql[MAXSQL];

    snprintf(sql, sizeof(sql), "SELECT agecat(%ld);", time(nullptr) + 1);
    EXPECT_NE(sqlite3_exec(db, sql,
                           copy_columns_callback, &output, nullptr), SQLITE_OK);

    const std::pair <int, const char *> tests[] = {
        std::make_pair(+ 0,                           "Zero"),
        std::make_pair(- 1,                           "Secs"),
        std::make_pair(- 60 - 1,                      "Mins"),
        std::make_pair(- 60 * 60 - 1,                 "Hrs"),
        std::make_pair(- 60 * 60 * 24 - 1,            "Days"),
        std::make_pair(- 60 * 60 * 24 * 7 - 1,        "Wks"),
        std::make_pair(- 60 * 60  * 24 * 30 - 1,      "Mos"),
        std::make_pair(- 60 * 60 * 24 * 365 - 1,      "Yrs"),
        std::make_pair(- 60 * 60 * 24 * 365 * 10 - 1, "Decs"),
    };

    for(std::pair <int, const char *> const &test : tests) {
        snprintf(sql, sizeof(sql), "SELECT agecat(%ld);", time(nullptr) + test.first);
        EXPECT_EQ(sqlite3_exec(db, sql,
                               copy_columns_callback, &output, nullptr), SQLITE_OK);
        EXPECT_STREQ(output, test.second);

        free(output);
        output = nullptr;
    }

    sqlite3_close(db);
}

TEST(addqueryfuncs, epochtoage) {
    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);

    char *output = nullptr;

    EXPECT_NE(sqlite3_exec(db, "SELECT epochtoage('abc', 's');",
                           copy_columns_callback, &output, nullptr), SQLITE_OK);

    char sql[MAXSQL];

    snprintf(sql, sizeof(sql), "SELECT epochtoage(%ld, 's');", time(nullptr) + 1);
    EXPECT_NE(sqlite3_exec(db, sql,
                           copy_columns_callback, &output, nullptr), SQLITE_OK);
    free(output);
    output = nullptr;

    for(const char unit : {'s', 'm', 'h', 'd', 'w', 'M', 'y', 'D', '-'}) {
        /* test 0 */
        snprintf(sql, sizeof(sql), "SELECT epochtoage(%ld, '%c');", time(nullptr), unit);
        EXPECT_EQ(sqlite3_exec(db, sql,
                               copy_columns_callback, &output, nullptr), SQLITE_OK);
        EXPECT_STREQ(output, "0");

        free(output);
        output = nullptr;
    }

    sqlite3_close(db);
}

TEST(addqueryfuncs, hostname) {
    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);

    char *output = nullptr;

    EXPECT_EQ(sqlite3_exec(db, "SELECT hostname();",
                           copy_columns_callback, &output, nullptr), SQLITE_OK);
    EXPECT_STRNE(output, "");
    free(output);
    output = nullptr;

    sqlite3_close(db);
}

// uint64_t goes up to E
static const char SIZE[] = {'K',    // 10
                            'M',    // 20
                            'G',    // 30
                            'T',    // 40
                            'P',    // 50
                            'E',    // 60
};

TEST(addqueryfuncs, blocksize) {
    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);

    const char expecteds[] = {'1', '1', '2'};

    std::size_t Bsize = 1;
    std::size_t iBsize = 1;
    for(size_t i = 0; i < sizeof(SIZE); i++) {
        Bsize  *= 1000;
        const std::size_t Binputs[] = {
            Bsize - 1,
            Bsize,
            Bsize + 1
        };

        iBsize *= 1024;
        const std::size_t iBinputs[] = {
            iBsize - 1,
            iBsize,
            iBsize + 1
        };

        for(size_t j = 0; j < 3; j++) {
            char query[MAXSQL] = {};
            char *output = nullptr;
            char expected[MAXPATH] = {};

            SNPRINTF(query, sizeof(query), "SELECT blocksize(%zu, '%c');", iBinputs[j], SIZE[i]);
            EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);
            SNPRINTF(expected, sizeof(expected), "%c%c", expecteds[j], SIZE[i]);
            EXPECT_STREQ(output, expected);
            free(output);
            output = nullptr;

            SNPRINTF(query, sizeof(query), "SELECT blocksize(%zu, '%cB');", Binputs[j], SIZE[i]);
            EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);
            SNPRINTF(expected, sizeof(expected), "%c%cB", expecteds[j], SIZE[i]);
            EXPECT_STREQ(output, expected);
            free(output);
            output = nullptr;

            SNPRINTF(query, sizeof(query), "SELECT blocksize(%zu, '%ciB');", iBinputs[j], SIZE[i]);
            EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);
            SNPRINTF(expected, sizeof(expected), "%c%ciB", expecteds[j], SIZE[i]);
            EXPECT_STREQ(output, expected);
            free(output);
            output = nullptr;
        }
    }

    EXPECT_EQ(sqlite3_exec(db, "SELECT blocksize('', '');",      copy_columns_callback, nullptr, nullptr), SQLITE_ERROR); /* non-integer size argument */
    EXPECT_EQ(sqlite3_exec(db, "SELECT blocksize('abc', '');",   copy_columns_callback, nullptr, nullptr), SQLITE_ERROR); /* missing size argument */
    EXPECT_EQ(sqlite3_exec(db, "SELECT blocksize(0, 'i');",      copy_columns_callback, nullptr, nullptr), SQLITE_ERROR); /* single bad character */
    EXPECT_EQ(sqlite3_exec(db, "SELECT blocksize(0, 'iB');",     copy_columns_callback, nullptr, nullptr), SQLITE_ERROR); /* 1st character isn't valid */
    EXPECT_EQ(sqlite3_exec(db, "SELECT blocksize(0, 'ij');",     copy_columns_callback, nullptr, nullptr), SQLITE_ERROR); /* 2nd character isn't B */
    EXPECT_EQ(sqlite3_exec(db, "SELECT blocksize(0, 'AiB');",    copy_columns_callback, nullptr, nullptr), SQLITE_ERROR); /* 1st character isn't valid */
    EXPECT_EQ(sqlite3_exec(db, "SELECT blocksize(0, 'AjB');",    copy_columns_callback, nullptr, nullptr), SQLITE_ERROR); /* 2nd character isn't i */
    EXPECT_EQ(sqlite3_exec(db, "SELECT blocksize(0, 'Aij');",    copy_columns_callback, nullptr, nullptr), SQLITE_ERROR); /* 3nd character isn't B */
    EXPECT_EQ(sqlite3_exec(db, "SELECT blocksize(0, '0KB');",    copy_columns_callback, nullptr, nullptr), SQLITE_ERROR); /* block coefficient is 0 */
    EXPECT_EQ(sqlite3_exec(db, "SELECT blocksize(0, '1.2KiB');", copy_columns_callback, nullptr, nullptr), SQLITE_ERROR); /* decimal point is not allowed */
    EXPECT_EQ(sqlite3_exec(db, "SELECT blocksize(0, 'KiBGiB');", copy_columns_callback, nullptr, nullptr), SQLITE_ERROR); /* suffix is too long */

    sqlite3_close(db);
}

TEST(addqueryfuncs, human_readable_size) {
    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);

    char query[MAXSQL] = {};
    char *output = nullptr;

    std::size_t size = 1;

    SNPRINTF(query, sizeof(query), "SELECT human_readable_size(%zu);", size);
    EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);
    EXPECT_STREQ(output, "1.0");
    free(output);
    output = nullptr;

    // greater than 1K - has unit suffix
    for(size_t i = 0; i < sizeof(SIZE); i++) {
        char expected[MAXPATH] = {};

        size *= 1024;

        SNPRINTF(query, sizeof(query), "SELECT human_readable_size(%zu);", size + (size / 10));
        EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);
        SNPRINTF(expected, sizeof(expected), "1.1%c", SIZE[i]);
        EXPECT_STREQ(output, expected);
        free(output);
        output = nullptr;
    }

    EXPECT_EQ(sqlite3_exec(db, "SELECT human_readable_size('');",    copy_columns_callback, nullptr, nullptr), SQLITE_ERROR); /* empty input */
    EXPECT_EQ(sqlite3_exec(db, "SELECT human_readable_size('abc');", copy_columns_callback, nullptr, nullptr), SQLITE_ERROR); /* non-integer input */

    sqlite3_close(db);
}

TEST(addqueryfuncs, basename) {
    const std::vector <std::pair <const char *, const char *> > tests = {
        /* from basename(3) manpage */
        std::make_pair("SELECT basename('/usr/lib');", "lib"),
        std::make_pair("SELECT basename('/usr/');",    "usr"),
        std::make_pair("SELECT basename('usr');",      "usr"),
        std::make_pair("SELECT basename('/');",        "/"),
        std::make_pair("SELECT basename('.');",        "."),
        std::make_pair("SELECT basename('..');",       ".."),

        /* not from manpage */
        std::make_pair("SELECT basename(NULL);",       ""),
    };

    test_str(tests, nullptr);
}

TEST(addqueryfuncs, ext) {
    const std::vector <std::pair <const char *, const char *> > tests = {
        std::make_pair("SELECT ext('file.txt');",      "txt"),
        std::make_pair("SELECT ext('name.csv');",      "csv"),
        std::make_pair("SELECT ext('files.tar.gz');",  "gz"),
        std::make_pair("SELECT ext('.hidden');",       "hidden"),
        std::make_pair("SELECT ext('.');",             nullptr),
        std::make_pair("SELECT ext('no-extension');",  nullptr),
        std::make_pair("SELECT ext(NULL);",            nullptr),
    };

    test_str(tests, nullptr);
}

TEST(addqueryfuncs, rpad) {
    const std::vector <std::pair <const char *, const char *> > tests = {
        std::make_pair("SELECT rpad(1, 1);", "1"),
        std::make_pair("SELECT rpad(1, 2);", "1 "),
        std::make_pair("SELECT rpad(1, 3);", "1  "),
    };

    test_str(tests,
             [] (sqlite3 *db) {
                 // input is longer than requested padding - return entire input (don't cut)
                 char *output = NULL;
                 EXPECT_EQ(sqlite3_exec(db, "SELECT rpad('abcde', 3);", copy_columns_callback, &output, nullptr), SQLITE_OK);
                 EXPECT_STREQ(output, "abcde");
                 free(output);
             });
}

TEST(addqueryfuncs, strop) {
    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);

    char *output = nullptr;

    EXPECT_EQ(sqlite3_exec(db, "SELECT strop('echo abc def');", copy_columns_callback, &output, nullptr), SQLITE_OK);
    EXPECT_STREQ(output, "abc def");
    free(output);
    output = nullptr;

    char *err = nullptr;

    EXPECT_EQ(sqlite3_exec(db, "SELECT strop('');", copy_columns_callback, &output, &err), SQLITE_ERROR);
    EXPECT_NE(err, nullptr);
    free(output);
    output = nullptr;
    sqlite3_free(err);
    err = nullptr;

    EXPECT_EQ(sqlite3_exec(db, "SELECT strop('echo');", copy_columns_callback, &output, &err), SQLITE_ERROR);
    EXPECT_NE(err, nullptr);
    free(output);
    output = nullptr;
    sqlite3_free(err);
    err = nullptr;

    // not enough file descriptors
    {
        struct rlimit orig_fds;
        ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &orig_fds), 0);

        struct rlimit fewer_fds = orig_fds;
        fewer_fds.rlim_cur = 3;
        ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &fewer_fds), 0);

        EXPECT_EQ(sqlite3_exec(db, "SELECT strop('echo abc def');", copy_columns_callback, &output, &err), SQLITE_ERROR);
        EXPECT_NE(err, nullptr);
        free(output);
        output = nullptr;
        sqlite3_free(err);
        err = nullptr;

        ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &orig_fds), 0);
    }

    sqlite3_close(db);
}

TEST(addqueryfuncs, intop) {
    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);

    char *output = nullptr;

    EXPECT_EQ(sqlite3_exec(db, "SELECT intop('echo 123');", copy_columns_callback, &output, nullptr), SQLITE_OK);
    EXPECT_STREQ(output, "123");

    // wc is part of coreutils
    EXPECT_EQ(sqlite3_exec(db, "SELECT intop('echo 123 | wc -l');", copy_columns_callback, &output, nullptr), SQLITE_OK);
    EXPECT_STREQ(output, "1");
    free(output);
    output = nullptr;

    char *err = nullptr;

    EXPECT_EQ(sqlite3_exec(db, "SELECT intop('');", copy_columns_callback, &output, &err), SQLITE_ERROR);
    EXPECT_NE(err, nullptr);
    free(output);
    output = nullptr;
    sqlite3_free(err);
    err = nullptr;

    EXPECT_EQ(sqlite3_exec(db, "SELECT intop('echo');", copy_columns_callback, &output, &err), SQLITE_ERROR);
    EXPECT_NE(err, nullptr);
    free(output);
    output = nullptr;
    sqlite3_free(err);
    err = nullptr;

    EXPECT_EQ(sqlite3_exec(db, "SELECT intop('echo abc');", copy_columns_callback, &output, &err), SQLITE_ERROR);
    EXPECT_NE(err, nullptr);
    free(output);
    output = nullptr;
    sqlite3_free(err);
    err = nullptr;

    // not enough file descriptors
    {
        struct rlimit orig_fds;
        ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &orig_fds), 0);

        struct rlimit fewer_fds = orig_fds;
        fewer_fds.rlim_cur = 3;
        ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &fewer_fds), 0);

        EXPECT_EQ(sqlite3_exec(db, "SELECT intop('echo 123');", copy_columns_callback, &output, &err), SQLITE_ERROR);
        EXPECT_NE(err, nullptr);
        free(output);
        output = nullptr;
        sqlite3_free(err);
        err = nullptr;

        ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &orig_fds), 0);
    }

    sqlite3_close(db);
}

static int copy_blob_column_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    str_t *out = (str_t *) args;

    std::size_t len = 0;
    if (sscanf(data[0], "%zu", &len) != 1) {
        fprintf(stderr, "Error: Could not parse blob length: %s\n", data[0]);
        return SQLITE_ERROR;
    }

    str_alloc_existing(out, len);
    memcpy(out->data, data[1], len);

    return SQLITE_OK;
}

TEST(addqueryfuncs, blobop) {
    // sqlite3 does not like binary in SQL statements, so write contents to temp file first
    char temp[] = "XXXXXX";
    {
        const int fd = mkstemp(temp);
        ASSERT_GT(fd, -1);
        close(fd);
    }

    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);
    ASSERT_EQ(addqueryfuncs(db), 0);

    char cmd[1024];
    str_t output;
    memset(&output, 0, sizeof(output));
    char *err = nullptr;

    // have to insert into table to get length later without opening file twice

    // empty file
    {
        snprintf(cmd, sizeof(cmd), "CREATE TABLE data(col BLOB); INSERT INTO data SELECT blobop('cat %s');", temp);
        EXPECT_EQ(sqlite3_exec(db, cmd, nullptr, nullptr, nullptr), SQLITE_OK);

        EXPECT_EQ(sqlite3_exec(db, "SELECT length(col), col FROM data; DROP TABLE data;",
                               copy_blob_column_callback, &output, &err), SQLITE_OK);
        EXPECT_EQ(err, nullptr);
        EXPECT_EQ(output.len, (std::size_t) 0);
        ASSERT_NE(output.data, nullptr);
        str_free_existing(&output);
    }

    // short result
    {
        const char data[] = "abc def\n\x00\x01\x02 \x03\x04\x05\nghi jkl";
        const std::size_t len = sizeof(data) - 1;
        std::ofstream(temp).write(data, len);
        snprintf(cmd, sizeof(cmd), "CREATE TABLE data(col BLOB); INSERT INTO data SELECT blobop('cat %s');", temp);
        EXPECT_EQ(sqlite3_exec(db, cmd, nullptr, nullptr, nullptr), SQLITE_OK);

        EXPECT_EQ(sqlite3_exec(db, "SELECT length(col), col FROM data; DROP TABLE data;",
                               copy_blob_column_callback, &output, &err), SQLITE_OK);
        EXPECT_EQ(err, nullptr);
        EXPECT_EQ(output.len, len);
        ASSERT_NE(output.data, nullptr);
        EXPECT_EQ(memcmp(output.data, data, len), 0);
        str_free_existing(&output);
    }

    // "long" result (force a reallocation)
    {
        const std::size_t len = 256;
        const std::string A(len, 'a');
        std::ofstream(temp) << A;
        snprintf(cmd, sizeof(cmd), "CREATE TABLE data(col BLOB); INSERT INTO data SELECT blobop('cat %s');", temp);
        EXPECT_EQ(sqlite3_exec(db, cmd, NULL, NULL, NULL), SQLITE_OK);

        EXPECT_EQ(sqlite3_exec(db, "SELECT length(col), col FROM data; DROP TABLE data;",
                               copy_blob_column_callback, &output, &err), SQLITE_OK);
        EXPECT_EQ(err, nullptr);
        EXPECT_EQ(output.len, len);
        ASSERT_NE(output.data, nullptr);
        EXPECT_EQ(memcmp(output.data, A.c_str(), len), 0);
        str_free_existing(&output);
    }

    // not enough file descriptors
    {
        struct rlimit orig_fds;
        ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &orig_fds), 0);

        struct rlimit fewer_fds = orig_fds;
        fewer_fds.rlim_cur = 3;
        ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &fewer_fds), 0);

        EXPECT_EQ(sqlite3_exec(db, "SELECT blobop('echo blob');",
                               copy_columns_callback, &output, &err), SQLITE_ERROR);
        EXPECT_NE(err, nullptr);
        sqlite3_free(err);
        err = nullptr;

        ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &orig_fds), 0);
    }

    sqlite3_close(db);
    remove(temp);
}

static int double_callback(void *arg, int, char **data, char **) {
    if (!data[0]) {
        return SQLITE_ERROR;
    }
    return !(sscanf(data[0], "%lf", (double *) arg) == 1);
}

TEST(addqueryfuncs, stdevs) {
    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);
    EXPECT_EQ(sqlite3_exec(db, "CREATE TABLE t (value INT);", nullptr, nullptr, nullptr), SQLITE_OK);

    double stdev = 0;

    // no values
    {
        EXPECT_EQ(sqlite3_exec(db, "SELECT stdevs(value) FROM t", double_callback, &stdev, nullptr), SQLITE_ABORT);
    }

    // 1 value
    {
        EXPECT_EQ(sqlite3_exec(db, "INSERT INTO t (value) VALUES (1);", nullptr, nullptr, nullptr), SQLITE_OK);

        EXPECT_EQ(sqlite3_exec(db, "SELECT stdevs(value) FROM t", double_callback, &stdev, nullptr), SQLITE_ABORT);
    }

    // 5 values
    {
        EXPECT_EQ(sqlite3_exec(db, "INSERT INTO t (value) VALUES (2), (3), (4), (5);", nullptr, nullptr, nullptr), SQLITE_OK);

        EXPECT_EQ(sqlite3_exec(db, "SELECT stdevs(value) FROM t", double_callback, &stdev, nullptr), SQLITE_OK);
        EXPECT_DOUBLE_EQ(stdev * stdev * 2, (double) 5); /* sqrt(5 / 2) */
    }

    sqlite3_close(db);
}

TEST(addqueryfuncs, stdevp) {
    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);
    EXPECT_EQ(sqlite3_exec(db, "CREATE TABLE t (value INT);", nullptr, nullptr, nullptr), SQLITE_OK);

    double stdev = 0;

    // no values
    {
        EXPECT_EQ(sqlite3_exec(db, "SELECT stdevp(value) FROM t", double_callback, &stdev, nullptr), SQLITE_ABORT);
    }

    // 1 value
    {
        EXPECT_EQ(sqlite3_exec(db, "INSERT INTO t (value) VALUES (1);", nullptr, nullptr, nullptr), SQLITE_OK);

        EXPECT_EQ(sqlite3_exec(db, "SELECT stdevp(value) FROM t", double_callback, &stdev, nullptr), SQLITE_ABORT);
    }

    // 5 values
    {
        EXPECT_EQ(sqlite3_exec(db, "INSERT INTO t (value) VALUES (2), (3), (4), (5);", nullptr, nullptr, nullptr), SQLITE_OK);

        EXPECT_EQ(sqlite3_exec(db, "SELECT stdevp(value) FROM t", double_callback, &stdev, nullptr), SQLITE_OK);
        EXPECT_FLOAT_EQ(stdev * stdev, (double) 2); /* sqrt(2); need to use float instead of double due to slightly too large differences */
    }

    sqlite3_close(db);
}

TEST(addqueryfuncs, stdev_from_parts) {
    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);

    const int values[] = {0, 1, 2, 3, 4};
    const std::size_t count = sizeof(values) / sizeof(values[0]);

    int sum_sq = 0;
    int sum = 0;
    for(std::size_t i = 0; i < count; i++) {
        sum_sq += values[i] * values[i];
        sum    += values[i];
    }

    const double mean = sum / count; // 2, no precision loss

    double num = 0;                  // numerator
    for(std::size_t i = 0; i < count; i++) {
        const int diff = values[i] - mean;
        num += diff * diff;
    }

    char sql[4096];
    for(int sample = 0; sample < 2; sample++) {
        const double expected = sqrt(num / (count - sample));

        SNPRINTF(sql, sizeof(sql), "SELECT stdev_from_parts(%d, %d, %zu, %d);", sum_sq, sum, count, sample);

        double computed = 0;
        EXPECT_EQ(sqlite3_exec(db, sql, double_callback, &computed, nullptr), SQLITE_OK);
        EXPECT_FLOAT_EQ(computed, expected); // too many digits of precision for EXPECT_DOUBLE_EQ
    }

    sqlite3_close(db);
}

TEST(addqueryfuncs, median) {
    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(addqueryfuncs(db), 0);
    EXPECT_EQ(sqlite3_exec(db, "CREATE TABLE t (value INT);", nullptr, nullptr, nullptr), SQLITE_OK);

    double median = 0;

    // no values
    {
        EXPECT_EQ(sqlite3_exec(db, "SELECT median(value) FROM t", double_callback, &median, nullptr), SQLITE_ABORT);
    }

    // 1 value short circuit
    {
        EXPECT_EQ(sqlite3_exec(db, "INSERT INTO t (value) VALUES (1);", nullptr, nullptr, nullptr), SQLITE_OK);

        EXPECT_EQ(sqlite3_exec(db, "SELECT median(value) FROM t", double_callback, &median, nullptr), SQLITE_OK);
        EXPECT_DOUBLE_EQ(median, (double) 1);
    }

    // 2 values short circuit
    {
        EXPECT_EQ(sqlite3_exec(db, "INSERT INTO t (value) VALUES (2);", nullptr, nullptr, nullptr), SQLITE_OK);

        EXPECT_EQ(sqlite3_exec(db, "SELECT median(value) FROM t", double_callback, &median, nullptr), SQLITE_OK);
        EXPECT_DOUBLE_EQ(median, (double) 1.5);
    }

    // odd number of values
    {
        EXPECT_EQ(sqlite3_exec(db, "INSERT INTO t (value) VALUES (3), (4), (5);", nullptr, nullptr, nullptr), SQLITE_OK);

        EXPECT_EQ(sqlite3_exec(db, "SELECT median(value) FROM t", double_callback, &median, nullptr), SQLITE_OK);
        EXPECT_DOUBLE_EQ(median, (double) 3);
    }

    // even number of values
    {
        EXPECT_EQ(sqlite3_exec(db, "INSERT INTO t (value) VALUES (6);", nullptr, nullptr, nullptr), SQLITE_OK);

        EXPECT_EQ(sqlite3_exec(db, "SELECT median(value) FROM t", double_callback, &median, nullptr), SQLITE_OK);
        EXPECT_DOUBLE_EQ(median, (double) 3.5);
    }

    sqlite3_close(db);
}

TEST(addqueryfuncs_with_context, path) {
    const char dirname[] = "dirname";

    struct work *work = new_work_with_name("index_root", 10, dirname, strlen(dirname));
    work->root_parent = REFSTR("", 0);
    work->basename_len = strlen(dirname);

    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    aqfctx_t ctx;
    ctx.in = nullptr;
    ctx.work = work;
    ASSERT_EQ(addqueryfuncs_with_context(db, &ctx), 0);

    char *output = nullptr;

    // good orig_root
    {
        work->orig_root = REFSTR("index_root", 10);
        work->root_basename_len = work->orig_root.len;

        EXPECT_EQ(sqlite3_exec(db, "SELECT path();", copy_columns_callback, &output, nullptr), SQLITE_OK);
        EXPECT_STREQ(output, work->name);
        free(output);
        output = nullptr;
    }

    // empty orig_root
    {
        work->orig_root = REFSTR("", 0);
        work->root_basename_len = work->orig_root.len;

        EXPECT_EQ(sqlite3_exec(db, "SELECT path();", copy_columns_callback, &output, nullptr), SQLITE_OK);
        EXPECT_EQ(strncmp(output, work->name, work->basename_len), 0);
        free(output);
        output = nullptr;

        work->orig_root = REFSTR(nullptr, 1);
        work->root_basename_len = work->orig_root.len;

        EXPECT_EQ(sqlite3_exec(db, "SELECT path();", copy_columns_callback, &output, nullptr), SQLITE_OK);
        EXPECT_EQ(strncmp(output, work->name, work->basename_len), 0);
        free(output);
        output = nullptr;

        work->orig_root = REFSTR(nullptr, 0);
        work->root_basename_len = work->orig_root.len;

        EXPECT_EQ(sqlite3_exec(db, "SELECT path();", copy_columns_callback, &output, nullptr), SQLITE_OK);
        EXPECT_EQ(strncmp(output, work->name, work->basename_len), 0);
        free(output);
        output = nullptr;
    }

    sqlite3_close(db);
    free(work);
}

TEST(addqueryfuncs_with_context, epath) {
    const char dirname[] = "dirname";

    struct work *work = new_work_with_name("index_root", 10, dirname, strlen(dirname));
    work->basename_len = strlen(dirname);
    work->root_parent = REFSTR("index_root", 10);     // currently at index_root/dirname

    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    aqfctx_t ctx;
    ctx.in = nullptr;
    ctx.work = work;
    ASSERT_EQ(addqueryfuncs_with_context(db, &ctx), 0);

    char *output = nullptr;
    EXPECT_EQ(sqlite3_exec(db, "SELECT epath();", copy_columns_callback, &output, nullptr), SQLITE_OK);

    EXPECT_STREQ(output, dirname);
    free(output);

    sqlite3_close(db);
    free(work);
}

TEST(addqueryfuncs_with_context, fpath) {
    struct work *work = new_work_with_name(nullptr, 0, "/", 1);
    work->fullpath = nullptr;

    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    aqfctx_t ctx;
    ctx.in = nullptr;
    ctx.work = work;
    ASSERT_EQ(addqueryfuncs_with_context(db, &ctx), 0);

    EXPECT_EQ(sqlite3_exec(db, "SELECT fpath();", nullptr, nullptr, nullptr), SQLITE_OK);
    EXPECT_NE(work->fullpath, nullptr);
    EXPECT_GT(work->fullpath_len, (size_t) 0);

    // copy current values
    const char *fullpath = work->fullpath;
    const std::size_t fullpath_len = work->fullpath_len;

    // call again - should not update fullpath
    EXPECT_EQ(sqlite3_exec(db, "SELECT fpath();", nullptr, nullptr, nullptr), SQLITE_OK);
    EXPECT_EQ(work->fullpath, fullpath);
    EXPECT_EQ(work->fullpath_len, fullpath_len);

    sqlite3_close(db);
    free(work->fullpath);
    free(work);
}

TEST(addqueryfuncs_with_context, rpath) {
    const char dirname[] = "dirname";

    struct work *work = new_work_with_name("index_root", 10, dirname, strlen(dirname));
    work->orig_root = REFSTR("index_root", 10);
    work->root_parent = REFSTR("", 0);
    work->root_basename_len = work->orig_root.len;
    work->basename_len = strlen(dirname);

    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    aqfctx_t ctx;
    ctx.in = nullptr;
    ctx.work = work;
    ASSERT_EQ(addqueryfuncs_with_context(db, &ctx), 0);

    // directory
    for(int isrolledup : {0, 1}) {
        char query[MAXSQL] = {};
        SNPRINTF(query, sizeof(query), "SELECT rpath('%s', %d);", dirname, isrolledup);

        // the path returned by the query is the path without the index prefix
        char *output = nullptr;
        EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);

        EXPECT_STREQ(output, work->name);
        free(output);
    }

    // non-directory
    const char entry[] = "entry";
    char entry_path[MAXPATH];
    SNFORMAT_S(entry_path, sizeof(entry_path), 3,
               work->name, work->name_len,
               "/", (std::size_t) 1,
               entry, sizeof(entry) - 1);

    for(int isrolledup : {0, 1}) {
        char query[MAXSQL] = {};
        SNPRINTF(query, sizeof(query), "SELECT rpath('%s', %d, '%s');", dirname, isrolledup, entry);

        // the path returned by the query is the path without the index prefix
        char *output = nullptr;
        EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);

        EXPECT_STREQ(output, entry_path);
        free(output);
    }

    sqlite3_close(db);
    free(work);
}

TEST(addqueryfuncs_with_context, spath) {
    const char dirname[] = "dirname";
    const char source[]  = "source/prefix";

    struct input in;
    in.source_prefix = REFSTR(source, strlen(source));

    struct work *work = new_work_with_name("index_root", 10, dirname, strlen(dirname));
    work->orig_root = REFSTR("index_root", 10);
    work->root_parent = REFSTR("", 0);
    work->root_basename_len = work->orig_root.len;
    work->basename_len = strlen(dirname);

    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    aqfctx_t ctx;
    ctx.in = &in;
    ctx.work = work;
    ASSERT_EQ(addqueryfuncs_with_context(db, &ctx), 0);

    char source_path[MAXPATH];
    SNFORMAT_S(source_path, sizeof(source_path), 3,
               in.source_prefix.data, in.source_prefix.len,
               "/", (std::size_t) 1,
               dirname, sizeof(dirname) - 1);

    // directory
    for(int isrolledup : {0, 1}) {
        char query[MAXSQL] = {};
        SNPRINTF(query, sizeof(query), "SELECT spath('%s', %d);", dirname, isrolledup);

        // the path returned by the query is the path without the index prefix
        char buf[MAXPATH] = {};
        char *output = buf;
        EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);

        EXPECT_STREQ(output, source_path);
    }

    const char entry[] = "entry;";
    SNFORMAT_S(source_path, sizeof(source_path), 5,
               in.source_prefix.data, in.source_prefix.len,
               "/", (std::size_t) 1,
               dirname, sizeof(dirname) - 1,
               "/", (std::size_t) 1,
               entry, sizeof(entry) - 1);

    // non-directory
    for(int isrolledup : {0, 1}) {
        char query[MAXSQL] = {};
        SNPRINTF(query, sizeof(query), "SELECT spath('%s', %d, '%s');", dirname, isrolledup, entry);

        // the path returned by the query is the path without the index prefix
        char buf[MAXPATH] = {};
        char *output = buf;
        EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, &output, nullptr), SQLITE_OK);

        EXPECT_STREQ(output, source_path);
    }

    // missing source prefix
    char query[MAXSQL] = {};
    SNPRINTF(query, sizeof(query), "SELECT spath('%s', %d);", dirname, 0);

    char *err = nullptr;

    in.source_prefix = REFSTR(nullptr, strlen(source));
    EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, nullptr, &err), SQLITE_ERROR);
    EXPECT_NE(err, nullptr);
    sqlite3_free(err);
    err = nullptr;

    in.source_prefix = REFSTR(source, 0);
    EXPECT_EQ(sqlite3_exec(db, query, copy_columns_callback, nullptr, &err), SQLITE_ERROR);
    EXPECT_NE(err, nullptr);
    sqlite3_free(err);
    err = nullptr;

    sqlite3_close(db);
    free(work);
}

TEST(addqueryfuncs_with_context, starting_point) {
    struct work *work = new_work_with_name(nullptr, 0, "", 0);
    work->orig_root = REFSTR("/index_root", 11);

    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    aqfctx_t ctx;
    ctx.in = nullptr;
    ctx.work = work;
    ASSERT_EQ(addqueryfuncs_with_context(db, &ctx), 0);

    char *output = nullptr;
    EXPECT_EQ(sqlite3_exec(db, "SELECT starting_point();", copy_columns_callback, &output, nullptr), SQLITE_OK);

    EXPECT_STREQ(output, work->orig_root.data);
    free(output);

    sqlite3_close(db);
    free(work);
}

TEST(addqueryfuncs_with_context, level) {
    struct work *work = new_work_with_name(nullptr, 0, "", 0);

    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    aqfctx_t ctx;
    ctx.in = nullptr;
    ctx.work = work;

    for(work->level = 0; work->level < 10; work->level++) {
        ASSERT_EQ(addqueryfuncs_with_context(db, &ctx), 0);

        char *output = nullptr;
        EXPECT_EQ(sqlite3_exec(db, "SELECT level();", copy_columns_callback, &output, nullptr), SQLITE_OK);

        char expected[MAXPATH] = {};
        SNPRINTF(expected, sizeof(expected), "%zu", work->level);

        EXPECT_STREQ(output, expected);
        free(output);
    }

    sqlite3_close(db);
    free(work);
}

TEST(addqueryfuncs_with_context, pinode) {
    struct work *work = new_work_with_name(nullptr, 0, "", 0);
    work->pinode = 0x12345;

    sqlite3 *db = nullptr;
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, &db), SQLITE_OK);
    ASSERT_NE(db, nullptr);

    aqfctx_t ctx;
    ctx.in = nullptr;
    ctx.work = work;

    for(work->level = 0; work->level < 10; work->level++) {
        ASSERT_EQ(addqueryfuncs_with_context(db, &ctx), 0);

        char *output = nullptr;
        EXPECT_EQ(sqlite3_exec(db, "SELECT pinode();", copy_columns_callback, &output, nullptr), SQLITE_OK);

        char expected[MAXPATH] = {};
        SNPRINTF(expected, sizeof(expected), "%lld", work->pinode);

        EXPECT_STREQ(output, expected);
        free(output);
    }

    sqlite3_close(db);
    free(work);
}
