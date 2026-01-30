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



#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "bf.h"
#include "dbutils.h"
#include "histogram.h"

static void setup_db(sqlite3 **db) {
    ASSERT_EQ(sqlite3_open(SQLITE_MEMORY, db), SQLITE_OK);
    ASSERT_NE(*db, nullptr);
    ASSERT_EQ(addhistfuncs(*db), 1);
}

static const char INSERT_ERR[] = "Error: Could not set test value: %s\n";

// macro so that error messages print useful line numbers
#define insert(db, col, val)                                            \
    do {                                                                \
        char sql[MAXSQL];                                               \
        snprintf(sql, sizeof(sql),                                      \
                 "INSERT INTO test (%s) VALUES (%s);",                  \
                 col, val.c_str());                                     \
        char *err = nullptr;                                            \
        const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);   \
        if (rc != SQLITE_OK) {                                          \
            sqlite_print_err_and_free(err, stderr,                      \
                                      (char *) INSERT_ERR,              \
                                      err);                             \
        }                                                               \
        ASSERT_EQ(rc, SQLITE_OK);                                       \
    } while (0)

#ifdef __cplusplus
extern "C" {
#endif
int serialize_bucket(sqlite3_context *context,
                     char **buf, char **curr,
                     std::size_t *size,
                     ssize_t (*serialize)(char *curr, const std::size_t avail,
                                          void *key, void *data),
                     void *key, void *data);
#ifdef __cplusplus
}
#endif

static void test_hist_step(sqlite3_context *context, int, sqlite3_value **argv) {
    int *r = (int *) sqlite3_aggregate_context(context, sizeof(*r));
    *r = sqlite3_value_int(argv[0]);
}

static ssize_t serialize_test_bucket(char *, const std::size_t, void *key, void *) {
    int *r = (int *) sqlite3_aggregate_context((sqlite3_context *) key, sizeof(*r));
    return 1 + *r;
}

static void test_hist_final(sqlite3_context *context) {
    std::size_t size = 2;
    char *serialized = (char *) malloc(size);
    char *curr = serialized;

    serialize_bucket(context, &serialized, &curr, &size,
                     serialize_test_bucket, context, nullptr);

    free(serialized);

    sqlite3_result_null(context);
}

TEST(histogram, serialize_bucket) {
    sqlite3 *db = nullptr;
    setup_db(&db);
    ASSERT_EQ(sqlite3_create_function(db,               "test_hist",         1,  SQLITE_UTF8,
                                      nullptr, nullptr,  test_hist_step,     test_hist_final), SQLITE_OK);

    /* write */
    EXPECT_EQ(sqlite3_exec(db, "SELECT test_hist(0);",
                           nullptr, nullptr, nullptr), SQLITE_OK);

    /* write with realloc */
    EXPECT_EQ(sqlite3_exec(db, "SELECT test_hist(1);",
                           nullptr, nullptr, nullptr), SQLITE_OK);

    sqlite3_close(db);
}

static void test_log2_hist(const std::vector <std::string> &types,
                           const std::vector <std::string> &values) {
    for(std::string const &type : types) {
        sqlite3 *db = nullptr;
        setup_db(&db);

        char create[MAXSQL];
        snprintf(create, sizeof(create), "CREATE TABLE test (value %s);", type.c_str());
        ASSERT_EQ(sqlite3_exec(db, create, nullptr, nullptr, nullptr), SQLITE_OK);

        for(std::string const &value : values) {
            insert(db, "value", value);
        }

        char *hist_str = {nullptr};
        ASSERT_EQ(sqlite3_exec(db, "SELECT log_hist(value, 2, 3) FROM test;",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        ASSERT_NE(hist_str, nullptr);

        log_hist *hist = log_hist_parse(hist_str);
        ASSERT_NE(hist, nullptr);
        ASSERT_NE(hist->buckets, nullptr);

        EXPECT_EQ(hist->count,      (std::size_t) 3);
        EXPECT_EQ(hist->lt,         (std::size_t) 2); // NULL, 0
        EXPECT_EQ(hist->buckets[0], (std::size_t) 1); // 1
        EXPECT_EQ(hist->buckets[1], (std::size_t) 2); // 2, 3
        EXPECT_EQ(hist->buckets[2], (std::size_t) 1); // 4
        EXPECT_EQ(hist->ge,         (std::size_t) 1); // 8

        log_hist_free(hist);
        free(hist_str);
        sqlite3_close(db);
    }
}

TEST(histogram, log) {
    // nothing in buckets
    {
        sqlite3 *db = nullptr;
        setup_db(&db);

        char create[MAXSQL];
        snprintf(create, sizeof(create), "CREATE TABLE test (value INT);");
        ASSERT_EQ(sqlite3_exec(db, create, nullptr, nullptr, nullptr), SQLITE_OK);

        char *hist_str = nullptr;
        ASSERT_EQ(sqlite3_exec(db, "SELECT log_hist(value, 2, 3) FROM test;",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        ASSERT_NE(hist_str, nullptr);

        log_hist *hist = log_hist_parse(hist_str);
        ASSERT_NE(hist, nullptr);
        EXPECT_EQ(hist->buckets, nullptr);

        EXPECT_EQ(hist->count,  (std::size_t) 0);
        EXPECT_EQ(hist->lt,     (std::size_t) 0);
        EXPECT_EQ(hist->ge,     (std::size_t) 0);

        log_hist_free(hist);
        free(hist_str);
        sqlite3_close(db);
    }

    // normal usage
    test_log2_hist({"INT",  "DOUBLE"}, {"0",   "1",   "2",    "3",     "4",      "8",          "NULL"});
    test_log2_hist({"TEXT", "BLOB"},   {"''",  "'1'", "'22'", "'333'", "'4444'", "'88888888'", "NULL"});

    // combine histograms
    {
        sqlite3 *db = nullptr;
        setup_db(&db);

        ASSERT_EQ(sqlite3_exec(db, "CREATE TABLE hist (str TEXT, keep INT);",
                               nullptr, nullptr, nullptr), SQLITE_OK);
        ASSERT_EQ(sqlite3_exec(db, "INSERT INTO hist SELECT log_hist(str, 2, 2), 1 FROM hist;",
                               nullptr, nullptr, nullptr), SQLITE_OK);
        ASSERT_EQ(sqlite3_exec(db, "INSERT INTO hist SELECT log_hist(1, 2, 2), 1;",
                               nullptr, nullptr, nullptr), SQLITE_OK);
        ASSERT_EQ(sqlite3_exec(db, "INSERT INTO hist SELECT log_hist(2, 2, 2), 1;",
                               nullptr, nullptr, nullptr), SQLITE_OK);

        char *hist_str = nullptr;
        EXPECT_EQ(sqlite3_exec(db, "SELECT log_hist_combine(str) FROM hist;",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        ASSERT_NE(hist_str, nullptr);

        log_hist *hist = log_hist_parse(hist_str);
        ASSERT_NE(hist, nullptr);
        ASSERT_NE(hist->buckets, nullptr);

        EXPECT_EQ(hist->count,      (std::size_t) 2);
        EXPECT_EQ(hist->lt,         (std::size_t) 0);
        EXPECT_EQ(hist->ge,         (std::size_t) 0);
        EXPECT_EQ(hist->buckets[0], (std::size_t) 1);
        EXPECT_EQ(hist->buckets[1], (std::size_t) 1);

        log_hist_free(hist);
        free(hist_str);
        hist_str = nullptr;

        // cannot combine histograms with different bases
        ASSERT_EQ(sqlite3_exec(db, "INSERT INTO hist SELECT log_hist(2, 4, 2), 0;",
                               nullptr, nullptr, nullptr), SQLITE_OK);
        EXPECT_NE(sqlite3_exec(db, "SELECT log_hist_combine(str) FROM hist;",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        EXPECT_EQ(hist_str, nullptr);

        ASSERT_EQ(sqlite3_exec(db, "DELETE FROM hist WHERE keep == 0;",
                               nullptr, nullptr, nullptr), SQLITE_OK);

        // cannot combine histograms with different bucket counts
        ASSERT_EQ(sqlite3_exec(db, "INSERT INTO hist SELECT log_hist(2, 2, 4), 0;",
                               nullptr, nullptr, nullptr), SQLITE_OK);
        EXPECT_NE(sqlite3_exec(db, "SELECT log_hist_combine(str) FROM hist;",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        EXPECT_EQ(hist_str, nullptr);

        // NULL input
        EXPECT_NE(sqlite3_exec(db, "SELECT log_hist_combine(NULL);",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        EXPECT_EQ(hist_str, nullptr);

        // bad histogram string input
        EXPECT_NE(sqlite3_exec(db, "SELECT log_hist_combine('a');",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        EXPECT_EQ(hist_str, nullptr);

        sqlite3_close(db);
    }

    // bad base
    {
        sqlite3 *db = nullptr;
        setup_db(&db);

        ASSERT_EQ(sqlite3_exec(db, "CREATE TABLE test (value INT);", nullptr, nullptr, nullptr), SQLITE_OK);
        ASSERT_EQ(sqlite3_exec(db, "INSERT INTO  test  VALUES (0);", nullptr, nullptr, nullptr), SQLITE_OK);

        EXPECT_NE(sqlite3_exec(db, "SELECT log_hist(value, 1, 3) FROM test;",
                               nullptr, nullptr, nullptr), SQLITE_OK);

        sqlite3_close(db);
    }

    // bad strings
    EXPECT_EQ(log_hist_parse("'"),          nullptr);
    EXPECT_EQ(log_hist_parse("0"),          nullptr);
    EXPECT_EQ(log_hist_parse("0;"),         nullptr);
    EXPECT_EQ(log_hist_parse("0;0"),        nullptr);
    EXPECT_EQ(log_hist_parse("0;0;"),       nullptr);
    EXPECT_EQ(log_hist_parse("0;0;0"),      nullptr);
    EXPECT_EQ(log_hist_parse("0;0;0;0"),    nullptr);
    EXPECT_EQ(log_hist_parse("0;0;0;0:"),   nullptr);
    EXPECT_EQ(log_hist_parse("0;0;0;0:0"),  nullptr);
}

TEST(histogram, mode) {
    // C++20 is not required, so not using Designated Initializers
    std::size_t buckets[01000];
    memset(buckets, 0, sizeof(buckets));

    sqlite3 *db = nullptr;
    setup_db(&db);
    ASSERT_EQ(sqlite3_exec(db, "CREATE TABLE test (mode INT);",
                           nullptr, nullptr, nullptr), SQLITE_OK);

    // nothing in buckets
    {
        char *hist_str = nullptr;
        ASSERT_EQ(sqlite3_exec(db, "SELECT mode_hist(mode) FROM test;",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);

        mode_hist *hist = mode_hist_parse(hist_str);
        ASSERT_NE(hist, nullptr);

        EXPECT_EQ(memcmp(hist->buckets, buckets, sizeof(buckets)), 0);

        mode_hist_free(hist);
        free(hist_str);
    }

    buckets[0600] = 1;
    buckets[0644] = 1;
    buckets[0700] = 1;
    buckets[0755] = 2;

    for(size_t mode = 0; mode < 01000; mode++) {
        if (buckets[mode]) {
            for(size_t i = 0; i < buckets[mode]; i++) {
                std::stringstream s; s << mode;
                insert(db, "mode", s.str());
            }
        }
    }

    // normal usage
    {
        char *hist_str = nullptr;
        ASSERT_EQ(sqlite3_exec(db, "SELECT mode_hist(mode) FROM test;",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);

        mode_hist *hist = mode_hist_parse(hist_str);
        ASSERT_NE(hist, nullptr);

        EXPECT_EQ(memcmp(hist->buckets, buckets, sizeof(buckets)), 0);

        mode_hist_free(hist);
        free(hist_str);
    }

    // combine histograms
    {
        ASSERT_EQ(sqlite3_exec(db, "CREATE TABLE hist (str TEXT);",
                               nullptr, nullptr, nullptr), SQLITE_OK);
        ASSERT_EQ(sqlite3_exec(db, "INSERT INTO hist SELECT mode_hist(str) FROM hist;",
                               nullptr, nullptr, nullptr), SQLITE_OK);
        ASSERT_EQ(sqlite3_exec(db, "INSERT INTO hist SELECT mode_hist(1);",
                               nullptr, nullptr, nullptr), SQLITE_OK);
        ASSERT_EQ(sqlite3_exec(db, "INSERT INTO hist SELECT mode_hist(2);",
                               nullptr, nullptr, nullptr), SQLITE_OK);

        char *hist_str = nullptr;
        EXPECT_EQ(sqlite3_exec(db, "SELECT mode_hist_combine(str) FROM hist;",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        ASSERT_NE(hist_str, nullptr);

        mode_hist *hist = mode_hist_parse(hist_str);
        ASSERT_NE(hist, nullptr);

        EXPECT_EQ(hist->buckets[1], (std::size_t) 1);
        EXPECT_EQ(hist->buckets[2], (std::size_t) 1);

        mode_hist_free(hist);
        free(hist_str);
        hist_str = nullptr;

        // NULL input
        EXPECT_NE(sqlite3_exec(db, "SELECT mode_hist_combine(NULL);",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        EXPECT_EQ(hist_str, nullptr);

        // bad histogram string input
        EXPECT_NE(sqlite3_exec(db, "SELECT mode_hist_combine('a');",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        EXPECT_EQ(hist_str, nullptr);
    }

    sqlite3_close(db);

    // bad strings
    EXPECT_EQ(mode_hist_parse("777:"),   nullptr);
    EXPECT_EQ(mode_hist_parse("777:0"),  nullptr);
    EXPECT_EQ(mode_hist_parse("1000:1"), nullptr);
}

TEST(histogram, time) {
    const char reftime[] = "31536000";
    const std::string timestamps[] = {
        "0",         // 1 year ago
        "31532400",  // 1 hour ago
        "31535940",  // 1 minute ago
        "31535998",  // 2 seconds ago
        "31535999",  // 1 second ago
        "31536000",  // 0 seconds ago
        "31536001",  // future time
    };

    char select[MAXSQL];
    snprintf(select, sizeof(select), "SELECT time_hist(timestamp, %s) FROM test;", reftime);

    sqlite3 *db = nullptr;
    setup_db(&db);
    ASSERT_EQ(sqlite3_exec(db, "CREATE TABLE test (timestamp INT);",
                           nullptr, nullptr, nullptr), SQLITE_OK);

    // nothing in buckets
    {
        char *hist_str = nullptr;
        ASSERT_EQ(sqlite3_exec(db, select, copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        ASSERT_NE(hist_str, nullptr);

        time_hist *hist = time_hist_parse(hist_str);
        ASSERT_NE(hist, nullptr);

        EXPECT_EQ(hist->ref,        (std::time_t) 0);
        EXPECT_EQ(hist->buckets[0], (std::size_t) 0); // future
        EXPECT_EQ(hist->buckets[1], (std::size_t) 0); // < second
        EXPECT_EQ(hist->buckets[2], (std::size_t) 0); // < minute
        EXPECT_EQ(hist->buckets[3], (std::size_t) 0); // < hour
        EXPECT_EQ(hist->buckets[4], (std::size_t) 0); // < day
        EXPECT_EQ(hist->buckets[5], (std::size_t) 0); // < week
        EXPECT_EQ(hist->buckets[6], (std::size_t) 0); // < 4 weeks
        EXPECT_EQ(hist->buckets[7], (std::size_t) 0); // < year
        EXPECT_EQ(hist->buckets[8], (std::size_t) 0); // >= year

        time_hist_free(hist);
        free(hist_str);
    }

    for(std::string const &timestamp : timestamps) {
        insert(db, "timestamp", timestamp);
    }

    // normal usage
    {
        char *hist_str = nullptr;
        ASSERT_EQ(sqlite3_exec(db, select, copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        ASSERT_NE(hist_str, nullptr);

        time_hist *hist = time_hist_parse(hist_str);
        ASSERT_NE(hist, nullptr);

        EXPECT_EQ(hist->ref,        (std::time_t) 31536000);
        EXPECT_EQ(hist->buckets[0], (std::size_t) 1); // future
        EXPECT_EQ(hist->buckets[1], (std::size_t) 1); // < second
        EXPECT_EQ(hist->buckets[2], (std::size_t) 2); // < minute
        EXPECT_EQ(hist->buckets[3], (std::size_t) 1); // < hour
        EXPECT_EQ(hist->buckets[4], (std::size_t) 1); // < day
        EXPECT_EQ(hist->buckets[5], (std::size_t) 0); // < week
        EXPECT_EQ(hist->buckets[6], (std::size_t) 0); // < 4 weeks
        EXPECT_EQ(hist->buckets[7], (std::size_t) 0); // < year
        EXPECT_EQ(hist->buckets[8], (std::size_t) 1); // >= year

        time_hist_free(hist);
        free(hist_str);
    }

    // combine histograms
    {
        char sql[MAXSQL];

        ASSERT_EQ(sqlite3_exec(db, "CREATE TABLE hist (str TEXT);",
                               nullptr, nullptr, nullptr), SQLITE_OK);

        SNPRINTF(sql, sizeof(sql), "INSERT INTO hist SELECT time_hist(%s, %s);", timestamps[4].c_str(), reftime);
        ASSERT_EQ(sqlite3_exec(db, sql, nullptr, nullptr, nullptr), SQLITE_OK);
        SNPRINTF(sql, sizeof(sql), "INSERT INTO hist SELECT time_hist(%s, %s);", timestamps[1].c_str(), reftime);
        ASSERT_EQ(sqlite3_exec(db, sql, nullptr, nullptr, nullptr), SQLITE_OK);

        char *hist_str = nullptr;
        EXPECT_EQ(sqlite3_exec(db, "SELECT time_hist_combine(str) FROM hist;",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        ASSERT_NE(hist_str, nullptr);

        time_hist *hist = time_hist_parse(hist_str);
        ASSERT_NE(hist, nullptr);

        EXPECT_EQ(hist->ref,        (std::time_t) 31536000);
        EXPECT_EQ(hist->buckets[0], (std::size_t) 0); // future
        EXPECT_EQ(hist->buckets[1], (std::size_t) 0); // < second
        EXPECT_EQ(hist->buckets[2], (std::size_t) 1); // < minute
        EXPECT_EQ(hist->buckets[3], (std::size_t) 0); // < hour
        EXPECT_EQ(hist->buckets[4], (std::size_t) 1); // < day
        EXPECT_EQ(hist->buckets[5], (std::size_t) 0); // < week
        EXPECT_EQ(hist->buckets[6], (std::size_t) 0); // < 4 weeks
        EXPECT_EQ(hist->buckets[7], (std::size_t) 0); // < year
        EXPECT_EQ(hist->buckets[8], (std::size_t) 0); // >= year

        time_hist_free(hist);
        free(hist_str);
        hist_str = nullptr;

        // different reference time
        EXPECT_EQ(sqlite3_exec(db, "INSERT INTO hist SELECT time_hist(3600, 1);",
                               nullptr, nullptr, nullptr), SQLITE_OK);
        EXPECT_NE(sqlite3_exec(db, "SELECT time_hist_combine(str) FROM hist;",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        EXPECT_EQ(hist_str, nullptr);

        // NULL input
        EXPECT_NE(sqlite3_exec(db, "SELECT time_hist_combine(NULL);",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        EXPECT_EQ(hist_str, nullptr);

        // bad histogram string input
        EXPECT_NE(sqlite3_exec(db, "SELECT time_hist_combine('a');",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        EXPECT_EQ(hist_str, nullptr);
    }

    sqlite3_close(db);

    // bad strings
    EXPECT_EQ(time_hist_parse("'"),        nullptr);
    EXPECT_EQ(time_hist_parse("';"),       nullptr);
    EXPECT_EQ(time_hist_parse("0"),        nullptr);
    EXPECT_EQ(time_hist_parse("0;5"),      nullptr);
    EXPECT_EQ(time_hist_parse("0;5:"),     nullptr);
    EXPECT_EQ(time_hist_parse("0;5:0"),    nullptr);
    EXPECT_EQ(time_hist_parse("0;5:0;"),   nullptr);
    EXPECT_EQ(time_hist_parse("0;1:0;0"),  nullptr);
}

static const std::string CATEGORIES[] = {
    "'str'",
    "'3x'",
    "'string'",
    "NULL",

    "'3x'",
    "'string'",
    "'3x'",
    "'str'",
    "'abcd'",
    "'abcd'",
    "'once'",
};

static void check_combined(category_hist_t *hist) {
    ASSERT_NE(hist, nullptr);
    ASSERT_EQ(hist->count, (std::size_t) 5);

    EXPECT_STREQ(hist->buckets[0].name, "3x");
    EXPECT_EQ(hist->buckets[0].count, (std::size_t) 3 * 2);

    EXPECT_STREQ(hist->buckets[1].name, "abcd");
    EXPECT_EQ(hist->buckets[1].count, (std::size_t) 2 * 2);

    EXPECT_STREQ(hist->buckets[2].name, "str");
    EXPECT_EQ(hist->buckets[2].count, (std::size_t) 2 * 2);

    EXPECT_STREQ(hist->buckets[3].name, "string");
    EXPECT_EQ(hist->buckets[3].count, (std::size_t) 2 * 2);

    EXPECT_STREQ(hist->buckets[4].name, "once");
    EXPECT_EQ(hist->buckets[4].count, (std::size_t) 1);
}

TEST(histogram, category) {
    sqlite3 *db = nullptr;
    setup_db(&db);
    ASSERT_EQ(sqlite3_exec(db, "CREATE TABLE test (category TEXT);",
                           nullptr, nullptr, nullptr), SQLITE_OK);

    // no buckets
    {
        char *hist_str = nullptr;
        ASSERT_EQ(sqlite3_exec(db, "SELECT category_hist(category, 0) FROM test;",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        ASSERT_NE(hist_str, nullptr);

        category_hist_t *hist = category_hist_parse(hist_str);
        ASSERT_NE(hist, nullptr);
        ASSERT_NE(hist->buckets, nullptr);

        EXPECT_EQ(hist->count, (std::size_t) 0);

        category_hist_free(hist);
        free(hist_str);
    }

    for(std::string const &category : CATEGORIES) {
        insert(db, "category", category);
    }

    {
        char *hist_str = nullptr;
        ASSERT_EQ(sqlite3_exec(db, "SELECT category_hist(category, 1) FROM test;",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        ASSERT_NE(hist_str, nullptr);

        // normal usage
        {
            category_hist_t *hist = category_hist_parse(hist_str);
            ASSERT_NE(hist, nullptr);
            ASSERT_NE(hist->buckets, nullptr);

            EXPECT_EQ(hist->count, (std::size_t) 5);

            EXPECT_STREQ(hist->buckets[0].name, "3x");
            EXPECT_EQ(hist->buckets[0].count, (std::size_t) 3);

            EXPECT_STREQ(hist->buckets[1].name, "abcd");
            EXPECT_EQ(hist->buckets[1].count, (std::size_t) 2);

            EXPECT_STREQ(hist->buckets[2].name, "str");
            EXPECT_EQ(hist->buckets[2].count, (std::size_t) 2);

            EXPECT_STREQ(hist->buckets[3].name, "string");
            EXPECT_EQ(hist->buckets[3].count, (std::size_t) 2);

            EXPECT_STREQ(hist->buckets[4].name, "once");
            EXPECT_EQ(hist->buckets[4].count, (std::size_t) 1);

            category_hist_free(hist);
        }

        free(hist_str);
        hist_str = nullptr;

        ASSERT_EQ(sqlite3_exec(db, "SELECT category_hist(category, 0) FROM test;",
                               copy_columns_callback, &hist_str, nullptr), SQLITE_OK);
        ASSERT_NE(hist_str, nullptr);

        // normal usage
        {
            category_hist_t *hist = category_hist_parse(hist_str);
            ASSERT_NE(hist, nullptr);
            ASSERT_NE(hist->buckets, nullptr);

            EXPECT_EQ(hist->count, (std::size_t) 4);

            EXPECT_STREQ(hist->buckets[0].name, "3x");
            EXPECT_EQ(hist->buckets[0].count, (std::size_t) 3);

            EXPECT_STREQ(hist->buckets[1].name, "abcd");
            EXPECT_EQ(hist->buckets[1].count, (std::size_t) 2);

            EXPECT_STREQ(hist->buckets[2].name, "str");
            EXPECT_EQ(hist->buckets[2].count, (std::size_t) 2);

            EXPECT_STREQ(hist->buckets[3].name, "string");
            EXPECT_EQ(hist->buckets[3].count, (std::size_t) 2);

            // category "once" does not show up

            category_hist_free(hist);
        }

        {
            hist_str[0] = '3'; // incorrect count

            category_hist_t *hist = category_hist_parse(hist_str);
            ASSERT_NE(hist, nullptr);
            ASSERT_NE(hist->buckets, nullptr);

            EXPECT_EQ(hist->count, (std::size_t) 3);

            EXPECT_STREQ(hist->buckets[0].name, "3x");
            EXPECT_EQ(hist->buckets[0].count, (std::size_t) 3);

            // category "abcd" does not show up due to ordering of categories

            EXPECT_STREQ(hist->buckets[1].name, "str");
            EXPECT_EQ(hist->buckets[1].count, (std::size_t) 2);

            EXPECT_STREQ(hist->buckets[2].name, "string");
            EXPECT_EQ(hist->buckets[2].count, (std::size_t) 2);

            category_hist_free(hist);
        }

        free(hist_str);
    }

    // combine histograms in C
    {
        char *with = nullptr;
        ASSERT_EQ(sqlite3_exec(db, "SELECT category_hist(category, 1) FROM test;",
                               copy_columns_callback, &with, nullptr), SQLITE_OK);
        ASSERT_NE(with, nullptr);

        char *without = nullptr;
        ASSERT_EQ(sqlite3_exec(db, "SELECT category_hist(category, 0) FROM test;",
                               copy_columns_callback, &without, nullptr), SQLITE_OK);
        ASSERT_NE(without, nullptr);

        category_hist_t *w_hist = category_hist_parse(with);
        ASSERT_NE(w_hist, nullptr);
        ASSERT_EQ(w_hist->count, (std::size_t) 5);

        category_hist_t *wo_hist = category_hist_parse(without);
        ASSERT_NE(wo_hist, nullptr);
        ASSERT_EQ(wo_hist->count, (std::size_t) 4);

        category_hist_t *c_sum = category_hist_combine(w_hist, wo_hist);
        check_combined(c_sum);

        category_hist_free(c_sum);
        category_hist_free(wo_hist);
        category_hist_free(w_hist);

        free(without);
        free(with);
    }

    // combine histograms in SQL
    {
        ASSERT_EQ(sqlite3_exec(db, "CREATE TABLE hists(str TEXT);",
                               nullptr, nullptr, nullptr), SQLITE_OK);
        ASSERT_EQ(sqlite3_exec(db, "INSERT INTO hists SELECT category_hist(category, 1) FROM test;",
                               nullptr, nullptr, nullptr), SQLITE_OK);
        ASSERT_EQ(sqlite3_exec(db, "INSERT INTO hists SELECT category_hist(category, 0) FROM test;",
                               nullptr, nullptr, nullptr), SQLITE_OK);

        char *combined_str = nullptr;
        ASSERT_EQ(sqlite3_exec(db, "SELECT category_hist_combine(str) FROM hists;",
                               copy_columns_callback, &combined_str, nullptr), SQLITE_OK);

        category_hist_t *sql_sum = category_hist_parse(combined_str);
        check_combined(sql_sum);

        category_hist_free(sql_sum);

        free(combined_str);
        combined_str = nullptr;

        // empty histogram
        EXPECT_EQ(sqlite3_exec(db, "SELECT category_hist_combine(NULL);",
                               copy_columns_callback, &combined_str, nullptr), SQLITE_ERROR);
        EXPECT_EQ(combined_str, nullptr);

        // bad histogram string input
        EXPECT_EQ(sqlite3_exec(db, "SELECT category_hist_combine('a');",
                               copy_columns_callback, &combined_str, nullptr), SQLITE_ERROR);
        EXPECT_EQ(combined_str, nullptr);
    }

    sqlite3_close(db);

    // bad strings
    EXPECT_EQ(category_hist_parse("'"),          nullptr);
    EXPECT_EQ(category_hist_parse("1"),          nullptr);
    EXPECT_EQ(category_hist_parse("1;"),         nullptr);
    EXPECT_EQ(category_hist_parse("1;'"),        nullptr);
    EXPECT_EQ(category_hist_parse("1;3"),        nullptr);
    EXPECT_EQ(category_hist_parse("1;3:"),       nullptr);
    EXPECT_EQ(category_hist_parse("1;3:cat"),    nullptr);
    EXPECT_EQ(category_hist_parse("1;3:cat:"),   nullptr);
    EXPECT_EQ(category_hist_parse("1;3:cat:0"),  nullptr);
}

TEST(histogram, mode_count) {
    sqlite3 *db = nullptr;
    setup_db(&db);
    ASSERT_EQ(sqlite3_exec(db, "CREATE TABLE test (category TEXT);",
                           nullptr, nullptr, nullptr), SQLITE_OK);

    // no mode
    {
        char *mc_str = nullptr;
        ASSERT_EQ(sqlite3_exec(db, "SELECT mode_count(category) FROM test;",
                               copy_columns_callback, &mc_str, nullptr), SQLITE_OK);
        ASSERT_EQ(mc_str, nullptr);
    }

    // multiple values, but no mode
    {
        for(std::size_t i = 0; i < 4; i++) {
            insert(db, "category", CATEGORIES[i]);
        }

        char *mc_str = nullptr;
        ASSERT_EQ(sqlite3_exec(db, "SELECT mode_count(category) FROM test;",
                               copy_columns_callback, &mc_str, nullptr), SQLITE_OK);
        ASSERT_EQ(mc_str, nullptr);

        mode_count *mc = mode_count_parse(mc_str);
        EXPECT_EQ(mc, nullptr);

        free(mc_str);

        ASSERT_EQ(sqlite3_exec(db, "DELETE FROM test;",
                               nullptr, nullptr, nullptr), SQLITE_OK);
    }

    // one mode
    {
        for(std::string const &category : CATEGORIES) {
            insert(db, "category", category);
        }

        char *mc_str = nullptr;
        ASSERT_EQ(sqlite3_exec(db, "SELECT mode_count(category) FROM test;",
                               copy_columns_callback, &mc_str, nullptr), SQLITE_OK);
        ASSERT_NE(mc_str, nullptr);

        mode_count *mc = mode_count_parse(mc_str);
        ASSERT_NE(mc, nullptr);
        ASSERT_NE(mc->mode, nullptr);

        EXPECT_STREQ(mc->mode, "3x");
        EXPECT_EQ(mc->len,   (std::size_t) 2);
        EXPECT_EQ(mc->count, (std::size_t) 3);

        mode_count_free(mc);
        free(mc_str);
    }

    sqlite3_close(db);

    // bad strings
    EXPECT_EQ(mode_count_parse(""),         nullptr);
    EXPECT_EQ(mode_count_parse("'"),        nullptr);
    EXPECT_EQ(mode_count_parse("3"),        nullptr);
    EXPECT_EQ(mode_count_parse("3:"),       nullptr);
    EXPECT_EQ(mode_count_parse("3:cat"),    nullptr);
    EXPECT_EQ(mode_count_parse("3:cat:"),   nullptr);
    EXPECT_EQ(mode_count_parse("3:cat:0"),  nullptr);
}
