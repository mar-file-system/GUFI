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



#include <stdlib.h>
#include <string>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "dbutils.h"
#include "template_db.h"

#define TABLE_NAME "test_table"
#define TABLE_CREATE "CREATE TABLE " TABLE_NAME "(A INT, B TEXT);"

static const char template_name[] = "should-not-see-this-in-filesystem";

static int sql_callback(void *args, int, char **data, char **) {
    std::string **cols = (std::string **) args;
    **cols = data[1];
    (*cols)++;
    return 0;
}

static int create_test_tables(const char *name, sqlite3 *db, void *args) {
    (void) args;

    if (create_table_wrapper(name, db, TABLE_NAME, TABLE_CREATE) != SQLITE_OK) {
        return -1;
    }

    return 0;
}

TEST(template_db, create_copy) {
    struct template_db tdb;
    ASSERT_EQ(init_template_db(&tdb), 0);

    ASSERT_EQ(create_template(&tdb, create_test_tables, template_name), 0);
    EXPECT_GT(tdb.fd, -1);
    EXPECT_GT(tdb.size, 0);

    // the template file should disappear even
    // if it existed before this test
    struct stat st;
    EXPECT_EQ(lstat(template_name, &st), -1);

    char dst_name[] = "XXXXXX";
    int dst = mkstemp(dst_name);
    ASSERT_NE(dst, -1);
    EXPECT_EQ(close(dst), 0);

    sqlite3 *db = template_to_db(&tdb, dst_name, -1, -1);
    EXPECT_NE(db, nullptr);

    // make sure the columns are correct
    std::string cols[2];
    std::string *ptr = cols;
    EXPECT_EQ(sqlite3_exec(db, "PRAGMA TABLE_INFO(" TABLE_NAME ");",
                           sql_callback, &ptr, nullptr),
              SQLITE_OK);
    EXPECT_EQ(cols[0], "A");
    EXPECT_EQ(cols[1], "B");

    closedb(db);
    EXPECT_EQ(remove(dst_name), 0);
    ASSERT_EQ(close_template_db(&tdb), 0);
}

TEST(template_db, bad_inputs) {
    struct template_db tdb;
    ASSERT_EQ(init_template_db(&tdb), 0);

    EXPECT_EQ(create_template(nullptr, create_test_tables, template_name), -1);
    EXPECT_EQ(create_template(&tdb,    nullptr,            template_name), -1);
    EXPECT_EQ(create_template(&tdb,    create_test_tables, nullptr), -1);
    EXPECT_EQ(create_template(&tdb,    create_test_tables, ""), -1);

    tdb.fd = 0;
    EXPECT_EQ(create_template(&tdb,    create_test_tables, template_name), -1);
    tdb.fd = -1;

    EXPECT_EQ(copy_template(&tdb,      template_name, -1, -1), -1);

    EXPECT_EQ(template_to_db(&tdb,     template_name, -1, -1), nullptr);

    ASSERT_EQ(close_template_db(&tdb), 0);
}

TEST(create_temporary_template, not_enough_fds) {
    struct rlimit orig_fds;
    ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &orig_fds), 0);

    struct rlimit fewer_fds = orig_fds;
    fewer_fds.rlim_cur = 3;
    ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &fewer_fds), 0);

    // mkstemp should fail
    struct template_db tdb;
    EXPECT_EQ(create_dbdb_template(&tdb), -1);
    EXPECT_EQ(create_xattrs_template(&tdb), -1);

    ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &orig_fds), 0);
}

TEST(copy_template, not_enough_fds) {
    // need name of an a non-existent file
    char name[] = "XXXXXX";
    const int fd = mkstemp(name);
    ASSERT_GT(fd, 0);
    ASSERT_EQ(close(fd), 0);

    struct rlimit orig_fds;
    ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &orig_fds), 0);

    struct rlimit fewer_fds = orig_fds;
    fewer_fds.rlim_cur = 3;
    ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &fewer_fds), 0);

    // open should fail
    struct template_db tdb;
    EXPECT_EQ(copy_template(&tdb, name, -1, -1), -1);

    ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &orig_fds), 0);

    ASSERT_EQ(remove(name), 0);
}

TEST(copy_template, bad_size) {
    // need name of an a non-existent file
    char name[] = "XXXXXX";
    const int fd = mkstemp(name);
    ASSERT_GT(fd, 0);

    struct template_db tdb;
    tdb.fd = fd;
    tdb.size = 1024; // bad size
    EXPECT_EQ(copy_template(&tdb, name, -1, -1), -1);

    ASSERT_EQ(close(fd), 0);

    ASSERT_EQ(remove(name), 0);
}

TEST(create_empty_dbdb, good) {
    struct template_db tdb;
    ASSERT_EQ(init_template_db(&tdb), 0);
    ASSERT_EQ(create_template(&tdb, create_test_tables, template_name), 0);

    // create new directory
    char dirname[] = "XXXXXX";
    ASSERT_NE(mkdtemp(dirname), nullptr);

    str_t dst = REFSTR(dirname, strlen(dirname));

    // <dirname>/db.db
    char dbname[MAXPATH];
    SNFORMAT_S(dbname, sizeof(dbname), 3,
               dst.data, dst.len,
               "/", (std::size_t) 1,
               DBNAME, DBNAME_LEN);

    // create file under dirname not called db.db
    char filename[MAXPATH];
    SNFORMAT_S(filename, sizeof(filename), 2,
               dst.data, dst.len,
               "/XXXXXX", (std::size_t) 7);
    const int fd = mkstemp(filename);
    ASSERT_GT(fd, -1);
    EXPECT_EQ(close(fd), 0);

    // symlink to file is ok
    ASSERT_EQ(symlink(filename + dst.len + 1, dbname), 0);
    EXPECT_EQ(create_empty_dbdb(&tdb, &dst, -1, -1), 0);
    EXPECT_EQ(unlink(dbname), 0);
    EXPECT_EQ(unlink(filename), 0);

    // symlink to <dirname>
    char symlinktarget[MAXPATH];
    SNFORMAT_S(symlinktarget, sizeof(symlinktarget), 2,
               dst.data, dst.len,
               "/..", (std::size_t) 3);

    // symink to directory causes error
    ASSERT_EQ(symlink(symlinktarget + dst.len + 1, dbname), 0);
    EXPECT_EQ(create_empty_dbdb(&tdb, &dst, -1, -1), -1);
    EXPECT_EQ(unlink(dbname), 0);

    // try to create db with a bad file descriptor
    const int tfd = tdb.fd;
    tdb.fd = -1;
    EXPECT_EQ(create_empty_dbdb(&tdb, &dst, -1, -1), -1);
    tdb.fd = tfd;

    // create new db
    EXPECT_EQ(create_empty_dbdb(&tdb, &dst, -1, -1), 0);

    // db already exists, so nothing happens
    EXPECT_EQ(create_empty_dbdb(&tdb, &dst, -1, -1), 0);

    EXPECT_EQ(remove(dbname), 0);
    EXPECT_EQ(remove(dirname), 0);

    ASSERT_EQ(close_template_db(&tdb), 0);
}

TEST(create_empty_dbdb, path_is_file) {
    char filename[] = "XXXXXX";
    const int fd = mkstemp(filename);
    ASSERT_GT(fd, -1);
    EXPECT_EQ(close(fd), 0);

    str_t dst = REFSTR(filename, strlen(filename));

    // attempt to create file under file
    EXPECT_EQ(create_empty_dbdb(nullptr, &dst, -1, -1), -1);
    EXPECT_EQ(remove(filename), 0);
}
