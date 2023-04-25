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



#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <pthread.h>
#include <random>

#include <gtest/gtest.h>

extern "C" {

#include "gufi_query/PoolArgs.h"
#include "print.h"

}

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER; // not const
static const char DBNAME_FORMAT[] = "file:memory%zu?mode=memory&cache=shared" GUFI_SQLITE_VFS_URI;
static const size_t OB_SIZE = 4096;
static const std::string TABLE_NAME = "test_table";

void setup_input(struct input *in, OutputMethod om, bool aggregate) {
    static const std::string I = "CREATE TABLE " + TABLE_NAME + " (str TEXT, val INTEGER);";

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution <int> dist(1, 8);

    memset(in, 0, sizeof(*in));
    in->maxthreads = dist(gen);
    in->sql.init = I.c_str(); in->sql.init_len = strlen(in->sql.init);
    in->sql.init_agg_len = aggregate;
    in->output = om;
    in->output_buffer_size = OB_SIZE;
}

void test_common(PoolArgs *pa) {
    EXPECT_NE(pa->in, nullptr);
    EXPECT_NE(pa->skip, nullptr);
    ASSERT_NE(pa->ta, nullptr);

    for(size_t i = 0; i < (size_t) pa->in->maxthreads; i++) {
        ThreadArgs_t *ta = &pa->ta[i];
        ASSERT_NE(ta, nullptr);

        EXPECT_NE(ta->outdb, nullptr);

        struct OutputBuffer *ob = &ta->output_buffer;
        EXPECT_NE(ob->buf, nullptr);
        EXPECT_EQ(ob->capacity, OB_SIZE);
        EXPECT_EQ(ob->filled, (size_t) 0);
        EXPECT_EQ(ob->count, (size_t) 0);

        char file_buf[1024] = {0};
        FILE *file = fmemopen(file_buf, sizeof(file_buf), "w+b");
        ASSERT_NE(file, nullptr);

        PrintArgs print;
        print.output_buffer = ob;
        print.delim = ' ';
        print.mutex = nullptr;
        print.outfile = file;
        print.rows = 0;

        // read from the database being processed
        // no need for WHERE - there should only be 1 table
        EXPECT_EQ(sqlite3_exec(ta->outdb, "SELECT name FROM sqlite_master;",
                               print_parallel, &print, nullptr), SQLITE_OK);
        EXPECT_EQ(OutputBuffer_flush(ob, file), TABLE_NAME.size() + 2);
        EXPECT_EQ(fflush(file), 0);
        EXPECT_EQ(fclose(file), 0);

        EXPECT_EQ(print.rows, (size_t) 1);
        EXPECT_EQ(strlen(file_buf), TABLE_NAME.size() + 2);
        EXPECT_EQ(memcmp(file_buf, TABLE_NAME.c_str(), TABLE_NAME.size()), 0);
    }
}

void poolargs_stdout(bool aggregate) {
    struct input in;
    setup_input(&in, STDOUT, aggregate);

    PoolArgs pa;
    ASSERT_EQ(PoolArgs_init(&pa, &in, &mutex), 0);

    test_common(&pa);

    EXPECT_EQ(pa.stdout_mutex, &mutex);
    for(size_t i = 0; i < (size_t) in.maxthreads; i++) {
        ThreadArgs_t *ta = &pa.ta[i];

        // the per-thread database files are in memory
        char dbname[MAXPATH];
        const size_t dbname_len = snprintf(dbname, sizeof(dbname), DBNAME_FORMAT, i);
        EXPECT_EQ(strlen(ta->dbname), dbname_len);
        EXPECT_EQ(memcmp(ta->dbname, dbname, dbname_len), 0);

        EXPECT_EQ(ta->outfile, stdout);
    }

    EXPECT_NO_THROW(PoolArgs_fin(&pa, in.maxthreads));
}

TEST(PoolArgs, STDOUT) {
    poolargs_stdout(false);
}

TEST(PoolArgs, STDOUT_aggregate) {
    poolargs_stdout(true);
}

TEST(PoolArgs, OUTFILE) {
    struct input in;
    setup_input(&in, OUTFILE, false);

    char outname[MAXPATH];
    in.outname = outname;
    in.outname_len = snprintf(outname, sizeof(outname), "/tmp/XXXXXX");
    const int fd = mkstemp(outname);
    EXPECT_NE(fd, -1);
    EXPECT_EQ(close(fd), 0);
    EXPECT_EQ(remove(in.outname), 0);

    PoolArgs pa;
    ASSERT_EQ(PoolArgs_init(&pa, &in, &mutex), 0);

    test_common(&pa);

    EXPECT_EQ(pa.stdout_mutex, nullptr);
    for(size_t i = 0; i < (size_t) in.maxthreads; i++) {
        ThreadArgs_t *ta = &pa.ta[i];

        // the per-thread database files are in memory
        char dbname[MAXPATH];
        const size_t dbname_len = snprintf(dbname, sizeof(dbname), DBNAME_FORMAT, i);
        EXPECT_EQ(strlen(ta->dbname), dbname_len);
        EXPECT_EQ(memcmp(ta->dbname, dbname, dbname_len), 0);

        // stdout has been replaced
        EXPECT_NE(ta->outfile, stdout);
    }

    EXPECT_NO_THROW(PoolArgs_fin(&pa, in.maxthreads));

    // delete the files here since the filenames are not available in the previous loop
    for(size_t i = 0; i < (size_t) in.maxthreads; i++) {
        char filename[MAXPATH];
        snprintf(filename, sizeof(filename), "%s.%zu", in.outname, i);
        EXPECT_EQ(remove(filename), 0);
    }
}

TEST(PoolArgs, OUTFILE_aggregate) {
    struct input in;
    setup_input(&in, OUTFILE, true);

    char outname[MAXPATH];
    in.outname = outname;
    in.outname_len = snprintf(outname, sizeof(outname), "/tmp/XXXXXX");
    const int fd = mkstemp(outname);
    EXPECT_NE(fd, -1);
    EXPECT_EQ(close(fd), 0);
    EXPECT_EQ(remove(in.outname), 0);

    PoolArgs pa;
    ASSERT_EQ(PoolArgs_init(&pa, &in, &mutex), 0);

    test_common(&pa);

    EXPECT_EQ(pa.stdout_mutex, nullptr);
    for(size_t i = 0; i < (size_t) in.maxthreads; i++) {
        ThreadArgs_t *ta = &pa.ta[i];

        // the per-thread database files are in memory
        char dbname[MAXPATH];
        const size_t dbname_len = snprintf(dbname, sizeof(dbname), DBNAME_FORMAT, i);
        EXPECT_EQ(strlen(ta->dbname), dbname_len);
        EXPECT_EQ(memcmp(ta->dbname, dbname, dbname_len), 0);

        // stdout has not been replaced
        EXPECT_EQ(ta->outfile, stdout);
    }

    EXPECT_NO_THROW(PoolArgs_fin(&pa, in.maxthreads));

    // per-thread files are not created
    for(size_t i = 0; i < (size_t) in.maxthreads; i++) {
        char filename[MAXPATH];
        snprintf(filename, sizeof(filename), "%s.%zu", in.outname, i);
        EXPECT_EQ(remove(filename), -1);
    }

    // final file is created by aggregate, so don't test here
}

TEST(PoolArgs, OUTDB) {
    struct input in;
    setup_input(&in, OUTDB, false);

    char outname[MAXPATH];
    in.outname = outname;
    in.outname_len = snprintf(outname, sizeof(outname), "/tmp/XXXXXX");
    const int fd = mkstemp(outname);
    EXPECT_NE(fd, -1);
    EXPECT_EQ(close(fd), 0);
    EXPECT_EQ(remove(in.outname), 0);

    PoolArgs pa;
    ASSERT_EQ(PoolArgs_init(&pa, &in, &mutex), 0);

    test_common(&pa);

    EXPECT_EQ(pa.stdout_mutex, nullptr);
    for(size_t i = 0; i < (size_t) in.maxthreads; i++) {
        ThreadArgs_t *ta = &pa.ta[i];

        // the per-thread database files are in the filesystem
        char dbname[MAXPATH];
        const size_t dbname_len = snprintf(dbname, sizeof(dbname), "%s.%zu", in.outname, i);
        EXPECT_EQ(strlen(ta->dbname), dbname_len);
        EXPECT_EQ(memcmp(ta->dbname, dbname, dbname_len), 0);

        // delete the file now since the name is available
        EXPECT_EQ(remove(ta->dbname), 0);

        // outfile is set to the default and not used
        EXPECT_EQ(ta->outfile, stdout);
    }

    EXPECT_NO_THROW(PoolArgs_fin(&pa, in.maxthreads));
}

TEST(PoolArgs, OUTDB_aggregate) {
    struct input in;
    setup_input(&in, OUTDB, true);

    char outname[MAXPATH];
    in.outname = outname;
    in.outname_len = snprintf(outname, sizeof(outname), "/tmp/XXXXXX");
    const int fd = mkstemp(outname);
    EXPECT_NE(fd, -1);
    EXPECT_EQ(close(fd), 0);
    EXPECT_EQ(remove(in.outname), 0);

    PoolArgs pa;
    ASSERT_EQ(PoolArgs_init(&pa, &in, &mutex), 0);

    test_common(&pa);

    EXPECT_EQ(pa.stdout_mutex, nullptr);
    for(size_t i = 0; i < (size_t) in.maxthreads; i++) {
        ThreadArgs_t *ta = &pa.ta[i];

        // the per-thread database files are in memory
        char dbname[MAXPATH];
        const size_t dbname_len = snprintf(dbname, sizeof(dbname), DBNAME_FORMAT, i);
        EXPECT_EQ(strlen(ta->dbname), dbname_len);
        EXPECT_EQ(memcmp(ta->dbname, dbname, dbname_len), 0);

        // outfile is set to the default and not used
        EXPECT_EQ(ta->outfile, stdout);
    }

    EXPECT_NO_THROW(PoolArgs_fin(&pa, in.maxthreads));

    // final database file is created by aggregate, so don't test here
}
