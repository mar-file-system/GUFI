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

#include "print.h"

static void print_parallel_mutex_actual(pthread_mutex_t *mutex) {
    const std::string A   = "A";
    const std::string BC  = "BC";
    const std::string D   = "D";
    const std::string SEP = "|";
    const std::string NL  = "\n";

    const char *DATA[] = {
        A.c_str(),
        BC.c_str(),
        D.c_str(),
        nullptr,
        nullptr,
    };

    const std::string ABC  = A + NL + BC + NL;
    const std::string ABCD = ABC + D + NL;
    const std::size_t OUTPUTBUFFER_CAP = A.size() + NL.size();

    struct OutputBuffer ob;
    EXPECT_EQ(OutputBuffer_init(&ob, OUTPUTBUFFER_CAP), &ob);

    const std::size_t buf_size = ABCD.size() + SEP.size() + 2;
    char *buf = new char[buf_size]();
    FILE *file = fmemopen(buf, buf_size, "w+b");
    ASSERT_NE(file, nullptr);

    PrintArgs pa;
    pa.output_buffer = &ob;
    pa.delim = SEP[0];
    pa.mutex = mutex;
    pa.outfile = file;
    pa.rows = 0;
    pa.types = nullptr;
    pa.suppress_newline = 0;

    // A\n is buffered in OutputBuffer and takes up all available space
    {
        EXPECT_EQ(print_parallel(&pa, 1, (char **) DATA, nullptr), 0);
        EXPECT_EQ(ob.filled, ob.capacity);
        EXPECT_EQ(pa.rows, (std::size_t) 1);
        EXPECT_EQ(fflush(file), 0);

        EXPECT_EQ(strlen(buf), (std::size_t) 0);
    }

    // BC\n is too large, so A\n and BC\n both get flushed
    {
        EXPECT_EQ(print_parallel(&pa, 1, (char **) DATA + 1, nullptr), 0);
        EXPECT_EQ(ob.filled, (std::size_t) 0);
        EXPECT_EQ(fflush(file), 0);
        EXPECT_EQ(pa.rows, (std::size_t) 2);
        EXPECT_EQ(strlen(buf), ABC.size());
        EXPECT_STREQ(buf, ABC.c_str());
    }

    // D\n is buffered in OutputBuffer
    {
        EXPECT_EQ(print_parallel(&pa, 1, (char **) DATA + 2, nullptr), 0);
        EXPECT_EQ(ob.filled, ob.capacity);
        EXPECT_EQ(fflush(file), 0);
        EXPECT_EQ(pa.rows, (std::size_t) 3);
        EXPECT_EQ(strlen(buf), ABC.size());
        EXPECT_STREQ(buf, ABC.c_str());
    }

    // force the OutputBuffer to flush to get D\n
    {
        EXPECT_EQ(OutputBuffer_flush(&ob, file), D.size() + NL.size());
        EXPECT_EQ(fflush(file), 0);
        EXPECT_EQ(pa.rows, (std::size_t) 3);
        EXPECT_EQ(strlen(buf), ABCD.size());
        EXPECT_STREQ(buf, ABCD.c_str());
    }

    // \n is buffered
    {
        EXPECT_EQ(print_parallel(&pa, 1, (char **) DATA + 3, nullptr), 0);
        EXPECT_EQ(ob.filled, NL.size());
        EXPECT_EQ(OutputBuffer_flush(&ob, file), NL.size());
        EXPECT_EQ(fflush(file), 0);
        EXPECT_EQ(pa.rows, (std::size_t) 4);
        EXPECT_STREQ(buf, (ABCD + "\n").c_str());
    }

    // print all data as one row, which is too big for the buffer, so it gets flushed immediately
    {
        const std::string ROW = A + SEP + BC + SEP + D + SEP + SEP + NL; // final separator is for NULL data column, not line end

        EXPECT_EQ(ob.filled, (std::size_t) 0);

        // reset file
        EXPECT_EQ(fseek(file, 0, SEEK_SET), 0);
        memset(buf, 0, buf_size);

        EXPECT_EQ(print_parallel(&pa, sizeof(DATA) / sizeof(DATA[0]), (char **) DATA, nullptr), 0);
        EXPECT_EQ(ob.filled, (std::size_t) 0);
        EXPECT_EQ(fflush(file), 0);
        EXPECT_EQ(pa.rows, (std::size_t) 5);
        EXPECT_EQ(memcmp(buf, ROW.c_str(), ROW.size()), 0);
    }

    fclose(file);
    delete [] buf;
    OutputBuffer_destroy(&ob);
}

TEST(print_parallel, mutex) {
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    print_parallel_mutex_actual(&mutex);
    print_parallel_mutex_actual(nullptr);
}

static void print_parallel_tlv_actual(const bool use_len) {
    const std::string INTEGER = "1";
    const std::string FLOAT   = "1.0";
    const std::string TEXT    = "text";
    const std::string BLOB    = "blob";
    const std::string NULL_   = "NULL";
    const std::string DATE    = "date";

    const char *DATA[] = {
        INTEGER.c_str(),
        FLOAT.c_str(),
        TEXT.c_str(),
        BLOB.c_str(),
        NULL_.c_str(),
        DATE.c_str(),
    };

    const std::size_t COL_COUNT = sizeof(DATA) / sizeof(DATA[0]);

    const int TYPES[] = {
        SQLITE_INTEGER,
        SQLITE_FLOAT,
        SQLITE_TEXT,
        SQLITE_BLOB,
        SQLITE_NULL,
        0,
    };

    const std::size_t total_len =
        sizeof(std::size_t) +       // row length
        sizeof(int) +               // number of columns
        INTEGER.size() + FLOAT.size() + TEXT.size() +
        BLOB.size() + NULL_.size() + DATE.size() +
        COL_COUNT +                 // 1 octet types
        COL_COUNT * sizeof(size_t)  // lengths
        ;

    struct OutputBuffer ob;
    EXPECT_EQ(OutputBuffer_init(&ob, use_len?total_len + 1:1), &ob);

    char *buf = new char[total_len + 1]();
    FILE *file = fmemopen(buf, total_len + 1, "w+b");
    ASSERT_NE(file, nullptr);

    PrintArgs pa;
    pa.output_buffer = &ob;
    pa.delim = '|'; // ignored
    pa.mutex = nullptr;
    pa.outfile = file;
    pa.rows = 0;
    pa.types = TYPES;

    EXPECT_EQ(print_parallel(&pa, COL_COUNT, (char **) DATA, nullptr), 0);
    EXPECT_EQ(ob.filled, use_len?total_len:0);
    EXPECT_EQ(OutputBuffer_flush(&ob, file), use_len?total_len:0);
    EXPECT_EQ(fflush(file), 0);
    EXPECT_EQ(pa.rows, (std::size_t) 1);

    char *curr = buf;

    // column_count
    EXPECT_EQ((std::size_t) * (int *) curr, total_len);
    curr += sizeof(std::size_t);

    // column_count
    EXPECT_EQ((std::size_t) * (int *) curr, COL_COUNT);
    curr += sizeof(int);

    for(std::size_t i = 0; i < COL_COUNT; i++) {
        // type
        EXPECT_EQ(*curr, (char) TYPES[i]);
        curr++;

        // length
        const size_t len = * (size_t *) curr;
        curr += sizeof(size_t);

        // value
        EXPECT_EQ(len, strlen(DATA[i]));
        EXPECT_EQ(std::string(curr, len), DATA[i]);
        curr += len;
    }

    fclose(file);
    delete [] buf;
    OutputBuffer_destroy(&ob);
}

TEST(print_parallel, tlv) {
    print_parallel_tlv_actual(true);
    print_parallel_tlv_actual(false);
}
