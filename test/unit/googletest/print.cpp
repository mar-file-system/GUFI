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
#include <cstdio>
#include <cstring>

#include <gtest/gtest.h>

extern "C" {

#include "print.h"

}

static const char A[]        = "A";
static const char BC[]       = "BC";
static const char D[]        = "D";
static const char SEP[]      = "|\n";

static const char *DATA[] = {
    A,
    BC,
    D
};

static const char   ABC[]    = "A|\nBC|\n";
static const size_t ABC_LEN  = strlen(ABC);
static const char   ABCD[]   = "A|\nBC|\nD|\n";
static const size_t ABCD_LEN = strlen(ABCD);

static const size_t OUTPUTBUFFER_CAP = sizeof(A) + 1; // (string + delimiter) + newline

TEST(print, parallel) {
    struct OutputBuffer ob;
    EXPECT_EQ(OutputBuffer_init(&ob, OUTPUTBUFFER_CAP), &ob);

    char buf[sizeof(ABCD)] = {0};
    FILE *file = fmemopen(buf, sizeof(buf), "w+b");
    ASSERT_NE(file, nullptr);

    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

    PrintArgs pa;
    pa.output_buffer = &ob;
    pa.delim = SEP[0];
    pa.mutex = &mutex;
    pa.outfile = file;
    pa.rows = 0;

    // A is buffered in OutputBuffer
    {
        EXPECT_EQ(print_parallel(&pa, 1, (char **) DATA, nullptr), 0);
        EXPECT_EQ(fflush(file), 0);

        EXPECT_EQ(strlen(buf), (size_t) 0);
    }

    // BC is too large, so A gets flushed, followed by BC
    {
        EXPECT_EQ(print_parallel(&pa, 1, (char **) DATA + 1, nullptr), 0);
        EXPECT_EQ(fflush(file), 0);

        EXPECT_EQ(strlen(buf), ABC_LEN);
        EXPECT_EQ(memcmp(buf, ABC, ABC_LEN), 0);
    }

    // D is buffered in OutputBuffer
    {
        EXPECT_EQ(print_parallel(&pa, 1, (char **) DATA + 2, nullptr), 0);
        EXPECT_EQ(fflush(file), 0);

        EXPECT_EQ(strlen(buf), ABC_LEN);
        EXPECT_EQ(memcmp(buf, ABC, ABC_LEN), 0);
    }

    // force the OutputBuffer to flush to get D
    {
        const size_t expected_flush_len = snprintf(nullptr, 0, "%s%s", D, SEP);
        EXPECT_EQ(OutputBuffer_flush(&ob, file), expected_flush_len);
        EXPECT_EQ(fflush(file), 0);

        EXPECT_EQ(strlen(buf), ABCD_LEN);
        EXPECT_EQ(memcmp(buf, ABCD, ABCD_LEN), 0);
    }

    fclose(file);
    OutputBuffer_destroy(&ob);
}
