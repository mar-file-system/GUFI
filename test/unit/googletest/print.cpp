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

#include "print.h"

TEST(print, parallel) {
    const std::string A   = "A";
    const std::string BC  = "BC";
    const std::string D   = "D";
    const std::string SEP = "|";
    const std::string NL  = SEP + "\n";

    const char *DATA[] = {
        A.c_str(),
        BC.c_str(),
        D.c_str(),
        nullptr,
    };

    const std::string ABC  = A + NL + BC + NL;
    const std::string ABCD = ABC + D + NL;
    const std::size_t OUTPUTBUFFER_CAP = A.size() + NL.size(); // string + (delimiter + newline)

    struct OutputBuffer ob;
    EXPECT_EQ(OutputBuffer_init(&ob, OUTPUTBUFFER_CAP), &ob);

    const std::size_t buf_size = ABCD.size() + NL.size() + 1;
    char *buf = new char[buf_size]();
    FILE *file = fmemopen(buf, ABCD.size() + NL.size() + 1, "w+b");
    ASSERT_NE(file, nullptr);

    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

    PrintArgs pa;
    pa.output_buffer = &ob;
    pa.delim = NL[0];
    pa.mutex = &mutex;
    pa.outfile = file;
    pa.rows = 0;

    // A is buffered in OutputBuffer and takes up all available space
    {
        EXPECT_EQ(print_parallel(&pa, 1, (char **) DATA, nullptr), 0);
        EXPECT_EQ(ob.filled, ob.capacity);
        EXPECT_EQ(pa.rows, (std::size_t) 1);
        EXPECT_EQ(fflush(file), 0);

        EXPECT_EQ(strlen(buf), (std::size_t) 0);
    }

    // BC is too large, so A and BC both get flushed
    {
        EXPECT_EQ(print_parallel(&pa, 1, (char **) DATA + 1, nullptr), 0);
        EXPECT_EQ(ob.filled, (std::size_t) 0);
        EXPECT_EQ(fflush(file), 0);
        EXPECT_EQ(pa.rows, (std::size_t) 2);
        EXPECT_EQ(strlen(buf), ABC.size());
        EXPECT_STREQ(buf, ABC.c_str());
    }

    // D is buffered in OutputBuffer
    {
        EXPECT_EQ(print_parallel(&pa, 1, (char **) DATA + 2, nullptr), 0);
        EXPECT_EQ(ob.filled, ob.capacity);
        EXPECT_EQ(fflush(file), 0);
        EXPECT_EQ(pa.rows, (std::size_t) 3);
        EXPECT_EQ(strlen(buf), ABC.size());
        EXPECT_STREQ(buf, ABC.c_str());
    }

    // force the OutputBuffer to flush to get D
    {
        EXPECT_EQ(OutputBuffer_flush(&ob, file), D.size() + NL.size());
        EXPECT_EQ(fflush(file), 0);
        EXPECT_EQ(pa.rows, (std::size_t) 3);
        EXPECT_EQ(strlen(buf), ABCD.size());
        EXPECT_STREQ(buf, ABCD.c_str());
    }

    // print NULL data
    {
        EXPECT_EQ(print_parallel(&pa, 1, (char **) DATA + 3, nullptr), 0);
        EXPECT_EQ(ob.filled, NL.size());
        EXPECT_EQ(OutputBuffer_flush(&ob, file), NL.size());
        EXPECT_EQ(fflush(file), 0);
        EXPECT_EQ(pa.rows, (std::size_t) 4);
        EXPECT_EQ(strlen(buf), ABCD.size() + NL.size());
        EXPECT_STREQ(buf, (ABCD + NL).c_str());
    }

    // print all data as one row, which is too big for the buffer, so it gets flushed immediately
    {
        const std::string ROW = A + SEP + BC + SEP + D + SEP + NL;

        EXPECT_EQ(ob.filled, (std::size_t) 0);

        // reset file
        EXPECT_EQ(fseek(file, 0, SEEK_SET), 0);
        memset(buf, 0, buf_size);

        EXPECT_EQ(print_parallel(&pa, sizeof(DATA) / sizeof(DATA[0]), (char **) DATA, nullptr), 0);
        EXPECT_EQ(ob.filled, (std::size_t) 0);
        EXPECT_EQ(fflush(file), 0);
        EXPECT_EQ(pa.rows, (std::size_t) 5);
        EXPECT_EQ(strlen(buf), ROW.size());
        EXPECT_STREQ(buf, ROW.c_str());
    }

    fclose(file);
    OutputBuffer_destroy(&ob);
    delete [] buf;
}
