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


#include "OutputBuffers.h"

const char STR[] = "string";
const std::size_t LEN = strlen(STR); // includes the null terminator

TEST(OutputBuffer, use) {

    struct OutputBuffer obuf;
    ASSERT_EQ(OutputBuffer_init(&obuf, LEN), &obuf);
    EXPECT_EQ(obuf.capacity, LEN);
    EXPECT_EQ(obuf.filled, (std::size_t) 0);
    EXPECT_EQ(obuf.count, (std::size_t) 0);

    // fill up all but one octet of buffer
    EXPECT_EQ(OutputBuffer_write(&obuf, STR, LEN, 1), LEN);
    EXPECT_EQ(obuf.filled, LEN);
    EXPECT_EQ(obuf.count, (std::size_t) 1);

    // attempt to overflow buffer
    EXPECT_EQ(OutputBuffer_write(&obuf, STR, LEN, 1), (std::size_t) 0);
    EXPECT_EQ(obuf.filled, LEN);
    EXPECT_EQ(obuf.count, (std::size_t) 1);

    // flush buffer to a file
    char buf[4096];
    FILE *file = fmemopen(buf, sizeof(buf), "w");
    ASSERT_NE(file, nullptr);

    EXPECT_EQ(OutputBuffer_flush(&obuf, file), LEN);
    EXPECT_EQ(obuf.capacity, LEN);
    EXPECT_EQ(obuf.filled, (std::size_t) 0);

    fclose(file);

    // write more to the buffer (start of buffer)
    EXPECT_EQ(OutputBuffer_write(&obuf, STR, LEN, 1), LEN);
    EXPECT_EQ(obuf.filled, LEN);
    EXPECT_EQ(obuf.count, (std::size_t) 2);

    // one copy in the file
    EXPECT_STREQ(buf, STR);

    // one copy in the buffer
    EXPECT_EQ(memcmp((char *) obuf.buf, STR, LEN), 0);

    EXPECT_NO_THROW(OutputBuffer_destroy(&obuf));
}

TEST(OutputBuffers, use) {
    pthread_mutex_t mutex;
    ASSERT_EQ(pthread_mutex_init(&mutex, nullptr), 0);

    const std::size_t buffer_count = 2;
    struct OutputBuffers obufs;
    ASSERT_EQ(OutputBuffers_init(&obufs, buffer_count, LEN, &mutex), &obufs);
    EXPECT_EQ(obufs.mutex, &mutex);
    ASSERT_NE(obufs.buffers, nullptr);

    for(std::size_t i = 0; i < buffer_count; i++) {
        struct OutputBuffer & obuf = obufs.buffers[i];
        EXPECT_EQ(obuf.capacity, LEN);
        EXPECT_EQ(obuf.filled, (std::size_t) 0);
        EXPECT_EQ(obuf.count, (std::size_t) 0);
    }

    // flush buffers to a file
    {
        ASSERT_EQ(OutputBuffer_write(&obufs.buffers[0], STR, LEN, 0), LEN);
        ASSERT_EQ(OutputBuffer_write(&obufs.buffers[1], STR, LEN, 0), LEN);

        char buf[4096];
        FILE *file = fmemopen(buf, sizeof(buf), "w");
        ASSERT_NE(file, nullptr);

        EXPECT_EQ(OutputBuffers_flush_to_single(&obufs, file), LEN + LEN);
        fclose(file);

        char two_strs[4096];
        snprintf(two_strs, 4096, "%s%s", STR, STR);
        EXPECT_STREQ(buf, two_strs);
    }

    // flush buffers to multiple files
    {
        ASSERT_EQ(OutputBuffer_write(&obufs.buffers[0], STR, LEN, 0), LEN);
        ASSERT_EQ(OutputBuffer_write(&obufs.buffers[1], STR, LEN, 0), LEN);

        char buf1[4096];
        FILE *file1 = fmemopen(buf1, sizeof(buf1), "w");
        ASSERT_NE(file1, nullptr);

        // flush buffer to a file
        char buf2[4096];
        FILE *file2 = fmemopen(buf2, sizeof(buf2), "w");
        ASSERT_NE(file2, nullptr);

        FILE *files[] = {file1, file2};

        EXPECT_EQ(OutputBuffers_flush_to_multiple(&obufs, files), LEN + LEN);

        fclose(file2);
        fclose(file1);

        EXPECT_STREQ(buf1, STR);
        EXPECT_STREQ(buf2, STR);
    }

    // check if mutex is still valid
    EXPECT_EQ(pthread_mutex_lock(&mutex), 0);
    EXPECT_EQ(pthread_mutex_unlock(&mutex), 0);

    EXPECT_NO_THROW(OutputBuffers_destroy(&obufs));
    EXPECT_EQ(pthread_mutex_destroy(&mutex), 0);
}
