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

#include "debug.h"

static const uint64_t nsecs_per_sec = 1000000000ULL;

TEST(debug, since_epoch) {
    epoch = 0;

    struct timespec ts = {
        .tv_sec = 1,
        .tv_nsec = 0,
    };

    EXPECT_EQ(since_epoch(&ts), nsecs_per_sec);
}

TEST(debug, nsec) {
    struct start_end se = {
        .start = {
            .tv_sec = 1,
            .tv_nsec = 0,
        },
        .end = {
            .tv_sec = 2,
            .tv_nsec = 0,
        },
    };

    EXPECT_EQ(nsec(&se), nsecs_per_sec);
}

TEST(debug, sec) {
    EXPECT_EQ(sec(nsecs_per_sec), 1.0L);
}

TEST(debug, print_timer) {
    epoch = 0;

    struct start_end se = {
        .start = {
            .tv_sec = 1,
            .tv_nsec = 0,
        },
        .end = {
            .tv_sec = 2,
            .tv_nsec = 0,
        },
    };

    struct OutputBuffers obufs;
    memset(&obufs, 0, sizeof(obufs));
    EXPECT_EQ(print_timer(nullptr, 0, "", &se), -1);
    EXPECT_EQ(print_timer(&obufs,  0, "", &se), -1);

    ASSERT_EQ(OutputBuffers_init(&obufs, 1, (std::size_t) 4096, nullptr), &obufs);
    EXPECT_EQ(obufs.mutex, nullptr);
    ASSERT_NE(obufs.buffers, nullptr);

    struct OutputBuffer &obuf = obufs.buffers[0];
    EXPECT_EQ(obuf.capacity, (std::size_t) 4096);
    EXPECT_EQ(obuf.filled, (std::size_t) 0);
    EXPECT_EQ(obuf.count, (std::size_t) 0);

    EXPECT_EQ(print_timer(&obufs, 0, "", &se), 0);

    EXPECT_NO_THROW(OutputBuffers_destroy(&obufs));
}
