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

#include "swap.h"

static const char PREFIX[]  = "";
static const std::size_t ID = 0x1234;

TEST(swap, null_prefix) {
    struct Swap swap;
    swap_init(&swap);
    EXPECT_EQ(swap_start(&swap, nullptr, ID), 1);
    swap_destroy(&swap);
}

TEST(swap, restart_without_start) {
    struct Swap swap;
    swap_init(&swap);
    EXPECT_EQ(swap_restart(&swap, PREFIX, ID), 0);
    EXPECT_EQ(swap_restart(&swap, PREFIX, ID), 0);
    swap_stop(&swap);
    swap_destroy(&swap);
}

TEST(swap, good) {
    struct Swap swap;
    swap_init(&swap);
    EXPECT_EQ(swap.write.fd,                 -1);
    EXPECT_EQ(swap.write.count, (std::size_t) 0);
    EXPECT_EQ(swap.write.size,  (std::size_t) 0);
    EXPECT_EQ(swap.read.fd,                  -1);
    EXPECT_EQ(swap.read.count,  (std::size_t) 0);
    EXPECT_EQ(swap.read.size,   (std::size_t) 0);

    // set up write; read is not affected
    EXPECT_EQ(swap_start(&swap, PREFIX, ID),  0);
    EXPECT_GT(swap.write.fd, -1);
    EXPECT_EQ(swap.read.fd,  -1);

    // move write to read and open new write
    const int fd = swap.write.fd;
    EXPECT_EQ(swap_read_prep(&swap),          1); // nothing has been written, so read_prep fails

    // "write"
    const std::size_t write_count = 1;
    swap.write.count = write_count;

    EXPECT_EQ(swap_read_prep(&swap),          0);
    EXPECT_GT(swap.write.fd,                 -1);
    EXPECT_NE(swap.write.fd,                 fd);
    EXPECT_EQ(swap.write.count, (std::size_t) 0);
    EXPECT_EQ(swap.write.size,  (std::size_t) 0);
    EXPECT_EQ(swap.read.fd,                  fd);
    EXPECT_EQ(swap.read.count,      write_count);
    EXPECT_EQ(swap.read.size,   (std::size_t) 0);

    // close read; write is not affected
    swap_read_done(&swap);
    EXPECT_GT(swap.write.fd,                 -1);
    EXPECT_EQ(swap.write.count, (std::size_t) 0);
    EXPECT_EQ(swap.write.size,  (std::size_t) 0);
    EXPECT_EQ(swap.read.fd,                  -1);
    EXPECT_EQ(swap.read.count,  (std::size_t) 0);
    EXPECT_EQ(swap.read.size,   (std::size_t) 0);

    // close both write and read; prefix and id are not affected
    swap_stop(&swap);
    EXPECT_EQ(swap.prefix,               PREFIX);
    EXPECT_EQ(swap.id,                       ID);
    EXPECT_EQ(swap.write.fd,                 -1);
    EXPECT_EQ(swap.write.count, (std::size_t) 0);
    EXPECT_EQ(swap.write.size,  (std::size_t) 0);
    EXPECT_EQ(swap.read.fd,                  -1);
    EXPECT_EQ(swap.read.count,  (std::size_t) 0);
    EXPECT_EQ(swap.read.size,   (std::size_t) 0);

    // everything is cleared
    swap_destroy(&swap);
    EXPECT_EQ(swap.prefix,              nullptr);
    EXPECT_EQ(swap.id,          (std::size_t) 0);
    EXPECT_EQ(swap.write.fd,                 -1);
    EXPECT_EQ(swap.write.count, (std::size_t) 0);
    EXPECT_EQ(swap.write.size,  (std::size_t) 0);
    EXPECT_EQ(swap.read.fd,                  -1);
    EXPECT_EQ(swap.read.count,  (std::size_t) 0);
    EXPECT_EQ(swap.read.size,   (std::size_t) 0);
}

TEST(swap, read_prep_bad_lseek) {
    struct Swap swap;
    swap_init(&swap);
    EXPECT_EQ(swap.write.fd, -1);
    swap.write.count = 1;

    // not calling start to keep write.fd == -1

    // this will fail on lseek
    EXPECT_EQ(swap_read_prep(&swap), 1);
    EXPECT_EQ(swap.read.fd,                  -1);
    EXPECT_EQ(swap.read.count,  (std::size_t) 1);
    EXPECT_EQ(swap.read.size,   (std::size_t) 0);

    swap_read_done(&swap);

    swap_stop(&swap);
    swap_destroy(&swap);
}

TEST(swap, read_prep_bad_new_write) {
    struct Swap swap;
    swap_init(&swap);
    EXPECT_EQ(swap_start(&swap, PREFIX, ID), 0);

    // the swap file's has been deleted, so it can be replaced with a directory
    char path[1024];
    snprintf(path, sizeof(path), "%s%jd.swap.%zu", PREFIX, (intmax_t) getpid(), ID);
    ASSERT_EQ(mkdir(path, S_IRWXU), 0);

    swap.write.count = 1;

    // this will fail when the new write is opened
    EXPECT_EQ(swap_read_prep(&swap), 1);

    EXPECT_EQ(rmdir(path), 0);

    // write will have failed to open
    EXPECT_EQ(swap.write.fd,                 -1);
    EXPECT_EQ(swap.write.count, (std::size_t) 0);
    EXPECT_EQ(swap.write.size,  (std::size_t) 0);

    // read will have the old write file descriptor
    EXPECT_GT(swap.read.fd,                  -1);
    EXPECT_EQ(swap.read.count,  (std::size_t) 1);
    EXPECT_EQ(swap.read.size,   (std::size_t) 0);

    swap_read_done(&swap);

    swap_stop(&swap);
    swap_destroy(&swap);
}

TEST(swap, read_done_bad_fd) {
    struct Swap swap;
    swap_init(&swap);
    EXPECT_EQ(swap.read.fd, -1);
    swap_read_done(&swap);
    swap_stop(&swap);
    swap_destroy(&swap);
}

TEST(swap, stop_twice) {
    struct Swap swap;
    swap_init(&swap);
    EXPECT_EQ(swap_start(&swap, PREFIX, ID), 0);

    swap.write.count = 1;

    EXPECT_EQ(swap_read_prep(&swap), 0);
    EXPECT_GT(swap.write.fd, -1);
    EXPECT_GT(swap.read.fd, -1);

    // not calling read_done

    swap_stop(&swap);
    swap_stop(&swap);

    swap_destroy(&swap);
}

TEST(swap, nonexistant_prefix) {
    char parent[] = "XXXXXX";
    ASSERT_NE(mkdtemp(parent), nullptr);

    char prefix[1024];
    snprintf(prefix, sizeof(prefix), "%s/non_existant_dir/", parent);

    struct Swap swap;
    swap_init(&swap);

    // subdirectory doesn't exist
    EXPECT_EQ(swap_start(&swap, prefix, ID), 1);

    swap_destroy(&swap);

    EXPECT_EQ(rmdir(parent), 0);
}

TEST(swap, path_is_dir) {
    char path[1024];
    snprintf(path, sizeof(path), "%s%jd.swap.%zu", PREFIX, (intmax_t) getpid(), ID);
    ASSERT_EQ(mkdir(path, S_IRWXU), 0);

    struct Swap swap;
    swap_init(&swap);

    // swap file name already exists, and it is a directory
    EXPECT_EQ(swap_start(&swap, "", ID), 1);

    swap_destroy(&swap);

    EXPECT_EQ(rmdir(path), 0);
}
