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

#include "compress.h"

/* some data structure only visible here */
typedef struct {
    compressed_t comp;
    char data[1ULL << 10];
} data_t;

static const size_t DATA_LEN = sizeof(data_t);

TEST(compress, off) {
    data_t src;
    memset(&src, 0, sizeof(src));

    const int comp = 0;

    void *compressed = compress_struct(comp, &src, DATA_LEN);
    EXPECT_NE(compressed, nullptr);

    compressed_t *c = (compressed_t *) compressed;
    EXPECT_EQ(c->yes, 0);
    EXPECT_EQ(c->len, (size_t) 0);

    data_t *decompressed = nullptr;
    EXPECT_EQ(decompress_struct((void **) &decompressed, compressed, DATA_LEN), 0);
    EXPECT_EQ(decompressed, compressed);

    free_struct(decompressed, compressed, 0);
}

TEST(compress, on) {
    data_t src;
    memset(&src, 0, sizeof(src));

    const int comp = 1;

    void *compressed = compress_struct(comp, &src, DATA_LEN);
    EXPECT_NE(compressed, nullptr);

    compressed_t *c = (compressed_t *) compressed;
    EXPECT_EQ(c->yes, comp);
    EXPECT_LE(c->len, DATA_LEN);

    data_t stack;
    data_t *decompressed = &stack;
    EXPECT_EQ(decompress_struct((void **) &decompressed, compressed, DATA_LEN), 0);
    EXPECT_NE(decompressed, compressed);

    /* compressed was freed, decompressed points to stack address */
    free_struct(decompressed, compressed, 0);
}

#ifdef HAVE_ZLIB
TEST(compress_zlib, bad) {
    compressed_t src;
    memset(&src, 0, sizeof(src));

    compressed_t *dst = (compressed_t *) compress_struct(1, &src, sizeof(src));
    EXPECT_NE(dst, nullptr);
    EXPECT_NE(dst, &src);
    EXPECT_EQ(dst->yes, (std::int8_t) 0);
    EXPECT_EQ(dst->len, (std::size_t) 0);

    free(dst);
}

TEST(decompress_zlib, bad) {
    compressed_t src; // should not be freed
    src.yes = 1;
    src.len = 0;

    compressed_t dst;
    compressed_t *dstp = &dst;

    ASSERT_NO_THROW(decompress_struct((void **) &dstp, &src, sizeof(src)));
}
#endif
