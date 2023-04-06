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

#if HAVE_ZLIB

extern "C" {
void compress_zlib(void **dst, void *src, const size_t struct_len, const size_t max_len);
void decompress_zlib(void **dst, void *src, void *data, const size_t struct_len);
void free_zlib(void *heap, void *stack);
}

TEST(compress, zlib) {
    data_t src;

    // make sure src compresses well
    memset(&src, 0, sizeof(src));

    // compress successfully
    {
        data_t *comp = (data_t *) malloc(sizeof(data_t));

        compress_zlib((void **) &comp, &src, DATA_LEN, DATA_LEN);
        EXPECT_NE(comp, nullptr);              // realloc may or may not change addresses
        EXPECT_EQ(comp->comp.yes, 1);
        EXPECT_LT(comp->comp.len, DATA_LEN);

        data_t stack;
        data_t *decomp = nullptr;
        decompress_zlib((void **) &decomp, &stack, comp, DATA_LEN); // comp was freed
        EXPECT_NE(decomp, comp);
        EXPECT_EQ(decomp, &stack);

        EXPECT_EQ(memcmp(decomp->data, src.data, DATA_LEN - sizeof(compressed_t)), 0);

        free_zlib(decomp, &stack);
    }

    // did not compress enough
    {
        data_t *comp = (data_t *) malloc(sizeof(data_t));
        data_t *orig = comp;

        compress_zlib((void **) &comp, &src, DATA_LEN, 0);
        EXPECT_EQ(comp, orig);                 // no realloc
        EXPECT_EQ(comp->comp.yes, 1);          // still compressed, but not enough
        EXPECT_LT(comp->comp.len, DATA_LEN);

        data_t stack;
        data_t *decomp = nullptr;
        decompress_zlib((void **) &decomp, &stack, comp, DATA_LEN); // comp was freed
        EXPECT_EQ(decomp, &stack);

        EXPECT_EQ(memcmp(decomp->data, src.data, DATA_LEN - sizeof(compressed_t)), 0);

        free_zlib(decomp, &stack);
    }

    // src->compress.compressed == 0, so *dst = &src
    {
        data_t *dst = nullptr;
        EXPECT_NO_THROW(decompress_zlib((void **) &dst, NULL, &src, DATA_LEN));
        EXPECT_EQ(dst, &src);
    }

    // same address, so not freed
    {
        EXPECT_NO_THROW(free_zlib(&src, &src));
    }
}

#endif

TEST(compress, off) {
    data_t src;

    // make sure src compresses well
    memset(&src, 0, sizeof(src));

    const int comp = 0;

    {
        void *compressed = compress_struct(comp, &src, DATA_LEN);
        EXPECT_NE(compressed, nullptr);              // realloc may or may not change addresses
        compressed_t *c = (compressed_t *) compressed;
        EXPECT_EQ(c->yes, (int8_t) 0);
        EXPECT_EQ(c->len, (size_t) 0);

        data_t stack;
        data_t *decompressed = nullptr;
        decompress_struct(comp, compressed, (void **) &decompressed, &stack, DATA_LEN); // compressed was not freed
        EXPECT_EQ(decompressed, compressed);

        free_struct(comp, decompressed, &stack, 0);
    }
}

TEST(compress, on) {
    data_t src;

    // make sure src compresses well
    memset(&src, 0, sizeof(src));

    const int comp = 1;

    {
        void *compressed = compress_struct(comp, &src, DATA_LEN);
        EXPECT_NE(compressed, nullptr);              // realloc may or may not change addresses
        compressed_t *c = (compressed_t *) compressed;
        EXPECT_EQ(c->yes, (int8_t) comp);
        EXPECT_LE(c->len, DATA_LEN);

        data_t stack;
        data_t *decompressed = nullptr;
        decompress_struct(comp, compressed, (void **) &decompressed, &stack, DATA_LEN); // compressed was not freed
        EXPECT_NE(decompressed, compressed);

        free_struct(comp, decompressed, &stack, 0);
    }
}
