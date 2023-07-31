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
#include <string.h>

#include "compress.h"

/* starting offset of actual data */
static const size_t COMP_OFFSET = sizeof(compressed_t);

#if HAVE_ZLIB
#include <zlib.h>

static int compress_zlib(void *dst, size_t *dst_len, void *src, const size_t src_len) {
    return compress(dst, dst_len, src, src_len);
}

static int decompress_zlib(void *dst, size_t *dst_len, void *src, const size_t src_len) {
    return uncompress(dst, dst_len, src, src_len);
}
#endif

/* always return new address */
void *compress_struct(const int comp, void *src, const size_t struct_len) {
    compressed_t *dst = malloc(struct_len);
    dst->yes = 0;
    dst->len = 0;

    if (comp) {
        dst->len = struct_len - COMP_OFFSET;

        #if HAVE_ZLIB
        if (compress_zlib(((unsigned char *) dst) + COMP_OFFSET, &dst->len,
                          ((unsigned char *) src) + COMP_OFFSET,  dst->len) == Z_OK) {
            dst->yes = 1;
            dst->len += COMP_OFFSET;
        }
        #endif

        /* if compressed enough, reallocate to reduce memory usage */
        if (dst->yes && (dst->len < struct_len)) {
            void *r = realloc(dst, COMP_OFFSET + dst->len);
            if (r) {
                dst = r;
            }

            /* if realloc failed, dst can still be used */
            return dst;
        }
    }

    /* if no compression or compression failed, copy src */
    memcpy(dst, src, struct_len);

    return dst;
}

int decompress_struct(void **dst, void *src, const size_t struct_len) {
    compressed_t *comp = (compressed_t *) src;
    if (comp->yes) {
        compressed_t *decomp = (compressed_t *) *dst;
        decomp->len = struct_len - COMP_OFFSET;

        #if HAVE_ZLIB
        if (decompress_zlib(((unsigned char *) *dst) + COMP_OFFSET, &decomp->len,
                            ((unsigned char *)  src) + COMP_OFFSET,    comp->len) != Z_OK) {
            return 1;
        }
        #endif

        free(src);
        return 0;
    }

    /*
     * compression not enabled or compression enabled but
     * data was not compressed - use original pointer
     */
    *dst = src;

    return 0;
}

void free_struct(void *used, void *data, const size_t recursion_level) {
    if (used == data) {              /* used received data directly */
        if (recursion_level == 0) {  /* work was not from in-situ call, so data is dynamically allocated */
            free(data);
        }
    }
}
