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



#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "gufi_query/query_formatting.h"
#include "utils.h"

size_t find_formatting(const refstr_t *sql, sll_t *fmts) {
    /* fmts should not already have nodes */
    sll_init(fmts);

    for(size_t i = 0; i < sql->len; i++) {
        if (sql->data[i] == '%') {
            i++;

            /* format width is not allowed */

            /* only track known format chars */
            for(size_t j = 0; j < sizeof(FMT_CHARS); j++) {
                if (sql->data[i] == FMT_CHARS[j]) {
                    sll_push(fmts, (void *) (uintptr_t) i);
                    break;
                }
            }
        }
    }

    return (size_t) sll_get_size(fmts);
}

int replace_formatting(const refstr_t *sql, const sll_t *fmts, const refstr_t *source_prefix,
                       struct work *work,
                       char **buf, size_t *size) {
    if (sll_get_size(fmts) == 0) {
        *buf = (char *) sql->data;
        *size = sql->len;
        return 0;
    }

    size_t alloc = sql->len + 1;
    char *replaced = malloc(alloc);
    size_t src_start = 0; /* position in source string */
    size_t dst_start = 0; /* position in destination string */

    /* cache %s */
    char *s = NULL;
    size_t s_len = 0;

    /* root_parent contains a trailing slash */
    size_t full_root_len = work->root_parent.len + work->root_basename_len;

    sll_loop(fmts, node) {
        const size_t pos = (size_t) (uintptr_t) sll_node_data(node);

        /* length of chunk before format */
        const size_t chunk_len = pos - 2 + 1 - src_start; /* if last char is at pos, length is pos + 1 */

        /* replacement string */
        refstr_t str = {
            .data = NULL,
            .len = 0,
        };
        switch (sql->data[pos]) {
            case 'n':
                str.data = work->name + work->name_len - work->basename_len;
                str.len = work->basename_len;
                break;
            case 'i':
                str.data = work->name;
                str.len = work->name_len;
                break;
            case 's':
                if (!s) {
                    refstr_t relpath = {
                        .data = work->name + full_root_len,
                        .len = work->name_len - full_root_len,
                    };

                    /* root_basename_len does not count slash, but one will exist if not currently at root */
                    if (relpath.data[0] == '/') {
                        relpath.data++;
                        relpath.len--;
                        full_root_len++;
                    }

                    /* replace root parent with source_prefix */
                    if (source_prefix->len == 0) {
                        s_len = relpath.len;
                        s = malloc(s_len + 1);
                        SNFORMAT_S(s, s_len + 1, 1,
                                   relpath.data, relpath.len);
                    }
                    else if (relpath.len == 0) {
                        s_len = source_prefix->len;
                        s = malloc(s_len + 1);
                        SNFORMAT_S(s, s_len + 1, 1,
                                   source_prefix->data, source_prefix->len);
                    }
                    else {
                        s_len = source_prefix->len + 1 + relpath.len;
                        s = malloc(s_len + 1);
                        SNFORMAT_S(s, s_len + 1, 3,
                                   source_prefix->data, source_prefix->len,
                                   "/", (size_t) 1,
                                   relpath.data, relpath.len);
                    }
                }

                str.data = s;
                str.len = s_len;
                break;
            /* there should only ever be known formats */
            /* default: */
            /*     break; */
        }

        const size_t req_alloc = dst_start + chunk_len + str.len + 1;

        /* not enough space for chunk and replacement string */
        if (req_alloc >= alloc) {
            alloc = req_alloc * 2;
            char *new_buf = realloc(replaced, alloc);
            if (!new_buf) {
                const int err = errno;
                fprintf(stderr, "Error: Could not reallocate formatted buffer: %s (%d)\n",
                        strerror(err), err);
                free(replaced);
                return -1;
            }

            replaced = new_buf;
        }

        /* copy chunk */
        memcpy(replaced + dst_start, sql->data + src_start, chunk_len);
        dst_start += chunk_len;

        /* copy in replacement string */
        memcpy(replaced + dst_start, str.data, str.len);
        dst_start += str.len;

        src_start = pos + 1;    /* move past format char */
    }

    free(s);
    s = NULL;

    /* copy final chunk */
    const size_t rem = sql->len - src_start;

    if (rem) {
        /* not enough space for this format - realloc */
        if ((dst_start + rem + 1) > alloc) {
            alloc += rem + 1;
            char *new_buf = realloc(replaced, alloc);
            if (!new_buf) {
                const int err = errno;
                fprintf(stderr, "Error: Could not reallocate formatted buffer: %s (%d)\n",
                        strerror(err), err);
                free(replaced);
                return -1;
            }

            replaced = new_buf;
        }

        /* copy original string up to formatting */
        memcpy(replaced + dst_start, sql->data + src_start, rem);
        dst_start += rem;
    }

    replaced[dst_start] = '\0';

    /* shrink allocation down */
    char *new_buf = realloc(replaced, dst_start + 1);
    if (new_buf) {
        replaced = new_buf;
    } /* else use original buffer */

    *buf = replaced;
    *size = dst_start;

    return 0;
}

void cleanup_formatting(sll_t *fmts) {
    sll_destroy(fmts, NULL);
}
