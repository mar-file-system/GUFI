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

#include "gufi_query/query_fmt.h"
#include "utils.h"

/* insert position of single % into list of indexes */
void save_fmt(sll_t *idx, const refstr_t *sql, const size_t *i) {
    /* only track known format chars */
    const size_t c = *i + 1;
    for(size_t j = 0; j < sizeof(FMT_CHARS); j++) {
        if (sql->data[c] == FMT_CHARS[j]) {
            size_t *pos = malloc(sizeof(*pos));
            *pos = *i++;
            sll_push(idx, pos);
            break;
        }
    }
}

/* replace single format string */
int replace_fmt(const refstr_t *sql,
                size_t *src_start,
                void *pos,
                str_t *replaced,
                size_t *allocd,
                const refstr_t *source_prefix,
                struct work *work) {
    const size_t idx = * (size_t *) pos;

    /* root_parent contains a trailing slash */
    size_t full_root_len = work->root_parent.len + work->root_basename_len;

    /* replacement string */
    refstr_t str = {
        .data = NULL,
        .len = 0,
    };

    char *s = NULL;
    size_t s_len = 0;

    switch (sql->data[idx + 1]) {
        case 'n': /* directory name */
            str.data = work->name + work->name_len - work->basename_len;
            str.len = work->basename_len;
            break;
        case 'i': /* directory path */
            str.data = work->name;
            str.len = work->name_len;
            break;
        case 's': /* source path (need -p) */
            ;
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

            str.data = s;
            str.len = s_len;
            break;
            /* there should only ever be known formats */
            /* default: */
            /*     break; */
    }

    const size_t req_alloc = replaced->len + str.len + 1;

    /* not enough space for chunk and replacement string */
    if (req_alloc >= *allocd) {
        *allocd = req_alloc * 2;
        char *new_buf = realloc(replaced->data, *allocd);
        if (!new_buf) {
            const int err = errno;
            fprintf(stderr, "Error: Could not reallocate formatted buffer: %s (%d)\n",
                    strerror(err), err);
            return -1;
        }

        replaced->data = new_buf;
    }

    /* copy in replacement string */
    memcpy(replaced->data + replaced->len, str.data, str.len);
    replaced->len += str.len;

    *src_start = idx + 1;    /* move to format char */

    free(s);
    return 0;
}
