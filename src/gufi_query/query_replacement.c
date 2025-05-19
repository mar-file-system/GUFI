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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "gufi_query/query_replacement.h"
#include "gufi_query/query_user_strs.h"

size_t save_replacements(const refstr_t *sql, sll_t *idx) {
    /* idx should not already have nodes */
    sll_init(idx);

    for(size_t i = 0; i < sql->len; i++) {
        switch (sql->data[i]) {
            case USER_STR_START:
                save_user_str(idx, sql, &i);
                break;
            /* extensible */
        }
    }

    return (size_t) sll_get_size(idx);
}

int replace_sql(const refstr_t *orig, const sll_t *idx,
                const trie_t *user_strs,
                char **used) {
    if (sll_get_size(idx) == 0) {
        *used = (char *) orig->data;
        return 0;
    }

    size_t orig_start = 0;
    str_t replaced = {
        .data = NULL,
        .len = 0,
    };
    size_t allocd = 0;

    sll_loop(idx, node) {
        void *pos = sll_node_data(node);

        /* first element of pos must be a size_t */
        const size_t start = * (size_t *) pos;

        /* length of chunk between orig_start and the next replacement */
        const size_t unmodified_len = start - orig_start;

        const size_t required = replaced.len + unmodified_len + 1;

        if (allocd <= required) {
            allocd = required * 2;
            char *new_buf = realloc(replaced.data, allocd);
            if (!new_buf) {
                const int err = errno;
                fprintf(stderr, "Error: Could not realloc replacement buffer: %s (%d)\n",
                        strerror(err), err);
                free(replaced.data);
                return -11;
            }

            replaced.data = new_buf;
        }

        /* copy up to replacement */
        memcpy(replaced.data + replaced.len, orig->data + orig_start, unmodified_len);
        replaced.len += unmodified_len;
        orig_start += unmodified_len;

        /* insert replacement */
        int rc = 0;
        switch (orig->data[start]) {
            case USER_STR_START:
                rc = replace_user_str(orig, &orig_start, pos, &replaced, &allocd, user_strs);
                break;
            /* extensible */
        }

        if (rc != 0) {
            free(replaced.data);
            return rc;
        }

        orig_start++;
    }

    const size_t unmodified_len = orig->len - orig_start;
    const size_t required = replaced.len + unmodified_len + 1;
    if (allocd < required) {
        allocd = required;
        char *new_buf = realloc(replaced.data, allocd);
        if (!new_buf) {
            const int err = errno;
            fprintf(stderr, "Error: Could not realloc replacement buffer: %s (%d)\n",
                    strerror(err), err);
            free(replaced.data);
            return -1;
        }

        replaced.data = new_buf;
    }

    /* copy any remaining data */
    memcpy(replaced.data + replaced.len, orig->data + orig_start, unmodified_len);
    replaced.len += unmodified_len;

    /* NULL terminate */
    replaced.data[replaced.len] = '\0';

    /* shrink allocation */
    char *new_buf = realloc(replaced.data, replaced.len + 1);
    if (new_buf) {
        replaced.data = new_buf;
    }

    *used = replaced.data;
    return 0;
}

void free_sql(char *used, const char *orig) {
    if (used != orig) {
        free(used);
    }
}

void cleanup_replacements(sll_t *idx) {
    sll_destroy(idx, free);
}
