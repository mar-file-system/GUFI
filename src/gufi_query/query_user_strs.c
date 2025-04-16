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
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>

#include "gufi_query/query_user_strs.h"
#include "utils.h"

/* insert position of user string into list of indexes */
void save_user_str(sll_t *idx, const refstr_t *sql, const size_t *i) {
    size_t j = *i + 1;

    /* find stop character */
    for(; (j < sql->len) && (sql->data[j] != USER_STR_END); j++);

    /* not found */
    if ((*i + 1) == j) {
        /* {} is not replaced */
        return;
    }
    else if (j == sql->len) {
        fprintf(stderr, "Warning: Could not find stop character for user string key starting at index %zu of \"%s\"\n",
                *i, sql->data);
        return;
    }

    /* indexes of text between start and stop characters */
    usk_t *key = malloc(sizeof(*key));
    key->start = *i;
    key->end = j;
    sll_push(idx, key);
}

int replace_user_str(const refstr_t *sql,
                     size_t *src_start,
                     void *pos,
                     str_t *replaced,
                     size_t *allocd,
                     const trie_t *user_strs) {
    const usk_t *usk = (usk_t *) pos;
    const char *key = &sql->data[usk->start + 1];
    const size_t len = (usk->end - 1) - (usk->start + 1) + 1;

    /* find user string set for this key */
    str_t *val = NULL;
    if (trie_search(user_strs, key, len, (void **) &val) != 1) {
        fprintf(stderr, "Error: replacement key not found: %.*s\n", (int) len, key);
        return -1;
    }

    /* reallocate */
    const size_t req_alloc = replaced->len + val->len + 1;
    if (req_alloc >= *allocd) {
        *allocd = req_alloc * 2;
        char *new_buf = realloc(replaced->data, *allocd);
        if (!new_buf) {
            const int err = errno;
            fprintf(stderr, "Error: Could not reallocate user string buffer: %s (%d)\n",
                    strerror(err), err);
            return -1;
        }

        replaced->data = new_buf;
    }

    /* copy in replacement string */
    memcpy(replaced->data + replaced->len, val->data, val->len);
    replaced->len += val->len;

    *src_start = usk->end;    /* move to } */

    return 0;
}
