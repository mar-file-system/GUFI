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
#include <sys/stat.h>
#include <sys/types.h>

#include "descend.h"
#include "dbutils.h"
#include "gufi_query/gqw.h"
#include "utils.h"

size_t gqw_size(gqw_t *gqw) {
    return sizeof(*gqw) + gqw->work.name_len + 1 + gqw->sqlite3_name_len + 1;
}

/*
 * Allocates a new gqw_t on the heap with enough room to
 * fit the given `basename` with an optional `prefix`.
 *
 * Initializes the following fields:
 *   - name
 *   - name_len
 *   - sqlite3_name
 *   - sqlite3_name_len
 */
gqw_t *new_gqw_with_name(const char *prefix, const size_t prefix_len,
                         const char *basename, const size_t basename_len,
                         const unsigned char d_type, const int next_level,
                         const char *sqlite3_prefix, const size_t sqlite3_prefix_len) {
    /* +1 for path separator */
    const size_t name_len = prefix_len + 1 + basename_len;

    /* assume every character is replaced */
    const size_t sqlite3_name_len = sqlite3_prefix_len + 1 + 3 * basename_len;

    /* allocate space for sqlite3_name for now */
    gqw_t *gqw = calloc(1, sizeof(*gqw) + name_len + 1 + sqlite3_name_len + 1);
    gqw->work.name = (char *) &gqw[1];
    gqw->work.name_len = SNFORMAT_S(gqw->work.name, name_len + 1, 3,
                                    prefix, prefix_len,
                                    "/", (size_t) 1,
                                    basename, basename_len);

    /* allow for paths immediately under the input paths to be symlinks */
    if (next_level < 2) {
        if (stat(gqw->work.name, &gqw->work.statuso) != 0) {
            const int err = errno;
            fprintf(stderr, "Error: Could not stat \"%s\": %s (%d)\n",
                    gqw->work.name, strerror(err), err);
            free(gqw);
            return NULL;
        }

        gqw->work.stat_called = NOT_STATX_CALLED;
    }
    else {
        if (!try_skip_lstat(d_type, &gqw->work)) {
            free(gqw);
            return NULL;
        }
    }

    if (S_ISDIR(gqw->work.statuso.st_mode)) {
        gqw->sqlite3_name = gqw->work.name + name_len + 1;

        /* append converted entry name to converted directory */
        gqw->sqlite3_name_len = SNFORMAT_S(gqw->sqlite3_name, sqlite3_name_len + 1, 1,
                                           sqlite3_prefix, sqlite3_prefix_len);

        if (basename_len) {
            size_t len = basename_len;

            gqw->sqlite3_name_len += SNFORMAT_S(gqw->sqlite3_name + gqw->sqlite3_name_len,
                                                sqlite3_name_len + 1 - gqw->sqlite3_name_len, 1,
                                                "/", (size_t) 1);

            gqw->sqlite3_name_len += sqlite_uri_path(gqw->sqlite3_name + gqw->sqlite3_name_len,
                                                     sqlite3_name_len + 1 - gqw->sqlite3_name_len,
                                                     basename, &len);
        }
    }

    /* try to shrink allocation */
    gqw_t *r = realloc(gqw, sizeof(*gqw) + gqw->work.name_len + 1 + gqw->sqlite3_name_len + 1);

    if (!r) {
        r = gqw;
    }

    /* struct was moved - update pointers */
    if (r != gqw) {
        r->work.name = (char *) &r[1];
        if (r->sqlite3_name) {
            r->sqlite3_name = r->work.name + name_len + 1 + 1;
        }
    }

    return r;
}

void decompress_gqw(gqw_t **dst, void *src) {
    decompress_struct((void **) dst, src);
    (*dst)->work.name = (char *) &(*dst)[1];
    if ((*dst)->sqlite3_name) {
        (*dst)->sqlite3_name = (*dst)->work.name + (*dst)->work.name_len + 1;
    }
}
