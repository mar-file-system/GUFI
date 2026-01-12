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
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "bf.h"
#include "dsi.h"
#include "plugin.h"
#include "utils.h"           /* copyfd and SNFORMAT_S */
#include "xattrs.h"

static void dsi_indexing_global_init(void *global) {
    struct input *in = (struct input *) global;
    in->process_xattrs = 1;
}

static void dsi_indexing_dir(void *ptr, void *user_data) {
    PCS_t *pcs = (PCS_t *) ptr;
    struct entry_data *ed = pcs->ed;
    str_t *topath = (str_t *) pcs->data;
    (void) user_data;

    /* find DSI xattrs and copy collection dbs into current directory */
    for(size_t i = 0; i < ed->xattrs.count; i++) {
        struct xattr *xattr = &ed->xattrs.pairs[i];

        if (xattr->name_len < DSI_NAME_PREFIX_LEN) {
            continue;
        }

        if (strncmp(xattr->name, DSI_NAME_PREFIX, DSI_NAME_PREFIX_LEN) != 0) {
            continue;
        }

        /* get collection db name */
        const char *collectiondb = xattr->value + DSI_VALUE_PREFIX_LEN;

        const int src = open(collectiondb, O_RDONLY);
        if (src < 0) {
            const int err = errno;
            fprintf(stderr, "Error: Could not open original collection db \"%s\": %s (%d)\n",
                    collectiondb, strerror(err), err);
            continue;
        }

        struct stat st;
        if (lstat(collectiondb, &st) != 0) {
            const int err = errno;
            fprintf(stderr, "Error: Could not lstat original collection db \"%s\": %s (%d)\n",
                    collectiondb, strerror(err), err);
            goto close_src;
        }

        /*
         * Renaming to collection name since collection names are
         * unique but collection db file names are not
         */
        const size_t dstpath_len = topath->len + 1 + xattr->name_len - DSI_NAME_PREFIX_LEN + 3;
        char *dstpath = malloc(dstpath_len + 1);
        SNFORMAT_S(dstpath, dstpath_len + 1, 4,
                   topath->data, topath->len,
                   "/", (size_t) 1,
                   xattr->name + DSI_NAME_PREFIX_LEN, xattr->name_len - DSI_NAME_PREFIX_LEN,
                   ".db", (size_t) 3);

        const int dst = open(dstpath, O_WRONLY | O_CREAT | O_TRUNC,
                             st.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO));
        if (dst < 0) {
            const int err = errno;
            fprintf(stderr, "Error: Could not create collection db copy \"%s\": %s (%d)\n",
                    dstpath, strerror(err), err);
            goto free_dstpath;
        }

        /* do the copy */
        const ssize_t copied = copyfd(src, 0, dst, 0, st.st_size);
        if (copied != (ssize_t) st.st_size) {
            const int err = errno;
            fprintf(stderr, "Error: Could not copy collection db \"%s\" to \"%s\": %s (%d)\n",
                    collectiondb, dstpath, strerror(err), err);
            goto close_dst;
        }

        /* change ownership */
        if (fchown(dst, st.st_uid, st.st_gid) != 0) {
            const int err = errno;
            fprintf(stderr, "Error: Could not fchown collection db copy \"%s\": %s (%d)\n",
                    dstpath, strerror(err), err);
            /* fallthrough */
        }

      close_dst:
        close(dst);
      free_dstpath:
        free(dstpath);
      close_src:
        close(src);
    }
}

struct plugin_operations GUFI_PLUGIN_SYMBOL = {
    .type = PLUGIN_INDEX,
    .global_init = dsi_indexing_global_init,
    .ctx_init = NULL,
    .process_dir = dsi_indexing_dir,
    .process_file = NULL,
    .ctx_exit = NULL,
    .global_exit = NULL,
};
