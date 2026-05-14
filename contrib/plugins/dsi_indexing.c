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

static int dsi_indexing_global_init(void *global) {
    struct input *in = (struct input *) global;
    in->process_xattrs = 1;
    return 0;
}

static void copy_collection_db(const char *srcpath, const char *dstpath) {
    /* open+create the copied collection db first to see if it already exists */
    const int dst = open(dstpath, O_WRONLY | O_CREAT | O_TRUNC | O_EXCL,
                         S_IWUSR | S_IWGRP | S_IWOTH);

    /* if another thread created this file already, this thread should not do anything */
    if (dst < 0) {
        const int err = errno;
        if (err != EEXIST) {
            fprintf(stderr, "Error: Could not create collection db copy \"%s\": %s (%d)\n",
                    dstpath, strerror(err), err);
        }
        return;
    }

    const int src = open(srcpath, O_RDONLY);
    if (src < 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not open original collection db \"%s\": %s (%d)\n",
                srcpath, strerror(err), err);
        close(dst);
        return;
    }

    struct stat st;
    if (fstat(src, &st) != 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not fstat original collection db \"%s\": %s (%d)\n",
                srcpath, strerror(err), err);
        close(src);
        close(dst);
        return;
    }

    /* do the copy */
    const ssize_t copied = copyfd(src, 0, dst, 0, st.st_size);
    if (copied != (ssize_t) st.st_size) {
        const int err = errno;
        fprintf(stderr, "Error: Could not copy collection db \"%s\" to \"%s\": %s (%d)\n",
                srcpath, dstpath, strerror(err), err);
        /* keep going even if copy failed */
    }

    /* change ownership */
    if (fchown(dst, st.st_uid, st.st_gid) != 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not fchown collection db copy \"%s\": %s (%d)\n",
                dstpath, strerror(err), err);
        /* fallthrough */
    }

    /* change permission bits */
    if (fchmod(dst, st.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)) != 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not fchmod collection db copy \"%s\": %s (%d)\n",
                dstpath, strerror(err), err);
        /* fallthrough */
    }

    close(src);
    close(dst);
}

static void dsi_indexing_dir(void *ptr, void *user_data) {
    PCS_t *pcs = (PCS_t *) ptr;
    struct entry_data *ed = pcs->ed;
    str_t *index_parent = (str_t *) pcs->data;
    (void) user_data;

    /* need the absolute path to use with the DSI xattr */
    char *orig_fullpath = realpath(pcs->work->orig_root.data, NULL);
    if (!orig_fullpath) {
        const int err = errno;
        fprintf(stderr, "Error: Failed to resolve path of \"%s\": %s (%d)\n",
                pcs->work->orig_root.data, strerror(err), err);
        return;
    }

    /*
     * get the parent directory of the starting path to get where
     * the specific index starts under the top directory
     *
     * This includes the trailing /, so don't add 1
     *
     * This needs to be calculated at each directory because each
     * thread may have a different starting path. Doing lookups is
     * possible, but requires setup.
     */
    const size_t parent_len = dirname_len(orig_fullpath, strlen(orig_fullpath));
    free(orig_fullpath); /* only need length */

    /* find DSI xattrs and copy collection dbs into corresponding location in index once */
    for(size_t i = 0; i < ed->xattrs.count; i++) {
        struct xattr *xattr = &ed->xattrs.pairs[i];

        if (!is_dsi_xattr(xattr->name, xattr->name_len)) {
            continue;
        }

        /* get collection db name */
        const char *collectiondb = xattr->value + DSI_VALUE_PREFIX_LEN;
        const size_t collectiondb_len = xattr->value_len - DSI_VALUE_PREFIX_LEN;

        /*
         * Make sure the collection database is within the tree being
         * indexed. If it is not, there is no good place to copy it to.
         */
        if (dirname_len(collectiondb, collectiondb_len) <= parent_len) {
            fprintf(stderr, "Error: Collection db \"%s\" is outside the index. Not processing collection \"%s\".\n",
                    collectiondb, xattr->name + DSI_NAME_PREFIX_LEN);
            continue;
        }

        const size_t dstpath_len = index_parent->len + 1 +
            (collectiondb_len - parent_len);

        str_t *dstpath = str_alloc(dstpath_len);
        SNFORMAT_S(dstpath->data, dstpath->len + 1, 3,
                   index_parent->data, index_parent->len,
                   "/", (size_t) 1,
                   collectiondb + parent_len, collectiondb_len - parent_len);

        copy_collection_db(collectiondb, dstpath->data);

        str_free(dstpath);
    }
}

struct plugin_operations gufi_plugin_operations = {
    .type = PLUGIN_INDEX,
    .global_init = dsi_indexing_global_init,
    .dir_action = NULL,
    .ctx_init = NULL,
    .process_dir = dsi_indexing_dir,
    .process_file = NULL,
    .ctx_exit = NULL,
    .global_exit = NULL,
};
