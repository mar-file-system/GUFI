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
    void **plugin_data = (void **) pcs->data;
    str_t *topath = (str_t *) plugin_data[0];
    str_t *index_parent = (str_t *) plugin_data[1];
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

    /* find DSI xattrs and copy collection dbs into current directory */
    for(size_t i = 0; i < ed->xattrs.count; i++) {
        struct xattr *xattr = &ed->xattrs.pairs[i];

        if (!is_dsi_xattr(xattr->name, xattr->name_len)) {
            continue;
        }

        /* get collection db name */
        const char *collectiondb = xattr->value + DSI_VALUE_PREFIX_LEN;
        const size_t collectiondb_len = xattr->value_len - DSI_VALUE_PREFIX_LEN;

        /* get the directory containing the collection db */
        const size_t collectiondb_dir_len = dirname_len(collectiondb, collectiondb_len);

        if (collectiondb_dir_len <= parent_len) {
            fprintf(stderr, "Error: Collection db \"%s\" is outside the index. Not processing collection \"%s\".\n",
                    collectiondb, xattr->name + DSI_NAME_PREFIX_LEN);
            continue;
        }

        /*
         * Copy the collection db to its corresponding location in the
         * index.
         *     - Remove the parts of the path above the path being indexed
         *     - Remove the collection db name itself
         *     - Copied collection db is <remaining path>/<collection name>.db
         *         - Renaming to collection name since collection names are
         *           unique but collection db file names are not
         *
         * IMPORTANT: This assumes that the collection db is in an
         * source directory that is indexed and is at or above the
         * directory where it is first referenced.
         */
        const size_t dstpath_len = index_parent->len + 1 +
            (collectiondb_dir_len - parent_len) +
            xattr->name_len - DSI_NAME_PREFIX_LEN + 3;
        char *dstpath = malloc(dstpath_len + 1);
        SNFORMAT_S(dstpath, dstpath_len + 1, 5,
                   index_parent->data, index_parent->len,
                   "/", (size_t) 1,
                   /* already has / */
                   collectiondb + parent_len, collectiondb_dir_len - parent_len,
                   xattr->name + DSI_NAME_PREFIX_LEN, xattr->name_len - DSI_NAME_PREFIX_LEN,
                   ".db", (size_t) 3);

        copy_collection_db(collectiondb, dstpath);

        /*
         * whether:
         *     - this thread did not create the collection db file
         *     - this thread created the collection db file, whether or not the copy succeeded
         *
         * create a symlink in this index directory to point to the
         * copy of the collection db in the index
         */
        const size_t sym_len = topath->len + 1 + xattr->name_len - DSI_NAME_PREFIX_LEN + 3;
        char *sym = malloc(sym_len + 1);
        SNFORMAT_S(sym, sym_len + 1, 4,
                   topath->data, topath->len,
                   "/", (size_t) 1,
                   xattr->name + DSI_NAME_PREFIX_LEN, xattr->name_len - DSI_NAME_PREFIX_LEN,
                   ".db", (size_t) 3);

        /* no relative paths in the symlink */
        char *fullpath = realpath(dstpath, NULL);
        if (!fullpath) {
            const int err = errno;
            fprintf(stderr, "Error: Unable to get real path of %s: %s (%d)\n",
                    dstpath, strerror(err), err);
            free(dstpath);
            continue;
        }

        if (symlink(fullpath, sym) != 0) {
            const int err = errno;
            /* if the symlink path is also the collection db path, not an error */
            if (!((err == EEXIST) && (dstpath_len == sym_len) && !strncmp(dstpath, sym, sym_len))) {
                fprintf(stderr, "Error: Symlink \"%s\" -> \"%s\" failed: %s (%d)\n",
                        sym, fullpath, strerror(err), err);
            }
        }

        free(fullpath);
        free(sym);
        free(dstpath);
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
