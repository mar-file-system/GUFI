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
#include <sys/stat.h>

#include "path_list.h"
#include "utils.h"

/*
 * invoke fn for each non-empty line in the --path-list file,
 * optionally prefixing with root when --min-level > 0.
 * fn borrows full_path; foreach_path_list_entry frees it after fn returns.
 */
ssize_t foreach_path_list_entry(struct input *in, struct work *root,
                                int (*fn)(const char *path, size_t len, void *arg),
                                void *arg) {
    FILE *file = secure_fopen(in->path_list.data, "r", 0);
    if (!file) {
        const int err = errno;
        fprintf(stderr, "could not open directory list file \"%s\": %s (%d)\n",
                in->path_list.data, strerror(err), err);
        return -1;
    }

    ssize_t count = 0;

    char *line = NULL;
    size_t n = 0;
    ssize_t got;
    while ((got = getline(&line, &n, file)) != -1) {
        /* remove trailing CRLF */
        const size_t len = trailing_non_match_index(line, got, "\r\n", 2);

        if (len == 0) {
            continue;
        }

        /* if min-level == 0, do not prefix root */
        char *full_path = NULL;
        if (in->min_level > 0 && root) {
            SNFORMAT_S_ALLOC(&full_path, 3,
                             root->name, root->name_len,
                             "/", (size_t) 1,
                             line, len);
        }
        else {
            SNFORMAT_S_ALLOC(&full_path, 1, line, len);
        }
        if (!full_path) {
            continue;
        }

        if (fn(full_path, strlen(full_path), arg) != 0) {
            free(full_path);
            free(line);
            fclose(file);
            return -1;
        }
        count++;

        free(full_path);
    }

    free(line);
    fclose(file);

    return count;
}

struct path_list_cb_ctx {
    struct input *in;
    struct work *root;
    QPTPool_ctx_t *ctx;
    QPTPool_f func;
};

static int enqueue_path_list_entry(const char *path, size_t len, void *arg) {
    struct path_list_cb_ctx *cb = (struct path_list_cb_ctx *) arg;

    /* build a work item for the listed path itself */
    struct work *subtree_root = new_work_with_name(cb->in->min_level?cb->root->name:NULL,
                                                   cb->in->min_level?cb->root->name_len:0,
                                                   path, len);

    /* directory symlinks are not allowed under the root */
    if (lstat_wrapper(subtree_root->name, &subtree_root->statuso, &subtree_root->crtime,
                      &subtree_root->stat_called, 1, cb->in->no_print_errno) != 0) {
        free(subtree_root);
        return 0;
    }

    /* check that the subtree root is a directory */
    if (!S_ISDIR(subtree_root->statuso.st_mode)) {
        fprintf(stderr, "Error: Subtree root is not a directory \"%s\"\n", path);
        free(subtree_root);
        return 0;
    }

    subtree_root->orig_root = cb->root->orig_root;

    /* parent of the input path, not the subtree root */
    subtree_root->root_parent = cb->root->root_parent;

    /* remove trailing slashes (+ 1 to keep at least 1 character) */
    subtree_root->basename_len = subtree_root->name_len - (trailing_match_index(subtree_root->name + 1, subtree_root->name_len - 1, "/", 1) + 1);

    /* go directly to --min-level */
    subtree_root->level = cb->in->min_level;

    QPTPool_enqueue(cb->ctx, cb->func, subtree_root);

    return 0;
}

/*
 * attach directory paths directly to the root path and
 * run starting at --min-level instead of walking to --min-level first
 */
ssize_t process_path_list(struct input *in, struct work *root,
                          QPTPool_ctx_t *ctx, QPTPool_f func) {
    struct path_list_cb_ctx cb = {
        .in = in,
        .root = root,
        .ctx = ctx,
        .func = func,
    };
    ssize_t count = foreach_path_list_entry(in, root, enqueue_path_list_entry, &cb);
    free(root);
    return count;
}
