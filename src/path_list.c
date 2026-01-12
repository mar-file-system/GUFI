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
 * attach directory paths directly to the root path and
 * run starting at --min-level instead of walking to --min-level first
 */
ssize_t process_path_list(struct input *in, struct work *root,
                          QPTPool_ctx_t *ctx, QPTPool_f func) {
    FILE *file = fopen(in->path_list.data, "r");
    if (!file) {
        const int err = errno;
        fprintf(stderr, "could not open directory list file \"%s\": %s (%d)\n",
                in->path_list.data, strerror(err), err);
        return -1;
    }

    ssize_t enqueue_count = 0;

    char *line = NULL;
    size_t n = 0;
    ssize_t got = 0;
    while ((got = getline(&line, &n, file)) != -1) {
        /* remove trailing CRLF */
        const size_t len = trailing_non_match_index(line, got, "\r\n", 2);

        if (len == 0) {
            continue;
        }

        /* if min-level == 0, do not prefix root */
        struct work *subtree_root = new_work_with_name(in->min_level?root->name:NULL,
                                                       in->min_level?root->name_len:0,
                                                       line, len);

        /* directory symlinks are not allowed under the root */
        if (lstat_wrapper(subtree_root, in->print_eacces) != 0) {
            free(subtree_root);
            continue;
        }

        /* check that the subtree root is a directory */
        if (!S_ISDIR(subtree_root->statuso.st_mode)) {
            line[len] = '\0';
            fprintf(stderr, "Error: Subtree root is not a directory \"%s\"\n",
                    line);
            free(subtree_root);
            continue;
        }

        subtree_root->orig_root = root->orig_root;

        /* parent of the input path, not the subtree root */
        subtree_root->root_parent = root->root_parent;

        /* remove trailing slashes (+ 1 to keep at least 1 character) */
        subtree_root->basename_len = subtree_root->name_len - (trailing_match_index(subtree_root->name + 1, subtree_root->name_len - 1, "/", 1) + 1);

        /* go directly to --min-level */
        subtree_root->level = in->min_level;

        QPTPool_enqueue(ctx, func, subtree_root);

        enqueue_count++;
    }

    free(line);
    fclose(file);
    free(root);

    return enqueue_count;
}
