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



#include <cstring>

#include "gufi_minio2index/descend.h"

int descend2(QPTPool_t *ctx, const size_t id, void *args,
             struct input *in, gmw_t *gmw, ino_t inode,
             DIR *dir, trie_t *skip_names,
             const int stat_entries, QPTPoolFunc_t processdir,
             int (*processnondir)(gmw_t *nondir, struct entry_data *ed, void *nondir_args), void *nondir_args,
             size_t *subbucket_count, size_t *dir_count, size_t *object_count, size_t *objects_processed) {
    if (!gmw) {
        return 1;
    }

    size_t subbuckets = 0;
    size_t dirs = 0;
    size_t objects = 0;
    size_t processed = 0;

    struct dirent *dir_child = NULL;

    /* calculate once */
    const size_t next_level = gmw->work.level + 1;

    if (gmw->work.level >= in->max_level) {
        goto set_counts;
    }

    /*
     * look for xl.meta in child directories
     *
     * xl.meta cannot appear in the current directory since it is
     * being treated as a directory rather than as a file's directory
     */
    while ((dir_child = readdir(dir))) {
        const size_t len = strlen(dir_child->d_name);

        /* skip . and .. */
        const int skip = trie_search(skip_names, dir_child->d_name, len);
        if (skip) {
            continue;
        }

        static const char XLDIR[] = "__XLDIR__";
        static const size_t XLDIR_LEN = sizeof(XLDIR) - 1;

        /* if this directory contains subbucket metadata, skip it */
        if (memcmp(dir_child->d_name + len - XLDIR_LEN, XLDIR, XLDIR_LEN) == 0) {
            #if defined(DEBUG) && defined(PRINT_MINIO_TYPES)
            fprintf(stderr, "%-20s %s/%s\n", "subbucket metadata", gmw->work.name, dir_child->d_name);
            #endif
            subbuckets++;
            continue;
        }

        gmw_t child;
        memset(&child, 0, sizeof(child));

        /* get child path */
        child.work.name_len = SNFORMAT_S(child.work.name, MAXPATH, 3,
                                         gmw->work.name, gmw->work.name_len,
                                         "/", (size_t) 1,
                                         dir_child->d_name, len);

        struct entry_data child_ed;
        memset(&child_ed, 0, sizeof(child_ed));

        child.work.basename_len = len;
        child.work.level = next_level;
        child.work.root_parent = gmw->work.root_parent;
        child.work.pinode = inode;
        child.root_len = gmw->root_len;
        child.cache = gmw->cache;

        /* all entries are assumed to be directories */

        /* check if child has xl.meta */
        char xlmeta[MAXPATH];
        SNFORMAT_S(xlmeta, sizeof(xlmeta), 3,
                   child.work.name, child.work.name_len,
                   "/", (size_t) 1,
                   XLMETA, XLMETA_LEN);

        if (lstat(xlmeta, &child_ed.statuso) == 0) {
            child_ed.type = 'f'; /* minio doesn't know what symlinks are */

            if (processnondir) {
                #if defined(DEBUG) && defined(PRINT_MINIO_TYPES)
                fprintf(stderr, "%-20s %s\n", "object", child.work.name);
                #endif
                processnondir(&child, &child_ed, nondir_args);
                processed++;
            }
            objects++;
        }
        else {
            child_ed.type = 'd';

            /* could not access xl.meta for some reason */
            const int err = errno;
            switch (err) {
                /* xl.meta exists, but could not read */
                /* not a permission error because all paths are owned by server */
                case EACCES:
                    break;
                /* xl.meta does not exist, so this is a path */
                case ENOENT:
                default:
                    gmw_t *copy = (gmw_t *) compress_struct(in->compress, &child, sizeof(child));
                    #if defined(DEBUG) && defined(PRINT_MINIO_TYPES)
                    fprintf(stderr, "%-20s %s\n", "path", copy->work.name);
                    #endif
                    QPTPool_enqueue(ctx, id, processdir, copy);

                    dirs++;
                    break;
            }
        }
    }

  set_counts:
    if (subbucket_count) {
        *subbucket_count = subbuckets;
    }

    if (dir_count) {
        *dir_count = dirs;
    }

    if (object_count) {
        *object_count = objects;
    }

    if (objects_processed) {
        *objects_processed = processed;
    }

    return 0;
}
