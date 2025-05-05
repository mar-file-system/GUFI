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
#include <unistd.h>

#include "compress.h"
#include "descend.h"
#include "utils.h"

#ifdef QPTPOOL_SWAP
static int work_serialize_and_free(const int fd, QPTPool_f func, void *work, size_t *size) {
    struct work *w = (struct work *) work;
    const size_t len = w->compressed.yes?w->compressed.len:sizeof(*w);

    return QPTPool_generic_serialize_and_free(fd, func, work, len, size);
}
#endif

/*
 * Push the subdirectories in the current directory onto the queue
 * and process non directories using a user provided function.
 *
 * processdir() should always free the work item it is
 * processing. processnondir() should not free the work item it is
 * processing. This is an artifact left over from when the work item
 * was allocated on the stack and only copied to a dynamically
 * allocated space when the entry was a directory.
 */
int descend(QPTPool_t *ctx, const size_t id, void *args,
            struct input *in, struct work *work, ino_t inode,
            DIR *dir, const int skip_db,
            QPTPool_f processdir, process_nondir_f processnondir, void *nondir_args,
            struct descend_counters *counters) {
    if (!work) {
        return 1;
    }

    trie_t *skip_names = in->skip;

    struct descend_counters ctrs;
    memset(&ctrs, 0, sizeof(ctrs));

    /*
     * check current level because files/links are
     * in the same level as the directory
     */
    if (work->level <= in->max_level) {
        /* calculate once */
        const size_t next_level = work->level + 1;
        const size_t recursion_level = work->recursion_level + 1;

        int d_fd = gufi_dirfd(dir);

        struct dirent *dir_child = NULL;
        while ((dir_child = readdir(dir))) {
            const size_t len = strlen(dir_child->d_name);

            /* skip . and .. and *.db */
            const int skip = (trie_search(skip_names, dir_child->d_name, len, NULL) ||
                              (skip_db && (len >= 3) && (strncmp(dir_child->d_name + len - 3, ".db", 3) == 0)));
            if (skip) {
                continue;
            }

            struct work *child = new_work_with_name(work->name, work->name_len, dir_child->d_name, len);

            struct entry_data child_ed;
            memset(&child_ed, 0, sizeof(child_ed));
            child_ed.parent_fd = -1;

            child->basename_len = len;
            child->level = next_level;
            child->root_parent = work->root_parent;
            child->pinode = inode;

            switch (dir_child->d_type) {
                case DT_DIR:
                    child_ed.statuso.st_mode = S_IFDIR;
                    break;
                case DT_LNK:
                    child_ed.statuso.st_mode = S_IFLNK;
                    break;
                case DT_REG:
                    child_ed.statuso.st_mode = S_IFREG;
                    break;
                case DT_FIFO:
                case DT_SOCK:
                case DT_CHR:
                case DT_BLK:
                    break;
                case DT_UNKNOWN:
                default:
                    /* some filesystems don't support d_type - fall back to calling lstat */
                    if (lstat(child->name, &child_ed.statuso) != 0) {
                        continue;
                    }

                    child_ed.lstat_called = 1;
                    break;
            }

            /* push subdirectories onto the queue */
            if (S_ISDIR(child_ed.statuso.st_mode)) {
                /*
                 * check the next level because the subdirectory is
                 * NOT in the same level as the parent directory
                 */
                if (next_level <= in->max_level) {
                    child_ed.type = 'd';

                    if (!in->subdir_limit || (ctrs.dirs < in->subdir_limit)) {
                        struct work *copy = compress_struct(in->compress, child, struct_work_size(child));
                        #ifdef QPTPOOL_SWAP
                        QPTPool_enqueue_swappable(ctx, id, processdir, copy,
                                                  work_serialize_and_free, QPTPool_generic_alloc_and_deserialize);
                        #else
                        QPTPool_enqueue(ctx, id, processdir, copy);
                        #endif
                    }
                    else {
                        /*
                         * If this directory has too many subdirectories,
                         * process the current subdirectory here instead
                         * of enqueuing it. This only allows for one
                         * subdirectory work item to be allocated at a
                         * time instead of all of them, reducing overall
                         * memory usage. This branch is only applied at
                         * this level, so small subdirectories will still
                         * enqueue work, and large subdirectories will
                         * still enqueue some work and process the
                         * remaining in-situ.
                         *
                         * Return value should probably be used.
                         */
                        child->recursion_level = recursion_level;
                        processdir(ctx, id, child, args);
                        ctrs.dirs_insitu++;
                    }

                    ctrs.dirs++;
                }
                else {
                    /* skip enqueuing and just free */
                    free(child);
                }
                continue;
            }
            /* non directories */
            else if (S_ISLNK(child_ed.statuso.st_mode)) {
                child_ed.type = 'l';
                const ssize_t link_len = readlink(child->name, child_ed.linkname, MAXPATH);
                /* check for error? */
                child_ed.linkname[link_len] = '\0';
            }
            else if (S_ISREG(child_ed.statuso.st_mode)) {
                child_ed.type = 'f';
            }
            else {
                /* other types are not stored */
                free(child);
                continue;
            }

            ctrs.nondirs++;

            /* if this directory was processed, process the files/links */
            if (in->min_level <= work->level) {
                if (processnondir) {
                    if (in->process_xattrs) {
                        xattrs_setup(&child_ed.xattrs);
                        xattrs_get(child->name, &child_ed.xattrs);
                    }

                    child_ed.parent_fd = d_fd;
                    processnondir(child, &child_ed, nondir_args);
                    ctrs.nondirs_processed++;

                    if (in->process_xattrs) {
                        xattrs_cleanup(&child_ed.xattrs);
                    }
                }
            }

            free(child);
        }
    }

    if (counters) {
        *counters = ctrs;
    }

    return 0;
}

void decompress_work(struct work **dst, void *src) {
    decompress_struct((void **) dst, src);
    (*dst)->name = (char *) &(*dst)[1];
}
