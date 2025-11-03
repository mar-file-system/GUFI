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
#include <sys/types.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "dbutils.h"
#include "debug.h"
#include "external.h"
#include "utils.h"

struct Unrollup {
    char name[MAXPATH];
    size_t name_len;
    size_t level;
    int rolledup; /* set by parent, can be modified by self */
};

static struct Unrollup *unrollup_create(const char *name, const size_t name_len,
                                        const char *subpath, const size_t subpath_len,
                                        const size_t level) {
    /*
     * This assumes that lstat/S_ISDIR failures do not occur too
     * often, causing wasted mallocs.
     *
     * To avoid mallocs that get freed immediately, another way to do
     * this is to create the path name, stat it, and if it is a
     * directory, copy it into a newly malloc-ed struct Unrollup,
     * which is expected to happen most of it not all the time, so not
     * doing this to avoid memcpys.
     */
    struct Unrollup *work = malloc(sizeof(struct Unrollup));
    if (subpath && subpath_len) {
        work->name_len = SNFORMAT_S(work->name, MAXPATH, 3,
                                    name, name_len,
                                    "/", (size_t) 1,
                                    subpath, subpath_len);
    }
    else {
        work->name_len = SNFORMAT_S(work->name, MAXPATH, 1, name, name_len);
    }
    work->level = level;
    work->rolledup = 0; /* assume this path was not rolled up */

    struct stat st;
    if (lstat(work->name, &st) != 0) {
        fprintf(stderr, "Could not stat '%s'\n", work->name);
        free(work);
        return NULL;
    }

    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "'%s' is not a directory\n", work->name);
        free(work);
        return NULL;
    }

    return work;
}

static int deep_enough(struct input *in, struct Unrollup *work) {
    return (in->min_level <= work->level);
}

static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    struct input *in = (struct input *) args;
    struct Unrollup *work = (struct Unrollup *) data;
    int rc = 0;

    DIR *dir = opendir(work->name);
    if (!dir) {
        fprintf(stderr, "Error: Could not open directory \"%s\": %s", work->name, strerror(errno));
        rc = 1;
        goto cleanup;
    }

    sqlite3 *db = NULL;

    if (deep_enough(in, work)) {
        char dbname[MAXPATH];
        SNPRINTF(dbname, MAXPATH, "%s/" DBNAME, work->name);

        db = opendb(dbname, SQLITE_OPEN_READWRITE, 0, 0, NULL, NULL);

        if (!db) {
            rc = 1;
        }
    }

    /* get roll up status set by parent */
    int rolledup = work->rolledup;

    /*
     * if parent of this directory was rolled up, all children are rolled up, so skip this check
     * if parent of this directory was not rolled up, this directory might be
     */
    if (deep_enough(in, work) && db && !rolledup) {
        rc = !!get_rollupscore(db, &rolledup);
    }

    /*
     * < not <= because (max_level - 1) will queue up work at max_level
     *     - there is no point for max_level to queue up
     *       work at (max_level + 1) only to do nothing
     */
    if (work->level < in->max_level) {
        const size_t next_level = work->level + 1;

        /*
         * always descend
         *     if rolled up, all child also need processing
         *     if not rolled up, children might be
         *
         * descend first to keep working while cleaning up db
         */
        struct dirent *entry = NULL;
        while ((entry = readdir(dir))) {
            const size_t len = strlen(entry->d_name);
            if (len < 3) {
                if ((strncmp(entry->d_name, ".",  2) == 0) ||
                    (strncmp(entry->d_name, "..", 3) == 0)) {
                    continue;
                }
            }
            else if (len > 3) {
                if (strncmp(entry->d_name + len - 3, ".db", 4) == 0) {
                    continue;
                }
            }

            struct Unrollup *subdir = unrollup_create(work->name, work->name_len,
                                                      entry->d_name, len,
                                                      next_level);

            if (subdir) {
                /* set child rolledup status so that if this dir */
                /* was rolled up, child can skip roll up check */
                subdir->rolledup = rolledup;

                QPTPool_enqueue(ctx, id, processdir, subdir);
            }
        }
    }

    /* now that work has been pushed onto the queue, clean up this db */
    if (deep_enough(in, work) && db && rolledup) {
        refstr_t name = {
            .data = work->name,
            .len  = work->name_len,
        };

        char *err = NULL;
        if (sqlite3_exec(db, ROLLUP_CLEANUP, xattrs_rollup_cleanup, &name, &err) != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr, "Could not remove roll up data from \"%s\": %s\n",
                                      work->name, err);
            rc = 1;
        }
        err = NULL;
    }

    if (deep_enough(in, work)) {
        closedb(db);
    }

    closedir(dir);

  cleanup:
    free(work);

    return rc;
}

static int enqueue_subtree_roots(struct input *in, struct Unrollup *root,
                                 QPTPool_t *ctx, QPTPool_f func) {
    FILE *file = fopen(in->path_list.data, "r");
    if (!file) {
        const int err = errno;
        fprintf(stderr, "could not open directory list file \"%s\": %s (%d)\n",
                in->path_list.data, strerror(err), err);
        return 1;
    }

    char *line = NULL;
    size_t n = 0;
    ssize_t got = 0;
    while ((got = getline(&line, &n, file)) != -1) {
        /* remove trailing CRLF */
        const size_t len = trailing_non_match_index(line, got, "\r\n", 2);

        struct Unrollup *subtree_root = unrollup_create(root->name, root->name_len,
                                                        line, len,
                                                        in->min_level);

        if (subtree_root) {
            QPTPool_enqueue(ctx, 0, func, subtree_root);
        }
    }

    free(line);
    fclose(file);
    free(root);

    return 0;
}

static void sub_help(void) {
   printf("GUFI_tree        GUFI tree to unroll up\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_THREADS,

        /* tree walk flags */
        FLAG_MIN_LEVEL, FLAG_MAX_LEVEL, FLAG_PATH_LIST,

        FLAG_END
    };

    struct input in;
    process_args_and_maybe_exit(options, 1, "GUFI_tree ...", &in);

    const int root_count = argc - idx;

    QPTPool_t *pool = QPTPool_init(in.maxthreads, &in);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        input_fini(&in);
        return EXIT_FAILURE;
    }

    /* enqueue input paths */
    for(int i = idx; i < argc; i++) {
        size_t len = strlen(argv[i]);
        if (!len) {
            continue;
        }

        struct Unrollup *root = unrollup_create(argv[i], len, NULL, 0, 0);

        if (root) {
            if (doing_partial_walk(&in, root_count)) {
                enqueue_subtree_roots(&in, root, pool, processdir);
            }
            else {
                /* push the path onto the queue */
                QPTPool_enqueue(pool, i % in.maxthreads, processdir, root);
            }
        }
    }

    QPTPool_stop(pool);
    QPTPool_destroy(pool);
    input_fini(&in);

    return EXIT_SUCCESS;
}
