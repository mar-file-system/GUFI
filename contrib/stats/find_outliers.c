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
#include <stdlib.h>
#include <string.h>

/* GUFI headers */
#include "OutputBuffers.h"
#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "bf.h"
#include "dbutils.h"
#include "debug.h"
#include "print.h"
#include "str.h"
#include "trie.h"
#include "utils.h"

/* stats headers */
#include "DirData.h"
#include "handle_columns.h"

struct PoolArgs {
    struct input in;
    struct OutputBuffers obufs;
};

typedef struct OutlierWork {
    str_t path;
    refstr_t col;
    ColHandler_t *handler; /* functions for handling the column type */
    refstr_t query;        /* SQL selecting 1 column from summary table; reference to allocation in main */
    int is_outlier;
} OutlierWork_t;

static OutlierWork_t *OutlierWork_create(const str_t *path,
                                         const refstr_t col,
                                         ColHandler_t *handler, refstr_t query,
                                         const int is_outlier) {
    OutlierWork_t *ow = calloc(1, sizeof(OutlierWork_t));
    ow->path.data = malloc(path->len + 1);
    ow->path.len = SNFORMAT_S(ow->path.data, path->len + 1, 1,
                              path->data, path->len);

    ow->col = col;
    ow->handler = handler;
    ow->query = query;
    ow->is_outlier = is_outlier;

    return ow;
}

static void OutlierWork_free(OutlierWork_t *ow) {
    str_free_existing(&ow->path);
    free(ow);
}

static int is_subdir(const char *path, struct dirent *entry) {
    switch (entry->d_type) {
        case DT_DIR:
            return 1;
            break;
        case DT_LNK:
        case DT_REG:
        case DT_FIFO:
        case DT_SOCK:
        case DT_CHR:
        case DT_BLK:
            return 0;
            break;
        case DT_UNKNOWN:
        default:
            ;
            /* some filesystems don't support d_type - fall back to calling lstat */
            #if HAVE_STATX
            struct statx stx;
            if (statx(AT_FDCWD, path,
                      AT_SYMLINK_NOFOLLOW | AT_STATX_DONT_SYNC,
                      STATX_ALL, &stx) != 0) {
                const int err = errno;
                fprintf(stderr, "Error: Could not statx \"%s\": %s (%d)\n",
                        path, strerror(err), err);
                return -1;
            }

            return (S_ISDIR(stx.stx_mode));
            #else
            struct stat st;
            if (lstat(path, &st) != 0) {
                const int err = errno;
                fprintf(stderr, "Error: Could not lstat \"%s\": %s (%d)\n",
                        path, strerror(err), err);
                return -1;
            }

            return (S_ISDIR(st.st_mode));
            #endif
            break;
    }

    return 0; /* shoud never get here */
}

/* go down one level */
static void get_subdirs(OutlierWork_t *ow, sll_t *subdirs) {
    DIR *dir = opendir(ow->path.data);
    if (!dir) {
        const int err = errno;
        fprintf(stderr, "Error: Could not open directory \"%s\": %s (%d)\n", ow->path.data, strerror(err), err);
        return;
    }

    sll_init(subdirs);

    struct dirent *entry = NULL;
    while ((entry = readdir(dir))) {
        const size_t len = strlen(entry->d_name);

        const int skip = (
            /* skip . and .. ; not using skip_names */
            ((len == 1) && (strncmp(entry->d_name, ".",  1) == 0)) ||
            ((len == 2) && (strncmp(entry->d_name, "..", 2) == 0)) ||
            /* skip *.db */
            ((len >= 3) && (strncmp(entry->d_name + len - 3, ".db", 3) == 0))
            );

        if (skip) {
            continue;
        }

        struct DirData *dd = DirData_create(&ow->path, entry->d_name, len);
        if (is_subdir(dd->path.data, entry) != 1) {
            DirData_free(dd);
            continue;
        }

        /* create the db.db path */
        const size_t dbname_len = dd->path.len + 1 + DBNAME_LEN;
        char *dbname = malloc(dbname_len + 1);
        SNFORMAT_S(dbname, dbname_len + 1, 3,
                   dd->path.data, dd->path.len,
                   "/", (size_t) 1,
                   DBNAME, DBNAME_LEN);

        /* the db must exist or else it is not included */
        sqlite3 *db = opendb(dbname, SQLITE_OPEN_READONLY, 0, 0, NULL, NULL);
        if (!db) {
            free(dbname);
            DirData_free(dd);
            continue;
        }

        /* get data from db */
        char *err = NULL;
        if (sqlite3_exec(db, ow->query.data, ow->handler->sqlite_callback, dd, &err) == SQLITE_OK) {

            /* save this subdir */
            sll_push(subdirs, dd);
        }
        else {
            fprintf(stderr, "Error: Could not get column from \"%s\": %s\n", dbname, err);
            sqlite3_free(err);
            DirData_free(dd);
        }

        free(dbname);

        closedb(db);
    }

    closedir(dir);
}

/*
 * reasons to arrive at current node
 *     root - no choice / current level had no outliers
 *     contains an outlier
 *     current level had no outliers
 *
 * subdirectories
 *     0
 *     some outliers
 *     no outliers
 */
static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    struct PoolArgs *pa = (struct PoolArgs *) args;
    OutlierWork_t *ow = (OutlierWork_t *) data;

    int rc = 0;

    struct PrintArgs print = {
        .output_buffer = &pa->obufs.buffers[id],
        .delim = pa->in.delim,
        .mutex = pa->obufs.mutex,
        .outfile = stdout,
        .types = NULL,
        .suppress_newline = 0,
    };

    sll_t subdirs;
    get_subdirs(ow, &subdirs);

    const uint64_t n = sll_get_size(&subdirs);

    /* nothing under here */
    if (n == 0) {
        /* if this directory was an outlier, print this directory */
        if (ow->is_outlier) {
            char *out[] = {ow->path.data, (char *) ow->col.data};
            print_parallel(&print, 2, out, NULL);
        }
        sll_destroy(&subdirs, NULL);
    }
    else {
        sll_t *work = NULL;

        sll_t outliers;
        sll_init(&outliers);

        if (n == 1) {
            /* not enough samples to get standard deviation, so just go down a level */
            work = &subdirs;
        }
        else {
            /* compute mean and standard deviation */
            double mean = 0, stdev = 0;
            ow->handler->compute_mean_stdev(&subdirs, &mean, &stdev);

            /* get 3 sigma values */
            const double limit    = 3 * stdev;
            const double low      = mean - limit;
            const double high     = mean + limit;

            /* find outliers */
            sll_loop(&subdirs, node) {
                DirData_t *dd = (DirData_t *) sll_node_data(node);

                if ((dd->tot < low) || (high < dd->tot)) {
                    sll_push(&outliers, dd);
                }
            }

            /* if there is at least one outlier, process only outliers */
            work = (sll_get_size(&outliers) == 0)?&subdirs:&outliers;
        }

        const int sub_outlier = (work == &outliers); /* all outliers or all not outliers */

        /* current directory is an outlier but there are no subdirectory outliers */
        if (ow->is_outlier && !sub_outlier) {
            char *out[] = {ow->path.data, (char *) ow->col.data};
            print_parallel(&print, 2, out, NULL);
            /* TODO: keep processing subdirectories? */
        }
        /* current directory is NOT an outlier + descend with subdirs/outliers */
        else {
            sll_loop(work, node) {
                DirData_t *dd = (DirData_t *) sll_node_data(node);
                OutlierWork_t *new_ow = OutlierWork_create(&dd->path, ow->col,
                                                           ow->handler, ow->query,
                                                           sub_outlier);
                QPTPool_enqueue(ctx, id, processdir, new_ow);
            }
        }

        sll_destroy(&outliers, NULL); /* references to subdirs, so don't clean up */
        sll_destroy(&subdirs, DirData_free);
    }

    OutlierWork_free(ow);

    return rc;
}

static void sub_help(void) {
   printf("input_dir         walk one or more trees to find outliers\n");
   printf("column            column to look at\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    struct PoolArgs pa;
    process_args_and_maybe_exit("hHvn:d:B:", 2, "input_dir column ...", &pa.in);

    int rc = EXIT_SUCCESS;

    if ((argc - idx) & 1) {
        fprintf(stderr, "Error: Input should be pairs of paths and columns\n");
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    if (!OutputBuffers_init(&pa.obufs, pa.in.maxthreads, pa.in.output_buffer_size, &mutex)) {
        fprintf(stderr, "Error: Could not initialize %zu output buffers\n", pa.in.maxthreads);
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    QPTPool_t *pool = QPTPool_init(pa.in.maxthreads, &pa);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        rc = EXIT_FAILURE;
        goto cleanup_outputbuffers;
    }

    /* set up known column names to function mappings */
    trie_t *col_funcs = setup_column_functions();

    const size_t pairs = (argc - idx) / 2;
    str_t *queries = calloc(sizeof(*queries), pairs);

    struct start_end after_init;
    clock_gettime(CLOCK_MONOTONIC, &after_init.start);

    for(size_t i = 0; i < pairs; i++) {
        const size_t j = 2 * i + idx;
        const char *path = argv[j];
        const char *col = argv[j + 1];
        const size_t col_len = strlen(col);

        /* find column type handler */
        ColHandler_t *handler = NULL;
        if (trie_search(col_funcs, col, col_len, (void **) &handler) != 1) {
            fprintf(stderr, "Error: Could not find handler for column %s\n", col);
            continue;
        }

        /* this is actually a reference */
        str_t p = {
            .data = (char *) path,
            .len = strlen(path),
        };

        refstr_t c = {
            .data = col,
            .len = col_len,
        };

        str_t *query = &queries[i];
        handler->gen_sql(query, col);
        char *data = realloc(query->data, query->len + 1);
        if (data) {
            query->data = data;
        }

        refstr_t q = {
            .data = query->data,
            .len  = query->len,
        };

        OutlierWork_t *ow = OutlierWork_create(&p, c, handler, q,
                                               0); /*
                                                    * starting path cannot be an outlier because
                                                    * there is only one directory at that level
                                                    * (even if the path is not root and has
                                                    * siblings, we do not know that)
                                                    */
        QPTPool_enqueue(pool, i, processdir, ow);
    }

    QPTPool_stop(pool);

    OutputBuffers_flush_to_single(&pa.obufs, stdout);

    clock_gettime(CLOCK_MONOTONIC, &after_init.end);
    const long double processtime = sec(nsec(&after_init));

    fprintf(stdout, "Time Spent Processing: %.2Lfs\n", processtime);

    for(size_t i = 0; i < pairs; i++) {
        str_free_existing(&queries[i]);
    }
    free(queries);
    trie_free(col_funcs);

    QPTPool_destroy(pool);

  cleanup_outputbuffers:
    OutputBuffers_destroy(&pa.obufs);

  cleanup:
    input_fini(&pa.in);

    return rc;
}
