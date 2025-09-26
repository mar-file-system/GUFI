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
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "bf.h"
#include "debug.h"
#include "str.h"
#include "trie.h"

#include "gufi_find_outliers/OutlierWork.h"
#include "gufi_find_outliers/PoolArgs.h"
#include "gufi_find_outliers/handle_columns.h"
#include "gufi_find_outliers/handle_db.h"
#include "gufi_find_outliers/processdir.h"

static void sub_help(void) {
   printf("input_dir         walk this tree to find directories with outliers\n");
   printf("data              column or group (T_ONLY, MINS, MAXS, MINMAXS, TOTS, TIMES, ALL) to look at\n");
   printf("\n");
   printf("Make sure every directory under the starting path has a db.db file.\n");
   printf("Make sure treesummary tables have been generated for every directory.\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    const struct option flags[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_THREADS, FLAG_DELIM, FLAG_OUTPUT_DB, FLAG_END
    };
    struct PoolArgs pa;
    process_args_and_maybe_exit(flags, 2, "input_dir data ...", &pa.in);

    int rc = EXIT_SUCCESS;

    if ((argc - idx) & 1) {
        fprintf(stderr, "Error: Input should be pairs of paths and columns\n");
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    if (intermediate_dbs_init(&pa) != 0) {
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    QPTPool_t *pool = QPTPool_init(pa.in.maxthreads, &pa);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        rc = EXIT_FAILURE;
        goto cleanup_dbs;
    }

    pa.opendbs = calloc(pa.in.maxthreads, sizeof(size_t));

    /* set up known column names to function mappings */
    trie_t *col_funcs = setup_column_functions();

    const size_t pairs = (argc - idx) / 2;

    sll_t queries;
    sll_init(&queries);

    Stats_t root_stats = {
        .value   = NAN,
        .mean    = NAN,
        .stdev   = NAN,
    };

    struct start_end after_init;
    clock_gettime(CLOCK_MONOTONIC, &after_init.start);

    for(size_t i = 0; i < pairs; i++) {
        const size_t j = 2 * i + idx;
        const char *path = argv[j];
        const char *user_col = argv[j + 1];
        const size_t user_col_len = strlen(user_col);

        struct stat st;
        if (stat(path, &st) != 0) {
            const int err = errno;
            fprintf(stderr, "Error: Could not stat root directory \"%s\": %s (%d)\n",
                    path, strerror(err), err);
            continue;
        }

        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "Error: \"%s\" is not a directory. Skipping\n", path);
            continue;
        }

        const refstr_t user_cols[] = {
            { user_col,  user_col_len, },
            { NULL,      0,            },
        };

        /* check if processing multiple columns */
        const refstr_t *groups = handle_group(user_col, user_col_len);
        if (!groups) {
            groups = user_cols;
        }

        size_t g = 0;
        while (groups[g].data) {
            const refstr_t c = groups[g++];

            /* find column type handler */
            ColHandler_t *handler = NULL;
            if (trie_search(col_funcs, c.data, c.len, (void **) &handler) != 1) {
                fprintf(stderr, "Error: Could not find handler for column %s\n", c.data);
                continue;
            }

            /* this is actually a reference */
            str_t p = {
                .data = (char *) path,
                .len = strlen(path),
            };

            str_t *query = calloc(1, sizeof(*query));
            sll_push(&queries, query);

            handler->gen_sql(query, c.data);

            OutlierWork_t *ow = OutlierWork_create(&p, 0, c, handler, query,
                                                   0,  /*
                                                        * starting path cannot be an outlier because
                                                        * there is only one directory at that level
                                                        * (even if the path is not root and has
                                                        * siblings, we do not know that)
                                                        */
                                                   &root_stats, &root_stats);
            QPTPool_enqueue(pool, i % pa.in.maxthreads, processdir, ow);
        }
    }

    QPTPool_stop(pool);

    /* write results to stdout or a single database file */
    size_t rows = 0;
    write_results(&pa, &rows);

    clock_gettime(CLOCK_MONOTONIC, &after_init.end);
    const long double processtime = sec(nsec(&after_init));

    const uint64_t thread_count = QPTPool_threads_completed(pool);

    size_t opendbs = 0;
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        opendbs += pa.opendbs[i];
    }

    fprintf(stderr, "Ran %" PRIu64 " threads\n", thread_count);
    fprintf(stderr, "Ran opendb %zu times\n", opendbs);
    fprintf(stderr, "Found %zu outliers\n", rows);
    fprintf(stderr, "Time Spent Processing: %.2Lfs\n", processtime);

    /* cleanup */
    sll_destroy(&queries, (void (*)(void *)) str_free);
    free(pa.opendbs);
    trie_free(col_funcs);
    QPTPool_destroy(pool);

  cleanup_dbs:
    intermediate_dbs_fini(&pa);

  cleanup:
    input_fini(&pa.in);

    dump_memory_usage();

    return rc;
}
