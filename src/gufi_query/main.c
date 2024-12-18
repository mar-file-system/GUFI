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
#include <sys/types.h>
#include <unistd.h>

#include "OutputBuffers.h"
#include "QueuePerThreadPool.h"
#include "bf.h"
#include "compress.h"
#include "utils.h"

#include "gufi_query/aggregate.h"
#include "gufi_query/gqw.h"
#include "gufi_query/handle_sql.h"
#include "gufi_query/processdir.h"

static void sub_help(void) {
   printf("GUFI_index        find GUFI index here\n");
   printf("\n");
}

int main(int argc, char *argv[])
{
    /* process input args - all programs share the common 'struct input', */
    /* but allow different fields to be filled at the command-line. */
    /* Callers provide the options-string for get_opt(), which will */
    /* control which options are parsed for each program. */
    struct input in;
    process_args_and_maybe_exit("hHvT:S:E:an:jo:d:O:uI:F:y:z:J:K:G:mB:wxk:M:s:" COMPRESS_OPT "Q:", 1, "GUFI_index ...", &in);

    if (handle_sql(&in) != 0) {
        input_fini(&in);
        return EXIT_FAILURE;
    }

    /* mutex writing to stdout/stderr */
    pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;
    PoolArgs_t pa;
    if (PoolArgs_init(&pa, &in, &global_mutex) != 0) {
        return EXIT_FAILURE;
    }

    Aggregate_t aggregate;
    memset(&aggregate, 0, sizeof(aggregate));
    if (in.sql.init_agg.len) {
        if (!aggregate_init(&aggregate, &in)) {
            PoolArgs_fin(&pa, in.maxthreads);
            return EXIT_FAILURE;
        }
    }

    const uint64_t queue_limit = get_queue_limit(in.target_memory_footprint, in.maxthreads);
    QPTPool_t *pool = QPTPool_init_with_props(in.maxthreads, &pa, NULL, NULL, queue_limit, in.swap_prefix.data, 1, 2);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        aggregate_fin(&aggregate, &in);
        PoolArgs_fin(&pa, in.maxthreads);
        return EXIT_FAILURE;
    }

    /* enqueue input paths */
    char **realpaths = calloc(argc - idx, sizeof(char *));
    for(int i = idx; i < argc; i++) {
        size_t argvi_len = strlen(argv[i]);
        if (!argvi_len) {
            continue;
        }

        /* remove trailing slashes (+ 1 to keep at least 1 character) */
        argvi_len = trailing_non_match_index(argv[i] + 1, argvi_len - 1, "/", 1) + 1;

        realpaths[i - idx] = realpath(argv[i], NULL);
        char *root_name = realpaths[i - idx];
        if (!root_name) {
            const int err = errno;
            fprintf(stderr, "Could not get realpath of \"%s\": %s (%d)\n",
                    argv[i], strerror(err), err);
            continue;
        }

        struct stat st;
        if (lstat(root_name, &st) != 0) {
            const int err = errno;
            fprintf(stderr, "Could not stat directory \"%s\": %s (%d)\n",
                    argv[i], strerror(err), err);
            continue;
        }

        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr,"input-dir '%s' is not a directory\n",
                    argv[i]);
            continue;
        }

        const size_t root_name_len = strlen(root_name);

        /*
         * get sqlite3 name (copied into root instead of written
         * directly into root because root doesn't exist yet)
         */
        size_t rp_len = root_name_len;
        char sqlite3_name[MAXPATH];
        const size_t sqlite3_name_len = sqlite_uri_path(sqlite3_name, sizeof(sqlite3_name),
                                                        root_name, &rp_len);

        gqw_t *root = calloc(1, sizeof(*root) + root_name_len + 1 + sqlite3_name_len + 1);
        root->work.name = (char *) &root[1];;
        root->work.name_len = SNFORMAT_S(root->work.name, root_name_len + 1, 1,
                                         root_name, root_name_len);

        /* keep original user input */
        root->work.orig_root.data = argv[i];
        root->work.orig_root.len = argvi_len;

        /* set modified path name for SQLite3 */
        root->sqlite3_name = ((char *) root->work.name) + root->work.name_len + 1;
        root->sqlite3_name_len = SNFORMAT_S(root->sqlite3_name, sqlite3_name_len + 1, 1,
                                            sqlite3_name, sqlite3_name_len);

        /* parent of input path */
        root->work.root_parent.data = realpaths[i - idx];
        root->work.root_parent.len  = trailing_match_index(root->work.root_parent.data, root->work.name_len, "/", 1);

        ((char *) root->work.root_parent.data)[root->work.root_parent.len] = '\0';
        root->work.basename_len = root->work.name_len - root->work.root_parent.len;

        root->work.root_basename_len = root->work.basename_len;

        /* push the path onto the queue (no compression) */
        QPTPool_enqueue(pool, i % in.maxthreads, processdir, root);
    }

    QPTPool_stop(pool);
    QPTPool_destroy(pool);

    for(int i = idx; i < argc; i++) {
        free(realpaths[i - idx]);
    }
    free(realpaths);

    if (in.sql.init_agg.len) {
        /* aggregate the intermediate results */
        aggregate_intermediate(&aggregate, &pa, &in);
        aggregate_process(&aggregate, &in);
        aggregate_fin(&aggregate, &in);
    }

    for(size_t i = 0; i < in.maxthreads; i++) {
        ThreadArgs_t *ta = &(pa.ta[i]);

        /* clear out buffered data */
        OutputBuffer_flush(&ta->output_buffer, ta->outfile);
    }

    /* clean up globals */
    PoolArgs_fin(&pa, in.maxthreads);

    return EXIT_SUCCESS;
}
