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
#if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
#include "debug.h"
#endif
#include "utils.h"

#include "gufi_query/aggregate.h"
#include "gufi_query/gqw.h"
#include "gufi_query/processdir.h"
#include "gufi_query/timers.h"
#include "gufi_query/validate_inputs.h"

#if defined(DEBUG) && defined(CUMULATIVE_TIMES)
#define print_stats(normal_fmt, terse_fmt, ...)         \
    if (in.terse) {                                     \
        fprintf(stderr, terse_fmt " ", __VA_ARGS__);    \
    }                                                   \
    else {                                              \
        fprintf(stderr, normal_fmt "\n", __VA_ARGS__);  \
    }
#endif

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
    process_args_and_maybe_exit("hHvT:S:E:an:jo:d:O:I:F:y:z:J:K:G:mB:wxk:M:s:" COMPRESS_OPT "Q:", 1, "GUFI_index ...", &in);

    if (validate_inputs(&in) != 0) {
        input_fini(&in);
        return EXIT_FAILURE;
    }

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamp_create_start(setup_globals);
    #endif

    /* mutex writing to stdout/stderr */
    pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;
    PoolArgs_t pa;
    if (PoolArgs_init(&pa, &in, &global_mutex) != 0) {
        return EXIT_FAILURE;
    }

    #ifdef DEBUG
    #ifdef PER_THREAD_STATS
    timestamp_print_init(timestamp_buffers, in.maxthreads, 1073741824ULL, &global_mutex);
    #endif

    #ifdef CUMULATIVE_TIMES
    timestamp_set_end(setup_globals);
    const uint64_t setup_globals_time = timestamp_elapsed(setup_globals);
    #endif
    #endif

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamp_create_start(setup_aggregate);
    #endif

    Aggregate_t aggregate;
    memset(&aggregate, 0, sizeof(aggregate));
    if (in.sql.init_agg.len) {
        if (!aggregate_init(&aggregate, &in)) {
            PoolArgs_fin(&pa, in.maxthreads);
            return EXIT_FAILURE;
        }
    }

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamp_set_end(setup_aggregate);
    const uint64_t setup_aggregate_time = timestamp_elapsed(setup_aggregate);
    #endif

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    uint64_t total_time = 0;

    timestamp_create_start(work);
    #endif

    const uint64_t queue_limit = get_queue_limit(in.target_memory_footprint, in.maxthreads);
    QPTPool_t *pool = QPTPool_init_with_props(in.maxthreads, &pa, NULL, NULL, queue_limit, in.swap_prefix.data, 1, 2
                                              #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                              , timestamp_buffers
                                              #endif
        );
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

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    const size_t thread_count = QPTPool_threads_completed(pool);
    #endif

    QPTPool_destroy(pool);

    for(int i = idx; i < argc; i++) {
        free(realpaths[i - idx]);
    }
    free(realpaths);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamp_set_end(work);

    const uint64_t work_time = timestamp_elapsed(work);
    total_time += work_time;
    #endif

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    uint64_t aggregate_time = 0;
    uint64_t output_time = 0;
    size_t rows = 0;
    #endif

    if (in.sql.init_agg.len) {
        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        timestamp_create_start(aggregation);
        #endif

        /* aggregate the intermediate results */
        aggregate_intermediate(&aggregate, &pa, &in);

        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        timestamp_set_end(aggregation);
        aggregate_time = timestamp_elapsed(aggregation);
        total_time += aggregate_time;
        #endif

        /* final query on aggregate results */
        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        timestamp_create_start(output);

        rows +=
        #endif

        aggregate_process(&aggregate, &in);

        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        timestamp_set_end(output);
        output_time = timestamp_elapsed(output);
        total_time += output_time;
        #endif

        aggregate_fin(&aggregate, &in);
    }

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamp_create_start(cleanup_globals);

    size_t query_count = 0;
    #endif

    for(size_t i = 0; i < in.maxthreads; i++) {
        ThreadArgs_t *ta = &(pa.ta[i]);

        /* clear out buffered data */
        OutputBuffer_flush(&ta->output_buffer, ta->outfile);

        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        /* aggregate per thread query counts */
        query_count += ta->queries;

        /* aggregate output row count */
        rows += ta->output_buffer.count;
        #endif
    }

    /*
     * PoolArgs_fin does not deallocate the total_times
     * struct, but reading from pa would look weird
     * after PoolArgs_fin was called.
     */
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    total_time_t *tt = &pa.tt;
    #endif

    /* clean up globals */
    PoolArgs_fin(&pa, in.maxthreads);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamp_set_end(cleanup_globals);
    const uint64_t cleanup_globals_time = timestamp_elapsed(cleanup_globals);
    total_time += cleanup_globals_time;

    const long double total_time_sec = sec(total_time);
    #endif

    #ifdef DEBUG
    timestamp_print_destroy(timestamp_buffers);
    #endif

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    const uint64_t thread_time = total_time_sum(tt);

    print_stats("set up globals:                             %.2Lfs", "%Lf", sec(setup_globals_time));
    print_stats("set up intermediate databases:              %.2Lfs", "%Lf", sec(setup_aggregate_time));
    print_stats("thread pool:                                %.2Lfs", "%Lf", sec(work_time));
    print_stats("    open directories:                       %.2Lfs", "%Lf", sec(tt->opendir));
    print_stats("    attach index:                           %.2Lfs", "%Lf", sec(tt->attachdb));
    print_stats("    xattrprep:                              %.2Lfs", "%Lf", sec(tt->xattrprep));
    print_stats("    addqueryfuncs:                          %.2Lfs", "%Lf", sec(tt->addqueryfuncs));
    print_stats("    get_rollupscore:                        %.2Lfs", "%Lf", sec(tt->get_rollupscore));
    print_stats("    descend:                                %.2Lfs", "%Lf", sec(tt->descend));
    print_stats("        check args:                         %.2Lfs", "%Lf", sec(tt->check_args));
    print_stats("        check level:                        %.2Lfs", "%Lf", sec(tt->level));
    print_stats("        check level <= max_level branch:    %.2Lfs", "%Lf", sec(tt->level_branch));
    print_stats("        while true:                         %.2Lfs", "%Lf", sec(tt->while_branch));
    print_stats("            readdir:                        %.2Lfs", "%Lf", sec(tt->readdir));
    print_stats("            readdir != null branch:         %.2Lfs", "%Lf", sec(tt->readdir_branch));
    print_stats("            strncmp:                        %.2Lfs", "%Lf", sec(tt->strncmp));
    print_stats("            strncmp != . or ..:             %.2Lfs", "%Lf", sec(tt->strncmp_branch));
    print_stats("            snprintf:                       %.2Lfs", "%Lf", sec(tt->snprintf));
    print_stats("            isdir:                          %.2Lfs", "%Lf", sec(tt->isdir));
    print_stats("            isdir branch:                   %.2Lfs", "%Lf", sec(tt->isdir_branch));
    print_stats("            access:                         %.2Lfs", "%Lf", sec(tt->access));
    print_stats("            set:                            %.2Lfs", "%Lf", sec(tt->set));
    print_stats("            clone:                          %.2Lfs", "%Lf", sec(tt->clone));
    print_stats("            pushdir:                        %.2Lfs", "%Lf", sec(tt->pushdir));
    print_stats("    check if treesummary table exists:      %.2Lfs", "%Lf", sec(tt->sqltsumcheck));
    print_stats("    sqltsum:                                %.2Lfs", "%Lf", sec(tt->sqltsum));
    print_stats("    sqlsum:                                 %.2Lfs", "%Lf", sec(tt->sqlsum));
    print_stats("    sqlent:                                 %.2Lfs", "%Lf", sec(tt->sqlent));
    print_stats("    xattrdone:                              %.2Lfs", "%Lf", sec(tt->xattrdone));
    print_stats("    detach index:                           %.2Lfs", "%Lf", sec(tt->detachdb));
    print_stats("    close directories:                      %.2Lfs", "%Lf", sec(tt->closedir));
    print_stats("    restore timestamps:                     %.2Lfs", "%Lf", sec(tt->utime));
    print_stats("    free work:                              %.2Lfs", "%Lf", sec(tt->free_work));
    print_stats("    output timestamps:                      %.2Lfs", "%Lf", sec(tt->output_timestamps));
    print_stats("aggregate into final databases:             %.2Lfs", "%Lf", sec(aggregate_time));
    print_stats("print aggregated results:                   %.2Lfs", "%Lf", sec(output_time));
    print_stats("clean up globals:                           %.2Lfs", "%Lf", sec(cleanup_globals_time));
    if (!in.terse) {
        fprintf(stderr, "\n");
    }
    print_stats("Threads run:                                %zu",    "%zu", thread_count);
    print_stats("Queries performed:                          %zu",    "%zu", query_count);
    print_stats("Rows printed to stdout or outfiles:         %zu",    "%zu", rows);
    print_stats("Total Thread Time (not including main):     %.2Lfs", "%Lf", sec(thread_time));
    print_stats("Real time (main):                           %.2Lfs", "%Lf", total_time_sec);
    if (in.terse) {
        fprintf(stderr, "\n");
    }
    #endif

    return EXIT_SUCCESS;
}
