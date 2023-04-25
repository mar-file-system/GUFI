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



#include <stdlib.h>

#include "gufi_query/PoolArgs.h"

int PoolArgs_init(PoolArgs_t *pa, struct input *in, pthread_mutex_t *global_mutex) {
    /* Not checking arguments */

    memset(pa, 0, sizeof(*pa));
    pa->in = in;

    if (setup_directory_skip(in->skip, &pa->skip)) {
        fprintf(stderr, "Error: Bad input skip list\n");
        return 1;
    }

    /* only STDOUT writes to the same destination */
    /* aggregate does not need mutex since aggregation is done serially */
    if (in->output == STDOUT) {
        pa->stdout_mutex = global_mutex;
    }

    pa->ta = calloc(in->maxthreads, sizeof(ThreadArgs_t));

    size_t i = 0;
    for(; i < (size_t) in->maxthreads; i++) {
        ThreadArgs_t *ta = &pa->ta[i];

        /* only create per-thread db files when not aggregating and outputting to OUTDB */
        if (!in->sql.init_agg_len && (in->output == OUTDB)) {
            SNPRINTF(ta->dbname, MAXPATH, "%s.%zu", in->outname, i);
        }
        else {
            SNPRINTF(ta->dbname, MAXPATH, "file:memory%zu?mode=memory&cache=shared" GUFI_SQLITE_VFS_URI, i);
        }

        ta->outdb = opendb(ta->dbname, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 1, 1, NULL, NULL
                           #if defined(DEBUG) && defined(PER_THREAD_STATS)
                           , NULL, NULL
                           , NULL, NULL
                           #endif
            );

        if (!ta->outdb) {
            fprintf(stderr, "Error: Could not open in-memory database file\n");
            break;
        }

        #ifdef ADDQUERYFUNCS
        if (addqueryfuncs_common(ta->outdb) != 0) {
            fprintf(stderr, "Warning: Could not add functions to sqlite\n");
        }
        #endif

        /* run -I */
        if (in->sql.init_len) {
            if (sqlite3_exec(ta->outdb, in->sql.init, NULL, NULL, NULL) != SQLITE_OK) {
                fprintf(stderr, "Error: Could not run SQL Init \"%s\" on %s\n", in->sql.init, ta->dbname);
                break;
            }
        }

        ta->outfile = stdout;

        /* write to per-thread files during walk - aggregation is handled outside */
        if (in->output == OUTFILE) {
            if (!in->sql.init_agg_len) {
                char outname[MAXPATH];
                SNPRINTF(outname, MAXPATH, "%s.%zu", in->outname, i);
                ta->outfile = fopen(outname, "w");
                if (!ta->outfile) {
                    fprintf(stderr, "Error: Could not open output file");
                    break;
                }
            }
        }

        if (!OutputBuffer_init(&ta->output_buffer, in->output_buffer_size)) {
            fprintf(stderr, "Error: Failed to initialize output buffer with size %zu\n", in->output_buffer_size);
            break;
        }
    }

    if (i != (size_t) in->maxthreads) {
        PoolArgs_fin(pa, i + 1);
        return 1;
    }

    #if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
    clock_gettime(CLOCK_MONOTONIC, &pa->start_time);
    epoch = since_epoch(&pa->start_time); /* debug.h */
    #endif

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    total_time_init(&pa->tt);
    #endif

    return 0;
}

void PoolArgs_fin(PoolArgs_t *pa, const size_t allocated) {
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    total_time_destroy(&pa->tt);
    #endif

    for(size_t i = 0; i < allocated; i++) {
        ThreadArgs_t *ta = &pa->ta[i];

        closedb(ta->outdb);

        OutputBuffer_flush(&ta->output_buffer, ta->outfile);
        OutputBuffer_destroy(&ta->output_buffer);

       if (ta->outfile && (ta->outfile != stdout)) {
            fclose(ta->outfile);
        }
    }

    trie_free(pa->skip);
    free(pa->ta);
}
