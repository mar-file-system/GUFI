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
#include <string.h>

#include "gufi_query/PoolArgs.h"
#include "gufi_query/external.h"

int PoolArgs_init(PoolArgs_t *pa, struct input *in, pthread_mutex_t *global_mutex) {
    /* Not checking arguments */

    memset(pa, 0, sizeof(*pa));
    pa->in = in;

    /* only STDOUT writes to the same destination */
    /* aggregate does not need mutex since aggregation is done serially */
    if (in->output == STDOUT) {
        pa->stdout_mutex = global_mutex;
    }

    /* catch this failure - it can be handled cleanly */
    pa->ta = calloc(in->maxthreads, sizeof(ThreadArgs_t));
    if (!pa->ta) {
        fprintf(stderr, "Error: Could not allocate %zu thread structures\n", in->maxthreads);
        return 1;
    }

    size_t i = 0;
    for(; i < in->maxthreads; i++) {
        ThreadArgs_t *ta = &pa->ta[i];

        /* only create per-thread db files when not aggregating and outputting to OUTDB */
        if (!in->sql.init_agg.len && (in->output == OUTDB)) {
            SNPRINTF(ta->dbname, MAXPATH, "%s.%zu", in->outname.data, i);
        }
        else {
            SNPRINTF(ta->dbname, MAXPATH, "file:memory%zu?mode=memory&cache=shared" GUFI_SQLITE_VFS_URI, i);
        }

        ta->outdb = opendb(ta->dbname, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 1, 1, NULL, NULL);
        if (!ta->outdb) {
            fprintf(stderr, "Error: Could not open per-thread database file \"%s\"\n", ta->dbname);
            break;
        }

        if (addqueryfuncs(ta->outdb) != 0) {
            fprintf(stderr, "Warning: Could not add functions to sqlite\n");
        }

        const int runvt_rc = sqlite3_runvt_init(ta->outdb, NULL, NULL);
        if (runvt_rc != SQLITE_OK) {
            fprintf(stderr, "Error: Could not initialize runvt: %s (%d)\n",
                    sqlite3_errstr(runvt_rc), runvt_rc);
            break;
        }

        /* create empty xattr tables to UNION to */
        char *err = NULL;
        if (sqlite3_exec(ta->outdb, XATTRS_TEMPLATE_CREATE,
                         NULL, NULL, &err) != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr, "Error: Could create xattr template \"%s\" on %s: %s\n", in->sql.init.data, ta->dbname, err);
            break;
        }

        /*
         * if xattr processing is not enabled, still need to create
         * xattrs view and variants, so create here to not repeat at
         * every directory
         */
        if (!in->process_xattrs) {
            size_t extdb_count = 0;

            setup_xattrs_views(in, ta->outdb,
                               NULL, &extdb_count);
        }

        /*
         * If there are no external databases to attach, create
         * external database views once here and skip in processdir.
         * Backing data will be swapped out at each directory.
         *
         * No need to drop.
         */
        if (sll_get_size(&in->external_attach) == 0) {
            if (create_extdb_views_noiter(ta->outdb) != 0) {
                break;
            }
        }

        /* run -I */
        if (in->sql.init.len) {
            if (sqlite3_exec(ta->outdb, in->sql.init.data, NULL, NULL, &err) != SQLITE_OK) {
                sqlite_print_err_and_free(err, stderr, "Error: Could not run SQL Init \"%s\" on %s: %s\n", in->sql.init.data, ta->dbname, err);
                break;
            }
        }

        ta->outfile = stdout;

        /* write to per-thread files during walk - aggregation is handled outside */
        if (in->output == OUTFILE) {
            if (!in->sql.init_agg.len) {
                char outname[MAXPATH];
                SNPRINTF(outname, MAXPATH, "%s.%zu", in->outname.data, i);
                ta->outfile = fopen(outname, "w");
                if (!ta->outfile) {
                    fprintf(stderr, "Error: Could not open output file \"%s\"\n", outname);
                    break;
                }
            }
        }

        if (!OutputBuffer_init(&ta->output_buffer, in->output_buffer_size)) {
            fprintf(stderr, "Error: Failed to initialize output buffer with size %zu\n",
                    in->output_buffer_size);
            break;
        }
    }

    if (i != in->maxthreads) {
        PoolArgs_fin(pa, i + 1);
        return 1;
    }

    /* cache this to not have to do repeated constructions of the same SQL statement */
    sqlite3_snprintf(sizeof(pa->detach), pa->detach, "DETACH %Q;", ATTACH_NAME);

    return 0;
}

void PoolArgs_fin(PoolArgs_t *pa, const size_t allocated) {
    for(size_t i = 0; i < allocated; i++) {
        ThreadArgs_t *ta = &pa->ta[i];

        closedb(ta->outdb);

        if (ta->outfile) {
            OutputBuffer_flush(&ta->output_buffer, ta->outfile);
        }
        OutputBuffer_destroy(&ta->output_buffer);

       if (ta->outfile && (ta->outfile != stdout)) {
            fclose(ta->outfile);
        }
    }

    free(pa->ta);
    pa->ta = NULL;

    input_fini(pa->in);
}
