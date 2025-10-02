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
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "dbutils.h"
#include "external.h"
#include "template_db.h"
#include "trace.h"
#include "utils.h"

/* global to pool - passed around in "args" argument */
struct PoolArgs {
    struct input in;
    struct template_db db;
    struct template_db xattr;
};

/* process the work under one directory (no recursion) */
static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    /* Not checking arguments */

    (void) ctx; (void) id;

    struct PoolArgs *pa = (struct PoolArgs *) args;
    struct input *in = &pa->in;
    struct row *w = (struct row *) data;
    const int trace = w->trace;

    // TODO: based on following comment, can this be stack allocated?
    struct work *dir = NULL; /* name and name_len are not used */
    struct entry_data ed;

    /* parse the directory data */
    linetowork(w->line, w->len, in->delim, &dir, &ed);

    /* create the directory */
    const size_t topath_len = in->nameto.len + 1 + w->first_delim;

    /*
     * allocate space for "/db.db" in topath
     *
     * extra buffer is not needed and save on memcpy-ing
     */
    const size_t topath_size = topath_len + 1 + DBNAME_LEN + 1;
    char *topath = malloc(topath_size);
    SNFORMAT_S(topath, topath_size, 4,
               in->nameto.data, in->nameto.len,
               "/", (size_t) 1,
               w->line, w->first_delim,
               "\0" DBNAME, (size_t) 1 + DBNAME_LEN);

    /* have to dupdir here because directories can show up in any order */
    if (dupdir(topath, &dir->statuso)) {
        const int err = errno;
        fprintf(stderr, "Dupdir failure: \"%s\": %s (%d)\n",
                topath, strerror(err), err);
        free(topath);
        row_destroy(&w);
        return 1;
    }

    /* restore "/db.db" (no need to remove afterwards) */
    topath[topath_len] = '/';

    sqlite3 *db = template_to_db(&pa->db, topath, dir->statuso.st_uid, dir->statuso.st_gid);

    if (db) {
        struct sum summary;
        zeroit(&summary);

        sll_t xattr_db_list;
        sll_init(&xattr_db_list);

        /* INSERT statement bindings into db.db */
        sqlite3_stmt *entries_res     = insertdbprep(db, ENTRIES_INSERT);           /* entries */
        sqlite3_stmt *xattrs_res      = insertdbprep(db, XATTRS_PWD_INSERT);        /* xattrs within db.db */
        sqlite3_stmt *xattr_files_res = insertdbprep(db, EXTERNAL_DBS_PWD_INSERT);  /* per-user and per-group db file names */

        startdb(db);

        size_t row_count = 0;
        char *line = NULL;
        size_t size = 0;
        for(size_t i = 0; i < w->entries; i++) {
            const ssize_t len = getline_fd(&line, &size, trace, &w->offset, GETLINE_DEFAULT_SIZE);
            if (len < 1) {
                break;
            }

            struct work *row;
            struct entry_data row_ed;

            linetowork(line, len, in->delim, &row, &row_ed);

            if (row_ed.type == 'e') {
                /* insert right here (instead of bulk inserting) since this is likely to be very rare */
                external_insert(db, EXTERNAL_TYPE_USER_DB_NAME, row->statuso.st_ino, row->name);
            }
            else {
                /* update summary table */
                sumit(&summary, row, &row_ed);

                /* don't record pinode */
                row->pinode = 0;

                /* add row to bulk insert */
                insertdbgo(row, &row_ed, entries_res);
                insertdbgo_xattrs(in, &row->statuso, row, &row_ed,
                                  &xattr_db_list, &pa->xattr,
                                  topath, topath_len,
                                  xattrs_res, xattr_files_res);

                xattrs_cleanup(&row_ed.xattrs);

                row_count++;
                if (row_count > 100000) {
                    stopdb(db);
                    startdb(db);
                    row_count = 0;
                }
            }

            free(row);
        }

        free(line); /* reuse line and only alloc+free once */

        stopdb(db);

        /* write out per-user and per-group xattrs */
        sll_destroy(&xattr_db_list, destroy_xattr_db);

        /* write out the current directory's xattrs */
        insertdbgo_xattrs_avail(dir, &ed, xattrs_res);

        /* write out data going into db.db */
        insertdbfin(xattr_files_res); /* per-user and per-group xattr db file names */
        insertdbfin(xattrs_res);
        insertdbfin(entries_res);

        /* find the basename of this path */
        w->line[w->first_delim] = '\x00';
        const size_t basename_start = trailing_match_index(w->line, w->first_delim, "/", 1);

        insertsumdb(db, w->line + basename_start, dir, &ed, &summary);
        xattrs_cleanup(&ed.xattrs);

        closedb(db); /* don't set to nullptr */
    }

    free(topath);
    row_destroy(&w);
    free(dir);

    return !db;
}

static void sub_help(void) {
   printf("trace_file...     parse one or more trace files to produce the GUFI tree\n");
   printf("output_dir        build GUFI tree here\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    /* have to call clock_gettime explicitly to get start time and epoch */
    struct start_end main_func;
    clock_gettime(CLOCK_MONOTONIC, &main_func.start);
    epoch = since_epoch(&main_func.start);

    struct PoolArgs pa;
    process_args_and_maybe_exit("hHvn:d:M:s:", 2, "trace_file... output_dir", &pa.in);

    /* parse positional args, following the options */
    INSTALL_STR(&pa.in.nameto, argv[argc - 1]);

    /* open trace files for threads to jump around in */
    /* open the trace files here to not repeatedly open in threads */
    const size_t trace_count = argc - idx - 1;
    int *traces = open_traces(&argv[idx], trace_count);
    if (!traces) {
        fprintf(stderr, "Failed to open trace file for each thread\n");
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    int rc = EXIT_SUCCESS;

    init_template_db(&pa.db);
    if (create_dbdb_template(&pa.db) != 0) {
        fprintf(stderr, "Could not create template file\n");
        rc = EXIT_FAILURE;
        goto free_traces;
    }

    struct stat st;
    st.st_mode = S_IRWXU | S_IRWXG | S_IRWXO;
    st.st_uid = geteuid();
    st.st_gid = getegid();

    if (dupdir(pa.in.nameto.data, &st)) {
        fprintf(stderr, "Could not create directory %s\n", pa.in.nameto.data);
        rc = EXIT_FAILURE;
        goto free_traces;
    }

    /*
     * create empty db.db in index parent (this file is placed in
     * "${dst}/db.db"; index is placed in "${dst}/$(basename ${src}))"
     * so that when querying "${dst}", no error is printed
     */
    if (create_empty_dbdb(&pa.db, &pa.in.nameto, geteuid(), getegid()) != 0) {
        rc = EXIT_FAILURE;
        goto free_traces;
    }

    init_template_db(&pa.xattr);
    if (create_xattrs_template(&pa.xattr) != 0) {
        fprintf(stderr, "Could not create xattr template file\n");
        rc = EXIT_FAILURE;
        goto free_db;
    }

    const uint64_t queue_limit = get_queue_limit(pa.in.target_memory_footprint, pa.in.maxthreads);
    QPTPool_t *pool = QPTPool_init_with_props(pa.in.maxthreads, &pa, NULL, NULL, queue_limit, pa.in.swap_prefix.data, 1, 2);

    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        rc = -1;
        goto free_xattr;
    }

    fprintf(stdout, "Creating GUFI Index %s with %zu threads\n", pa.in.nameto.data, pa.in.maxthreads);
    fflush(stdout);

    /* parse the trace files and enqueue work */
    struct TraceStats stats;
    enqueue_traces(&argv[idx], traces, trace_count,
                   pa.in.delim,
                   /* allow for some threads to start processing while reading */
                   (pa.in.maxthreads / 2) + !!(pa.in.maxthreads & 1),
                   pool, processdir,
                   &stats);

    QPTPool_stop(pool);

    clock_gettime(CLOCK_MONOTONIC, &main_func.end);
    const long double processtime = sec(nsec(&main_func));

    /* don't count as part of processtime */
    QPTPool_destroy(pool);

    /* set top level permissions */
    chmod(pa.in.nameto.data, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    fprintf(stdout, "Total Dirs:          %zu\n",    stats.dirs);
    fprintf(stdout, "Total Files:         %zu\n",    stats.files);
    fprintf(stdout, "Time Spent Indexing: %.2Lfs\n", processtime);
    fprintf(stdout, "Dirs/Sec:            %.2Lf\n",  stats.dirs / processtime);
    fprintf(stdout, "Files/Sec:           %.2Lf\n",  stats.files / processtime);

  free_xattr:
    close_template_db(&pa.xattr);
  free_db:
    close_template_db(&pa.db);
  free_traces:
    close_traces(traces, trace_count);

    input_fini(&pa.in);

    dump_memory_usage();

    return rc;
}
