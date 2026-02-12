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
#include "debug.h"
#include "external.h"
#include "str.h"
#include "template_db.h"
#include "trace.h"
#include "utils.h"

/* global to pool - passed around in "args" argument */
struct PoolArgs {
    struct input in;
    str_t index_parent; /* actual index is placed at <index parent>/<path in trace> */

    struct template_db db;
    struct template_db xattr;
};

struct NonDirArgs {
    struct PoolArgs *pa;

    char *topath;
    size_t topath_len;

    struct sum summary;

    sll_t xattr_db_list;
    sqlite3_stmt *entries_res;      /* entries */
    sqlite3_stmt *xattrs_res;       /* xattrs within db.db */
    sqlite3_stmt *xattr_files_res;  /* per-user and per-group db file names */

    size_t count;
};

static void process_nondir(sqlite3 *db, char *line, const size_t len,
                           struct NonDirArgs *nda) {
    struct work *row = NULL;
    struct entry_data row_ed;

    linetowork(line, len, nda->pa->in.delim, &row, &row_ed);

    if (row_ed.type == 'e') {
        /* insert right here (instead of bulk inserting) since this is likely to be very rare */
        external_insert(db, EXTERNAL_TYPE_USER_DB_NAME, row->statuso.st_ino, row->name);
    }
    else {
        /* update summary table */
        sumit(&nda->summary, row, &row_ed);

        /* don't record pinode */
        row->pinode = 0;

        /* add row to bulk insert */
        insertdbgo(row, &row_ed, nda->entries_res);
        insertdbgo_xattrs(&nda->pa->in, &row->statuso, row, &row_ed,
                          &nda->xattr_db_list, &nda->pa->xattr,
                          nda->topath, nda->topath_len,
                          nda->xattrs_res, nda->xattr_files_res);

        xattrs_cleanup(&row_ed.xattrs);

        nda->count++;
        if (nda->count > 100000) {
            stopdb(db);
            startdb(db);
            nda->count = 0;
        }
    }

    free(row);
}

/* process the work under one directory (no recursion) */
static int processdir(QPTPool_ctx_t *ctx, void *data) {
    /* Not checking arguments */

    struct NonDirArgs nda;
    nda.pa = (struct PoolArgs *) QPTPool_get_args_internal(ctx);

    struct input *in = &nda.pa->in;
    struct row *w = (struct row *) data;
    const int trace = w->trace;

    // TODO: based on following comment, can this be stack allocated?
    struct work *dir = NULL; /* name and name_len are not used */
    struct entry_data ed;

    /* parse the directory data */
    linetowork(w->line, w->len, in->delim, &dir, &ed);

    /* create the directory */
    nda.topath_len = nda.pa->index_parent.len + 1 + w->first_delim;

    /*
     * allocate space for "/db.db" in topath
     *
     * extra buffer is not needed and save on memcpy-ing
     */
    const size_t topath_size = nda.topath_len + 1 + DBNAME_LEN + 1;
    nda.topath = malloc(topath_size);
    SNFORMAT_S(nda.topath, topath_size, 4,
               nda.pa->index_parent.data, nda.pa->index_parent.len,
               "/", (size_t) 1,
               w->line, w->first_delim,
               "\0" DBNAME, (size_t) 1 + DBNAME_LEN);

    /* have to dupdir here because directories can show up in any order */
    if (dupdir(nda.topath, dir->statuso.st_mode, dir->statuso.st_uid, dir->statuso.st_gid)) {
        const int err = errno;
        fprintf(stderr, "Dupdir failure: \"%s\": %s (%d)\n",
                nda.topath, strerror(err), err);
        free(nda.topath);
        row_destroy(&w);
        return 1;
    }

    /* restore "/db.db" (no need to remove afterwards) */
    nda.topath[nda.topath_len] = '/';

    sqlite3 *db = template_to_db(&nda.pa->db, nda.topath, dir->statuso.st_uid, dir->statuso.st_gid);

    if (db) {
        zeroit(&nda.summary);
        sll_init(&nda.xattr_db_list);

        /* INSERT statement bindings into db.db */
        nda.entries_res     = insertdbprep(db, ENTRIES_INSERT);           /* entries */
        nda.xattrs_res      = insertdbprep(db, XATTRS_PWD_INSERT);        /* xattrs within db.db */
        nda.xattr_files_res = insertdbprep(db, EXTERNAL_DBS_PWD_INSERT);  /* per-user and per-group db file names */

        startdb(db);

        nda.count = 0;
        if (trace == STDIN_FILENO) {
            sll_loop(&w->entry_lines, node) {
                str_t *str = (str_t *) sll_node_data(node);
                process_nondir(db, str->data, str->len, &nda);
                /* str is freed with row */
            }
        }
        else {
            char *line = NULL;
            size_t size = 0;
            for(size_t i = 0; i < w->entries; i++) {
                const ssize_t len = getline_fd_seekable(&line, &size, trace, &w->offset, GETLINE_DEFAULT_SIZE);
                if (len < 1) {
                    break;
                }

                process_nondir(db, line, size, &nda);
            }
            free(line); /* reuse line and only alloc+free once */
        }

        stopdb(db);

        /* write out per-user and per-group xattrs */
        sll_destroy(&nda.xattr_db_list, destroy_xattr_db);

        /* write out the current directory's xattrs */
        insertdbgo_xattrs_avail(dir, &ed, nda.xattrs_res);

        /* write out data going into db.db */
        insertdbfin(nda.xattr_files_res); /* per-user and per-group xattr db file names */
        insertdbfin(nda.xattrs_res);
        insertdbfin(nda.entries_res);

        /* find the basename of this path */
        w->line[w->first_delim] = '\x00';
        const size_t basename_start = trailing_match_index(w->line, w->first_delim, "/", 1);

        insertsumdb(db, w->line + basename_start, dir, &ed, &nda.summary);
        xattrs_cleanup(&ed.xattrs);

        closedb(db); /* don't set to nullptr */
    }

    free(nda.topath);
    row_destroy(&w);
    free(dir);

    return !db;
}

static void sub_help(void) {
   printf("trace...          parse one or more trace files to produce the GUFI tree\n");
   printf("GUFI_tree_parent  build GUFI tree under here\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_THREADS,

        /* input flags */
        FLAG_DELIM,

        /* memory usage flags  */
        FLAG_TARGET_MEMORY, FLAG_SWAP_PREFIX,

        FLAG_END
    };

    struct PoolArgs pa;
    process_args_and_maybe_exit(options, 2, "trace... GUFI_tree_parent", &pa.in);

    /* parse positional args, following the options */
    INSTALL_STR(&pa.index_parent, pa.in.pos.argv[pa.in.pos.argc - 1]);

    /* open trace files for threads to jump around in */
    /* open the trace files here to not repeatedly open in threads */
    const size_t trace_count = pa.in.pos.argc - 1;
    int *traces = open_traces(&pa.in.pos.argv[0], trace_count);
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

    if (dupdir(pa.index_parent.data, S_IRWXU | S_IRWXG | S_IRWXO, geteuid(), getegid())) {
        fprintf(stderr, "Could not create directory %s\n", pa.index_parent.data);
        rc = EXIT_FAILURE;
        goto free_traces;
    }

    /*
     * create empty db.db in index parent (this file is placed in
     * "${dst}/db.db"; index is placed in "${dst}/$(basename ${src}))"
     * so that when querying "${dst}", no error is printed
     */
    if (create_empty_dbdb(&pa.db, &pa.index_parent, geteuid(), getegid()) != 0) {
        rc = EXIT_FAILURE;
        goto free_traces;
    }

    init_template_db(&pa.xattr);
    if (create_xattrs_template(&pa.xattr) != 0) {
        fprintf(stderr, "Could not create xattr template file\n");
        rc = EXIT_FAILURE;
        goto free_db;
    }

    const uint64_t queue_limit = get_queue_limit(pa.in.target_memory, pa.in.maxthreads);
    QPTPool_ctx_t *ctx = QPTPool_init_with_props(pa.in.maxthreads, &pa, NULL, NULL, queue_limit, pa.in.swap_prefix.data, 1, 2);

    if (QPTPool_start(ctx) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(ctx);
        rc = -1;
        goto free_xattr;
    }

    fprintf(stdout, "Creating GUFI tree %s with %zu threads\n", pa.index_parent.data, pa.in.maxthreads);
    fflush(stdout);

    struct start_end rt;
    clock_gettime(CLOCK_REALTIME, &rt.start);

    struct start_end after_init;
    clock_gettime(CLOCK_MONOTONIC, &after_init.start);

    /* parse the trace files and enqueue work */
    struct TraceStats stats = {0};
    stats.mutex = &print_mutex; /* debug.h */
    if (traces[0] == STDIN_FILENO) {
        struct ScoutTraceArgs sta = {
            .delim = pa.in.delim,
            .tracename = "-",
            .tr = {
                .fd = STDIN_FILENO,
                .start = 0,
                .end = (off_t) -1,
            },
            .processdir = processdir,
            .free = NULL,
            .stats = &stats,
        };

        /* one scout thread */
        QPTPool_enqueue(ctx, scout_stream, &sta);
    }
    else {
        enqueue_traces(&pa.in.pos.argv[0], traces, trace_count,
                       pa.in.delim,
                       /* allow for some threads to start processing while reading */
                       (pa.in.maxthreads / 2) + !!(pa.in.maxthreads & 1),
                       ctx, processdir,
                       &stats);
    }

    QPTPool_stop(ctx);

    clock_gettime(CLOCK_MONOTONIC, &after_init.end);
    clock_gettime(CLOCK_REALTIME, &rt.end);
    const long double processtime = sec(nsec(&after_init));

    /* don't count as part of processtime */
    QPTPool_destroy(ctx);

    /* set top level permissions */
    chmod(pa.index_parent.data, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    fprintf(stderr, "Total Dirs:          %zu\n",    stats.dirs);
    fprintf(stderr, "Total Files:         %zu\n",    stats.files);
    fprintf(stderr, "Start Time:          %.6Lf\n",  sec(since_epoch(&rt.start)));
    fprintf(stderr, "End Time:            %.6Lf\n",  sec(since_epoch(&rt.end)));
    fprintf(stderr, "Time Spent Indexing: %.2Lfs\n", processtime);
    fprintf(stderr, "Dirs/Sec:            %.2Lf\n",  stats.dirs / processtime);
    fprintf(stderr, "Files/Sec:           %.2Lf\n",  stats.files / processtime);

  free_xattr:
    close_template_db(&pa.xattr);
  free_db:
    close_template_db(&pa.db);
  free_traces:
    close_traces(traces, trace_count);

    input_fini(&pa.in);

    dump_memory_usage(stderr);

    return rc;
}
