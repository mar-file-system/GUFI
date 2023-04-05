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

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "debug.h"
#include "dbutils.h"
#include "template_db.h"
#include "trace.h"
#include "utils.h"

#if defined(DEBUG) && defined(PER_THREAD_STATS)
#include "OutputBuffers.h"
struct OutputBuffers debug_output_buffers;
#endif

struct ScoutArgs {
    struct input *in;  /* reference to PoolArgs */
    FILE **traces;     /* reference to array */

    /* everything below is locked with print_mutex from debug.h */

    size_t *remaining; /* number of scouts still running */

    /* sum of counts from all trace files */
    uint64_t *time;
    size_t *files;
    size_t *dirs;
    size_t *empty;     /* number of directories without files/links
                        * can still have child directories */
};

/* global to pool - passed around in "args" argument */
struct PoolArgs {
    struct input in;
    struct template_db db;
    struct template_db xattr;
};

/* Data stored during first pass of input file */
struct row {
    FILE **trace; /* use thread id to index into this array */
    size_t first_delim;
    char *line;
    size_t len;
    long offset;
    size_t entries;
};

static struct row *row_init(FILE **trace, const size_t first_delim, char *line,
                            const size_t len, const long offset) {
    struct row *row = malloc(sizeof(struct row));
    if (row) {
        row->trace = trace;
        row->first_delim = first_delim;
        row->line = line; /* takes ownership of line */
        row->len = len;
        row->offset = offset;
        row->entries = 0;
    }
    return row;
}

static void row_destroy(struct row *row) {
    if (row) {
        free(row->line);
        free(row);
    }
}

#ifdef DEBUG

#ifdef CUMULATIVE_TIMES
uint64_t total_handle_args      = 0;
uint64_t total_memset_work      = 0;
uint64_t total_dir_linetowork   = 0;
uint64_t total_dupdir           = 0;
uint64_t total_copy_template    = 0;
uint64_t total_opendb           = 0;
uint64_t total_zero_summary     = 0;
uint64_t total_insertdbprep     = 0;
uint64_t total_startdb          = 0;
uint64_t total_fseek            = 0;
uint64_t total_read_entries     = 0;
uint64_t total_getline          = 0;
uint64_t total_memset_row       = 0;
uint64_t total_entry_linetowork = 0;
uint64_t total_free             = 0;
uint64_t total_sumit            = 0;
uint64_t total_insertdbgo       = 0;
uint64_t total_stopdb           = 0;
uint64_t total_insertdbfin      = 0;
uint64_t total_insertsumdb      = 0;
uint64_t total_closedb          = 0;
uint64_t total_row_destroy      = 0;
size_t total_files              = 0;
#endif

#endif

/* process the work under one directory (no recursion) */
static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    #ifdef DEBUG
    #ifdef CUMULATIVE_TIMES
    uint64_t thread_handle_args      = 0;
    uint64_t thread_memset_work      = 0;
    uint64_t thread_dir_linetowork   = 0;
    uint64_t thread_dupdir           = 0;
    uint64_t thread_copy_template    = 0;
    uint64_t thread_opendb           = 0;
    uint64_t thread_zero_summary     = 0;
    uint64_t thread_insertdbprep     = 0;
    uint64_t thread_startdb          = 0;
    uint64_t thread_fseek            = 0;
    uint64_t thread_read_entries     = 0;
    uint64_t thread_getline          = 0;
    uint64_t thread_memset_row       = 0;
    uint64_t thread_entry_linetowork = 0;
    uint64_t thread_free             = 0;
    uint64_t thread_sumit            = 0;
    uint64_t thread_insertdbgo       = 0;
    uint64_t thread_stopdb           = 0;
    uint64_t thread_insertdbfin      = 0;
    uint64_t thread_insertsumdb      = 0;
    uint64_t thread_closedb          = 0;
    uint64_t thread_row_destroy      = 0;
    size_t   thread_files            = 0;
    #endif
    #endif

    timestamp_create_start(handle_args);

    /* Not checking arguments */

    (void) ctx;

    struct PoolArgs *pa = (struct PoolArgs *) args;
    struct input *in = &pa->in;
    struct row *w = (struct row *) data;
    FILE *trace = w->trace[id];

    timestamp_set_end(handle_args);

    timestamp_create_start(memset_work);
    struct work dir; /* name and name_len are not used */
    struct entry_data ed;
    timestamp_set_end(memset_work);

    /* parse the directory data */
    timestamp_create_start(dir_linetowork);
    linetowork(w->line, w->len, in->delim, &dir, &ed);
    timestamp_set_end(dir_linetowork);

    /* create the directory */
    timestamp_create_start(dupdir);
    char topath[MAXPATH];
    const size_t topath_len = SNFORMAT_S(topath, MAXPATH, 3,
                                         in->nameto, in->nameto_len,
                                         "/", (size_t) 1,
                                         w->line, w->first_delim);

    if (dupdir(topath, &ed.statuso)) {
        const int err = errno;
        fprintf(stderr, "Dupdir failure: \"%s\": %s (%d)\n",
                topath, strerror(err), err);
        row_destroy(w);
        return 1;
    }
    timestamp_set_end(dupdir);

    timestamp_create_start(copy_template);

    /* create the database name */
    char dbname[MAXPATH];
    SNFORMAT_S(dbname, MAXPATH, 2,
               topath, topath_len,
               "/" DBNAME, (size_t) (DBNAME_LEN + 1));

    /* copy the template file */
    if (copy_template(&pa->db, dbname, ed.statuso.st_uid, ed.statuso.st_gid)) {
        row_destroy(w);
        return 1;
    }

    timestamp_set_end(copy_template);

    /* process the work */
    timestamp_create_start(opendb);
    sqlite3 *db = opendb(dbname, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 1, 0
                          , NULL, NULL
                          #if defined(DEBUG) && defined(PER_THREAD_STATS)
                          , NULL, NULL
                          , NULL, NULL
                          #endif
                          );
    timestamp_set_end(opendb);

    if (db) {
        timestamp_create_start(zero_summary);
        struct sum summary;
        zeroit(&summary);
        timestamp_set_end(zero_summary);

        sll_t xattr_db_list;
        sll_init(&xattr_db_list);

        /* INSERT statement bindings into db.db */
        timestamp_create_start(insertdbprep);
        sqlite3_stmt *entries_res     = insertdbprep(db, ENTRIES_INSERT);           /* entries */
        sqlite3_stmt *xattrs_res      = insertdbprep(db, XATTRS_PWD_INSERT);        /* xattrs within db.db */
        sqlite3_stmt *xattr_files_res = insertdbprep(db, EXTERNAL_DBS_PWD_INSERT);  /* per-user and per-group db file names */
        timestamp_set_end(insertdbprep);

        timestamp_create_start(startdb);
        startdb(db);
        timestamp_set_end(startdb);

        /* move the trace file to the offset */
        timestamp_create_start(fseek);
        fseek(trace, w->offset, SEEK_SET);
        timestamp_set_end(fseek);

        timestamp_create_start(read_entries);
        size_t row_count = 0;
        for(size_t i = 0; i < w->entries; i++) {
            timestamp_create_start(getline);
            char *line = NULL;
            size_t len = 0;
            if (getline(&line, &len, trace) == -1) {
                free(line);
                break;
            }
            timestamp_set_end(getline);

            timestamp_create_start(memset_row);
            struct work row;
            struct entry_data row_ed;
            timestamp_set_end(memset_row);

            timestamp_create_start(entry_linetowork);
            linetowork(line, len, in->delim, &row, &row_ed);
            timestamp_set_end(entry_linetowork);

            timestamp_create_start(free);
            free(line);
            timestamp_set_end(free);

            /* update summary table */
            timestamp_create_start(sumit);
            sumit(&summary, &row_ed);
            timestamp_set_end(sumit);

            /* don't record pinode */
            row.pinode = 0;

            /* add row to bulk insert */
            timestamp_create_start(insertdbgo);
            insertdbgo(&row, &row_ed, db, entries_res);
            insertdbgo_xattrs(in, &ed.statuso, &row, &row_ed,
                              &xattr_db_list, &pa->xattr,
                              topath, topath_len,
                              xattrs_res, xattr_files_res);
            timestamp_set_end(insertdbgo);

            xattrs_cleanup(&row_ed.xattrs);

            row_count++;
            if (row_count > 100000) {
                timestamp_create_start(stopdb);
                stopdb(db);
                timestamp_set_end(stopdb);

                timestamp_create_start(startdb);
                startdb(db);
                timestamp_set_end(startdb);

                #ifdef DEBUG
                timestamp_create_start(print_timestamps);
                timestamp_print    (ctx->buffers, id, "startdb",          startdb);
                timestamp_print    (ctx->buffers, id, "stopdb",           stopdb);
                timestamp_end_print(ctx->buffers, id, "print_timestamps", print_timestamps);

                #ifdef CUMULATIVE_TIMES
                thread_startdb += timestamp_elapsed(startdb);
                thread_stopdb  += timestamp_elapsed(stopdb);
                #endif
                #endif

                row_count = 0;
            }

            #ifdef DEBUG
            timestamp_create_start(print_timestamps);
            timestamp_print    (ctx->buffers, id, "getline",          getline);
            timestamp_print    (ctx->buffers, id, "memset_row",       memset_row);
            timestamp_print    (ctx->buffers, id, "entry_linetowork", entry_linetowork);
            timestamp_print    (ctx->buffers, id, "free",             free);
            timestamp_print    (ctx->buffers, id, "sumit",            sumit);
            timestamp_print    (ctx->buffers, id, "insertdbgo",       insertdbgo);
            timestamp_end_print(ctx->buffers, id, "print_timestamps", print_timestamps);

            #ifdef CUMULATIVE_TIMES
            thread_getline          += timestamp_elapsed(getline);
            thread_memset_row       += timestamp_elapsed(memset_row);
            thread_entry_linetowork += timestamp_elapsed(entry_linetowork);
            thread_free             += timestamp_elapsed(free);
            thread_sumit            += timestamp_elapsed(sumit);
            thread_insertdbgo       += timestamp_elapsed(insertdbgo);
            #endif

            #endif
        }

        timestamp_set_end(read_entries);

        timestamp_create_start(stopdb);
        stopdb(db);
        timestamp_set_end(stopdb);

        /* write out per-user and per-group xattrs */
        sll_destroy(&xattr_db_list, destroy_xattr_db);

        /* write out the current directory's xattrs */
        insertdbgo_xattrs_avail(&ed, xattrs_res);

        /* write out data going into db.db */
        timestamp_create_start(insertdbfin);
        insertdbfin(xattr_files_res); /* per-user and per-group xattr db file names */
        insertdbfin(xattrs_res);
        insertdbfin(entries_res);
        timestamp_set_end(insertdbfin);

        timestamp_create_start(insertsumdb);

        /* find the basename of this path */
        w->line[w->first_delim] = '\x00';
        size_t basename_start = w->first_delim;
        while (basename_start && (w->line[basename_start - 1] != '/')) {
            basename_start--;
        }

        insertsumdb(db, w->line + basename_start, &dir, &ed, &summary);
        xattrs_cleanup(&ed.xattrs);
        timestamp_set_end(insertsumdb);

        timestamp_create_start(closedb);
        closedb(db); /* don't set to nullptr */
        timestamp_set_end(closedb);

        #ifdef DEBUG
        timestamp_create_start(print_timestamps);
        timestamp_print(ctx->buffers, id, "zero_summary", zero_summary);
        timestamp_print(ctx->buffers, id, "insertdbprep", insertdbprep);
        timestamp_print(ctx->buffers, id, "startdb",      startdb);
        timestamp_print(ctx->buffers, id, "fseek",        fseek);
        timestamp_print(ctx->buffers, id, "read_entries", read_entries);
        timestamp_print(ctx->buffers, id, "read_entries", read_entries);
        timestamp_print(ctx->buffers, id, "stopdb",       stopdb);
        timestamp_print(ctx->buffers, id, "insertdbfin",  insertdbfin);
        timestamp_print(ctx->buffers, id, "insertsumdb",  insertsumdb);
        timestamp_print(ctx->buffers, id, "closedb",      closedb);
        timestamp_set_end(print_timestamps);

        #ifdef CUMULATIVE_TIMES
        thread_zero_summary += timestamp_elapsed(zero_summary);
        thread_insertdbprep += timestamp_elapsed(insertdbprep);
        thread_startdb      += timestamp_elapsed(startdb);
        thread_fseek        += timestamp_elapsed(fseek);
        thread_read_entries += timestamp_elapsed(read_entries);
        thread_stopdb       += timestamp_elapsed(stopdb);
        thread_insertdbfin  += timestamp_elapsed(insertdbfin);
        thread_insertsumdb  += timestamp_elapsed(insertsumdb);
        thread_closedb      += timestamp_elapsed(closedb);
        thread_files        += row_count;
        #endif

        timestamp_print(ctx->buffers, id, "print_timestamps", print_timestamps);
        #endif
    }

    timestamp_create_start(row_destroy);
    row_destroy(w);
    timestamp_set_end(row_destroy);

    #ifdef DEBUG
    timestamp_create_start(print_timestamps);
    timestamp_print(ctx->buffers, id, "handle_args",      handle_args);
    timestamp_print(ctx->buffers, id, "memset_work",      memset_work);
    timestamp_print(ctx->buffers, id, "dir_linetowork",   dir_linetowork);
    timestamp_print(ctx->buffers, id, "dupdir",           dupdir);
    timestamp_print(ctx->buffers, id, "copy_template",    copy_template);
    timestamp_print(ctx->buffers, id, "opendb",           opendb);
    timestamp_print(ctx->buffers, id, "row_destroy",      row_destroy);
    timestamp_set_end(print_timestamps);
    #ifdef CUMULATIVE_TIMES
    thread_handle_args     += timestamp_elapsed(handle_args);
    thread_memset_work     += timestamp_elapsed(memset_work);
    thread_dir_linetowork  += timestamp_elapsed(dir_linetowork);
    thread_dupdir          += timestamp_elapsed(dupdir);
    thread_copy_template   += timestamp_elapsed(copy_template);
    thread_opendb          += timestamp_elapsed(opendb);
    thread_row_destroy     += timestamp_elapsed(row_destroy);

    pthread_mutex_lock(&print_mutex);
    total_handle_args      += thread_handle_args;
    total_memset_work      += thread_memset_work;
    total_dir_linetowork   += thread_dir_linetowork;
    total_dupdir           += thread_dupdir;
    total_copy_template    += thread_copy_template;
    total_opendb           += thread_opendb;
    total_zero_summary     += thread_zero_summary;
    total_insertdbprep     += thread_insertdbprep;
    total_startdb          += thread_startdb;
    total_fseek            += thread_fseek;
    total_read_entries     += thread_read_entries;
    total_getline          += thread_getline;
    total_memset_row       += thread_memset_row;
    total_entry_linetowork += thread_entry_linetowork;
    total_free             += thread_free;
    total_sumit            += thread_sumit;
    total_insertdbgo       += thread_insertdbgo;
    total_stopdb           += thread_stopdb;
    total_insertdbfin      += thread_insertdbfin;
    total_insertsumdb      += thread_insertsumdb;
    total_closedb          += thread_closedb;
    total_row_destroy      += thread_row_destroy;
    total_files            += thread_files;
    pthread_mutex_unlock(&print_mutex);
    #endif
    timestamp_print(ctx->buffers, id, "print_timestamps", print_timestamps);
    #endif

    return !db;
}

static size_t parsefirst(char *line, const size_t len, const char delim) {
    size_t first_delim = 0;
    while ((first_delim < len) && (line[first_delim] != delim)) {
        first_delim++;
    }

    if (first_delim == len) {
        first_delim = -1;
    }

    return first_delim;
}

/* Read ahead to figure out where files under directories start */
static int scout_function(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    struct start_end scouting;
    clock_gettime(CLOCK_MONOTONIC, &scouting.start);

    /* skip argument checking */
    struct ScoutArgs *sa = (struct ScoutArgs *) data;
    struct input *in = sa->in;
    FILE **traces = sa->traces;
    FILE *trace = traces[sa->in->maxthreads];

    (void) id;
    (void) args;

    /* keep current directory while finding next directory */
    /* in order to find out whether or not the current */
    /* directory has files in it */
    char *line = NULL;
    size_t len = 0;
    if (getline(&line, &len, trace) == -1) {
        free(line);
        fclose(trace);
        traces[in->maxthreads] = NULL;
        fprintf(stderr, "Could not get the first line of the trace\n");
        return 1;
    }

    /* find a delimiter */
    size_t first_delim = parsefirst(line, len, in->delim[0]);
    if (first_delim == (size_t) -1) {
        free(line);
        fclose(trace);
        traces[in->maxthreads] = NULL;
        fprintf(stderr, "Could not find the specified delimiter\n");
        return 1;
    }

    /* make sure the first line is a directory */
    if (line[first_delim + 1] != 'd') {
        free(line);
        fclose(trace);
        traces[in->maxthreads] = NULL;
        fprintf(stderr, "First line of trace is not a directory\n");
        return 1;
    }

    struct row *work = row_init(traces, first_delim, line, len, ftell(trace));

    size_t target_thread = 0;

    size_t file_count = 0;
    size_t dir_count = 1; /* always start with a directory */
    size_t empty = 0;

    /* don't free line - the pointer is now owned by work */

    /* have getline allocate a new buffer */
    line = NULL;
    len = 0;
    while (getline(&line, &len, trace) != -1) {
        first_delim = parsefirst(line, len, in->delim[0]);

        /* bad line */
        if (first_delim == (size_t) -1) {
            free(line);
            line = NULL;
            len = 0;
            fprintf(stderr, "Scout encountered bad line ending at offset %ld\n", ftell(trace));
            continue;
        }

        /* push directories onto queues */
        if (line[first_delim + 1] == 'd') {
            dir_count++;

            empty += !work->entries;

            /* put the previous work on the queue */
            QPTPool_enqueue(ctx, target_thread, processdir, work);
            target_thread = (target_thread + 1) % in->maxthreads;

            /* put the current line into a new work item */
            work = row_init(traces, first_delim, line, len, ftell(trace));
        }
        /* ignore non-directories */
        else {
            work->entries++;
            file_count++;

            /* this line is not needed */
            free(line);
        }

        /* have getline allocate a new buffer */
        line = NULL;
        len = 0;
    }

    /* end of file, so getline failed - still have to free line */
    free(line);

    /* insert the last work item */
    QPTPool_enqueue(ctx, target_thread, processdir, work);

    fclose(trace);
    traces[in->maxthreads] = NULL;

    clock_gettime(CLOCK_MONOTONIC, &scouting.end);

    pthread_mutex_lock(&print_mutex);
    *sa->time += nsec(&scouting);
    *sa->files += file_count;
    *sa->dirs += dir_count;
    *sa->empty += empty;

    (*sa->remaining)--;

    /* print here to print as early as possible instead of after thread pool completes */
    if ((*sa->remaining) == 0) {
        fprintf(stdout, "Scouts took total of %.2Lf seconds\n", sec(*sa->time));
        fprintf(stdout, "Files: %zu\n", *sa->files);
        fprintf(stdout, "Dirs:  %zu (%zu empty)\n", *sa->dirs, *sa->empty);
        fprintf(stdout, "Total: %zu\n", *sa->files + *sa->dirs);
    }
    pthread_mutex_unlock(&print_mutex);

    free(sa);

    return 0;
}

static void sub_help() {
   printf("trace_file...     parse one or more trace files to produce the GUFI index\n");
   printf("output_dir        build GUFI index here\n");
   printf("\n");
}

static void close_per_thread_traces(FILE ***traces, size_t trace_count, size_t thread_count) {
    if (traces) {
        /* also close scount_function trace if it hasn't been closed yet */
        thread_count++;

        for(size_t i = 0; i < trace_count; i++) {
            if (traces[i]) {
                for(size_t j = 0; j < thread_count; j++) {
                    if (traces[i][j]) {
                        fclose(traces[i][j]);
                    }
                }
                free(traces[i]);
            }
        }

        free(traces);
    }
}

/* open each trace file thread_count + 1 times */
static FILE ***open_per_thread_traces(char **trace_names, size_t trace_count, size_t thread_count) {
    FILE ***traces = (FILE ***) calloc(trace_count, sizeof(FILE **));
    if (!traces) {
        return NULL;
    }

    /* open one extra trace for the scout_function */
    thread_count++;

    for(size_t i = 0; i < trace_count; i++) {
        traces[i] = (FILE **) calloc(thread_count, sizeof(FILE *));
        if (!traces[i]) {
            close_per_thread_traces(traces, i, thread_count);
            return NULL;
        }

        for(size_t j = 0; j < thread_count; j++) {
            if (!(traces[i][j] = fopen(trace_names[i], "rb"))) {
                 /* if the file exists, probably ran out of file descriptors */
                const int err = errno;
                fprintf(stderr, "Could not open \"%s\": %s (%d)\n", trace_names[i], strerror(err), err);
                close_per_thread_traces(traces, i + 1, thread_count - 1); /* pass in original thread_count */
                return NULL;
            }
        }
    }

    return traces;
}

int main(int argc, char *argv[]) {
    /* have to call clock_gettime explicitly to get start time and epoch */
    struct start_end main_func;
    clock_gettime(CLOCK_MONOTONIC, &main_func.start);
    epoch = since_epoch(&main_func.start);

    struct PoolArgs pa;
    int idx = parse_cmd_line(argc, argv, "hHn:d:M:", 2, "trace_file... output_dir", &pa.in);
    if (pa.in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        /* parse positional args, following the options */
        int retval = 0;
        INSTALL_STR(pa.in.nameto, argv[argc - 1], MAXPATH, "output_dir");

        if (retval)
            return retval;

        pa.in.nameto_len = strlen(pa.in.nameto);
    }

    /* open trace files for threads to jump around in */
    /* open the trace files here to not repeatedly open in threads */
    const size_t trace_count = argc - idx - 1;
    FILE ***traces = open_per_thread_traces(&argv[idx], trace_count, pa.in.maxthreads);
    if (!traces) {
        fprintf(stderr, "Failed to open trace file for each thread\n");
        return -1;
    }

    init_template_db(&pa.db);
    if (create_dbdb_template(&pa.db) != 0) {
        fprintf(stderr, "Could not create template file\n");
        close_per_thread_traces(traces, trace_count, pa.in.maxthreads);
        return -1;
    }

    init_template_db(&pa.xattr);
    if (create_xattrs_template(&pa.xattr) != 0) {
        fprintf(stderr, "Could not create xattr template file\n");
        close_template_db(&pa.db);
        close_per_thread_traces(traces, trace_count, pa.in.maxthreads);
        return -1;
    }

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    OutputBuffers_init(&debug_output_buffers, pa.in.maxthreads, 1073741824ULL, &print_mutex);
    #endif

    const uint64_t queue_depth = pa.in.target_memory_footprint / sizeof(struct work) / pa.in.maxthreads;
    QPTPool_t *pool = QPTPool_init(pa.in.maxthreads, &pa, NULL, NULL, queue_depth
                                   #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                   , &debug_output_buffers
                                   #endif
        );
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        close_template_db(&pa.xattr);
        close_template_db(&pa.db);
        close_per_thread_traces(traces, trace_count, pa.in.maxthreads);
        return -1;
    }

    /* parse the trace files */
    size_t remaining = trace_count;
    uint64_t scout_time = 0;
    size_t files = 0;
    size_t dirs = 0;
    size_t empty = 0;
    for(size_t i = 0; i < trace_count; i++) {
        /* freed by scout_function */
        struct ScoutArgs *sa = malloc(sizeof(struct ScoutArgs));
        sa->in = &pa.in;
        sa->traces = traces[i];
        sa->remaining = &remaining;
        sa->time = &scout_time;
        sa->files = &files;
        sa->dirs = &dirs;
        sa->empty = &empty;

        /* scout_function pushes more work into the queue */
        QPTPool_enqueue(pool, 0, scout_function, sa);
    }

    QPTPool_wait(pool);
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    const size_t completed = QPTPool_threads_completed(pool);
    #endif
    QPTPool_destroy(pool);

    close_template_db(&pa.xattr);
    close_template_db(&pa.db);
    close_per_thread_traces(traces, trace_count, pa.in.maxthreads);

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    OutputBuffers_flush_to_single(&debug_output_buffers, stderr);
    OutputBuffers_destroy(&debug_output_buffers);
    #endif

    /* set top level permissions */
    chmod(pa.in.nameto, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    /* have to call clock_gettime explicitly to get end time */
    clock_gettime(CLOCK_MONOTONIC, &main_func.end);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    fprintf(stderr, "Handle args:               %.2Lfs\n", sec(total_handle_args));
    fprintf(stderr, "memset(work):              %.2Lfs\n", sec(total_memset_work));
    fprintf(stderr, "Parse directory line:      %.2Lfs\n", sec(total_dir_linetowork));
    fprintf(stderr, "dupdir:                    %.2Lfs\n", sec(total_dupdir));
    fprintf(stderr, "copy_template:             %.2Lfs\n", sec(total_copy_template));
    fprintf(stderr, "opendb:                    %.2Lfs\n", sec(total_opendb));
    fprintf(stderr, "Zero summary struct:       %.2Lfs\n", sec(total_zero_summary));
    fprintf(stderr, "insertdbprep:              %.2Lfs\n", sec(total_insertdbprep));
    fprintf(stderr, "startdb:                   %.2Lfs\n", sec(total_startdb));
    fprintf(stderr, "fseek:                     %.2Lfs\n", sec(total_fseek));
    fprintf(stderr, "Read entries:              %.2Lfs\n", sec(total_read_entries));
    fprintf(stderr, "    getline:               %.2Lfs\n", sec(total_getline));
    fprintf(stderr, "    memset(entry struct):  %.2Lfs\n", sec(total_memset_row));
    fprintf(stderr, "    Parse entry line:      %.2Lfs\n", sec(total_entry_linetowork));
    fprintf(stderr, "    free(entry line):      %.2Lfs\n", sec(total_free));
    fprintf(stderr, "    sumit:                 %.2Lfs\n", sec(total_sumit));
    fprintf(stderr, "    insertdbgo:            %.2Lfs\n", sec(total_insertdbgo));
    fprintf(stderr, "stopdb:                    %.2Lfs\n", sec(total_stopdb));
    fprintf(stderr, "insertdbfin:               %.2Lfs\n", sec(total_insertdbfin));
    fprintf(stderr, "insertsumdb:               %.2Lfs\n", sec(total_insertsumdb));
    fprintf(stderr, "closedb:                   %.2Lfs\n", sec(total_closedb));
    fprintf(stderr, "cleanup:                   %.2Lfs\n", sec(total_row_destroy));
    fprintf(stderr, "\n");
    fprintf(stderr, "Directories created:       %zu\n", completed);
    fprintf(stderr, "Files inserted:            %zu\n", total_files);
    #endif

    fprintf(stdout, "main completed in %.2Lf seconds\n", sec(nsec(&main_func)));

    return 0;
}
