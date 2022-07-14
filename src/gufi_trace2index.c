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
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "debug.h"
#include "dbutils.h"
#include "template_db.h"
#include "trace.h"
#include "utils.h"

extern int errno;

#if defined(DEBUG) && defined(PER_THREAD_STATS)
#include "OutputBuffers.h"
struct OutputBuffers debug_output_buffers;
#endif

struct PoolArgs {
    FILE **traces;
    struct templates *templates;
};

/* Data stored during first pass of input file */
struct row {
    size_t first_delim;
    char *line;
    size_t len;
    long offset;
    size_t entries;
};

struct row *row_init(const size_t first_delim, char *line, const size_t len, const long offset) {
    struct row *row = malloc(sizeof(struct row));
    if (row) {
        row->first_delim = first_delim;
        row->line = line; /* takes ownership of line */
        row->len = len;
        row->offset = offset;
        row->entries = 0;
    }
    return row;
}

void row_destroy(struct row *row) {
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
int processdir(struct QPTPool *ctx, const size_t id, void *data, void *args) {
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

    timestamp_create_buffer(4096);
    timestamp_start(handle_args);

    /* skip argument checking */
    /* if (!data) { */
    /*     return 1; */
    /* } */

    /* if (!ctx || (id >= ctx->size)) { */
    /*     free(data); */
    /*     return 1; */
    /* } */

    (void) ctx;

    struct PoolArgs *pa = (struct PoolArgs *) args;
    FILE *trace = ((FILE **) pa->traces)[id];
    struct templates *templates = pa->templates;

    struct row *w = (struct row *) data;

    timestamp_set_end(handle_args);

    timestamp_start(memset_work);
    struct work dir;
    /* memset(&dir, 0, sizeof(struct work)); */
    timestamp_set_end(memset_work);

    /* parse the directory data */
    timestamp_start(dir_linetowork);
    linetowork(w->line, w->len, in.delim, &dir);
    timestamp_set_end(dir_linetowork);

    /* create the directory */
    timestamp_start(dupdir);
    char topath[MAXPATH];
    const size_t topath_len = SNFORMAT_S(topath, MAXPATH, 3,
                                         in.nameto, in.nameto_len,
                                         "/", (size_t) 1,
                                         dir.name, w->first_delim);

    if (dupdir(topath, &dir.statuso)) {
        const int err = errno;
        fprintf(stderr, "Dupdir failure: %d %s\n", err, strerror(err));
        row_destroy(w);
        return 1;
    }
    timestamp_set_end(dupdir);

    timestamp_start(copy_template);

    /* create the database name */
    char dbname[MAXPATH];
    SNFORMAT_S(dbname, MAXPATH, 2,
               topath, topath_len,
               "/" DBNAME, (size_t) (DBNAME_LEN + 1));

    /* /\* don't bother doing anything if there is nothing to insert *\/ */
    /* /\* (the database file will not exist for empty directories) *\/ */
    /* if (!w->entries) { */
    /*     row_destroy(w); */
    /*     return 0; */
    /* } */

    /* copy the template file */
    if (copy_template(&templates->db, dbname, dir.statuso.st_uid, dir.statuso.st_gid)) {
        row_destroy(w);
        return 1;
    }

    timestamp_set_end(copy_template);

    /* process the work */
    timestamp_start(opendb);
    sqlite3 *db = opendb(dbname, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 1, 0
                          , NULL, NULL
                          #if defined(DEBUG) && defined(PER_THREAD_STATS)
                          , NULL, NULL
                          , NULL, NULL
                          #endif
                          );
    timestamp_set_end(opendb);

    if (db) {
        timestamp_start(zero_summary);
        struct sum summary;
        zeroit(&summary);
        timestamp_set_end(zero_summary);

        struct sll xattr_db_list;
        sll_init(&xattr_db_list);

        /* INSERT statement bindings into db.db */
        timestamp_start(insertdbprep);
        sqlite3_stmt *res = insertdbprep(db, esqli);                                /* entries */
        sqlite3_stmt *xattrs_res = insertdbprep(db, XATTRS_PWD_INSERT);             /* xattrs within db.db */
        sqlite3_stmt *xattr_files_res = insertdbprep(db, XATTR_FILES_PWD_INSERT);   /* per-user and per-group db file names*/
        timestamp_set_end(insertdbprep);

        timestamp_start(startdb);
        startdb(db);
        timestamp_set_end(startdb);

        /* move the trace file to the offset */
        timestamp_start(fseek);
        fseek(trace, w->offset, SEEK_SET);
        timestamp_set_end(fseek);

        timestamp_start(read_entries);
        size_t row_count = 0;
        for(size_t i = 0; i < w->entries; i++) {
            timestamp_start(getline);
            char *line = NULL;
            size_t len = 0;
            if (getline(&line, &len, trace) == -1) {
                free(line);
                break;
            }
            timestamp_set_end(getline);

            timestamp_start(memset_row);
            struct work row;
            memset(&row, 0, sizeof(struct work));
            timestamp_set_end(memset_row);

            timestamp_start(entry_linetowork);
            linetowork(line, len, in.delim, &row);
            timestamp_set_end(entry_linetowork)

            timestamp_start(free);
            free(line);
            timestamp_set_end(free);

            /* /\* don't need this now because this loop uses the count acquired by the scout function *\/ */
            /* /\* stop on directories, since files are listed first *\/ */
            /* if (row.type[0] == 'd') { */
            /*     break; */
            /* } */

            /* update summary table */
            timestamp_start(sumit);
            sumit(&summary, &row);
            timestamp_set_end(sumit);

            /* don't record pinode */
            row.pinode = 0;

            /* add row to bulk insert */
            timestamp_start(insertdbgo);
            insertdbgo(&row, db, res);
            insertdbgo_xattrs(&dir.statuso, &row,
                              &xattr_db_list, &templates->xattr,
                              topath, topath_len,
                              xattrs_res, xattr_files_res);
            timestamp_set_end(insertdbgo);

            xattrs_cleanup(&row.xattrs);

            row_count++;
            if (row_count > 100000) {
                timestamp_start(stopdb);
                stopdb(db);
                timestamp_set_end(stopdb);

                timestamp_start(startdb);
                startdb(db);
                timestamp_set_end(startdb);

                #ifdef DEBUG
                timestamp_start(print_timestamps);
                timestamp_print(ctx->buffers, id, "startdb",          startdb);
                timestamp_print(ctx->buffers, id, "stopdb",           stopdb);
                timestamp_end(ctx->buffers,   id, "print_timestamps", print_timestamps);

                #ifdef CUMULATIVE_TIMES
                thread_startdb += timestamp_elapsed(startdb);
                thread_stopdb  += timestamp_elapsed(stopdb);
                #endif
                #endif

                row_count = 0;
            }

            #ifdef DEBUG
            timestamp_start(print_timestamps);
            timestamp_print(ctx->buffers, id, "getline",          getline);
            timestamp_print(ctx->buffers, id, "memset_row",       memset_row);
            timestamp_print(ctx->buffers, id, "entry_linetowork", entry_linetowork);
            timestamp_print(ctx->buffers, id, "free",             free);
            timestamp_print(ctx->buffers, id, "sumit",            sumit);
            timestamp_print(ctx->buffers, id, "insertdbgo",       insertdbgo);
            timestamp_end  (ctx->buffers, id, "print_timestamps", print_timestamps);

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

        timestamp_start(stopdb);
        stopdb(db);
        timestamp_set_end(stopdb);

        /* write out per-user and per-group xattrs */
        sll_destroy(&xattr_db_list, destroy_xattr_db);

        /* write out the current directory's xattrs */
        insertdbgo_xattrs_avail(&dir, xattrs_res);

        /* write out data going into db.db */
        timestamp_start(insertdbfin);
        insertdbfin(xattr_files_res); /* per-user and per-group xattr db file names */
        insertdbfin(xattrs_res);      /* xattrs */
        insertdbfin(res);             /* entries */
        timestamp_set_end(insertdbfin);

        timestamp_start(insertsumdb);
        insertsumdb(db, &dir, &summary);
        xattrs_cleanup(&dir.xattrs);
        timestamp_set_end(insertsumdb);

        timestamp_start(closedb);
        closedb(db); /* don't set to nullptr */
        timestamp_set_end(closedb);

        #ifdef DEBUG
        timestamp_start(print_timestamps);
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

    timestamp_start(row_destroy);
    row_destroy(w);
    timestamp_set_end(row_destroy);

    #ifdef DEBUG
    timestamp_start(print_timestamps);
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

size_t parsefirst(char *line, const size_t len, const char delim) {
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
int scout_function(struct QPTPool *ctx, const size_t id, void *data, void *args) {
    struct start_end scouting;
    clock_gettime(CLOCK_MONOTONIC, &scouting.start);

    /* skip argument checking */
    char *filename = (char *) data;
    FILE *trace = fopen(filename, "rb");

    (void) id;
    (void) args;

    /* the trace file must exist */
    if (!trace) {
        fprintf(stderr, "Could not open file %s\n", filename);
        return 1;
    }

    /* keep current directory while finding next directory */
    /* in order to find out whether or not the current */
    /* directory has files in it */
    char *line = NULL;
    size_t len = 0;
    if (getline(&line, &len, trace) == -1) {
        free(line);
        fclose(trace);
        fprintf(stderr, "Could not get the first line of the trace\n");
        return 1;
    }

    /* find a delimiter */
    size_t first_delim = parsefirst(line, len, in.delim[0]);
    if (first_delim == (size_t) -1) {
        free(line);
        fclose(trace);
        fprintf(stderr, "Could not find the specified delimiter\n");
        return 1;
    }

    /* make sure the first line is a directory */
    if (line[first_delim + 1] != 'd') {
        free(line);
        fclose(trace);
        fprintf(stderr, "First line of trace is not a directory\n");
        return 1;
    }

    struct row *work = row_init(first_delim, line, len, ftell(trace));

    size_t target_thread = 0;

    size_t file_count = 0;
    size_t dir_count = 1; /* always start with a directory */
    size_t empty = 0;

    /* don't free line - the pointer is now owned by work */

    /* have getline allocate a new buffer */
    line = NULL;
    len = 0;
    while (getline(&line, &len, trace) != -1) {
        first_delim = parsefirst(line, len, in.delim[0]);

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
            target_thread = (target_thread + 1) % ctx->size;

            /* put the current line into a new work item */
            work = row_init(first_delim, line, len, ftell(trace));
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

    clock_gettime(CLOCK_MONOTONIC, &scouting.end);

    pthread_mutex_lock(&print_mutex);
    fprintf(stdout, "Scout finished in %.2Lf seconds\n", sec(nsec(&scouting)));
    fprintf(stdout, "Files: %zu\n", file_count);
    fprintf(stdout, "Dirs:  %zu (%zu empty)\n", dir_count, empty);
    fprintf(stdout, "Total: %zu\n", file_count + dir_count);
    pthread_mutex_unlock(&print_mutex);

    return 0;
}

void sub_help() {
   printf("input_file        parse this trace file to produce the GUFI index\n");
   printf("output_dir        build GUFI index here\n");
   printf("\n");
}

void close_per_thread_traces(FILE **traces, const int count) {
    if (traces) {
        for(int i = 0; i < count; i++) {
            fclose(traces[i]);
        }

        free(traces);
    }
}

FILE **open_per_thread_traces(char *filename, const int count) {
    FILE **traces = (FILE **) calloc(count, sizeof(FILE *));
    if (traces) {
        for(int i = 0; i < count; i++) {
            if (!(traces[i] = fopen(filename, "rb"))) {
                close_per_thread_traces(traces, i);
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

    int idx = parse_cmd_line(argc, argv, "hHn:d:", 2, "input_file output_dir", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        /* parse positional args, following the options */
        int retval = 0;
        INSTALL_STR(in.name,   argv[idx++], MAXPATH, "input_file");
        INSTALL_STR(in.nameto, argv[idx++], MAXPATH, "output_dir");

        if (retval)
            return retval;

        in.name_len = strlen(in.name);
        in.nameto_len = strlen(in.nameto);
    }

    struct templates templates;
    init_template_db(&templates.db);
    if (create_dbdb_template(&templates.db) != 0) {
        fprintf(stderr, "Could not create template file\n");
        return -1;
    }

    init_template_db(&templates.xattr);
    if (create_xattrs_template(&templates.xattr) != 0) {
        fprintf(stderr, "Could not create xattr template file\n");
        close_template_db(&templates.db);
        return -1;
    }

    /* open trace files for threads to jump around in */
    /* all have to be passed in at once because there's no way to send one to each thread */
    /* the trace files have to be opened outside of the thread in order to not repeatedly open the files */
    FILE **traces = open_per_thread_traces(in.name, in.maxthreads);
    if (!traces) {
        fprintf(stderr, "Failed to open trace file for each thread\n");
        return -1;
    }

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    OutputBuffers_init(&debug_output_buffers, in.maxthreads, 1073741824ULL, &print_mutex);
    #endif

    struct QPTPool *pool = QPTPool_init(in.maxthreads
                                         #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                         , &debug_output_buffers
                                         #endif
                                         );
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        close_per_thread_traces(traces, in.maxthreads);
        close_template_db(&templates.xattr);
        close_template_db(&templates.db);
        return -1;
    }

    struct PoolArgs args = {
        .traces = traces,
        .templates = &templates,
    };

    if (!QPTPool_start(pool, &args)) {
        fprintf(stderr, "Failed to start threads\n");
        close_per_thread_traces(traces, in.maxthreads);
        close_template_db(&templates.xattr);
        close_template_db(&templates.db);
        return -1;
    }

    /* the scout thread pushes more work into the queue instead of processdir */
    QPTPool_enqueue(pool, 0, scout_function, in.name);

    QPTPool_wait(pool);
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    const size_t completed = QPTPool_threads_completed(pool);
    #endif
    QPTPool_destroy(pool);

    close_template_db(&templates.xattr);
    close_template_db(&templates.db);

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    OutputBuffers_flush_to_single(&debug_output_buffers, stderr);
    OutputBuffers_destroy(&debug_output_buffers);
    #endif

    /* set top level permissions */
    chmod(in.nameto, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    /* have to call clock_gettime explicitly to get end time */
    clock_gettime(CLOCK_MONOTONIC, &main_func.end);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    fprintf(stderr, "Handle Args:               %.2Lfs\n", sec(total_handle_args));
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
