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
#include "opendb.h"
#include "template_db.h"
#include "trace.h"
#include "utils.h"

extern int errno;

#if defined(DEBUG) && defined(PER_THREAD_STATS)
#include "OutputBuffers.h"
extern struct OutputBuffers debug_output_buffers;
#endif

int templatefd = -1;    /* this is really a constant that is set at runtime */
off_t templatesize = 0; /* this is really a constant that is set at runtime */

/* Data stored during first pass of input file */
struct row {
    size_t first_delim;
    char * line;
    size_t len;
    long offset;
    size_t entries;
};

struct row * row_init(const size_t first_delim, char * line, const size_t len, const long offset) {
    struct row * row = malloc(sizeof(struct row));
    if (row) {
        row->first_delim = first_delim;
        row->line = line;
        row->len = len;
        row->offset = offset;
        row->entries = 0;
    }
    return row;
}

void row_destroy(struct row * row) {
    if (row) {
        free(row->line);
        free(row);
    }
}

#ifdef DEBUG
#ifdef CUMULATIVE_TIMES
size_t total_handle_args      = 0;
size_t total_memset_work      = 0;
size_t total_dir_linetowork   = 0;
size_t total_dupdir           = 0;
size_t total_copy_template    = 0;
size_t total_opendb           = 0;
size_t total_zero_summary     = 0;
size_t total_insertdbprep     = 0;
size_t total_startdb          = 0;
size_t total_fseek            = 0;
size_t total_read_entries     = 0;
size_t total_getline          = 0;
size_t total_memset_row       = 0;
size_t total_entry_linetowork = 0;
size_t total_free             = 0;
size_t total_sumit            = 0;
size_t total_insertdbgo       = 0;
size_t total_stopdb           = 0;
size_t total_insertdbfin      = 0;
size_t total_insertsumdb      = 0;
size_t total_closedb          = 0;
size_t total_row_destroy      = 0;
size_t total_files            = 0;
#endif
#endif

/* process the work under one directory (no recursion) */
int processdir(struct QPTPool * ctx, const size_t id, void * data, void * args) {
    #ifdef DEBUG
    #ifdef CUMULATIVE_TIMES
    size_t thread_handle_args      = 0;
    size_t thread_memset_work      = 0;
    size_t thread_dir_linetowork   = 0;
    size_t thread_dupdir           = 0;
    size_t thread_copy_template    = 0;
    size_t thread_opendb           = 0;
    size_t thread_zero_summary     = 0;
    size_t thread_insertdbprep     = 0;
    size_t thread_startdb          = 0;
    size_t thread_fseek            = 0;
    size_t thread_read_entries     = 0;
    size_t thread_getline          = 0;
    size_t thread_memset_row       = 0;
    size_t thread_entry_linetowork = 0;
    size_t thread_free             = 0;
    size_t thread_sumit            = 0;
    size_t thread_insertdbgo       = 0;
    size_t thread_stopdb           = 0;
    size_t thread_insertdbfin      = 0;
    size_t thread_insertsumdb      = 0;
    size_t thread_closedb          = 0;
    size_t thread_row_destroy      = 0;
    size_t thread_files = 0;
    #endif

    struct timespec print_timestamps_start;
    struct timespec print_timestamps_end;

    #ifdef PER_THREAD_STATS
    char buf[4096];
    const size_t size = 4096;
    #endif
    #endif

    #ifdef DEBUG
    struct timespec handle_args_start;
    clock_gettime(CLOCK_MONOTONIC, &handle_args_start);
    #endif

    /* skip argument checking */
    /* if (!data) { */
    /*     return 1; */
    /* } */

    /* if (!ctx || (id >= ctx->size)) { */
    /*     free(data); */
    /*     return 1; */
    /* } */

    (void) ctx;

    struct row * w = (struct row *) data;
    FILE * trace = ((FILE **) args)[id];

    #ifdef DEBUG
    struct timespec handle_args_end;
    clock_gettime(CLOCK_MONOTONIC, &handle_args_end);
    #endif

    #ifdef DEBUG
    struct timespec memset_work_start;
    clock_gettime(CLOCK_MONOTONIC, &memset_work_start);
    #endif

    struct work dir;
    /* memset(&dir, 0, sizeof(struct work)); */

    #ifdef DEBUG
    struct timespec memset_work_end;
    clock_gettime(CLOCK_MONOTONIC, &memset_work_end);
    #endif

    #ifdef DEBUG
    struct timespec dir_linetowork_start;
    clock_gettime(CLOCK_MONOTONIC, &dir_linetowork_start);
    #endif

    /* parse the directory data */
    linetowork(w->line, in.delim, &dir);

    #ifdef DEBUG
    struct timespec dir_linetowork_end;
    clock_gettime(CLOCK_MONOTONIC, &dir_linetowork_end);
    #endif

    #ifdef DEBUG
    struct timespec dupdir_start;
    clock_gettime(CLOCK_MONOTONIC, &dupdir_start);
    #endif

    /* create the directory */
    char topath[MAXPATH];
    SNFORMAT_S(topath, MAXPATH, 3, in.nameto, strlen(in.nameto), "/", (size_t) 1, dir.name, strlen(dir.name));
    if (dupdir(topath, &dir.statuso)) {
        const int err = errno;
        fprintf(stderr, "Dupdir failure: %d %s\n", err, strerror(err));
        row_destroy(w);
        return 1;
    }

    #ifdef DEBUG
    struct timespec dupdir_end;
    clock_gettime(CLOCK_MONOTONIC, &dupdir_end);
    #endif

    #ifdef DEBUG
    struct timespec copy_template_start;
    clock_gettime(CLOCK_MONOTONIC, &copy_template_start);
    #endif

    /* create the database name */
    char dbname[MAXPATH];
    SNFORMAT_S(dbname, MAXPATH, 2, topath, strlen(topath), "/" DBNAME, (size_t) (DBNAME_LEN + 1));

    /* /\* don't bother doing anything if there is nothing to insert *\/ */
    /* /\* (the database file will not exist for empty directories) *\/ */
    /* if (!w->entries) { */
    /*     row_destroy(w); */
    /*     return 0; */
    /* } */

    /* copy the template file */
    if (copy_template(templatefd, dbname, templatesize, dir.statuso.st_uid, dir.statuso.st_gid)) {
        row_destroy(w);
        return 1;
    }

    #ifdef DEBUG
    struct timespec copy_template_end;
    clock_gettime(CLOCK_MONOTONIC, &copy_template_end);
    #endif

    #ifdef DEBUG
    struct timespec opendb_start;
    clock_gettime(CLOCK_MONOTONIC, &opendb_start);
    #endif

    /* process the work */
    sqlite3 * db = opendb2(dbname, 0, 0, 1);

    #ifdef DEBUG
    struct timespec opendb_end;
    clock_gettime(CLOCK_MONOTONIC, &opendb_end);
    #endif

    if (db) {
        #ifdef DEBUG
        struct timespec zero_summary_start;
        clock_gettime(CLOCK_MONOTONIC, &zero_summary_start);
        #endif

        struct sum summary;
        zeroit(&summary);

        #ifdef DEBUG
        struct timespec zero_summary_end;
        clock_gettime(CLOCK_MONOTONIC, &zero_summary_end);
        #endif

        #ifdef DEBUG
        struct timespec insertdbprep_start;
        clock_gettime(CLOCK_MONOTONIC, &insertdbprep_start);
        #endif

        sqlite3_stmt * res = insertdbprep(db);

        #ifdef DEBUG
        struct timespec insertdbprep_end;
        clock_gettime(CLOCK_MONOTONIC, &insertdbprep_end);
        #endif

        #ifdef DEBUG
        struct timespec startdb_start;
        clock_gettime(CLOCK_MONOTONIC, &startdb_start);
        #endif

        startdb(db);

        #ifdef DEBUG
        struct timespec startdb_end;
        clock_gettime(CLOCK_MONOTONIC, &startdb_end);
        #endif

        #ifdef DEBUG
        struct timespec fseek_start;
        clock_gettime(CLOCK_MONOTONIC, &fseek_start);
        #endif

        /* move the trace file to the offet */
        fseek(trace, w->offset, SEEK_SET);

        #ifdef DEBUG
        struct timespec fseek_end;
        clock_gettime(CLOCK_MONOTONIC, &fseek_end);
        #endif

        #ifdef DEBUG
        struct timespec read_entries_start;
        clock_gettime(CLOCK_MONOTONIC, &read_entries_start);
        #endif

        size_t row_count = 0;
        for(size_t i = 0; i < w->entries; i++) {
            #ifdef DEBUG
            struct timespec getline_start;
            clock_gettime(CLOCK_MONOTONIC, &getline_start);
            #endif

            char * line = NULL;
            size_t len = 0;
            if (getline(&line, &len, trace) == -1) {
                free(line);
                break;
            }

            #ifdef DEBUG
            struct timespec getline_end;
            clock_gettime(CLOCK_MONOTONIC, &getline_end);
            #endif

            #ifdef DEBUG
            struct timespec memset_row_start;
            clock_gettime(CLOCK_MONOTONIC, &memset_row_start);
            #endif

            struct work row;
            memset(&row, 0, sizeof(struct work));

            #ifdef DEBUG
            struct timespec memset_row_end;
            clock_gettime(CLOCK_MONOTONIC, &memset_row_end);
            #endif

            #ifdef DEBUG
            struct timespec entry_linetowork_start;
            clock_gettime(CLOCK_MONOTONIC, &entry_linetowork_start);
            #endif

            linetowork(line, in.delim, &row);

            #ifdef DEBUG
            struct timespec entry_linetowork_end;
            clock_gettime(CLOCK_MONOTONIC, &entry_linetowork_end);
            #endif

            #ifdef DEBUG
            struct timespec free_start;
            clock_gettime(CLOCK_MONOTONIC, &free_start);
            #endif

            free(line);

            #ifdef DEBUG
            struct timespec free_end;
            clock_gettime(CLOCK_MONOTONIC, &free_end);
            #endif

            /* /\* don't need this now because this loop uses the count acquired by the scout function *\/ */
            /* /\* stop on directories, since files are listed first *\/ */
            /* if (row.type[0] == 'd') { */
            /*     break; */
            /* } */

            #ifdef DEBUG
            struct timespec sumit_start;
            clock_gettime(CLOCK_MONOTONIC, &sumit_start);
            #endif

            /* update summary table */
            sumit(&summary,&row);

            #ifdef DEBUG
            struct timespec sumit_end;
            clock_gettime(CLOCK_MONOTONIC, &sumit_end);
            #endif

            /* dont't record pinode */
            row.pinode = 0;

            /* add row to bulk insert */
            #ifdef DEBUG
            struct timespec insertdbgo_start;
            clock_gettime(CLOCK_MONOTONIC, &insertdbgo_start);
            #endif

            insertdbgo(&row,db,res);

            #ifdef DEBUG
            struct timespec insertdbgo_end;
            clock_gettime(CLOCK_MONOTONIC, &insertdbgo_end);
            #endif

            row_count++;
            if (row_count > 100000) {
                #ifdef DEBUG
                struct timespec stopdb_start;
                clock_gettime(CLOCK_MONOTONIC, &stopdb_start);
                #endif

                stopdb(db);

                #ifdef DEBUG
                struct timespec stopdb_end;
                clock_gettime(CLOCK_MONOTONIC, &stopdb_end);
                #endif

                #ifdef DEBUG
                struct timespec startdb_start;
                clock_gettime(CLOCK_MONOTONIC, &startdb_start);
                #endif

                startdb(db);

                #ifdef DEBUG
                struct timespec startdb_end;
                clock_gettime(CLOCK_MONOTONIC, &startdb_end);
                #endif

                #ifdef DEBUG
                clock_gettime(CLOCK_MONOTONIC, &print_timestamps_start);
                #ifdef PER_THREAD_STATS
                print_debug(&debug_output_buffers, id, buf, size, "startdb",          &startdb_start, &startdb_end);
                print_debug(&debug_output_buffers, id, buf, size, "stopdb",           &stopdb_start, &stopdb_end);
                #endif
                clock_gettime(CLOCK_MONOTONIC, &print_timestamps_end);
                #ifdef CUMULATIVE_TIMES
                thread_startdb      += timestamp(&startdb_end) - timestamp(&startdb_start);
                thread_stopdb       += timestamp(&stopdb_end) - timestamp(&stopdb_start);
                #endif
                #ifdef PER_THREAD_STATS
                print_debug(&debug_output_buffers, id, buf, size, "print_timestamps", &print_timestamps_start, &print_timestamps_end);
                #endif
                #endif

                row_count=0;
            }

            #ifdef DEBUG
            clock_gettime(CLOCK_MONOTONIC, &print_timestamps_start);
            #ifdef PER_THREAD_STATS
            print_debug(&debug_output_buffers, id, buf, size, "getline",          &getline_start, &getline_end);
            print_debug(&debug_output_buffers, id, buf, size, "memset_row",       &memset_row_start, &memset_row_end);
            print_debug(&debug_output_buffers, id, buf, size, "entry_linetowork", &entry_linetowork_start, &entry_linetowork_end);
            print_debug(&debug_output_buffers, id, buf, size, "free",             &free_start, &free_end);
            print_debug(&debug_output_buffers, id, buf, size, "sumit",            &sumit_start, &sumit_end);
            print_debug(&debug_output_buffers, id, buf, size, "insertdbgo",       &insertdbgo_start, &insertdbgo_end);
            #endif
            clock_gettime(CLOCK_MONOTONIC, &print_timestamps_end);

            #ifdef CUMULATIVE_TIMES
            thread_getline          += timestamp(&getline_end) - timestamp(&getline_start);
            thread_memset_row       += timestamp(&memset_row_end) - timestamp(&memset_row_start);
            thread_entry_linetowork += timestamp(&entry_linetowork_end) - timestamp(&entry_linetowork_start);
            thread_free             += timestamp(&free_end) - timestamp(&free_start);
            thread_sumit            += timestamp(&sumit_end) - timestamp(&sumit_start);
            thread_insertdbgo       += timestamp(&insertdbgo_end) - timestamp(&insertdbgo_start);
            #endif

            #ifdef PER_THREAD_STATS
            print_debug(&debug_output_buffers, id, buf, size, "print_timestamps", &print_timestamps_start, &print_timestamps_end);
            #endif
            #endif
        }

        #ifdef DEBUG
        struct timespec read_entries_end;
        clock_gettime(CLOCK_MONOTONIC, &read_entries_end);
        #endif

        #ifdef DEBUG
        struct timespec stopdb_start;
        clock_gettime(CLOCK_MONOTONIC, &stopdb_start);
        #endif

        stopdb(db);

        #ifdef DEBUG
        struct timespec stopdb_end;
        clock_gettime(CLOCK_MONOTONIC, &stopdb_end);
        #endif

        #ifdef DEBUG
        struct timespec insertdbfin_start;
        clock_gettime(CLOCK_MONOTONIC, &insertdbfin_start);
        #endif

        insertdbfin(res);

        #ifdef DEBUG
        struct timespec insertdbfin_end;
        clock_gettime(CLOCK_MONOTONIC, &insertdbfin_end);
        #endif

        #ifdef DEBUG
        struct timespec insertsumdb_start;
        clock_gettime(CLOCK_MONOTONIC, &insertsumdb_start);
        #endif

        insertsumdb(db, &dir, &summary);

        #ifdef DEBUG
        struct timespec insertsumdb_end;
        clock_gettime(CLOCK_MONOTONIC, &insertsumdb_end);
        #endif

        #ifdef DEBUG
        struct timespec closedb_start;
        clock_gettime(CLOCK_MONOTONIC, &closedb_start);
        #endif

        closedb(db); /* don't set to nullptr */

        #ifdef DEBUG
        struct timespec closedb_end;
        clock_gettime(CLOCK_MONOTONIC, &closedb_end);
        #endif

        #ifdef DEBUG
        clock_gettime(CLOCK_MONOTONIC, &print_timestamps_start);
        #ifdef PER_THREAD_STATS
        print_debug(&debug_output_buffers, id, buf, size, "zero_summary", &zero_summary_start, &zero_summary_end);
        print_debug(&debug_output_buffers, id, buf, size, "insertdbprep", &insertdbprep_start, &insertdbprep_end);
        print_debug(&debug_output_buffers, id, buf, size, "startdb",      &startdb_start, &startdb_end);
        print_debug(&debug_output_buffers, id, buf, size, "fseek",        &fseek_start, &fseek_end);
        print_debug(&debug_output_buffers, id, buf, size, "read_entries", &read_entries_start, &read_entries_end);
        print_debug(&debug_output_buffers, id, buf, size, "read_entries", &read_entries_start, &read_entries_end);
        print_debug(&debug_output_buffers, id, buf, size, "stopdb",       &stopdb_start, &stopdb_end);
        print_debug(&debug_output_buffers, id, buf, size, "insertdbfin",  &insertdbfin_start, &insertdbfin_end);
        print_debug(&debug_output_buffers, id, buf, size, "insertsumdb",  &insertsumdb_start, &insertsumdb_end);
        print_debug(&debug_output_buffers, id, buf, size, "closedb",      &closedb_start, &closedb_end);
        #endif
        clock_gettime(CLOCK_MONOTONIC, &print_timestamps_end);

        #ifdef CUMULATIVE_TIMES
        thread_zero_summary += timestamp(&zero_summary_end) - timestamp(&zero_summary_start);
        thread_insertdbprep += timestamp(&insertdbprep_end) - timestamp(&insertdbprep_start);
        thread_startdb      += timestamp(&startdb_end) - timestamp(&startdb_start);
        thread_fseek        += timestamp(&fseek_end) - timestamp(&fseek_start);
        thread_read_entries += timestamp(&read_entries_end) - timestamp(&read_entries_start);
        thread_read_entries += timestamp(&read_entries_end) - timestamp(&read_entries_start);
        thread_stopdb       += timestamp(&stopdb_end) - timestamp(&stopdb_start);
        thread_insertdbfin  += timestamp(&insertdbfin_end) - timestamp(&insertdbfin_start);
        thread_insertsumdb  += timestamp(&insertsumdb_end) - timestamp(&insertsumdb_start);
        thread_closedb      += timestamp(&closedb_end) - timestamp(&closedb_start);
        thread_files        += row_count;
        #endif

        #ifdef PER_THREAD_STATS
        print_debug(&debug_output_buffers, id, buf, size, "%zu print_timestamps %", &print_timestamps_start, &print_timestamps_end);
        #endif
        #endif
    }

    #ifdef DEBUG
    struct timespec row_destroy_start;
    clock_gettime(CLOCK_MONOTONIC, &row_destroy_start);
    #endif

    row_destroy(w);

    #ifdef DEBUG
    struct timespec row_destroy_end;
    clock_gettime(CLOCK_MONOTONIC, &row_destroy_end);
    #endif

    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &print_timestamps_start);
    #ifdef PER_THREAD_STATS
    print_debug(&debug_output_buffers, id, buf, size, "handle_args",      &handle_args_start, &handle_args_end);
    print_debug(&debug_output_buffers, id, buf, size, "memset_work",      &memset_work_start, &memset_work_end);
    print_debug(&debug_output_buffers, id, buf, size, "dir_linetowork",   &dir_linetowork_start, &dir_linetowork_end);
    print_debug(&debug_output_buffers, id, buf, size, "dupdir",           &dupdir_start, &dupdir_end);
    print_debug(&debug_output_buffers, id, buf, size, "copy_template",    &copy_template_start, &copy_template_end);
    print_debug(&debug_output_buffers, id, buf, size, "opendb",           &opendb_start, &opendb_end);
    print_debug(&debug_output_buffers, id, buf, size, "row_destroy",      &row_destroy_start, &row_destroy_end);
    #endif
    clock_gettime(CLOCK_MONOTONIC, &print_timestamps_end);
    #ifdef CUMULATIVE_TIMES
    thread_handle_args     += timestamp(&handle_args_end) - timestamp(&handle_args_start);
    thread_memset_work     += timestamp(&memset_work_end) - timestamp(&memset_work_start);
    thread_dir_linetowork  += timestamp(&dir_linetowork_end) - timestamp(&dir_linetowork_start);
    thread_dupdir          += timestamp(&dupdir_end) - timestamp(&dupdir_start);
    thread_copy_template   += timestamp(&copy_template_end) - timestamp(&copy_template_start);
    thread_opendb          += timestamp(&opendb_end) - timestamp(&opendb_start);
    thread_row_destroy     += timestamp(&row_destroy_end) - timestamp(&row_destroy_start);

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
    #ifdef PER_THREAD_STATS
    print_debug(&debug_output_buffers, id, buf, size, "print_timestamps", &print_timestamps_start, &print_timestamps_end);
    #endif
    #endif

    return !db;
}

size_t parsefirst(char * line, const size_t len, const char delim) {
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
int scout_function(struct QPTPool * ctx, const size_t id, void * data, void * args) {
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);

    /* skip argument checking */
    char * filename = (char *) data;
    FILE * trace = fopen(filename, "rb");

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
    char * line = NULL;
    size_t len = 0;
    if (getline(&line, &len, trace) == -1) {
        free(line);
        fclose(trace);
        return 1;
    }

    /* find a delimiter */
    size_t first_delim = parsefirst(line, len, in.delim[0]);
    if (first_delim == (size_t) -1) {
        free(line);
        fclose(trace);
        return 1;
    }

    /* make sure the first line is a directory */
    if (line[first_delim + 1] != 'd') {
        free(line);
        fclose(trace);
        return 1;
    }

    struct row * work = row_init(first_delim, line, len, ftell(trace));

    size_t target_thread = 0;

    size_t file_count = 0;
    size_t dir_count = 1; /* always start with a directory */
    size_t empty = 0;

    line = NULL;
    len = 0;
    while (getline(&line, &len, trace) != -1) {
        first_delim = parsefirst(line, len, in.delim[0]);
        if (first_delim == (size_t) -1) {
            work->entries++;
            continue;
        }

        /* push directories onto queues */
        if (line[first_delim + 1] == 'd') {
            dir_count++;

            empty += !work->entries;

            /* put the previous work on the queue */
            QPTPool_enqueue(ctx, target_thread, processdir, work);
            target_thread = (target_thread + 1) % ctx->size;

            work = row_init(first_delim, line, len, ftell(trace));
        }
        /* ignore non-directories */
        else {
            work->entries++;
            file_count++;
        }

        line = NULL;
        len = 0;
    }
    free(line);

    /* insert the last work item */
    QPTPool_enqueue(ctx, target_thread, processdir, work);

    fclose(trace);

    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);

    pthread_mutex_lock(&print_mutex);
    fprintf(stderr, "Scout finished in %.2Lf seconds\n", elapsed(&start, &end));
    fprintf(stderr, "Files: %zu\n", file_count);
    fprintf(stderr, "Dirs:  %zu (%zu empty)\n", dir_count, empty);
    fprintf(stderr, "Total: %zu\n", file_count + dir_count);
    pthread_mutex_unlock(&print_mutex);

    return 0;
}

void sub_help() {
   printf("input_file        parse this trace file to produce the GUFI index\n");
   printf("output_dir        build GUFI index here\n");
   printf("\n");
}

void close_per_thread_traces(FILE ** traces, const int count) {
    if (traces) {
        for(int i = 0; i < count; i++) {
            fclose(traces[i]);
        }

        free(traces);
    }
}

FILE ** open_per_thread_traces(char * filename, const int count) {
    FILE ** traces = (FILE **) calloc(count, sizeof(FILE *));
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

int main(int argc, char * argv[]) {
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    epoch = timestamp(&start);

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
    }

    if ((templatesize = create_template(&templatefd)) == (off_t) -1) {
        fprintf(stderr, "Could not create template file\n");
        return -1;
    }

    /* open trace files for threads to jump around in */
    /* all have to be passed in at once because there's no way to send one to each thread */
    /* the trace files have to be opened outside of the thread in order to not repeatedly open the files */
    FILE ** traces = open_per_thread_traces(in.name, in.maxthreads);
    if (!traces) {
        fprintf(stderr, "Failed to open trace file for each thread\n");
        return -1;
    }

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    OutputBuffers_init(&debug_output_buffers, in.maxthreads, 1073741824ULL);
    #endif

    struct QPTPool * pool = QPTPool_init(in.maxthreads);
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        close_per_thread_traces(traces, in.maxthreads);
        close(templatefd);
        return -1;
    }

    if (!QPTPool_start(pool, traces)) {
        fprintf(stderr, "Failed to start threads\n");
        close_per_thread_traces(traces, in.maxthreads);
        close(templatefd);
        return -1;
    }

    /* the scout thread pushes more work into the queue instead of processdir */
    QPTPool_enqueue(pool, 0, scout_function, in.name);

    QPTPool_wait(pool);
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    const size_t completed = QPTPool_threads_completed(pool);
    #endif
    QPTPool_destroy(pool);

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    OutputBuffers_flush_single(&debug_output_buffers, in.maxthreads, stderr);
    OutputBuffers_destroy(&debug_output_buffers, in.maxthreads);
    #endif

    /* set top level permissions */
    chmod(in.nameto, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    close_per_thread_traces(traces, in.maxthreads);
    close(templatefd);

    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    fprintf(stderr, "Handle Args:               %.2Lfs\n", ((long double) total_handle_args) / 1e9L);
    fprintf(stderr, "memset(work):              %.2Lfs\n", ((long double) total_memset_work) / 1e9L);
    fprintf(stderr, "Parse directory line:      %.2Lfs\n", ((long double) total_dir_linetowork) / 1e9L);
    fprintf(stderr, "dupdir:                    %.2Lfs\n", ((long double) total_dupdir) / 1e9L);
    fprintf(stderr, "copy_template:             %.2Lfs\n", ((long double) total_copy_template) / 1e9L);
    fprintf(stderr, "opendb:                    %.2Lfs\n", ((long double) total_opendb) / 1e9L);
    fprintf(stderr, "Zero summary struct:       %.2Lfs\n", ((long double) total_zero_summary) / 1e9L);
    fprintf(stderr, "insertdbprep:              %.2Lfs\n", ((long double) total_insertdbprep) / 1e9L);
    fprintf(stderr, "startdb:                   %.2Lfs\n", ((long double) total_startdb) / 1e9L);
    fprintf(stderr, "fseek:                     %.2Lfs\n", ((long double) total_fseek) / 1e9L);
    fprintf(stderr, "Read entries:              %.2Lfs\n", ((long double) total_read_entries) / 1e9L);
    fprintf(stderr, "    getline:               %.2Lfs\n", ((long double) total_getline) / 1e9L);
    fprintf(stderr, "    memset(entry struct):  %.2Lfs\n", ((long double) total_memset_row) / 1e9L);
    fprintf(stderr, "    Parse entry line:      %.2Lfs\n", ((long double) total_entry_linetowork) / 1e9L);
    fprintf(stderr, "    free(entry line):      %.2Lfs\n", ((long double) total_free) / 1e9L);
    fprintf(stderr, "    sumit:                 %.2Lfs\n", ((long double) total_sumit) / 1e9L);
    fprintf(stderr, "    insertdbgo:            %.2Lfs\n", ((long double) total_insertdbgo) / 1e9L);
    fprintf(stderr, "stopdb:                    %.2Lfs\n", ((long double) total_stopdb) / 1e9L);
    fprintf(stderr, "insertdbfin:               %.2Lfs\n", ((long double) total_insertdbfin) / 1e9L);
    fprintf(stderr, "insertsumdb:               %.2Lfs\n", ((long double) total_insertsumdb) / 1e9L);
    fprintf(stderr, "closedb:                   %.2Lfs\n", ((long double) total_closedb) / 1e9L);
    fprintf(stderr, "cleanup:                   %.2Lfs\n", ((long double) total_row_destroy) / 1e9L);
    fprintf(stderr, "\n");
    fprintf(stderr, "Directories created:       %zu\n", completed);
    fprintf(stderr, "Files inserted:            %zu\n", total_files);
    #endif

    fprintf(stderr, "main completed %.2Lf seconds\n", elapsed(&start, &end));

    return 0;
}
