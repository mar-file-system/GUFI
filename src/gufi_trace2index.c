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

#define debug_start(name) define_start(name)
#define debug_end(name) timestamp_end(name)

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

#else
#define debug_start(name)
#define debug_end(name)
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

    struct start_end print_timestamps;

    #ifdef PER_THREAD_STATS
    char buf[4096];
    const size_t size = 4096;
    #endif
    #endif

    debug_start(handle_args);

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

    debug_end(handle_args);

    debug_start(memset_work);
    struct work dir;
    /* memset(&dir, 0, sizeof(struct work)); */
    debug_end(memset_work);

    /* parse the directory data */
    debug_start(dir_linetowork);
    linetowork(w->line, in.delim, &dir);
    debug_end(dir_linetowork);

    /* create the directory */
    debug_start(dupdir_call);
    char topath[MAXPATH];
    SNFORMAT_S(topath, MAXPATH, 3, in.nameto, strlen(in.nameto), "/", (size_t) 1, dir.name, strlen(dir.name));
    if (dupdir(topath, &dir.statuso)) {
        const int err = errno;
        fprintf(stderr, "Dupdir failure: %d %s\n", err, strerror(err));
        row_destroy(w);
        return 1;
    }
    debug_end(dupdir_call);

    debug_start(copy_template_call);

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

    debug_end(copy_template_call);

    /* process the work */
    debug_start(opendb_call);
    sqlite3 * db = opendb(dbname, RDWR, 1, 0,
                          NULL, NULL
                          #ifdef DEBUG
                          , NULL, NULL
                          , NULL, NULL
                          , NULL, NULL
                          , NULL, NULL
                          #endif
                          );
    debug_end(opendb_call);

    if (db) {
        debug_start(zero_summary);
        struct sum summary;
        zeroit(&summary);
        debug_end(zero_summary);

        debug_start(insertdbprep_call);
        sqlite3_stmt * res = insertdbprep(db);
        debug_end(insertdbprep_call);

        debug_start(startdb_call);
        startdb(db);
        debug_end(startdb_call);

        /* move the trace file to the offet */
        debug_start(fseek_call);
        fseek(trace, w->offset, SEEK_SET);
        debug_end(fseek_call);

        debug_start(read_entries);
        size_t row_count = 0;
        for(size_t i = 0; i < w->entries; i++) {
            debug_start(getline_call);
            char * line = NULL;
            size_t len = 0;
            if (getline(&line, &len, trace) == -1) {
                free(line);
                break;
            }
            debug_end(getline_call);

            debug_start(memset_row);
            struct work row;
            memset(&row, 0, sizeof(struct work));
            debug_end(memset_row);

            debug_start(entry_linetowork);
            linetowork(line, in.delim, &row);
            debug_end(entry_linetowork)

            debug_start(free_call);
            free(line);
            debug_end(free_call);

            /* /\* don't need this now because this loop uses the count acquired by the scout function *\/ */
            /* /\* stop on directories, since files are listed first *\/ */
            /* if (row.type[0] == 'd') { */
            /*     break; */
            /* } */

            /* update summary table */
            debug_start(sumit_call);
            sumit(&summary,&row);
            debug_end(sumit_call);

            /* dont't record pinode */
            row.pinode = 0;

            /* add row to bulk insert */
            debug_start(insertdbgo_call);
            insertdbgo(&row,db,res);
            debug_end(insertdbgo_call);

            row_count++;
            if (row_count > 100000) {
                debug_start(stopdb_call);
                stopdb(db);
                debug_end(stopdb_call);

                debug_start(startdb_call);
                startdb(db);
                debug_end(startdb_call);

                #ifdef DEBUG
                timestamp_start(print_timestamps);
                #ifdef PER_THREAD_STATS
                print_debug(&debug_output_buffers, id, buf, size, "startdb",          &startdb_call);
                print_debug(&debug_output_buffers, id, buf, size, "stopdb",           &stopdb_call);
                #endif
                debug_end(print_timestamps);
                #ifdef CUMULATIVE_TIMES
                thread_startdb      += since_epoch(&startdb_call.end) - since_epoch(&startdb_call.start);
                thread_stopdb       += since_epoch(&stopdb_call.end) - since_epoch(&stopdb_call.start);
                #endif
                #ifdef PER_THREAD_STATS
                print_debug(&debug_output_buffers, id, buf, size, "print_timestamps", &print_timestamps);
                #endif
                #endif

                row_count=0;
            }

            #ifdef DEBUG
            timestamp_start(print_timestamps);
            #ifdef PER_THREAD_STATS
            print_debug(&debug_output_buffers, id, buf, size, "getline",          &getline_call);
            print_debug(&debug_output_buffers, id, buf, size, "memset_row",       &memset_row);
            print_debug(&debug_output_buffers, id, buf, size, "entry_linetowork", &entry_linetowork);
            print_debug(&debug_output_buffers, id, buf, size, "free",             &free_call);
            print_debug(&debug_output_buffers, id, buf, size, "sumit",            &sumit_call);
            print_debug(&debug_output_buffers, id, buf, size, "insertdbgo",       &insertdbgo_call);
            #endif
            debug_end(print_timestamps);

            #ifdef CUMULATIVE_TIMES
            thread_getline          += since_epoch(&getline_call.end) - since_epoch(&getline_call.start);
            thread_memset_row       += since_epoch(&memset_row.end) - since_epoch(&memset_row.start);
            thread_entry_linetowork += since_epoch(&entry_linetowork.end) - since_epoch(&entry_linetowork.start);
            thread_free             += since_epoch(&free_call.end) - since_epoch(&free_call.start);
            thread_sumit            += since_epoch(&sumit_call.end) - since_epoch(&sumit_call.start);
            thread_insertdbgo       += since_epoch(&insertdbgo_call.end) - since_epoch(&insertdbgo_call.start);
            #endif

            #ifdef PER_THREAD_STATS
            print_debug(&debug_output_buffers, id, buf, size, "print_timestamps", &print_timestamps);
            #endif
            #endif
        }

        debug_end(read_entries);

        debug_start(stopdb_call);
        stopdb(db);
        debug_end(stopdb_call);

        debug_start(insertdbfin_call);
        insertdbfin(res);
        debug_end(insertdbfin_call);

        debug_start(insertsumdb_call);
        insertsumdb(db, &dir, &summary);
        debug_end(insertsumdb_call);

        debug_start(closedb_call);
        closedb(db); /* don't set to nullptr */
        debug_end(closedb_call);

        #ifdef DEBUG
        timestamp_start(print_timestamps);
        #ifdef PER_THREAD_STATS
        print_debug(&debug_output_buffers, id, buf, size, "zero_summary", &zero_summary);
        print_debug(&debug_output_buffers, id, buf, size, "insertdbprep", &insertdbprep_call);
        print_debug(&debug_output_buffers, id, buf, size, "startdb",      &startdb_call);
        print_debug(&debug_output_buffers, id, buf, size, "fseek",        &fseek_call);
        print_debug(&debug_output_buffers, id, buf, size, "read_entries", &read_entries);
        print_debug(&debug_output_buffers, id, buf, size, "read_entries", &read_entries);
        print_debug(&debug_output_buffers, id, buf, size, "stopdb",       &stopdb_call);
        print_debug(&debug_output_buffers, id, buf, size, "insertdbfin",  &insertdbfin_call);
        print_debug(&debug_output_buffers, id, buf, size, "insertsumdb",  &insertsumdb_call);
        print_debug(&debug_output_buffers, id, buf, size, "closedb",      &closedb_call);
        #endif
        debug_end(print_timestamps);

        #ifdef CUMULATIVE_TIMES
        thread_zero_summary += since_epoch(&zero_summary.end) - since_epoch(&zero_summary.start);
        thread_insertdbprep += since_epoch(&insertdbprep_call.end) - since_epoch(&insertdbprep_call.start);
        thread_startdb      += since_epoch(&startdb_call.end) - since_epoch(&startdb_call.start);
        thread_fseek        += since_epoch(&fseek_call.end) - since_epoch(&fseek_call.start);
        thread_read_entries += since_epoch(&read_entries.end) - since_epoch(&read_entries.start);
        thread_stopdb       += since_epoch(&stopdb_call.end) - since_epoch(&stopdb_call.start);
        thread_insertdbfin  += since_epoch(&insertdbfin_call.end) - since_epoch(&insertdbfin_call.start);
        thread_insertsumdb  += since_epoch(&insertsumdb_call.end) - since_epoch(&insertsumdb_call.start);
        thread_closedb      += since_epoch(&closedb_call.end) - since_epoch(&closedb_call.start);
        thread_files        += row_count;
        #endif

        #ifdef PER_THREAD_STATS
        print_debug(&debug_output_buffers, id, buf, size, "print_timestamps", &print_timestamps);
        #endif
        #endif
    }

    debug_start(row_destroy_call);
    row_destroy(w);
    debug_end(row_destroy_call);

    #ifdef DEBUG
    timestamp_start(print_timestamps);
    #ifdef PER_THREAD_STATS
    print_debug(&debug_output_buffers, id, buf, size, "handle_args",      &handle_args);
    print_debug(&debug_output_buffers, id, buf, size, "memset_work",      &memset_work);
    print_debug(&debug_output_buffers, id, buf, size, "dir_linetowork",   &dir_linetowork);
    print_debug(&debug_output_buffers, id, buf, size, "dupdir",           &dupdir_call);
    print_debug(&debug_output_buffers, id, buf, size, "copy_template",    &copy_template_call);
    print_debug(&debug_output_buffers, id, buf, size, "opendb",           &opendb_call);
    print_debug(&debug_output_buffers, id, buf, size, "row_destroy",      &row_destroy_call);
    #endif
    debug_end(print_timestamps);
    #ifdef CUMULATIVE_TIMES
    thread_handle_args     += since_epoch(&handle_args.end) - since_epoch(&handle_args.start);
    thread_memset_work     += since_epoch(&memset_work.end) - since_epoch(&memset_work.start);
    thread_dir_linetowork  += since_epoch(&dir_linetowork.end) - since_epoch(&dir_linetowork.start);
    thread_dupdir          += since_epoch(&dupdir_call.end) - since_epoch(&dupdir_call.start);
    thread_copy_template   += since_epoch(&copy_template_call.end) - since_epoch(&copy_template_call.start);
    thread_opendb          += since_epoch(&opendb_call.end) - since_epoch(&opendb_call.start);
    thread_row_destroy     += since_epoch(&row_destroy_call.end) - since_epoch(&row_destroy_call.start);

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
    print_debug(&debug_output_buffers, id, buf, size, "print_timestamps", &print_timestamps);
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
    struct start_end scouting;
    clock_gettime(CLOCK_MONOTONIC, &scouting.start);

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

    clock_gettime(CLOCK_MONOTONIC, &scouting.end);

    pthread_mutex_lock(&print_mutex);
    fprintf(stderr, "Scout finished in %.2Lf seconds\n", elapsed(&scouting));
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
    /* have to call clock_gettime explicitly to get start time and epoch */
    struct start_end main_call;
    clock_gettime(CLOCK_MONOTONIC, &main_call.start);
    epoch = since_epoch(&main_call.start);

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

    /* have to call clock_gettime explicitly to get end time */
    clock_gettime(CLOCK_MONOTONIC, &main_call.end);

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

    fprintf(stderr, "main completed %.2Lf seconds\n", elapsed(&main_call));

    return 0;
}
