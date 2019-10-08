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

-----
NOTE:
-----

GUFI uses the C-Thread-Pool library.  The original version, written by
Johan Hanssen Seferidis, is found at
https://github.com/Pithikos/C-Thread-Pool/blob/master/LICENSE, and is
released under the MIT License.  LANS, LLC added functionality to the
original work.  The original work, plus LANS, LLC added functionality is
found at https://github.com/jti-lanl/C-Thread-Pool, also under the MIT
License.  The MIT License can be found at
https://opensource.org/licenses/MIT.


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



#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <unistd.h>
#include <utime.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "dbutils.h"
#ifndef DEBUG
#include "opendb.h"
#endif
#include "outdbs.h"
#include "outfiles.h"
#include "OutputBuffers.h"
#include "pcre.h"
#include "SinglyLinkedList.h"
#include "utils.h"

extern int errno;
#define AGGREGATE_NAME         "file:aggregate%d?mode=memory&cache=shared"
#define AGGREGATE_ATTACH_NAME  "aggregate"

#if BENCHMARK
#include "debug.h"
#endif

#ifdef DEBUG
#include "debug.h"

struct start_end {
    struct timespec start;
    struct timespec end;
};

struct buffer {
    struct start_end data[7000];
    size_t i;
};

struct start_end * buffer_get(struct buffer * buffer) {
    return &(buffer->data[buffer->i++]);
}

struct descend_timers {
    struct buffer within_descend;
    struct buffer check_args;
    struct buffer level;
    struct buffer level_branch;
    struct buffer while_branch;
    struct buffer readdir;
    struct buffer readdir_branch;
    struct buffer strncmp;
    struct buffer strncmp_branch;
    struct buffer snprintf;
    struct buffer lstat;
    struct buffer isdir;
    struct buffer isdir_branch;
    struct buffer access;
    struct buffer set;
    struct buffer clone;
    struct buffer pushdir;
};

struct descend_timers * global_timers = NULL;

#ifdef PER_THREAD_STATS
extern struct OutputBuffers debug_output_buffers;

void print_timers(struct OutputBuffers * obufs, const size_t id, char * buf, const size_t size, const char * name, struct buffer * timers) {
    for(size_t i = 0; i < timers->i; i++) {
        print_debug(obufs, id, buf, size, name, &timers->data[i].start, &timers->data[i].end);
    }
}
#endif

#ifdef CUMULATIVE_TIMES
long double total_opendir_time = 0;
long double total_open_time = 0;
long double total_sqlite3_open_time = 0;
long double total_create_tables_time = 0;
long double total_set_pragmas_time = 0;
long double total_load_extension_time = 0;
long double total_addqueryfuncs_time = 0;
long double total_descend_time = 0;
long double total_check_args_time = 0;
long double total_level_time = 0;
long double total_level_branch_time = 0;
long double total_while_branch_time = 0;
long double total_readdir_time = 0;
long double total_readdir_branch_time = 0;
long double total_strncmp_time = 0;
long double total_strncmp_branch_time = 0;
long double total_snprintf_time = 0;
long double total_lstat_time = 0;
long double total_isdir_time = 0;
long double total_isdir_branch_time = 0;
long double total_access_time = 0;
long double total_set_time = 0;
long double total_clone_time = 0;
long double total_pushdir_time = 0;
long double total_attach_time = 0;
long double total_exec_time = 0;
long double total_detach_time = 0;
long double total_close_time = 0;
long double total_closedir_time = 0;
long double total_utime_time = 0;
long double total_free_work_time = 0;
long double total_output_timestamps_time = 0;

long double buffer_sum(struct buffer * timer) {
    long double sum = 0;
    for(size_t i = 0; i < timer->i; i++) {
        sum += elapsed(&timer->data[i].start, &timer->data[i].end);
    }

    return sum;
}
#endif

static const char GUFI_SQLITE_VFS[] = "unix-none";

int create_tables(const char *name, sqlite3 *db);
int set_pragmas(sqlite3 * db);

static sqlite3 * opendb2(const char * name, const int rdonly, const int createtables, const int setpragmas
                         , struct timespec * sqlite3_open_start
                         , struct timespec * sqlite3_open_end
                         , struct timespec * create_tables_start
                         , struct timespec * create_tables_end
                         , struct timespec * set_pragmas_start
                         , struct timespec * set_pragmas_end
                         , struct timespec * load_extension_start
                         , struct timespec * load_extension_end
    ) {
    sqlite3 * db = NULL;

    if (rdonly && createtables) {
        fprintf(stderr, "Cannot open database: readonly and createtables cannot both be set at the same time\n");
        return NULL;
    }

    clock_gettime(CLOCK_MONOTONIC, sqlite3_open_start);

    int flags = SQLITE_OPEN_URI;
    if (rdonly) {
        flags |= SQLITE_OPEN_READONLY;
    }
    else {
        flags |= SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE;
    }

    if (sqlite3_open_v2(name, &db, flags, GUFI_SQLITE_VFS) != SQLITE_OK) {
        clock_gettime(CLOCK_MONOTONIC, sqlite3_open_end);
        /* fprintf(stderr, "Cannot open database: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db)); */
        sqlite3_close(db); /* close db even if it didn't open to avoid memory leaks */
        return NULL;
    }
    clock_gettime(CLOCK_MONOTONIC, sqlite3_open_end);

    clock_gettime(CLOCK_MONOTONIC, create_tables_start);
    if (createtables) {
        if (create_tables(name, db) != 0) {
            fprintf(stderr, "Cannot create tables: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db));
            sqlite3_close(db);
            return NULL;
        }
    }
    clock_gettime(CLOCK_MONOTONIC, create_tables_end);

    clock_gettime(CLOCK_MONOTONIC, set_pragmas_start);
    if (setpragmas) {
        /* ignore errors */
        set_pragmas(db);
    }
    clock_gettime(CLOCK_MONOTONIC, set_pragmas_end);

    clock_gettime(CLOCK_MONOTONIC, load_extension_start);
    if ((sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, NULL) != SQLITE_OK) || /* enable loading of extensions */
        (sqlite3_extension_init(db, NULL, NULL)                                != SQLITE_OK)) { /* load the sqlite3-pcre extension */
        fprintf(stderr, "Unable to load regex extension\n");
        sqlite3_close(db);
        db = NULL;
    }
    clock_gettime(CLOCK_MONOTONIC, load_extension_end);

    return db;
}
#endif

int processdir(struct QPTPool * ctx, const size_t id, void * data, void * args);

/* Push the subdirectories in the current directory onto the queue */
static size_t descend2(struct QPTPool *ctx,
                       const size_t id,
                       struct work *passmywork,
                       DIR *dir,
                       const size_t max_level
                       #ifdef DEBUG
                       , struct descend_timers * timers
                       #endif
    ) {
    #ifdef DEBUG
    struct start_end * within_descend = buffer_get(&timers->within_descend);
    clock_gettime(CLOCK_MONOTONIC, &within_descend->start);
    #endif

    #ifdef DEBUG
    struct start_end * check_args = buffer_get(&timers->check_args);
    clock_gettime(CLOCK_MONOTONIC, &check_args->start);
    #endif
    /* passmywork was already checked in the calling thread */
    /* if (!passmywork) { */
    /*     fprintf(stderr, "Got NULL work\n"); */
    /*     return 0; */
    /* } */

    /* dir was already checked in the calling thread */
    /* if (!dir) { */
    /*     fprintf(stderr, "Could not open directory %s: %d %s\n", passmywork->name, errno, strerror(errno)); */
    /*     return 0; */
    /* } */
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &check_args->end);
    #endif

    #ifdef DEBUG
    struct start_end * level_cmp = buffer_get(&timers->level);
    clock_gettime(CLOCK_MONOTONIC, &level_cmp->start);
    #endif
    size_t pushed = 0;

    const size_t next_level = passmywork->level + 1;
    const int level_check = (next_level <= max_level);
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &level_cmp->end);
    #endif

    #ifdef DEBUG
    struct start_end * level_branch = buffer_get(&timers->level_branch);
    clock_gettime(CLOCK_MONOTONIC, &level_branch->start);
    #endif

    if (level_check) {
        #ifdef DEBUG
        clock_gettime(CLOCK_MONOTONIC, &level_branch->end);
        #endif

        /* go ahead and send the subdirs to the queue since we need to look */
        /* further down the tree.  loop over dirents, if link push it on the */
        /* queue, if file or link print it, fill up qwork structure for */
        /* each */
        #ifdef DEBUG
        struct start_end * while_branch = buffer_get(&timers->while_branch);
        clock_gettime(CLOCK_MONOTONIC, &while_branch->start);
        #endif
        while (1) {
            #ifdef DEBUG
            struct start_end * readdir_call = buffer_get(&timers->readdir);
            clock_gettime(CLOCK_MONOTONIC, &readdir_call->start);
            #endif
            struct dirent * entry = readdir(dir);
            #ifdef DEBUG
            clock_gettime(CLOCK_MONOTONIC, &readdir_call->end);
            #endif

            #ifdef DEBUG
            struct start_end * readdir_branch = buffer_get(&timers->readdir_branch);
            clock_gettime(CLOCK_MONOTONIC, &readdir_branch->start);
            #endif
            if (!entry) {
                #ifdef DEBUG
                clock_gettime(CLOCK_MONOTONIC, &readdir_branch->end);
                #endif
                break;
            }
            else {
                #ifdef DEBUG
                clock_gettime(CLOCK_MONOTONIC, &readdir_branch->end);
                #endif
            }

            #ifdef DEBUG
            struct start_end * strncmp_call = buffer_get(&timers->strncmp);
            clock_gettime(CLOCK_MONOTONIC, &strncmp_call->start);
            #endif
            const size_t len = strlen(entry->d_name);
            const int skip = (((len == 1) && (strncmp(entry->d_name, ".",  1) == 0)) ||
                              ((len == 2) && (strncmp(entry->d_name, "..", 2) == 0)));
            #ifdef DEBUG
            clock_gettime(CLOCK_MONOTONIC, &strncmp_call->end);
            #endif

            #ifdef DEBUG
            struct start_end * strncmp_branch = buffer_get(&timers->strncmp_branch);
            clock_gettime(CLOCK_MONOTONIC, &strncmp_branch->start);
            #endif
            if (skip) {
                #ifdef DEBUG
                clock_gettime(CLOCK_MONOTONIC, &strncmp_branch->end);
                #endif

                continue;
            }
            else {
                #ifdef DEBUG
                clock_gettime(CLOCK_MONOTONIC, &strncmp_branch->end);
                #endif
            }

            #ifdef DEBUG
            struct start_end * snprintf_call = buffer_get(&timers->snprintf);
            clock_gettime(CLOCK_MONOTONIC, &snprintf_call->start);
            #endif
            struct work qwork;
            SNFORMAT_S(qwork.name, MAXPATH, 3, passmywork->name, strlen(passmywork->name), "/", (size_t) 1, entry->d_name, len);
            #ifdef DEBUG
            clock_gettime(CLOCK_MONOTONIC, &snprintf_call->end);
            #endif

            /* #ifdef DEBUG */
            /* struct start_end * lstat_call = buffer_get(&timers->lstat); */
            /* clock_gettime(CLOCK_MONOTONIC, &lstat_call->start); */
            /* #endif */
            /* lstat(qwork.name, &qwork.statuso); */
            /* #ifdef DEBUG */
            /* clock_gettime(CLOCK_MONOTONIC, &lstat_call->end); */
            /* #endif */

            #ifdef DEBUG
            struct start_end * isdir_call = buffer_get(&timers->isdir);
            clock_gettime(CLOCK_MONOTONIC, &isdir_call->start);
            #endif
            const int isdir = (entry->d_type == DT_DIR);
            /* const int isdir = S_ISDIR(qwork.statuso.st_mode); */
            #ifdef DEBUG
            clock_gettime(CLOCK_MONOTONIC, &isdir_call->end);
            #endif

            #ifdef DEBUG
            struct start_end * isdir_branch = buffer_get(&timers->isdir_branch);
            clock_gettime(CLOCK_MONOTONIC, &isdir_branch->start);
            #endif
            if (isdir) {
                #ifdef DEBUG
                clock_gettime(CLOCK_MONOTONIC, &isdir_branch->end);
                #endif

                /* const int accessible = !access(qwork.name, R_OK | X_OK); */

                /* if (accessible) { */
                    #ifdef DEBUG
                    struct start_end * set = buffer_get(&timers->set);
                    clock_gettime(CLOCK_MONOTONIC, &set->start);
                    #endif
                    qwork.level = next_level;
                    /* qwork.type[0] = 'd'; */

                    /* this is how the parent gets passed on */
                    /* qwork.pinode = passmywork->statuso.st_ino; */
                    #ifdef DEBUG
                    clock_gettime(CLOCK_MONOTONIC, &set->end);
                    #endif

                    #ifdef DEBUG
                    struct start_end * make_clone = buffer_get(&timers->clone);
                    clock_gettime(CLOCK_MONOTONIC, &make_clone->start);
                    #endif

                    /* make a clone here so that the data can be pushed into the queue */
                    /* this is more efficient than malloc+free for every single entry */
                    struct work * clone = (struct work *) malloc(sizeof(struct work));
                    memcpy(clone, &qwork, sizeof(struct work));

                    #ifdef DEBUG
                    clock_gettime(CLOCK_MONOTONIC, &make_clone->end);
                    #endif

                    #ifdef DEBUG
                    struct start_end * pushdir = buffer_get(&timers->pushdir);
                    clock_gettime(CLOCK_MONOTONIC, &pushdir->start);
                    #endif
                    /* push the subdirectory into the queue for processing */
                    QPTPool_enqueue(ctx, id, processdir, clone);
                    #ifdef DEBUG
                    clock_gettime(CLOCK_MONOTONIC, &pushdir->end);
                    #endif

                    pushed++;
                /* } */
                /* else { */
                /*     fprintf(stderr, "couldn't access dir '%s': %s\n", */
                /*             qwork->name, strerror(errno)); */
                /* } */
            }
            else {
                #ifdef DEBUG
                clock_gettime(CLOCK_MONOTONIC, &isdir_branch->end);
                #endif
            }
        }
        #ifdef DEBUG
        clock_gettime(CLOCK_MONOTONIC, &while_branch->end);
        #endif
    }
    else {
        #ifdef DEBUG
        clock_gettime(CLOCK_MONOTONIC, &level_branch->end);
        #endif
    }

    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &within_descend->end);
    #endif

    return pushed;
}

/* ////////////////////////////////////////////////// */
/* these functions need to be moved back into dbutils */
static void path2(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    const size_t id = QPTPool_get_index((struct QPTPool *) sqlite3_user_data(context), pthread_self());
    sqlite3_result_text(context, gps[id].gpath, -1, SQLITE_TRANSIENT);

    return;
}

int addqueryfuncs2(sqlite3 *db, struct QPTPool * ctx) {
    return !(sqlite3_create_function(db, "path", 0, SQLITE_UTF8, ctx, &path2, NULL, NULL) == SQLITE_OK);
}
/* ////////////////////////////////////////////////// */

/* sqlite3_exec callback argument data */
struct CallbackArgs {
    struct OutputBuffers * output_buffers;
    int id;
};

static int print_callback(void * args, int count, char **data, char **columns) {
    /* skip argument checking */
    /* if (!args) { */
    /*     return 1; */
    /* } */

    struct CallbackArgs * ca = (struct CallbackArgs *) args;
    const int id = ca->id;
    /* if (gts.outfd[id]) { */
        size_t * lens = malloc(count * sizeof(size_t));
        size_t row_len = count + 1; /* one delimiter per column + newline */
        for(int i = 0; i < count; i++) {
            lens[i] = strlen(data[i]);
            row_len += lens[i];
        }

        const size_t capacity = ca->output_buffers->buffers[id].capacity;

        /* if the row can fit within an empty buffer, try to add the new row to the buffer */
        if (row_len < capacity) {
            /* if there's not enough space in the buffer to fit the new row, flush it first */
            if ((ca->output_buffers->buffers[id].filled + row_len) >= capacity) {
                OutputBuffer_flush(&ca->output_buffers->mutex, &ca->output_buffers->buffers[id], gts.outfd[id]);
            }

            char * buf = ca->output_buffers->buffers[id].buf;
            size_t filled = ca->output_buffers->buffers[id].filled;
            for(int i = 0; i < count; i++) {
                memcpy(&buf[filled], data[i], lens[i]);
                filled += lens[i];

                buf[filled] = in.delim[0];
                filled++;
            }

            buf[filled] = '\n';
            filled++;

            ca->output_buffers->buffers[id].filled = filled;
            ca->output_buffers->buffers[id].count++;
        }
        else {
            /* if the row does not fit the buffer, output immediately instead of buffering */
            pthread_mutex_lock(&ca->output_buffers->mutex);
            for(int i = 0; i < count; i++) {
                fwrite(data[i], sizeof(char), lens[i], gts.outfd[id]);
                fwrite(in.delim, sizeof(char), 1, gts.outfd[id]);
            }
            fwrite("\n", sizeof(char), 1, gts.outfd[id]);
            ca->output_buffers->buffers[id].count++;
            pthread_mutex_unlock(&ca->output_buffers->mutex);
        }

        free(lens);
    /* } */
    return 0;
}

struct ThreadArgs {
    struct OutputBuffers output_buffers;
    int (*print_callback_func)(void*,int,char**,char**);
};

int processdir(struct QPTPool * ctx, const size_t id, void * data, void * args) {
    sqlite3 *db = NULL;
    int recs;
    int trecs;
    char shortname[MAXPATH];
    char endname[MAXPATH];
    DIR * dir = NULL;

    /* /\* Can probably skip this *\/ */
    /* if (!data) { */
    /*     return 1; */
    /* } */

    /* /\* Can probably skip this *\/ */
    /* if (!ctx || (id >= ctx->size)) { */
    /*     free(data); */
    /*     return 1; */
    /* } */

    struct work * work = (struct work *) data;
    const size_t work_name_len = strlen(work->name);

    char dbname[MAXSQL];
    SNFORMAT_S(dbname, MAXSQL, 2, work->name, work_name_len, "/" DBNAME, DBNAME_LEN + 1);

    #ifdef DEBUG
    struct timespec opendir_start;
    struct timespec opendir_end;
    struct timespec open_start;
    struct timespec open_end;
    struct timespec sqlite3_open_start;
    struct timespec sqlite3_open_end;
    struct timespec create_tables_start;
    struct timespec create_tables_end;
    struct timespec set_pragmas_start;
    struct timespec set_pragmas_end;
    struct timespec load_extension_start;
    struct timespec load_extension_end;
    struct timespec addqueryfuncs_start;
    struct timespec addqueryfuncs_end;
    struct timespec descend_start;
    struct timespec descend_end;
    struct descend_timers * descend_timers = &global_timers[id];
    struct timespec attach_start;
    struct timespec attach_end;
    struct timespec exec_start;
    struct timespec exec_end;
    struct timespec detach_start;
    struct timespec detach_end;
    struct timespec close_start;
    struct timespec close_end;
    struct timespec closedir_start;
    struct timespec closedir_end;
    struct timespec utime_start;
    struct timespec utime_end;
    struct timespec free_work_start;
    struct timespec free_work_end;

    memset(&opendir_start, 0, sizeof(struct timespec));
    memset(&opendir_end, 0, sizeof(struct timespec));
    memset(&open_start, 0, sizeof(struct timespec));
    memset(&open_end, 0, sizeof(struct timespec));
    memset(&sqlite3_open_start, 0, sizeof(struct timespec));
    memset(&sqlite3_open_end, 0, sizeof(struct timespec));
    memset(&create_tables_start, 0, sizeof(struct timespec));
    memset(&create_tables_end, 0, sizeof(struct timespec));
    memset(&set_pragmas_start, 0, sizeof(struct timespec));
    memset(&set_pragmas_end, 0, sizeof(struct timespec));
    memset(&load_extension_start, 0, sizeof(struct timespec));
    memset(&load_extension_end, 0, sizeof(struct timespec));
    memset(&addqueryfuncs_start, 0, sizeof(struct timespec));
    memset(&addqueryfuncs_end, 0, sizeof(struct timespec));
    memset(&descend_start, 0, sizeof(struct timespec));
    memset(&descend_end, 0, sizeof(struct timespec));
    descend_timers->within_descend.i = 0;
    descend_timers->check_args.i = 0;
    descend_timers->level.i = 0;
    descend_timers->level_branch.i = 0;
    descend_timers->while_branch.i = 0;
    descend_timers->readdir.i = 0;
    descend_timers->readdir_branch.i = 0;
    descend_timers->strncmp.i = 0;
    descend_timers->strncmp_branch.i = 0;
    descend_timers->snprintf.i = 0;
    descend_timers->lstat.i = 0;
    descend_timers->isdir.i = 0;
    descend_timers->isdir_branch.i = 0;
    descend_timers->access.i = 0;
    descend_timers->set.i = 0;
    descend_timers->clone.i = 0;
    descend_timers->pushdir.i = 0;
    memset(&attach_start, 0, sizeof(struct timespec));
    memset(&attach_end, 0, sizeof(struct timespec));
    memset(&exec_start, 0, sizeof(struct timespec));
    memset(&exec_end, 0, sizeof(struct timespec));
    memset(&detach_start, 0, sizeof(struct timespec));
    memset(&detach_end, 0, sizeof(struct timespec));
    memset(&close_start, 0, sizeof(struct timespec));
    memset(&close_end, 0, sizeof(struct timespec));
    memset(&closedir_start, 0, sizeof(struct timespec));
    memset(&closedir_end, 0, sizeof(struct timespec));
    memset(&utime_start, 0, sizeof(struct timespec));
    memset(&utime_end, 0, sizeof(struct timespec));
    memset(&free_work_start, 0, sizeof(struct timespec));
    memset(&free_work_end, 0, sizeof(struct timespec));
    #endif

    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &opendir_start);
    #endif
    /* keep opendir near opendb to help speed up sqlite3_open_v2 */
    dir = opendir(work->name);
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &opendir_end);
    #endif

    /* if the directory can't be opened, don't bother with anything else */
    if (!dir) {
        /* fprintf(stderr, "Could not open directory %s: %d %s\n", work->name, errno, strerror(errno)); */
        goto out_free;
    }

    /* if we have out db then we have that db open so we just attach the gufi db */
    #ifndef NO_OPENDB
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &open_start);
    #endif
    if (in.outdb > 0) {
      db = gts.outdbd[id];
      attachdb(dbname, db, "tree");
    } else {
      db = opendb2(dbname, in.readonly, 0, 0
                   #ifdef DEBUG
                   , &sqlite3_open_start
                   , &sqlite3_open_end
                   , &create_tables_start
                   , &create_tables_end
                   , &set_pragmas_start
                   , &set_pragmas_end
                   , &load_extension_start
                   , &load_extension_end
                   #endif
          );
    }
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &open_end);
    #endif
    #endif

    #ifndef NO_ADDQUERYFUNCS
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &addqueryfuncs_start);
    #endif
    /* this is needed to add some query functions like path() uidtouser() gidtogroup() */
    if (db) {
        addqueryfuncs2(db, ctx);
    }
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &addqueryfuncs_end);
    #endif
    #endif

    recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
            /* if it fails then this will be set to 1 and will go on */

    /* if AND operation, and sqltsum is there, run a query to see if there is a match. */
    /* if this is OR, as well as no-sql-to-run, skip this query */
    if (in.sqltsum_len > 1) {

       if (in.andor == 0) {      /* AND */
           trecs=rawquerydb(work->name, 0, db, (char *) "select name from sqlite_master where type=\'table\' and name=\'treesummary\';", 0, 0, 0, id);
         if (trecs<1) {
           recs=-1;
         } else {
           recs=rawquerydb(work->name, 0, db, in.sqltsum, 0, 0, 0, id);
         }
      }
      /* this is an OR or we got a record back. go on to summary/entries */
      /* queries, if not done with this dir and all dirs below it */
      /* this means that no tree table exists so assume we have to go on */
      if (recs < 0) {
        recs=1;
      }
    }
    /* so we have to go on and query summary and entries possibly */
    if (recs > 0) {
        #ifdef DEBUG
        clock_gettime(CLOCK_MONOTONIC, &descend_start);
        #ifdef SUBDIRECTORY_COUNTS
        const size_t pushed =
        #endif
        #endif
        /* push subdirectories into the queue */
        descend2(ctx, id, work, dir, in.max_level
                 #ifdef DEBUG
                 , descend_timers
                 #endif
            );
        #ifdef DEBUG
        clock_gettime(CLOCK_MONOTONIC, &descend_end);
        #ifdef SUBDIRECTORY_COUNTS
        pthread_mutex_lock(&print_mutex);
        fprintf(stderr, "%s %zu\n", work->name, pushed);
        pthread_mutex_unlock(&print_mutex);
        #endif
        #endif

        if (db) {
            /* only query this level if the min_level has been reached */
            if (work->level >= in.min_level) {
                /* run query on summary, print it if printing is needed, if returns none */
                /* and we are doing AND, skip querying the entries db */
                /* memset(endname, 0, sizeof(endname)); */
                shortpath(work->name,shortname,endname);
                SNFORMAT_S(gps[id].gepath, MAXPATH, 1, endname, strlen(endname));

                if (in.sqlsum_len > 1) {
                    recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
                    /* for directories we have to take off after the last slash */
                    /* and set the path so users can put path() in their queries */
                    SNFORMAT_S(gps[id].gpath, MAXPATH, 1, shortname, strlen(shortname));
                    /* printf("processdir: setting gpath = %s and gepath %s\n",gps[mytid].gpath,gps[mytid].gepath); */
                    realpath(work->name,gps[id].gfpath);
                    recs = rawquerydb(work->name, 1, db, in.sqlsum, 1, 0, 0, id);
                    /* printf("summary ran %s on %s returned recs %d\n",in.sqlsum,work->name,recs); */
                } else {
                    recs = 1;
                }
                if (in.andor > 0) {
                    recs = 1;
                }

                /* if we have recs (or are running an OR) query the entries table */
                if (recs > 0) {
                    if (in.sqlent_len > 1) {
                         #ifdef DEBUG
                         clock_gettime(CLOCK_MONOTONIC, &attach_start);
                         #endif
                         if (in.aggregate_or_print == AGGREGATE) {
                             /* attach in-memory result aggregation database */
                             char intermediate_name[MAXSQL];
                             SNPRINTF(intermediate_name, MAXSQL, AGGREGATE_NAME, (int) id);
                             if (db && !attachdb(intermediate_name, db, AGGREGATE_ATTACH_NAME)) {
                                 fprintf(stderr, "Could not attach database\n");
                                 closedb(db);
                                 goto out_dir;
                             }
                         }
                        #ifdef DEBUG
                        clock_gettime(CLOCK_MONOTONIC, &attach_end);
                        #endif
                        /* set the path so users can put path() in their queries */
                        /* printf("****entries len of in.sqlent %lu\n",strlen(in.sqlent)); */
                        SNFORMAT_S(gps[id].gpath, MAXPATH, 1, work->name, work_name_len);
                        realpath(work->name,gps[id].gfpath);

                        #ifndef NO_SQL_EXEC
                        struct ThreadArgs * ta = (struct ThreadArgs *) args;
                        struct CallbackArgs ca;
                        ca.output_buffers = &ta->output_buffers;
                        ca.id = id;

                        #ifdef DEBUG
                        clock_gettime(CLOCK_MONOTONIC, &exec_start);
                        #endif
                        char *err = NULL;
                        if (sqlite3_exec(db, in.sqlent, ta->print_callback_func, &ca, &err) != SQLITE_OK) {
                            fprintf(stderr, "Error: %s: %s\n", err, dbname);
                            sqlite3_free(err);
                        }
                        #ifdef DEBUG
                        clock_gettime(CLOCK_MONOTONIC, &exec_end);
                        #endif
                        #endif

                        #ifdef DEBUG
                        clock_gettime(CLOCK_MONOTONIC, &detach_start);
                        #endif
                        if (in.aggregate_or_print == AGGREGATE) {
                            /* detach in-memory result aggregation database */
                            if (db && !detachdb(AGGREGATE_NAME, db, AGGREGATE_ATTACH_NAME)) {
                                closedb(db);
                                goto out_dir;
                            }
                        }
                        #ifdef DEBUG
                        clock_gettime(CLOCK_MONOTONIC, &detach_end);
                        #endif
                    }
                }
            }
        }
    }

    #ifndef NO_OPENDB
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &close_start);
    #endif
    /* if we have an out db we just detach gufi db */
    if (in.outdb > 0) {
      detachdb(dbname, db, "tree");
    } else {
      closedb(db);
    }
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &close_end);
    #endif
    #endif

  out_dir:
    ;

    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &closedir_start);
    #endif
    closedir(dir);
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &closedir_end);
    #endif

    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &utime_start);
    #endif
    /* restore mtime and atime */
    if (in.keep_matime) {
        struct utimbuf dbtime = {};
        dbtime.actime  = work->statuso.st_atime;
        dbtime.modtime = work->statuso.st_mtime;
        utime(dbname, &dbtime);
    }
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &utime_end);
    #endif

  out_free:
    ;

    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &free_work_start);
    #endif
    free(work);
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &free_work_end);
    #endif

    #ifdef DEBUG
    struct timespec output_timestamps_start;
    clock_gettime(CLOCK_MONOTONIC, &output_timestamps_start);

    #ifdef PER_THREAD_STATS
    char buf[4096];
    const size_t size = sizeof(buf);
    print_debug (&debug_output_buffers, id, buf, size, "opendir",         &opendir_start, &opendir_end);
    print_debug (&debug_output_buffers, id, buf, size, "opendb",          &open_start, &open_end);
    print_debug (&debug_output_buffers, id, buf, size, "sqlite3_open_v2", &sqlite3_open_start, &sqlite3_open_end);
    print_debug (&debug_output_buffers, id, buf, size, "create_tables",   &create_tables_start, &create_tables_end);
    print_debug (&debug_output_buffers, id, buf, size, "set_pragmas",     &set_pragmas_start, &set_pragmas_end);
    print_debug (&debug_output_buffers, id, buf, size, "load_extensions", &load_extension_start, &load_extension_end);
    print_debug (&debug_output_buffers, id, buf, size, "addqueryfuncs",   &addqueryfuncs_start, &addqueryfuncs_end);
    print_debug (&debug_output_buffers, id, buf, size, "descend",         &descend_start, &descend_end);
    print_timers(&debug_output_buffers, id, buf, size, "within_descend",  &descend_timers->within_descend);
    print_timers(&debug_output_buffers, id, buf, size, "check_args",      &descend_timers->check_args);
    print_timers(&debug_output_buffers, id, buf, size, "level",           &descend_timers->level);
    print_timers(&debug_output_buffers, id, buf, size, "level_branch",    &descend_timers->level_branch);
    print_timers(&debug_output_buffers, id, buf, size, "while_branch",    &descend_timers->while_branch);
    print_timers(&debug_output_buffers, id, buf, size, "readdir",         &descend_timers->readdir);
    print_timers(&debug_output_buffers, id, buf, size, "readdir_branch",  &descend_timers->readdir_branch);
    print_timers(&debug_output_buffers, id, buf, size, "strncmp",         &descend_timers->strncmp);
    print_timers(&debug_output_buffers, id, buf, size, "strncmp_branch",  &descend_timers->strncmp_branch);
    print_timers(&debug_output_buffers, id, buf, size, "snprintf",        &descend_timers->snprintf);
    print_timers(&debug_output_buffers, id, buf, size, "lstat",           &descend_timers->lstat);
    print_timers(&debug_output_buffers, id, buf, size, "isdir",           &descend_timers->isdir);
    print_timers(&debug_output_buffers, id, buf, size, "isdir_branch",    &descend_timers->isdir_branch);
    print_timers(&debug_output_buffers, id, buf, size, "access",          &descend_timers->access);
    print_timers(&debug_output_buffers, id, buf, size, "set",             &descend_timers->set);
    print_timers(&debug_output_buffers, id, buf, size, "clone",           &descend_timers->clone);
    print_timers(&debug_output_buffers, id, buf, size, "pushdir",         &descend_timers->pushdir);
    print_debug (&debug_output_buffers, id, buf, size, "attach",          &attach_start, &attach_end);
    print_debug (&debug_output_buffers, id, buf, size, "sqlite3_exec",    &exec_start, &exec_end);
    print_debug (&debug_output_buffers, id, buf, size, "detach",          &detach_start, &detach_end);
    print_debug (&debug_output_buffers, id, buf, size, "closedb",         &close_start, &close_end);
    print_debug (&debug_output_buffers, id, buf, size, "closedir",        &closedir_start, &closedir_end);
    print_debug (&debug_output_buffers, id, buf, size, "utime",           &utime_start, &utime_end);
    #endif

    struct timespec output_timestamps_end;
    clock_gettime(CLOCK_MONOTONIC, &output_timestamps_end);

    #ifdef CUMULATIVE_TIMES
    pthread_mutex_lock(&print_mutex);
    total_opendir_time           += elapsed(&opendir_start, &opendir_end);
    total_open_time              += elapsed(&open_start, &open_end);
    total_sqlite3_open_time      += elapsed(&sqlite3_open_start, &sqlite3_open_end);
    total_create_tables_time     += elapsed(&create_tables_start, &create_tables_end);
    total_set_pragmas_time       += elapsed(&set_pragmas_start, &set_pragmas_end);
    total_load_extension_time    += elapsed(&load_extension_start, &load_extension_end);
    total_addqueryfuncs_time     += elapsed(&addqueryfuncs_start, &addqueryfuncs_end);
    total_descend_time           += elapsed(&descend_start, &descend_end);
    total_check_args_time        += buffer_sum(&descend_timers->check_args);
    total_level_time             += buffer_sum(&descend_timers->level);
    total_level_branch_time      += buffer_sum(&descend_timers->level_branch);
    total_while_branch_time      += buffer_sum(&descend_timers->while_branch);
    total_readdir_time           += buffer_sum(&descend_timers->readdir);
    total_readdir_branch_time    += buffer_sum(&descend_timers->readdir_branch);
    total_strncmp_time           += buffer_sum(&descend_timers->strncmp);
    total_strncmp_branch_time    += buffer_sum(&descend_timers->strncmp_branch);
    total_snprintf_time          += buffer_sum(&descend_timers->snprintf);
    total_lstat_time             += buffer_sum(&descend_timers->lstat);
    total_isdir_time             += buffer_sum(&descend_timers->isdir);
    total_isdir_branch_time      += buffer_sum(&descend_timers->isdir_branch);
    total_access_time            += buffer_sum(&descend_timers->access);
    total_set_time               += buffer_sum(&descend_timers->set);
    total_clone_time             += buffer_sum(&descend_timers->clone);
    total_pushdir_time           += buffer_sum(&descend_timers->pushdir);
    total_closedir_time          += elapsed(&closedir_start, &closedir_end);
    total_attach_time            += elapsed(&attach_start, &attach_end);
    total_exec_time              += elapsed(&exec_start, &exec_end);
    total_detach_time            += elapsed(&detach_start, &detach_end);
    total_close_time             += elapsed(&close_start, &close_end);
    total_utime_time             += elapsed(&utime_start, &utime_end);
    total_free_work_time         += elapsed(&free_work_start, &free_work_end);
    total_output_timestamps_time += elapsed(&output_timestamps_start, &output_timestamps_end);
    pthread_mutex_unlock(&print_mutex);
    #endif

    #ifdef PER_THREAD_STATS
    print_debug(&debug_output_buffers, id, buf, size, "output_timestamps", &output_timestamps_start, &output_timestamps_end);
    #endif

    #endif

    return 0;
}

void sub_help() {
   printf("GUFI_tree         find GUFI index-tree here\n");
   printf("\n");
}

static void cleanup_intermediates(sqlite3 **intermediates, const size_t count) {
    for(size_t i = 0; i < count; i++) {
        closedb(intermediates[i]);
    }
    free(intermediates);
}

int main(int argc, char *argv[])
{
    #if defined(DEBUG) || BENCHMARK
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    #endif

    #ifdef DEBUG
    epoch = timestamp(&start);
    #endif

    /* process input args - all programs share the common 'struct input', */
    /* but allow different fields to be filled at the command-line. */
    /* Callers provide the options-string for get_opt(), which will */
    /* control which options are parsed for each program. */
    int idx = parse_cmd_line(argc, argv, "hHT:S:E:an:o:d:O:I:F:y:z:G:J:e:m:B:w", 1, "GUFI_tree ...", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    struct timespec setup_globals_start;
    clock_gettime(CLOCK_MONOTONIC, &setup_globals_start);
    #endif

    struct ThreadArgs args;

    /* initialize globals */
    if (!outfiles_init(gts.outfd,  in.outfile, in.outfilen, in.maxthreads + 1)              ||
        !outdbs_init  (gts.outdbd, in.outdb,   in.outdbn,   in.maxthreads, in.sqlinit)      ||
        !OutputBuffers_init(&args.output_buffers, in.maxthreads + 1, in.output_buffer_size)) {
        return -1;
    }

    #ifdef DEBUG
    #ifdef PER_THREAD_STATS
    OutputBuffers_init(&debug_output_buffers, in.maxthreads, 1073741824ULL);
    #endif

    global_timers = malloc(in.maxthreads * sizeof(struct descend_timers));

    #ifdef CUMULATIVE_TIMES
    struct timespec setup_globals_end;
    clock_gettime(CLOCK_MONOTONIC, &setup_globals_end);
    const long double setup_globals_time = elapsed(&setup_globals_start, &setup_globals_end);
    #endif
    #endif

    #if BENCHMARK
    fprintf(stderr, "Querying GUFI Index");
    for(int i = idx; i < argc; i++) {
        fprintf(stderr, " %s", argv[i]);
    }
    fprintf(stderr, " with %d threads\n", in.maxthreads);
    #endif

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    struct timespec setup_aggregate_start;
    clock_gettime(CLOCK_MONOTONIC, &setup_aggregate_start);
    #endif

    sqlite3 *aggregate = NULL;
    sqlite3 **intermediates = NULL;
    char aggregate_name[MAXSQL] = {};
    if (in.aggregate_or_print == AGGREGATE) {
        /* modify in.sqlent to insert the results into the aggregate table */
        char orig_sqlent[MAXSQL];
        SNFORMAT_S(orig_sqlent, MAXSQL, 1, in.sqlent, strlen(in.sqlent));
        SNFORMAT_S(in.sqlent, MAXSQL, 4, "INSERT INTO ", 12, AGGREGATE_ATTACH_NAME, strlen(AGGREGATE_ATTACH_NAME), ".entries ", (size_t) 9, orig_sqlent, strlen(orig_sqlent));

        /* create the aggregate database */
        SNPRINTF(aggregate_name, MAXSQL, AGGREGATE_NAME, -1);
        if (!(aggregate = open_aggregate(aggregate_name, AGGREGATE_ATTACH_NAME, orig_sqlent))) {
            return -1;
        }

        intermediates = (sqlite3 **) malloc(sizeof(sqlite3 *) * in.maxthreads);
        for(int i = 0; i < in.maxthreads; i++) {
            char intermediate_name[MAXSQL];
            SNPRINTF(intermediate_name, MAXSQL, AGGREGATE_NAME, (int) i);
            if (!(intermediates[i] = open_aggregate(intermediate_name, AGGREGATE_ATTACH_NAME, orig_sqlent))) {
                fprintf(stderr, "Could not open %s\n", intermediate_name);
                cleanup_intermediates(intermediates, i);
                closedb(aggregate);
                return -1;
            }
        }
    }

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    struct timespec setup_aggregate_end;
    clock_gettime(CLOCK_MONOTONIC, &setup_aggregate_end);
    const long double setup_aggregate_time = elapsed(&setup_aggregate_start, &setup_aggregate_end);
    #endif

    #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
    long double total_time = 0;

    struct timespec work_start;
    clock_gettime(CLOCK_MONOTONIC, &work_start);
    #endif

    /* provide a function to print if PRINT is set */
    args.print_callback_func = ((in.aggregate_or_print == PRINT)?print_callback:NULL);
    struct QPTPool * pool = QPTPool_init(in.maxthreads);
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        return -1;
    }

    if (QPTPool_start(pool, &args) != (size_t) in.maxthreads) {
        fprintf(stderr, "Failed to start threads\n");
        return -1;
    }

    /* enqueue all input paths */
    for(int i = idx; i < argc; i++) {
        struct work * mywork = calloc(1, sizeof(struct work));

        /* copy argv[i] into the work item */
        SNFORMAT_S(mywork->name, MAXPATH, 1, argv[i], strlen(argv[i]));

        lstat(mywork->name,&mywork->statuso);
        if (!S_ISDIR(mywork->statuso.st_mode) ) {
            fprintf(stderr,"input-dir '%s' is not a directory\n", mywork->name);
            free(mywork);
            continue;
        }

        /* push the path onto the queue */
        QPTPool_enqueue(pool, i % in.maxthreads, processdir, mywork);
    }

    QPTPool_wait(pool);

    #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
    const size_t thread_count = QPTPool_threads_completed(pool);
    #endif

    QPTPool_destroy(pool);

    #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
    struct timespec work_end;
    clock_gettime(CLOCK_MONOTONIC, &work_end);
    const long double work_time = elapsed(&work_start, &work_end);
    total_time += work_time;
    #endif

    #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
    long double aggregate_time = 0;
    long double cleanup_time = 0;
    long double output_time = 0;
    #endif

    if (in.aggregate_or_print == AGGREGATE) {
        /* prepend the intermediate database query with "INSERT INTO" to move */
        /* the data from the databases into the final aggregation database */
        char intermediate[MAXSQL];
        SNFORMAT_S(intermediate, MAXSQL, 4, "INSERT INTO ", 12, AGGREGATE_ATTACH_NAME, strlen(AGGREGATE_ATTACH_NAME), ".entries ", (size_t) 9, in.intermediate, strlen(in.intermediate));

        #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
        struct timespec aggregate_start;
        clock_gettime(CLOCK_MONOTONIC, &aggregate_start);
        #endif

        /* aggregate the intermediate results */
        for(int i = 0; i < in.maxthreads; i++) {
            if (!attachdb(aggregate_name, intermediates[i], AGGREGATE_ATTACH_NAME)            ||
                (sqlite3_exec(intermediates[i], intermediate, NULL, NULL, NULL) != SQLITE_OK)) {
                printf("Final aggregation error: %s\n", sqlite3_errmsg(intermediates[i]));
            }
        }

        #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
        struct timespec aggregate_end;
        clock_gettime(CLOCK_MONOTONIC, &aggregate_end);
        aggregate_time = elapsed(&aggregate_start, &aggregate_end);
        total_time += aggregate_time;
        #endif

        #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
        struct timespec cleanup_start;
        clock_gettime(CLOCK_MONOTONIC, &cleanup_start);
        #endif

        /* cleanup the intermediate databases (no need to detach) */
        cleanup_intermediates(intermediates, in.maxthreads);

        #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
        struct timespec cleanup_end;
        clock_gettime(CLOCK_MONOTONIC, &cleanup_end);
        cleanup_time = elapsed(&cleanup_start, &cleanup_end);
        total_time += cleanup_time;
        #endif

        #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
        struct timespec output_start;
        clock_gettime(CLOCK_MONOTONIC, &output_start);
        #endif

        struct CallbackArgs ca;
        ca.output_buffers = &args.output_buffers;
        ca.id = in.maxthreads;

        char * err;
        if (sqlite3_exec(aggregate, in.aggregate, print_callback, &ca, &err) != SQLITE_OK) {
            fprintf(stderr, "Cannot print from aggregate database: %s\n", err);
            sqlite3_free(err);
        }

        #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
        struct timespec output_end;
        clock_gettime(CLOCK_MONOTONIC, &output_end);
        output_time = elapsed(&output_start, &output_end);
        total_time += output_time;
        #endif

        closedb(aggregate);
    }

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    struct timespec cleanup_globals_start;
    clock_gettime(CLOCK_MONOTONIC, &cleanup_globals_start);
    #endif

    /* clear out buffered data */
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES) || BENCHMARK
    const size_t rows =
    #endif
    OutputBuffers_flush_multiple(&args.output_buffers, in.maxthreads + 1, gts.outfd);

    /* clean up globals */
    OutputBuffers_destroy(&args.output_buffers, in.maxthreads + 1);
    outdbs_fin  (gts.outdbd, in.maxthreads, in.sqlfin);
    outfiles_fin(gts.outfd,  in.maxthreads);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    struct timespec cleanup_globals_end;
    clock_gettime(CLOCK_MONOTONIC, &cleanup_globals_end);
    const long double cleanup_globals_time = elapsed(&cleanup_globals_start, &cleanup_globals_end);
    #endif

    #ifdef DEBUG
    free(global_timers);
    #ifdef PER_THREAD_STATS
    OutputBuffers_flush_single(&debug_output_buffers, in.maxthreads, stderr);
    OutputBuffers_destroy(&debug_output_buffers, in.maxthreads);
    #endif
    #endif

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    fprintf(stderr, "set up globals:                              %.2Lfs\n", setup_globals_time);
    fprintf(stderr, "set up intermediate databases:               %.2Lfs\n", setup_aggregate_time);
    fprintf(stderr, "thread pool:                                 %.2Lfs\n", work_time);
    fprintf(stderr, "     open directories:                       %.2Lfs\n", total_opendir_time);
    fprintf(stderr, "     open databases:                         %.2Lfs\n", total_open_time);
    fprintf(stderr, "         sqlite3_open_v2:                    %.2Lfs\n", total_sqlite3_open_time);
    fprintf(stderr, "         create tables:                      %.2Lfs\n", total_create_tables_time);
    fprintf(stderr, "         set pragmas:                        %.2Lfs\n", total_set_pragmas_time);
    fprintf(stderr, "         load extensions:                    %.2Lfs\n", total_load_extension_time);
    fprintf(stderr, "     addqueryfuncs:                          %.2Lfs\n", total_addqueryfuncs_time);
    fprintf(stderr, "     descend:                                %.2Lfs\n", total_descend_time);
    fprintf(stderr, "         check args:                         %.2Lfs\n", total_check_args_time);
    fprintf(stderr, "         check level:                        %.2Lfs\n", total_level_time);
    fprintf(stderr, "         check level <= max_level branch:    %.2Lfs\n", total_level_branch_time);
    fprintf(stderr, "         while true:                         %.2Lfs\n", total_while_branch_time);
    fprintf(stderr, "             readdir:                        %.2Lfs\n", total_readdir_time);
    fprintf(stderr, "             readdir != null branch:         %.2Lfs\n", total_readdir_branch_time);
    fprintf(stderr, "             strncmp:                        %.2Lfs\n", total_strncmp_time);
    fprintf(stderr, "             strncmp != . or ..:             %.2Lfs\n", total_strncmp_branch_time);
    fprintf(stderr, "             snprintf:                       %.2Lfs\n", total_snprintf_time);
    fprintf(stderr, "             lstat:                          %.2Lfs\n", total_lstat_time);
    fprintf(stderr, "             isdir:                          %.2Lfs\n", total_isdir_time);
    fprintf(stderr, "             isdir branch:                   %.2Lfs\n", total_isdir_branch_time);
    fprintf(stderr, "             access:                         %.2Lfs\n", total_access_time);
    fprintf(stderr, "             set:                            %.2Lfs\n", total_set_time);
    fprintf(stderr, "             clone:                          %.2Lfs\n", total_clone_time);
    fprintf(stderr, "             pushdir:                        %.2Lfs\n", total_pushdir_time);
    fprintf(stderr, "     attach intermediate databases:          %.2Lfs\n", total_attach_time);
    fprintf(stderr, "     sqlite3_exec                            %.2Lfs\n", total_exec_time);
    fprintf(stderr, "     detach intermediate databases:          %.2Lfs\n", total_detach_time);
    fprintf(stderr, "     close databases:                        %.2Lfs\n", total_close_time);
    fprintf(stderr, "     close directories:                      %.2Lfs\n", total_closedir_time);
    fprintf(stderr, "     restore timestamps:                     %.2Lfs\n", total_utime_time);
    fprintf(stderr, "     free work:                              %.2Lfs\n", total_free_work_time);
    fprintf(stderr, "output timestamps:                           %.2Lfs\n", total_output_timestamps_time);
    fprintf(stderr, "aggregate into final databases:              %.2Lfs\n", aggregate_time);
    fprintf(stderr, "clean up intermediate databases:             %.2Lfs\n", cleanup_time);
    fprintf(stderr, "print aggregated results:                    %.2Lfs\n", output_time);
    fprintf(stderr, "clean up globals:                            %.2Lfs\n", cleanup_globals_time);
    fprintf(stderr, "\n");
    fprintf(stderr, "Rows returned:                               %zu\n",    rows);
    fprintf(stderr, "Queries performed:                           %zu\n",    thread_count);
    fprintf(stderr, "Real time:                                   %.2Lfs\n", total_time);
    #endif

    #if BENCHMARK
    fprintf(stderr, "Total Dirs:            %zu\n",    thread_count);
    fprintf(stderr, "Total Files:           %zu\n",    rows);
    fprintf(stderr, "Time Spent Querying:   %.2Lfs\n", total_time);
    fprintf(stderr, "Dirs/Sec:              %.2Lf\n",  thread_count / total_time);
    fprintf(stderr, "Files/Sec:             %.2Lf\n",  rows / total_time);
    #endif

    return 0;
}
