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

#include "bf.h"
#include "debug.h"
#include "dbutils.h"
#include "outdbs.h"
#include "outfiles.h"
#include "OutputBuffers.h"
#include "pcre.h"
#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "utils.h"

extern int errno;
#define AGGREGATE_NAME         "file:aggregate%d?mode=memory&cache=shared"
#define AGGREGATE_ATTACH_NAME  "aggregate"

#ifdef DEBUG
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
    struct buffer level_cmp;
    struct buffer level_branch;
    struct buffer while_branch;
    struct buffer readdir_call;
    struct buffer readdir_branch;
    struct buffer strncmp_call;
    struct buffer strncmp_branch;
    struct buffer snprintf_call;
    struct buffer lstat_call;
    struct buffer isdir_cmp;
    struct buffer isdir_branch;
    struct buffer access_call;
    struct buffer set;
    struct buffer make_clone;
    struct buffer pushdir;
};

#define debug_start(name) timestamp_start(name)
#define debug_end(name) timestamp_end(name)
#define debug_define_start(name) define_start(name)

#define buffered_start(name)                                \
    struct start_end * name = buffer_get(&timers->name);    \
    timestamp_start((*(name)));

#define buffered_end(name)                                  \
    timestamp_end((*(name)));

struct descend_timers * global_timers = NULL;

#ifdef PER_THREAD_STATS
extern struct OutputBuffers debug_output_buffers;

void print_timers(struct OutputBuffers * obufs, const size_t id, char * buf, const size_t size, const char * name, struct buffer * timers) {
    for(size_t i = 0; i < timers->i; i++) {
        print_debug(obufs, id, buf, size, name, &timers->data[i]);
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
long double total_sqlsum_time = 0;
long double total_sqlent_time = 0;
long double total_detach_time = 0;
long double total_close_time = 0;
long double total_closedir_time = 0;
long double total_utime_time = 0;
long double total_free_work_time = 0;
long double total_output_timestamps_time = 0;

long double buffer_sum(struct buffer * timer) {
    long double sum = 0;
    for(size_t i = 0; i < timer->i; i++) {
        sum += elapsed(&timer->data[i]);
    }

    return sum;
}
#endif
#else
#define debug_start(name)
#define debug_end(name)
#define debug_define_start(name)
#define buffered_start(name)
#define buffered_end(name)
#endif

int processdir(struct QPTPool * ctx, const size_t id, void * data, void * args);

/* Push the subdirectories in the current directory onto the queue */
static size_t descend2(struct QPTPool *ctx,
                       const size_t id,
                       struct work *passmywork,
                       DIR *dir,
                       QPTPoolFunc_t func,
                       const size_t max_level
                       #ifdef DEBUG
                       , struct descend_timers * timers
                       #endif
    ) {
    buffered_start(within_descend);

    /* buffered_start(check_args); */
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
    /* buffered_end(check_args); */

    buffered_start(level_cmp);
    size_t pushed = 0;
    const size_t next_level = passmywork->level + 1;
    const int level_check = (next_level <= max_level);
    buffered_end(level_cmp);

    buffered_start(level_branch);
    if (level_check) {
        buffered_end(level_branch);

        /* go ahead and send the subdirs to the queue since we need to look */
        /* further down the tree.  loop over dirents, if link push it on the */
        /* queue, if file or link print it, fill up qwork structure for */
        /* each */
        buffered_start(while_branch);
        while (1) {
            buffered_start(readdir_call);
            struct dirent * entry = readdir(dir);
            buffered_end(readdir_call);

            buffered_start(readdir_branch);
            if (!entry) {
                buffered_end(readdir_branch);
                break;
            }
            else {
                buffered_end(readdir_branch);
            }

            buffered_start(strncmp_call);
            const size_t len = strlen(entry->d_name);
            const int skip = (((len == 1) && (strncmp(entry->d_name, ".",  1) == 0)) ||
                              ((len == 2) && (strncmp(entry->d_name, "..", 2) == 0)));
            buffered_end(strncmp_call);

            buffered_start(strncmp_branch);
            if (skip) {
                buffered_end(strncmp_branch);
                continue;
            }
            else {
                buffered_end(strncmp_branch);
            }

            buffered_start(snprintf_call);
            struct work qwork;
            SNFORMAT_S(qwork.name, MAXPATH, 3, passmywork->name, strlen(passmywork->name), "/", (size_t) 1, entry->d_name, len);
            buffered_end(snprintf_call);

            /* buffered_end(lstat_call); */
            /* lstat(qwork.name, &qwork.statuso); */
            /* buffered_end(lstat_call); */

            buffered_start(isdir_cmp);
            const int isdir = (entry->d_type == DT_DIR);
            /* const int isdir = S_ISDIR(qwork.statuso.st_mode); */
            buffered_end(isdir_cmp);

            buffered_start(isdir_branch);
            if (isdir) {
                buffered_end(isdir_branch);

                /* const int accessible = !access(qwork.name, R_OK | X_OK); */

                /* if (accessible) { */
                    buffered_start(set);
                    qwork.level = next_level;
                    /* qwork.type[0] = 'd'; */

                    /* this is how the parent gets passed on */
                    /* qwork.pinode = passmywork->statuso.st_ino; */
                    buffered_end(set);

                    /* make a clone here so that the data can be pushed into the queue */
                    /* this is more efficient than malloc+free for every single entry */
                    buffered_start(make_clone);
                    struct work * clone = (struct work *) malloc(sizeof(struct work));
                    memcpy(clone, &qwork, sizeof(struct work));
                    buffered_end(make_clone);

                    /* push the subdirectory into the queue for processing */
                    buffered_start(pushdir);
                    QPTPool_enqueue(ctx, id, processdir, clone);
                    buffered_end(pushdir);

                    pushed++;
                /* } */
                /* else { */
                /*     fprintf(stderr, "couldn't access dir '%s': %s\n", */
                /*             qwork->name, strerror(errno)); */
                /* } */
            }
            else {
                buffered_end(isdir_branch);
            }
        }
        buffered_end(while_branch);
    }
    else {
        buffered_end(level_branch);
    }
    buffered_end(within_descend);

    return pushed;
}

/* sqlite3_exec callback argument data */
struct CallbackArgs {
    struct OutputBuffers * output_buffers;
    int id;
    size_t count;
};

static int print_callback(void *args, int count, char **data, char **columns) {
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

        ca->count++;
    /* } */
    return 0;
}

struct ThreadArgs {
    struct OutputBuffers output_buffers;
    int (*print_callback_func)(void*,int,char**,char**);
    #ifdef DEBUG
    struct timespec *start_time;
    #endif
};

#ifdef DEBUG
#define init_start_end(name, zero)              \
    struct start_end name;                      \
    memcpy(&name.start, zero, sizeof(*zero));   \
    memcpy(&name.end, zero, sizeof(*zero));
#endif

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

    /* /\* print directory *\/ */
    /* if (in.printdir) { */
    /*     struct ThreadArgs * ta = (struct ThreadArgs *) args; */
    /*     struct CallbackArgs ca; */
    /*     ca.output_buffers = &ta->output_buffers; */
    /*     ca.id = id; */
    /*     char * ptr = &(work->name[0]); */
    /*     ta->print_callback_func(&ca, 1, &ptr, NULL); */
    /* } */

    char dbname[MAXSQL];
    SNFORMAT_S(dbname, MAXSQL, 2, work->name, work_name_len, "/" DBNAME, DBNAME_LEN + 1);

    struct ThreadArgs * ta = (struct ThreadArgs *) args;

    #ifdef DEBUG
    init_start_end(opendir_call, ta->start_time);
    init_start_end(open_call, ta->start_time);
    init_start_end(sqlite3_open_call, ta->start_time);
    init_start_end(create_tables_call, ta->start_time);
    init_start_end(set_pragmas, ta->start_time);
    init_start_end(load_extension, ta->start_time);
    init_start_end(addqueryfuncs_call, ta->start_time);
    init_start_end(descend_call, ta->start_time);
    struct descend_timers * descend_timers = &global_timers[id];
    init_start_end(attach_call, ta->start_time);
    init_start_end(sqlsum, ta->start_time);
    init_start_end(sqlent, ta->start_time);
    init_start_end(detach_call, ta->start_time);
    init_start_end(close_call, ta->start_time);
    init_start_end(closedir_call, ta->start_time);
    init_start_end(utime_call, ta->start_time);
    init_start_end(free_work, ta->start_time);

    descend_timers->within_descend.i = 0;
    descend_timers->check_args.i     = 0;
    descend_timers->level_cmp.i      = 0;
    descend_timers->level_branch.i   = 0;
    descend_timers->while_branch.i   = 0;
    descend_timers->readdir_call.i   = 0;
    descend_timers->readdir_branch.i = 0;
    descend_timers->strncmp_call.i   = 0;
    descend_timers->strncmp_branch.i = 0;
    descend_timers->snprintf_call.i  = 0;
    descend_timers->lstat_call.i     = 0;
    descend_timers->isdir_cmp.i      = 0;
    descend_timers->isdir_branch.i   = 0;
    descend_timers->access_call.i    = 0;
    descend_timers->set.i            = 0;
    descend_timers->make_clone.i     = 0;
    descend_timers->pushdir.i        = 0;
    #endif

    /* keep opendir near opendb to help speed up sqlite3_open_v2 */
    debug_start(opendir_call);
    dir = opendir(work->name);
    debug_end(opendir_call);

    /* if the directory can't be opened, don't bother with anything else */
    if (!dir) {
        /* fprintf(stderr, "Could not open directory %s: %d %s\n", work->name, errno, strerror(errno)); */
        goto out_free;
    }

    #ifndef NO_OPENDB
    debug_start(open_call);
    if (gts.outdbd[id]) {
      /* if we have an out db then only have to attach the gufi db */
      db = gts.outdbd[id];
      if (!attachdb(dbname, db, "tree", in.open_mode)) {
          goto close_dir;
      }
    }
    else {
      /* otherwise, open a standalone database to query from */
      db = opendb(dbname, in.open_mode, 1, 1,
                  NULL, NULL
                  #ifdef DEBUG
                  , &sqlite3_open_call.start,  &sqlite3_open_call.end
                  , &create_tables_call.start, &create_tables_call.end
                  , &set_pragmas.start,        &set_pragmas.end
                  , &load_extension.start,     &load_extension.end
                  #endif
                  );
    }
    debug_end(open_call);
    #endif

    #ifndef NO_ADDQUERYFUNCS
    debug_start(addqueryfuncs_call);
    /* this is needed to add some query functions like path() uidtouser() gidtogroup() */
    if (db) {
        addqueryfuncs(db, id);
    }
    debug_end(addqueryfuncs_call);
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
        debug_start(descend_call);
        #ifdef DEBUG
        #ifdef SUBDIRECTORY_COUNTS
        const size_t pushed =
        #endif
        #endif
        /* push subdirectories into the queue */
        descend2(ctx, id, work, dir, processdir, in.max_level
                 #ifdef DEBUG
                 , descend_timers
                 #endif
            );
        debug_end(descend_call);

        #ifdef DEBUG
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

                    #ifndef NO_SQL_EXEC
                    struct CallbackArgs ca;
                    ca.output_buffers = &ta->output_buffers;
                    ca.id = id;
                    ca.count = 0;

                    /* this probably needs a timer */
                    #ifdef DEBUG
                    debug_start(sqlsum);
                    char *err = NULL;
                    if (
                    #endif
                        sqlite3_exec(db, in.sqlsum, ta->print_callback_func, &ca,
                    #ifdef DEBUG
                        &err) != SQLITE_OK) {
                        fprintf(stderr, "Error: %s: %s: \"%s\"\n", err, dbname, in.sqlent);
                        sqlite3_free(err);
                    }
                    debug_end(sqlsum);
                    #else
                    NULL);
                    #endif
                    #endif

                    recs = ca.count;
                } else {
                    recs = 1;
                }
                if (in.andor > 0) {
                    recs = 1;
                }

                /* if we have recs (or are running an OR) query the entries table */
                if (recs > 0) {
                    if (in.sqlent_len > 1) {
                        /* set the path so users can put path() in their queries */
                        /* printf("****entries len of in.sqlent %lu\n",strlen(in.sqlent)); */
                        SNFORMAT_S(gps[id].gpath, MAXPATH, 1, work->name, work_name_len);
                        realpath(work->name,gps[id].gfpath);

                        #ifndef NO_SQL_EXEC
                        struct CallbackArgs ca;
                        ca.output_buffers = &ta->output_buffers;
                        ca.id = id;

                        #ifdef DEBUG
                        debug_start(sqlent);
                        char *err = NULL;
                        if (
                        #endif
                        sqlite3_exec(db, in.sqlent, ta->print_callback_func, &ca,
                        #ifdef DEBUG
                        &err) != SQLITE_OK) {
                            fprintf(stderr, "Error: %s: %s: \"%s\"\n", err, dbname, in.sqlent);
                            sqlite3_free(err);
                        }
                        debug_end(sqlent);
                        #else
                        NULL);
                        #endif
                        #endif
                    }
                }
            }
        }
    }

    #ifndef NO_OPENDB
    debug_start(close_call);
    /* if we have an out db we just detach gufi db */
    if (gts.outdbd[id]) {
      detachdb(dbname, db, "tree");
    } else {
      closedb(db);
    }
    debug_end(close_call);
    #endif

  close_dir:
    debug_start(closedir_call);
    closedir(dir);
    debug_end(closedir_call);

    debug_start(utime_call);
    /* restore mtime and atime */
    if (in.keep_matime) {
        struct utimbuf dbtime = {};
        dbtime.actime  = work->statuso.st_atime;
        dbtime.modtime = work->statuso.st_mtime;
        utime(dbname, &dbtime);
    }
    debug_end(utime_call);

  out_free:
    ;

    debug_start(free_work);
    free(work);
    debug_end(free_work);

    #ifdef DEBUG
    struct start_end output_timestamps;
    debug_start(output_timestamps);

    #ifdef PER_THREAD_STATS
    char buf[4096];
    const size_t size = sizeof(buf);
    print_debug(         &debug_output_buffers, id, buf, size, "opendir",         &opendir_call);
    if (dir) {
        print_debug(     &debug_output_buffers, id, buf, size, "opendb",          &open_call);
        if (db) {
            print_debug (&debug_output_buffers, id, buf, size, "sqlite3_open_v2", &sqlite3_open_call);
            print_debug (&debug_output_buffers, id, buf, size, "create_tables",   &create_tables_call);
            print_debug (&debug_output_buffers, id, buf, size, "set_pragmas",     &set_pragmas);
            print_debug (&debug_output_buffers, id, buf, size, "load_extensions", &load_extension);
            print_debug (&debug_output_buffers, id, buf, size, "addqueryfuncs",   &addqueryfuncs_call);
            print_debug (&debug_output_buffers, id, buf, size, "descend",         &descend_call);
            print_timers(&debug_output_buffers, id, buf, size, "within_descend",  &descend_timers->within_descend);
            print_timers(&debug_output_buffers, id, buf, size, "check_args",      &descend_timers->check_args);
            print_timers(&debug_output_buffers, id, buf, size, "level",           &descend_timers->level_cmp);
            print_timers(&debug_output_buffers, id, buf, size, "level_branch",    &descend_timers->level_branch);
            print_timers(&debug_output_buffers, id, buf, size, "while_branch",    &descend_timers->while_branch);
            print_timers(&debug_output_buffers, id, buf, size, "readdir",         &descend_timers->readdir_call);
            print_timers(&debug_output_buffers, id, buf, size, "readdir_branch",  &descend_timers->readdir_branch);
            print_timers(&debug_output_buffers, id, buf, size, "strncmp",         &descend_timers->strncmp_call);
            print_timers(&debug_output_buffers, id, buf, size, "strncmp_branch",  &descend_timers->strncmp_branch);
            print_timers(&debug_output_buffers, id, buf, size, "snprintf",        &descend_timers->snprintf_call);
            print_timers(&debug_output_buffers, id, buf, size, "lstat",           &descend_timers->lstat_call);
            print_timers(&debug_output_buffers, id, buf, size, "isdir",           &descend_timers->isdir_cmp);
            print_timers(&debug_output_buffers, id, buf, size, "isdir_branch",    &descend_timers->isdir_branch);
            print_timers(&debug_output_buffers, id, buf, size, "access",          &descend_timers->access_call);
            print_timers(&debug_output_buffers, id, buf, size, "set",             &descend_timers->set);
            print_timers(&debug_output_buffers, id, buf, size, "clone",           &descend_timers->make_clone);
            print_timers(&debug_output_buffers, id, buf, size, "pushdir",         &descend_timers->pushdir);
            print_debug (&debug_output_buffers, id, buf, size, "attach",          &attach_call);
            print_debug (&debug_output_buffers, id, buf, size, "sqlsum",          &sqlsum);
            print_debug (&debug_output_buffers, id, buf, size, "sqlent",          &sqlent);
            print_debug (&debug_output_buffers, id, buf, size, "detach",          &detach_call);
            print_debug (&debug_output_buffers, id, buf, size, "closedb",         &close_call);
        }
        print_debug(     &debug_output_buffers, id, buf, size, "closedir",        &closedir_call);
        print_debug(     &debug_output_buffers, id, buf, size, "utime",           &utime_call);
        print_debug(     &debug_output_buffers, id, buf, size, "free_work",       &free_work);
    }
    #endif

    debug_end(output_timestamps);

    #ifdef CUMULATIVE_TIMES
    pthread_mutex_lock(&print_mutex);
    total_opendir_time           += elapsed(&opendir_call);
    total_open_time              += elapsed(&open_call);
    total_sqlite3_open_time      += elapsed(&sqlite3_open_call);
    total_create_tables_time     += elapsed(&create_tables_call);
    total_set_pragmas_time       += elapsed(&set_pragmas);
    total_load_extension_time    += elapsed(&load_extension);
    total_addqueryfuncs_time     += elapsed(&addqueryfuncs_call);
    total_descend_time           += elapsed(&descend_call);
    total_check_args_time        += buffer_sum(&descend_timers->check_args);
    total_level_time             += buffer_sum(&descend_timers->level_cmp);
    total_level_branch_time      += buffer_sum(&descend_timers->level_branch);
    total_while_branch_time      += buffer_sum(&descend_timers->while_branch);
    total_readdir_time           += buffer_sum(&descend_timers->readdir_call);
    total_readdir_branch_time    += buffer_sum(&descend_timers->readdir_branch);
    total_strncmp_time           += buffer_sum(&descend_timers->strncmp_call);
    total_strncmp_branch_time    += buffer_sum(&descend_timers->strncmp_branch);
    total_snprintf_time          += buffer_sum(&descend_timers->snprintf_call);
    total_lstat_time             += buffer_sum(&descend_timers->lstat_call);
    total_isdir_time             += buffer_sum(&descend_timers->isdir_cmp);
    total_isdir_branch_time      += buffer_sum(&descend_timers->isdir_branch);
    total_access_time            += buffer_sum(&descend_timers->access_call);
    total_set_time               += buffer_sum(&descend_timers->set);
    total_clone_time             += buffer_sum(&descend_timers->make_clone);
    total_pushdir_time           += buffer_sum(&descend_timers->pushdir);
    total_closedir_time          += elapsed(&closedir_call);
    total_attach_time            += elapsed(&attach_call);
    total_sqlsum_time            += elapsed(&sqlsum);
    total_sqlent_time            += elapsed(&sqlent);
    total_detach_time            += elapsed(&detach_call);
    total_close_time             += elapsed(&close_call);
    total_utime_time             += elapsed(&utime_call);
    total_free_work_time         += elapsed(&free_work);
    total_output_timestamps_time += elapsed(&output_timestamps);
    pthread_mutex_unlock(&print_mutex);
    #endif

    #ifdef PER_THREAD_STATS
    print_debug(&debug_output_buffers, id, buf, size, "output_timestamps", &output_timestamps);
    #endif

    #endif

    return 0;
}

/* create aggregate database and intermediate per-thread databases
 * the user must create the intermediate table with -I and insert into it
 * the per-thread databases reuse outdb array
 */
static sqlite3 *aggregate_init(const char *AGGREGATE_NAME_TEMPLATE,
                               char *aggregate_name, size_t count) {
    for(int i = 0; i < in.maxthreads; i++) {
        char intermediate_name[MAXSQL];
        SNPRINTF(intermediate_name, MAXSQL, AGGREGATE_NAME, (int) i);
        if (!(gts.outdbd[i] = opendb(intermediate_name, RDWR, 1, 1, NULL, NULL
                                     #ifdef DEBUG
                                     , NULL, NULL
                                     , NULL, NULL
                                     , NULL, NULL
                                     , NULL, NULL
                                     #endif
                  ))) {
            fprintf(stderr, "Could not open %s\n", intermediate_name);
            outdbs_fin(gts.outdbd, i, NULL, 0);
            return NULL;
        }

        addqueryfuncs(gts.outdbd[i], i);

        // create table
        if (sqlite3_exec(gts.outdbd[i], in.sqlinit, NULL, NULL, NULL) != SQLITE_OK) {
            fprintf(stderr, "Could not run SQL Init \"%s\" on %s\n", in.sqlinit, intermediate_name);
            outdbs_fin(gts.outdbd, i, NULL, 0);
            return NULL;
        }
    }

    SNPRINTF(aggregate_name, MAXSQL, AGGREGATE_NAME, (int) -1);
    sqlite3 *aggregate = NULL;
    if (!(aggregate = opendb(aggregate_name, RDWR, 1, 1, NULL, NULL
                             #ifdef DEBUG
                             , NULL, NULL
                             , NULL, NULL
                             , NULL, NULL
                             , NULL, NULL
                             #endif
              ))) {
        fprintf(stderr, "Could not open %s\n", aggregate_name);
        outdbs_fin(gts.outdbd, in.maxthreads, NULL, 0);
        closedb(aggregate);
        return NULL;
    }

    addqueryfuncs(aggregate, in.maxthreads);

    // create table
    if (sqlite3_exec(aggregate, in.sqlinit, NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "Could not run SQL Init \"%s\" on %s\n", in.sqlinit, aggregate_name);
        outdbs_fin(gts.outdbd, in.maxthreads, NULL, 0);
        closedb(aggregate);
        return NULL;
    }

    return aggregate;
}

static void aggregate_fin(sqlite3 *aggregate) {
    closedb(aggregate);
}

void sub_help() {
   printf("GUFI_index        find GUFI index here\n");
   printf("\n");
}

int main(int argc, char *argv[])
{
    #ifdef DEBUG
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    epoch = since_epoch(&now);
    #endif

    /* process input args - all programs share the common 'struct input', */
    /* but allow different fields to be filled at the command-line. */
    /* Callers provide the options-string for get_opt(), which will */
    /* control which options are parsed for each program. */
    int idx = parse_cmd_line(argc, argv, "hHT:S:E:an:o:d:O:I:F:y:z:G:J:e:m:B:w", 1, "GUFI_index ...", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    debug_define_start(setup_globals)
    #endif

    struct ThreadArgs args;
    #ifdef DEBUG
    args.start_time = &now;
    #endif

    /* initialize globals */
    const size_t output_count = in.maxthreads + !!(in.show_results == AGGREGATE);
    if (!outfiles_init(gts.outfd,  in.outfile, in.outfilen, output_count)                              ||
        !outdbs_init  (gts.outdbd, in.outdb,   in.outdbn,   in.maxthreads, in.sqlinit, in.sqlinit_len) ||
        !OutputBuffers_init(&args.output_buffers, output_count, in.output_buffer_size))                 {
        OutputBuffers_destroy(&args.output_buffers, output_count);
        outdbs_fin  (gts.outdbd, in.maxthreads, in.sqlfin, in.sqlfin_len);
        outfiles_fin(gts.outfd, output_count);
        return -1;
    }

    #ifdef DEBUG
    #ifdef PER_THREAD_STATS
    OutputBuffers_init(&debug_output_buffers, in.maxthreads, 1073741824ULL);
    #endif

    global_timers = malloc(in.maxthreads * sizeof(struct descend_timers));

    #ifdef CUMULATIVE_TIMES
    debug_end(setup_globals);
    const long double setup_globals_time = elapsed(&setup_globals);
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
    debug_define_start(setup_aggregate);
    #endif

    char aggregate_name[MAXSQL];
    sqlite3 * aggregate = NULL;
    if (in.show_results == AGGREGATE) {
        if (!(aggregate = aggregate_init(AGGREGATE_NAME, aggregate_name, in.maxthreads))) {
            OutputBuffers_destroy(&args.output_buffers, output_count);
            outdbs_fin  (gts.outdbd, in.maxthreads, in.sqlfin, in.sqlfin_len);
            outfiles_fin(gts.outfd, output_count);
            return -1;
        }
    }

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    debug_end(setup_aggregate);
    const long double setup_aggregate_time = elapsed(&setup_aggregate);
    #endif

    #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
    long double total_time = 0;

    debug_define_start(work);
    #endif

    /* provide a function to print if PRINT is set */
    args.print_callback_func = ((in.show_results == PRINT)?print_callback:NULL);
    struct QPTPool * pool = QPTPool_init(in.maxthreads);
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        OutputBuffers_destroy(&args.output_buffers, output_count);
        outdbs_fin  (gts.outdbd, in.maxthreads, in.sqlfin, in.sqlfin_len);
        outfiles_fin(gts.outfd, output_count);
        return -1;
    }

    if (QPTPool_start(pool, &args) != (size_t) in.maxthreads) {
        fprintf(stderr, "Failed to start threads\n");
        OutputBuffers_destroy(&args.output_buffers, output_count);
        outdbs_fin  (gts.outdbd, in.maxthreads, in.sqlfin, in.sqlfin_len);
        outfiles_fin(gts.outfd, output_count);
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
    debug_end(work);

    const long double work_time = elapsed(&work);
    total_time += work_time;
    #endif

    #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
    long double aggregate_time = 0;
    long double output_time = 0;
    #endif

    int rc = 0;
    if (in.show_results == AGGREGATE) {
        #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
        debug_define_start(aggregation);
        #endif

        /* aggregate the intermediate results */
        for(int i = 0; i < in.maxthreads; i++) {
            if (!attachdb(aggregate_name, gts.outdbd[i], AGGREGATE_ATTACH_NAME, RDONLY)       ||
                (sqlite3_exec(gts.outdbd[i], in.intermediate, NULL, NULL, NULL) != SQLITE_OK)) {
                fprintf(stderr, "Aggregation of intermediate databases error: %s\n", sqlite3_errmsg(gts.outdbd[i]));
            }
        }

        #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
        debug_end(aggregation);
        aggregate_time = elapsed(&aggregation);
        total_time += aggregate_time;
        #endif

        /* final query on aggregate results */
        #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
        debug_define_start(output);
        #endif

        struct CallbackArgs ca;
        ca.output_buffers = &args.output_buffers;
        ca.id = in.maxthreads;

        char * err;
        if (sqlite3_exec(aggregate, in.aggregate, print_callback, &ca, &err) != SQLITE_OK) {
            fprintf(stderr, "Final aggregation error: %s\n", err);
            sqlite3_free(err);
            rc = -1;
        }

        #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
        debug_end(output);
        output_time = elapsed(&output);
        total_time += output_time;
        #endif

        aggregate_fin(aggregate);
    }

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    debug_define_start(cleanup_globals);
    #endif

    /* clear out buffered data */
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES) || BENCHMARK
    const size_t rows =
    #endif
    OutputBuffers_flush_multiple(&args.output_buffers, output_count, gts.outfd);

    /* clean up globals */
    OutputBuffers_destroy(&args.output_buffers, output_count);
    outdbs_fin  (gts.outdbd, in.maxthreads, in.sqlfin, in.sqlfin_len);
    outfiles_fin(gts.outfd, output_count);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    debug_end(cleanup_globals);
    const long double cleanup_globals_time = elapsed(&cleanup_globals);
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
    fprintf(stderr, "     sqlsum                                  %.2Lfs\n", total_sqlsum_time);
    fprintf(stderr, "     sqlent                                  %.2Lfs\n", total_sqlent_time);
    fprintf(stderr, "     detach intermediate databases:          %.2Lfs\n", total_detach_time);
    fprintf(stderr, "     close databases:                        %.2Lfs\n", total_close_time);
    fprintf(stderr, "     close directories:                      %.2Lfs\n", total_closedir_time);
    fprintf(stderr, "     restore timestamps:                     %.2Lfs\n", total_utime_time);
    fprintf(stderr, "     free work:                              %.2Lfs\n", total_free_work_time);
    fprintf(stderr, "output timestamps:                           %.2Lfs\n", total_output_timestamps_time);
    fprintf(stderr, "aggregate into final databases:              %.2Lfs\n", aggregate_time);
    fprintf(stderr, "print aggregated results:                    %.2Lfs\n", output_time);
    fprintf(stderr, "clean up globals:                            %.2Lfs\n", cleanup_globals_time);
    fprintf(stderr, "\n");
    fprintf(stderr, "Rows returned:                               %zu\n",    rows);
    fprintf(stderr, "Queries performed:                           %zu\n",    thread_count * (!!in.sqltsum_len + !!in.sqlsum_len + !!in.sqlent_len));
    fprintf(stderr, "Real time:                                   %.2Lfs\n", total_time);
    #endif

    #if BENCHMARK
    fprintf(stderr, "Total Dirs:            %zu\n",    thread_count);
    fprintf(stderr, "Total Files:           %zu\n",    rows);
    fprintf(stderr, "Time Spent Querying:   %.2Lfs\n", total_time);
    fprintf(stderr, "Dirs/Sec:              %.2Lf\n",  thread_count / total_time);
    fprintf(stderr, "Files/Sec:             %.2Lf\n",  rows / total_time);
    #endif

    return rc;
}
