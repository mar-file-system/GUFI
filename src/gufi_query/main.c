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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <utime.h>

#include "OutputBuffers.h"
#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "bf.h"
#include "dbutils.h"
#include "debug.h"
#include "external.h"
#include "outfiles.h"
#include "pcre.h"
#include "trie.h"
#include "utils.h"

#include "gufi_query/aggregate.h"
#include "gufi_query/rollup.h"
#include "gufi_query/query.h"

extern int errno;

#ifdef DEBUG
struct start_end *buffer_create(sll_t *timers) {
    struct start_end *timer = malloc(sizeof(struct start_end));
    sll_push(timers, timer);
    return timer;
}

/* descend timer types */
enum {
    dt_within_descend = 0,
    dt_check_args,
    dt_level_cmp,
    dt_level_branch,
    dt_while_branch,
    dt_readdir_call,
    dt_readdir_branch,
    dt_strncmp_call,
    dt_strncmp_branch,
    dt_snprintf_call,
    dt_lstat_call,
    dt_isdir_cmp,
    dt_isdir_branch,
    dt_access_call,
    dt_set,
    dt_make_clone,
    dt_pushdir,

    dt_max
};

sll_t *descend_timers_init() {
    sll_t *dt = malloc(dt_max * sizeof(sll_t));
    for(int i = 0; i < dt_max; i++) {
        sll_init(&dt[i]);
    }

    return dt;
}

void descend_timers_destroy(sll_t *dt) {
    for(int i = 0; i < dt_max; i++) {
        sll_destroy(&dt[i], free);
    }
    free(dt);
}

#define buffered_name(name) (dt_##name)
#define buffered_get(timers, name)  timers[buffered_name(name)]

#define buffered_start(name)                                             \
    struct start_end *name = buffer_create(&buffered_get(timers, name)); \
    timestamp_set_start_raw((*(name)));

#define buffered_end(name) timestamp_set_end_raw((*(name)));

#ifdef PER_THREAD_STATS

#define print_descend_timers(obufs, id, name, timers, type)              \
    sll_loop(&buffered_get(timers, type), node) {                        \
        struct start_end *timestamp = sll_node_data(node);               \
        print_timer(obufs, id, ts_buf, sizeof(ts_buf), name, timestamp); \
    }
#else
#define print_descend_timers(obufs, id, name, timers, type)
#endif

#ifdef CUMULATIVE_TIMES
uint64_t total_opendir_time           = 0;
uint64_t total_attachdb_time          = 0;
uint64_t total_addqueryfuncs_time     = 0;
uint64_t total_xattrprep_time         = 0;
uint64_t total_get_rollupscore_time   = 0;
uint64_t total_descend_time           = 0;
uint64_t total_check_args_time        = 0;
uint64_t total_level_time             = 0;
uint64_t total_level_branch_time      = 0;
uint64_t total_while_branch_time      = 0;
uint64_t total_readdir_time           = 0;
uint64_t total_readdir_branch_time    = 0;
uint64_t total_strncmp_time           = 0;
uint64_t total_strncmp_branch_time    = 0;
uint64_t total_snprintf_time          = 0;
uint64_t total_lstat_time             = 0;
uint64_t total_isdir_time             = 0;
uint64_t total_isdir_branch_time      = 0;
uint64_t total_access_time            = 0;
uint64_t total_set_time               = 0;
uint64_t total_clone_time             = 0;
uint64_t total_pushdir_time           = 0;
uint64_t total_sqltsumcheck_time      = 0;
uint64_t total_sqltsum_time           = 0;
uint64_t total_sqlsum_time            = 0;
uint64_t total_sqlent_time            = 0;
uint64_t total_xattrdone_time         = 0;
uint64_t total_detachdb_time          = 0;
uint64_t total_closedir_time          = 0;
uint64_t total_utime_time             = 0;
uint64_t total_free_work_time         = 0;
uint64_t total_output_timestamps_time = 0;

uint64_t buffer_sum(sll_t *timers) {
    uint64_t sum = 0;
    sll_loop(timers, node) {
        struct start_end *timer = (struct start_end *) sll_node_data(node);
        sum += nsec(timer);
    }

    return sum;
}

#define print_stats(normal_fmt, terse_fmt, ...)             \
    if (in.terse) {                                         \
        fprintf(stderr, terse_fmt " ", ##__VA_ARGS__);      \
    }                                                       \
    else {                                                  \
        fprintf(stderr, normal_fmt "\n", ##__VA_ARGS__);    \
    }

#endif
#else
sll_t *descend_timers_init() { return NULL; }
void descend_timers_destroy(sll_t *dt) { (void) dt; }
#define buffered_start(name)
#define buffered_end(name)
#endif

static void increment_query_count(ThreadArgs_t *ta) {
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    ta->queries++;
    #else
    (void) ta;
    #endif
}

/* name doesn't matter, so long as it is not used by callers */
static const char ATTACH_NAME[] = "tree";

/* mutex doing global things */
pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;

/* prepend the current direcoty to the database filenamee */
size_t xattr_modify_filename(char *dst, const size_t dst_size,
                             const char *src, const size_t src_len,
                             struct work *work) {
    return SNFORMAT_S(dst, dst_size, 3,
                      work->name, work->name_len,
                      "/", (size_t) 1,
                      src, src_len + 1); /* NULL terminate */
}

/* Push the subdirectories in the current directory onto the queue */
static size_t descend2(QPTPool_t *ctx,
                       const size_t id,
                       struct work *passmywork,
                       DIR *dir,
                       trie_t *skip_names,
                       QPTPoolFunc_t func,
                       const size_t max_level
                       #ifdef DEBUG
                       , sll_t *timers
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
    const int level_check = (next_level < max_level);
    buffered_end(level_cmp);

    buffered_start(level_branch);
    if (level_check) {
        buffered_end(level_branch);

        /* Send subdirs to queue */
        /* loop over dirents*/
        /* skip db.db and any filename listed in the trie struct*/
        /* fill qwork struct for each dirent*/
        buffered_start(while_branch);
        while (1) {
            buffered_start(readdir_call);
            struct dirent *entry = readdir(dir);
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
            size_t len = strlen(entry->d_name);
            const int skip = (trie_search(skip_names, entry->d_name, len) ||
                             (strncmp(entry->d_name + len - 3, ".db", 3) == 0));
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

            /* append entry name to directory */
            qwork.name_len = SNFORMAT_S(qwork.name, MAXPATH, 3, passmywork->name, passmywork->name_len, "/", (size_t) 1, entry->d_name, len);

            /* append convertd entry name to convertd directory */
            qwork.sqlite3_name_len = SNFORMAT_S(qwork.sqlite3_name, MAXPATH, 2, passmywork->sqlite3_name, passmywork->sqlite3_name_len, "/", (size_t) 1);
            size_t convertd_len = sqlite_uri_path(qwork.sqlite3_name + qwork.sqlite3_name_len, MAXPATH - qwork.sqlite3_name_len, entry->d_name, &len);
            qwork.sqlite3_name_len += convertd_len;
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
                    qwork.root = passmywork->root;
                    qwork.root_len = passmywork->root_len;
                    /* qwork.type[0] = 'd'; */

                    /* this is how the parent gets passed on */
                    /* qwork.pinode = passmywork->statuso.st_ino; */
                    buffered_end(set);

                    /* make a clone here so that the data can be pushed into the queue */
                    /* this is more efficient than malloc+free for every single entry */
                    buffered_start(make_clone);
                    struct work *clone = (struct work *) malloc(sizeof(struct work));
                    memcpy(clone, &qwork, sizeof(struct work));
                    buffered_end(make_clone);

                    /* push the subdirectory into the queue for processing */
                    buffered_start(pushdir);
                    QPTPool_enqueue(ctx, id, func, clone);
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

int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    int recs;
    char shortname[MAXPATH];
    char endname[MAXPATH];
    DIR *dir = NULL;

    /* /\* Can probably skip this *\/ */
    /* if (!data) { */
    /*     return 1; */
    /* } */

    /* /\* Can probably skip this *\/ */
    /* if (!ctx || (id >= ctx->size)) { */
    /*     free(data); */
    /*     return 1; */
    /* } */

    PoolArgs_t *pa = (PoolArgs_t *) args;
    ThreadArgs_t *ta = &(pa->ta[id]);

    struct work *work = (struct work *) data;

    char dbname[MAXPATH];
    SNFORMAT_S(dbname, MAXPATH, 2, work->sqlite3_name, work->sqlite3_name_len, "/" DBNAME, DBNAME_LEN + 1);

    timestamp_create_zero(opendir_call,         pa->start_time);
    timestamp_create_zero(attachdb_call,        pa->start_time);
    timestamp_create_zero(addqueryfuncs_call,   pa->start_time);
    timestamp_create_zero(xattrprep_call,       pa->start_time);
    timestamp_create_zero(get_rollupscore_call, pa->start_time);
    timestamp_create_zero(descend_call,         pa->start_time);
    sll_t *descend_timers = descend_timers_init();
    timestamp_create_zero(sqltsumcheck,         pa->start_time);
    timestamp_create_zero(sqltsum,              pa->start_time);
    timestamp_create_zero(sqlsum,               pa->start_time);
    timestamp_create_zero(sqlent,               pa->start_time);
    timestamp_create_zero(xattrdone_call,       pa->start_time);
    timestamp_create_zero(detachdb_call,        pa->start_time);
    timestamp_create_zero(closedir_call,        pa->start_time);
    timestamp_create_zero(utime_call,           pa->start_time);
    timestamp_create_zero(free_work,            pa->start_time);

    /* keep opendir near opendb to help speed up sqlite3_open_v2 */
    timestamp_set_start(opendir_call);
    dir = opendir(work->name);
    timestamp_set_end(opendir_call);

    /* if the directory can't be opened, don't bother with anything else */
    if (!dir) {
        /* fprintf(stderr, "Could not open directory %s: %d %s\n", work->name, errno, strerror(errno)); */
        goto out_free;
    }

    #if OPENDB
    timestamp_set_start(attachdb_call);
    sqlite3 *db = attachdb(dbname, ta->outdb, ATTACH_NAME, in.open_flags, 1);
    timestamp_set_end(attachdb_call);
    increment_query_count(ta);
    #endif

    /* this is needed to add some query functions like path() uidtouser() gidtogroup() */
    #ifdef ADDQUERYFUNCS
    timestamp_set_start(addqueryfuncs_call);
    if (db) {
        if (addqueryfuncs_with_context(db, id, work) != 0) {
            fprintf(stderr, "Could not add functions to sqlite\n");
        }
    }
    timestamp_set_end(addqueryfuncs_call);
    #endif

    /* set up XATTRS so that it can be used by -T, -S, and -E */
    if (db && in.external_enabled) {
        static const char XATTR_COLS[] = " SELECT inode, name, value FROM ";

        timestamp_set_start(xattrprep_call);
        external_loop(work, db,
                      XATTRS, sizeof(XATTRS) - 1,
                      XATTR_COLS, sizeof(XATTR_COLS) - 1,
                      XATTRS_AVAIL, sizeof(XATTRS_AVAIL) - 1,
                      xattr_modify_filename
                      #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                      , &ta->queries
                      #endif
            );
        xattr_create_views(db
                           #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                           , &ta->queries
                           #endif
            );
        timestamp_set_end(xattrprep_call);
    }

    recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
            /* if it fails then this will be set to 1 and will go on */

    /* if AND operation, and sqltsum is there, run a query to see if there is a match. */
    /* if this is OR, as well as no-sql-to-run, skip this query */
    if (in.sql.tsum_len > 1) {
        if (in.andor == 0) {      /* AND */
            /* make sure the treesummary table exists */
            timestamp_set_start(sqltsumcheck);
            querydb(dbname, db, "SELECT name FROM sqlite_master WHERE type=\'table\' AND name='treesummary';",
                  pa, id, &recs);
            timestamp_set_end(sqltsumcheck);
            increment_query_count(ta);
            if (recs < 1) {
                recs = -1;
            }
            else {
                /* run in.sql.tsum */
                timestamp_set_start(sqltsum);
                querydb(dbname, db, in.sql.tsum, pa, id, &recs);
                timestamp_set_end(sqltsum);
                increment_query_count(ta);
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
        /* get the rollup score
         * ignore errors - if the db wasn't opened, or if
         * summary is missing the columns, keep descending
         */
        timestamp_set_start(get_rollupscore_call);
        int rollupscore = 0;
        if (db) {
            get_rollupscore(db, &rollupscore);
            increment_query_count(ta);
        }
        timestamp_set_end(get_rollupscore_call);

        /* push subdirectories into the queue */
        if (rollupscore == 0) {
            #ifdef DEBUG
            timestamp_set_start(descend_call);
            #ifdef SUBDIRECTORY_COUNTS
            const size_t pushed =
            #endif
            #endif
            descend2(ctx, id, work, dir, pa->skip, processdir, in.max_level
                     #ifdef DEBUG
                     , descend_timers
                     #endif
                    );
            #ifdef DEBUG
            timestamp_set_end(descend_call);
            #ifdef SUBDIRECTORY_COUNTS
            pthread_mutex_lock(&print_mutex);
            fprintf(stderr, "%s %zu\n", work->name, pushed);
            pthread_mutex_unlock(&print_mutex);
            #endif
            #endif
        }

        if (db) {
            /* only query this level if the min_level has been reached */
            if (work->level >= in.min_level) {
                /* run query on summary, print it if printing is needed, if returns none */
                /* and we are doing AND, skip querying the entries db */
                /* memset(endname, 0, sizeof(endname)); */
                shortpath(work->name,shortname,endname);
                SNFORMAT_S(gps[id].gepath, MAXPATH, 1, endname, strlen(endname));

                if (in.sql.sum_len > 1) {
                    recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
                    /* put in the path relative to the user's input */
                    SNFORMAT_S(gps[id].gpath, MAXPATH, 1, work->name, work->name_len);
                    /* printf("processdir: setting gpath = %s and gepath %s\n",gps[mytid].gpath,gps[mytid].gepath); */
                    realpath(work->name,gps[id].gfpath);

                    timestamp_set_start(sqlsum);
                    querydb(dbname, db, in.sql.sum, pa, id, &recs);
                    timestamp_set_end(sqlsum);
                    increment_query_count(ta);
                } else {
                    recs = 1;
                }
                if (in.andor > 0) {
                    recs = 1;
                }
                /* if we have recs (or are running an OR) query the entries table */
                if (recs > 0) {
                    if (in.sql.ent_len > 1) {
                        /* set the path so users can put path() in their queries */
                        /* printf("****entries len of in.sql.ent %lu\n",strlen(in.sql.ent)); */
                        SNFORMAT_S(gps[id].gpath, MAXPATH, 1, work->name, work->name_len);
                        realpath(work->name,gps[id].gfpath);

                        timestamp_set_start(sqlent);
                        querydb(dbname, db, in.sql.ent, pa, id, &recs); /* recs is not used */
                        timestamp_set_end(sqlent);
                        increment_query_count(ta);
                    }
                }
            }
        }
    }

    /* detach xattr dbs */
    timestamp_set_start(xattrdone_call);
    if (db && in.external_enabled) {
        external_done(db, "DROP VIEW " XATTRS ";"
                      #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                      , &ta->queries
                      #endif
        );
    }
    timestamp_set_end(xattrdone_call);

    #ifdef OPENDB
    timestamp_set_start(detachdb_call);
    if (db) {
        detachdb(dbname, db, ATTACH_NAME, 1);
        increment_query_count(ta);
    }
    timestamp_set_end(detachdb_call);
    #endif

    timestamp_set_start(closedir_call);
    closedir(dir);
    timestamp_set_end(closedir_call);

    timestamp_set_start(utime_call);
    /* restore mtime and atime */
    if (in.keep_matime) {
        struct utimbuf dbtime = {};
        dbtime.actime  = work->statuso.st_atime;
        dbtime.modtime = work->statuso.st_mtime;
        utime(dbname, &dbtime);
    }
    timestamp_set_end(utime_call);

  out_free:
    ;

    timestamp_set_start(free_work);
    free(work);
    timestamp_set_end(free_work);

    #ifdef DEBUG
    timestamp_start(output_timestamps);

    timestamp_create_buffer(4096);
    timestamp_print             (ctx->buffers, id, "opendir",            opendir_call);
    if (dir) {
        #ifdef OPENDB
        timestamp_print         (ctx->buffers, id, "attachdb",           attachdb_call);
        #endif
        if (db) {
            #ifdef ADDQUERYFUNCS
            timestamp_print     (ctx->buffers, id, "addqueryfuncs",      addqueryfuncs_call);
            #endif
            timestamp_print     (ctx->buffers, id, "xattrprep",          xattrprep_call);
            timestamp_print     (ctx->buffers, id, "get_rollupscore",    get_rollupscore_call);
            timestamp_print     (ctx->buffers, id, "descend",            descend_call);
            print_descend_timers(ctx->buffers, id, "within_descend",     descend_timers, within_descend);
            print_descend_timers(ctx->buffers, id, "check_args",         descend_timers, check_args);
            print_descend_timers(ctx->buffers, id, "level",              descend_timers, level_cmp);
            print_descend_timers(ctx->buffers, id, "level_branch",       descend_timers, level_branch);
            print_descend_timers(ctx->buffers, id, "while_branch",       descend_timers, while_branch);
            print_descend_timers(ctx->buffers, id, "readdir",            descend_timers, readdir_call);
            print_descend_timers(ctx->buffers, id, "readdir_branch",     descend_timers, readdir_branch);
            print_descend_timers(ctx->buffers, id, "strncmp",            descend_timers, strncmp_call);
            print_descend_timers(ctx->buffers, id, "strncmp_branch",     descend_timers, strncmp_branch);
            print_descend_timers(ctx->buffers, id, "snprintf",           descend_timers, snprintf_call);
            print_descend_timers(ctx->buffers, id, "lstat",              descend_timers, lstat_call);
            print_descend_timers(ctx->buffers, id, "isdir",              descend_timers, isdir_cmp);
            print_descend_timers(ctx->buffers, id, "isdir_branch",       descend_timers, isdir_branch);
            print_descend_timers(ctx->buffers, id, "access",             descend_timers, access_call);
            print_descend_timers(ctx->buffers, id, "set",                descend_timers, set);
            print_descend_timers(ctx->buffers, id, "clone",              descend_timers, make_clone);
            print_descend_timers(ctx->buffers, id, "pushdir",            descend_timers, pushdir);
            #ifdef SQL_EXEC
            timestamp_print     (ctx->buffers, id, "sqltsumcheck",       sqltsumcheck);
            timestamp_print     (ctx->buffers, id, "sqltsum",            sqltsum);
            timestamp_print     (ctx->buffers, id, "sqlsum",             sqlsum);
            timestamp_print     (ctx->buffers, id, "sqlent",             sqlent);
            #endif
            timestamp_print     (ctx->buffers, id, "xattrdone",          xattrdone_call);
            timestamp_print     (ctx->buffers, id, "detachdb",           detachdb_call);
        }
        timestamp_print         (ctx->buffers, id, "closedir",           closedir_call);
        timestamp_print         (ctx->buffers, id, "utime",              utime_call);
        timestamp_print         (ctx->buffers, id, "free_work",          free_work);
    }

    timestamp_set_end(output_timestamps);
    timestamp_print             (ctx->buffers, id, "output_timestamps",  output_timestamps);

    #ifdef CUMULATIVE_TIMES
    pthread_mutex_lock(&print_mutex);
    total_opendir_time           += timestamp_elapsed(opendir_call);
    total_attachdb_time          += timestamp_elapsed(attachdb_call);
    total_xattrprep_time         += timestamp_elapsed(xattrprep_call);
    total_get_rollupscore_time   += timestamp_elapsed(get_rollupscore_call);
    total_descend_time           += timestamp_elapsed(descend_call);
    total_check_args_time        += buffer_sum(&descend_timers[dt_check_args]);
    total_level_time             += buffer_sum(&descend_timers[dt_level_cmp]);
    total_level_branch_time      += buffer_sum(&descend_timers[dt_level_branch]);
    total_while_branch_time      += buffer_sum(&descend_timers[dt_while_branch]);
    total_readdir_time           += buffer_sum(&descend_timers[dt_readdir_call]);
    total_readdir_branch_time    += buffer_sum(&descend_timers[dt_readdir_branch]);
    total_strncmp_time           += buffer_sum(&descend_timers[dt_strncmp_call]);
    total_strncmp_branch_time    += buffer_sum(&descend_timers[dt_strncmp_branch]);
    total_snprintf_time          += buffer_sum(&descend_timers[dt_snprintf_call]);
    total_lstat_time             += buffer_sum(&descend_timers[dt_lstat_call]);
    total_isdir_time             += buffer_sum(&descend_timers[dt_isdir_cmp]);
    total_isdir_branch_time      += buffer_sum(&descend_timers[dt_isdir_branch]);
    total_access_time            += buffer_sum(&descend_timers[dt_access_call]);
    total_set_time               += buffer_sum(&descend_timers[dt_set]);
    total_clone_time             += buffer_sum(&descend_timers[dt_make_clone]);
    total_pushdir_time           += buffer_sum(&descend_timers[dt_pushdir]);
    total_closedir_time          += timestamp_elapsed(closedir_call);
    total_sqltsumcheck_time      += timestamp_elapsed(sqltsumcheck);
    total_sqltsum_time           += timestamp_elapsed(sqltsum);
    total_sqlsum_time            += timestamp_elapsed(sqlsum);
    total_sqlent_time            += timestamp_elapsed(sqlent);
    total_xattrdone_time         += timestamp_elapsed(xattrdone_call);
    total_detachdb_time          += timestamp_elapsed(detachdb_call);
    total_utime_time             += timestamp_elapsed(utime_call);
    total_free_work_time         += timestamp_elapsed(free_work);
    total_output_timestamps_time += timestamp_elapsed(output_timestamps);
    pthread_mutex_unlock(&print_mutex);
    #endif

    #ifdef PER_THREAD_STATS
    timestamp_print(ctx->buffers, id, "output_timestamps", output_timestamps);
    #endif
    #endif

    /* call here to match descend_timers_init call outside of DEBUG block */
    descend_timers_destroy(descend_timers);

    return 0;
}

int validate_inputs(struct input *in) {
    /*
     * - Leaves are final outputs
     * - OUTFILE/OUTDB + aggregation will create per thread and final aggregation files
     *
     *                         init                       | if writing to outdb or aggregating
     *                          |                         | -I CREATE [TEMP] TABLE <name>
     *                          |
     *                          |                         | if aggregating, create an aggregate table
     *                          |                         | -K CREATE [TEMP] TABLE <aggregate name>
     *                          |
     *   -----------------------------------------------  | start walk
     *                          |
     *                       thread
     *                          |
     *          -------------------------------
     *          |               |             |
     *          |               |       stdout/outfile.*  | -T/-S/-E SELECT FROM <index table>
     *          |               |
     *   intermediate db      outdb.*                     | -T/-S/-E INSERT into <name> SELECT FROM <index table>
     *          |
     *          |
     *   -----------------------------------------------  | after walking index
     *          |
     *          |                                         | move intermediate results into aggregate table
     *    aggregate db - outdb                            | -J INSERT INTO <aggregate name>
     *    |          |
     * outfile     stdout                                 | Get final results
     *                                                    | -G SELECT * FROM <aggregate name>
     */
     if ((in->output == OUTDB) || in->sql.init_agg_len) {
        /* -I (required) */
        if (!in->sql.init_len) {
            fprintf(stderr, "Error: Missing -I\n");
            return -1;
        }
    }

    /* -T, -S, -E (at least 1) */
    if ((in->sql.tsum_len + in->sql.sum_len + in->sql.ent_len) == 0) {
        fprintf(stderr, "Error: Need at least one of -T, -S, or -E\n");
        return -1;
    }

    /* not aggregating */
    if (!in->sql.init_agg_len) {
        if (in->sql.intermediate_len) {
            fprintf(stderr, "Warning: Got -J even though not aggregating. Ignoring\n");
        }

        if (in->sql.agg_len) {
            fprintf(stderr, "Warning: Got -G even though not aggregating. Ignoring\n");
        }
    }
    /* aggregating */
    else {
        /* need -J to write to aggregate db */
        if (!in->sql.intermediate_len) {
            fprintf(stderr, "Error: Missing -J\n");
            return -1;
        }

        if ((in->output == STDOUT) || (in->output == OUTFILE)) {
            /* need -G to write out results */
            if (!in->sql.agg_len) {
                fprintf(stderr, "Error: Missing -G\n");
                return -1;
            }
        }
        /* -G can be called when aggregating, but is not necessary */
    }

    return 0;
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
    int idx = parse_cmd_line(argc, argv, "hHT:S:E:an:jo:d:O:I:F:y:z:J:K:G:m:B:wxk:", 1, "GUFI_index ...", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;

    if (validate_inputs(&in) != 0) {
        return -1;
    }

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamp_start(setup_globals);
    #endif

    PoolArgs_t pa;
    if (PoolArgs_init(&pa, &in, &global_mutex) != 0) {
        return -1;
    }

    #ifdef DEBUG
    pa.start_time = &now;
    #endif

    #ifdef DEBUG
    timestamp_init(timestamp_buffers, in.maxthreads, 1073741824ULL, &global_mutex);

    #ifdef CUMULATIVE_TIMES
    timestamp_set_end(setup_globals);
    const uint64_t setup_globals_time = timestamp_elapsed(setup_globals);
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
    timestamp_start(setup_aggregate);
    #endif

    Aggregate_t aggregate;
    memset(&aggregate, 0, sizeof(aggregate));
    if (in.sql.init_agg_len) {
        if (!aggregate_init(&aggregate, &in)) {
            PoolArgs_fin(&pa, in.maxthreads);
            return -1;
        }
    }

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamp_set_end(setup_aggregate);
    const uint64_t setup_aggregate_time = timestamp_elapsed(setup_aggregate);
    #endif

    #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
    uint64_t total_time = 0;

    timestamp_start(work);
    #endif

    /* provide a function to print if PRINT is set */
    QPTPool_t *pool = QPTPool_init(in.maxthreads, &pa, NULL, NULL
                                   #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                   , timestamp_buffers
                                   #endif
        );

    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        aggregate_fin(&aggregate, &in);
        PoolArgs_fin(&pa, in.maxthreads);
        return -1;
    }

    /* enqueue all input paths */
    for(int i = idx; i < argc; i++) {
        /* remove trailing slashes */
        size_t len = trailing_match_index(argv[i], strlen(argv[i]), "/", 1);

        /* root is special case */
        if (len == 0) {
            argv[i][0] = '/';
            len = 1;
        }

        struct work *mywork = calloc(1, sizeof(struct work));

        /* copy argv[i] into the work item */
        mywork->name_len = SNFORMAT_S(mywork->name, MAXPATH, 1, argv[i], len);
        mywork->sqlite3_name_len = sqlite_uri_path(mywork->sqlite3_name, MAXPATH, argv[i], &len);

        lstat(mywork->name,&mywork->statuso);
        if (!S_ISDIR(mywork->statuso.st_mode) ) {
            fprintf(stderr,"input-dir '%s' is not a directory\n", mywork->name);
            free(mywork);
            continue;
        }

        /* parent of input path */
        mywork->root = argv[i];
        mywork->root_len = mywork->name_len;
        while (mywork->root_len && (mywork->root[mywork->root_len - 1] != '/')) {
            mywork->root_len--;
        }

        mywork->root[mywork->root_len] = '\0';

        /* push the path onto the queue */
        QPTPool_enqueue(pool, i % in.maxthreads, processdir, mywork);
    }

    QPTPool_wait(pool);

    #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
    const size_t thread_count = QPTPool_threads_completed(pool);
    #endif

    QPTPool_destroy(pool);

    #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
    timestamp_set_end(work);

    const uint64_t work_time = timestamp_elapsed(work);
    total_time += work_time;
    #endif

    #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
    uint64_t aggregate_time = 0;
    uint64_t output_time = 0;
    size_t rows = 0;
    #endif

    int rc = 0;
    if (in.sql.init_agg_len) {
        #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
        timestamp_start(aggregation);
        #endif

        /* aggregate the intermediate results */
        aggregate_intermediate(&aggregate, &pa, &in);

        #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
        timestamp_set_end(aggregation);
        aggregate_time = timestamp_elapsed(aggregation);
        total_time += aggregate_time;
        #endif

        /* final query on aggregate results */
        #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
        timestamp_start(output);

        rows +=
        #endif

        aggregate_process(&aggregate, &in);

        #if (defined(DEBUG) && defined(CUMULATIVE_TIMES)) || BENCHMARK
        timestamp_set_end(output);
        output_time = timestamp_elapsed(output);
        total_time += output_time;
        #endif

        aggregate_fin(&aggregate, &in);
    }

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES) || BENCHMARK
    timestamp_start(cleanup_globals);
    #endif

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    /* aggregate per thread query counts */
    size_t query_count = 0;
    for(int i = 0; i < in.maxthreads; i++) {
        query_count += pa.ta[i].queries;
    }
    #endif

    /* clear out buffered data */
    for(int i = 0; i < in.maxthreads; i++) {
        ThreadArgs_t *ta = &(pa.ta[i]);
        OutputBuffer_flush(&ta->output_buffer, ta->outfile);

        #if defined(DEBUG) && defined(CUMULATIVE_TIMES) || BENCHMARK
        rows += ta->output_buffer.count;
        #endif
    }

    /* clean up globals */
    PoolArgs_fin(&pa, in.maxthreads);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES) || BENCHMARK
    timestamp_set_end(cleanup_globals);
    const uint64_t cleanup_globals_time = timestamp_elapsed(cleanup_globals);
    total_time += cleanup_globals_time;

    const long double total_time_sec = sec(total_time);
    #endif

    #ifdef DEBUG
    timestamp_destroy(timestamp_buffers);
    #endif

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    const long double thread_time =
        total_opendir_time + total_xattrprep_time +
        total_attachdb_time + total_addqueryfuncs_time +
        total_descend_time + total_sqltsumcheck_time +
        total_sqltsum_time + total_sqlsum_time +
        total_sqlent_time + total_xattrdone_time +
        total_detachdb_time + total_closedir_time +
        total_utime_time + total_free_work_time +
        total_output_timestamps_time;

    print_stats("set up globals:                             %.2Lfs", "%Lf", sec(setup_globals_time));
    print_stats("set up intermediate databases:              %.2Lfs", "%Lf", sec(setup_aggregate_time));
    print_stats("thread pool:                                %.2Lfs", "%Lf", sec(work_time));
    print_stats("    open directories:                       %.2Lfs", "%Lf", sec(total_opendir_time));
    print_stats("    attach index:                           %.2Lfs", "%Lf", sec(total_attachdb_time));
    print_stats("    xattrprep:                              %.2Lfs", "%Lf", sec(total_xattrprep_time));
    print_stats("    addqueryfuncs:                          %.2Lfs", "%Lf", sec(total_addqueryfuncs_time));
    print_stats("    get_rollupscore:                        %.2Lfs", "%Lf", sec(total_get_rollupscore_time));
    print_stats("    descend:                                %.2Lfs", "%Lf", sec(total_descend_time));
    print_stats("        check args:                         %.2Lfs", "%Lf", sec(total_check_args_time));
    print_stats("        check level:                        %.2Lfs", "%Lf", sec(total_level_time));
    print_stats("        check level <= max_level branch:    %.2Lfs", "%Lf", sec(total_level_branch_time));
    print_stats("        while true:                         %.2Lfs", "%Lf", sec(total_while_branch_time));
    print_stats("            readdir:                        %.2Lfs", "%Lf", sec(total_readdir_time));
    print_stats("            readdir != null branch:         %.2Lfs", "%Lf", sec(total_readdir_branch_time));
    print_stats("            strncmp:                        %.2Lfs", "%Lf", sec(total_strncmp_time));
    print_stats("            strncmp != . or ..:             %.2Lfs", "%Lf", sec(total_strncmp_branch_time));
    print_stats("            snprintf:                       %.2Lfs", "%Lf", sec(total_snprintf_time));
    print_stats("            lstat:                          %.2Lfs", "%Lf", sec(total_lstat_time));
    print_stats("            isdir:                          %.2Lfs", "%Lf", sec(total_isdir_time));
    print_stats("            isdir branch:                   %.2Lfs", "%Lf", sec(total_isdir_branch_time));
    print_stats("            access:                         %.2Lfs", "%Lf", sec(total_access_time));
    print_stats("            set:                            %.2Lfs", "%Lf", sec(total_set_time));
    print_stats("            clone:                          %.2Lfs", "%Lf", sec(total_clone_time));
    print_stats("            pushdir:                        %.2Lfs", "%Lf", sec(total_pushdir_time));
    print_stats("    check if treesummary table exists:      %.2Lfs", "%Lf", sec(total_sqltsumcheck_time));
    print_stats("    sqltsum:                                %.2Lfs", "%Lf", sec(total_sqltsum_time));
    print_stats("    sqlsum:                                 %.2Lfs", "%Lf", sec(total_sqlsum_time));
    print_stats("    sqlent:                                 %.2Lfs", "%Lf", sec(total_sqlent_time));
    print_stats("    xattrdone:                              %.2Lfs", "%Lf", sec(total_xattrdone_time));
    print_stats("    detach index:                           %.2Lfs", "%Lf", sec(total_detachdb_time));
    print_stats("    close directories:                      %.2Lfs", "%Lf", sec(total_closedir_time));
    print_stats("    restore timestamps:                     %.2Lfs", "%Lf", sec(total_utime_time));
    print_stats("    free work:                              %.2Lfs", "%Lf", sec(total_free_work_time));
    print_stats("    output timestamps:                      %.2Lfs", "%Lf", sec(total_output_timestamps_time));
    print_stats("aggregate into final databases:             %.2Lfs", "%Lf", sec(aggregate_time));
    print_stats("print aggregated results:                   %.2Lfs", "%Lf", sec(output_time));
    print_stats("clean up globals:                           %.2Lfs", "%Lf", sec(cleanup_globals_time));
    if (!in.terse) {
        fprintf(stderr, "\n");
    }
    print_stats("Threads run:                                %zu",    "%zu", thread_count);
    print_stats("Queries performed:                          %zu",    "%zu", query_count);
    print_stats("Rows printed to stdout or outfiles:         %zu",    "%zu", rows);
    print_stats("Total Thread Time (not including main):     %.2Lfs", "%Lf", sec(thread_time));
    print_stats("Real time (main):                           %.2Lfs", "%Lf", total_time_sec);
    if (in.terse) {
        fprintf(stderr, "\n");
    }
    #endif

    #if BENCHMARK
    fprintf(stderr, "Total Dirs:            %zu\n",    thread_count);
    fprintf(stderr, "Total Files:           %zu\n",    rows);
    fprintf(stderr, "Time Spent Querying:   %.2Lfs\n", total_time_sec);
    fprintf(stderr, "Dirs/Sec:              %.2Lf\n",  thread_count / total_time_sec);
    fprintf(stderr, "Files/Sec:             %.2Lf\n",  rows / total_time_sec);
    #endif

    return rc;
}
