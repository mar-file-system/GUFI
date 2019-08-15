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



#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <ctype.h>
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
#include "pcre.h"
#include "utils.h"

extern int errno;
#define AGGREGATE_NAME         "file:aggregate%d?mode=memory&cache=shared"
#define AGGREGATE_ATTACH_NAME  "aggregate"

#if BENCHMARK
#include <time.h>

static size_t total_files = 0;
static pthread_mutex_t total_files_mutex = PTHREAD_MUTEX_INITIALIZER;

static int total_files_callback(void * unused, int count, char ** data, char ** columns) {
    const size_t files = atol(data[0]);
    pthread_mutex_lock(&total_files_mutex);
    total_files += files;
    pthread_mutex_unlock(&total_files_mutex);
    return 0;
}

#endif

#ifdef DEBUG

#include <stdint.h>
#include <inttypes.h>

uint64_t timestamp(struct timespec * ts) {
    uint64_t ns = ts->tv_sec;
    ns *= 1000000000ULL;
    ns += ts->tv_nsec;
    return ns;
}

extern uint64_t epoch;

static const char GUFI_SQLITE_VFS[] = "unix-none";

int create_table_wrapper(const char *name, sqlite3 * db, const char * sql_name, const char * sql, int (*callback)(void*,int,char**,char**), void * args);
int create_tables(const char *name, sqlite3 *db);
int set_pragmas(sqlite3 * db);

static sqlite3 * opendb2(const char * name, const int rdonly, const int createtables, const int setpragmas
                  , struct timespec * sqlite3_open_start
                  , struct timespec * sqlite3_open_end
                  , struct timespec * create_tables_start
                  , struct timespec * create_tables_end
                  , struct timespec * set_pragmas_start
                  , struct timespec * set_pragmas_end
) {
    sqlite3 * db = NULL;

    if (rdonly && createtables) {
        fprintf(stderr, "Cannot open database: readonly and createtables cannot both be set at the same time\n");
        return NULL;
    }

    int flags = SQLITE_OPEN_URI;
    if (rdonly) {
        flags |= SQLITE_OPEN_READONLY;
    }
    else {
        flags |= SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE;
    }

    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, sqlite3_open_start);
    #endif
    // no need to create because the file should already exist
    if (sqlite3_open_v2(name, &db, flags, GUFI_SQLITE_VFS) != SQLITE_OK) {
        #ifdef DEBUG
        clock_gettime(CLOCK_MONOTONIC, sqlite3_open_end);
        #endif
        /* fprintf(stderr, "Cannot open database: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db)); */
        sqlite3_close(db); // close db even if it didn't open to avoid memory leaks
        return NULL;
    }
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, sqlite3_open_end);
    #endif

    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, create_tables_start);
    #endif
    if (createtables) {
        if (create_tables(name, db) != 0) {
            fprintf(stderr, "Cannot create tables: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db));
            sqlite3_close(db);
            return NULL;
        }
    }
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, create_tables_end);
    #endif

    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, set_pragmas_start);
    #endif
    if (setpragmas) {
        // ignore errors
        set_pragmas(db);
    }
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, set_pragmas_end);
    #endif

    return db;
}
#endif

// Push the subdirectories in the current directory onto the queue
static size_t descend2(struct QPTPool *ctx,
                       const size_t id,
                       struct work *passmywork,
                       DIR *dir,
                       const size_t max_level
                       #ifdef DEBUG
                       , struct timespec *readdir_start
                       , struct timespec *readdir_end
                       , struct timespec *lstat_start
                       , struct timespec *lstat_end
                       , struct timespec *pushdir_start
                       , struct timespec *pushdir_end
                       #endif
    ) {

    if (!passmywork) {
        fprintf(stderr, "Got NULL work\n");
        return 0;
    }

    if (!dir) {
        fprintf(stderr, "Could not open directory %s: %d %s\n", passmywork->name, errno, strerror(errno));
        return 0;
    }

    size_t pushed = 0;
    const size_t next_level = passmywork->level + 1;
    if (next_level <= max_level) {
        // go ahead and send the subdirs to the queue since we need to look
        // further down the tree.  loop over dirents, if link push it on the
        // queue, if file or link print it, fill up qwork structure for
        // each
        struct dirent *entry = NULL;
        while (1) {
            #ifdef DEBUG
            clock_gettime(CLOCK_MONOTONIC, readdir_start);
            #endif
            entry = readdir(dir);
            #ifdef DEBUG
            clock_gettime(CLOCK_MONOTONIC, readdir_end);
            #endif
            if (!entry) {
                break;
            }

            const size_t len = strlen(entry->d_name);
            if (((len == 1) && (strncmp(entry->d_name, ".",  1) == 0)) ||
                ((len == 2) && (strncmp(entry->d_name, "..", 2) == 0))) {
                continue;
            }

            struct work qwork;
            SNPRINTF(qwork.name, MAXPATH, "%s/%s", passmywork->name, entry->d_name);

            #ifdef DEBUG
            clock_gettime(CLOCK_MONOTONIC, lstat_start);
            #endif
            lstat(qwork.name, &qwork.statuso);
            #ifdef DEBUG
            clock_gettime(CLOCK_MONOTONIC, lstat_end);
            #endif

            if (S_ISDIR(qwork.statuso.st_mode)) {
                if (!access(qwork.name, R_OK | X_OK)) {
                    qwork.level = next_level;
                    qwork.type[0] = 'd';

                    // this is how the parent gets passed on
                    qwork.pinode = passmywork->statuso.st_ino;

                    // make a copy here so that the data can be pushed into the queue
                    // this is more efficient than malloc+free for every single entry
                    struct work * copy = (struct work *) calloc(1, sizeof(struct work));
                    memcpy(copy, &qwork, sizeof(struct work));

                    // this pushes the dir onto queue - pushdir does locking around queue update
                    #ifdef DEBUG
                    clock_gettime(CLOCK_MONOTONIC, pushdir_start);
                    #endif
                    QPTPool_enqueue(ctx, id, copy);
                    #ifdef DEBUG
                    clock_gettime(CLOCK_MONOTONIC, pushdir_end);
                    #endif
                    pushed++;
                }
                /* else { */
                /*     fprintf(stderr, "couldn't access dir '%s': %s\n", */
                /*             qwork->name, strerror(errno)); */
                /* } */
            }
            /* else { */
            /*     fprintf(stderr, "not a dir '%s': %s\n", */
            /*             qwork->name, strerror(errno)); */
            /* } */
        }
    }

    return pushed;
}

// //////////////////////////////////////////////////////
// these functions need to be moved back into dbutils
static void path2(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    const size_t id = QPTPool_get_index((struct QPTPool *) sqlite3_user_data(context), pthread_self());
    sqlite3_result_text(context, gps[id].gpath, -1, SQLITE_TRANSIENT);

    return;
}

int addqueryfuncs2(sqlite3 *db, struct QPTPool * ctx) {
    return !(sqlite3_create_function(db, "path", 0, SQLITE_UTF8, ctx, &path2, NULL, NULL) == SQLITE_OK);
}
// //////////////////////////////////////////////////////

// print the results of the query
static int print_callback(void *out, int count, char **data, char **columns) {
    static pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;
    static char ffielddelim[2];
    switch (in.dodelim) {
        case 0:
            SNPRINTF(ffielddelim,2,"|");
            break;
        case 1:
            SNPRINTF(ffielddelim,2,"%s",fielddelim);
            break;
        case 2:
            SNPRINTF(ffielddelim,2,"%s",in.delim);
            break;
    }

    if (out) {
        pthread_mutex_lock(&print_mutex);
        for(int i = 0; i < count; i++) {
            fprintf((FILE *) out, "%s%s", data[i], ffielddelim);
        }
        fprintf((FILE *) out, "\n");
        pthread_mutex_unlock(&print_mutex);
    }
    return 0;
}

int processdir(struct QPTPool * ctx, void * data , const size_t id, void * args) {
    sqlite3 *db = NULL;
    int recs;
    int trecs;
    char shortname[MAXPATH];
    char endname[MAXPATH];
    DIR * dir = NULL;

    if (!data) {
        return 1;
    }

    if (!ctx || (id >= ctx->size)) {
        free(data);
        return 1;
    }

    struct work * work = (struct work *) data;

    // only print if not aggregating and user wants to print
    // where to output to when printing
    FILE *out = stdout;
    if (in.outfile > 0) {
       out = gts.outfd[id];
    }

    char dbname[MAXSQL];
    SNPRINTF(dbname, MAXSQL, "%s/" DBNAME, work->name);

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
    struct timespec attach_start;
    struct timespec attach_end;
    struct timespec descend_start;
    struct timespec descend_end;
    struct timespec readdir_start;
    struct timespec readdir_end;
    struct timespec lstat_start;
    struct timespec lstat_end;
    struct timespec pushdir_start;
    struct timespec pushdir_end;
    struct timespec exec_start;
    struct timespec exec_end;
    struct timespec detach_start;
    struct timespec detach_end;
    struct timespec close_start;
    struct timespec close_end;
    struct timespec closedir_start;
    struct timespec closedir_end;
    #endif

    // keep track of the mtime and atime
    struct utimbuf dbtime = {};
    if (in.keep_matime) {
        struct stat st;
        if (lstat(dbname, &st) != 0) {
            perror("stat");
            goto out_free;
        }

        dbtime.actime  = st.st_atime;
        dbtime.modtime = st.st_mtime;
    }

    // open directory
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &opendir_start);
   #endif
    // keep opendir near opendb to help speed up sqlite3_open_v2
    dir = opendir(work->name);
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &opendir_end);
    #endif

    // if we have out db then we have that db open so we just attach the gufi db
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &open_start);
    #endif
    if (in.outdb > 0) {
      db = gts.outdbd[id];
      attachdb(dbname, db, "tree");
    } else {
      db = opendb2(dbname, 1, 0, 0
                   #ifdef DEBUG
                   , &sqlite3_open_start
                   , &sqlite3_open_end
                   , &create_tables_start
                   , &create_tables_end
                   , &set_pragmas_start
                   , &set_pragmas_end
                   #endif
          );
    }
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &open_end);
    #endif

    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &attach_start);
    #endif
    if (in.aggregate_or_print == AGGREGATE) {
        // attach in-memory result aggregation database
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

    // this is needed to add some query functions like path() uidtouser() gidtogroup()
    if (db) {
        addqueryfuncs2(db, ctx);
    }

    recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
             /* if it fails then this will be set to 1 and will go on */

    // if AND operation, and sqltsum is there, run a query to see if there is a match.
    // if this is OR, as well as no-sql-to-run, skip this query
    if (in.sqltsum_len > 1) {

       if (in.andor == 0) {      // AND
           trecs=rawquerydb(work->name, 0, db, (char *) "select name from sqlite_master where type=\'table\' and name=\'treesummary\';", 0, 0, 0, id);
         if (trecs<1) {
           recs=-1;
         } else {
           recs=rawquerydb(work->name, 0, db, in.sqltsum, 0, 0, 0, id);
         }
      }
      // this is an OR or we got a record back. go on to summary/entries
      // queries, if not done with this dir and all dirs below it
      // this means that no tree table exists so assume we have to go on
      if (recs < 0) {
        recs=1;
      }
    }
    // so we have to go on and query summary and entries possibly
    if (recs > 0) {
        #ifdef DEBUG
        clock_gettime(CLOCK_MONOTONIC, &descend_start);
        #endif
        // push subdirectories into the queue
        descend2(ctx, id, work, dir, in.max_level
                 #ifdef DEBUG
                 , &readdir_start
                 , &readdir_end
                 , &lstat_start
                 , &lstat_end
                 , &pushdir_start
                 , &pushdir_end
                 #endif
            );
        #ifdef DEBUG
        clock_gettime(CLOCK_MONOTONIC, &descend_end);
        #endif

        if (db) {
            // only query this level if the min_level has been reached
            if (work->level >= in.min_level) {
                // run query on summary, print it if printing is needed, if returns none
                // and we are doing AND, skip querying the entries db
                // memset(endname, 0, sizeof(endname));
                shortpath(work->name,shortname,endname);
                SNPRINTF(gps[id].gepath,MAXPATH,"%s",endname);

                if (in.sqlsum_len > 1) {
                    recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
                    // for directories we have to take off after the last slash
                    // and set the path so users can put path() in their queries
                    SNPRINTF(gps[id].gpath,MAXPATH,"%s",shortname);
                    //printf("processdir: setting gpath = %s and gepath %s\n",gps[mytid].gpath,gps[mytid].gepath);
                    realpath(work->name,gps[id].gfpath);
                    recs = rawquerydb(work->name, 1, db, in.sqlsum, 1, 0, 0, id);
                    //printf("summary ran %s on %s returned recs %d\n",in.sqlsum,work->name,recs);
                } else {
                    recs = 1;
                }
                if (in.andor > 0)
                    recs = 1;

                // if we have recs (or are running an OR) query the entries table
                if (recs > 0) {
                    if (in.sqlent_len > 1) {
                        // set the path so users can put path() in their queries
                        //printf("****entries len of in.sqlent %lu\n",strlen(in.sqlent));
                        SNPRINTF(gps[id].gpath,MAXPATH,"%s",work->name);
                        realpath(work->name,gps[id].gfpath);

                        #ifdef DEBUG
                        clock_gettime(CLOCK_MONOTONIC, &exec_start);
                        #endif
                        char *err = NULL;
                        if (sqlite3_exec(db, in.sqlent, (int (*)(void*,int,char**,char**)) args, out, &err) != SQLITE_OK) {
                            fprintf(stderr, "Error: %s: \"%s\"\n", err, in.sqlent);
                            sqlite3_free(err);
                        }
                        #ifdef DEBUG
                        clock_gettime(CLOCK_MONOTONIC, &exec_end);
                        #endif

                        #if BENCHMARK
                        // get the total number of files in this database, regardless of whether or not the query was successful
                        if (in.outdb > 0) {
                            sqlite3_exec(db, "SELECT COUNT(*) FROM tree.entries", total_files_callback, NULL, NULL);
                        }
                        else {
                            sqlite3_exec(db, "SELECT COUNT(*) FROM entries", total_files_callback, NULL, NULL);
                        }
                        #endif
                    }
                }
            }
        }
    }

    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &detach_start);
    #endif
    if (in.aggregate_or_print == AGGREGATE) {
        // detach in-memory result aggregation database
        if (db && !detachdb(AGGREGATE_NAME, db, AGGREGATE_ATTACH_NAME)) {
            closedb(db);
            goto out_dir;
        }
    }
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &detach_end);
    #endif

    // if we have an out db we just detach gufi db
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &close_start);
    #endif
    if (in.outdb > 0) {
      detachdb(dbname, db, "tree");
    } else {
      closedb(db);
    }
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &close_end);
    #endif

  out_dir:
    ;

    // close dir
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &closedir_start);
    #endif
    closedir(dir);
    #ifdef DEBUG
    clock_gettime(CLOCK_MONOTONIC, &closedir_end);
    #endif

    // restore mtime and atime
    if (in.keep_matime) {
        utime(dbname, &dbtime);
    }

  out_free:
    ;
    /* #if defined(DEBUG */
    /* static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER; */
    /* pthread_mutex_lock(&mutex); */
    /* fprintf(stderr, "%zu ", id); */
    /* fprintf(stderr, "%s ", work->name); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&opendir_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&opendir_end) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&open_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&open_end) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&sqlite3_open_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&sqlite3_open_end) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&create_tables_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&create_tables_end) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&set_pragmas_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&set_pragmas_end) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&attach_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&attach_end) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&descend_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&descend_end) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&readdir_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&readdir_end) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&lstat_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&lstat_end) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&pushdir_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&pushdir_end) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&exec_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&exec_end) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&detach_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&detach_end) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&close_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&close_end) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&closedir_start) - epoch); */
    /* fprintf(stderr, "%" PRIu64 " ", timestamp(&closedir_end) - epoch); */
    /* fprintf(stderr, "\n"); */
    /* pthread_mutex_unlock(&mutex); */
    /* #endif */

    free(work);

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

    // process input args - all programs share the common 'struct input',
    // but allow different fields to be filled at the command-line.
    // Callers provide the options-string for get_opt(), which will
    // control which options are parsed for each program.
    int idx = parse_cmd_line(argc, argv, "hHT:S:E:Papn:o:d:O:I:F:y:z:G:J:e:m:", 1, "GUFI_tree ...", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;

    // load the pcre extension every time a database is opened
    const int rc = sqlite3_auto_extension((void (*)(void)) sqlite3_extension_init);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "sqlite3_auto_extension error: %d\n", rc);
        return -1;
    }

    if (!outfiles_init(gts.outfd,  in.outfile, in.outfilen, in.maxthreads)             ||
        !outdbs_init  (gts.outdbd, in.outdb,   in.outdbn,   in.maxthreads, in.sqlinit)) {
        return -1;
    }

    #if BENCHMARK
    fprintf(stderr, "Querying GUFI Index");
    for(int i = idx; i < argc; i++) {
        fprintf(stderr, " %s", argv[i]);
    }
    fprintf(stderr, " with %d threads\n", in.maxthreads);
    #endif

    sqlite3 *aggregate = NULL;
    sqlite3 **intermediates = NULL;
    char aggregate_name[MAXSQL] = {};
    if (in.aggregate_or_print == AGGREGATE) {
        // modify in.sqlent to insert the results into the aggregate table
        char orig_sqlent[MAXSQL];
        SNPRINTF(orig_sqlent, MAXSQL, "%s", in.sqlent);
        SNPRINTF(in.sqlent, MAXSQL, "INSERT INTO %s.entries %s", AGGREGATE_ATTACH_NAME, orig_sqlent);

        // create the aggregate database
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

    // provide a function to print if PRINT is set
    int (*print_callback_func)(void*,int,char**,char**) = (((in.aggregate_or_print == PRINT) && in.printdir)?print_callback:NULL);

    long double total_time = 0;

    #if defined(DEBUG) || BENCHMARK
    struct timespec intermediate_start;
    clock_gettime(CLOCK_MONOTONIC, &intermediate_start);
    #endif

    struct QPTPool * pool = QPTPool_init(in.maxthreads);
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        return -1;
    }

    // enqueue all input paths
    for(int i = idx; i < argc; i++) {
        struct work * mywork = calloc(1, sizeof(struct work));

        // check that the top level path is an accessible directory
        SNPRINTF(mywork->name,MAXPATH,"%s",argv[i]);
        lstat(mywork->name,&mywork->statuso);
        if (access(mywork->name, R_OK | X_OK)) {
            fprintf(stderr, "couldn't access input dir '%s': %s\n",
                    mywork->name, strerror(errno));
            free(mywork);
            continue;
        }
        if (!S_ISDIR(mywork->statuso.st_mode) ) {
            fprintf(stderr,"input-dir '%s' is not a directory\n", mywork->name);
            free(mywork);
            continue;
        }

        // push the path onto the queue
        QPTPool_enqueue(pool, i % in.maxthreads, mywork);
    }

    if (QPTPool_start(pool, processdir, print_callback_func) != (size_t) in.maxthreads) {
        fprintf(stderr, "Failed to start all threads\n");
        return -1;
    }

    QPTPool_wait(pool);
    const size_t thread_count = QPTPool_threads_started(pool);

    QPTPool_destroy(pool);

    #if defined(DEBUG) || BENCHMARK
    struct timespec intermediate_end;
    clock_gettime(CLOCK_MONOTONIC, &intermediate_end);
    total_time += elapsed(&intermediate_start, &intermediate_end);
    #endif

    /* #ifdef DEBUG */
    /* if (in.aggregate_or_print == PRINT) { */
    /*     const long double main_time = elapsed(&intermediate_start, &intermediate_end); */
    /*     fprintf(stderr, "Time to do main work:                           %.2Lfs\n", main_time); */
    /*     fprintf(stderr, "Time to open directories:                       %.2Lfs\n", total_opendir_time); */
    /*     fprintf(stderr, "Time to open databases:                         %.2Lfs\n", total_open_time); */
    /*     fprintf(stderr, "Time to create tables:                          %.2Lfs\n", total_create_tables_time); */
    /*     fprintf(stderr, "Time to load extensions:                        %.2Lfs\n", total_load_extension_time); */
    /*     fprintf(stderr, "Time to attach intermediate databases:          %.2Lfs\n", total_attach_time); */
    /*     fprintf(stderr, "Time to descend (w/o readdir + pushdir):        %.2Lfs\n", total_descend_time - total_readdir_time - total_pushdir_time); */
    /*     fprintf(stderr, "Time to readdir:                                %.2Lfs\n", total_readdir_time); */
    /*     fprintf(stderr, "Time to pushdir:                                %.2Lfs\n", total_pushdir_time); */
    /*     fprintf(stderr, "Time to sqlite3_exec (query and print)          %.2Lfs\n", total_exec_time); */
    /*     fprintf(stderr, "Time to detach intermediate databases:          %.2Lfs\n", total_detach_time); */
    /*     fprintf(stderr, "Time to close databases:                        %.2Lfs\n", total_close_time); */
    /*     fprintf(stderr, "Time to close directories:                      %.2Lfs\n", total_closedir_time); */
    /*     fprintf(stderr, "Queries performed:                              %zu\n",    thread_count); */
    /*     fprintf(stderr, "Real time:                                      %.2Lfs\n", main_time); */
    /* } */
    /* #endif */

    if (in.aggregate_or_print == AGGREGATE) {
        // prepend the intermediate database query with "INSERT INTO" to move
        // the data from the databases into the final aggregation database
        char intermediate[MAXSQL];
        sqlite3_snprintf(MAXSQL, intermediate, "INSERT INTO %s.entries %s", AGGREGATE_ATTACH_NAME, in.intermediate);

        #if defined(DEBUG) || BENCHMARK
        struct timespec aggregate_start;
        clock_gettime(CLOCK_MONOTONIC, &aggregate_start);
        #endif

        // aggregate the intermediate results
        for(int i = 0; i < in.maxthreads; i++) {
            if (!attachdb(aggregate_name, intermediates[i], AGGREGATE_ATTACH_NAME)            ||
                (sqlite3_exec(intermediates[i], intermediate, NULL, NULL, NULL) != SQLITE_OK)) {
                printf("Final aggregation error: %s\n", sqlite3_errmsg(intermediates[i]));
            }
         }

        #if defined(DEBUG) || BENCHMARK
        struct timespec aggregate_end;
        clock_gettime(CLOCK_MONOTONIC, &aggregate_end);
        total_time += elapsed(&aggregate_start, &aggregate_end);
        #endif

        // cleanup the intermediate databases outside of the timing (no need to detach)
        cleanup_intermediates(intermediates, in.maxthreads);

        #if defined(DEBUG) || BENCHMARK
        struct timespec output_start;
        clock_gettime(CLOCK_MONOTONIC, &output_start);
        #endif

        // run the aggregate query on the aggregated results
        size_t rows = 0;
        sqlite3_stmt *res = NULL;
        if (sqlite3_prepare_v2(aggregate, in.aggregate, MAXSQL, &res, NULL) == SQLITE_OK) {
            rows = print_results(res, stdout, 1, 0, in.printing, in.delim);
        }
        else {
            fprintf(stderr, "%s\n", sqlite3_errmsg(aggregate));
        }
        sqlite3_finalize(res);

        #if defined(DEBUG) || BENCHMARK
        struct timespec output_end;
        clock_gettime(CLOCK_MONOTONIC, &output_end);
        total_time += elapsed(&output_start, &output_end);
        #endif

        /* #ifdef DEBUG */
        /* const long double intermediate_time = elapsed(&intermediate_start, &intermediate_end); */
        /* const long double aggregate_time = elapsed(&aggregate_start, &aggregate_end); */
        /* const long double output_time = elapsed(&output_start, &output_end); */

        /* fprintf(stderr, "Time to do main work:                           %.2Lfs\n", intermediate_time); */
        /* fprintf(stderr, "Time to open directories:                       %.2Lfs\n", total_opendir_time); */
        /* fprintf(stderr, "Time to open databases:                         %.2Lfs\n", total_open_time); */
        /* fprintf(stderr, "Time to create tables:                          %.2Lfs\n", total_create_tables_time); */
        /* fprintf(stderr, "Time to load extensions:                        %.2Lfs\n", total_load_extension_time); */
        /* fprintf(stderr, "Time to attach intermediate databases:          %.2Lfs\n", total_attach_time); */
        /* fprintf(stderr, "Time to descend (w/o readdir + pushdir):        %.2Lfs\n", total_descend_time - total_readdir_time - total_pushdir_time); */
        /* fprintf(stderr, "Time to readdir:                                %.2Lfs\n", total_readdir_time); */
        /* fprintf(stderr, "Time to pushdir:                                %.2Lfs\n", total_pushdir_time); */
        /* fprintf(stderr, "Time to sqlite3_exec (query and insert)         %.2Lfs\n", total_exec_time); */
        /* fprintf(stderr, "Time to detach intermediate databases:          %.2Lfs\n", total_detach_time); */
        /* fprintf(stderr, "Time to close databases:                        %.2Lfs\n", total_close_time); */
        /* fprintf(stderr, "Time to close directories:                      %.2Lfs\n", total_closedir_time); */
        /* fprintf(stderr, "Time to aggregate into final databases:         %.2Lfs\n", aggregate_time); */
        /* fprintf(stderr, "Time to print:                                  %.2Lfs\n", output_time); */
        /* fprintf(stderr, "Rows returned:                                  %zu\n",    rows); */
        /* fprintf(stderr, "Queries performed:                              %d\n",     (int) (thread_count + in.maxthreads + 1)); */
        /* fprintf(stderr, "Real time:                                      %.2Lfs\n", total_time); */
        /* #endif */

        closedb(aggregate);
     }

     outdbs_fin  (gts.outdbd, in.maxthreads, in.sqlfin);
     outfiles_fin(gts.outfd,  in.maxthreads);

     #if BENCHMARK
     struct timespec end;
     clock_gettime(CLOCK_MONOTONIC, &end);

     fprintf(stderr, "Total Dirs:            %zu\n",    thread_count);
     fprintf(stderr, "Total Files:           %zu\n",    total_files);
     fprintf(stderr, "Time Spent Querying:   %.2Lfs\n", total_time);
     fprintf(stderr, "Dirs/Sec:              %.2Lf\n",  thread_count / total_time);
     fprintf(stderr, "Files/Sec:             %.2Lf\n",  total_files / total_time);
     #endif

     return 0;
}
