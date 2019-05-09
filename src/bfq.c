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

#include "bf.h"
#include "structq.h"
#include "utils.h"
#include "dbutils.h"
#include "pcre.h"

/* #ifdef DEBUG */
/* #undef DEBUG */
/* #endif */

#ifndef THREAD_STATS
#define THREAD_STATS
#endif

#ifdef DEBUG
#include <time.h>
long double elapsed(const struct timespec *start, const struct timespec *end) {
    const long double s = ((long double) start->tv_sec) + ((long double) start->tv_nsec) / 1000000000ULL;
    const long double e = ((long double) end->tv_sec)   + ((long double) end->tv_nsec)   / 1000000000ULL;
    return e - s;
}
#endif

extern int errno;
#define AGGREGATE_NAME         "file:aggregate%d?mode=memory&cache=shared"
#define AGGREGATE_ATTACH_NAME  "aggregate"

// callback for setting qwork's aggregate_id
int set_aggregate_id(struct work *qwork, void *args) {
    if (!qwork || !args) {
        return -1;
    }

    size_t * id = (size_t *) args;
    if (in.aggregate_or_print == AGGREGATE) {
        qwork->aggregate_id  = (*id + 1) % in.maxthreads;
    }

    return 0;
}

#ifdef DEBUG
long double total_opendir_time = 0;
long double total_open_time = 0;
long double total_create_tables_time = 0;
long double total_load_extension_time = 0;
long double total_attach_time = 0;
long double total_descend_time = 0;
long double total_pushdir_time = 0;
long double total_exec_time = 0;
long double total_detach_time = 0;
long double total_close_time = 0;
long double total_closedir_time = 0;
pthread_mutex_t total_mutex = PTHREAD_MUTEX_INITIALIZER;
#endif

// Push the subdirectories in the current directory onto the queue
static size_t descend2(struct work *passmywork,
                       DIR * dir,
                       const size_t max_level,
                       int (*callback)(struct work *, void *), void *args
                       #ifdef DEBUG
                       , long double *pushdir_time
                       #endif
    ) {

    if (!passmywork) {
        fprintf(stderr, "Got NULL work\n");
        return 0;
    }

    if (!dir) {
        fprintf(stderr, "Could not open %s: %d %s\n", passmywork->name, errno, strerror(errno));
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
        while ((entry = (readdir(dir)))) {
            const size_t len = strlen(entry->d_name);
            if (((len == 1) && (strncmp(entry->d_name, ".",  1) == 0)) ||
                ((len == 2) && (strncmp(entry->d_name, "..", 2) == 0))) {
                continue;
            }

            struct work qwork;
            memset(&qwork, 0, sizeof(qwork));
            SNPRINTF(qwork.name, MAXPATH, "%s/%s", passmywork->name, entry->d_name);

            lstat(qwork.name, &qwork.statuso);
            if (S_ISDIR(qwork.statuso.st_mode)) {
                if (!access(qwork.name, R_OK | X_OK)) {
                    qwork.level = next_level;
                    qwork.type[0] = 'd';

                    // this is how the parent gets passed on
                    qwork.pinode = passmywork->statuso.st_ino;

                    if (callback && (callback(&qwork, args) != 0)) {
                        continue;
                    }

                    // this pushes the dir onto queue - pushdir does locking around queue update
                    #ifdef DEBUG
                    struct timespec pushdir_start;
                    clock_gettime(CLOCK_MONOTONIC, &pushdir_start);
                    #endif
                    pushdir(&qwork);
                    #ifdef DEBUG
                    struct timespec pushdir_end;
                    clock_gettime(CLOCK_MONOTONIC, &pushdir_end);
                    *pushdir_time += elapsed(&pushdir_start, &pushdir_end);
                    #endif
                    pushed++;
                }
                /* else { */
                /*     fprintf(stderr, "couldn't access dir '%s': %s\n", */
                /*             qwork.name, strerror(errno)); */
                /* } */
            }
            /* else { */
            /*     fprintf(stderr, "not a dir '%s': %s\n", */
            /*             qwork.name, strerror(errno)); */
            /* } */
        }
    }

    return pushed;
}

sqlite3 * opendb2(const char *name, int openwhat, int createtables
                  #ifdef DEBUG
                       , long double * open_time, long double * create_tables_time, long double * load_extension_time
                  #endif
    )
{
    sqlite3 * db = NULL;
    char *err_msg = NULL;
    char dbn[MAXPATH];

    // sqlite3_snprintf(MAXSQL, dbn, "%s/%s/%s", in.nameto, name, DBNAME);

    sqlite3_snprintf(MAXSQL, dbn, "%s/" DBNAME, name);

    if (createtables) {
        if (openwhat != 3)
            sqlite3_snprintf(MAXSQL, dbn, "%s/%s/" DBNAME, in.nameto, name);
        if (openwhat==7 || openwhat==8)
            sqlite3_snprintf(MAXSQL, dbn, "%s", name);
    }
    else {
        if (openwhat == 6)
            sqlite3_snprintf(MAXSQL, dbn, "%s/%s/" DBNAME, in.nameto, name);
        if (openwhat == 5)
            sqlite3_snprintf(MAXSQL, dbn, "%s", name);
    }

    #ifdef DEBUG
    struct timespec open_start;
    clock_gettime(CLOCK_MONOTONIC, &open_start);
    #endif
    if (sqlite3_open_v2(dbn, &db, /*SQLITE_OPEN_CREATE | */SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, "unix-none") != SQLITE_OK) {
        /* fprintf(stderr, "Cannot open database: %s %s rc %d\n", dbn, sqlite3_errmsg(db), sqlite3_errcode(db)); */
        return NULL;
    }
    #ifdef DEBUG
    struct timespec open_end;
    clock_gettime(CLOCK_MONOTONIC, &open_end);
    *open_time = elapsed(&open_start, &open_end);
    #endif

    /* // try to turn sychronization off */
    /* if (sqlite3_exec(db, "PRAGMA synchronous = OFF", NULL, NULL, NULL) != SQLITE_OK) { */
    /* } */

    /* // try to turn journaling off */
    /* if (sqlite3_exec(db, "PRAGMA journal_mode = OFF", NULL, NULL, NULL) != SQLITE_OK) { */
    /* } */

    /* // try to get an exclusive lock */
    /* if (sqlite3_exec(db, "PRAGMA locking_mode = EXCLUSIVE", NULL, NULL, NULL) != SQLITE_OK) { */
    /* } */

    /* // try increasing the page size */
    /* if (sqlite3_exec(db, "PRAGMA page_size = 16777216", NULL, NULL, NULL) != SQLITE_OK) { */
    /* } */

    #ifdef DEBUG
    struct timespec create_tables_start;
    clock_gettime(CLOCK_MONOTONIC, &create_tables_start);
    #endif
    if (createtables) {
        if (create_tables(dbn, openwhat, db) != 0) {
            fprintf(stderr, "Cannot create tables: %s %s rc %d\n", dbn, sqlite3_errmsg(db), sqlite3_errcode(db));
            sqlite3_close(db);
            return NULL;
        }
    }
    #ifdef DEBUG
    struct timespec create_tables_end;
    clock_gettime(CLOCK_MONOTONIC, &create_tables_end);
    *create_tables_time = elapsed(&create_tables_start, &create_tables_end);
    #endif

    #ifdef DEBUG
    struct timespec load_extension_start;
    clock_gettime(CLOCK_MONOTONIC, &load_extension_start);
    #endif
    if ((sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, NULL) != SQLITE_OK) || // enable loading of extensions
        (sqlite3_extension_init(db, &err_msg, NULL)                            != SQLITE_OK)) { // load the sqlite3-pcre extension
        fprintf(stderr, "Unable to load regex extension\n");
        sqlite3_free(err_msg);
        sqlite3_close(db);
        db = NULL;
    }
    #ifdef DEBUG
    struct timespec load_extension_end;
    clock_gettime(CLOCK_MONOTONIC, &load_extension_end);
    *load_extension_time = elapsed(&load_extension_start, &load_extension_end);
    #endif

    return db;
}

// This becomes an argument to thpool_add_work(), so it must return void,
// instead of void*.
static void processdir(void * passv)
{
    struct work *passmywork = passv;
    const int mytid = gettid();     // get thread id so we can get access to thread state we need to keep until the thread ends
    sqlite3 *db;
    int recs;
    int trecs;
    char shortname[MAXPATH];
    char endname[MAXPATH];

    // only print if not aggregating and user wants to print
    // where to output to when printing
    FILE *out = stdout;
    if (in.outfile > 0) {
        out = gts.outfd[mytid];
    }

    char name[MAXSQL];
    SNPRINTF(name, MAXSQL, "%s/" DBNAME, passmywork->name);

    #ifdef DEBUG
    long double opendir_time = 0;
    long double open_time = 0;
    long double create_tables_time = 0;
    long double load_extension_time = 0;
    long double attach_time = 0;
    long double descend_time = 0;
    long double pushdir_time = 0;
    long double exec_time = 0;
    long double detach_time = 0;
    long double close_time = 0;
    long double closedir_time = 0;
    #endif

    // keep track of the mtime and atime
    struct utimbuf dbtime = {};
    if (in.keep_matime) {
        struct stat st;
        if (lstat(name, &st) != 0) {
            perror("stat");
            goto out_free;
        }

        dbtime.actime  = st.st_atime;
        dbtime.modtime = st.st_mtime;
    }

    // open directory
    #ifdef DEBUG
    struct timespec opendir_start;
    clock_gettime(CLOCK_MONOTONIC, &opendir_start);
    #endif
    // keep opendir near opendb to help speed up sqlite3_open_v2
    DIR * dir = opendir(passmywork->name);
    #ifdef DEBUG
    struct timespec opendir_end;
    clock_gettime(CLOCK_MONOTONIC, &opendir_end);
    opendir_time = elapsed(&opendir_start, &opendir_end);
    #endif

    // if we have out db then we have that db open so we just attach the gufi db
    if (in.outdb > 0) {
      db = gts.outdbd[mytid];
      attachdb(name, db, "tree");
    } else {
      db = opendb2(passmywork->name, 1, 0
                   #ifdef DEBUG
                   , &open_time, &create_tables_time, &load_extension_time
                   #endif
          );
    }

    #ifdef DEBUG
    struct timespec attach_start;
    clock_gettime(CLOCK_MONOTONIC, &attach_start);
    #endif
    if (in.aggregate_or_print == AGGREGATE) {
        // attach in-memory result aggregation database
        char intermediate_name[MAXSQL];
        SNPRINTF(intermediate_name, MAXSQL, AGGREGATE_NAME, (int) passmywork->aggregate_id);
        if (db && !attachdb(intermediate_name, db, AGGREGATE_ATTACH_NAME)) {
            fprintf(stderr, "Could not attach database\n");
            closedb(db);
            goto out_dir;
        }
    }
    #ifdef DEBUG
    struct timespec attach_end;
    clock_gettime(CLOCK_MONOTONIC, &attach_end);
    attach_time = elapsed(&attach_start, &attach_end);
    #endif

    // this is needed to add some query functions like path() uidtouser() gidtogroup()
    if (db) {
        addqueryfuncs(db);
    }

    recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
             /* if it fails then this will be set to 1 and will go on */

    // if AND operation, and sqltsum is there, run a query to see if there is a match.
    // if this is OR, as well as no-sql-to-run, skip this query
    if (strlen(in.sqltsum) > 1) {

       if (in.andor == 0) {      // AND
         trecs=rawquerydb(passmywork->name, 0, db, "select name from sqlite_master where type=\'table\' and name=\'treesummary\';", 0, 0, 0, mytid);
         if (trecs<1) {
           recs=-1;
         } else {
           recs=rawquerydb(passmywork->name, 0, db, in.sqltsum, 0, 0, 0, mytid);
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
        struct timespec descend_start;
        clock_gettime(CLOCK_MONOTONIC, &descend_start);
        #endif
        // push subdirectories into the queue
        descend2(passmywork, dir, in.max_level,
            set_aggregate_id, &passmywork->aggregate_id
            #ifdef DEBUG
            , &pushdir_time
            #endif
            );
        #ifdef DEBUG
        struct timespec descend_end;
        clock_gettime(CLOCK_MONOTONIC, &descend_end);
        descend_time = elapsed(&descend_start, &descend_end);
        #endif

        if (db) {
            // only query this level if the min_level has been reached
            if (passmywork->level >= in.min_level) {
                // run query on summary, print it if printing is needed, if returns none
                // and we are doing AND, skip querying the entries db
                // memset(endname, 0, sizeof(endname));
                shortpath(passmywork->name,shortname,endname);
                SNPRINTF(gps[mytid].gepath,MAXPATH,"%s",endname);
                if (strlen(in.sqlsum) > 1) {
                    recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
                    // for directories we have to take off after the last slash
                    // and set the path so users can put path() in their queries
                    SNPRINTF(gps[mytid].gpath,MAXPATH,"%s",shortname);
                    //printf("processdir: setting gpath = %s and gepath %s\n",gps[mytid].gpath,gps[mytid].gepath);
                    realpath(passmywork->name,gps[mytid].gfpath);
                    recs = rawquerydb(passmywork->name, 1, db, in.sqlsum, 1, 0, 0, mytid);
                    //printf("summary ran %s on %s returned recs %d\n",in.sqlsum,passmywork->name,recs);
                } else {
                    recs = 1;
                }
                if (in.andor > 0)
                    recs = 1;

                // if we have recs (or are running an OR) query the entries table
                if (recs > 0) {
                    if (strlen(in.sqlent) > 1) {
                        // set the path so users can put path() in their queries
                        //printf("****entries len of in.sqlent %lu\n",strlen(in.sqlent));
                        SNPRINTF(gps[mytid].gpath,MAXPATH,"%s",passmywork->name);
                        realpath(passmywork->name,gps[mytid].gfpath);

                        #ifdef DEBUG
                        struct timespec exec_start;
                        clock_gettime(CLOCK_MONOTONIC, &exec_start);
                        #endif
                        char *err = NULL;
                        if (sqlite3_exec(db, in.sqlent, in.print_callback, out, &err) != SQLITE_OK) {
                            fprintf(stderr, "Error: %s \"%s\"\n", err, in.sqlent);
                        }
                        #ifdef DEBUG
                        struct timespec exec_end;
                        clock_gettime(CLOCK_MONOTONIC, &exec_end);
                        exec_time = elapsed(&exec_start, &exec_end);
                        #endif

                        sqlite3_free(err);
                    }
                }
            }
        }
    }

    #ifdef DEBUG
    struct timespec detach_start;
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
    struct timespec detach_end;
    clock_gettime(CLOCK_MONOTONIC, &detach_end);
    detach_time = elapsed(&detach_start, &detach_end);
    #endif

    // if we have an out db we just detach gufi db
    #ifdef DEBUG
    struct timespec close_start;
    clock_gettime(CLOCK_MONOTONIC, &close_start);
    #endif
    if (in.outdb > 0) {
      detachdb(name, db, "tree");
    } else {
      closedb(db);
    }
    #ifdef DEBUG
    struct timespec close_end;
    clock_gettime(CLOCK_MONOTONIC, &close_end);
    close_time = elapsed(&close_start, &close_end);
    #endif

  out_dir:
    ;

    // close dir
    #ifdef DEBUG
    struct timespec closedir_start;
    clock_gettime(CLOCK_MONOTONIC, &closedir_start);
    #endif
    closedir(dir);
    #ifdef DEBUG
    struct timespec closedir_end;
    clock_gettime(CLOCK_MONOTONIC, &closedir_end);
    closedir_time = elapsed(&closedir_start, &closedir_end);
    #endif

    // restore mtime and atime
    if (in.keep_matime) {
        utime(name, &dbtime);
    }

  out_free:
    // one less thread running
    decrthread();

#ifdef DEBUG
    #ifdef THREAD_STATS
    fprintf(stderr, "%s %d %Lf %Lf %Lf %Lf %Lf %Lf %Lf %Lf %Lf %Lf %Lf\n", passmywork->name, !!db, opendir_time, open_time, create_tables_time, load_extension_time, attach_time, descend_time - pushdir_time, pushdir_time, exec_time, detach_time, close_time, closedir_time);
    #endif

    pthread_mutex_lock(&total_mutex);
    total_opendir_time           += opendir_time;
    total_open_time              += open_time;
    total_create_tables_time     += create_tables_time;
    total_load_extension_time    += load_extension_time;
    total_attach_time            += attach_time;
    total_descend_time           += descend_time;
    total_opendir_time           += opendir_time;
    total_pushdir_time           += pushdir_time;
    total_closedir_time          += closedir_time;
    total_exec_time              += exec_time;
    total_detach_time            += detach_time;
    total_close_time             += close_time;
    pthread_mutex_unlock(&total_mutex);
#endif

    // free the queue entry - this has to be here or there will be a leak
    free(passmywork->freeme);

    // return NULL;
}

int processinit(void * myworkin) {
     struct work * mywork = myworkin;
     int i;
     char outfn[MAXPATH];
     char outdbn[MAXPATH];

     //open up the output files if needed
     if (in.outfile > 0) {
       i=0;
       while (i < in.maxthreads) {
         SNPRINTF(outfn,MAXPATH,"%s.%d",in.outfilen,i);
         gts.outfd[i]=fopen(outfn,"w");
         i++;
       }
     }

     if (in.outdb > 0) {
       i=0;
       while (i < in.maxthreads) {
         SNPRINTF(outdbn,MAXPATH,"%s.%d",in.outdbn,i);
         gts.outdbd[i]=opendb(outdbn,5,0);
         if (strlen(in.sqlinit) > 1) {
           char *err = NULL;
           if (sqlite3_exec(gts.outdbd[i], in.sqlinit, NULL, NULL, &err) != SQLITE_OK) {
             fprintf(stderr, "Error: %s\n", err);
           }
           sqlite3_free(err);
         }
         i++;
       }
     }

     //  ******  create and open output db's here

     // set the first mywork to be the root node
     mywork->level = 0;

     // process input directory and put it on the queue
     SNPRINTF(mywork->name,MAXPATH,"%s",in.name);
     // this should be stat so we can make links to the top of the tree or an dir in the tree where we start our walking
     //lstat(in.name,&mywork->statuso);
     stat(in.name,&mywork->statuso);
     if (access(in.name, R_OK | X_OK)) {
        fprintf(stderr, "couldn't access input dir '%s': %s\n",
                in.name, strerror(errno));
        return 1;
     }
     if (!S_ISDIR(mywork->statuso.st_mode) ) {
        fprintf(stderr,"input-dir '%s' is not a directory\n", in.name);
        return 1;
     }

     pushdir(mywork);
     return 0;
}

int processfin() {
    int i;

     // close outputfiles
     if (in.outfile > 0) {
       i=0;
       while (i < in.maxthreads) {
         fclose(gts.outfd[i]);
         i++;
       }
     }

     // close output dbs here
     if (in.outdb > 0) {
       i=0;
       while (i < in.maxthreads) {
         closedb(gts.outdbd[i]);
         if (strlen(in.sqlfin) > 1) {
           char *err = NULL;
           if (sqlite3_exec(gts.outdbd[i], in.sqlfin, NULL, NULL, &err) != SQLITE_OK) {
             fprintf(stderr, "Error: %s\n", err);
           }
           sqlite3_free(err);
         }
         i++;
       }
     }

     return 0;
}

void sub_help() {
   printf("GUFI_tree         find GUFI index-tree here\n");
   printf("\n");
}

int main(int argc, char *argv[])
{
     // process input args - all programs share the common 'struct input',
     // but allow different fields to be filled at the command-line.
     // Callers provide the options-string for get_opt(), which will
     // control which options are parsed for each program.
    int idx = parse_cmd_line(argc, argv, "hHT:S:E:Papn:o:d:O:I:F:y:z:G:J:e:m:", 1, "GUFI_tree ...", &in);
     if (in.helped)
        sub_help();
     if (idx < 0)
        return -1;

     const int rc = sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
     if (rc != SQLITE_OK) {
         fprintf(stderr, "sqlite3_config error: %d\n", rc);
         return -1;
     }

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

         intermediates = malloc(sizeof(sqlite3 *) * in.maxthreads);
         for(size_t i = 0; i < in.maxthreads; i++) {
             char intermediate_name[MAXSQL];
             SNPRINTF(intermediate_name, MAXSQL, AGGREGATE_NAME, (int) i);
             if (!(intermediates[i] = open_aggregate(intermediate_name, AGGREGATE_ATTACH_NAME, orig_sqlent))) {
                 fprintf(stderr, "Could not open %s\n", intermediate_name);
                 for(size_t j = 0; j < i; j++) {
                     closedb(intermediates[j]);
                 }
                 free(intermediates);
                 closedb(aggregate);
                 return -1;
             }
         }
     }

     // start threads and loop watching threads needing work and queue size
     // - this always stays in main right here
     mythpool = thpool_init(in.maxthreads);
     if (thpool_null(mythpool)) {
        fprintf(stderr, "thpool_init() failed!\n");
        for(size_t i = 0; i < in.maxthreads; i++) {
            closedb(intermediates[i]);
        }
        free(intermediates);
        closedb(aggregate);
        return -1;
     }

#ifdef DEBUG
     struct timespec intermediate_start;
     clock_gettime(CLOCK_MONOTONIC, &intermediate_start);
#endif

     int aggregate_id = 0;
     int thread_count = 0;

     long double acquire_mutex_time = 0;
     long double work_time = 0;

     for(; idx < argc; idx++) {
         // parse positional args, following the options
         int retval = 0;
         INSTALL_STR(in.name, argv[idx], MAXPATH, "GUFI_tree");

         if (retval) {
             break;
         }

         // process initialization, this is work done once the threads are up
         // but not busy yet - this will be different for each instance of a bf
         // program in this case we are stating the directory passed in and
         // putting that directory on the queue
         struct work mywork;
         if (in.aggregate_or_print == AGGREGATE) {
             mywork.aggregate_id = aggregate_id++ % in.maxthreads;
         }
         processinit(&mywork);

         // processdirs - if done properly, this routine is common and does not
         // have to be done per instance of a bf program loops through and
         // processes all directories that enter the queue by farming the work
         // out to the threadpool
         thread_count += processdirs2(processdir, &acquire_mutex_time, &work_time);

         // processfin - this is work done after the threads are done working
         // before they are taken down - this will be different for each
         // instance of a bf program
         processfin();
     }

     // wait for all threads to stop before processing the aggregate data
     thpool_wait(mythpool);

#ifdef DEBUG
     struct timespec intermediate_end;
     clock_gettime(CLOCK_MONOTONIC, &intermediate_end);

     fprintf(stderr, "Time to acquire queue lock for popping work:    %.2Lf\n", acquire_mutex_time);
     fprintf(stderr, "Time to do processdir work:                     %.2Lf\n", work_time);

     if (in.aggregate_or_print == PRINT) {
         fprintf(stderr, "Time to open directories:                       %.2Lfs\n", total_opendir_time);
         fprintf(stderr, "Time to open databases:                         %.2Lfs\n", total_open_time);
         fprintf(stderr, "Time to attach intermediate databases:          %.2Lfs\n", total_attach_time);
         fprintf(stderr, "Time to descend:                                %.2Lfs\n", total_descend_time - total_pushdir_time);
         fprintf(stderr, "Time to pushdir:                                %.2Lfs\n", total_pushdir_time);
         fprintf(stderr, "Time to sqlite3_exec (query and print):         %.2Lfs\n", total_exec_time);
         fprintf(stderr, "Time to detach intermediate databases:          %.2Lfs\n", total_detach_time);
         fprintf(stderr, "Time to close databases:                        %.2Lfs\n", total_close_time);
         fprintf(stderr, "Time to close directories:                      %.2Lfs\n", total_closedir_time);
         fprintf(stderr, "Queries performed:                              %d\n",     thread_count);
         fprintf(stderr, "Real time:                                      %.2Lfs\n", elapsed(&intermediate_start, &intermediate_end));
     }
#endif

     thpool_destroy(mythpool);

     if (in.aggregate_or_print == AGGREGATE) {
         // prepend the intermediate database query with "INSERT INTO" to move
         // the data from the databases into the final aggregation database
         char intermediate[MAXSQL];
         sqlite3_snprintf(MAXSQL, intermediate, "INSERT INTO %s.entries %s", AGGREGATE_ATTACH_NAME, in.intermediate);

#ifdef DEBUG
         struct timespec aggregate_start;
         clock_gettime(CLOCK_MONOTONIC, &aggregate_start);
#endif

         // aggregate the intermediate results
         for(size_t i = 0; i < in.maxthreads; i++) {
             if (!attachdb(aggregate_name, intermediates[i], AGGREGATE_ATTACH_NAME)            ||
                 (sqlite3_exec(intermediates[i], intermediate, NULL, NULL, NULL) != SQLITE_OK)) {
                 printf("Final aggregation error: %s\n", sqlite3_errmsg(intermediates[i]));
             }
         }

#ifdef DEBUG
         struct timespec aggregate_end;
         clock_gettime(CLOCK_MONOTONIC, &aggregate_end);
#endif

         // cleanup the intermediate databases outside of the timing (no need to detach)
         for(size_t i = 0; i < in.maxthreads; i++) {
             sqlite3_close(intermediates[i]);
         }
         free(intermediates);

#ifdef DEBUG
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

#ifdef DEBUG
         struct timespec output_end;
         clock_gettime(CLOCK_MONOTONIC, &output_end);

         const long double intermediate_time = elapsed(&intermediate_start, &intermediate_end);
         const long double aggregate_time = elapsed(&aggregate_start, &aggregate_end);
         const long double output_time = elapsed(&output_start, &output_end);

         fprintf(stderr, "Time to do main work:                           %.2Lfs\n", intermediate_time);
         fprintf(stderr, "Time to open directories:                       %.2Lfs\n", total_opendir_time);
         fprintf(stderr, "Time to open databases:                         %.2Lfs\n", total_open_time);
         fprintf(stderr, "Time to create tables:                          %.2Lfs\n", total_create_tables_time);
         fprintf(stderr, "Time to load extensions:                        %.2Lfs\n", total_load_extension_time);
         fprintf(stderr, "Time to attach intermediate databases:          %.2Lfs\n", total_attach_time);
         fprintf(stderr, "Time to descend:                                %.2Lfs\n", total_descend_time - total_pushdir_time);
         fprintf(stderr, "Time to pushdir:                                %.2Lfs\n", total_pushdir_time);
         fprintf(stderr, "Time to sqlite3_exec (query and insert)         %.2Lfs\n", total_exec_time);
         fprintf(stderr, "Time to detach intermediate databases:          %.2Lfs\n", total_detach_time);
         fprintf(stderr, "Time to close databases:                        %.2Lfs\n", total_close_time);
         fprintf(stderr, "Time to close directories:                      %.2Lfs\n", total_closedir_time);
         fprintf(stderr, "Time to aggregate into final databases:         %.2Lfs\n", aggregate_time);
         fprintf(stderr, "Time to print:                                  %.2Lfs\n", output_time);
         fprintf(stderr, "Rows returned:                                  %zu\n",    rows);
         fprintf(stderr, "Queries performed:                              %d\n",     (int) (thread_count + in.maxthreads + 1));
         fprintf(stderr, "Real time:                                      %.2Lfs\n", intermediate_time + aggregate_time + output_time);
#endif

         closedb(aggregate);
     }

     return 0;
}
