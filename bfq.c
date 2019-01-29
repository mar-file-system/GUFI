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
#ifdef DEBUG
#include <time.h>
#endif
#include <unistd.h>

#include <pwd.h>
#include <grp.h>
//#include <uuid/uuid.h>

#include "bf.h"
#include "structq.h"
#include "utils.h"
#include "dbutils.h"
// #include "putils.h"

#define AGGREGATE_NAME         "file:aggregate%d?mode=memory&cache=shared"
#define AGGREGATE_ATTACH_NAME  "aggregate"

struct aggregate_id_args {
    ShowResults_t aggregate_or_print;
    size_t id;
    size_t skip;
    size_t count;
};

// callback for setting qwork's aggregate_id
int set_aggregate_id(struct work *qwork, void *args) {
    if (!qwork || !args) {
        return -1;
    }

    struct aggregate_id_args *id_args = (struct aggregate_id_args *) args;
    if (id_args->aggregate_or_print == AGGREGATE) {
        id_args->id = (id_args->id + id_args->skip) % id_args->count;
    }

    qwork->aggregate_id = id_args->id;
    return 0;
}

// This becomes an argument to thpool_add_work(), so it must return void,
// instead of void*.
static void processdir(void * passv)
{
    struct work *passmywork = passv;
    struct work qwork;
    DIR *dir;
    struct dirent *entry;
    const int mytid = gettid();     // get thread id so we can get access to thread state we need to keep until the thread ends
    sqlite3_stmt *res;
    char dbpath[MAXPATH];
    sqlite3 *db;
    int recs;
    int trecs;
    char shortname[MAXPATH];
    char endname[MAXPATH];

    // open directory
    if (!(dir = opendir(passmywork->name)))
       goto out_free; // return NULL;

    sprintf(passmywork->type, "%s", "d");
    //if (in.printdir > 0) {
    //  printits(passmywork,mytid);
    //}

    // if we have out db then we have that db open so we just attach the gufi db
    if (in.outdb > 0) {
      db = gts.outdbd[mytid];
      char name[MAXSQL];
      snprintf(name, MAXSQL, "%s/%s", passmywork->name, DBNAME);
      attachdb(name, db, "tree");
    } else {
        db = opendb(passmywork->name, 1, 0);
    }

    if (in.aggregate_or_print == AGGREGATE) {
        // attach in-memory result aggregation database
        char intermediate_name[MAXSQL];
        snprintf(intermediate_name, MAXSQL, AGGREGATE_NAME, passmywork->aggregate_id);
        if (!attachdb(intermediate_name, db, AGGREGATE_ATTACH_NAME)) {
            closedb(db);
            return;
        }
    }

    // this is needed to add some query functions like path() uidtouser() gidtogroup()
    addqueryfuncs(db);

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
    }
    // this means that no tree table exists so assume we have to go on
    if (recs < 0) {
      recs=1;
    }
    // so we have to go on and query summary and entries possibly
    if (recs > 0) {
        struct aggregate_id_args aggregate_id_args;
        aggregate_id_args.aggregate_or_print = in.aggregate_or_print;
        aggregate_id_args.id = passmywork->aggregate_id;
        aggregate_id_args.skip = in.intermediate_skip;
        aggregate_id_args.count = in.intermediate_count;

        // push subdirectories into the queue
        descend(passmywork, dir, in.max_level,
                set_aggregate_id, &aggregate_id_args);

        // only query this level if the min_level has been reached
        if (passmywork->level >= in.min_level) {
            // run query on summary, print it if printing is needed, if returns none
            // and we are doing AND, skip querying the entries db
            // memset(endname, 0, sizeof(endname));
            shortpath(passmywork->name,shortname,endname);
            sprintf(gps[mytid].gepath,"%s",endname);
            if (strlen(in.sqlsum) > 1) {
                recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
                // for directories we have to take off after the last slash
                // and set the path so users can put path() in their queries
                sprintf(gps[mytid].gpath,"%s",shortname);
                //printf("processdir: setting gpath = %s and gepath %s\n",gps[mytid].gpath,gps[mytid].gepath);
                realpath(passmywork->name,gps[mytid].gfpath);
                recs = rawquerydb(passmywork->name, 1, db, in.sqlsum, 1, 0, in.printdir, mytid);
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
                    sprintf(gps[mytid].gpath,"%s",passmywork->name);
                    realpath(passmywork->name,gps[mytid].gfpath);
                    if (in.aggregate_or_print == AGGREGATE) {
                        char *err = NULL;
                        if (sqlite3_exec(db, in.sqlent, NULL, NULL, &err) != SQLITE_OK) {
                            fprintf(stderr, "Error: %s\n", err);
                        }

                        sqlite3_free(err);
                    }
                    else {
                        rawquerydb(passmywork->name, 0, db, in.sqlent, 1, 0, in.printdir, mytid);
                    }
                    //printf("entries ran %s on %s returned recs %d len of in.sqlent %lu\n",
                    //       in.sqlent,passmywork->name,recs,strlen(in.sqlent));
                }
            }
        }
    }

    if (in.aggregate_or_print == AGGREGATE) {
        // detach in-memory result aggregation database
        if (!detachdb(AGGREGATE_NAME, db, AGGREGATE_ATTACH_NAME)) {
            closedb(db);
            return;
        }
    }

    // if we have an out db we just detach gufi db
    if (in.outdb > 0) {
      char name[MAXSQL];
      snprintf(name, MAXSQL, "%s/%s", passmywork->name, DBNAME);
      detachdb(name, db, "tree");
    } else {
      closedb(db);
    }

 out_dir:
    // close dir
    closedir(dir);

 out_free:
    // free the queue entry - this has to be here or there will be a leak
    free(passmywork->freeme);

    // one less thread running
    decrthread();

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
         sprintf(outfn,"%s.%d",in.outfilen,i);
         gts.outfd[i]=fopen(outfn,"w");
         i++;
       }
     }

     if (in.outdb > 0) {
       i=0;
       while (i < in.maxthreads) {
         sprintf(outdbn,"%s.%d",in.outdbn,i);
         gts.outdbd[i]=opendb(outdbn,5,0);
         if (strlen(in.sqlinit) > 1) {
           if (in.aggregate_or_print == AGGREGATE) {
             char *err = NULL;
             if (sqlite3_exec(gts.outdbd[i], in.sqlent, NULL, NULL, &err) != SQLITE_OK) {
               fprintf(stderr, "Error: %s\n", err);
             }

             sqlite3_free(err);
           }
           else {
             rawquerydb(outdbn, 1, gts.outdbd[i], in.sqlinit, 1, 0, in.printdir, i);
           }
         }
         i++;
       }
     }

     //  ******  create and open output db's here

     // set the first mywork to be the root node
     mywork->level = 0;

     // process input directory and put it on the queue
     sprintf(mywork->name,"%s",in.name);
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
           if (in.aggregate_or_print == AGGREGATE) {
             char *err = NULL;
             if (sqlite3_exec(gts.outdbd[i], in.sqlent, NULL, NULL, &err) != SQLITE_OK) {
               fprintf(stderr, "Error: %s\n", err);
             }

             sqlite3_free(err);
           }
           else {
             rawquerydb("fin", 1, gts.outdbd[i], in.sqlfin, 1, 0, in.printdir, i);
           }
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

#ifdef DEBUG
long double elapsed(const struct timespec *start, const struct timespec *end) {
    const long double s = ((long double) start->tv_sec) + ((long double) start->tv_nsec) / 1000000000ULL;
    const long double e = ((long double) end->tv_sec)   + ((long double) end->tv_nsec)   / 1000000000ULL;
    return e - s;
}
#endif

int main(int argc, char *argv[])
{
     // process input args - all programs share the common 'struct input',
     // but allow different fields to be filled at the command-line.
     // Callers provide the options-string for get_opt(), which will
     // control which options are parsed for each program.
    int idx = parse_cmd_line(argc, argv, "hHT:S:E:Papn:o:d:O:I:F:y:z:v:w:G:J:e:", 1, "GUFI_tree ...", &in);
     if (in.helped)
        sub_help();
     if (idx < 0)
        return -1;

     sqlite3 *aggregate = NULL;
     sqlite3 **intermediates = NULL;
     char aggregate_name[MAXSQL] = {};
     if (in.aggregate_or_print == AGGREGATE) {
         // modify in.sqlent to insert the results into the aggregate table
         char orig_sqlent[MAXSQL];
         snprintf(orig_sqlent, MAXSQL, "%s", in.sqlent);
         snprintf(in.sqlent, MAXSQL, "INSERT INTO %s.entries %s", AGGREGATE_ATTACH_NAME, orig_sqlent);

         // create the aggregate database
         snprintf(aggregate_name, MAXSQL, AGGREGATE_NAME, -1);
         if (!(aggregate = open_aggregate(aggregate_name, AGGREGATE_ATTACH_NAME, orig_sqlent))) {
             return -1;
         }

         intermediates = malloc(sizeof(sqlite3 *) * in.intermediate_count);
         for(int i = 0; i < in.intermediate_count; i++) {
             char intermediate_name[MAXSQL];
             snprintf(intermediate_name, MAXSQL, AGGREGATE_NAME, i);
             if (!(intermediates[i] = open_aggregate(intermediate_name, AGGREGATE_ATTACH_NAME, orig_sqlent))) {
                 for(int j = 0; j < i; j++) {
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
        for(int i = 0; i < in.intermediate_count; i++) {
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
             mywork.aggregate_id = (aggregate_id + in.intermediate_skip) % in.intermediate_count;
         }
         processinit(&mywork);

         // processdirs - if done properly, this routine is common and does not
         // have to be done per instance of a bf program loops through and
         // processes all directories that enter the queue by farming the work
         // out to the threadpool
         thread_count += processdirs(processdir);

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

     if (in.aggregate_or_print == PRINT) {
         fprintf(stderr, "Time to query and print: %Les\n", elapsed(&intermediate_start, &intermediate_end));
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

         // aggregate the intermediate aggregations
         for(int i = 0; i < in.intermediate_count; i++) {
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
         for(int i = 0; i < in.intermediate_count; i++) {
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

         fprintf(stderr, "Rows returned:                                  %zu\n",    rows);
         fprintf(stderr, "Queries performed:                              %d\n",     thread_count + in.intermediate_count + 1);
         fprintf(stderr, "Time to aggregate into intermediate databases:  %Les\n", intermediate_time);
         fprintf(stderr, "Time to aggregate into final databases:         %Les\n", aggregate_time);
         fprintf(stderr, "Time to print:                                  %Les\n", output_time);
         fprintf(stderr, "Time to complete all:                           %Les\n", intermediate_time + aggregate_time + output_time);
#endif

         closedb(aggregate);
     }

     return 0;
}
