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

#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <utime.h>
#include <sys/xattr.h>
#include <sqlite3.h>
#include <ctype.h>
#include <errno.h>
#include <pthread.h>

#include <pwd.h>
#include <grp.h>
//#include <uuid/uuid.h>

#include "bf.h"
#include "structq.h"
#include "utils.h"
#include "dbutils.h"
// #include "putils.h"

#define AGGREGATE_NAME         "file:aggregate?mode=memory&cache=shared"
#define AGGREGATE_ATTACH_NAME  "aggregate"

// This becomes an argument to thpool_add_work(), so it must return void,
// instead of void*.
static void processdir(void * passv)
{
    struct work *passmywork = passv;
    struct work qwork;
    DIR *dir;
    struct dirent *entry;
    int mytid;
    char *records;
    sqlite3_stmt *res;
    sqlite3_stmt *reso;
    char dbpath[MAXPATH];
    sqlite3 *db;
    sqlite3 *db1;
    int recs;
    char shortname[MAXPATH];
    char endname[MAXPATH];
    const size_t next_level = passmywork->level + 1;

    // get thread id so we can get access to thread state we need to keep until the thread ends
    mytid=0;
    if (in.outfile > 0) mytid=gettid();
    if (in.outdb > 0) mytid=gettid();

    // open directory
    if (!(dir = opendir(passmywork->name)))
       goto out_free; // return NULL;

    if (!(entry = readdir(dir)))
       goto out_dir; // return NULL;

    sprintf(passmywork->type,"%s","d");
    //if (in.printdir > 0) {
    //  printits(passmywork,mytid);
    //}

    // if we have out db then we have that db open so we just attach the gufi db
    if (in.outdb > 0) {
      db=gts.outdbd[mytid];
      attachdb(passmywork->name,db,"tree");
    } else {
      db=opendb(passmywork->name,db1,1,0);
    }

    // attach in-memory result aggregation database
    if (sqlite3_exec(db, "ATTACH '" AGGREGATE_NAME "' AS " AGGREGATE_ATTACH_NAME ";", 0, 0, NULL) != SQLITE_OK) {
        fprintf(stderr, "Cannot attach in memory database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return;
    }

    // this is needed to add some query functions like path() uidtouser() gidtogroup()
    addqueryfuncs(db);

    recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
             /* if it fails then this will be set to 1 and will go on */

    // if AND operation, and sqltsum is there, run a query to see if there is a match.
    // if this is OR, as well as no-sql-to-run, skip this query
    if (strlen(in.sqltsum) > 1) {

       if (in.andor == 0)       // AND
         recs=rawquerydb(passmywork->name, 0, db, in.sqltsum, 0, 0, 0, mytid);

      // this is an OR or we got a record back. go on to summary/entries
      // queries, if not done with this dir and all dirs below it
    }
    // this means that no tree table exists so assume we have to go on
    if (recs < 0) {
      recs=1;
    }
    // so we have to go on and query summary and entries possibly
    if (recs > 0) {
        // only push more levels in if needed
        if (next_level <= in.max_level) {
            // go ahead and send the subdirs to the queue since we need to look
            // further down the tree.  loop over dirents, if link push it on the
            // queue, if file or link print it, fill up qwork structure for
            // each
            do {
                const size_t len = strlen(entry->d_name);
                if (((len == 1) && (strncmp(entry->d_name, ".", 1) == 0))  ||
                    ((len == 2) && (strncmp(entry->d_name, "..", 2) == 0))) {
                    continue;
                }

                memset(&qwork, 0, sizeof(qwork));
                sprintf(qwork.name,"%s/%s", passmywork->name, entry->d_name);
                qwork.pinode=passmywork->statuso.st_ino;
                qwork.level = next_level;
                lstat(qwork.name, &qwork.statuso);
                if (S_ISDIR(qwork.statuso.st_mode)) {
                    if (!access(qwork.name, R_OK | X_OK)) {
                        // this is how the parent gets passed on
                        qwork.pinode=passmywork->statuso.st_ino;
                        // this pushes the dir onto queue - pushdir does locking around queue update
                        pushdir(&qwork);
                    }
                }
            } while ((entry = (readdir(dir))));
        }

        // only query this level if the min_level has been reached
        if (passmywork->level >= in.min_level) {
            // run query on summary, print it if printing is needed, if returns none
            // and we are doing AND, skip querying the entries db
            // bzero(endname,sizeof(endname));
            shortpath(passmywork->name,shortname,endname);
            sprintf(gps[mytid].gepath,"%s",endname);
            if (strlen(in.sqlsum) > 1) {
                recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
                // for directories we have to take off after the last slash
                // and set the path so users can put path() in their queries
                sprintf(gps[mytid].gpath,"%s",shortname);
                recs=rawquerydb(passmywork->name, 1, db, in.sqlsum, 1, 0, in.printdir, mytid);
                //printf("summary ran %s on %s returned recs %d\n",in.sqlsum,passmywork->name,recs);
            } else {
                recs=1;
            }
            if (in.andor > 0)
                recs=1;

            // if we have recs (or are running an OR) query the entries table
            if (recs > 0) {
                if (strlen(in.sqlent) > 1) {
                    // set the path so users can put path() in their queries
                    //printf("****entries len of in.sqlent %lu\n",strlen(in.sqlent));
                    sprintf(gps[mytid].gpath,"%s",passmywork->name);
                    rawquerydb(passmywork->name, 0, db, in.sqlent, 1, 0, in.printing, mytid);
                    //printf("entries ran %s on %s returned recs %d len of in.sqlent %lu\n",
                    //       in.sqlent,passmywork->name,recs,strlen(in.sqlent));
                }
            }
        }
    }

    // detach in-memory result aggregation database
    if (sqlite3_exec(db, "DETACH aggregate;", 0, 0, NULL) != SQLITE_OK) {
        fprintf(stderr, "Cannot attach in memory database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return;
    }

    // if we have an out db we just detach gufi db
    if (in.outdb > 0) {
      detachdb(passmywork->name,db,"tree");
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
     sqlite3 *dbo;

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
         gts.outdbd[i]=opendb(outdbn,dbo,5,0);
         if (strlen(in.sqlinit) > 1) {
           rawquerydb(outdbn, 1, gts.outdbd[i], in.sqlinit, 1, 0, in.printdir, i);
         }
         i++;
       }
     }


     //  ******  create and open output db's here

     // set the first mywork to be the root node
     mywork->level = 0;

     // process input directory and put it on the queue
     sprintf(mywork->name,"%s",in.name);
     lstat(in.name,&mywork->statuso);
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
           rawquerydb("fin", 1, gts.outdbd[i], in.sqlfin, 1, 0, in.printdir, i);
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

sqlite3 *open_aggregate(const char *query) {
    // find the first clause after FROM
    // more clauses are probably needed
    static const char *clauses[] = {"WHERE", "GROUP", "ORDER", "LIMIT", ";"};
    char *after_from = NULL;
    for(size_t i = 0; (i < sizeof(clauses) / sizeof(char *)) && !after_from; i++) {
        after_from = strstr(query, clauses[i]);
    }

    // create the aggregate table using the SELECT and FROM portions of the original query
    char create_results_table[MAXSQL];
    char af = 0;
    if (after_from) {
        af = *after_from;
        *after_from = 0;
    }

    // there is no need to modify the query, since the entries table is empty (but will generate a NULL row, which will be removed)
    snprintf(create_results_table, MAXSQL, "CREATE TABLE aggregate AS %s;", query);
    if (after_from) {
        *after_from = af;
    }

    sqlite3 *aggregate = NULL;
    char *err_msg = NULL;
    if ((sqlite3_open_v2(AGGREGATE_NAME, &aggregate, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) || // create the aggregate database
        (sqlite3_db_config(aggregate, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, NULL)                                    != SQLITE_OK) || // enable extension loading
        (sqlite3_load_extension(aggregate, "sqlite3-pcre/pcre.so", NULL, NULL)                                           != SQLITE_OK) || // load sqlite3-pcre
        (sqlite3_exec(aggregate, esql, NULL, NULL, &err_msg)                                                             != SQLITE_OK) || // create the original entries table for the aggregate table to copy from
        (sqlite3_exec(aggregate, create_results_table, NULL, NULL, &err_msg)                                             != SQLITE_OK) || // create the aggregate table
        (sqlite3_exec(aggregate, "DROP TABLE entries;", NULL, NULL, &err_msg)                                            != SQLITE_OK) || // drop the entries table
        (sqlite3_exec(aggregate, "ALTER TABLE aggregate RENAME TO entries;", NULL, NULL, &err_msg)                       != SQLITE_OK) || // rename the aggregate table to entries
        (sqlite3_exec(aggregate, "DELETE FROM entries;", NULL, NULL, &err_msg)                                           != SQLITE_OK)) { // delete all rows from the entries table, since there shouldn't be anything in the table at this point
        fprintf(stderr, "failed to create result aggregation database: %s\n", err_msg);
        sqlite3_close(aggregate);
        aggregate = NULL;
    }

    sqlite3_free(err_msg);

    return aggregate;
}

int main(int argc, char *argv[])
{
     // process input args - all programs share the common 'struct input',
     // but allow different fields to be filled at the command-line.
     // Callers provide the options-string for get_opt(), which will
     // control which options are parsed for each program.
     int idx = parse_cmd_line(argc, argv, "hHT:S:E:Papn:o:d:O:I:F:y:z:G:", 1, "GUFI_tree ...");
     if (in.helped)
        sub_help();
     if (idx < 0)
        return -1;

     // modify in.sqlent to insert the results into the aggregate table
     char orig_sqlent[MAXSQL];
     snprintf(orig_sqlent, MAXSQL, in.sqlent);
     snprintf(in.sqlent, MAXSQL, "INSERT INTO %s.entries %s", AGGREGATE_ATTACH_NAME, orig_sqlent);

     // create the aggregate database
     sqlite3 *aggregate = open_aggregate(orig_sqlent);
     if (!aggregate) {
         return -1;
     }

     // start threads and loop watching threads needing work and queue size
     // - this always stays in main right here
     mythpool = thpool_init(in.maxthreads);
     if (thpool_null(mythpool)) {
        fprintf(stderr, "thpool_init() failed!\n");
        sqlite3_close(aggregate);
        return -1;
     }

     for(; idx < argc; idx++) {
         // parse positional args, following the options
         int retval = 0;
         INSTALL_STR(in.name, argv[idx], MAXPATH, "GUFI_tree");

         if (retval) {
             sqlite3_close(aggregate);
             return retval;
         }

         // process initialization, this is work done once the threads are up
         // but not busy yet - this will be different for each instance of a bf
         // program in this case we are stating the directory passed in and
         // putting that directory on the queue
         struct work mywork;
         processinit(&mywork);

         // processdirs - if done properly, this routine is common and does not
         // have to be done per instance of a bf program loops through and
         // processes all directories that enter the queue by farming the work
         // out to the threadpool
         processdirs(processdir);

         // processfin - this is work done after the threads are done working
         // before they are taken down - this will be different for each
         // instance of a bf program
         processfin();
     }

     // wait for all threads to stop before processing the aggregate data
     thpool_wait(mythpool);
     thpool_destroy(mythpool);

     // run the original query on the aggregated results
     sqlite3_stmt *res = NULL;
     sqlite3_prepare_v2(aggregate, in.aggregate, MAXSQL, &res, NULL);
     print_results(res, stdout, 1, 0, in.printing, in.delim);
     sqlite3_finalize(res);

     sqlite3_close(aggregate);
     return 0;
}
