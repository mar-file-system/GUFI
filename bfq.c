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

    /* // attach in-memory result aggregation database */
    /* { */
    /*     static const char sqlstmt[MAXSQL] = "ATTACH 'file:aggregate?mode=memory&cache=shared' AS aggregate;"; */
    /*     if (sqlite3_exec(db, sqlstmt,0, 0, NULL) != SQLITE_OK) { */
    /*         fprintf(stderr, "Cannot attach in memory database: %s\n", sqlite3_errmsg(db)); */
    /*         sqlite3_close(db); */
    /*         return; */
    /*     } */
    /* } */

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
                if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                    continue;
                bzero(&qwork,sizeof(qwork));
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

    /* // detach in-memory result aggregation database */
    /* { */
    /*     static const char sqlstmt[MAXSQL] = "DETACH aggregate;"; */
    /*     if (sqlite3_exec(db, sqlstmt,0, 0, NULL) != SQLITE_OK) { */
    /*         fprintf(stderr, "Cannot attach in memory database: %s\n", sqlite3_errmsg(db)); */
    /*         sqlite3_close(db); */
    /*         return; */
    /*     } */
    /* } */

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

int main(int argc, char *argv[])
{
     struct work mywork;

     // process input args - all programs share the common 'struct input',
     // but allow different fields to be filled at the command-line.
     // Callers provide the options-string for get_opt(), which will
     // control which options are parsed for each program.
     int idx = parse_cmd_line(argc, argv, "hHT:S:E:Papn:o:d:O:I:F:y:z:", 1, "GUFI_tree ...");
     if (in.helped)
        sub_help();
     if (idx < 0)
        return -1;

     size_t len = strlen(in.sqlent) + 1;
     char orig_sqlent[MAXSQL];
     memcpy(orig_sqlent, in.sqlent, len);

     {
         // find FROM clause
         char *from = strstr(orig_sqlent, "FROM");
         if (!from) {
             printf("Could not locate FROM clause\n");
             return -1;
         }
         snprintf(in.sqlent, len, "SELECT * %s", from);

         const char *clauses[] = {"WHERE", "GROUP", "ORDER", "LIMIT", ";"};
         char *after_from = NULL;
         for(size_t i = 0; (i < sizeof(clauses)) && !after_from; i++) {
             after_from = strstr(orig_sqlent, clauses[i]);
         }
         if (!after_from) {
             printf("No clauses found after from\n");
             return -1;
         }

         *from = 0;

         {
             char buf1[MAXSQL];
             snprintf(buf1, MAXSQL, "%s", orig_sqlent);
             char buf2[MAXSQL];
             snprintf(buf2, MAXSQL, "%s", after_from);

             snprintf(orig_sqlent, MAXSQL, "%s FROM aggregate %s", buf1, buf2);
         }
     }

     // start threads and loop watching threads needing work and queue size
     // - this always stays in main right here
     mythpool = thpool_init(in.maxthreads);
     if (thpool_null(mythpool)) {
        fprintf(stderr, "thpool_init() failed!\n");
        return -1;
     }

     sqlite3 *aggregate = NULL;
     if (sqlite3_open_v2("file:aggregate?mode=memory&cache=shared", &aggregate, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
         fprintf(stderr, "failed to open result aggregation database");
         return -1;
     }

     char *err_msg = NULL;
     if (sqlite3_exec(aggregate, "DROP TABLE IF EXISTS aggregate; CREATE TABLE aggregate(name TEXT PRIMARY KEY, type TEXT, inode INT64, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattrs TEXT, crtime INT64, ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, osstext1 TEXT, osstext2 TEXT);", NULL, NULL, &err_msg)) {
         fprintf(stderr, "failed to create result aggregation table: %s\n", err_msg);
         sqlite3_free(err_msg);
         sqlite3_close(aggregate);

         // clean up threads and exit
         thpool_wait(mythpool);
         thpool_destroy(mythpool);
         return -1;
     }

     sqlite3_free(err_msg);

     for(; idx < argc; idx++) {
         // parse positional args, following the options
         int retval = 0;
         INSTALL_STR(in.name, argv[idx], MAXPATH, "GUFI_tree");

         if (retval)
             return retval;

         // process initialization, this is work done once the threads are up
         // but not busy yet - this will be different for each instance of a bf
         // program in this case we are stating the directory passed in and
         // putting that directory on the queue
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

     int printpath = 1;
     int printheader = 0;
     int printrows = in.printing;

     sqlite3_stmt *res;
     int error = sqlite3_prepare_v2(aggregate, orig_sqlent, MAXSQL, &res, NULL);

     int rec_count = 0;
     int onetime = 0;
     while (sqlite3_step(res) == SQLITE_ROW) {
         /* int ncols = sqlite3_column_count(res); */

         /* for(int i = 0; i < ncols; i++) { */
         /*     fprintf(stdout,"%s%s", sqlite3_column_text(res, i),in.delim); */
         /* } */
         /* fprintf(stdstdout,"\n"); */

         //printf("looping through rec_count %ds\n",rec_count);

         // find the column whose name is "name"
         int name_col = -1; // default to -1 if not found
         int ncols=sqlite3_column_count(res);
         for(int cnt = 0; cnt < ncols; cnt++) {
             const char *col_name = sqlite3_column_name(res, cnt);
             if ((strlen(col_name) == 4) &&
                 (strncmp(col_name, "name", 4) == 0)) {
                 name_col = cnt;
                 break;
             }
         }

        // maybe print column-names as a header (once)
        if (printheader) {
           if (onetime == 0) {
              int cnt=0;
              while (ncols > 0) {
                 if (cnt==0) {
                    //if (printpath) fprintf(stdout,"path/%s",in.delim);
                 }
                 fprintf(stdout,"%s%s", sqlite3_column_name(res,cnt),in.delim);
                 //fprintf(stdout,"%s%s", sqlite3_column_decltype(res,cnt),in.delim);
                 ncols--;
                 cnt++;
              }
              fprintf(stdout,"\n");
              onetime++;
           }
        }

        // maybe print result-row values
        if (printrows) {
           //if (printpath) printf("%s/", name);
           int cnt=0;
           while (ncols > 0) {
              if (cnt==0) {
                 //if (printpath) fprintf(stdout,"%s/%s",shortname,in.delim);
              }
              if (cnt == name_col) {
                  fprintf(stdout,"%s%s", sqlite3_column_text(res,cnt),in.delim);
              }
              else {
                  fprintf(stdout,"%s%s", sqlite3_column_text(res,cnt),in.delim);
              }
              ncols--;
              cnt++;
           }
           fprintf(stdout,"\n");
        }

        // count of rows in query-result
        rec_count++;

     }

     sqlite3_close(aggregate);

     // clean up threads and exit
     thpool_wait(mythpool);
     thpool_destroy(mythpool);
     return 0;
}
