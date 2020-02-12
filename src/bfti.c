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



#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <grp.h>
#include <pthread.h>
#include <pwd.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <unistd.h>
#include <utime.h>

#include <pwd.h>
#include <grp.h>

#include "bf.h"
#include "utils.h"
#include "dbutils.h"
#include "QueuePerThreadPool.h"

extern int errno;

static int create_tables(const char *name, sqlite3 *db, void * args) {
     printf("writetsum %d\n", in.writetsum);
    if ((create_table_wrapper(name, db, "tsql",        tsql,        NULL, NULL) != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vtssqldir",   vtssqldir,   NULL, NULL) != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vtssqluser",  vtssqluser,  NULL, NULL) != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vtssqlgroup", vtssqlgroup, NULL, NULL) != SQLITE_OK)) {
        return -1;
    }

    return 0;
}

static int processdir(struct QPTPool * ctx, const size_t id, void * data, void * args)
{
    struct work *passmywork = data;
    char dbname[MAXPATH];
    DIR *dir;
    sqlite3 *db;
    struct sum sumin;
    int recs;
    int trecs;

    (void) args;

    if (!(dir = opendir(passmywork->name)))
       goto out_free;

    SNPRINTF(passmywork->type,2,"%s","d");
    if (in.printing || in.printdir) {
      printits(passmywork,id);
    }

    // push subdirectories into the queue
    //descend(passmywork, dir, in.max_level);

    SNPRINTF(dbname, MAXPATH, "%s/%s", passmywork->name, DBNAME);
    if ((db=opendb(dbname, RDONLY, 1, 1,
                   NULL, NULL
                   #ifdef DEBUG
                   , NULL, NULL
                   , NULL, NULL
                   , NULL, NULL
                   , NULL, NULL
                   #endif
                   ))) {
       zeroit(&sumin);

       trecs=rawquerydb(passmywork->name, 0, db, "select name from sqlite_master where type=\'table\' and name=\'treesummary\';", 0, 0, 0, id);
       if (trecs<1) {
         // push subdirectories into the queue
         descend(ctx, id, passmywork, dir, processdir, in.max_level);
         querytsdb(passmywork->name,&sumin,db,&recs,0);
       } else {
         querytsdb(passmywork->name,&sumin,db,&recs,1);
       }

       tsumit(&sumin,&sumout);
    }

    closedb(db);
    closedir(dir);

 out_free:
    free(passmywork);

    return 0;
}

int processinit(struct QPTPool * ctx) {

     struct work * mywork = malloc(sizeof(struct work));

     // process input directory and put it on the queue
     SNPRINTF(mywork->name,MAXPATH,"%s",in.name);
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

     /* push the path onto the queue */
     QPTPool_enqueue(ctx, 0, processdir, mywork);

     return 0;
}

int processfin() {

     sqlite3 *tdb;
     struct stat smt;
     int rc;
     char dbpath[MAXPATH];
     struct utimbuf utimeStruct;

     SNPRINTF(dbpath,MAXPATH,"%s/%s",in.name,DBNAME);
     rc=1;
     rc=lstat(dbpath,&smt);
     if (in.writetsum) {
        if (! (tdb = opendb(dbpath, RDWR, 1, 1,
                            create_tables, NULL
                            #ifdef DEBUG
                            , NULL, NULL
                            , NULL, NULL
                            , NULL, NULL
                            , NULL, NULL
                            #endif
                            )))
           return -1;
        inserttreesumdb(in.name,tdb,&sumout,0,0,0);
        closedb(tdb);
     }
     if (rc==0) {
        utimeStruct.actime  = smt.st_atime;
        utimeStruct.modtime = smt.st_mtime;
        if(utime(dbpath, &utimeStruct) != 0) {
           fprintf(stderr,"ERROR: utime failed with error number: %d on %s\n", errno,dbpath);
           exit(1);
        }
     }

     printf("totals: \n");
     printf("totfiles %lld totlinks %lld\n",sumout.totfiles,sumout.totlinks);
     printf("totsize %lld\n",sumout.totsize);
     printf("minuid %lld maxuid %lld mingid %lld maxgid %lld\n",sumout.minuid,sumout.maxuid,sumout.mingid,sumout.maxgid);
     printf("minsize %lld maxsize %lld\n",sumout.minsize,sumout.maxsize);
     printf("totltk %lld totmtk %lld totltm %lld totmtm %lld totmtg %lld totmtt %lld\n",sumout.totltk,sumout.totmtk,sumout.totltm,sumout.totmtm,sumout.totmtg,sumout.totmtt);
     printf("minctime %lld maxctime %lld\n",sumout.minctime,sumout.maxctime);
     printf("minmtime %lld maxmtime %lld\n",sumout.minmtime,sumout.maxmtime);
     printf("minatime %lld maxatime %lld\n",sumout.minatime,sumout.maxatime);
     printf("minblocks %lld maxblocks %lld\n",sumout.minblocks,sumout.maxblocks);
     printf("totxattr %lld\n",sumout.totxattr);
     printf("mincrtime %lld maxcrtime %lld\n",sumout.mincrtime,sumout.maxcrtime);
     printf("minossint1 %lld maxossint1 %lld totossint1 %lld\n",sumout.minossint1,sumout.maxossint1,sumout.totossint1);
     printf("minossint2 %lld maxossint2 %lld totossint2 %lld\n",sumout.minossint2,sumout.maxossint2,sumout.totossint2);
     printf("minossint3 %lld maxossint3 %lld totossint3 %lld\n",sumout.minossint3,sumout.maxossint3,sumout.totossint3);
     printf("minossint4 %lld maxossint4 %lld totossint4 %lld\n",sumout.minossint4,sumout.maxossint4,sumout.totossint4);
     printf("totsubdirs %lld maxsubdirfiles %lld maxsubdirlinks %lld maxsubdirsize %lld\n",sumout.totsubdirs,sumout.maxsubdirfiles,sumout.maxsubdirlinks,sumout.maxsubdirsize);

     return 0;

}



void sub_help() {
   printf("GUFI_index        path to GUFI index\n");
   printf("\n");
}

int validate_inputs() {
   // not an error, but you might want to know ...
   if (! in.writetsum)
      fprintf(stderr, "WARNING: Not [re]generating tree-summary table without '-s'\n");

   return 0;
}


int main(int argc, char *argv[])
{
     // process input args - all programs share the common 'struct input',
     // but allow different fields to be filled at the command-line.
     // Callers provide the options-string for get_opt(), which will
     // control which options are parsed for each program.
     int idx = parse_cmd_line(argc, argv, "hHPn:s", 1, "GUFI_index", &in);
     if (in.helped)
        sub_help();
     if (idx < 0)
        return -1;
     else {
        // parse positional args, following the options
        int retval = 0;
        INSTALL_STR(in.name, argv[idx++], MAXPATH, "GUFI_index");

        if (retval)
           return retval;
     }

     // option-parsing can't tell that some options are required,
     // or which combinations of options interact.
     if (validate_inputs())
        return -1;

     struct QPTPool * pool = QPTPool_init(in.maxthreads);
     if (!pool) {
         fprintf(stderr, "Failed to initialize thread pool\n");
         return -1;
     }

     if (QPTPool_start(pool, NULL) != (size_t) in.maxthreads) {
         fprintf(stderr, "Failed to start threads\n");
         return -1;
     }

     processinit(pool);

     QPTPool_wait(pool);

     QPTPool_destroy(pool);

     processfin();

     return 0;
}
