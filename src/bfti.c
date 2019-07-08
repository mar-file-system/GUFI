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

extern int errno;

// This becomes an argument to thpool_add_work(), so it must return void,
// instead of void*.
static void processdir(void * passv)
{
    struct work *passmywork = passv;
    DIR *dir;
    int mytid;
    sqlite3 *db;
    struct sum sumin;
    int recs;
    int trecs;

    // get thread id so we can get access to thread state we need to keep
    // until the thread ends
    mytid=0;
    if (in.outfile > 0) mytid=gettid();

    // open directory
    if (!(dir = opendir(passmywork->name)))
       goto out_free; // return NULL;

    SNPRINTF(passmywork->type,2,"%s","d");
    if (in.printing || in.printdir) {
      printits(passmywork,mytid);
    }

    // push subdirectories into the queue
    //descend(passmywork, dir, in.max_level);

    if ((db=opendb(passmywork->name,3,0))) {
       zeroit(&sumin);

       trecs=rawquerydb(passmywork->name, 0, db, "select name from sqlite_master where type=\'table\' and name=\'treesummary\';", 0, 0, 0, mytid);
       if (trecs<1) {
         // push subdirectories into the queue
         descend(passmywork, dir, in.max_level);
         querytsdb(passmywork->name,&sumin,db,&recs,0);
       } else {
         querytsdb(passmywork->name,&sumin,db,&recs,1);
         //printf("using treesummary %s\n",passmywork->name);
       }

       //querytsdb(passmywork->name,&sumin,db,&recs,0);
       tsumit(&sumin,&sumout);

       //printf("after tsumit %s dminuid %lld dmaxuid %lld minuid %lld maxuid %lld maxsize %lld totfiles %lld totsubdirs %lld\n",
       //       passmywork->name,sumin.minuid,sumin.maxuid,sumout.minuid,sumout.maxuid,sumout.maxsize,
       //       sumout.totfiles,sumout.totsubdirs);
       closedb(db);
    }

    // close dir
    closedir(dir);

 out_free:
    // one less thread running
    decrthread();

    // free the queue entry - this has to be here or there will be a leak
    free(passmywork->freeme);

    // return NULL;
}

int processinit(void * myworkin) {

     struct work * mywork = myworkin;

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

     pushdir(mywork);
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
        if (! (tdb = opendb(in.name,3,1)))
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
   printf("GUFI_tree         path to GUFI tree-dir\n");
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
     //char nameo[MAXPATH];
     struct work mywork;

     // process input args - all programs share the common 'struct input',
     // but allow different fields to be filled at the command-line.
     // Callers provide the options-string for get_opt(), which will
     // control which options are parsed for each program.
     int idx = parse_cmd_line(argc, argv, "hHPn:s", 1, "GUFI_tree", &in);
     if (in.helped)
        sub_help();
     if (idx < 0)
        return -1;
     else {
        // parse positional args, following the options
        int retval = 0;
        INSTALL_STR(in.name, argv[idx++], MAXPATH, "GUFI_tree");

        if (retval)
           return retval;
     }

     // option-parsing can't tell that some options are required,
     // or which combinations of options interact.
     if (validate_inputs())
        return -1;


     // start threads and loop watching threads needing work and queue size
     // - this always stays in main right here
     mythpool = thpool_init(in.maxthreads);
     if (thpool_null(mythpool)) {
        fprintf(stderr, "thpool_init() failed!\n");
        return -1;
     }

     // process initialization, this is work done once the threads are up
     // but not busy yet - this will be different for each instance of a bf
     // program in this case we are statting the directory passed in and
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

     // clean up threads and exit
     thpool_wait(mythpool);
     thpool_destroy(mythpool);
     return 0;
}
