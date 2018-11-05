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


// This becomes an argument to thpool_add_work(), so it must return void,
// instead of void*.
static void processdir(void * passv)
{
    struct work *passmywork = passv;
    struct work qwork;
    DIR *dir;
    struct dirent *entry;
    char lpatho[MAXPATH];
    int mytid;
    sqlite3 *db;
    sqlite3 *db1;
    char *records; 
    struct sum summary;
    sqlite3_stmt *res;   
    sqlite3_stmt *reso;   
    char dbpath[MAXPATH];
    int transcnt;

    // get thread id so we can get access to thread state we need to keep
    // until the thread ends
    //mytid=0;
    mytid=gettid();

    // open directory
    if (!(dir = opendir(passmywork->name)))
       goto out_free; // return NULL;

    if (!(entry = readdir(dir)))
       goto out_dir; // return NULL;

    sprintf(passmywork->type,"%s","d");
    if (in.printing > 0 || in.printdir > 0) {
      printits(passmywork,mytid);
    }

    if (in.outdb > 0) {
       res=insertdbprepr(gts.outdbd[0],reso);
       startdb(gts.outdbd[0]);
       if (in.insertdir > 0) {
         bzero(&qwork,sizeof(qwork));
         sprintf(qwork.name,"%s", passmywork->name);
         sprintf(qwork.type,"d");
         qwork.statuso.st_ino=passmywork->statuso.st_ino;
         qwork.pinode=passmywork->pinode;
         insertdbgor(&qwork,gts.outdbd[0],res);
       }
    }

    // loop over dirents, if link push it on the queue, if file or link
    // print it, fill up qwork structure for each
    do {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
           continue;
        bzero(&qwork,sizeof(qwork));
        sprintf(qwork.name,"%s/%s", passmywork->name, entry->d_name);
        qwork.pinode=passmywork->statuso.st_ino;
        qwork.statuso.st_ino=entry->d_ino;
        //if (S_ISDIR(qwork.statuso.st_mode) ) {
        if (entry->d_type==DT_DIR) {
            if (!access(qwork.name, R_OK | X_OK)) {
                sprintf(qwork.type,"d");
                // this pushes the dir onto queue - pushdir does locking around queue update
                pushdir(&qwork);
            }
        //} else if (S_ISLNK(qwork.statuso.st_mode) {
        } else if (entry->d_type==DT_LNK) {
            sprintf(qwork.type,"%s","l");
            if (in.printing > 0) {
              printits(&qwork,mytid);
            }
            if (in.outdb > 0) {
              if (in.insertfl > 0) {
                insertdbgor(&qwork,gts.outdbd[0],res);
              }
            }
        //} else if (S_ISREG(qwork.statuso.st_mode) ) {
        } else if (entry->d_type==DT_REG) {
            sprintf(qwork.type,"%s","f");
            if (in.printing > 0) {
              printits(&qwork,mytid);
            }
            if (in.outdb > 0) {
              if (in.insertfl > 0) {
                insertdbgor(&qwork,gts.outdbd[0],res);
              }
            }
        }
    } while ((entry = (readdir(dir))));

    if (in.outdb > 0) {
      stopdb(gts.outdbd[0]);
      insertdbfin(gts.outdbd[0],res);

      // this i believe has to be after we close off the entries transaction 
      //closedb(db);  dont do this here to it in main at the end since its a shared db

    }

 out_dir:
    // close dir
    closedir(dir);

 out_free:
    // free the queue entry - this has to be here or there will be a leak
    free(passmywork->freeme);

    // one less thread running
    decrthread();

    /// return NULL;
}

int processinit(void * myworkin) {
    
     struct work * mywork = myworkin;
     int i;

     //open up the output files if needed

     // process input directory and put it on the queue
     sprintf(mywork->name,"%s",in.name);
     lstat(in.name, &mywork->statuso);
     if (access(in.name, R_OK | X_OK)) {
        fprintf(stderr, "couldn't access input dir '%s': %s\n",
                in.name, strerror(errno));
         return 1;
     }
     if (!S_ISDIR(mywork->statuso.st_mode) ) {
        fprintf(stderr,"input-dir '%s' is not a directory\n", in.name);
        return 1;
     }

     // set top parent inode to zero
     mywork->pinode=0;
     pushdir(mywork);
 
     return 0;
}

int processfin() {
  
     return 0;
}

// This app allows users to do a readdirplus walk and optionally print dirs, print links/files, create outputdb 
int validate_inputs() {

   return 0;
}

void sub_help() {
   printf("input_dir         walk this tree to produce GUFI-tree\n");
   printf("\n");
}

int main(int argc, char *argv[])
{
     //char nameo[MAXPATH];
     struct work mywork;
     int i;
     sqlite3 *dbo;

     // process input args - all programs share the common 'struct input',
     // but allow different fields to be filled at the command-line.
     // Callers provide the options-string for get_opt(), which will
     // control which options are parsed for each program.
     int idx = parse_cmd_line(argc, argv, "hHpPn:O:rR", 1, "input_dir");
     if (in.helped)
        sub_help();

     if (idx < 0)
        return -1;
     else {
        // parse positional args, following the options
        int retval = 0;
        INSTALL_STR(in.name,   argv[idx++], MAXPATH, "input_dir");

        if (retval)
           return retval;
     }
     if (validate_inputs())
        return -1;

     /* open the db at the end because its a shared output db */
     if (in.outdb > 0) {
       gts.outdbd[0]=opendb(in.outdbn,dbo,7,1);
     }

     // start threads and loop watching threads needing work and queue size
     // - this always stays in main right here
     mythpool = thpool_init(in.maxthreads);
     if (thpool_null(mythpool)) {
        fprintf(stderr, "thpool_init() failed!\n");
        return -1;
     }

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

     /* close the db at the end because its a shared output db */
     if (in.outdb > 0) {
        closedb(gts.outdbd[0]);
     }

     // clean up threads and exit
     thpool_wait(mythpool);
     thpool_destroy(mythpool);
     return 0;
}
