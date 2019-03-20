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

#include <pwd.h>
#include <grp.h>
#include "mysql.h"

extern int errno;

struct mysqlinput {
   char  dbuser[50];
   char  dbpasswd[50];
   char  dbhost[50];
   char  dbdb[50];
   char  robinin[MAXPATH];
   MYSQL mysql[MAXPTHREAD];
} msn;

int mysql_execute_sql(MYSQL *mysql,const char *create_definition)
{
   return mysql_real_query(mysql, create_definition, strlen(create_definition));
}

// This becomes an argument to thpool_add_work(), so it must return void,
// instead of void*.
static void processdir(void * passv)
{
    struct work *passmywork = passv;
    struct work qwork;
    DIR *dir;
    struct dirent *entry;
    struct dirent myentry;
    char lpatho[MAXPATH];
    int mytid;
    sqlite3 *db;
    char *records;
    struct sum summary;
    sqlite3_stmt *res;
    sqlite3_stmt *reso;
    char dbpath[MAXPATH];
    int transcnt;
    MYSQL lmysql;
    MYSQL_RES *lresult;
    MYSQL_ROW lrow;
    char lpinodec[128];
    unsigned int lnum_fields;
    char ltchar[256];
    struct passwd *lmypasswd;
    struct group *lmygrp;
    char myinsql[MAXSQL];

    // get thread id so we can get access to thread state we need to keep until the thread ends
    mytid=0;
    mytid=gettid();
    //printf("mytid is %d\n",mytid);

    // open directory
    //if (!(dir = opendir(passmywork->name)))
    //    return NULL;
    //if (!(entry = readdir(dir)))
    //    return NULL;
    SNPRINTF(passmywork->type,2,"%s","d");
    // print?
    if (in.printing > 0 || in.printdir > 0) {
      printits(passmywork,mytid);
    }

    if (in.buildindex > 0) {
       dupdir(passmywork);
       records=malloc(MAXRECS);
       memset(records, 0, MAXRECS);
       //sqlite3 *  opendb(const char *name, sqlite3 *db, int openwhat, int createtables)
       zeroit(&summary);
       db = opendb(passmywork->name,4,1);
       res=insertdbprep(db,reso);
       //printf("inbfilistdir res %d\n",res);
       startdb(db);
    }

    SNPRINTF(myinsql,MAXSQL,"select name, type, n.id, n.parent_id, mode, uid, gid, size, last_access, last_mod, last_mdchange, blocks, nlink, replace(replace(replace(replace(replace(replace(replace(replace(n.id,\":\",\"\"),\"0x\",\"\"),\"a\",\"10\"),\"b\",\"11\"),\"c\",\"12\"),\"d\",\"13\"),\"e\",\"14\"),\"f\",\"15\") from entries e left join names n on n.id=e.id  where parent_id = \'%s\'",passmywork->pinodec);
    //printf("select name, type, n.id, n.parent_id, mode, uid, gid, size, last_access, last_mod, last_mdchange, blocks, nlink, replace(replace(replace(replace(replace(replace(replace(replace(n.id,\":\",\"\"),\"0x\",\"\"),\"a\",\"10\"),\"b\",\"11\"),\"c\",\"12\"),\"d\",\"13\"),\"e\",\"14\"),\"f\",\"15\") from entries e left join names n on n.id=e.id  where parent_id = \'%s\'\n",passmywork->pinodec);
    if(mysql_execute_sql(&msn.mysql[mytid],myinsql)!=0) {
      printf( "select  %s failed\n", mysql_error(&msn.mysql[mytid]));
      printf( "%ld Record Found\n",(long)mysql_affected_rows(&msn.mysql[mytid]));
    } else {
      lresult = mysql_store_result(&msn.mysql[mytid]);
    }
    if (lresult) {
      lnum_fields = mysql_num_fields(lresult);
      //printf("lnum_fields = %d\n",lnum_fields);
      transcnt = 0;
      while ((lrow = mysql_fetch_row(lresult))) {
        //printf("fetching row\n");
        entry = &myentry;
        SNPRINTF(ltchar,128,"%s",lrow[0]);  SNPRINTF(qwork.name,MAXPATH,"%s/%s", passmywork->name,ltchar);
        SNPRINTF(ltchar,128,"%s",lrow[1]);  SNPRINTF(qwork.type, 2, "%1s", ltchar);
        SNPRINTF(ltchar,128,"%s",lrow[2]);  /* this is char inode*/ SNPRINTF(lpinodec,"%s",ltchar); /* the inode becomes the pinodec */
        SNPRINTF(ltchar,128,"%s",lrow[3]);  /* this is the character pinode*/
        SNPRINTF(ltchar,128,"%s",lrow[4]);  qwork.statuso.st_mode = atol(ltchar);
        SNPRINTF(ltchar,128,"%s",lrow[5]);  lmypasswd = getpwnam(ltchar); qwork.statuso.st_uid = lmypasswd->pw_uid;
        SNPRINTF(ltchar,128,"%s",lrow[6]);  lmygrp = getgrnam(ltchar); qwork.statuso.st_gid = lmygrp->gr_gid;
        SNPRINTF(ltchar,128,"%s",lrow[7]);  qwork.statuso.st_size = atol(ltchar);
                                       qwork.statuso.st_blksize= 4096; /* dont have blocksize yet */
        SNPRINTF(ltchar,128,"%s",lrow[8]);  qwork.statuso.st_atime = atol(ltchar);
        SNPRINTF(ltchar,128,"%s",lrow[9]);  qwork.statuso.st_mtime = atol(ltchar);
        SNPRINTF(ltchar,128,"%s",lrow[10]); qwork.statuso.st_ctime = atol(ltchar);
        SNPRINTF(ltchar,128,"%s",lrow[11]); qwork.statuso.st_blocks= atoll(ltchar);
        SNPRINTF(ltchar,128,"%s",lrow[12]); qwork.statuso.st_nlink = atol(ltchar);
        SNPRINTF(ltchar,128,"%s",lrow[13]); qwork.statuso.st_ino = atoll(ltchar);
        memset(qwork.linkname, 0, sizeof(qwork.linkname)); /* dont have linkname yet */
        memset(qwork.xattr, 0, sizeof(qwork.xattr));
        /* need to get xattre right here */
        qwork.xattrs=0;
        qwork.xattrs=strlen(qwork.xattr);
        if (qwork.xattrs > 0) {
          qwork.xattrs = 1;
        }
        qwork.pinode=passmywork->statuso.st_ino;
        if (!strncmp(qwork.type,"d",1)) {
            if (strcmp(qwork.name, ".") == 0 || strcmp(qwork.name, "..") == 0)
                continue;
            SNPRINTF(qwork.pinodec,128,"%s",lpinodec);
            // this pushes the dir onto queue - pushdir does locking around queue update
            pushdir(&qwork);
        } else if (!strncmp(qwork.type,"l",1)) {
            if (in.printing > 0 || in.printdir > 0) {
               printits(&qwork,mytid);
            }
            if (in.buildindex > 0) {
              sumit(&summary,&qwork);
              insertdbgo(&qwork,db,res);
              transcnt++;
              if (transcnt > 100000) {
                stopdb(db);
                startdb(db);
                transcnt=0;
              }
            }
            if (in.printing > 0 || in.printdir > 0) {
               printits(&qwork,mytid);
            }
        }
        else if (!strncmp(qwork.type,"f",1)) {
            if (in.buildindex > 0) {
              sumit(&summary,&qwork);
              insertdbgo(&qwork,db,res);
              transcnt++;
              if (transcnt > 100000) {
                stopdb(db);
                startdb(db);
                transcnt=0;
              }
            }
            if (in.printing > 0 || in.printdir > 0) {
               printits(&qwork,mytid);
            }
        }
      }
    }
    mysql_free_result(lresult);

    if (in.buildindex > 0) {
      stopdb(db);
      insertdbfin(db,res);

      // this i believe has to be after we close off the entries transaction
      insertsumdb(db,passmywork,&summary);
      closedb(db);

      SNPRINTF(dbpath, MAXPATH, "%s/%s/DBNAME", in.nameto,passmywork->name);
      chown(dbpath, passmywork->statuso.st_uid, passmywork->statuso.st_gid);
      chmod(dbpath, passmywork->statuso.st_mode | S_IRUSR);
      free(records);
    }

    // one less thread running
    decrthread();

    // free the queue entry - this has to be here or there will be a leak
    free(passmywork->freeme);

    // return NULL;
}


int processinit(void * myworkin) {
     struct work * mywork = myworkin;
     int i;
     char outfn[MAXPATH];
     FILE *robinfd;
     char tchar[256];

     //open up the output files if needed
     if (in.outfile > 0) {
       i=0;
       while (i < in.maxthreads) {
         SNPRINTF(outfn,MAXPATH,"%s.%d",in.outfilen,i);
         gts.outfd[i]=fopen(outfn,"w");
         i++;
       }
     }

     // read in how we start off the first root query from robinhoodin file
     robinfd=fopen(msn.robinin,"r");
     fgets(mywork->name, sizeof(mywork->name),robinfd); strtok(mywork->name,"\n");
     fgets(mywork->pinodec, sizeof(mywork->pinodec),robinfd); strtok(mywork->pinodec,"\n");
     fgets(tchar, sizeof(tchar),robinfd); mywork->statuso.st_ino=atoll(tchar);
     fgets(tchar,sizeof(tchar),robinfd);  mywork->statuso.st_mode=atol(tchar);
     fgets(tchar,sizeof(tchar),robinfd);  mywork->statuso.st_atime = atol(tchar);
                                          mywork->statuso.st_mtime = atol(tchar);
                                          mywork->statuso.st_ctime = atol(tchar);
     fgets(msn.dbhost, sizeof(msn.dbhost),robinfd); strtok(msn.dbhost,"\n");
     fgets(msn.dbuser, sizeof(msn.dbuser),robinfd); strtok(msn.dbuser,"\n");
     fgets(msn.dbpasswd, sizeof(msn.dbpasswd),robinfd); strtok(msn.dbpasswd,"\n");
     fgets(msn.dbdb, sizeof(msn.dbdb),robinfd); strtok(msn.dbdb,"\n");
     fclose(robinfd);
     mywork->statuso.st_nlink = 1;
     mywork->statuso.st_uid = 0;
     mywork->statuso.st_gid = 0;
     mywork->statuso.st_size = 4096;
     mywork->statuso.st_blksize= 4096;
     mywork->statuso.st_blocks= 1;
     mywork->pinode = 0;
     memset(mywork->xattr, 0, sizeof(mywork->xattr));
     memset(mywork->linkname, 0, sizeof(mywork->linkname));
     //printf("name %s pinodec %s\n",mywork->name,mywork->pinodec);

     // open connection to mysql db per thread
     i=0;
     while (i < in.maxthreads) {
       if(mysql_init(&msn.mysql[i])==NULL) {
         printf("MySQL init failed");
         exit(1);
       }
       if (!mysql_real_connect(&msn.mysql[i],msn.dbhost,msn.dbuser,msn.dbpasswd,NULL,0,NULL,0)) {
          printf( "connect to localhost %s failed\n", mysql_error(&msn.mysql[i]));
          exit(1);
       }
       if(mysql_select_db(&msn.mysql[i],msn.dbdb)!=0) {
        printf( "connection to database %s: Error: %s\n",msn.dbdb,mysql_error(&msn.mysql[i]));
       }
       i++;
     }

     pushdir(mywork);
     return 0;
}


int processfin() {
     int i;

     // close output files
     if (in.outfile > 0) {
       i=0;
       while (i < in.maxthreads) {
         fclose(gts.outfd[i]);
         i++;
       }
     }

     // close mysql connections
     i=0;
     while (i < in.maxthreads) {
       mysql_close(&msn.mysql[i]);
       i++;
     }

     return 0;
}



int validate_inputs() {
   if (in.buildindex && !in.nameto[0]) {
      fprintf(stderr, "building an index '-b' requires a destination dir (see '-t'))\n");
      return -1;
   }
   else if (in.nameto[0] && ! in.buildindex) {
      fprintf(stderr, "Destination dir '-t' found.  Assuming implicit '-b'.\n");
      in.buildindex = 1; // you're welcome
   }

   return 0;
}

void sub_help() {
   // printf("GUFI_tree         find GUFI index-tree here\n");
   printf("robin_in          file containing custom RobinHood parameters\n");
   printf("                     example contents:\n");
   printf("                     /path  - top dir-path (RH doesnt have a name for the root)\n");
   printf("                     0x200004284:0x11:0x0 - fid of the root\n");
   printf("                     20004284110          - inode of root\n");
   printf("                     16877                - mode of root\n");
   printf("                     1500000000           - atime=mtime=ctime of root\n");
   printf("                     localhost            - host of mysql\n");
   printf("                     ggrider              - user of mysql\n");
   printf("                     mypassword           - password for user of mysql\n");
   printf("                     institutes           - name of db of mysql\n");
   printf("\n");
}

int main(int argc, char *argv[])
{
     //char nameo[MAXPATH];
     struct work mywork;
     int i;

     // process input args - all programs share the common 'struct input',
     // but allow different fields to be filled at the command-line.
     // Callers provide the options-string for get_opt(), which will
     // control which options are parsed for each program.
     int idx = parse_cmd_line(argc, argv, "hHpn:d:xPbt:o:", 1, "robin_in");
     if (in.helped)
        sub_help();
     if (idx < 0)
         return -1;
     else {
        // parse positional args, following the options
        int retval = 0;
        INSTALL_STR(msn.robinin,  argv[idx++], MAXPATH, "robin_in");

        if (retval)
           return retval;
     }

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
     // program in this case we are stat'ing the directory passed in and
     // putting that directory on the queue
     processinit(&mywork);

     // processdirs - if done properly, this routine is common and does not
     // have to be done per instance of a bf program.  Loops through and
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
