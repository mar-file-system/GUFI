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

/* used to allow all threads to open up the input file if there is one */
FILE *gin[MAXPTHREAD];

pthread_t gtid;
pthread_attr_t gattr;

void parsetowork (char * inpdelim, char * inpline, void * inpwork ) {

    struct work *pinwork=inpwork;
    char *p;
    char *q;

    //printf("in parsetowork delim %s inpline %s\n",inpdelim,inpline);
    inpline[strlen(inpline)-1]= '\0';
    p=inpline; q=strstr(p,inpdelim); memset(q, 0, 1); sprintf(pinwork->name,"%s",p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1), sprintf(pinwork->type,"%s",p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); pinwork->statuso.st_ino=atol(p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); pinwork->statuso.st_mode=atol(p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); pinwork->statuso.st_nlink=atol(p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); pinwork->statuso.st_uid=atol(p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); pinwork->statuso.st_gid=atol(p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); pinwork->statuso.st_size=atol(p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); pinwork->statuso.st_blksize=atol(p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); pinwork->statuso.st_blocks=atol(p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); pinwork->statuso.st_atime=atol(p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); pinwork->statuso.st_mtime=atol(p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); pinwork->statuso.st_ctime=atol(p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); sprintf(pinwork->linkname,"%s",p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); sprintf(pinwork->xattr,"%s",p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); pinwork->crtime=atol(p);

}

void *scout(void * param) {
    char *ret;
    FILE *finfile;
    char linein[MAXPATH+MAXPATH+MAXPATH];
    long long int foffset;
    struct work * mywork;

    mywork=malloc(sizeof(struct work));
    //printf("in scout arg %s\n",param);
    //incrthread(); /* add one thread so the others wait for the scout */
    finfile=fopen(in.name,"r");
    if (finfile == NULL) {
      fprintf(stderr,"Cant open up the input file %s\n",in.name);
      exit(-1); /* not the best way out i suppose */
    }
    //printf("reading input file now\n");
    memset(linein, 0, sizeof(linein));
    //sleep(5);
    while (fgets (linein, sizeof(linein), finfile) !=NULL ) {
          //printf("got input line %s\n",linein);
          parsetowork (in.delim, linein, mywork );
          //printf("%s %s %llu %d %d %d %d %llu %d %llu %lu %lu %lu\n",mywork->name,mywork->type,mywork->statuso.st_ino,mywork->statuso.st_mode,mywork->statuso.st_nlink,mywork->statuso.st_uid,mywork->statuso.st_gid,mywork->statuso.st_size,mywork->statuso.st_blksize,mywork->statuso.st_blocks,mywork->statuso.st_atime,mywork->statuso.st_mtime,mywork->statuso.st_ctime);
          fgetpos(finfile,&foffset);
          if (!strncmp("d",mywork->type,1)) {
             mywork->pinode=0;
             //printf("pushing %s %s %llu %d %d %d %d %llu %d %llu %lu %lu %lu\n",mywork->name,mywork->type,mywork->statuso.st_ino,mywork->statuso.st_mode,mywork->statuso.st_nlink,mywork->statuso.st_uid,mywork->statuso.st_gid,mywork->statuso.st_size,mywork->statuso.st_blksize,mywork->statuso.st_blocks,mywork->statuso.st_atime,mywork->statuso.st_mtime,mywork->statuso.st_ctime);
             //printf("foffsett %lld\n",foffset);
             mywork->offset=foffset;
             pushdir(mywork);
          }
          memset(linein, 0, sizeof(linein));
    }
    fclose(finfile);
    //sleep(5);
    decrthread(); /* take one thread away so others can finish */
    ret=(char *) malloc(20);
    strcpy(ret, "scout finished");
    pthread_exit(ret);
}

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
    sqlite3 *db = NULL;
    char *records;
    struct sum summary;
    sqlite3_stmt *res = NULL;
    sqlite3_stmt *reso = NULL;
    char dbpath[MAXPATH];
    int transcnt;
    int loop;
    char plinein[MAXPATH+MAXPATH+MAXPATH];
    long long int pos;

    // get thread id so we can get access to thread state we need to keep
    // until the thread ends
    mytid=0;
    //if (in.outfile > 0) mytid=gettid();
    mytid=gettid();

    //if (in.infile > 0) return;
    //printf("in processdir tid %d passin name %s type %s offset %lld\n",mytid,passmywork->name,passmywork->type,passmywork->offset);

    if (in.infile == 0) {
      // open directory
      if (!(dir = opendir(passmywork->name)))
         goto out_free; // return NULL;
      //if (!(entry = readdir(dir)))
      //   goto out_dir; // return NULL;
    } else {
      /* seek to the fileslinks following the directory in my fopened file */
      fsetpos(gin[mytid],&passmywork->offset);
      //fgetpos(gin[mytid],&pos);
      //printf("position set for tid %d position %lld\n",mytid,pos);
    }

    sprintf(passmywork->type,"%s","d");
    if (in.printing > 0 || in.printdir > 0) {
      printits(passmywork,mytid);
    }
    //printf("tid %d make dir %s\n",mytid,passmywork->name);

    if (in.buildindex > 0) {
       if (in.buildinindir == 0) {
         dupdir(passmywork);
         //printf("tid %d made dir %s\n",mytid,passmywork->name);
       }
       records=malloc(MAXRECS);
       memset(records, 0, MAXRECS);
       zeroit(&summary);
       if (!(db = opendb(passmywork->name,4,1)))
          goto out_dir;
       res=insertdbprep(db,reso);
       startdb(db);
    }

    //printf("tid %d made db\n",mytid);
    // loop over dirents, if link push it on the queue, if file or link
    // print it, fill up qwork structure for each
    transcnt = 0;
    loop=1;
    //do {
    while (loop == 1) {

        if (in.infile == 0) {
          /* get the next dirent */
          if (!(entry = readdir(dir))) break;
        } else {
          /* get the next record */
          memset(plinein, 0, sizeof(plinein));
          if (fgets (plinein, sizeof(plinein), gin[mytid]) ==NULL ) break;
          //printf("tid %d got line %s\n",mytid,plinein);
        }

/*?????  farret out two modes from here */

        memset(&qwork, 0, sizeof(qwork));
        qwork.pinode=passmywork->statuso.st_ino;
        if (in.infile == 0) {
          if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
             continue;
          if (in.buildinindir == 1) {
            if (strcmp(entry->d_name, DBNAME) == 0)
               continue;
          }
          sprintf(qwork.name,"%s/%s", passmywork->name, entry->d_name);
          lstat(qwork.name, &qwork.statuso);
          qwork.xattrs=0;
          if (in.doxattrs > 0) {
            bzero(qwork.xattr,sizeof(qwork.xattr));
            qwork.xattrs=pullxattrs(qwork.name,qwork.xattr);
          }
        } else {
          /* just parse the read line into qwork */
          parsetowork(in.delim, plinein, &qwork);
        }
        if (S_ISDIR(qwork.statuso.st_mode) ) {
            // this is how the parent gets passed on
            qwork.pinode=passmywork->statuso.st_ino;
            if (in.infile == 0) {
              if (!access(qwork.name, R_OK | X_OK)) {
                // this pushes the dir onto queue - pushdir does locking around queue update
                if (in.dontdescend < 1) {
                  pushdir(&qwork);
                }
              }
            } else {
/*
              sprintf(qwork.type,"%s","d");
              fgetpos(gin[mytid],&pos);
              qwork.offset=pos;
              pushdir(&qwork);
*/
              loop=0;
            }
        } else if (S_ISLNK(qwork.statuso.st_mode) ) {
            // its a link so get the linkname
            if (in.infile == 0) {
              /* if its infile we have to get this elsewhere */
              bzero(lpatho,sizeof(lpatho));
              readlink(qwork.name,lpatho,MAXPATH);
              //sprintf(qwork.linkname,"%s/%s",passmywork->name,lpatho);
              sprintf(qwork.linkname,"%s",lpatho);
            }
            sprintf(qwork.type,"%s","l");
            if (in.printing > 0) {
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
        } else if (S_ISREG(qwork.statuso.st_mode) ) {
            sprintf(qwork.type,"%s","f");
            if (in.printing > 0) {
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
        }
    //} while ((entry = (readdir(dir))));
    }

    if (in.buildindex > 0) {
      stopdb(db);
      insertdbfin(db,res);

      // this i believe has to be after we close off the entries transaction
      insertsumdb(db,passmywork,&summary);
      closedb(db);

      sprintf(dbpath, "%s/%s/DBNAME", in.nameto,passmywork->name);
      chown(dbpath, passmywork->statuso.st_uid, passmywork->statuso.st_gid);
      chmod(dbpath, passmywork->statuso.st_mode | S_IRUSR);
      free(records);
    }

 out_dir:
    if (in.infile == 0) {
      // close dir
      closedir(dir);
    }

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
     char outfn[MAXPATH];
     FILE *finfile;
     char linein[MAXPATH+MAXPATH+MAXPATH];
     long long int foffset;

     //open up the output files if needed
     if (in.outfile > 0) {
       i=0;
       while (i < in.maxthreads) {
         sprintf(outfn,"%s.%d",in.outfilen,i);
         gts.outfd[i]=fopen(outfn,"w");
         i++;
       }
     }

     //open up the input file if needed
     if (in.infile > 0) {
       //finfile=fopen(in.name,"r");
       if (finfile == NULL) {
         fprintf(stderr,"Cant open up the input file %s\n",in.name);
         exit(-1); /* not the best way out i suppose */
       }
       i=0;
       while (i < in.maxthreads) {
         gin[i]=fopen(in.name,"r");
         i++;
       }
       /* loop through reading the input file and putting the directories found on the queue */
       /* we will not know if this file is in sort order of paths, we know that the file */
       /* has all the files/links in a dir immediatly following the dir record but */
       /* the dirs may not be in order, for this reason we can just set the parent inode */
       /* equal zero i think, what gets put into the entries and summary tables doesnt */
       /* have the parent inode anyway, so this is presentation level stuff only */
       /* the parent inode is not stored in the tables anyway because of rename/unlink  */
       //foffset=0;
/*
       while (fgets (linein, sizeof(linein), finfile) !=NULL ) {
          parsetowork (in.delim, linein, mywork );
          printf("%s %s %llu %d %d %d %d %llu %d %llu %lu %lu %lu\n",mywork->name,mywork->type,mywork->statuso.st_ino,mywork->statuso.st_mode,mywork->statuso.st_nlink,mywork->statuso.st_uid,mywork->statuso.st_gid,mywork->statuso.st_size,mywork->statuso.st_blksize,mywork->statuso.st_blocks,mywork->statuso.st_atime,mywork->statuso.st_mtime,mywork->statuso.st_ctime);
          fgetpos(finfile,&foffset);
          if (!strncmp("d",mywork->type,1)) {
             mywork->pinode=0;
             printf("pushing %s %s %llu %d %d %d %d %llu %d %llu %lu %lu %lu\n",mywork->name,mywork->type,mywork->statuso.st_ino,mywork->statuso.st_mode,mywork->statuso.st_nlink,mywork->statuso.st_uid,mywork->statuso.st_gid,mywork->statuso.st_size,mywork->statuso.st_blksize,mywork->statuso.st_blocks,mywork->statuso.st_atime,mywork->statuso.st_mtime,mywork->statuso.st_ctime);
             printf("foffsett %lld\n",foffset);
             mywork->offset=foffset;
             pushdir(mywork);
          }
          memset(linein, 0, sizeof(linein));;
       }
*/
/***
       memset(linein, 0, sizeof(linein));;
       if (fgets(linein, sizeof(linein), finfile) !=NULL ) {
          parsetowork (in.delim, linein, mywork );
          if (!strncmp(mywork->type,"d",1)) {
            mywork->pinode=0;
            fgetpos(finfile,&foffset);
            mywork->offset=foffset;
            pushdir(mywork);
          } else {
            fprintf(stderr,"first line of input file is not a dir %s\n",in.name);
          }
       } else {
         fprintf(stderr,"cant read first line of input file %s\n",in.name);
       }
       fclose(finfile);
***/
       //pthread_attr_init(&gattr);
       //printf("starting scout\n");
       incrthread(); /* add one thread so the others wait for the scout */
       pthread_create(&gtid,NULL,scout,&in.name);
     } else { /* no input file we are walking*/
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
       bzero(mywork->xattr,sizeof(mywork->xattr));
       bzero(mywork->linkname,sizeof(mywork->linkname));
       if (in.doxattrs > 0) {
         mywork->xattrs=0;
         mywork->xattrs=pullxattrs(in.name,mywork->xattr);
       }

       // set top parent inode to zero
       mywork->pinode=0;
       pushdir(mywork);
     }

     return 0;
}

int processfin() {
     int i;
     void *retval;

     if (in.outfile > 0) {
       i=0;
       while (i < in.maxthreads) {
         fclose(gts.outfd[i]);
         i++;
       }
     }
     if (in.infile > 0) {
       i=0;
       while (i < in.maxthreads) {
         fclose(gin[i]);
         i++;
       }
       /* wait on the scout thread */
       pthread_join(gtid, &retval);
       //printf("retval=%s\n",retval);
     }

     return 0;
}

// This app allows users to do any of the following: (a) just walk the
// input tree, (b) like a, but also creating corresponding GUFI-tree
// directories, (c) like b, but also creating an index.
int validate_inputs() {
   char expathin[MAXPATH];
   char expathout[MAXPATH];
   char expathtst[MAXPATH];

   if (in.buildindex && !in.nameto[0]) {
      fprintf(stderr, "Building an index '-b' requires a destination dir '-t'.\n");
      return -1;
   }
   if (in.nameto[0] && ! in.buildindex) {
      fprintf(stderr, "Destination dir '-t' found.  Assuming implicit '-b'.\n");
      in.buildindex = 1;        // you're welcome
   }

   sprintf(expathtst,"%s/%s",in.nameto,in.name);
   realpath(expathtst,expathout);
   //printf("expathtst: %s expathout %s\n",expathtst,expathout);
   realpath(in.name,expathin);
   //printf("in.name: %s expathin %s\n",in.name,expathin);
   if (!strcmp(expathin,expathout)) {
     fprintf(stderr,"You are putting the index dbs in input directory\n");
     in.buildinindir = 1;
   }

   // not errors, but you might want to know ...
   if (! in.nameto[0])
      fprintf(stderr, "WARNING: No GUFI-tree specified (-t).  No GUFI-tree will be built.\n");
   //   else if (! in.buildindex)
   //      fprintf(stderr, "WARNING: Index-building not requested (-b).  No GUFI index will be built.\n");

   return 0;
}

void sub_help() {
   printf("input_dir         walk this tree to produce GUFI-tree\n");
   // printf("GUFI_dir          build GUFI index here (if -b)\n");
   printf("\n");
}

int main(int argc, char *argv[])
{
     //char nameo[MAXPATH];
     struct work mywork = {};
     int i;

     // process input args - all programs share the common 'struct input',
     // but allow different fields to be filled at the command-line.
     // Callers provide the options-string for get_opt(), which will
     // control which options are parsed for each program.
     int idx = parse_cmd_line(argc, argv, "hHpn:d:xPbo:t:Du", 1, "input_dir", &in);
     if (in.helped)
        sub_help();
     if (idx < 0)
        return -1;
     else {
        // parse positional args, following the options
        int retval = 0;
        INSTALL_STR(in.name,   argv[idx++], MAXPATH, "input_dir");
        // INSTALL_STR(in.nameto, argv[idx++], MAXPATH, "to_dir");

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
     // program in this case we are stating the directory passed in and
     // putting that directory on the queue
     processinit(&mywork);

     // processdirs - if done properly, this routine is common and does not
     // have to be done per instance of a bf program loops through and
     // processes all directories that enter the queue by farming the work
     // out to the threadpool
     //sleep(5);
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
