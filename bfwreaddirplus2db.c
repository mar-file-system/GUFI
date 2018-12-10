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

pthread_mutex_t outdb_mutex[MAXPTHREAD];
sqlite3_stmt *global_res[MAXPTHREAD];

long long int glsuspectflmin;
long long int glsuspectflmax;
long long int glsuspectdmin;
long long int glsuspectdmax;

int gltodirmode;

/* a triell for directories and one for files and links */
struct Trie* headd;
struct Trie* headfl;

/* search the trie either the directory trie type 0 or the filelink type 1 */
int searchmyll(long long int lull, int lutype) {
   char lut[CHAR_SIZE];
   int ret;

   ret=0;
   if (lutype==0) {
     //printf("in searchmyll before dirheadd %lld %d ret %d\n",lull, lutype,ret);
     if (headd==NULL) return 0;
     if (lull < glsuspectdmin) return 0;
     if (lull > glsuspectdmax) return 0;
     sprintf(lut,"%lld",lull);
     ret=searchll(headd,lut); 
     //printf("in searchmyll search dir %lld %d ret %d lut %s\n",lull, lutype,ret,lut);
     // if (ret==1) deletionll(&headd,lut);  this is not thread safe
   }
   if (lutype==1) {
     if (headfl==NULL) return 0;
     if (lull < glsuspectflmin) return 0;
     if (lull > glsuspectflmax) return 0;
     sprintf(lut,"%lld",lull);
     ret=searchll(headfl,lut); 
     //printf("in searchmyll search fl %lld %d ret %d lut %s\n",lull, lutype,ret,lut);
     // if (ret==1) deletionll(&headfl,lut); this is not thread safe
   }
   //printf("in searchmyll %lld %d ret %d\n",lull, lutype,ret);
   return(ret); 
}


int reprocessdir(void * passv, DIR *dir)
{
    struct work *passmywork = passv;
    struct work qwork;
    //DIR *dir;
    struct dirent *entry;
    int mytid;
    sqlite3 *db;
    sqlite3 *db1;
    char *records; 
    char lpatho[MAXPATH];
    struct sum summary;
    sqlite3_stmt *res;   
    sqlite3_stmt *reso;   
    char dbpath[MAXPATH];
    int transcnt;
    int loop;
    char plinein[MAXPATH+MAXPATH+MAXPATH];
    long long int pos;
    int rc;

    // rewind the directory
    rewinddir(dir);

    //open the gufi db for this directory into the parking lot directory the name as the inode of the dir 
    sprintf(dbpath,"%s/%lld",in.nameto,passmywork->statuso.st_ino);
    if (!(db = opendb(dbpath,db1,8,1)))
       return -1;
    res=insertdbprep(db,reso);
    startdb(db);
    records=malloc(MAXRECS);
    bzero(records,MAXRECS);
    zeroit(&summary);

    // loop over dirents, if link push it on the queue, if file or link
    // print it, fill up qwork structure for each
    transcnt = 0;
    loop=1;
    while (loop == 1) {
        
        /* get the next dirent */
        if (!(entry = readdir(dir))) break;
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
          continue;
        //printf("reprocessdir: dir %s entry %s\n",passmywork->name,entry->d_name);

        bzero(&qwork,sizeof(qwork));
        qwork.pinode=passmywork->statuso.st_ino;
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
           continue;
        sprintf(qwork.name,"%s/%s", passmywork->name, entry->d_name);
        lstat(qwork.name, &qwork.statuso);
        qwork.xattrs=0;
        if (in.doxattrs > 0) {
          qwork.xattrs=pullxattrs(qwork.name,qwork.xattr);
        }
        if (S_ISDIR(qwork.statuso.st_mode) ) {
            // this is how the parent gets passed on
            //qwork.pinode=passmywork->statuso.st_ino;
            // there is no work to do for a directory here - we are processing files and links of this dir into a gufi db
            continue;
        } else if (S_ISLNK(qwork.statuso.st_mode) ) {
            // its a link so get the linkname
            bzero(lpatho,sizeof(lpatho));
            readlink(qwork.name,lpatho,MAXPATH);
            sprintf(qwork.linkname,"%s/%s",passmywork->name,lpatho);
            sumit(&summary,&qwork);
            insertdbgo(&qwork,db,res);
            transcnt++;
            if (transcnt > 100000) {
              stopdb(db);
              startdb(db);
              transcnt=0;
            }
        } else if (S_ISREG(qwork.statuso.st_mode) ) {
            sprintf(qwork.type,"%s","f");
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

    stopdb(db);
    insertdbfin(db,res);

    // this i believe has to be after we close off the entries transaction 
    insertsumdb(db,passmywork,&summary);
    closedb(db);

    chown(dbpath, passmywork->statuso.st_uid, passmywork->statuso.st_gid);
    chmod(dbpath, passmywork->statuso.st_mode | S_IRUSR);
    free(records);

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
    int wentry;
    int todb;
    int lookup;
    struct stat st;
    int rc;

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
    passmywork->suspect=in.suspectd;

    /* if we are not looking for suspect directories we should just put the directory at the top of all the dirents */
    if (in.outdb > 0) {
         if (in.insertdir > 0) {
           if (in.suspectmethod == 0) {
             todb=mytid;
             if (in.stride > 0) {
               todb=(passmywork->statuso.st_ino/in.stride)%in.maxthreads; //striping inodes
               //******** start a lock
               pthread_mutex_lock(&outdb_mutex[todb]);
             }
             //printf("in processdirs in.outdb=%d in.insertdir=%d mytid=%d in.insertfl=%d\n",in.outdb,in.insertdir,mytid,in.insertfl);
             startdb(gts.outdbd[todb]);
             insertdbgor(passmywork,gts.outdbd[todb],global_res[todb]);
             if (in.stride > 0) {
               stopdb(gts.outdbd[todb]); //striping inodes
               //***** drop a lock
               pthread_mutex_unlock(&outdb_mutex[todb]);
             }
           }
           if (in.suspectmethod == 1) {  /* look up inode in trie to see if this is a suspect dir */
             lookup=0;
             lookup=searchmyll(passmywork->statuso.st_ino,0);
             if (lookup == 1) passmywork->suspect=1;  /* set the directory suspect flag on so we will mark it in output */
           }
           if (in.suspectmethod > 1) {
             /* ????? we would add a stat call on the directory here and compare mtime and ctime with the last run time provided */
             /* and mark the dir suspect if mtime or ctime are >= provided last run time */
             st.st_ctime=0;
             st.st_mtime=0;
             lstat(passmywork->name,&st);
             if (st.st_ctime >= in.suspecttime) passmywork->suspect=1;
             if (st.st_mtime >= in.suspecttime) passmywork->suspect=1;
           }
         }
    }
 
    if (in.printing > 0 || in.printdir > 0) {
      printits(passmywork,mytid);
    }

    // loop over dirents, if link push it on the queue, if file or link
    // print it, fill up qwork structure for each
    transcnt=0;
    do {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
           continue;
        bzero(&qwork,sizeof(qwork));
        sprintf(qwork.name,"%s/%s", passmywork->name, entry->d_name);
        qwork.pinode=passmywork->statuso.st_ino;
        qwork.statuso.st_ino=entry->d_ino;
        qwork.suspect=in.suspectfl;
        wentry=0;
        //if (S_ISDIR(qwork.statuso.st_mode) ) {
        if (entry->d_type==DT_DIR) {
            if (!access(qwork.name, R_OK | X_OK)) {
                sprintf(qwork.type,"d");
                // this pushes the dir onto queue - pushdir does locking around queue update
                pushdir(&qwork);
            }
        } else if (entry->d_type==DT_LNK) {
            sprintf(qwork.type,"%s","l");
            wentry=1;
        } else if (entry->d_type==DT_REG) {
            sprintf(qwork.type,"%s","f");
            wentry=1;
        }
        if (wentry==1) {
          if (in.printing > 0) {
            printits(&qwork,mytid);
          }
          /* if suspect method is not zero then we can insert files and links, if not we dont care about files and links in db */
          if (in.suspectmethod == 0) {
            if (in.outdb > 0) {
              if (in.insertfl > 0) {
                todb=mytid;
                if (in.stride > 0) {
                  todb=(qwork.statuso.st_ino/in.stride)%in.maxthreads; //striping inodes
                  //************** start a lock
                  pthread_mutex_lock(&outdb_mutex[todb]);
                  startdb(gts.outdbd[todb]); //striping inodes
                }
                insertdbgor(&qwork,gts.outdbd[todb],global_res[todb]);
                if (in.stride > 0) {
                  stopdb(gts.outdbd[todb]); //striping inodes
                  //************** drop a lock
                  pthread_mutex_unlock(&outdb_mutex[todb]);
                } else {
                  transcnt++;
                  if (transcnt > 100000) {
                    stopdb(gts.outdbd[todb]);
                    startdb(gts.outdbd[todb]);
                    transcnt=0;
                  }
                }
              }
            }
          }
          if (passmywork->suspect == 0) { /* if suspect dir just skip looking any further */
            if ((in.suspectmethod == 1) || (in.suspectmethod == 2)) {   /* if method 1 or 2 we look up the inode in trie and mark dir suspect or not */
               lookup=0;
               lookup=searchmyll(qwork.statuso.st_ino,1);
               if (lookup == 1) passmywork->suspect=1;  /* set the directory suspect flag on so we will mark it in output */

            }
            if (in.suspectmethod > 2) {
              /* ???? we would stat the file/link and if ctime or mtime is >= provided last run time mark dir suspect */
              st.st_ctime=0;
              st.st_mtime=0;
              lstat(qwork.name,&st);
              if (st.st_ctime >= in.suspecttime) passmywork->suspect=1;
              if (st.st_mtime >= in.suspecttime) passmywork->suspect=1;
            }
          }
        }
    } while ((entry = (readdir(dir))));

    /* if we are not looking for suspect directories we should just put the directory at the top of all the dirents */
    if (in.suspectmethod > 0) {
      if (in.outdb > 0) {
         if (in.insertdir > 0) {
           todb=mytid;
           if (in.stride > 0) {
             todb=(passmywork->statuso.st_ino/in.stride)%in.maxthreads; //striping inodes
             //******** start a lock
             pthread_mutex_lock(&outdb_mutex[todb]);
           }
           startdb(gts.outdbd[todb]);
           //printf("in processdirs in.outdb=%d in.insertdir=%d mytid=%d in.insertfl=%d\n",in.outdb,in.insertdir,mytid,in.insertfl);
           insertdbgor(passmywork,gts.outdbd[todb],global_res[todb]);
           if (in.stride > 0) {
             stopdb(gts.outdbd[todb]); //striping inodes
             //***** drop a lock
             pthread_mutex_unlock(&outdb_mutex[todb]);
           }
         }
      }
    }
 
    if (in.outdb > 0) {
      if (in.stride == 0) {
        todb=mytid;
        stopdb(gts.outdbd[todb]);
      }
    }

    if (passmywork->suspect==1) {
      if (gltodirmode==1) {
        rc=reprocessdir(passmywork,dir);
        if (rc !=0) fprintf(stderr,"problem producing gufi db for suspect directory\n");
      }
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
     sqlite3_stmt *reso;
     sqlite3_stmt *res;
     sqlite3 *dbo;
     char outdbn[MAXPATH];
     FILE *isf;
     char incsuspect[24];
     char incsuspecttype[2];
     long long int testll;
     long long int cntd;
     long long int cntfl;

     if (in.suspectfile > 0) {
       if( (isf = fopen(in.insuspect, "r")) == NULL)
       {
          fprintf(stderr,"Cant open input suspect file %s\n",in.insuspect);
          exit(1);
       }  
       cntfl=0;
       cntd=0;
       /* set up triell for directories and one for files and links */
       headd = getNewTrieNode();
       headfl = getNewTrieNode();
       while (fscanf(isf,"%s %s",incsuspect, incsuspecttype)!= EOF) { 
          //printf("insuspect |%s| |%s|\n",incsuspect, incsuspecttype );  
          testll=atoll(incsuspect);
          if (!strncmp(incsuspecttype,"f",1)) {
             if (cntfl==0) {
                glsuspectflmin=testll;
                glsuspectflmax=testll;;
             } else {
               if (testll < glsuspectflmin) glsuspectflmin=testll;
               if (testll > glsuspectflmax) glsuspectflmax=testll;
             }
             //printf("insuspect %s %s %lld %lld\n",incsuspect, incsuspecttype,glsuspectflmin,glsuspectflmax );  
             insertll(&headfl, incsuspect);
             cntfl++;
          }
          if (!strncmp(incsuspecttype,"l",1)) {
             if (cntfl==0) {
                glsuspectflmin=testll;
                glsuspectflmax=testll;;
             } else {
               if (testll < glsuspectflmin) glsuspectflmin=testll;
               if (testll > glsuspectflmax) glsuspectflmax=testll;
             }
             //printf("insuspect %s %s %lld %lld\n",incsuspect, incsuspecttype,glsuspectflmin,glsuspectflmax );  
             insertll(&headfl, incsuspect);
             cntfl++;
          }
          if (!strncmp(incsuspecttype,"d",1)) {
             if (cntd==0) {
                glsuspectdmin=testll;
                glsuspectdmax=testll;;
             } else {
               if (testll < glsuspectdmin) glsuspectdmin=testll;
               if (testll > glsuspectdmax) glsuspectdmax=testll;
             }
             //printf("insuspect %s %s %lld %lld\n",incsuspect, incsuspecttype,glsuspectdmin,glsuspectdmax );  
             insertll(&headd, incsuspect);
             cntd++;
          }
       }  
       fclose(isf);
     }  

     if (in.outdb > 0) {
       i=0;
       while (i < in.maxthreads) {
         sprintf(outdbn,"%s.%d",in.outdbn,i);
         gts.outdbd[i]=opendb(outdbn,dbo,7,1);
         global_res[i]=insertdbprepr(gts.outdbd[i],reso);
         if (in.stride > 0) {
           if (pthread_mutex_init(&outdb_mutex[i], NULL) != 0) {
             fprintf(stderr,"\n mutex %d init failed\n",i);
           }
         }
         i++;
       }
     }

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
int i;

     // close output dbs here
     if (in.outdb > 0) {
       i=0;
       while (i < in.maxthreads) {
         insertdbfin(gts.outdbd[i],global_res[i]);
         closedb(gts.outdbd[i]);
         if (in.stride > 0) {
           pthread_mutex_destroy(&outdb_mutex[i]);
         }
         i++;
       }
     }
  
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
     struct stat st;
     int rc;

     // process input args - all programs share the common 'struct input',
     // but allow different fields to be filled at the command-line.
     // Callers provide the options-string for get_opt(), which will
     // control which options are parsed for each program.
     int idx = parse_cmd_line(argc, argv, "hHpPn:O:rRYZW:g:A:c:xt:", 1, "input_dir");
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

     /* check the output directory for the gufi dbs for suspect dirs if provided */
     gltodirmode=0;
     rc=1;
     if (strlen(in.nameto) > 0) {
       gltodirmode=1;
       /*make sure the directory to put the gufi dbs into exists and we can write to it */       
       rc=lstat(in.nameto,&st);
       if (rc != 0) {
         fprintf(stdout,"directory to place gufi dbs problem for %s\n",in.nameto);
         return -1;
       }
       if (!S_ISDIR(st.st_mode) ) {
         fprintf(stdout,"directory to place gufi dbs is not a directory\n");
         return -1;
       }
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

     // clean up threads and exit
     thpool_wait(mythpool);
     thpool_destroy(mythpool);
     return 0;
}

