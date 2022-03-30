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
#include <pthread.h>
#include <sqlite3.h>
#include <stdio.h>
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

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "dbutils.h"
#include "utils.h"

extern int errno;
pthread_mutex_t outdb_mutex[MAXPTHREAD];
pthread_mutex_t outfile_mutex[MAXPTHREAD];
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
     SNPRINTF(lut,CHAR_SIZE,"%lld",lull);
     ret=searchll(headd,lut);
     //printf("in searchmyll search dir %lld %d ret %d lut %s\n",lull, lutype,ret,lut);
     // if (ret==1) deletionll(&headd,lut);  this is not thread safe
   }
   if (lutype==1) {
     if (headfl==NULL) return 0;
     if (lull < glsuspectflmin) return 0;
     if (lull > glsuspectflmax) return 0;
     SNPRINTF(lut,CHAR_SIZE,"%lld",lull);
     ret=searchll(headfl,lut);
     //printf("in searchmyll search fl %lld %d ret %d lut %s\n",lull, lutype,ret,lut);
     // if (ret==1) deletionll(&headfl,lut); this is not thread safe
   }
   //printf("in searchmyll %lld %d ret %d\n",lull, lutype,ret);
   return(ret);
}

static int create_tables(const char * name, sqlite3 * db, void * args) {
    if ((create_table_wrapper(name, db, "esql",        esql,        NULL, NULL) != SQLITE_OK) ||
        (create_table_wrapper(name, db, "ssql",        ssql,        NULL, NULL) != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vssqldir",    vssqldir,    NULL, NULL) != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vssqluser",   vssqluser,   NULL, NULL) != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vssqlgroup",  vssqlgroup,  NULL, NULL) != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vesql",       vesql,       NULL, NULL) != SQLITE_OK)) {
        return -1;
    }

    return 0;
}

static int create_readdirplus_tables(const char * name, sqlite3 * db, void * args) {
    if (create_table_wrapper(name, db, "rsql",         rsql,        NULL, NULL) != SQLITE_OK) {
        return -1;
    }

    return 0;
}

int reprocessdir(void * passv, DIR *dir)
{
    struct work *passmywork = passv;
    struct work qwork;
    //DIR *dir;
    struct dirent *entry = NULL;
    sqlite3 *db = NULL;
    char *records = NULL;
    char lpatho[MAXPATH];
    struct sum summary;
    sqlite3_stmt *res = NULL;
    char dbpath[MAXPATH];
    int transcnt;
    int loop;

    // rewind the directory
    rewinddir(dir);

    //printf(" in reprocessdir rebuilding gufi for %s\n",passmywork->name);

    /* need to fill this in for the directory as we dont need to do this unless we are making a new gufi db */
    passmywork->xattrs_len = 0;
    bzero(passmywork->xattrs,sizeof(passmywork->xattrs));
    bzero(passmywork->linkname,sizeof(passmywork->linkname));
    SNPRINTF(passmywork->type,2,"d");
    if (in.xattr.index > 0) {
      passmywork->xattrs_len = pullxattrs(passmywork->name,passmywork->xattrs, sizeof(passmywork->xattrs));
    }


    //open the gufi db for this directory into the parking lot directory the name as the inode of the dir
    SNPRINTF(dbpath,MAXPATH,"%s/%"STAT_ino"",in.nameto,passmywork->statuso.st_ino);
    if (in.buildinindir == 1) {
      SNPRINTF(dbpath,MAXPATH,"%s/%s",passmywork->name,DBNAME);
    } else {
        SNPRINTF(dbpath,MAXPATH,"%s/%"STAT_ino"",in.nameto,passmywork->statuso.st_ino);
    }

    /* if we are building a gufi in the src tree and the suspect mode is not zero then we need to wipe it out first */
    if (in.buildinindir == 1) {
        if (in.suspectmethod > 0) {
          //unlink(dbpath);
          truncate(dbpath,0);
        }
    }
    if (!(db = opendb(dbpath, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 1, 1
                      , create_tables, NULL
                      #if defined(DEBUG) && defined(PER_THREAD_STATS)
                      , NULL, NULL
                      , NULL, NULL
                      #endif
                      ))) {
       return -1;
    }
    res=insertdbprep(db);
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
        if (in.buildinindir == 1) {
          if (strcmp(entry->d_name, DBNAME) == 0)
               continue;
        }
        //printf("reprocessdir: dir %s entry %s\n",passmywork->name,entry->d_name);

        bzero(&qwork,sizeof(qwork));
        qwork.pinode=passmywork->statuso.st_ino;
        //if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
        //   continue;
        SNPRINTF(qwork.name,MAXPATH,"%s/%s", passmywork->name, entry->d_name);
        lstat(qwork.name, &qwork.statuso);
        /* qwork.xattrs_len = 0; */
        if (in.xattr.index > 0) {
          qwork.xattrs_len = pullxattrs(qwork.name,qwork.xattrs, sizeof(qwork.xattrs));
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
            SNPRINTF(qwork.linkname,MAXPATH,"%s",lpatho);
            SNPRINTF(qwork.type,2,"l");
            //sprintf(qwork.linkname,"%s/%s",passmywork->name,lpatho);
            sumit(&summary,&qwork);
            insertdbgo(&qwork,db,res);
            transcnt++;
            if (transcnt > 100000) {
              stopdb(db);
              startdb(db);
              transcnt=0;
            }
        } else if (S_ISREG(qwork.statuso.st_mode) ) {
            SNPRINTF(qwork.type,2,"%s","f");
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
    insertdbfin(res);

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
static int processdir(struct QPTPool * ctx, const size_t id, void * data, void * args)
{
    struct work *passmywork = data;
    struct work qwork;
    DIR *dir;
    struct dirent *entry;
    char dbpath[MAXPATH];
    char sortf[MAXPATH];
    int transcnt;
    int wentry;
    int todb;
    int tooutfile;
    int lookup;
    struct stat st;
    int rc;
    int locsuspecttime;
    struct stat sst;

    (void) args;

    // open directory
    if (!(dir = opendir(passmywork->name)))
       goto out_free; // return NULL;

    if (!(entry = readdir(dir)))
       goto out_dir; // return NULL;

    SNPRINTF(passmywork->type,2,"%s","d");
    passmywork->suspect=in.suspectd;
    /* if we are putting the gufi index into the source tree we can modify the suspecttime to be the mtime of the gufi db */
    /* this way we will just be looking at dirs or files that have changed since the gufi db was last updated */
    locsuspecttime=in.suspecttime;
    if (in.buildinindir == 1) {
      locsuspecttime=0;
      SNPRINTF(dbpath,MAXPATH,"%s/%s",passmywork->name,DBNAME);
      rc=lstat(dbpath,&sst);
      if (rc == 0) {
        locsuspecttime=sst.st_mtime;
      } else {
        passmywork->suspect=1;
      }
      //printf("in processdir setting suspecttime: set to %d rc from stat %d\n",locsuspecttime,rc);
    }

    /* if we are not looking for suspect directories we should just put the directory at the top of all the dirents */
    if (in.outdb > 0) {
         if (in.insertdir > 0) {
           if (in.suspectmethod == 0) {
             todb=id;
             if (in.stride > 0) {
               todb=(passmywork->statuso.st_ino/in.stride)%in.maxthreads; //striping inodes
               //******** start a lock
               pthread_mutex_lock(&outdb_mutex[todb]);
             }
             //printf("in processdirs in.outdb=%d in.insertdir=%d id=%d in.insertfl=%d\n",in.outdb,in.insertdir,id,in.insertfl);
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
             // needed to fill in passmywork status structure
             passmywork->statuso.st_ctime=0;
             passmywork->statuso.st_mtime=0;
             lstat(passmywork->name,&passmywork->statuso);
             if (passmywork->statuso.st_ctime >= locsuspecttime) passmywork->suspect=1;
             if (passmywork->statuso.st_mtime >= locsuspecttime) passmywork->suspect=1;
           }
         }
    }

    if (in.outfile > 0) {
      tooutfile=id;
      if (in.stride > 0) {
        tooutfile=(passmywork->statuso.st_ino/in.stride)%in.maxthreads; //striping inodes
        //******** start a lock
        pthread_mutex_lock(&outfile_mutex[todb]);
      }
      //fprintf(stderr,"threadd %d inode %lld file %d\n",id,passmywork->statuso.st_ino,tooutfile);
      /* only directories are here so sortf is set to the directory full pathname */
      SNPRINTF(sortf,MAXPATH,"%s",passmywork->name);
      fprintf(gts.outfd[tooutfile],"%s%s%"STAT_ino"%s%lld%s%s%s%s%s\n",passmywork->name,in.delim,passmywork->statuso.st_ino,in.delim,passmywork->pinode,in.delim,passmywork->type,in.delim,sortf,in.delim);
      if (in.stride > 0) {
        pthread_mutex_unlock(&outfile_mutex[todb]);
      }
    }
/*
    if (in.printing > 0 || in.printdir > 0) {
      printits(passmywork,id);
    }
*/
    // loop over dirents, if dir push it on the queue, if file or link
    // print it, fill up qwork structure for each
    transcnt=0;
    do {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
           continue;
        if (in.buildinindir == 1) {
          if (strcmp(entry->d_name, DBNAME) == 0)
               continue;
        }
        bzero(&qwork,sizeof(qwork));
        SNPRINTF(qwork.name,MAXPATH,"%s/%s", passmywork->name, entry->d_name);
        qwork.pinode=passmywork->statuso.st_ino;
        qwork.statuso.st_ino=entry->d_ino;
        qwork.suspect=in.suspectfl;
        wentry=0;
        //if (S_ISDIR(qwork.statuso.st_mode) ) {
        if (entry->d_type==DT_DIR) {
            if (!access(qwork.name, R_OK | X_OK)) {
                SNPRINTF(qwork.type,2,"d");
                struct work * work = malloc(sizeof(struct work));
                *work = qwork;
                QPTPool_enqueue(ctx, id, processdir, work);
            }
        } else if (entry->d_type==DT_LNK) {
            SNPRINTF(qwork.type,2,"%s","l");
            wentry=1;
        } else if (entry->d_type==DT_REG) {
            SNPRINTF(qwork.type,2,"%s","f");
            wentry=1;
        }
        if (wentry==1) {
/*
          if (in.printing > 0) {
            printits(&qwork,id);
          }
*/
          /* if suspect method is not zero then we can insert files and links, if not we dont care about files and links in db */
          if (in.suspectmethod == 0) {
            if (in.outdb > 0) {
              if (in.insertfl > 0) {
                todb=id;
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
               if (lookup == 1) {
                 passmywork->suspect=1;  /* set the directory suspect flag on so we will mark it in output */
                 //printf("######### found a file suspect %s %lld\n",qwork.name,qwork.statuso.st_ino);
               }
            }
            if (in.suspectmethod > 2) {
              /* ???? we would stat the file/link and if ctime or mtime is >= provided last run time mark dir suspect */
              st.st_ctime=0;
              st.st_mtime=0;
              lstat(qwork.name,&st);
              if (st.st_ctime >= locsuspecttime) passmywork->suspect=1;
              if (st.st_mtime >= locsuspecttime) passmywork->suspect=1;
            }
          }
          if (in.outfile > 0) {
            tooutfile=id;
            if (in.stride > 0) {
              tooutfile=(qwork.statuso.st_ino/in.stride)%in.maxthreads; //striping inodes
              //******** start a lock
              pthread_mutex_lock(&outfile_mutex[todb]);
            }
            //fprintf(stderr,"threadf %d inode %lld file %d\n",id,qwork.statuso.st_ino,tooutfile);
            /* since this is a file or link, we need the path to the file or link without the name as the sortf */
            SNPRINTF(sortf,MAXPATH,"%s",passmywork->name);
            fprintf(gts.outfd[tooutfile],"%s%s%"STAT_ino"%s%lld%s%s%s%s%s\n",qwork.name,in.delim,qwork.statuso.st_ino,in.delim,qwork.pinode,in.delim,qwork.type,in.delim,sortf,in.delim);
            if (in.stride > 0) {
              pthread_mutex_unlock(&outfile_mutex[todb]);
            }
          }
        }
    } while ((entry = (readdir(dir))));

    /* if we are not looking for suspect directories we should just put the directory at the top of all the dirents */
    if (in.suspectmethod > 0) {
      if (in.outdb > 0) {
         if (in.insertdir > 0) {
           todb=id;
           if (in.stride > 0) {
             todb=(passmywork->statuso.st_ino/in.stride)%in.maxthreads; //striping inodes
             //******** start a lock
             pthread_mutex_lock(&outdb_mutex[todb]);
           }
           startdb(gts.outdbd[todb]);
           //printf("in processdirs in.outdb=%d in.insertdir=%d id=%d in.insertfl=%d\n",in.outdb,in.insertdir,id,in.insertfl);
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
        todb=id;
        stopdb(gts.outdbd[todb]);
      }
    }

    if (passmywork->suspect==1) {
      if (gltodirmode==1) {
        /* we may not have stat on the directory we may be told its suspect somehow not stating it */
        lstat(passmywork->name,&passmywork->statuso);
        rc=reprocessdir(passmywork,dir);
        if (rc !=0) fprintf(stderr,"problem producing gufi db for suspect directory\n");
      }
    }

 out_dir:
    // close dir
    closedir(dir);

 out_free:

    // free the queue entry - this has to be here or there will be a leak
    free(passmywork);

    return 0;
}

int processinit(struct QPTPool * ctx) {

     struct work * mywork = malloc(sizeof(struct work));
     int i;
     char outdbn[MAXPATH];
     FILE *isf = NULL;
     char incsuspect[24];
     char incsuspecttype[2];
     long long int testll;
     long long int cntd;
     long long int cntfl;
     char outfn[MAXPATH];

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
           SNPRINTF(outdbn,MAXPATH,"%s.%d",in.outdbn,i);
           gts.outdbd[i]=opendb(outdbn, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 1, 1
                                , create_readdirplus_tables, NULL
                                #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                , NULL, NULL
                                , NULL, NULL
                                #endif
                                );
         global_res[i]=insertdbprepr(gts.outdbd[i]);
         if (in.stride > 0) {
           if (pthread_mutex_init(&outdb_mutex[i], NULL) != 0) {
             fprintf(stderr,"\n mutex %d init failed\n",i);
           }
         }
         i++;
       }
     }

     //open up the output files if needed
     if (in.outfile > 0) {
       i=0;
       while (i < in.maxthreads) {
         SNPRINTF(outfn,MAXPATH,"%s.%d",in.outfilen,i);
         //fprintf(stderr,"init opening %s.%d",in.outfilen,i);
         gts.outfd[i]=fopen(outfn,"w");
         if (in.stride > 0) {
           if (pthread_mutex_init(&outfile_mutex[i], NULL) != 0) {
             fprintf(stderr,"\n mutex %d init failed\n",i);
           }
         }
         i++;
       }
     }

     // process input directory and put it on the queue
     SNPRINTF(mywork->name,MAXPATH,"%s",in.name);
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
     QPTPool_enqueue(ctx, 0, processdir, mywork);

     return 0;
}

int processfin() {
int i;

     // close output dbs here
     if (in.outdb > 0) {
       i=0;
       while (i < in.maxthreads) {
         insertdbfin(global_res[i]);
         closedb(gts.outdbd[i]);
         if (in.stride > 0) {
           pthread_mutex_destroy(&outdb_mutex[i]);
         }
         i++;
       }
     }

     // close outputfiles
     if (in.outfile > 0) {
       i=0;
       while (i < in.maxthreads) {
         fclose(gts.outfd[i]);
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
   if (in.buildindex && in.nameto[0]) {
      fprintf(stderr, "In bfwreaddirplus2db building an index '-b' the index must go into the src tree\n");
      fprintf(stderr, "and -t means you are specifying a parking lot directory for gufi directory db's to be put under their znumber\n");
      fprintf(stderr, "for incremental operations by inode\n");
      return -1;
   }

   if (in.buildindex) {
     fprintf(stderr,"You are putting the index dbs in input directory\n");
     in.buildinindir = 1;
     SNPRINTF(in.nameto,MAXPATH,"%s",in.name);
   }
   return 0;

}

void sub_help() {
   printf("input_dir         walk this tree to produce GUFI index\n");
   printf("\n");
}

int main(int argc, char *argv[])
{
     struct stat st;
     int rc;

     // process input args - all programs share the common 'struct input',
     // but allow different fields to be filled at the command-line.
     // Callers provide the options-string for get_opt(), which will
     // control which options are parsed for each program.
     //fprintf(stderr,"in main beforeparse\n");
     int idx = parse_cmd_line(argc, argv, "hHn:O:ro:d:RYZW:g:A:c:xbt:", 1, "input_dir", &in);
     //fprintf(stderr,"in main right after parse\n");
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
     //fprintf(stderr,"in main after parse\n");
     if (validate_inputs())
        return -1;

     //fprintf(stderr,"in main after validate\n");
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

     if (in.buildinindir == 1) gltodirmode=1;

    struct QPTPool * pool = QPTPool_init(in.maxthreads
                                         #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                         , NULL
                                         #endif
        );
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
