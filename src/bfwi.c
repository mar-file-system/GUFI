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


#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <float.h>
#include <pthread.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <time.h>
#include <unistd.h>
#include <utime.h>

#include <pwd.h>
#include <grp.h>

#include "bf.h"
#include "structq.h"
#include "utils.h"
#include "dbutils.h"

extern int errno;

/* used to allow all threads to open up the input file if there is one */
FILE *gin[MAXPTHREAD];

pthread_t gtid;
pthread_attr_t gattr;

// template database file variables
const char templatename[] = "tmp.db";
size_t templatesize = 0; // this is really a constant that is set at runtime

void parsetowork (char * inpdelim, char * inpline, void * inpwork ) {

    struct work *pinwork=inpwork;
    char *p;
    char *q;

    //printf("in parsetowork delim %s inpline %s\n",inpdelim,inpline);
    inpline[strlen(inpline)-1]= '\0';
    p=inpline; q=strstr(p,inpdelim); memset(q, 0, 1); SNPRINTF(pinwork->name,MAXPATH,"%s",p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); SNPRINTF(pinwork->type,2,"%s",p);
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
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); SNPRINTF(pinwork->linkname,MAXPATH,"%s",p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); SNPRINTF(pinwork->xattr,MAXXATTR,"%s",p);
    p=q+1;     q=strstr(p,inpdelim); memset(q, 0, 1); pinwork->crtime=atol(p);

}

double diff(struct timespec start, struct timespec end) {
    return (((end.tv_sec * 1e9) + end.tv_nsec) - ((start.tv_sec * 1e9) + start.tv_nsec)) / 1e9;
}

void *scout(void * param) {
    FILE *finfile;
    char linein[MAXPATH+MAXPATH+MAXPATH];
    long long int foffset;
    struct work mywork;

    /* struct timespec total_start, total_end; */
    /* struct timespec parse_start, parse_end; */
    /* struct timespec queue_start, queue_end; */
    /* double total = 0; */
    /* double parse = 0; */
    /* double queue = 0; */

    /* clock_gettime(CLOCK_MONOTONIC, &total_start); */

    /* mywork=malloc(sizeof(struct work)); */
    //printf("in scout arg %s\n",param);
    //incrthread(); /* add one thread so the others wait for the scout */
    finfile=fopen(in.name,"r");
    if (finfile == NULL) {
        fprintf(stderr,"Cant open up the input file %s\n",in.name);
        exit(-1); /* not the best way out i suppose */
    }
    //printf("reading input file now\n");
    //sleep(5);
    while (fgets (linein, sizeof(linein), finfile) != NULL) {

        /* clock_gettime(CLOCK_MONOTONIC, &parse_start); */

        //printf("got input line %s\n",linein);
        parsetowork (in.delim, linein, &mywork );
        /* clock_gettime(CLOCK_MONOTONIC, &parse_end); */
        /* parse += diff(parse_start, parse_end); */

        fgetpos(finfile,(fpos_t *) &foffset);

        /* clock_gettime(CLOCK_MONOTONIC, &queue_start); */
        if (!strncmp("d",mywork.type,1)) {
            mywork.pinode=0;
            mywork.offset=foffset;
            pushdir(&mywork);
        }
        /* clock_gettime(CLOCK_MONOTONIC, &queue_end); */
        /* queue += diff(queue_start, queue_end); */
    }
    fclose(finfile);
    //sleep(5);
    decrthread(); /* take one thread away so others can finish */
    /* clock_gettime(CLOCK_MONOTONIC, &total_end); */
    /* total = diff(total_start, total_end); */

    /* fprintf(stderr, "%f %f %f\n", total, parse, queue); */

    pthread_exit("scout finished");
}

#ifdef MEMORY
/* /\* */
/* ** This function is used to load the contents of a database file on disk */
/* ** into the "main" database of open database connection pInMemory, or */
/* ** to save the current contents of the database opened by pInMemory into */
/* ** a database file on disk. pInMemory is probably an in-memory database, */
/* ** but this function will also work fine if it is not. */
/* ** */
/* ** Parameter zFilename points to a nul-terminated string containing the */
/* ** name of the database file on disk to load from or save to. If parameter */
/* ** isSave is non-zero, then the contents of the file zFilename are */
/* ** overwritten with the contents of the database opened by pInMemory. If */
/* ** parameter isSave is zero, then the contents of the database opened by */
/* ** pInMemory are replaced by data loaded from the file zFilename. */
/* ** */
/* ** If the operation is successful, SQLITE_OK is returned. Otherwise, if */
/* ** an error occurs, an SQLite error code is returned. */
/* *\/ */
/* int loadOrSaveDb(sqlite3 *pInMemory, const char *zFilename, int isSave){ */
/*     int rc;                   /\* Function return code *\/ */
/*     sqlite3 *pFile;           /\* Database connection opened on zFilename *\/ */
/*     sqlite3_backup *pBackup;  /\* Backup object used to copy data *\/ */
/*     sqlite3 *pTo;             /\* Database to copy to (pFile or pInMemory) *\/ */
/*     sqlite3 *pFrom;           /\* Database to copy from (pFile or pInMemory) *\/ */

/*     /\* Open the database file identified by zFilename. Exit early if this fails */
/*     ** for any reason. *\/ */
/*     rc = sqlite3_open(zFilename, &pFile); */
/*     if( rc==SQLITE_OK ){ */

/*         /\* If this is a 'load' operation (isSave==0), then data is copied */
/*         ** from the database file just opened to database pInMemory. */
/*         ** Otherwise, if this is a 'save' operation (isSave==1), then data */
/*         ** is copied from pInMemory to pFile.  Set the variables pFrom and */
/*         ** pTo accordingly. *\/ */
/*         pFrom = (isSave ? pInMemory : pFile); */
/*         pTo   = (isSave ? pFile     : pInMemory); */

/*         /\* Set up the backup procedure to copy from the "main" database of */
/*         ** connection pFile to the main database of connection pInMemory. */
/*         ** If something goes wrong, pBackup will be set to NULL and an error */
/*         ** code and message left in connection pTo. */
/*         ** */
/*         ** If the backup object is successfully created, call backup_step() */
/*         ** to copy data from pFile to pInMemory. Then call backup_finish() */
/*         ** to release resources associated with the pBackup object.  If an */
/*         ** error occurred, then an error code and message will be left in */
/*         ** connection pTo. If no error occurred, then the error code belonging */
/*         ** to pTo is set to SQLITE_OK. */
/*         *\/ */
/*         pBackup = sqlite3_backup_init(pTo, "main", pFrom, "main"); */
/*         if( pBackup ){ */
/*             (void)sqlite3_backup_step(pBackup, -1); */
/*             (void)sqlite3_backup_finish(pBackup); */
/*         } */
/*         rc = sqlite3_errcode(pTo); */
/*     } */

/*     /\* Close the database connection opened on database file zFilename */
/*     ** and return the result of this function. *\/ */
/*     (void)sqlite3_close(pFile); */
/*     return rc; */
/* } */
#endif

/* struct stats { */
/*     double total; */
/*     double open; */
/*     double table; */
/*     double insert; */
/* }; */

/* struct minmax { */
/*     struct stats min; */
/*     struct stats max; */
/* }; */

/* struct minmax minmaxstats[MAXPTHREAD]; */

// This becomes an argument to thpool_add_work(), so it must return void,
// instead of void*.
size_t active_threads = 0;
pthread_mutex_t active_threads_mutex = PTHREAD_MUTEX_INITIALIZER;

static void processdir(void * passv)
{
    /* pthread_mutex_lock(&active_threads_mutex); */
    /* active_threads++; */
    /* fprintf(stderr, "%zu\n", active_threads); */
    /* pthread_mutex_unlock(&active_threads_mutex); */

    struct work *passmywork = passv;
    struct work qwork;
    DIR *dir;
    struct dirent *entry;
    char lpatho[MAXPATH];
    int mytid;
    sqlite3 *db = NULL;
    struct sum summary;
    sqlite3_stmt *res = NULL;
    char dbpath[MAXPATH];
    int transcnt;
    char plinein[MAXPATH+MAXPATH+MAXPATH];
    /* int dupdirectory = 0; */

    /* struct timespec total_start, total_end; */
    /* struct timespec opendb_start, opendb_end; */
    /* struct timespec table_start, table_end; */
    /* struct timespec insert_start, insert_end; */
    /* /\* struct timespec dump_start, dump_end; *\/ */
    /* double opendb_time = 0; */
    /* double table_time = 0; */
    /* /\* double dump_time = 0; *\/ */
    /* clock_gettime(CLOCK_MONOTONIC, &total_start); */

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
        fsetpos(gin[mytid], (fpos_t *) &passmywork->offset);
      //fgetpos(gin[mytid],&pos);
      //printf("position set for tid %d position %lld\n",mytid,pos);
    }

    SNPRINTF(passmywork->type,2,"%s","d");
    if (in.printing > 0 || in.printdir > 0) {
      printits(passmywork,mytid);
    }
    //printf("tid %d make dir %s\n",mytid,passmywork->name);

    if (in.buildindex > 0) {
       if (in.buildinindir == 0) {
         dupdir(passmywork);
         //printf("tid %d made dir %s\n",mytid,passmywork->name);
       }
       zeroit(&summary);

       char dbname[MAXPATH];
       sqlite3_snprintf(MAXSQL, dbname, "%s/%s/%s", in.nameto, passmywork->name, DBNAME);

       // ignore errors here
       const int template = open(templatename, O_RDONLY);
       const int new_db = open(dbname, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRWXG | S_IWGRP | S_IROTH | S_IROTH);
       const ssize_t sf = sendfile(new_db, template, NULL, templatesize);
       lseek(new_db, 0, SEEK_SET);
       close(template);
       close(new_db);

       if (sf == -1) {
           fprintf(stderr, "Could not copy template file\n");
           goto out_dir;
       }

       /* clock_gettime(CLOCK_MONOTONIC, &opendb_start); */
       if (!(db = opendb(dbname,4,0))) {
       /* if (!(db = opendb(":memory:",4,0))) { */
          goto out_dir;
       }
       /* clock_gettime(CLOCK_MONOTONIC, &opendb_end); */
       /* opendb_time += diff(opendb_start, opendb_end); */

       /* // create tables separately to detect when it fails (database already exists) */
       /* dupdirectory = (create_tables(passmywork->name, 4, db) != 0); */
       /* clock_gettime(CLOCK_MONOTONIC, &table_start); */
       /* create_tables(passmywork->name, 4, db); */
       /* create_tables(":memory:", 4, db); */
       /* clock_gettime(CLOCK_MONOTONIC, &table_end); */
       /* table_time += diff(table_start, table_end); */

       res=insertdbprep(db,NULL);
       startdb(db);
    }

    //printf("tid %d made db\n",mytid);
    // loop over dirents, if link push it on the queue, if file or link
    // print it, fill up qwork structure for each
    /* clock_gettime(CLOCK_MONOTONIC, &insert_start); */
    transcnt = 0;
    //do {
    while (1) {
        if (in.infile == 0) {
          /* get the next dirent */
          if (!(entry = readdir(dir))) break;
        } else {
          /* get the next record */
          memset(plinein, 0, sizeof(plinein));
          if (fgets (plinein, sizeof(plinein), gin[mytid]) == NULL) break;
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
          SNPRINTF(qwork.name,MAXPATH,"%s/%s", passmywork->name, entry->d_name);
          lstat(qwork.name, &qwork.statuso);
          qwork.xattrs=0;
          if (in.doxattrs > 0) {
            memset(qwork.xattr,0,sizeof(qwork.xattr));
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
              break;
            }
        } else if (S_ISLNK(qwork.statuso.st_mode) ) {
            // its a link so get the linkname
            if (in.infile == 0) {
              /* if its infile we have to get this elsewhere */
              memset(lpatho,0,sizeof(lpatho));
              readlink(qwork.name,lpatho,MAXPATH);
              //sprintf(qwork.linkname,"%s/%s",passmywork->name,lpatho);
              SNPRINTF(qwork.linkname,MAXPATH,"%s",lpatho);
            }
            SNPRINTF(qwork.type,2,"%s","l");
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
            SNPRINTF(qwork.type,2,"%s","f");
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
    /* clock_gettime(CLOCK_MONOTONIC, &insert_end); */

    if (in.buildindex > 0) {
      stopdb(db);
      insertdbfin(db,res);

      // this i believe has to be after we close off the entries transaction
      insertsumdb(db,passmywork,&summary);

      SNPRINTF(dbpath, MAXPATH, "%s/%s/" DBNAME, in.nameto,passmywork->name);

      #ifdef MEMORY
      /* clock_gettime(CLOCK_MONOTONIC, &dump_start); */
      /* if (loadOrSaveDb(db, dbpath, 1) != SQLITE_OK) { */
      /*     fprintf(stderr, "Error writing to %s\n", dbpath); */
      /* } */
      /* clock_gettime(CLOCK_MONOTONIC, &dump_end); */
      #endif
      closedb(db);

      chown(dbpath, passmywork->statuso.st_uid, passmywork->statuso.st_gid);
      chmod(dbpath, passmywork->statuso.st_mode | S_IRUSR);
    }

 out_dir:
    if (in.infile == 0) {
      // close dir
      closedir(dir);
    }

 out_free:
    // one less thread running
    decrthread();

    // free the queue entry - this has to be here or there will be a leak
    free(passmywork->freeme);
    /* clock_gettime(CLOCK_MONOTONIC, &total_end); */

    /* double total_time = diff(total_start, total_end); */
    /* double insert_time = diff(insert_start, insert_end) / transcnt; */

    /* if (total_time > minmaxstats[mytid].max.total) { */
    /*     minmaxstats[mytid].max.total = total_time; */
    /* } */

    /* if (opendb_time > minmaxstats[mytid].max.open) { */
    /*     minmaxstats[mytid].max.open = opendb_time; */
    /* } */

    /* if (table_time > minmaxstats[mytid].max.table) { */
    /*     minmaxstats[mytid].max.table = table_time; */
    /* } */

    /* if (transcnt) { */
    /*     if (insert_time > minmaxstats[mytid].max.insert) { */
    /*         minmaxstats[mytid].max.insert = insert_time; */
    /*     } */
    /* } */

    /* if (total_time < minmaxstats[mytid].min.total) { */
    /*     minmaxstats[mytid].min.total = total_time; */
    /* } */

    /* if (opendb_time < minmaxstats[mytid].min.open) { */
    /*     minmaxstats[mytid].min.open = opendb_time; */
    /* } */

    /* if (table_time < minmaxstats[mytid].min.table) { */
    /*     minmaxstats[mytid].min.table = table_time; */
    /* } */

    /* if (transcnt) { */
    /*     if (insert_time < minmaxstats[mytid].min.insert) { */
    /*         minmaxstats[mytid].min.insert = insert_time; */
    /*     } */
    /* } */

    /* fprintf(stdout, "%f, %f, %f, %f\n", total_time, opendb_time, table_time, insert_time); */
    /// return NULL;

    /* pthread_mutex_lock(&active_threads_mutex); */
    /* active_threads--; */
    /* fprintf(stderr, "%zu\n",active_threads); */
    /* pthread_mutex_unlock(&active_threads_mutex); */
}



int processinit(void * myworkin) {

     struct work * mywork = myworkin;
     int i;
     char outfn[MAXPATH];
     FILE *finfile = NULL;

     //open up the output files if needed
     if (in.outfile > 0) {
       i=0;
       while (i < in.maxthreads) {
         SNPRINTF(outfn,MAXPATH,"%s.%d",in.outfilen,i);
         gts.outfd[i]=fopen(outfn,"w");
         i++;
       }
     }

     //open up the input file if needed
     if (in.infile > 0) {
       finfile=fopen(in.name,"r");
       if (finfile == NULL) {
         fprintf(stderr,"Cant open up the input file %s\n",in.name);
         exit(-1); /* not the best way out i suppose */
       }
       i=0;
       /* memset(&minmaxstats, 0, sizeof(minmaxstats)); */
       while (i < in.maxthreads) {
         gin[i]=fopen(in.name,"r");
         /* minmaxstats[i].min.total  =  DBL_MAX; */
         /* minmaxstats[i].min.open   =  DBL_MAX; */
         /* minmaxstats[i].min.table  =  DBL_MAX; */
         /* minmaxstats[i].min.insert =  DBL_MAX; */
         /* minmaxstats[i].max.total   = -DBL_MAX; */
         /* minmaxstats[i].max.open    = -DBL_MAX; */
         /* minmaxstats[i].max.table   = -DBL_MAX; */
         /* minmaxstats[i].max.insert  = -DBL_MAX; */

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
       memset(mywork->xattr,0,sizeof(mywork->xattr));
       memset(mywork->linkname,0,sizeof(mywork->linkname));
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

     /* struct minmax final; */
     /* final.min.total  =  DBL_MAX; */
     /* final.min.open   =  DBL_MAX; */
     /* final.min.table  =  DBL_MAX; */
     /* final.min.insert =  DBL_MAX; */
     /* final.max.total   = -DBL_MAX; */
     /* final.max.open    = -DBL_MAX; */
     /* final.max.table   = -DBL_MAX; */
     /* final.max.insert  = -DBL_MAX; */

     /* printf("Total Min, Total Max, Open Min, Open Max, Table Min, Table Max, Insert Min, Insert Max\n"); */
     /* for(i = 0; i < in.maxthreads; i++) { */
     /*     printf("%f, %f, %f, %f, %f, %f, %f, %f\n", */
     /*            minmaxstats[i].min.total,  minmaxstats[i].max.total, */
     /*            minmaxstats[i].min.open,   minmaxstats[i].max.open, */
     /*            minmaxstats[i].min.table,  minmaxstats[i].max.table, */
     /*            minmaxstats[i].min.insert, minmaxstats[i].max.insert); */

     /*     if (minmaxstats[i].max.total > final.max.total) { */
     /*         final.max.total = minmaxstats[i].max.total; */
     /*     } */

     /*     if (minmaxstats[i].max.open > final.max.open) { */
     /*         final.max.open = minmaxstats[i].max.open; */
     /*     } */

     /*     if (minmaxstats[i].max.table > final.max.table) { */
     /*         final.max.table = minmaxstats[i].max.table; */
     /*     } */

     /*     if (minmaxstats[i].max.insert > final.max.insert) { */
     /*         final.max.insert = minmaxstats[i].max.insert; */
     /*     } */

     /*     if (minmaxstats[i].min.total < final.min.total) { */
     /*         final.min.total = minmaxstats[i].min.total; */
     /*     } */

     /*     if (minmaxstats[i].min.open < final.min.open) { */
     /*         final.min.open = minmaxstats[i].min.open; */
     /*     } */

     /*     if (minmaxstats[i].min.table < final.min.table) { */
     /*         final.min.table = minmaxstats[i].min.table; */
     /*     } */

     /*     if (minmaxstats[i].min.insert < final.min.insert) { */
     /*         final.min.insert = minmaxstats[i].min.insert; */
     /*     } */
     /* } */

     /* printf("\n%f, %f, %f, %f, %f, %f, %f, %f\n", */
     /*        final.min.total,  final.max.total, */
     /*        final.min.open,   final.max.open, */
     /*        final.min.table,  final.max.table, */
     /*        final.min.insert, final.max.insert); */
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

   SNPRINTF(expathtst,MAXPATH,"%s/%s",in.nameto,in.name);
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

     // create the initial database file to copy from
     sqlite3 * templatedb = NULL;
     if (sqlite3_open_v2(templatename, &templatedb, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
         fprintf(stderr, "Cannot open database: %s %s rc %d\n", templatename, sqlite3_errmsg(templatedb), sqlite3_errcode(templatedb));
         return 1;
     }
     create_tables(templatename, 4, templatedb);
     closedb(templatedb);
     int templatefd = -1;
     if ((templatefd = open(templatename, O_RDONLY)) == -1) {
         fprintf(stderr, "Could not open template file\n");
         return 1;
     }

     templatesize = lseek(templatefd, 0, SEEK_END);
     close(templatefd);
     if (templatesize == (off_t) -1) {
         fprintf(stderr, "failed to lseek\n");
         return 1;
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
     //sleep(5);
     processdirs(processdir);

     // processfin - this is work done after the threads are done working
     // before they are taken down - this will be different for each
     // instance of a bf program
     processfin();

     // clean up threads and exit
     thpool_wait(mythpool);
     thpool_destroy(mythpool);

     // remove the template file
     remove(templatename);
     return 0;
}
