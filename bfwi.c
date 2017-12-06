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
#include <uuid/uuid.h>

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

    // get thread id so we can get access to thread state we need to keep until the thread ends
    mytid=0;
    if (in.outfile > 0) mytid=gettid();

    // open directory
    if (!(dir = opendir(passmywork->name)))
        return; // return NULL;
    if (!(entry = readdir(dir)))
        return; // return NULL;
    sprintf(passmywork->type,"%s","d");
    // print?
    if (in.printing > 0 || in.printdir > 0) {
      printits(passmywork,mytid);
    }

    if (in.buildindex > 0) {
       dupdir(passmywork);
       records=malloc(MAXRECS);
       bzero(records,MAXRECS);
       //sqlite3 *  opendb(const char *name, sqlite3 *db, int openwhat, int createtables)
       zeroit(&summary);
       db = opendb(passmywork->name,db1,4,1);
       res=insertdbprep(db,reso);
    //printf("inbfilistdir res %d\n",res);
    startdb(db);

    }

    // loop over dirents, if link push it on the queue, if file or link print it, fill up qwork structure for each
    transcnt = 0;
    do {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
           continue;
        bzero(&qwork,sizeof(qwork));
        sprintf(qwork.name,"%s/%s", passmywork->name, entry->d_name);
        qwork.pinode=passmywork->statuso.st_ino;
        lstat(qwork.name, &qwork.statuso);
        qwork.xattrs=0;
        if (in.doxattrs > 0) {
          qwork.xattrs=pullxattrs(qwork.name,qwork.xattr);
        }
        if (S_ISDIR(qwork.statuso.st_mode) ) {
            if (!access(qwork.name, R_OK | X_OK)) 
                // this is how the parent gets passed on
                qwork.pinode=passmywork->statuso.st_ino;
                // this pushes the dir onto queue - pushdir does locking around queue update
                pushdir(&qwork);
        } else if (S_ISLNK(qwork.statuso.st_mode) ) {
            // its a link so get the linkname
            bzero(lpatho,sizeof(lpatho));
            readlink(qwork.name,lpatho,MAXPATH);
            sprintf(qwork.linkname,"%s/%s",passmywork->name,lpatho);
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
    } while ((entry = (readdir(dir))));

    // free the queue entry - this has to be here or there will be a leak
    //free(passmywork->freeme);

    if (in.buildindex > 0) {
      stopdb(db);
      insertdbfin(db,res);

      // this i believe has to be after we close off the entries transaction 
      insertsumdb(db,passmywork,&summary);
      closedb(db);

      sprintf(dbpath, "%s/%s/db.db", in.nameto,passmywork->name);
      chown(dbpath, passmywork->statuso.st_uid, passmywork->statuso.st_gid);
      chmod(dbpath, passmywork->statuso.st_mode | S_IRUSR);
      free(records);
    }

    // free the queue entry - this has to be here or there will be a leak
    free(passmywork->freeme);

    // close dir
    closedir(dir);

    // one less thread running
    decrthread();

    /// return NULL;
}

int processin(int c, char *v[]) {

     char outfn[MAXPATH];
     int i;
     // this is where we process input variables

     // this is not how you should do this, it should be a case statement with edits etc.
     //printf("in %d 0 %s 1 %s\n",c, v[0],v[1]);
     sprintf(in.name,"%s",v[1]);
     in.printing = atoi(v[2]);
     in.maxthreads = atoi(v[3]);
     in.dodelim=atoi(v[4]);
     sprintf(in.delim,"%s",v[5]);
     in.doxattrs=atoi(v[6]);
     in.printdir=atoi(v[7]);
     in.buildindex=atoi(v[8]);
     sprintf(in.nameto,"%s",v[9]);
     in.outfile=atoi(v[10]);
     sprintf(in.outfilen,"%s",v[11]);

     return 0;

}

int processinit(void * myworkin) {
    
     struct work * mywork = myworkin;
     int i;
     char outfn[MAXPATH];

     //open up the output files if needed
     if (in.outfile > 0) {
       i=0;
       while (i < in.maxthreads) {
         sprintf(outfn,"%s.%d",in.outfilen,i);
         gts.outfd[i]=fopen(outfn,"w");
         i++;
       }
     }

     // process input directory and put it on the queue
     sprintf(mywork->name,"%s",in.name);
     lstat(in.name,&mywork->statuso);
     if (access(in.name, R_OK | X_OK)) {
         perror("couldn't access input dir");
         return 1;
     }
     if (!S_ISDIR(mywork->statuso.st_mode) ) {
         fprintf(stderr,"not a directory as input\n");
         return 1;
     }
     if (in.doxattrs > 0) {
       mywork->xattrs=0;
       mywork->xattrs=pullxattrs(in.name,mywork->xattr);
     }
     // set top parent inode to zero
     mywork->pinode=0;
     pushdir(mywork);
 
     return 0;
}


int processfin() {
     int i;
  
     if (in.outfile > 0) {
       i=0;
       while (i < in.maxthreads) {
         fclose(gts.outfd[i]);
         i++;
       }
     }
     return 0;
}

int main(int argc, char *argv[])
{
     //char nameo[MAXPATH];
     struct work mywork;
     int i;

     // process input args, this is not a common routine and will need to be different for each instance of a bf program
     processin(argc,argv);

     // start threads and loop watching threads needing work and queue size - this always stays in main right here
     mythpool = thpool_init(in.maxthreads);
     if (thpool_null(mythpool)) {
        fprintf(stderr, "thpool_init() failed!\n");
        return -1;
     }

     // process initialization, this is work done once the threads are up but not busy yet - this will be different for each instance of a bf program
     // in this case we are stating the directory passed in and putting that directory on the queue
     processinit(&mywork);

     // processdirs - if done properly, this routine is common and does not have to be done per instance of a bf program
     // loops through and processes all directories that enter the queue by farming the work out to the threadpool
     processdirs(processdir);

     // processfin - this is work done after the threads are done working before they are taken down - this will be different for each instance of a bf program
     processfin();

     // clean up threads and exit
     thpool_wait(mythpool);
     thpool_destroy(mythpool);
     return 0;
}
