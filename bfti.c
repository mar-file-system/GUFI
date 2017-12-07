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
    int mytid;
    char *records; 
    struct sum summary;
    sqlite3_stmt *res;   
    sqlite3_stmt *reso;   
    char dbpath[MAXPATH];
    sqlite3 *db;
    sqlite3 *db1;
    struct sum sumin;
    int recs;

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

    // loop over dirents, if link push it on the queue, if file or link print it, fill up qwork structure for each
    do {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
           continue;
        bzero(&qwork,sizeof(qwork));
        sprintf(qwork.name,"%s/%s", passmywork->name, entry->d_name);
        qwork.pinode=passmywork->statuso.st_ino;
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

    zeroit(&sumin);
    db=opendb(passmywork->name,db1,3,0);
    querytsdb(passmywork->name,&sumin,db,&recs,0);
    tsumit(&sumin,&sumout);

    //printf("after tsumit %s minuid %d maxuid %d maxsize %d totfiles %d totsubdirs %d\n",mywork->name,sumout.minuid,sumout.maxuid,sumout.maxsize,sumout.totfiles,sumout.totsubdirs);
    closedb(db);

    // free the queue entry - this has to be here or there will be a leak
    free(passmywork->freeme);

    // close dir
    closedir(dir);

    // one less thread running
    decrthread();

    // return NULL;
}

int processinit(void * myworkin) {
    
     struct work * mywork = myworkin;

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

     pushdir(mywork);
     return 0;
}


int processfin() {

     sqlite3 *tdb;
     sqlite3 *tdb1;
     if (in.writetsum) {
       tdb = opendb(in.name,tdb1,3,1);
       inserttreesumdb(in.name,tdb,&sumout,0,0,0);
       closedb(tdb);
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


#if 0
int processin(int c, char *v[]) {

   char outfn[MAXPATH];
   int i;
   // this is where we process input variables

   // this is not how you should do this, it should be a case statement with edits etc.
   //printf("in %d 0 %s 1 %s\n",c, v[0],v[1]);
   sprintf(in.name,"%s",v[1]);
   in.printdir=atoi(v[2]);
   in.maxthreads = atoi(v[3]);
   in.writetsum = atoi(v[4]);

   return 0;
}
#endif


int validate_inputs() {
   if (! in.name[0]) {
      fprintf(stderr, "must supply source-dir '-i'\n");
      return -1;
   }

   return 0;
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
     if (processin(argc, argv, "hHi:Pn:s"))
         return -1;

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
