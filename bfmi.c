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
struct mysqlinput {
char dbuser[50];
char dbpasswd[50];
char dbhost[50];
char dbdb[50];
char robinin[MAXPATH];
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
    sqlite3 *db1;
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

    sprintf(myinsql,"select name, type, n.id, n.parent_id, mode, uid, gid, size, last_access, last_mod, last_mdchange, blocks, nlink, replace(replace(replace(replace(replace(replace(replace(replace(n.id,\":\",\"\"),\"0x\",\"\"),\"a\",\"10\"),\"b\",\"11\"),\"c\",\"12\"),\"d\",\"13\"),\"e\",\"14\"),\"f\",\"15\") from entries e left join names n on n.id=e.id  where parent_id = \'%s\'",passmywork->pinodec);
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
        sprintf(ltchar,"%s",lrow[0]);  sprintf(qwork.name,"%s/%s", passmywork->name,ltchar);
        sprintf(ltchar,"%s",lrow[1]);  sprintf(qwork.type, "%1s", ltchar);
        sprintf(ltchar,"%s",lrow[2]);  /* this is char inode*/ sprintf(lpinodec,"%s",ltchar); /* the inode becomes the pinodec */
        sprintf(ltchar,"%s",lrow[3]);  /* this is the character pinode*/
        sprintf(ltchar,"%s",lrow[4]);  qwork.statuso.st_mode = atol(ltchar);
        sprintf(ltchar,"%s",lrow[5]);  lmypasswd = getpwnam(ltchar); qwork.statuso.st_uid = lmypasswd->pw_uid;
        sprintf(ltchar,"%s",lrow[6]);  lmygrp = getgrnam(ltchar); qwork.statuso.st_gid = lmygrp->gr_gid;
        sprintf(ltchar,"%s",lrow[7]);  qwork.statuso.st_size = atol(ltchar);
                                       qwork.statuso.st_blksize= 4096; /* dont have blocksize yet */
        sprintf(ltchar,"%s",lrow[8]);  qwork.statuso.st_atime = atol(ltchar);
        sprintf(ltchar,"%s",lrow[9]);  qwork.statuso.st_mtime = atol(ltchar);
        sprintf(ltchar,"%s",lrow[10]); qwork.statuso.st_ctime = atol(ltchar);
        sprintf(ltchar,"%s",lrow[11]); qwork.statuso.st_blocks= atoll(ltchar);
        sprintf(ltchar,"%s",lrow[12]); qwork.statuso.st_nlink = atol(ltchar);
        sprintf(ltchar,"%s",lrow[13]); qwork.statuso.st_ino = atoll(ltchar);
        bzero(qwork.linkname,sizeof(qwork.linkname)); /* dont have linkname yet */
        bzero(qwork.xattr,sizeof(qwork.xattr));
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
            sprintf(qwork.pinodec,"%s",lpinodec);
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

      sprintf(dbpath, "%s/%s/db.db", in.nameto,passmywork->name);
      chown(dbpath, passmywork->statuso.st_uid, passmywork->statuso.st_gid);
      chmod(dbpath, passmywork->statuso.st_mode | S_IRUSR);
      free(records);
    }

    // free the queue entry - this has to be here or there will be a leak
    free(passmywork->freeme);

    // one less thread running
    decrthread();

    // return NULL;
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
     sprintf(msn.robinin,"%s",v[12]);

     return 0;

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
         sprintf(outfn,"%s.%d",in.outfilen,i);
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
     bzero(mywork->xattr,sizeof(mywork->xattr));
     bzero(mywork->linkname,sizeof(mywork->linkname));
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

int main(int argc, char *argv[])
{
     //char nameo[MAXPATH];
     struct work mywork;
     int i;

     // process input args, this is not a common routine and will need to be different for each instance of a bf program
     processin(argc,argv);

     // start threads and loop watching threads needing work and queue size - this always stays in main right here
     mythpool = thpool_init(in.maxthreads);

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
