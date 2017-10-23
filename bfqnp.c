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

#define MAXPATH 1024
#define MAXXATTR 1024
#define MAXSQL 1024
#include "bf.h"
#include "structq.c"
#include "dbutils.c"
#include "utils.c"

volatile int runningthreads = 0;
pthread_mutex_t running_mutex = PTHREAD_MUTEX_INITIALIZER;
volatile int queuelock = 0;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
volatile int startlock = 0;
pthread_mutex_t start_mutex = PTHREAD_MUTEX_INITIALIZER;

//void listdir(const char *name, char *nameto, char *sqlsum, char * sqlent, struct stat *status,int printdir,int andor,int printing,struct work * mywork)
//void listdir(struct work * mywork)
static void * listdir(void * passv)
{
    struct work *passmywork = passv;;
    struct work *mywork;
    DIR *dir;
    struct dirent *entry;
    int len;
    char path[MAXPATH];
    char lpath[MAXPATH];
    char lpatho[MAXPATH];
    char tolpath[MAXPATH];
    char xattr[MAXXATTR];
    int xattrs = 0;;
    char *key;
    struct stat statuso;
    struct stat stato;
    sqlite3 *db;
    sqlite3 *sdb;
    sqlite3 *tsdb;
    sqlite3 *db1;
    sqlite3 *db2;
    sqlite3 *db3;
    char type[2]; 
    int recs;
    int issummary;
    struct work rmywork;
    long long pinode;
    long long dpinode;
    long long fpinode;
    FILE *pinodefd;
    char pinodepath[MAXPATH];
    char treesum[MAXPATH];
    char treesuml[MAXPATH];
    struct stat treesums;

    sprintf(rmywork.name,"%s",passmywork->name);
    sprintf(rmywork.nameto,"%s",passmywork->nameto);
    sprintf(rmywork.sqlsum,"%s",passmywork->sqlsum);
    sprintf(rmywork.sqlent,"%s",passmywork->sqlent);
    rmywork.printdir=passmywork->printdir;
    rmywork.andor=passmywork->andor;
    rmywork.printing=passmywork->printing;
    rmywork.pinodeplace=passmywork->pinodeplace;
    rmywork.statuso.st_ino=passmywork->statuso.st_ino;;
    stato.st_ino=passmywork->statuso.st_ino;;
    rmywork.statuso.st_mode=passmywork->statuso.st_mode;
    rmywork.statuso.st_nlink=passmywork->statuso.st_nlink;
    rmywork.statuso.st_uid=passmywork->statuso.st_uid;
    rmywork.statuso.st_gid=passmywork->statuso.st_gid;
    rmywork.statuso.st_size=passmywork->statuso.st_size;
    rmywork.statuso.st_blksize=passmywork->statuso.st_blksize;
    rmywork.statuso.st_blocks=passmywork->statuso.st_blocks;
    rmywork.statuso.st_atime=passmywork->statuso.st_atime;
    rmywork.statuso.st_mtime=passmywork->statuso.st_mtime;
    rmywork.statuso.st_ctime=passmywork->statuso.st_ctime;
    pinode=passmywork->pinode;
    mywork=&rmywork; 
    //printf("copying input in listdir\n");
    pthread_mutex_lock(&start_mutex);
    startlock = 0;
    pthread_mutex_unlock(&start_mutex);

    //printf("in listdir mywork.sqlent = %s\n",mywork->sqlent);
    //printf("dir: ");
    /* this is the sql statement for the treesum db */
    /* if it exists then run the query if the db exists */
    /* if it returns a row then go on, if not bail now as nothing below is interesting*/
    sprintf(type,"%s","d");
    if (strlen(rmywork.nameto) > 1) {
      sprintf(treesum,"%s",mywork->name);
      sprintf(treesuml,"%s/treesummary.db",mywork->name);
      //printf("checking for treesum %s\n",treesuml);
      if (lstat(treesuml,&treesums) == 0) {
        //printf("checking for treesum found %s\n",treesuml);
        tsdb = opendb(mywork->name,mywork->nameto,&treesums,db3,2,0);
        recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
                /* if it fails then this will be set to 1 and will go on */
        issummary=1;
        querydb(mywork->name,&mywork->statuso,tsdb,type,lpath,xattrs,xattr,mywork->nameto,&recs,0,issummary,rmywork.pinodeplace,dpinode,0);
        //printf("-tsum------ recs %d \n",recs);
        closedb(tsdb);
        if (recs < 1) {
          pthread_mutex_lock(&running_mutex);
          runningthreads--;
          pthread_mutex_unlock(&running_mutex);
          return NULL;
        } 
      } 
    } 

    sprintf(type,"%s","d");
    bzero(lpath,sizeof(lpath));
    if (!(dir = opendir(mywork->name))) {
        printf("cant open directory %s\n",mywork->name);
        return NULL;
    } 
    //if (!(entry = readdir(dir)))
    //    return;
    //printf("in listdir printing %d\n",mywork->printing);
    xattrs=0;
    if (mywork->printing) {
      bzero(xattr,sizeof(xattr));
      xattrs=pullxattrs(mywork->name,xattr);
      //if (xattrs>0) printf("pullxattr returns %d\n", xattrs);
      printit(mywork->name,&mywork->statuso,type,lpath,xattrs,xattr,mywork->printing,pinode);
    }
    db = opendb(mywork->name,mywork->nameto,&mywork->statuso,db1,1,0);
    sdb = opendb(mywork->name,mywork->nameto,&mywork->statuso,db2,0,0);

    sprintf(pinodepath,"%s/pinode",mywork->name);
    //printf("%s\n",pinodepath);
    pinodefd=fopen(pinodepath,"r");
    fscanf(pinodefd,"%lld %lld",&dpinode,&fpinode);
    //printf("got %lld %lld\n",dpinode,fpinode);
    fclose(pinodefd);

    /*????? there would obviously be more */
    recs=1;
    if (strlen(mywork->sqlsum) > 1) {
      recs=0;
      issummary=1;
      querydb(mywork->name,&mywork->statuso,sdb,type,lpath,xattrs,xattr,mywork->sqlsum,&recs,mywork->printdir,issummary,rmywork.pinodeplace,dpinode,0);
    }
    //printf("------- recs %d \n",recs);
    if (mywork->andor > 0) recs=1;
    if (strlen(mywork->sqlent) > 1) {
      if (recs > 0) {
        issummary=0;
        //printf("calling querydb printdir %d issummary %d pinodeplace %d pinode %lld\n",mywork->printdir, issummary,rmywork.pinodeplace,fpinode);
        sprintf(type,"%s","?");
        querydb(mywork->name,&mywork->statuso,db,type,lpath,xattrs,xattr,mywork->sqlent,&recs,mywork->printdir ,issummary,rmywork.pinodeplace,fpinode,0);
      }
    }

     while ((entry = (readdir(dir)))) {
     //do {
        len = snprintf(path, sizeof(path)-1, "%s/%s",mywork->name, entry->d_name);
        lstat(path, &statuso);
        //printf("in readdirloop found %s/%s type %d\n",mywork->name,entry->d_name,statuso.st_mode);
        //if (entry->d_type == DT_DIR) {
        if (S_ISDIR(statuso.st_mode) ) {
            //printf("in readdirloop found dir %s/%s\n",mywork->name,entry->d_name);
            //len = snprintf(path, sizeof(path)-1, "%s/%s",mywork->name, entry->d_name);
            //path[len] = 0;
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;
            /* no need to queue this if we cant read/stat the dir/dirents */
            //stat(path, &statuso);
            /*?????? if you thread this you need to mutex around push *****/
            if (!access(path, R_OK | X_OK)) {
                //printf("queueing %s inode %lld\n",path,statuso.st_ino);
                pthread_mutex_lock(&queue_mutex);
                //printf("tid %u queueing pushing path %s statusino %lld\n",pthread_self(),path,statuso.st_ino);
                //printf("before calling pthread_create getpid: %d getpthread_self: %lu tid:%lu\n",getpid(), pthread_self(), syscall(SYS_gettid));
                push(path,&statuso,stato.st_ino);
                pthread_mutex_unlock(&queue_mutex);
            }
        }
    }
    //} while ((entry = (readdir(dir))));
    closedir(dir);
    closedb(db);
    closedb(sdb);
    pthread_mutex_lock(&running_mutex);
    runningthreads--;
    pthread_mutex_unlock(&running_mutex);
    return NULL;
}
 
int main(int argc, char *argv[])
{
     struct work mywork;
     struct work *pmywork;
     pthread_t thread;
     pthread_attr_t attr;
     int rc;
     int startone;
     int myqent;
     int myrunningthreads;
     int maxthreads;

     qent=0;
     front=NULL;
     sprintf(mywork.name,"%s",argv[1]);
     sprintf(mywork.nameto,"%s",argv[2]);
     sprintf(mywork.sqlsum,"%s",argv[3]);
     sprintf(mywork.sqlent,"%s",argv[4]);
     mywork.printdir=atoi(argv[5]);
     mywork.andor=atoi(argv[6]);
     mywork.printing = atoi(argv[7]);
     maxthreads = atoi(argv[8]);
     mywork.pinodeplace=atoi(argv[9]);
//     printf("dirname %s sqltsum %s sqlent %s sqlsum %s printdir %d andor %d printing %d maxthreads %d printparentinodeplace %d\n",mywork.name, mywork.nameto, mywork.sqlent, mywork.sqlsum ,mywork.printdir,mywork.andor,mywork.printing,maxthreads,mywork.pinodeplace);
     mywork.pinode=0;
     /* process input directory */
     lstat(mywork.name,&mywork.statuso);
     if (!access(mywork.name, R_OK | X_OK)) {
         pmywork=&mywork;
         //listdir(&mywork);
         runningthreads++;
         listdir(pmywork);
     } else {
         return 1;
     }

     myqent=0;
     myrunningthreads=0;
     while (1) {
         /*????????? if you thread this you need to clean up this pulling from queue addrcurrents and delQueue */
         //sleep(1);
         startone=1;
         pthread_mutex_lock(&queue_mutex);
         pthread_mutex_lock(&running_mutex);
         myqent=addrqent();
         myrunningthreads=runningthreads;;
         //printf("topwhile myqent %d myrunningthreads %d\n",myqent,myrunningthreads);
         pthread_mutex_unlock(&queue_mutex);
         pthread_mutex_unlock(&running_mutex);

         if (myrunningthreads == 0) {
           if (myqent == 0) {
             //printf("no threads no qent\n");            
             startone=0;
             break;
           }
         }
         if (myrunningthreads > maxthreads) {
            //printf("threads > %d\n",maxthreads);            
            startone=0;
         }
         if (myqent == 0) {
             //printf("threads avail qent %d\n",addrqent());            
             startone=0;
         }
         if (startone) {
         pthread_mutex_lock(&queue_mutex);
         //printf("about to addrcurrent %d\n",addrqent());
         sprintf(mywork.name,"%s",addrcurrent());
         mywork.statuso.st_ino=addrcurrents()->st_ino;;
         mywork.statuso.st_mode=addrcurrents()->st_mode;
         mywork.statuso.st_nlink=addrcurrents()->st_nlink;
         mywork.statuso.st_uid=addrcurrents()->st_uid;
         mywork.statuso.st_gid=addrcurrents()->st_gid;
         mywork.statuso.st_size=addrcurrents()->st_size;
         mywork.statuso.st_blksize=addrcurrents()->st_blksize;
         mywork.statuso.st_blocks=addrcurrents()->st_blocks;
         mywork.statuso.st_atime=addrcurrents()->st_atime;
         mywork.statuso.st_mtime=addrcurrents()->st_mtime;
         mywork.statuso.st_ctime=addrcurrents()->st_ctime;
         mywork.pinode=addrcurrentp();
         pmywork=&mywork;
         delQueue();
         //pthread_mutex_unlock(&queue_mutex);
         pthread_mutex_lock(&running_mutex);
         runningthreads++;
         pthread_mutex_unlock(&running_mutex);
         pthread_mutex_unlock(&queue_mutex);
         //printf("loop queueing create %s\n",pmywork->name);
         pthread_mutex_lock(&start_mutex);
         startlock = 1;
         pthread_mutex_unlock(&start_mutex);
rc = pthread_attr_init(&attr);
//printf("attr_init: %d\n",rc);
rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
//printf("attr_setdetachedstate: %d\n",rc);
         //rc = pthread_create(&thread, NULL, listdir, pmywork);     
         rc = pthread_create(&thread, &attr, listdir, pmywork);     
         //printf("loop created tid %d queueing create %s\n",thread,pmywork->name);
         while (startlock) {
           //printf(".");
         } 
         //listdir(pmywork); 
         //pthread_mutex_unlock(&running_mutex);
         //delQueue();
         //pthread_mutex_unlock(&running_mutex);
         }
         //printf("mainloop queue %d threads %d\n",addrqent(),runningthreads);
     }  
     //} while (addrqent() > 0);
     return 0;
}
