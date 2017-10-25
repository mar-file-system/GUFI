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

#define MAXPATH 1024
#define MAXXATTR 1024
#define MAXSQL 1024
#include "bf.h"
#include "structq.c"
#include "utils.c"
#include <pthread.h>

volatile int runningthreads = 0;
pthread_mutex_t running_mutex = PTHREAD_MUTEX_INITIALIZER;

volatile int queuelock = 0;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

volatile int startlock = 0;
pthread_mutex_t start_mutex = PTHREAD_MUTEX_INITIALIZER;

//void listdir(const char *name, char *nameto, struct stat *status,int printing)
static void * listdir(void * passv)
{
    struct work *passmywork = passv;;
    DIR *dir;
    struct dirent *entry;
    int len;
    char path[MAXPATH];
    char lpath[MAXPATH];
    char lpatho[MAXPATH];
    char dpath[MAXPATH];
    char xattr[MAXXATTR];
    char dxattr[MAXXATTR];
    int xattrs = 0;;
    int dxattrs = 0;;
    struct stat statuso;
    char type[2]; 
    char dtype[2]; 
    char name[MAXPATH];
    char nameto[MAXPATH];
    struct stat stato;
    struct stat *status;
    struct stat *statussave;
    int printing;
    int pinode;
    int ldodelim;

    //printf("in listdir %s\n",passmywork->name);
    sprintf(name,"%s",passmywork->name);
    sprintf(nameto,"%s",passmywork->nameto);
    //sprintf(rmywork.sqlsum,"%s",passmywork->sqlsum);
    //sprintf(rmywork.sqlent,"%s",passmywork->sqlent);
    //rmywork.printdir=passmywork->printdir;
    //rmywork.andor=passmywork->andor;
    printing           = passmywork->printing;
    statuso.st_ino     = passmywork->statuso.st_ino;;
    stato.st_ino       = passmywork->statuso.st_ino;;
    statuso.st_mode    = passmywork->statuso.st_mode;
    stato.st_mode      = passmywork->statuso.st_mode;
    statuso.st_nlink   = passmywork->statuso.st_nlink;
    stato.st_nlink     = passmywork->statuso.st_nlink;
    statuso.st_uid     = passmywork->statuso.st_uid;
    stato.st_uid       = passmywork->statuso.st_uid;
    statuso.st_gid     = passmywork->statuso.st_gid;
    stato.st_gid       = passmywork->statuso.st_gid;
    statuso.st_size    = passmywork->statuso.st_size;
    stato.st_size      = passmywork->statuso.st_size;
    statuso.st_blksize = passmywork->statuso.st_blksize;
    stato.st_blksize   = passmywork->statuso.st_blksize;
    statuso.st_blocks  = passmywork->statuso.st_blocks;
    stato.st_blocks    = passmywork->statuso.st_blocks;
    statuso.st_atime   = passmywork->statuso.st_atime;
    stato.st_atime     = passmywork->statuso.st_atime;
    statuso.st_mtime   = passmywork->statuso.st_mtime;
    stato.st_mtime     = passmywork->statuso.st_mtime;
    statuso.st_ctime   = passmywork->statuso.st_ctime;
    stato.st_ctime     = passmywork->statuso.st_ctime;

    statussave         = &stato;
    status             = &statuso;
    pinode             = passmywork->pinode;
    ldodelim           = passmywork->dodelim;

    //printf("copying input in listdir\n");
    pthread_mutex_lock(&start_mutex);
    startlock = 0;
    pthread_mutex_unlock(&start_mutex);
    //printf("listdir started\n");
    //printf("ldodelim %d \n",ldodelim);

    //printf("dir: ");
    sprintf(dtype,"%s","d");
    bzero(dpath,sizeof(dpath));
    bzero(lpath,sizeof(lpath));
    if (!(dir = opendir(name)))
        return NULL;
    if (!(entry = readdir(dir)))
        return NULL;
    dxattrs=0;
    bzero(dxattr,sizeof(dxattr));
    dxattrs=pullxattrs(name,dxattr);
    //if (dxattrs>0) printf("pullxattr returns %d\n", dxattrs);
    if (ldodelim) {
      printitd(name,status,dtype,lpath,dxattrs,dxattr,printing,pinode);
    } else {
      printit(name,status,dtype,lpath,dxattrs,dxattr,printing,pinode);
    }

    do {
        len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
        //stat(path, &statuso);
        lstat(path, &statuso);
        //if (entry->d_type == DT_DIR) {
        if (S_ISDIR(statuso.st_mode) ) {
            //len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
            //path[len] = 0;
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;
            /* no need to queue this if we cant read/stat the dir/dirents */
            //stat(path, &statuso);
            /*?????? if you thread this you need to mutex around push *****/
            //printf("push indir %s\n",path);
            if (!access(path, R_OK | X_OK)) 
                //printf("push pushing %s\n",path);
                pthread_mutex_lock(&queue_mutex);
                push(path,&statuso,stato.st_ino);
                pthread_mutex_unlock(&queue_mutex);
        //} else if (entry->d_type == DT_LNK) {
          } else if (S_ISLNK(statuso.st_mode) ) {
            //len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
            //stat(path, &statuso);
            bzero(lpatho,sizeof(lpatho));
            readlink(path,lpatho,MAXPATH);
            sprintf(lpath,"%s/%s/%s",nameto,name,lpatho);
            //printf("linkto %s :",lpath);
            sprintf(type,"%s","l");
            bzero(xattr,sizeof(xattr));
            xattrs=0;
            xattrs=pullxattrs(path,xattr);
            //printf("push inlnk %s\n",path);
            if (ldodelim) {
              printitd(path,&statuso,type,lpath,xattrs,xattr,printing,stato.st_ino);
            } else {
              printit(path,&statuso,type,lpath,xattrs,xattr,printing,stato.st_ino);
            }
              
        }
        //else if (entry->d_type == DT_REG) {
          else if (S_ISREG(statuso.st_mode) ) {
            //len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
            //stat(path, &statuso);
            //printf("file: ");
            sprintf(type,"%s","f");
            bzero(lpath,sizeof(lpath));
            bzero(xattr,sizeof(xattr));
            xattrs=0;
            xattrs=pullxattrs(path,xattr);
            bzero(lpatho,sizeof(lpatho));
            if (ldodelim) {
              printitd(path,&statuso,type,lpath,xattrs,xattr,printing,stato.st_ino);
            } else {
              printit(path,&statuso,type,lpath,xattrs,xattr,printing,stato.st_ino);
            }
            //if (xattrs > 0) exit(9);
        }
    } while ((entry = (readdir(dir))));
    closedir(dir);

    pthread_mutex_lock(&running_mutex);
    runningthreads--;
    pthread_mutex_unlock(&running_mutex);

    return NULL;
}
 
char value1[MAXPATH];
char value2[MAXPATH];
int printing;
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
     printing=1;
     front=NULL;
     front=NULL;
     sprintf(mywork.name,"%s",argv[1]);
     //sprintf(mywork.nameto,"%s",argv[2]);
     //sprintf(mywork.sqlsum,"%s",argv[3]);
     //sprintf(mywork.sqlent,"%s",argv[4]);
     //mywork.printdir=atoi(argv[5]);
     //mywork.andor=atoi(argv[6]);
     mywork.printing = atoi(argv[2]);
     maxthreads = atoi(argv[3]);
     mywork.dodelim=atoi(argv[4]);
     mywork.pinode=0;
     if (!mywork.dodelim)
        printf("dirname %s printing %d maxthreads %d\n",mywork.name, mywork.printing,maxthreads);
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
         //sleep(1);
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
            sprintf(mywork.name,"%s", addrcurrent());
            mywork.statuso.st_ino     = addrcurrents()->st_ino;;
            mywork.statuso.st_mode    = addrcurrents()->st_mode;
            mywork.statuso.st_nlink   = addrcurrents()->st_nlink;
            mywork.statuso.st_uid     = addrcurrents()->st_uid;
            mywork.statuso.st_gid     = addrcurrents()->st_gid;
            mywork.statuso.st_size    = addrcurrents()->st_size;
            mywork.statuso.st_blksize = addrcurrents()->st_blksize;
            mywork.statuso.st_blocks  = addrcurrents()->st_blocks;
            mywork.statuso.st_atime   = addrcurrents()->st_atime;
            mywork.statuso.st_mtime   = addrcurrents()->st_mtime;
            mywork.statuso.st_ctime   = addrcurrents()->st_ctime;
            mywork.pinode             = addrcurrentp();

            pmywork = &mywork;
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
            //printf("push loop created tid %d queueing create %s\n",thread,pmywork->name);
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
     return 0;
}
