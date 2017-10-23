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
#define MAXRECS 100000
#include "bf.h"
#include "structq.c"
#include "utils.c"
#include "dbutils.c"
#include <pthread.h>

#define NS_PERMS_RD       (0x20)  /* Read */
#define NS_PERMS_WR       (0x40)  /* Write */
#define NS_PERMS_XS       (0x80)  /* Exec/Search */

volatile int runningthreads = 0;
pthread_mutex_t running_mutex = PTHREAD_MUTEX_INITIALIZER;
volatile int queuelock = 0;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
volatile int startlock = 0;
pthread_mutex_t start_mutex = PTHREAD_MUTEX_INITIALIZER;


sqlite3 *indb;

//void listdir(const char *name, char *nameto, struct stat *status,int printing)
static void * listdir(void * passv)
{
    struct work *passmywork = passv;;
    struct work *mywork;
    DIR *dir;
    struct dirent myentry;
    struct dirent *entry;
    int len;
    char path[MAXPATH];
    char lpath[MAXPATH];
    char lpatho[MAXPATH];
    char dpath[MAXPATH];
    char tolpath[MAXPATH];
    char xattr[MAXXATTR];
    char dxattr[MAXXATTR];
    int xattrs = 0;;
    int dxattrs = 0;;
    char *key;
    struct stat statuso;
    sqlite3 *db;
    sqlite3 *sdb;
    sqlite3 *db1;
    sqlite3 *db2;
    char type[2]; 
    char dtype[2]; 
    struct sum summary;
    struct work rmywork;
    char name[MAXPATH];
    char nameto[MAXPATH];
    struct stat stato;
    struct stat *status;
    struct stat *statussave;
    int printing;
    char *records;
    int recsize;
    int insertsize;
    long long pinode;
    FILE *pinodefd;
    char pinodepath[MAXPATH];
    sqlite3_stmt *res;
    sqlite3_stmt *reso;

     char *myerr_msg = 0;
     sqlite3_stmt *myinreso;
     char myinsql[MAXSQL];
     int     myrec_count = 0;
     int  myerror;
     const char      *myerrMSG;
     const char      *myintail;
     char mytype[2];

     char cperms[20];
     char cpermsp1[10];
     char cpermsp2[10];
     char *cpermsp;
     int tmode;

     int tape;
 
    //printf("in listdir %s\n",passmywork->name);
    sprintf(name,"%s",passmywork->name);
    sprintf(nameto,"%s",passmywork->nameto);
    //sprintf(rmywork.sqlsum,"%s",passmywork->sqlsum);
    //sprintf(rmywork.sqlent,"%s",passmywork->sqlent);
    //rmywork.printdir=passmywork->printdir;
    //rmywork.andor=passmywork->andor;
    printing=passmywork->printing;
    statuso.st_ino=passmywork->statuso.st_ino;;
    stato.st_ino=passmywork->statuso.st_ino;;
    statuso.st_mode=passmywork->statuso.st_mode;
    stato.st_mode=passmywork->statuso.st_mode;
    statuso.st_nlink=passmywork->statuso.st_nlink;
    stato.st_nlink=passmywork->statuso.st_nlink;
    statuso.st_uid=passmywork->statuso.st_uid;
    stato.st_uid=passmywork->statuso.st_uid;
    statuso.st_gid=passmywork->statuso.st_gid;
    stato.st_gid=passmywork->statuso.st_gid;
    statuso.st_size=passmywork->statuso.st_size;
    stato.st_size=passmywork->statuso.st_size;
    statuso.st_blksize=passmywork->statuso.st_blksize;
    stato.st_blksize=passmywork->statuso.st_blksize;
    statuso.st_blocks=passmywork->statuso.st_blocks;
    stato.st_blocks=passmywork->statuso.st_blocks;
    statuso.st_atime=passmywork->statuso.st_atime;
    stato.st_atime=passmywork->statuso.st_atime;
    statuso.st_mtime=passmywork->statuso.st_mtime;
    stato.st_mtime=passmywork->statuso.st_mtime;
    statuso.st_ctime=passmywork->statuso.st_ctime;
    stato.st_ctime=passmywork->statuso.st_ctime;
    statussave=&stato;
    status=&statuso;
    pinode=passmywork->pinode;
    //printf("copying input in listdir\n");
    pthread_mutex_lock(&start_mutex);
    startlock = 0;
    pthread_mutex_unlock(&start_mutex);
    //printf("listdir started\n");

    //printf("dir: ");
    sprintf(dtype,"%s","d");
    bzero(dpath,sizeof(dpath));
    bzero(lpath,sizeof(lpath));

    printf("made it to listdir with first thread for name %s nameto %s inode %lld pinode %lld printing %d mode %d\n",name,nameto, statuso.st_ino, pinode, printing, status->st_mode);

    //sprintf(myinsql,"select name, type, inode, pinode, perms, links, uid, gid, size, atime, mtime, crtime from nsobjall where pinode = %llu",statuso.st_ino);
    sprintf(myinsql,"select name, type, inode, pinode, perms, links, uid, gid, bfsize, atime, mtime, crtime, tape, linkname, xattr from nsbf where pinode = %llu",statuso.st_ino);
    myerror = sqlite3_prepare_v2(indb, myinsql, 1000, &myinreso, &myintail);
    if (myerror != SQLITE_OK) {
          fprintf(stderr, "SQL error on query: %s  errr %s\n",myinsql,sqlite3_errmsg(indb));
          return 0;
    }
 
    //if (!(dir = opendir(name)))
    //    return NULL;
    //if (!(entry = readdir(dir)))
    //    return NULL;
    //dxattrs=0;
    //bzero(dxattr,sizeof(dxattr));
    //dxattrs=pullxattrs(name,dxattr);
    //if (dxattrs>0) printf("pullxattr returns %d\n", dxattrs);
    /* no xattrs yet */
    bzero(dxattr,sizeof(dxattr));
    dxattrs=0;
    dxattrs=strlen(passmywork->xattr);
    if (dxattrs > 0) {
      sprintf(dxattr,"%s",passmywork->xattr);
      dxattrs = 1;
    }
    printit(name,status,dtype,lpath,dxattrs,dxattr,printing,pinode);
    
    if (dupdir(name,nameto,status,dxattrs,dxattr))
        return NULL; 

    /* just for now */
    sprintf(pinodepath,"%s/%s",nameto,name);
    chmod(pinodepath, S_IRWXU|S_IRWXG);

    sprintf(pinodepath,"%s/%s/pinode",nameto,name);
    //printf("%s\n",pinodepath);
    pinodefd=fopen(pinodepath,"w");
    fprintf(pinodefd,"%lld %lld",pinode,stato.st_ino);
    fclose(pinodefd); 
    //db = openedb(name,nameto,status,db1);
    db = opendb(name,nameto,status,db1,1,1);
    res=insertdbprep(db,reso);
    //printf("inbfilistdir res %d\n",res);
    startdb(db);
    //sdb = opensumdb(name,nameto,status,db2);
    sdb = opendb(name,nameto,status,db2,0,1);
    /*????? there would obviously be more */
    zeroit(&summary);
    //summary.totfiles=0;
    //summary.totsize=0;
    records=malloc(MAXRECS);
    bzero(records,MAXRECS);
    recsize=0;
    
printf("entering loop where pinode is  %llu\n",statuso.st_ino);
    while (sqlite3_step(myinreso) == SQLITE_ROW) {
       
        sprintf(myentry.d_name,"%s", sqlite3_column_text(myinreso, 0)); 
        entry = &myentry;
printf("1\n");
        sprintf(mytype, "%s", sqlite3_column_text(myinreso, 1)); 
        statuso.st_ino = sqlite3_column_int(myinreso, 2); 

        sprintf(cperms, "%s", sqlite3_column_text(myinreso, 4)); 
        cpermsp=cperms;
        printf("cperms %s\n",cpermsp);
        statuso.st_mode=0;
        bzero(cpermsp1,sizeof(cpermsp1));
        if (!strncmp(mytype,"d",1)) statuso.st_mode |= S_IFDIR;
        if (!strncmp(mytype,"f",1)) statuso.st_mode |= S_IFREG;
        if (!strncmp(mytype,"l",1)) statuso.st_mode |= S_IFLNK;
        strncpy(cpermsp1,cpermsp+1,2);  sprintf(cpermsp2,"0x%s",cpermsp1); tmode=strtol(cpermsp2,0,16); 
        if (tmode & NS_PERMS_RD) statuso.st_mode |= S_IRUSR;
        if (tmode & NS_PERMS_WR) statuso.st_mode |= S_IWUSR;
        if (tmode & NS_PERMS_XS) statuso.st_mode |= S_IXUSR;
        strncpy(cpermsp1,cpermsp+4,2);  sprintf(cpermsp2,"0x%s",cpermsp1); tmode=strtol(cpermsp2,0,16); 
        if (tmode & NS_PERMS_RD) statuso.st_mode |= S_IRGRP;
        if (tmode & NS_PERMS_WR) statuso.st_mode |= S_IWGRP;
        if (tmode & NS_PERMS_XS) statuso.st_mode |= S_IXGRP;
        strncpy(cpermsp1,cpermsp+7,2);  sprintf(cpermsp2,"0x%s",cpermsp1); tmode=strtol(cpermsp2,0,16); 
        if (tmode & NS_PERMS_RD) statuso.st_mode |= S_IROTH;
        if (tmode & NS_PERMS_WR) statuso.st_mode |= S_IWOTH;
        if (tmode & NS_PERMS_XS) statuso.st_mode |= S_IXOTH;

        //statuso.st_mode=strtol(cpermsp,0,16);
        printf("mode %d\n",statuso.st_mode);
 
//       statuso.st_mode = 11111; 

        statuso.st_nlink = sqlite3_column_int(myinreso, 5); 
        statuso.st_uid = sqlite3_column_int(myinreso, 6); 
        statuso.st_gid = sqlite3_column_int(myinreso, 7); 
        statuso.st_size = sqlite3_column_int(myinreso, 8); 
        statuso.st_blksize=4096; 
        statuso.st_blocks=0; 
        statuso.st_atime = sqlite3_column_int(myinreso, 9); 
        statuso.st_mtime = sqlite3_column_int(myinreso, 10); 
        statuso.st_ctime = sqlite3_column_int(myinreso, 11); 
        statuso.st_ctime = statuso.st_mtime; 
        statuso.st_ctime = sqlite3_column_int(myinreso, 11); 
        tape = 0;  
        tape = sqlite3_column_int(myinreso, 12);
        if (tape == 0) statuso.st_blksize=4096;
        if (tape == 1) statuso.st_blksize=1048576; 
        statuso.st_blocks = statuso.st_size/statuso.st_blksize + 1;
        sprintf(lpath,"%s", sqlite3_column_text(myinreso, 13)); 
        bzero(xattr,sizeof(xattr));
        sprintf(xattr,"%s", sqlite3_column_text(myinreso, 14)); 
        xattrs=0;
        xattrs=strlen(xattr);
        if (xattrs > 0) {
          xattrs = 1;
        }  

printf("we got one %s tape %d\n",name, tape);

        len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
        //lstat(path, &statuso);
        if (!strncmp(mytype,"d",1)) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;
            /* no need to queue this if we cant read/stat the dir/dirents */
            //stat(path, &statuso);
            /*?????? if you thread this you need to mutex around push *****/
            //if (!access(path, R_OK | X_OK)) { 
                pthread_mutex_lock(&queue_mutex);
                //push(path,&statuso,stato.st_ino);
                pushx(path,&statuso,stato.st_ino,xattr);
                pthread_mutex_unlock(&queue_mutex);
            //}
        } else if (!strncmp(mytype,"l",1)) {
            //len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
            //stat(path, &statuso);
            /* no readlink yet */
            //bzero(lpatho,sizeof(lpatho));
            //readlink(path,lpatho,MAXPATH);
            //if (!strncmp(lpatho,"/",1)) {
            //  sprintf(lpath,"%s",lpatho);
            //} else {
            //  sprintf(lpath,"%s/%s/%s",nameto,name,lpatho);
            //}
            //printf("linkto %s :",lpath);
            sprintf(type,"%s","l");
            //bzero(xattr,sizeof(xattr));
            /* no xattrs yet */
            //xattrs=0;
            //xattrs=pullxattrs(path,xattr);
            printit(path,&statuso,type,lpath,xattrs,xattr,printing,stato.st_ino);
            /* xattrs already formatted */
            insertdbgo(entry->d_name,&statuso,db,type,lpath,xattrs,xattr,0,res);
            sumit(&summary,&statuso,xattrs,type);
        }
        else if (!strncmp(mytype,"f",1)) {
            //len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
            //stat(path, &statuso);
            //printf("file: ");
            sprintf(type,"%s","f");
            bzero(lpath,sizeof(lpath));
            //bzero(xattr,sizeof(xattr));
            //xattrs=0;
            //xattrs=pullxattrs(path,xattr);
            bzero(lpatho,sizeof(lpatho));
            printit(path,&statuso,type,lpath,xattrs,xattr,printing,stato.st_ino);
            //if (xattrs > 0) exit(9);
            /* xattrs already formated */
            insertdbgo(entry->d_name,&statuso,db,type,lpath,xattrs,xattr,0,res);
            sumit(&summary,&statuso,xattrs,type);
        }
    }
    sqlite3_finalize(myinreso);
    //closedir(dir);
/*
    if (recsize > 0) {
       insertsize = insertdb(entry->d_name,&statuso,db,type,lpath,xattrs,xattr,records,recsize,0);
    }
*/
    stopdb(db);
    insertdbfin(db,res);
    closedb(db);
    insertsumdb(name,statussave,sdb,dtype,dpath,dxattrs,dxattr,&summary,pinode);
    closedb(sdb);
/******  reall need to do chmod of both DBs to be same as parent directory */
/*******  make it so its owner and group are same as parent directory and */
/****   its mode is same as directory but maybe take out write and execute */
    sprintf(path, "%s/%s/entries.db", nameto,name);
    chown(path, stato.st_uid, stato.st_gid);
    chmod(path, stato.st_mode | S_IRUSR);
    sprintf(path, "%s/%s/summary.db", nameto,name);
    chown(path, stato.st_uid, stato.st_gid);
    chmod(path, stato.st_mode | S_IRUSR);

    free(records);
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

     struct stat status;
     struct stat statuso;
     char nameo[MAXPATH];
     struct work mywork;
     struct work *pmywork;
     pthread_t thread;
     pthread_attr_t attr;
     int rc;
     int error;
     int startone;
     int myqent;
     int myrunningthreads;
     int maxthreads;
     int i=0;
     char *err_msg = 0;
     char indbn[MAXPATH];
     sqlite3_stmt *inreso;
     char insql[MAXSQL];
     int     rec_count = 0;
     const char      *errMSG;
     const char      *intail;


     char vperms[20];
     char vpermsp1[10];
     char vpermsp2[10];
     char *vpermsp;
     int vmode;

     qent=0;
     printing=1;
     front=NULL;
     front=NULL;
     sprintf(indbn,"%s",argv[1]);
     sprintf(mywork.nameto,"%s",argv[2]);
     //sprintf(mywork.sqlsum,"%s",argv[3]);
     //sprintf(mywork.sqlent,"%s",argv[4]);
     //mywork.printdir=atoi(argv[5]);
     //mywork.andor=atoi(argv[6]);
     mywork.printing = atoi(argv[3]);
     maxthreads = atoi(argv[4]);
     printf("indbn %s todirname %s printing %d maxthreads %d\n",indbn, mywork.nameto, mywork.printing,maxthreads);
     /* process input directory */
/*
     lstat(mywork.name,&mywork.statuso);
     mywork.pinode=0;
 */
    rc = sqlite3_open(indbn, &indb);
    //printf("rc from open indbn %d %s err %s\n",rc, indbn,sqlite3_errmsg(indb));
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s %s\n", indbn, sqlite3_errmsg(indb));
        sqlite3_close(indb);
        return 0;
    }

    //sprintf(insql,"select name, type, inode, pinode, perms, links, uid, gid, size, atime, mtime, crtime from nsobjall where type = 'r'");
    sprintf(insql,"select name, type, inode, pinode, perms, links, uid, gid, bfsize, atime, mtime, crtime, xattr from nsbf where type = 'r'");
    error = sqlite3_prepare_v2(indb, insql, 1000, &inreso, &intail);
    if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on query: %s name %s errr %s\n",insql,indbn,sqlite3_errmsg(indb));
          return 0;
    }
 
    if (sqlite3_step(inreso) != SQLITE_ROW) {
          fprintf(stderr, "SQL error on stwp: %s name %s sqlite3_errmsg %s \n",insql,indbn,sqlite3_errmsg(indb));
          //return 0;
    }
       
    //printf("name %s|", sqlite3_column_text(inreso, 0)); 
    sprintf(mywork.name,"%s", sqlite3_column_text(inreso, 0)); 
    //printf("type %s|", sqlite3_column_text(inreso, 1)); 
    //printf("inode %d|", sqlite3_column_int(inreso, 2)); 
    //sprintf(mywork.statuso.st_ino,"%lld", sqlite3_column_int(inreso, 2)); 
    mywork.statuso.st_ino = sqlite3_column_int(inreso, 2); 
    //printf("pinode %d|", sqlite3_column_int(inreso, 3)); 
    //sprintf(mywork.pinode,"%d", sqlite3_column_int(inreso, 3)); 
    mywork.pinode = sqlite3_column_int(inreso, 3); 

    //printf("mode %s|",sqlite3_column_int(inreso, 4));
        sprintf(vperms, "%s", sqlite3_column_text(inreso, 4)); 
        vpermsp=vperms;
        printf("vperms %s\n",vpermsp);
        mywork.statuso.st_mode=0;
        mywork.statuso.st_mode |= S_IFDIR;
        bzero(vpermsp1,sizeof(vpermsp1));
        //printf("statuso.st_mode %d\n",statuso.st_mode);
        strncpy(vpermsp1,vpermsp+1,2);  sprintf(vpermsp2,"0x%s",vpermsp1); vmode=strtol(vpermsp2,0,16); 
        if (vmode & NS_PERMS_RD) mywork.statuso.st_mode |= S_IRUSR;
        if (vmode & NS_PERMS_WR) mywork.statuso.st_mode |= S_IWUSR;
        if (vmode & NS_PERMS_XS) mywork.statuso.st_mode |= S_IXUSR;
        strncpy(vpermsp1,vpermsp+4,2);  sprintf(vpermsp2,"0x%s",vpermsp1); vmode=strtol(vpermsp2,0,16); 
        if (vmode & NS_PERMS_RD) mywork.statuso.st_mode |= S_IRGRP;
        if (vmode & NS_PERMS_WR) mywork.statuso.st_mode |= S_IWGRP;
        if (vmode & NS_PERMS_XS) mywork.statuso.st_mode |= S_IXGRP;
        strncpy(vpermsp1,vpermsp+7,2);  sprintf(vpermsp2,"0x%s",vpermsp1); vmode=strtol(vpermsp2,0,16); 
        if (vmode & NS_PERMS_RD) mywork.statuso.st_mode |= S_IROTH;
        if (vmode & NS_PERMS_WR) mywork.statuso.st_mode |= S_IWOTH;
        if (vmode & NS_PERMS_XS) mywork.statuso.st_mode |= S_IXOTH;

        //statuso.st_mode=strtol(cpermsp,0,16);
        printf("mode %d\n",mywork.statuso.st_mode);
 
//       statuso.st_mode = 11111; 

/* for now */
//    mywork.statuso.st_mode = 57344; 
    mywork.statuso.st_nlink = sqlite3_column_int(inreso, 5); 
    mywork.statuso.st_uid = sqlite3_column_int(inreso, 6); 
    mywork.statuso.st_gid = sqlite3_column_int(inreso, 7); 
    mywork.statuso.st_size = sqlite3_column_int(inreso, 8); 
    mywork.statuso.st_blksize=4096; 
    mywork.statuso.st_blocks=0; 
    mywork.statuso.st_atime = sqlite3_column_int(inreso, 9); 
    mywork.statuso.st_mtime = sqlite3_column_int(inreso, 10); 
    mywork.statuso.st_ctime = sqlite3_column_int(inreso, 11); 
    mywork.statuso.st_ctime = mywork.statuso.st_mtime;
    bzero(mywork.xattr,sizeof(mywork.xattr));
    sprintf(mywork.xattr, "%s", sqlite3_column_text(inreso, 12)); 

    sqlite3_finalize(inreso);

    /* call printit? */

    pmywork=&mywork;
    runningthreads++;
    listdir(pmywork);

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
         sprintf(mywork.xattr,"%s",addrcurrentx());
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
         //rc = pthread_detach(thread);
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
     sqlite3_free(err_msg);
     sqlite3_close(indb);
     return 0;
}
