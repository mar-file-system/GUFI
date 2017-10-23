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
#include <libgen.h>

#define MAXPATH 1024
#define MAXXATTR 1024
#define MAXSQL 1024
#define MAXRECS 100000
#include "bf.h"
#include "structq.c"
#include "utils.c"
#include "dbutils.c"
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
    struct work *mywork;
    DIR *dir;
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
    int dxattrs = 0;
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
    char savename[MAXPATH];
    char savepath[MAXPATH];
    struct stat stato;
    struct stat *status;
    struct stat *statussave;
    char *records;
    int recsize;
    int insertsize;
    long long pinode;
    FILE *pinodefd;
    char pinodepath[MAXPATH];
    sqlite3_stmt *res;
    sqlite3_stmt *reso;

     char ddirin[MAXPATH];
     char dlinein[MAXPATH+MAXPATH+MAXPATH];
     FILE *ddumpin;
     char *dtestname;
     char *dtestinode;
     char *dtesttype;
     char *dmode;
     char *duid;
     char *dgid;
     char *dsize;
     char *dblksize;
     char *dblocks;
     char *datime;
     char *dmtime;
     char *dctime;
     char *dtestlinkname;
     char *dtestxattr;
     char *dfg;
     int done;
     long long int doffset;
     int printing;
     char *p;
     char *q;
     int plen;
     int transcnt;

    pthread_mutex_lock(&start_mutex);
    sprintf(ddirin,"%s",passmywork->name);
    sprintf(nameto,"%s",passmywork->nameto);
    doffset=passmywork->offset;
    printing=passmywork->printing;
    //pinode=passmywork->pinode;
    startlock = 0;
    pthread_mutex_unlock(&start_mutex);
    //printf("thread got ddirin %s nameto %s offset %lld printing %d pinode %lld\n",ddirin,passmywork->nameto,doffset,printing,pinode);
    bzero(dlinein,sizeof(dlinein));
    ddumpin=fopen(ddirin,"r");
    fseek(ddumpin,doffset,SEEK_SET);
    dfg = fgets (dlinein, sizeof(dlinein), ddumpin);
    dlinein[strlen(dlinein)-1]='\0';
    //printf("thread dir got linein %s\n",dlinein);
    p=dlinein; q=strstr(p,fielddelim); bzero(q,1); sprintf(name,"%s",p);       sprintf(savename,"%s",name);
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); sprintf(type,"%s",p);
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_ino=atol(p);     stato.st_ino=statuso.st_ino;
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); pinode=atol(p);
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_mode=atol(p);    stato.st_mode=statuso.st_mode;
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_nlink=atol(p);   stato.st_nlink=statuso.st_nlink;
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_uid=atol(p);     stato.st_uid=statuso.st_uid;
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_gid=atol(p);     stato.st_gid=statuso.st_gid;
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_size=atol(p);    stato.st_size=statuso.st_size;
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_blksize=atol(p); stato.st_blksize=statuso.st_blksize;
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_blocks=atol(p);  stato.st_blocks=statuso.st_blocks;
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_atime=atol(p);   stato.st_atime=statuso.st_atime;
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_mtime=atol(p);   stato.st_mtime=statuso.st_mtime;
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_ctime=atol(p);   stato.st_ctime=statuso.st_ctime;
    //printf("name %s type %s inode %llu mode %hu nlink %hu uid %u gid %u size %lld, blksize %d blocks %lld, atime %ld mtime %ld ctime %ld\n",name,dtype,statuso.st_ino,statuso.st_mode,statuso.st_nlink,statuso.st_uid,statuso.st_gid,statuso.st_size,statuso.st_blksize,statuso.st_blocks,statuso.st_atime,statuso.st_mtime,statuso.st_ctime);
    statussave=&stato;
    status=&statuso;
    bzero(lpath,sizeof(lpath));
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); if (q>p) sprintf(lpath,"%s/%s",nameto,p);
    bzero(xattr,sizeof(xattr));
    p=q+1;     q=strstr(p,fielddelim); bzero(q,1); sprintf(xattr,"%s",p);
    xattrs=0;
    if (q>p) {
      //printf("got an xattr on %s\n",name);
      plen=0;
      plen=strlen(p);
      while (plen > 0) {
       if (!strncmp(p,xattrdelim,1)) xattrs++;
        plen--;
        p++;
      }
      xattrs=xattrs/2;
    }
    //while(*p) if (*p++ == '<') ++dxattrs;

    printit(name,status,type,lpath,xattrs,xattr,printing,pinode);
//    if (dupdir(name,nameto,status,xattrs,xattr))
//        return NULL; 
    sprintf(pinodepath,"%s/%s/pinode",nameto,name);
    //printf("%s\n",pinodepath);
//    pinodefd=fopen(pinodepath,"w");
//    fprintf(pinodefd,"%lld %lld",pinode,stato.st_ino);
//    fclose(pinodefd); 

//    db = opendb(name,nameto,status,db1,1,1);
//    res=insertdbprep(db,reso);
    //printf("inbfilistdir res %d\n",res);
    transcnt=0;
//    startdb(db);
//    sdb = opendb(name,nameto,status,db2,0,1);
    zeroit(&summary);
    //printf("going into read loop\n");
    done=0;
    while (1) {
      //sleep(1);
      bzero(dlinein,sizeof(dlinein));
      dfg = fgets (dlinein, sizeof(dlinein), ddumpin);
      if (dfg == NULL) {
         break;
      }
      dlinein[strlen(dlinein)-1]='\0';
      //printf("thread dlinein %s\n",dlinein);
      p=dlinein; q=strstr(p,fielddelim); bzero(q,1); sprintf(name,"%s",p);
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); sprintf(dtype,"%s",p);
      if (!strncmp(dtype,"d",1)) break;
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_ino=atol(p);
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); //skip the parent inode 
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_mode=atol(p);
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_nlink=atol(p);
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_uid=atol(p);
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_gid=atol(p);
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_size=atol(p);
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_blksize=atol(p);
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_blocks=atol(p);
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_atime=atol(p);
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_mtime=atol(p);
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); statuso.st_ctime=atol(p);
      statussave=&stato;
      status=&statuso;
      bzero(lpath,sizeof(lpath));
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); 
      if (q>p) {
        if (!strncmp(p,"/",1)) {
          sprintf(lpath,"%s",p);
        } else {
          sprintf(lpath,"%s/%s",nameto,p);
        }
      }
      bzero(dxattr,sizeof(dxattr));
      p=q+1;     q=strstr(p,fielddelim); bzero(q,1); sprintf(dxattr,"%s",p);
      dxattrs=0;
      if (q>p) {
        //printf("got an xattr on %s\n",name);
        dxattrs=0;
        plen=0;
        plen=strlen(p);
        while (plen > 0) {
         if (!strncmp(p,xattrdelim,1)) dxattrs++;
          plen--;
          p++;
        }
        dxattrs=dxattrs/2;
      }
     //while(*p) if (*p++ == '<') ++dxattrs;
/*
      sprintf(name,"%s",strtok(dlinein, "|"));
      //printf("read line %s\n",name);
      dtesttype = strtok(NULL,"|");
      sprintf(dtype,"%s",dtesttype);
      if (!strncmp(dtype,"d",1)) break;
      statuso.st_ino=atol(strtok(NULL,"|"));
      statuso.st_mode=atol(strtok(NULL,"|"));
      statuso.st_nlink=atol(strtok(NULL,"|"));
      statuso.st_uid=atol(strtok(NULL,"|"));
      statuso.st_gid=atol(strtok(NULL,"|"));
      statuso.st_size=atol(strtok(NULL,"|"));
      statuso.st_blksize=atol(strtok(NULL,"|"));
      statuso.st_blocks=atol(strtok(NULL,"|"));
      statuso.st_atime=atol(strtok(NULL,"|"));
      statuso.st_mtime=atol(strtok(NULL,"|"));
      statuso.st_ctime=atol(strtok(NULL,"|"));
      statussave=&stato;
      status=&statuso;
      p=strtok(NULL,"");
      //printf("p = %s\n",p);
      bzero(lpath,sizeof(lpath)); 
      q=strstr(p,"|");
      if (q>p) {
         bzero(q,1);
         sprintf(lpath,"%s",p);
         //printf("     q %s q+1 %s\n",q,q+1);
         p=q+1;
      } else p++;
      //printf("lpath %s\n",lpath);
      //printf("pp = %s\n",p);
      bzero(dxattr,sizeof(dxattr)); 
      //p=q;
      dxattrs=0;
      q=strstr(p,"|");
      if (q>p) {
         bzero(q,1);
         sprintf(dxattr,"%s",p);
         //dxattrs=charCounter(dxattr,'<' );
         while(*p) if (*p++ == '<') ++dxattrs;
      }
      //printf("dxattrs %d dxattr %s\n",dxattrs,dxattr);
*/
//      printit(name,&statuso,dtype,lpath,dxattrs,dxattr,printing,stato.st_ino);
      // insertdbgo formats xattrs and we already formatted them
//      insertdbgo(basename(name),&statuso,db,dtype,lpath,dxattrs,dxattr,0,res);
      transcnt++;
      if (transcnt > 100000) {
//        stopdb(db);
//        startdb(db);
        transcnt=0;
      }
      //printf("calling sumit %s\n",basename(name));
      sumit(&summary,&statuso,dxattrs,dtype);
      //printf("called sumit %s\n",basename(name));
    }  
    fclose(ddumpin);
//    stopdb(db);
//    insertdbfin(db,res);
//    closedb(db);
    sprintf(savepath,"%s/%s",nameto,savename);
    //printf("insertsum xattrs %d xattr %s\n",xattrs,xattr);
//    insertsumdb(savepath,statussave,sdb,type,dpath,xattrs,xattr,&summary,pinode);
//    closedb(sdb);
//  reall need to do chmod of both DBs to be same as parent directory 
//  make it so its owner and group are same as parent directory and 
//   its mode is same as directory but maybe take out write and execute 
    
    ///printf("path will be %s/%s/entries.db", nameto,savename);
    sprintf(path, "%s/%s/entries.db", nameto,savename);
//    chown(path, stato.st_uid, stato.st_gid);
//    chmod(path, stato.st_mode | S_IRUSR);
    sprintf(path, "%s/%s/summary.db", nameto,savename);
//    chown(path, stato.st_uid, stato.st_gid);
//    chmod(path, stato.st_mode | S_IRUSR);
    //sleep(1);
    pthread_mutex_lock(&running_mutex);
    runningthreads--;
    pthread_mutex_unlock(&running_mutex);
    return NULL;
}
 
//int printing;
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
     int startone;
     int myrunningthreads;
     int maxthreads;
     int i=0;

     char dirin[MAXPATH];
     char linein[MAXPATH+MAXPATH+MAXPATH];
     char lineins[MAXPATH+MAXPATH+MAXPATH];
     FILE *dumpin;
     char testname[MAXPATH];
     char testinode[MAXPATH];
     long long int offset = 0;
     long long int offsets = 0;
     char testtype[MAXPATH];
     char *fg;
     int done;
     long long int pinode;
     int printing;
     char *p;
     char *q;
     int totdirs=0;

     printing=1;
     front=NULL;
     front=NULL;
     sprintf(dirin,"%s",argv[1]);
     sprintf(mywork.nameto,"%s",argv[2]);
     mywork.printing = atoi(argv[3]);
     maxthreads = atoi(argv[4]);

     
     //printf("dumpfilein %s nameto %s printing %d maxthreads %d\n",dirin, mywork.nameto, mywork.printing,maxthreads);
     /* process input directory */
     //printf("dev %d size of dev %ld\n",mywork.statuso.st_dev,sizeof(mywork.statuso.st_dev));
     if (access(dirin, R_OK )) {
         printf("problem with dumpin %s\n",dirin);
         return 1;
     }
     pmywork=&mywork;
     pinode=0;
     sprintf(dirin,"%s",argv[1]);
     dumpin=fopen(dirin,"r");
     myrunningthreads=0;
     done=0;
     bzero(linein,sizeof(linein));
     bzero(lineins,sizeof(lineins));
     while (1) {
         //sleep(1);
         bzero(linein,sizeof(linein));
         fg = fgets (linein, sizeof(linein), dumpin);
         //printf("main got line %s\n",linein);
         if (fg != NULL) { 
/******  >?????? not sure why i am getting duplicate records from bfq dump  need to fix that ****/
          offsets=strlen(linein);
          if (strcmp(linein,lineins)) {
           bzero(lineins,sizeof(lineins));
           sprintf(lineins,"%s",linein);
           p=linein; q=strstr(p,fielddelim); bzero(q,1); sprintf(testname,"%s",p);  
           p=q+1;    q=strstr(p,fielddelim); bzero(q,1); sprintf(testtype,"%s",p); 
           //testname = strtok(linein, "|");
           //testtype = strtok(NULL,"|");
           //testinode = strtok(NULL,"|");
           //printf("name %s type %s\n",testname,testtype);
           if (!strncmp(testtype,"d",1)) {
              totdirs++;
              //printf("%d\n",totdirs);
              //printf("found directory %s type %s offset %lld\n",testname,testtype,offset);
              //myrunningthreads=runningthreads;
              while (1) {
                pthread_mutex_lock(&running_mutex);
                myrunningthreads=runningthreads;
                pthread_mutex_unlock(&running_mutex);
                //printf("-%d",myrunningthreads);
                if (myrunningthreads <= maxthreads) {
                  //launch one and get out
                  //printf("threads dropped runningthreads to %d %s\n",myrunningthreads,testname);
                  pthread_mutex_lock(&running_mutex);
                  runningthreads++;
                  pthread_mutex_unlock(&running_mutex);
                  //printf("bumping runningthreads to %d %s\n",runningthreads,testname);
                  pthread_mutex_lock(&start_mutex);
                  sprintf(mywork.name,"%s",dirin);
                  //mywork.pinode=pinode;
                  mywork.offset=offset;
                  startlock = 1;
                  pthread_mutex_unlock(&start_mutex);
                  rc = pthread_attr_init(&attr);
                  rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
                  rc = pthread_create(&thread, &attr, listdir, pmywork);
                  //printf("created thread for %s\n",testname);
                  while (startlock) {
                    //printf(".");
                  }
                  break;
                }
              }
              //pinode=atoi(testinode);;
           }
          }
          offset=offset+offsets;
          bzero(linein,sizeof(linein));;
         } else { 
           // we are out of records  spin to see when threads are done 
           while (1) {
             pthread_mutex_lock(&running_mutex);
             myrunningthreads=runningthreads;
             pthread_mutex_unlock(&running_mutex);
             //printf("t%d.",myrunningthreads);
             if (myrunningthreads < 1 ) {
               done=1; 
               break;
             }
           }
           if (done) break;
         }
     }
     fclose(dumpin);
     printf("totdirs = %d\n",totdirs);
     return 0;
}
