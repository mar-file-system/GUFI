#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <ctype.h>
#include <errno.h>
#include <utime.h> 

#include <sqlite3.h>
#include <libgen.h>
#include <pthread.h>

#define MAXXATTR 1024
#define MAXPATH 1024
#define MAXSQL 1024
//#define MAXRECS 100000
//#define MAXRECS 4 
#define MAXRECS 10000

#include "bf.h"
#include "utils.c"
 
volatile int runningthreads = 0;
pthread_mutex_t running_mutex = PTHREAD_MUTEX_INITIALIZER;
volatile int startlock = 0;
pthread_mutex_t start_mutex = PTHREAD_MUTEX_INITIALIZER;

int counter;
int insertbatch;
char *err_msg = 0;
char dbn[MAXPATH];
int rc;
char sqlstmt[1024];
char *esql = "DROP TABLE IF EXISTS readdir;"
          "CREATE TABLE readdir(name TEXT, type TEXT, inode INT PRIMARY KEY, pinode INT);";
sqlite3 *db;
    const char *tail;
    int error;
    sqlite3_stmt *reso;

    char *zname;
    char *ztype;

struct inserts {
char name[MAXPATH];
char type[2];
long long int inode;
long long int pinode;
};
struct inserts insert[2][MAXRECS];

static void * insertthread( void * passing) {
int lcounter;
int linsertbatch;
int test;

    pthread_mutex_lock(&start_mutex);
    lcounter=counter;
    linsertbatch=insertbatch;
    startlock=0;
    pthread_mutex_unlock(&start_mutex);
    //printf("threadstart batch %d counter %d \n",linsertbatch,lcounter);

    sqlite3_exec(db, "BEGIN TRANSACTION", NULL , NULL, &err_msg);
    if (rc != SQLITE_OK) {
      //printf("begin transaction issue %s\n",sqlite3_errmsg(db));
      sqlite3_free(err_msg);
    }
    test=0;
    while (test < lcounter) {
      zname = sqlite3_mprintf("%q",insert[linsertbatch][test].name);
      ztype = sqlite3_mprintf("%q",insert[linsertbatch][test].type);
      sqlite3_bind_text(reso,1,zname,-1,SQLITE_TRANSIENT);
      sqlite3_bind_text(reso,2,ztype,-1,SQLITE_TRANSIENT);
      sqlite3_bind_int64(reso,3,insert[linsertbatch][test].inode);
      sqlite3_bind_int64(reso,4,insert[linsertbatch][test].pinode);
      sqlite3_free(zname);
      sqlite3_free(ztype);
      rc = sqlite3_step(reso);
      if (rc != SQLITE_ROW) {
          //fprintf(stderr, "SQL error on insertdbgo: error %d err %s\n",error,sqlite3_errmsg(db));
      }
      sqlite3_clear_bindings(reso);
      sqlite3_reset(reso);
      test++;
    }
      
    sqlite3_exec(db,"END TRANSACTION",NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
      //printf("end transaction issue %s\n",sqlite3_errmsg(db));
      sqlite3_free(err_msg);
    }
    pthread_mutex_lock(&running_mutex);
    runningthreads=0;
    pthread_mutex_unlock(&running_mutex);
    return NULL;
}

void listdir(const char *name, long long int level, struct dirent *entry, long long int pin, int statit, int xattrit )
{
    DIR *dir;
    //struct dirent *entry;
    char path[1024];
    char lpath[1024];
    long long int ppin;
    struct stat st;
    char type[2];
    pthread_t thread;
    pthread_attr_t attr;
    int myrunningthreads;

    //printf("inlistdir name %s\n",name);
    if (!(dir = opendir(name)))
        return;
    printf("d %s %lld %lld batch %d counter %d\n", name, pin , level,insertbatch,counter);
    insert[insertbatch][counter].inode = pin;
    insert[insertbatch][counter].pinode = level;
    sprintf(insert[insertbatch][counter].type,"d");
    sprintf(insert[insertbatch][counter].name,"%s",name);
    counter++;
    if (counter >= MAXRECS) {
       while (1) {
         pthread_mutex_lock(&running_mutex);
         myrunningthreads=runningthreads;
         pthread_mutex_unlock(&running_mutex);
         //printf("t%d.",myrunningthreads);
         if (myrunningthreads == 0 ) {
               break;
         }
       }
       pthread_mutex_lock(&running_mutex);
       runningthreads=1;
       pthread_mutex_unlock(&running_mutex);
       pthread_mutex_lock(&start_mutex);
       startlock = 1;
       pthread_mutex_unlock(&start_mutex);
       rc = pthread_attr_init(&attr);
       rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
       rc = pthread_create(&thread, &attr, insertthread, NULL);
       while (startlock) {
           //printf(".");
       }
      //fork thread wait for start
      if (insertbatch == 0) { insertbatch = 1; } else { insertbatch=0; } 
      counter=0;
    }
    ppin=pin;
    while ((entry = (readdir(dir)))) {
        int len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
        path[len] = 0;
        if (entry->d_type == DT_DIR) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;
            sprintf(type,"d");
            listdir(path, pin, entry, entry->d_ino, 0, 0);
        } else {
            len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
            if (entry->d_type == DT_REG) {
               sprintf(type,"f");
            }
            if (entry->d_type == DT_LNK) {
               sprintf(type,"l");
            }
            printf("%s %s %lld %lld batch %d counter %d\n",type, path, entry->d_ino,pin,insertbatch,counter);
           insert[insertbatch][counter].inode = entry->d_ino;
           insert[insertbatch][counter].pinode = pin;
           sprintf(insert[insertbatch][counter].type,"%s",type);
           sprintf(insert[insertbatch][counter].name,"%s",path);
           counter++;
           if (counter >= MAXRECS) {
             while (1) {
               pthread_mutex_lock(&running_mutex);
               myrunningthreads=runningthreads;
               pthread_mutex_unlock(&running_mutex);
               //printf("t%d.",myrunningthreads);
               if (myrunningthreads == 0 ) {
                     break;
               }
             }
             pthread_mutex_lock(&running_mutex);
             runningthreads=1;
             pthread_mutex_unlock(&running_mutex);
             pthread_mutex_lock(&start_mutex);
             startlock = 1;
             pthread_mutex_unlock(&start_mutex);
             rc = pthread_attr_init(&attr);
             rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
             rc = pthread_create(&thread, &attr, insertthread, NULL);
             while (startlock) {
                //printf(".");
             }
             if (insertbatch == 0) { insertbatch = 1; } else { insertbatch=0; } 
             counter=0;
           }
        }
    }
    closedir(dir);
    //printf("counter %d\n",counter);

}
 
int main(int argc, char *argv[])
{
    struct dirent *entries;
    struct stat status;
    int statit;
    int xattrit;
    int rc; 
    pthread_t thread;
    pthread_attr_t attr;
    int myrunningthreads;
    //statit=atoi(argv[2]);
    //xattrit=atoi(argv[3]);
    lstat(argv[1],&status);
    //printf("inmain d %s %lld 0\n", argv[1],status.st_ino);
    counter=0;
    insertbatch=0;
    sprintf(dbn,"readdirplus.db");
    rc = sqlite3_open(dbn, &db);
    //printf("rc from open dbn %d %s err %s\n",rc, dbn,sqlite3_errmsg(db));
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open readdirplus database: %s %s\n", dbn, sqlite3_errmsg(db));
        sqlite3_close(db);
        exit(9);
    }
    rc = sqlite3_exec(db, esql, 0, 0, &err_msg);
         //fprintf(stderr, "entries SQL error: %s\n", sqlite3_errmsg(db));
    if (rc != SQLITE_OK ) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        exit(9);
    }

    rc=sqlite3_exec(db, "PRAGMA synchronous = OFF", NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
          fprintf(stderr, "SQL error on insertdbprep setting sync off: error %d err %s\n",error,sqlite3_errmsg(db));
    }
    sprintf(sqlstmt,"INSERT INTO readdir VALUES (@name,@type,@inode,@pinode)");
    error= sqlite3_prepare_v2(db,  sqlstmt, MAXSQL, &reso, &tail);
    //printf("in prep reso %d\n",reso);
    if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on insertdbprep: error %d %s err %s\n",error,sqlstmt,sqlite3_errmsg(db));
          return 0;
    }
    listdir(argv[1], 0, entries, status.st_ino,0,0);
    if (counter > 0) {
       while (1) {
         pthread_mutex_lock(&running_mutex);
         myrunningthreads=runningthreads;
         pthread_mutex_unlock(&running_mutex);
         //printf("t%d.",myrunningthreads);
         if (myrunningthreads == 0 ) {
               break;
         }
       }
       pthread_mutex_lock(&running_mutex);
       runningthreads=1;
       pthread_mutex_unlock(&running_mutex);
       pthread_mutex_lock(&start_mutex);
       startlock = 1;
       pthread_mutex_unlock(&start_mutex);
       rc = pthread_attr_init(&attr);
       rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
       rc = pthread_create(&thread, &attr, insertthread, NULL);
       while (startlock) {
           //printf(".");
       }
      //fork thread wait for start
      if (insertbatch == 0) { insertbatch = 1; } else { insertbatch=0; }
      counter=0;
    }
    while (1) {
      pthread_mutex_lock(&running_mutex);
      myrunningthreads=runningthreads;
      pthread_mutex_unlock(&running_mutex);
      if (myrunningthreads == 0 ) {
            break;
      }
    }

    sqlite3_free(err_msg);
    sqlite3_close(db);
    return 0;
}
 
