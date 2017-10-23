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

#define MAXXATTR 1024
#define MAXPATH 1024
#define MAXSQL 1024

#include "bf.h"
#include "utils.c"
 
int counter;
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


void listdir(const char *name, long long int level, struct dirent *entry, long long int pin, int statit, int xattrit )
{
    DIR *dir;
    //struct dirent *entry;
    char path[1024];
    char lpath[1024];
    long long int ppin;
    struct stat st;
    char type[2];
    int xattrs;
    char xattr[MAXXATTR];

    counter++;
    if (counter >= 100000) {
      sqlite3_exec(db,"END TRANSACTION",NULL, NULL, &err_msg);
      if (rc != SQLITE_OK) {
        printf("end transaction issue %s\n",sqlite3_errmsg(db));
        sqlite3_free(err_msg);
        //exit(9);
      }
      sqlite3_exec(db, "BEGIN TRANSACTION", NULL , NULL, &err_msg);
      if (rc != SQLITE_OK) {
        printf("begin transaction issue %s\n",sqlite3_errmsg(db));
        sqlite3_free(err_msg);
        //exit(9);
      }
      counter = 0;
    }

//printf("inlistdir name %s\n",name);
    if (!(dir = opendir(name)))
        return;
    //if (statit) lstat(name,&st);
    //bzero(xattr, sizeof(xattr));
    //xattrs=0;
    //if (xattrit) {
    //  xattrs=pullxattrs(name,xattr);
    //}
    //if (statit+xattrit > 0) {
    sprintf(type,"d");
    //  printit(name,&st,type,lpath,xattrs,xattr,1,pin);
    //} else {
      printf("d %s %lld %lld\n", name, pin , level);
      zname = sqlite3_mprintf("%q",name);
      ztype = sqlite3_mprintf("%q",type);
      sqlite3_bind_text(reso,1,zname,-1,SQLITE_TRANSIENT);
      sqlite3_bind_text(reso,2,ztype,-1,SQLITE_TRANSIENT);
      sqlite3_bind_int64(reso,3,pin);
      sqlite3_bind_int64(reso,4,level);
      sqlite3_free(zname);
      sqlite3_free(ztype);
      rc = sqlite3_step(reso);
    if (rc != SQLITE_ROW) {
          //fprintf(stderr, "SQL error on insertdbgo: error %d err %s\n",error,sqlite3_errmsg(db));
          //return 0;
    }
    sqlite3_clear_bindings(reso);
    sqlite3_reset(reso);

    //}
    ppin=pin;
    while ((entry = (readdir(dir)))) {
       counter++;
       int len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
       path[len] = 0;
       //if (statit) lstat(path,&st);
       //xattrs=0;
       // bzero(xattr, sizeof(xattr));
       //if (xattrit) {
       //  xattrs=pullxattrs(path,xattr);
       // }
        if (entry->d_type == DT_DIR) {
        //if (S_ISDIR(st.st_mode) ) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;
            sprintf(type,"d");
            //printf("inwhile d %s %lld %lld\n", name, entry->d_ino, ppin);
            listdir(path, pin, entry, entry->d_ino, 0, 0);
        } else {
            len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
            //bzero(lpath,sizeof(lpath));
            if (entry->d_type == DT_REG) {
            //if (S_ISREG(st.st_mode)) {
               sprintf(type,"f");
            }
            if (entry->d_type == DT_LNK) {
            //if (S_ISLNK(st.st_mode)) {
               sprintf(type,"l");
               //if (statit) readlink(path,lpath,MAXPATH);
            }
            //if (statit+xattrit > 0) {
              //printf("%lld ", pin);
              //bzero(type,sizeof(type));
              //printf("readlink %s %s\n",path,lpath);
              //printit(path,&st,type,lpath,xattrs,xattr,1,pin);
            //} else {
              printf("%s %s %lld %lld\n",type, path, entry->d_ino,pin);
      zname = sqlite3_mprintf("%q",path);
      ztype = sqlite3_mprintf("%q",type);
      sqlite3_bind_text(reso,1,zname,-1,SQLITE_TRANSIENT);
      sqlite3_bind_text(reso,2,ztype,-1,SQLITE_TRANSIENT);
      sqlite3_bind_int64(reso,3,entry->d_ino);
      sqlite3_bind_int64(reso,4,pin);
      sqlite3_free(zname);
      sqlite3_free(ztype);
      rc = sqlite3_step(reso);
    if (rc != SQLITE_ROW) {
          //fprintf(stderr, "SQL error on insertdbgo: error %d err %s\n",error,sqlite3_errmsg(db));
          //return 0;
    }
    sqlite3_clear_bindings(reso);
    sqlite3_reset(reso);
            //}
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
    //statit=atoi(argv[2]);
    //xattrit=atoi(argv[3]);
    lstat(argv[1],&status);
    //printf("inmain d %s %lld 0\n", argv[1],status.st_ino);
    counter=0;
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
    sqlite3_exec(db, "BEGIN TRANSACTION", NULL , NULL, &err_msg);
    if (rc != SQLITE_OK) {
      printf("begin transaction issue %s\n",sqlite3_errmsg(db));
      sqlite3_free(err_msg);
      exit(9);
    }
    listdir(argv[1], 0, entries, status.st_ino,0,0);
    sqlite3_exec(db,"END TRANSACTION",NULL, NULL, &err_msg);
        sqlite3_finalize(reso);
    sqlite3_free(err_msg);
    sqlite3_close(db);
    return 0;
}
 
