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

#include "bf.h"
#include "utils.c"
 
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

    //printf("inlistdir name %s\n",name);
    if (!(dir = opendir(name)))
        return;
    if (statit) lstat(name,&st);
    bzero(xattr, sizeof(xattr));
    xattrs=0;
    if (xattrit) {
      xattrs=pullxattrs(name,xattr);
    }
    if (statit+xattrit > 0) {
      bzero(lpath,sizeof(lpath));
      bzero(type,sizeof(type));
      //printf("%lld d ", level);
      sprintf(type,"d");
      printit(name,&st,type,lpath,xattrs,xattr,1,pin);
    } else {
      printf("d %s %lld %lld\n", name, pin , level);
    }
    ppin=pin;
    while ((entry = (readdir(dir)))) {
       int len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
       path[len] = 0;
       if (statit) lstat(path,&st);
       xattrs=0;
       bzero(xattr, sizeof(xattr));
       if (xattrit) {
         xattrs=pullxattrs(path,xattr);
       }
        //if (entry->d_type == DT_DIR) {
        if (S_ISDIR(st.st_mode) ) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;
            sprintf(type,"d");
            //printf("inwhile d %s %lld %lld\n", name, entry->d_ino, ppin);
            listdir(path, pin, entry, entry->d_ino, statit, xattrit);
        } else {
            len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
            bzero(lpath,sizeof(lpath));
            //if (entry->d_type == DT_REG) {
            if (S_ISREG(st.st_mode)) {
               sprintf(type,"f");
            }
            if (S_ISLNK(st.st_mode)) {
               sprintf(type,"l");
               if (statit) readlink(path,lpath,MAXPATH);
            }
            if (statit+xattrit > 0) {
              //printf("%lld ", pin);
              //bzero(type,sizeof(type));
              //printf("readlink %s %s\n",path,lpath);
              printit(path,&st,type,lpath,xattrs,xattr,1,pin);
            } else {
              printf("%s %s %lld %lld\n",type, path, entry->d_ino,pin);
            }
        }
    }
    closedir(dir);
}
 
int main(int argc, char *argv[])
{
    struct dirent *entries;
    struct stat status;
    int statit;
    int xattrit;
    statit=atoi(argv[2]);
    xattrit=atoi(argv[3]);
    lstat(argv[1],&status);
    //printf("inmain d %s %lld 0\n", argv[1],status.st_ino);
    listdir(argv[1], 0, entries, status.st_ino,statit,xattrit);
    return 0;
}
 
