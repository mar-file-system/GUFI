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
#include "structq.h"
#include "dbutils.h"
#include "utils.h"

int main(int argc, char *argv[])
{
     char name[MAXPATH];
     char dbname[MAXPATH];
     int printent = 0;
     struct stat statuso;
     int rc;
     sqlite3 *tsdb;
     sqlite3 *db3;
     sqlite3 *sdb;
     sqlite3 *db2;
     sqlite3 *db;
     sqlite3 *db1;
     char sqlstmt[MAXSQL];
     int recs;
     struct sum sumout;

     sprintf(name,"%s",argv[1]);
     printent=atoi(argv[2]);

     printf("dirname %s printent %d\n",name, printent);
     /* process input directory */
     rc=lstat(name,&statuso);
     if (rc != 0) {
       printf("issue with directory name %s\n",name);
       return 1;
     }
     if (!S_ISDIR(statuso.st_mode)) {    
       printf("name %s not a directory\n",name);
       return 1;
     }
     sprintf(dbname,"%s/treesummary.db",name);
     rc=lstat(dbname,&statuso);
     if (rc != 0) {
       printf("issue with treessummary name %s\n",dbname);
     } else {
       tsdb = opendb(name,"",&statuso,db3,2,0);
       zeroit(&sumout);
       querytsdb(name,&sumout,tsdb,&recs,1);
       closedb(tsdb);
       printf("tsdb: \n");
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
       printf("totsubdirs %lld maxsubdirfiles %lld maxsubdirlinks %lld maxsubdirsize %lld\n",sumout.totsubdirs,sumout.maxsubdirfiles,sumout.maxsubdirlinks,sumout.maxsubdirsize);
     } 
     sprintf(dbname,"%s/summary.db",name);
     rc=lstat(dbname,&statuso);
     if (rc != 0) {
       printf("issue with summary name %s\n",dbname);
       return 1;
     } else {
       sdb = opendb(name,"",&statuso,db2,0,0);
       zeroit(&sumout);
       querytsdb(name,&sumout,sdb,&recs,0);
       closedb(sdb);
       printf("sdb: \n");
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
     }
     if (printent) {
       sprintf(dbname,"%s/entries.db",name);
       rc=lstat(dbname,&statuso);
       if (rc != 0) {
         printf("issue with entries name %s\n",dbname);
         return 1;
       } else {
         db = opendb(name,"",&statuso,db1,1,0);
         printf("ents:\n");
         sprintf(sqlstmt,"select name,\'type\',type,\'inode\',inode,\'mode\',mode,\'nlink\',nlink,\'uid\',uid,\'gid\',gid,\'size\',size,\'blksize\',blksize,\'blocks\',blocks,\'atime\',atime,\'mtime\',mtime,\'ctime\',ctime,\'linkname\',linkname,\'xattrs\',xattrs from entries;");
         querydb(name,&statuso,db,"d","",0,"",sqlstmt,&recs,0,0,0,0,1);
         closedb(db);
       }
     }

     return 0;
}
