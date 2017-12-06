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

#include "bf.h"
#include "structq.h"
#include "utils.h"
#include "dbutils.h"


int main(int argc, char *argv[])
{
     char name[MAXPATH];
     char shortname[MAXPATH];
     char dbname[MAXPATH];
     char rsqlstmt[MAXSQL];
     struct stat statuso;
     int printpath = 0;
     int rc;
     sqlite3 *db;
     sqlite3 *db1;
     int recs;
     int printheader=0;
     int dirsummary;

     sprintf(name,"%s",argv[1]);
     sprintf(rsqlstmt,"%s",argv[2]);
     printpath=atoi(argv[3]);
     printheader=atoi(argv[4]);
     dirsummary=atoi(argv[5]);

     printf("processing query name %s  printpath %d\n",name, printpath);
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
     sprintf(dbname,"%s/db.db",name);
     rc=lstat(dbname,&statuso);
     if (rc != 0) {
       printf("issue with db name %s\n",dbname);
     } else {
       db = opendb(name,db1,0,0);
       //add query funcs to get uidtouser() gidtogroup() and path()
       addqueryfuncs(db);
       // set the global path so path() is the path passed in
       if (dirsummary) {
         shortpath(name,shortname);
         sprintf(gps[0].gpath,"%s",shortname); 
       } else {
         sprintf(gps[0].gpath,"%s",name); 
       }

       recs=rawquerydb(name, dirsummary, db, rsqlstmt, printpath, printheader,1,0);
       if (recs >= 0) {
         printf("query returned %d records\n",recs);
       } else {
         printf("query returned error\n");
       }

       closedb(db);

     }

     return 0;
}
