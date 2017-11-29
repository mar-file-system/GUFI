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
#include "dbutils.c"

int main(int argc, char *argv[])
{
     char name[MAXPATH];
     char dbname[MAXPATH];
     char rsqlstmt[MAXSQL];
     struct stat statuso;
     int printpath = 0;
     int rc;
     sqlite3 *db;
     sqlite3 *db1;
     int recs;
     int printheader=0;
     struct sum sumout;
     int dirsummary;
     int numdbs;
     int i;
     char dbnam[MAXPATH];
     char dbn[MAXPATH];
     char sqlu[MAXSQL];
     char sqlat[MAXSQL];
     char tabnam[MAXPATH];
     char up[MAXPATH];
     char *err_msg = 0;

     sprintf(name,"%s",argv[1]);
     sprintf(rsqlstmt,"%s",argv[2]);
     printpath=atoi(argv[3]);
     printheader=atoi(argv[4]);
     dirsummary=atoi(argv[5]);
     numdbs=atoi(argv[6]);
     sprintf(tabnam,"%s",argv[7]);

     printf("processing query name %s  numb dbs %d\n",name, numdbs);
     sprintf(dbname,"%s",name);
     db = opendb(name,db1,5,0);
   
     bzero(sqlu,sizeof(sqlu));
     sprintf(sqlu,"create temp view v%s as ",tabnam);
     bzero(up,sizeof(up));
     i=0;
     while (i < numdbs) {
         sprintf(dbnam,"%s.%d",name,i);
         sprintf(dbn,"%s%d",name,i);
         if (i != (numdbs-1)) {
           sprintf(up,"select * from %s.%s union all ",dbn,tabnam);
         } else {
           sprintf(up,"select * from %s.%s;",dbn,tabnam);
         }
         strcat(sqlu,up);
           sprintf(sqlat,"ATTACH \'%s\' as %s",dbnam,dbn);
           rc=sqlite3_exec(db, sqlat,0, 0, &err_msg);
           if (rc != SQLITE_OK) {
              fprintf(stderr, "Cannot attach database: %s %s\n", dbnam, sqlite3_errmsg(db));
              sqlite3_close(db);
              exit(9);
            }

         i++;
     }
//create view concat as select * from d1.summary union all d2.summary …… union all d10.summary;
       //printf("sqlu: %s\n",sqlu);
       rawquerydb(dbnam, dirsummary, db, sqlu, printpath, printheader,1,0);
       //printf("after union running %s\n",rsqlstmt);
       recs=rawquerydb(dbnam, dirsummary, db, rsqlstmt, printpath, printheader,1,0);
       if (recs >= 0) {
         printf("query returned %d records\n",recs);
       } else {
         printf("query returned error\n");
       }

       i=0;
       while (i < numdbs) {
         sprintf(dbnam,"%s.%d",name,i);
         sprintf(dbn,"%s%d",name,i);
         //detachdb(dbnam,db,dbn); 
         sprintf(sqlat,"DETACH %s",dbn);
         rc=sqlite3_exec(db, sqlat,0, 0, &err_msg);
         if (rc != SQLITE_OK) {
           fprintf(stderr, "Cannot detach database: %s/db.db %s\n", name, sqlite3_errmsg(db));
           sqlite3_close(db);
           exit(9);
         }
         i++;
       }


     return 0;
}
