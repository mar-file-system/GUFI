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

void sub_help() {
   printf("DB_path           path to dir containinng db.db.*\n");
   printf("SQL               arbitrary SQL on DB\n");
   printf("\n");
}

int main(int argc, char *argv[])
{
     char name[MAXPATH];
     char shortname[MAXPATH];
     char endname[MAXPATH];
     char dbname[MAXPATH];
     char rsqlstmt[MAXSQL];
     struct stat statuso;
     int printpath = 0;
     int rc;
     sqlite3 *db;
     sqlite3 *db1;
     int recs;
     int dirsummary = 0;        // rawquerydb() ignores this argument

     // sprintf(name,"%s",argv[1]);
     // sprintf(rsqlstmt,"%s",argv[2]);
     // printpath=atoi(argv[3]);
     // printheader=atoi(argv[4]);
     // dirsummary=atoi(argv[5]);

     int idx = parse_cmd_line(argc, argv, "hHNV", 2, "[-s] DB_path SQL");
     if (in.helped)
        sub_help();
     if (idx < 0)
        return -1;
     else {
        // parse our custom option '-s'
        if ((argc - idx) > 2) {
           if (strcmp(argv[idx], "-s")) {
              print_help(argv[0], "hHNV", "[-s] DB_path SQL");
              return -1;
           }
           dirsummary = 1;
           ++idx;
        }

        // parse positional args following the options
        int retval = 0;
        INSTALL_STR(name,     argv[idx++], MAXPATH, "<DB_path>");
        INSTALL_STR(rsqlstmt, argv[idx++], MAXSQL,  "<SQL>");
        if (retval)
           return retval;
     }
     printf("processing query name %s\n", name);


     // assure we have access to the directory
     rc=lstat(name,&statuso);
     if (rc != 0) {
        printf("ERROR: directory %s: %s\n", name, strerror(errno));
       return 1;
     }
     if (!S_ISDIR(statuso.st_mode)) {    
       printf("ERROR: %s not a directory\n",name);
       return 1;
     }

     // assure we have access to the DB
     sprintf(dbname,"%s/db.db",name);
     rc=lstat(dbname,&statuso);
     if (rc != 0) {
        printf("ERROR:  db %s: %s\n", dbname, strerror(errno));
        return 1;
     }

     
     // run the query
     db = opendb(name,db1,0,0);

     //add query funcs to get uidtouser() gidtogroup() and path()
     addqueryfuncs(db);

     // set the global path so path() is the path passed in
     bzero(endname,sizeof(endname));
     shortpath(name,shortname,endname);
     //printf("shortpath out shortname %s end name %s\n",shortname, endname);

     // per-thread
     sprintf(gps[0].gepath,"%s",endname); 
     if (dirsummary)
        sprintf(gps[0].gpath,"%s",shortname); 
     else
        sprintf(gps[0].gpath,"%s",name); 

     recs=rawquerydb(name, dirsummary, db, rsqlstmt, in.printing, in.printheader, in.printrows, 0);
     if (recs >= 0)
        printf("query returned %d records\n",recs);
     else
        printf("query returned error\n");

     closedb(db);



     return 0;
}
