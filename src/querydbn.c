/*
This file is part of GUFI, which is part of MarFS, which is released
under the BSD license.


Copyright (c) 2017, Los Alamos National Security (LANS), LLC
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


From Los Alamos National Security, LLC:
LA-CC-15-039

Copyright (c) 2017, Los Alamos National Security, LLC All rights reserved.
Copyright 2017. Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use,
reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
modified to produce derivative works, such modified software should be
clearly marked, so as not to confuse it with the version available from
LANL.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/



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

#include <pwd.h>
#include <grp.h>

#include "bf.h"
#include "utils.h"
#include "dbutils.h"

extern int errno;

void sub_help() {
   printf("  -s              dir-summary (currently-unused internal functionality)\n");
   // printf("  -N <DB_count>   query DBs written by multiple threads (\n");}
   printf("\n");
   printf("DB_path           path to dir containinng %s.*\n",DBNAME);
   printf("DB_count          number of DBs (should match thread-count used in 'bfq')\n");
   printf("SQL               arbitrary SQL on each DB (unified into single view)\n");
   printf("table_name        name of view table = 'v<table_name>'\n");
   printf("\n");
}

int main(int argc, char *argv[])
{
   char name[MAXPATH];
   char rsqlstmt[MAXSQL];
   int rc;
   sqlite3 *db;
   int recs;
   int dirsummary = 0;
   int numdbs = 0;
   int i;
   char dbnam[MAXPATH];
   char dbn[MAXPATH];
   char sqlu[MAXSQL];
   char sqlat[MAXSQL];
   char tabnam[MAXPATH];
   char up[MAXSQL];
   char *err_msg = 0;

   // sprintf(name,"%s",argv[1]);
   // sprintf(rsqlstmt,"%s",argv[2]);
   // printpath=atoi(argv[3]);
   // printheader=atoi(argv[4]);
   // dirsummary=atoi(argv[5]);
   // numdbs=atoi(argv[6]);
   // sprintf(tabnam,"%s",argv[7]);

   const char* pos_args = "[-s] DB_path DB_count SQL tabname";
   int idx = parse_cmd_line(argc, argv, "hHNVp", 4, pos_args, &in);
   if (in.helped)
      sub_help();
   if (idx < 0)
      return -1;
   else {
      // parse our custom option '-s'
      if ((argc - idx) > 4) {
         if (strcmp(argv[idx], "-s")) {
            print_help(argv[0], "hHNVp", pos_args);
            sub_help();
            return -1;
         }
         dirsummary = 1;
         ++idx;
      }

      // parse positional args following the options
      int retval = 0;
      INSTALL_STR(name,     argv[idx++], MAXPATH,       "<DB_path>");
      INSTALL_INT(numdbs,   argv[idx++], 1, MAXPTHREAD, "<DB_count>");
      INSTALL_STR(rsqlstmt, argv[idx++], MAXSQL,        "<SQL>");
      INSTALL_STR(tabnam,   argv[idx++], MAXPATH,       "<tabname>");
      if (retval)
         return retval;
   }


   //printf("processing query name %s  numb dbs %d\n",name, numdbs);
   if (!(db = opendb(name, RDWR, 1, 1,
               NULL, NULL
               #ifdef DEBUG
               , NULL, NULL
               , NULL, NULL
               , NULL, NULL
               , NULL, NULL
               #endif
             ))) {
       fprintf(stderr, "Error: Unable to create database file \"%s\".\n", name);
       return -1;
   }

   // add query funcs to get path() uidtouser() gidtogroup()
   addqueryfuncs(db, 0);

   // just zero out the global path so path() for this query is useless
   memset(gps[0].gpath, 0, sizeof(gps[0].gpath));

   SNPRINTF(sqlu,MAXSQL,"create temp view v%s as ",tabnam);
   i=0;
   while (i < numdbs) {
      SNPRINTF(dbnam,MAXPATH,"%s.%d",name,i);
      SNPRINTF(dbn,MAXPATH,"%s%d",name,i);
      if (i != (numdbs-1))
         SNPRINTF(up,MAXSQL,"select * from %s.%s union all ",dbn,tabnam);
      else
         SNPRINTF(up,MAXSQL,"select * from %s.%s;",dbn,tabnam);
      strcat(sqlu,up);
      SNPRINTF(sqlat,MAXSQL,"ATTACH \'%s\' as %s",dbnam,dbn);
      //printf("ATTACH \'%s\' as %s\n",dbnam,dbn);
      rc=sqlite3_exec(db, sqlat,0, 0, &err_msg);
      if (rc != SQLITE_OK) {
         fprintf(stderr, "Cannot attach database: %s %s\n", dbnam, sqlite3_errmsg(db));
         sqlite3_close(db);
         exit(9);
      }

      i++;
   }
   //create view concat as select * from d1.summary union all d2.summary
   //…… union all d10.summary;

   //printf("sqlu: %s\n",sqlu);
   rawquerydb(dbnam, dirsummary, db, sqlu,
              in.printing, in.printheader, in.printrows,0);

   //printf("after union running %s\n",rsqlstmt);
   recs=rawquerydb(dbnam, dirsummary, db, rsqlstmt,
                   in.printing, in.printheader, in.printrows, 0);
   if (recs >= 0)
      printf("query returned %d records\n",recs);
   else
      printf("query returned error\n");

   i=0;
   while (i < numdbs) {
      SNPRINTF(dbnam,MAXPATH,"%s.%d",name,i);
      SNPRINTF(dbn,MAXPATH,"%s%d",name,i);

      //detachdb(dbnam,db,dbn);
      SNPRINTF(sqlat,MAXSQL,"DETACH %s",dbn);
      rc=sqlite3_exec(db, sqlat,0, 0, &err_msg);
      if (rc != SQLITE_OK) {
         fprintf(stderr, "Cannot detach database: %s/%s %s\n", name, DBNAME,sqlite3_errmsg(db));
         sqlite3_close(db);
         exit(9);
      }
      i++;
   }


   return 0;
}
