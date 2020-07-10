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



#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "dbutils.h"

void sub_help() {
   printf("attach_name              name to use when attaching database files\n");
   printf("table_name               name of view table = 'v<table_name>'\n");
   printf("SQL                      arbitrary SQL on each DB (unified into single view)\n");
   printf("\n");
}

int print_callback(void *args, int count, char **data, char **columns) {
    size_t * rows = (size_t *) args;

    if (!*rows) {
        if (in.printheader) {
            for(int i = 0; i < count; i++) {
                fprintf(stdout, "%s%c", columns[i], in.delim[0]);
            }
            fprintf(stdout, "\n");
        }
    }

    if (in.printrows) {
        for(int i = 0; i < count; i++) {
            fprintf(stdout, "%s%c", data[i], in.delim[0]);
        }
        fprintf(stdout, "\n");
    }

    (*rows)++;

    return 0;
}

int main(int argc, char *argv[])
{
    char *dbname = NULL;
    char *tablename = NULL;
    char *rsqlstmt = NULL;
    sqlite3 *db = NULL;
    int rc = 0;

    const char* pos_args = "attach_name table_name SQL DB_name [DB_name ...]";
    int idx = parse_cmd_line(argc, argv, "hHNVp", 4, pos_args, &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        // parse positional args following the options
        dbname = argv[idx++];
        tablename = argv[idx++];
        rsqlstmt = argv[idx++];
    }

    if (!(db = opendb(":memory:", RDWR, 1, 1,
                      NULL, NULL
                      #ifdef DEBUG
                      , NULL, NULL
                      , NULL, NULL
                      , NULL, NULL
                      , NULL, NULL
                      #endif
              ))) {
        fprintf(stderr, "Error: Unable to open in-memory database.\n");
        return -1;
    }

   // add query funcs to get path() uidtouser() gidtogroup()
   addqueryfuncs(db, 0, -1, NULL);

   const int start = idx;
   const int numdbs = argc - idx;

   /* check if number of databases provided can be attached*/
   const int attach_limit = sqlite3_limit(db, SQLITE_LIMIT_ATTACHED, -1);
   if (attach_limit < numdbs) {
       fprintf(stderr, "Error: Cannot attach %d database files (max %d)\n", numdbs, attach_limit);
       rc = 1;
       goto done;
   }

   /* just zero out the global path so path() for this query is useless */
   memset(gps[0].gpath, 0, sizeof(gps[0].gpath));

   /* length of a single "SELECT * FROM %s.%s UNION ALL" */
   const size_t single_db_len = strlen(" SELECT * FROM ") +
                                strlen(dbname) +            /* %s */
                                3 +                         /* %d (max 125, so 3 chars) */
                                1 +                         /* .  */
                                strlen(tablename) +         /* %s */
                                strlen(" UNION ALL");

   /* overestimate size to allocate for CREATE TEMP VIEW string */
   const size_t create_len = strlen("CREATE TEMP VIEW v") +
                             strlen(tablename) +            /* %s */
                             strlen(" AS") +
                             (numdbs * single_db_len) +
                             1;                             /* NULL terminator */

   char * create_view = malloc(create_len);                 /* buffer for CREATE TEMP VIEW string */
   char * dbn = malloc(strlen(dbname) + 3 + 1);             /* buffer for database attach name    */

   char *curr = create_view;
   curr += SNPRINTF(curr, create_len, "CREATE TEMP VIEW v%s AS", tablename);

   /* build actual CREATE TEMP VIEW string */
   idx = start;
   for(int i = 0; idx < argc; idx++) {
       SNPRINTF(dbn, MAXSQL, "%s%d", dbname, i);
       curr += SNPRINTF(curr, MAXSQL, " SELECT * FROM %s.%s UNION ALL", dbn, tablename);

       // attach individual database files
       if (!attachdb(argv[idx], db, dbn, RDONLY)) {
           rc = 1;
           goto detach;
       }

       i++;
   }

   curr -= strlen(" UNION ALL");   /* remove last " UNION ALL" */
   *curr = '\0';

   char * err = NULL;

   /* create the view */
   if (sqlite3_exec(db, create_view, NULL, NULL, &err) != SQLITE_OK) {
       fprintf(stderr, "Error: Cannot create view with databases: %s\n", err);
       sqlite3_free(err);
       rc = 1;
       goto detach;
   }

   /* run the user query */
   size_t records = 0;
   if (sqlite3_exec(db, rsqlstmt, print_callback, &records, &err) != SQLITE_OK) {
       fprintf(stderr, "Error: User query failed: %s\n", err);
       sqlite3_free(err);
       rc = 1;
       goto detach;
   }

   printf("query returned %zu records\n", records);

  detach:
   /* /\* detach each database file *\/ */
   /* /\* not strictly necessary *\/ */
   /* idx = start; */
   /* for(int i = 0; idx < argc; idx++) { */
   /*     char dbn[MAXSQL]; */
   /*     SNPRINTF(dbn, MAXSQL, "%s%d", dbname, i); */
   /*     detachdb(argv[i], db, dbn); */

   /*     i++; */
   /* } */

   free(dbn);
   free(create_view);

  done:

   closedb(db);

   return rc;
}
