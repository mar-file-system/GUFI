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



#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "dbutils.h"

#define ATTACH_PREFIX               "querydbs_src"
#define ATTACH_PREFIX_LEN           sizeof(ATTACH_PREFIX) - 1

/*
 * not putting ATTACH_PREFIX into SELECT FROM because
 * the attach name is needed for attachdb
 */
const char   SELECT_FROM[]        = " SELECT * FROM ";
const size_t SELECT_FROM_LEN      = sizeof(SELECT_FROM) - 1;
const char   UNION_ALL[]          = " UNION ALL";
const size_t UNION_ALL_LEN        = sizeof(UNION_ALL) - 1;
const char   CREATE_TEMP_VIEW[]   = "CREATE TEMP VIEW v";
const size_t CREATE_TEMP_VIEW_LEN = sizeof(CREATE_TEMP_VIEW) - 1;
const char   AS[]                 = " AS";
const size_t AS_LEN               = sizeof(AS) - 1;

static void sub_help(void) {
   printf("table_name               name of table in database file to attach; also used for view table name: 'v<table_name>'\n");
   printf("SQL                      arbitrary SQL executed on view\n");
   printf("DB_name                  path of source database file(s) to add to view");
   printf("\n");
}

struct CallbackArgs {
    struct input *in;
    size_t rows;
};

static int print_callback(void *args, int count, char **data, char **columns) {
    struct CallbackArgs *ca = (struct CallbackArgs *) args;

    if (!ca->rows) {
        if (ca->in->printheader) {
            for(int i = 0; i < count; i++) {
                fprintf(stdout, "%s%c", columns[i], ca->in->delim);
            }
            fprintf(stdout, "\n");
        }
    }

    if (ca->in->printrows) {
        for(int i = 0; i < count; i++) {
            fprintf(stdout, "%s%c", data[i], ca->in->delim);
        }
        fprintf(stdout, "\n");
    }

    ca->rows++;

    return 0;
}

int main(int argc, char *argv[])
{
    struct input in;
    char *tablename = NULL;
    char *rsqlstmt = NULL;
    sqlite3 *db = NULL;

    const char* pos_args = "table_name SQL DB_name [DB_name ...]";
    int idx = parse_cmd_line(argc, argv, "hHNVd:", 3, pos_args, &in);
    if (in.helped)
        sub_help();
    if (idx < 0) {
        input_fini(&in);
        return EXIT_FAILURE;
    }

    // parse positional args following the options
    tablename = argv[idx++];
    rsqlstmt = argv[idx++];

    const size_t tablename_len = strlen(tablename);

    if (!(db = opendb(SQLITE_MEMORY, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, 1, 1, NULL, NULL))) {
        fprintf(stderr, "Error: Unable to open in-memory database.\n");
        input_fini(&in);
        return EXIT_FAILURE;
    }

   // add query funcs to get uidtouser() gidtogroup()
   addqueryfuncs(db);

   int rc = EXIT_SUCCESS;

   const int start = idx;
   const int numdbs = argc - idx;

   /* /\* check if number of databases provided can be attached *\/ */
   /* const int attach_limit = sqlite3_limit(db, SQLITE_LIMIT_ATTACHED, -1); */
   /* if (attach_limit < numdbs) { */
   /*     fprintf(stderr, "Error: Cannot attach %d database files (max %d)\n", numdbs, attach_limit); */
   /*     rc = EXIT_FAILURE; */
   /*     goto done; */
   /* } */

   /* length of a single " SELECT * FROM <attach prefix>%d.<tablename> UNION ALL" */
   const size_t single_db_len = SELECT_FROM_LEN +
                                ATTACH_PREFIX_LEN +
                                3 +                         /* max 254, so 3 chars */
                                1 +                         /* .  */
                                tablename_len +
                                UNION_ALL_LEN;

   /* allocation size for "CREATE TEMP VIEW v<tablename> AS ..." */
   const size_t create_view_len = CREATE_TEMP_VIEW_LEN +
                                  tablename_len +
                                  AS_LEN +
                                  (numdbs * single_db_len) +
                                  1 +                       /* semicolon */
                                  1;                        /* NULL terminator */

   char *create_view = malloc(create_view_len);             /* buffer for CREATE TEMP VIEW string */
   char *curr = create_view;
   curr += SNFORMAT_S(curr, create_view_len, 3,
                      CREATE_TEMP_VIEW, CREATE_TEMP_VIEW_LEN,
                      tablename, tablename_len,
                      AS, AS_LEN);

   /* init with prefix since it's the same each time */
   char dbn[ATTACH_PREFIX_LEN + 3 + 1] = ATTACH_PREFIX;     /* buffer for <attach prefix>ddd + NULL terminator */

   /* build actual CREATE TEMP VIEW string */
   idx = start;
   for(int i = 0; idx < argc; idx++) {
       SNPRINTF(dbn + ATTACH_PREFIX_LEN, 4, "%03d", i);

       /* attach individual database file */
       if (!attachdb(argv[idx], db, dbn, SQLITE_OPEN_READONLY, 1)) {
           rc = EXIT_FAILURE;
           goto detach;
       }

       /* append SQL for adding the contents of the current database into the view */
       curr += SNFORMAT_S(curr, create_view_len - (curr - create_view), 5,
                          SELECT_FROM, SELECT_FROM_LEN,
                          dbn, ATTACH_PREFIX_LEN + 3,
                          ".", (size_t) 1,
                          tablename, tablename_len,
                          UNION_ALL, UNION_ALL_LEN);

       i++;
   }

   curr -= UNION_ALL_LEN;   /* remove last " UNION ALL" since there's nothing to union with */
   curr[0] = ';';
   curr[1] = '\0';

   char * err = NULL;

   /* create the view */
   if (sqlite3_exec(db, create_view, NULL, NULL, &err) != SQLITE_OK) {
       sqlite_print_err_and_free(err, stderr, "Error: Cannot create view with databases: %s\n", err);
       rc = EXIT_FAILURE;
       goto detach;
   }

   /* run the user query */
   struct CallbackArgs ca = {
       .in = &in,
       .rows = 0,
   };
   if (sqlite3_exec(db, rsqlstmt, print_callback, &ca, &err) == SQLITE_OK) {
       printf("query returned %zu records\n", ca.rows);
   }
   else {
       sqlite_print_err_and_free(err, stderr, "Error: User query failed: %s\n", err);
       rc = EXIT_FAILURE;
   }

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

   free(create_view);

  /* done: */

   closedb(db);
   input_fini(&in);

   return rc;
}
