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



#include <string.h>

#include "dbutils.h"
#include "external.h"
#include "utils.h"

const char EXTERNAL_DBS_PWD_CREATE[] =
    "DROP TABLE IF EXISTS " EXTERNAL_DBS_PWD ";"
    "CREATE TABLE " EXTERNAL_DBS_PWD "(filename TEXT, attachname TEXT, mode INT64, uid INT64, gid INT64, PRIMARY KEY(filename, attachname));";

const char EXTERNAL_DBS_PWD_INSERT[] =
    "INSERT INTO " EXTERNAL_DBS_PWD " VALUES (@filename, @attachname, @mode, @uid, @gid);";

const char EXTERNAL_DBS_ROLLUP_CREATE[] =
    "CREATE TABLE " EXTERNAL_DBS_ROLLUP "(filename TEXT, attachname TEXT, mode INT64, uid INT64, gid INT64, PRIMARY KEY(filename, attachname));";

const char EXTERNAL_DBS_ROLLUP_INSERT[] =
    "INSERT INTO " EXTERNAL_DBS_ROLLUP " VALUES (@filename, @attachname, @mode, @uid, @gid);";

const char EXTERNAL_DBS_CREATE[] =
    "DROP VIEW IF EXISTS " EXTERNAL_DBS ";"
    "CREATE VIEW " EXTERNAL_DBS " AS SELECT * FROM " EXTERNAL_DBS_PWD " UNION SELECT * FROM " EXTERNAL_DBS_ROLLUP ";";

static const char EXTERNAL_DBS_GET[] = "SELECT filename, attachname FROM " EXTERNAL_DBS ";";

int external_loop(struct work *work, sqlite3 *db,
                  const char *viewname, const size_t viewname_len,
                  const char *select, const size_t select_len,
                  const char *tablename, const size_t tablename_len,
                  size_t (*modify_filename)(char *dst, const size_t dst_size,
                                            const char *src, const size_t src_len,
                                            struct work *work)
                  #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                  , size_t *query_count
                  #endif
    ) {
    int           rec_count = 0;
    sqlite3_stmt *res = NULL;
    /* not checking if the view exists - if it does, it's an error and will error */
    char          unioncmd[MAXSQL];
    char         *unioncmdp = unioncmd + SNFORMAT_S(unioncmd, sizeof(unioncmd), 3,
                                                    "CREATE TEMP VIEW ", (size_t) 17,
                                                    viewname, viewname_len,
                                                    " AS", 3);

    /* step through each external file recorded in the main database */
    int error = sqlite3_prepare_v2(db, EXTERNAL_DBS_GET, MAXSQL, &res, NULL);
    if (error != SQLITE_OK) {
        fprintf(stderr, "Error: Could not get external file names from table %s: %d err %s\n",
                EXTERNAL_DBS, error, sqlite3_errmsg(db));
        return -1;
    }

    while (sqlite3_step(res) == SQLITE_ROW) {
        const char *src_filename = (const char *) sqlite3_column_text(res, 0);
        const char *attachname   = (const char *) sqlite3_column_text(res, 1);

        /*
         * modify the stored filename in case it needs changing
         * the full path might have been removed to not store the
         * same prefix repeatedly
         */
        const char *attfile = src_filename;
        char dst_filename[MAXSQL];
        if (modify_filename) {
            modify_filename(dst_filename, sizeof(dst_filename),
                            src_filename, strlen(src_filename),
                            work);
            attfile = dst_filename;
        }

        /* if attach fails, you don't have access to the database - just continue */
        if (attachdb(attfile, db, attachname, SQLITE_OPEN_READONLY, 0)) {
            /* SELECT * FROM <attach name>.xattrs_avail UNION */
            unioncmdp += SNFORMAT_S(unioncmdp, sizeof(unioncmd) - (unioncmdp - unioncmd), 6,
                                    select, select_len,
                                    "'", (size_t) 1,
                                    attachname, strlen(attachname),
                                    "'.", (size_t) 2,
                                    tablename, tablename_len,
                                    " UNION", (size_t) 6);
            rec_count++;
        }
    }
    sqlite3_finalize(res);

    /*
     * close SQL statement using the table in db.db
     * i.e.
     *    ... UNION SELECT * FROM <table name>;
     */
    SNFORMAT_S(unioncmdp, sizeof(unioncmd) - (unioncmdp - unioncmd), 3,
               select, select_len,
               tablename, tablename_len,
               ";", (size_t) 1);

    /* create view */
    if (sqlite3_exec(db, unioncmd, NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "Error: Create external data view \"%s\" failed: %s: %s\n",
                viewname, sqlite3_errmsg(db), unioncmd);
        return -1;
    }

    return rec_count;
}

static int external_detach(void *args, int count, char **data, char **columns) {
    (void) count; (void) (columns);

    detachdb(NULL, (sqlite3 *) args, data[1], 0); /* don't check for errors */
    return 0;
}

void external_done(sqlite3 *db, const char *drop_view
                   #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                   , size_t *query_count
                   #endif
    )
{
    sqlite3_exec(db, drop_view, NULL, NULL, NULL);
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    (*query_count)++;
    #endif

    sqlite3_exec(db, EXTERNAL_DBS_GET, external_detach, db, NULL);
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    (*query_count)++;
    #endif
}
