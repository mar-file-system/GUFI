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



#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "dbutils.h"
#include "print.h"

#include "gufi_find_outliers/handle_db.h"

static int create_outliers_table(const char *name, sqlite3 *db, void *args) {
    (void) args;
    return (create_table_wrapper(name, db, OUTLIERS_TABLE, OUTLIERS_CREATE) != SQLITE_OK);
}

sqlite3 *db_init(const char *dbname) {
    return opendb(dbname, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 0, 0,
                  create_outliers_table, NULL);
}

int intermediate_dbs_init(struct PoolArgs *pa) {
    pa->dbs = calloc(pa->in.maxthreads, sizeof(sqlite3 *));
    if (!pa->dbs) {
        const int err = errno;
        fprintf(stderr, "Error: Could not allocate space for %zu db handles: %s (%d)\n",
                pa->in.maxthreads, strerror(err), err);
        return -1;
    }

    for(size_t i = 0; i < pa->in.maxthreads; i++) {
        char dbname[MAXSQL];
        SNPRINTF(dbname, sizeof(dbname), INTERMEDIATE_DBNAME_FORMAT, i);

        pa->dbs[i] = db_init(dbname);
        if (!pa->dbs[i]) {
            intermediate_dbs_fini(pa);
            return -1;
        }
    }

    return 0;
}

void intermediate_dbs_fini(struct PoolArgs *pa) {
    for(size_t i = 0; i < pa->in.maxthreads; i++) {
        sqlite3_close(pa->dbs[i]);
    }
    free(pa->dbs);
}

int insert_outlier(sqlite3_stmt *stmt, const str_t *path, const size_t level,
                   const char *outlier_type, const refstr_t *col,
                   const Stats_t *t, const Stats_t *s) {
    sqlite3_bind_text(stmt,    1, path->data,   path->len, SQLITE_STATIC); /* valid until after all paths are inserted */
    sqlite3_bind_int64(stmt,   2, level);

    sqlite3_bind_text(stmt,    3, outlier_type, -1,        SQLITE_STATIC); /* static value */
    sqlite3_bind_text(stmt,    4, col->data,    col->len,  SQLITE_STATIC); /* reference to argv[i] */

    sqlite3_bind_double(stmt,  5, t->value);
    sqlite3_bind_double(stmt,  6, t->mean);
    sqlite3_bind_double(stmt,  7, t->stdev);

    sqlite3_bind_double(stmt,  8, s->value);
    sqlite3_bind_double(stmt,  9, s->mean);
    sqlite3_bind_double(stmt, 10, s->stdev);

    const int ret = sqlite3_step(stmt);

    sqlite3_reset(stmt);

    if (ret == SQLITE_DONE) {
        sqlite3_clear_bindings(stmt); /* NULL will cause invalid read here */
    }
    else {
        sqlite_print_err_and_free(NULL, stderr, "SQL insert outlier step: %s error %d err %s\n",
                                  path->data, ret, sqlite3_errstr(ret));
        return 1;
    }

    return 0;
}

static int num_rows(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    return !(sscanf(data[0], "%zu", (size_t *) args) == 1);
}

void write_results(struct PoolArgs *pa, size_t *rows) {
    *rows = 0;

    if (pa->in.output == OUTDB) {
        #define attachname "intermediate"

        /* combine results into one database file */
        sqlite3 *db = db_init(pa->in.outname.data);
        for(size_t i = 0; i < pa->in.maxthreads; i++) {
            char dbname[MAXSQL];
            SNPRINTF(dbname, sizeof(dbname), INTERMEDIATE_DBNAME_FORMAT, i);

            if (!attachdb_raw(dbname, db, attachname, 1, 1)) {
                continue;
            }

            char *err = NULL;
            if (sqlite3_exec(db, "INSERT INTO " OUTLIERS_TABLE " SELECT * FROM " attachname "." OUTLIERS_TABLE ";",
                             NULL, NULL, &err) != SQLITE_OK) {
                sqlite_print_err_and_free(err, stderr, "Error: Could not move intermediate results into final database: %s\n",
                                          err);
            }

            detachdb(dbname, db, attachname, 1, 1);
        }

        char *err = NULL;
        if (sqlite3_exec(db, "SELECT COUNT(*) FROM " OUTLIERS_TABLE ";",
                         num_rows, rows, &err) != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr, "Error: Could not get number of rows in final database: %s\n",
                                      err);
        }

        sqlite3_close(db);
    }
    else {
        /* just print per-thread results */
        PrintArgs_t print = {
            .delim = pa->in.delim,
            .outfile = stdout,
            .newline = '\n',
            .rows = 0,
        };

        for(size_t i = 0; i < pa->in.maxthreads; i++) {
            char *err = NULL;
            if (sqlite3_exec(pa->dbs[i], "SELECT * FROM " OUTLIERS_TABLE ";",
                             print_uncached, &print, &err) != SQLITE_OK) {
                sqlite_print_err_and_free(err, stderr, "Error: Could not print intermediate results: %s\n", err);
            }
        }

        *rows = print.rows;
    }
}
