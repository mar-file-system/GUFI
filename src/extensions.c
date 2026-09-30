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



#if HAVE_AI
#include "sqlite-lembed.h"
#include "sqlite-vec.h"
#endif

#include "dbutils.h"
#include "plugin.h"

#include "gufi_query/PoolArgs.h"

int augment_db(sqlite3 *db, const sqlite3_api_routines *pApi) {
    addqueryfuncs(db);

    char *err = NULL;

    /* FIXME: figure out how to link with gufi_vt */
    #ifndef GUFI_VT
    if (sqlite3_runvt_init(db, &err, pApi) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Could not initialize runvt: %s\n", err);
        return 1;
    }
    #endif

    #if HAVE_AI
    /* load the sqlite-vec extension */
    if (sqlite3_vec_init(db, &err, pApi) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Could not initialize sqlite3-vec: %s\n", err);
        return 1;
    }

    /* load the sqlite-lembed extension */
    if (sqlite3_lembed_init(db, &err, pApi) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Could not initialize sqlite3-lembed: %s\n", err);
        return 1;
    }
    #else
    (void) err;
    (void) pApi;
    #endif

    return 0;
}

int set_up_global_db(str_t *sql, sqlite3 **db,
                     const sqlite3_api_routines *pApi,
                     const int no_print_sql_on_err) {
    *db = NULL;

    if (!str_exists(sql)) {
        return 0;
    }

    /* set up a globally available in-memory db */
    sqlite3 *rw_global_db = NULL;
    if (sqlite3_open_v2(GUFI_QUERY_GLOBAL_DB_FILENAME, &rw_global_db,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI,
                        NULL) != SQLITE_OK) {
        closedb(rw_global_db);
        return 1;
    }

    addqueryfuncs(rw_global_db);
    augment_db(rw_global_db, pApi);

    /* fill in the globally available in-memory db */
    char *err = NULL;
    if (sqlite3_exec(rw_global_db, sql->data, NULL, NULL, &err) != SQLITE_OK) {
        if (!no_print_sql_on_err) {
            sqlite_print_err_and_free(err, stderr, "Error: Could not set up global db with \"%s\": %s\n",
                                      sql->data, err);
        }
        else {
            sqlite_print_err_and_free(err, stderr, "Error: Could not set up global db: %s\n",
                                      err);
        }
        closedb(rw_global_db);
        return 1;
    }

    /* reopen the globally available db in readonly mode */
    const int rc = sqlite3_open_v2(GUFI_QUERY_GLOBAL_DB_FILENAME, db,
                                   SQLITE_OPEN_READONLY | SQLITE_OPEN_URI,
                                   NULL);
    closedb(rw_global_db);

    if (rc != SQLITE_OK) {
        closedb(*db);
        *db = NULL;
        return 1;
    }

    return 0;
}
