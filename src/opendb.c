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



#include "opendb.h"
#include "dbutils.h"
#include "pcre.h"

#include <stdio.h>
#include <string.h>

static const char GUFI_SQLITE_VFS[] = "unix-none";

static int create_table_wrapper(const char *name, sqlite3 * db, const char * sql_name, const char * sql, int (*callback)(void*,int,char**,char**), void * args) {
    char *err_msg = NULL;
    const int rc = sqlite3_exec(db, sql, callback, args, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "%s:%d SQL error while executing '%s' on %s: '%s' (%d)\n",
                __FILE__, __LINE__, sql_name, name, err_msg, rc);
        sqlite3_free(err_msg);
    }
    return rc;
}

int create_tables(const char *name, sqlite3 *db) {
    /* if (openwhat==1 || openwhat==4 || openwhat==8) { */
        if (create_table_wrapper(name, db, "esql",         esql,        NULL, NULL) != SQLITE_OK)  {
            return -1;
        }
    /* } */

    /* if (openwhat==3) { */
    /*     if ((create_table_wrapper(name, db, "tsql",        tsql,        NULL, NULL) != SQLITE_OK) || */
    /*         (create_table_wrapper(name, db, "vtssqldir",   vtssqldir,   NULL, NULL) != SQLITE_OK) || */
    /*         (create_table_wrapper(name, db, "vtssqluser",  vtssqluser,  NULL, NULL) != SQLITE_OK) || */
    /*         (create_table_wrapper(name, db, "vtssqlgroup", vtssqlgroup, NULL, NULL) != SQLITE_OK)) { */
    /*         return -1; */
    /*     } */
    /* } */

    /* if (openwhat==2 || openwhat==4 || openwhat==8) { */
        if ((create_table_wrapper(name, db, "ssql",        ssql,        NULL, NULL) != SQLITE_OK) ||
            (create_table_wrapper(name, db, "vssqldir",    vssqldir,    NULL, NULL) != SQLITE_OK) ||
            (create_table_wrapper(name, db, "vssqluser",   vssqluser,   NULL, NULL) != SQLITE_OK) ||
            (create_table_wrapper(name, db, "vssqlgroup",  vssqlgroup,  NULL, NULL) != SQLITE_OK) ||
            (create_table_wrapper(name, db, "vesql",       vesql,       NULL, NULL) != SQLITE_OK)) {
            return -1;
        }
    /* } */

    /* if (openwhat==7) { */
    /*     if (create_table_wrapper(name, db, "rsql",         rsql,        NULL, NULL) != SQLITE_OK) { */
    /*         return -1; */
    /*     } */
    /* } */

    return 0;
}

int set_pragmas(sqlite3 * db) {
    int rc = 0;

    // try to turn sychronization off
    if (sqlite3_exec(db, "PRAGMA synchronous = OFF", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "Could not turn off synchronization\n");
        rc = 1;
    }

    // try to turn journaling off
    if (sqlite3_exec(db, "PRAGMA journal_mode = OFF", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "Could not turn off journaling\n");
        rc = 1;
    }

    // try to store temp_store in memory
    if (sqlite3_exec(db, "PRAGMA temp_store = 2", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "Could not set temporary storage to in-memory\n");
        rc = 1;
    }

    // try to increase the page size
    if (sqlite3_exec(db, "PRAGMA page_size = 16777216", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "Could not set page size\n");
        rc = 1;
    }

    // try to increase the cache size
    if (sqlite3_exec(db, "PRAGMA cache_size = 16777216", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "Could not set cache size\n");
        rc = 1;
    }

    // try to get an exclusive lock
    if (sqlite3_exec(db, "PRAGMA locking_mode = EXCLUSIVE", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "Could not set locking mode\n");
        rc = 1;
    }

    return rc;
}

sqlite3 * opendb2(const char * name, const int rdonly, const int createtables, const int setpragmas) {
    sqlite3 * db = NULL;

    if (rdonly && createtables) {
        fprintf(stderr, "Cannot open database: readonly and createtables cannot both be set at the same time\n");
        return NULL;
    }

    int flags = SQLITE_OPEN_URI;
    if (rdonly) {
        flags |= SQLITE_OPEN_READONLY;
    }
    else {
        flags |= SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE;
    }

    // no need to create because the file should already exist
    if (sqlite3_open_v2(name, &db, flags, GUFI_SQLITE_VFS) != SQLITE_OK) {
        /* fprintf(stderr, "Cannot open database: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db)); */
        sqlite3_close(db); // close db even if it didn't open to avoid memory leaks
        return NULL;
    }

    if (createtables) {
        if (create_tables(name, db) != 0) {
            fprintf(stderr, "Cannot create tables: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db));
            sqlite3_close(db);
            return NULL;
        }
    }

    if (setpragmas) {
        // ignore errors
        set_pragmas(db);
    }

    if ((sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, NULL) != SQLITE_OK) || // enable loading of extensions
        (sqlite3_extension_init(db, NULL, NULL)                                != SQLITE_OK)) { // load the sqlite3-pcre extension
        fprintf(stderr, "Unable to load regex extension\n");
        sqlite3_close(db);
        db = NULL;
    }

    return db;
}
