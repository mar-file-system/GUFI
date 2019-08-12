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

    // load the PCRE extension
    char * err_msg = NULL;
    if ((sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, NULL) != SQLITE_OK) || // enable loading of extensions
        (sqlite3_extension_init(db, &err_msg, NULL)                            != SQLITE_OK)) { // load the sqlite3-pcre extension
        fprintf(stderr, "Unable to load regex extension\n");
        sqlite3_free(err_msg);
        sqlite3_close(db);
        db = NULL;
    }

    return db;
}
