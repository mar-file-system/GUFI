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
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include "dbutils.h"
#include "external.h"

/*
 * attachname, mode, uid, and gid are not strictly necessary but can be useful
 */

const char EXTERNAL_DBS_PWD_CREATE[] =
    "DROP TABLE IF EXISTS " EXTERNAL_DBS_PWD ";"
    "CREATE TABLE " EXTERNAL_DBS_PWD "(type TEXT, pinode TEXT, filename TEXT, mode INT64, uid INT64, gid INT64, PRIMARY KEY(type, pinode, filename));";

const char EXTERNAL_DBS_PWD_INSERT[] =
    "INSERT INTO " EXTERNAL_DBS_PWD " VALUES (@type, @pinode, @filename, @mode, @uid, @gid);";

const char EXTERNAL_DBS_ROLLUP_CREATE[] =
    "DROP TABLE IF EXISTS " EXTERNAL_DBS_ROLLUP ";"
    "CREATE TABLE " EXTERNAL_DBS_ROLLUP "(type TEXT, pinode TEXT, filename TEXT, mode INT64, uid INT64, gid INT64, PRIMARY KEY(type, filename));";

const char EXTERNAL_DBS_ROLLUP_INSERT[] =
    "INSERT INTO " EXTERNAL_DBS_ROLLUP " VALUES (@type, pinode, @filename, @mode, @uid, @gid);";

const refstr_t EXTERNAL_TYPE_XATTR = {
    .data = EXTERNAL_TYPE_XATTR_NAME,
    .len  = EXTERNAL_TYPE_XATTR_LEN,
};

const refstr_t EXTERNAL_TYPE_USER_DB = {
    .data = EXTERNAL_TYPE_USER_DB_NAME,
    .len  = EXTERNAL_TYPE_USER_DB_LEN,
};

int create_external_tables(const char *name, sqlite3 *db, void *args) {
    (void) args;

    static const char EXTERNAL_DBS_CREATE[] =
        "DROP VIEW IF EXISTS " EXTERNAL_DBS ";"
        "CREATE VIEW " EXTERNAL_DBS " AS SELECT * FROM " EXTERNAL_DBS_PWD " UNION SELECT * FROM " EXTERNAL_DBS_ROLLUP ";";

    return ((create_table_wrapper(name, db, EXTERNAL_DBS_PWD,    EXTERNAL_DBS_PWD_CREATE)    != SQLITE_OK) ||
            (create_table_wrapper(name, db, EXTERNAL_DBS_ROLLUP, EXTERNAL_DBS_ROLLUP_CREATE) != SQLITE_OK) ||
            (create_table_wrapper(name, db, EXTERNAL_DBS,        EXTERNAL_DBS_CREATE)        != SQLITE_OK));
}

/* SELECT <cols> FROM <table> [WHERE (type == '<type>') AND [extra]]; */
static size_t external_create_query(char *sql,         const size_t sql_size,
                                    const char *cols,  const size_t cols_len,
                                    const char *table, const size_t table_len,
                                    const refstr_t *type,
                                    const refstr_t *extra) {
    size_t len = SNFORMAT_S(sql, sql_size, 4,
                            "SELECT ", (size_t) 7,
                            cols, cols_len,
                            " FROM ", (size_t) 6,
                            table, table_len);

    if (type || extra) {
        len += SNFORMAT_S(sql + len, sql_size - len, 1,
                          " WHERE", 6);
    }

    if (type) {
        len += SNFORMAT_S(sql + len, sql_size - len, 3,
                          " (type == '", (size_t) 11,
                          type->data, type->len,
                          "')", (size_t) 2);
    }

    if (type && extra) {
        len += SNFORMAT_S(sql + len, sql_size - len, 1,
                          " AND ", (size_t) 5);
    }

    if (extra) {
        len += SNFORMAT_S(sql + len, sql_size - len, 1,
                          extra->data, extra->len);
    }

    len += SNFORMAT_S(sql + len, sql_size - len, 1,
                      ";", (size_t) 1);

    return len;
}

int external_insert(sqlite3 *db, const char *type, const long long int pinode, const char *filename) {
    char sql[MAXSQL];
    SNPRINTF(sql, sizeof(sql),
             "INSERT INTO " EXTERNAL_DBS_PWD " (type, pinode, filename) "
             "VALUES "
             "('%s', '%lld', '%s');",
             type, pinode, filename);

    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "Warning: Could not insert requested external db: %s: %s\n",
                filename, err);
        sqlite3_free(err);
        return 1;
    }

    return 0;
}

/* returns 1 for good/not checked, 0 for error */
static int check_is_db(const int check_extdb_valid, const char *path) {
    int rc = !check_extdb_valid;

    if (check_extdb_valid) {
        /* open the path to make sure it eventually resolves to a file */
        sqlite3 *extdb = opendb(path, SQLITE_OPEN_READONLY, 0, 0, NULL, NULL);
        if (extdb) {
            /* make sure this file is a sqlite3 db */
            /* can probably skip this check */
            char *err = NULL;
            if (sqlite3_exec(extdb, "SELECT '' FROM sqlite_master;", NULL, NULL, &err) == SQLITE_OK) {
                rc = 1;
            }
            else {
                fprintf(stderr, "Warning: %s is not a db: %s\n",
                        path, err);
                sqlite3_free(err);
            }

            closedb(extdb);
        }
    }

    return rc;
}

size_t external_read_file(struct input *in,
                          struct work *child,
                          external_process_db_f func,
                          void *args) {
    int extdb_list = open(child->name, O_RDONLY);
    if (extdb_list < 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not open user external database list in %s: %s (%d)\n",
                child->name, strerror(err), err);
        return 0;
    }

    size_t rc = 0;

    char *line = NULL;
    size_t len = 0;
    off_t offset = 0;
    while (getline_fd(&line, &len, extdb_list, &offset, 512) > 0) {
        char extdb_path_stack[MAXPATH];
        char *extdb_path = line;

        /* resolve relative paths */
        if (line[0] != '/')  {
            char path[MAXPATH];
            SNFORMAT_S(path, sizeof(path), 2,
                       child->name, child->name_len - child->basename_len,
                       /* basename does not include slash, so don't need to add another one */
                       line, len);

            if (!realpath(path, extdb_path_stack)) {
                const int err = errno;
                fprintf(stderr, "Error: Could not resolve external database path %s: %s (%d)\n",
                        path, strerror(err), err);
                free(line);
                line = NULL;
                continue;
            }

            extdb_path = extdb_path_stack;
        }

        if (check_is_db(in->check_extdb_valid, extdb_path) == 1){
            rc += !func(in, args, child->pinode, extdb_path);
        }

        free(line);
        line = NULL;
    }

    free(line);
    line = NULL;

    close(extdb_list);

    return rc;
}

int external_concatenate(sqlite3 *db,
                         const refstr_t *type,
                         const refstr_t *extra,
                         const refstr_t *viewname,
                         const refstr_t *select,
                         const refstr_t *tablename,
                         const refstr_t *default_table,
                         size_t (*modify_filename)(char **dst, const size_t dst_size,
                                                   const char *src, const size_t src_len,
                                                   void *args),
                         void *filename_args,
                         size_t (*set_attachname)(char *dst, const size_t dst_size,
                                                  void *args),
                         void *attachname_args
                         #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                         , size_t *query_count
                         #endif
    ) {
    /* Not checking arguments */

    int           rec_count = 0;
    /* not checking if the view exists - if it does, it's an error and will error */
    char          unioncmd[MAXSQL];
    char         *unioncmdp = unioncmd + SNFORMAT_S(unioncmd, sizeof(unioncmd), 3,
                                                    "CREATE TEMP VIEW ", (size_t) 17,
                                                    viewname->data, viewname->len,
                                                    " AS", (size_t) 3);

    if (type) {
        /* find external databases of given type */
        char get_mappings[MAXSQL];
        const size_t get_mappings_len = external_create_query(get_mappings, sizeof(get_mappings),
                                                                "filename", 8,
                                                                EXTERNAL_DBS, EXTERNAL_DBS_LEN,
                                                                type,
                                                                extra);

        /* step through each external file recorded in the main database */
        sqlite3_stmt *res = NULL;
        const int rc = sqlite3_prepare_v2(db, get_mappings, get_mappings_len, &res, NULL);
        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        (*query_count)++;
        #endif
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error: Could not get external file names from table '%s': '%s': err %s (%d)\n",
                    EXTERNAL_DBS, get_mappings, sqlite3_errmsg(db), rc);
            return -1;
        }

        while (sqlite3_step(res) == SQLITE_ROW) {
            const char *src_filename = (const char *) sqlite3_column_text(res, 0);

            char *filename = (char *) src_filename;
            char dst_filename[MAXSQL];
            if (modify_filename) {
                filename = dst_filename;
                modify_filename(&filename, sizeof(dst_filename),
                                src_filename, strlen(src_filename),
                                filename_args);
            }

            char attachname[MAXSQL];
            const size_t attachname_len = set_attachname(attachname, sizeof(attachname),
                                                         attachname_args);

            /* if attach fails, you don't have access to the database - just continue */
            if (attachdb(filename, db, attachname, SQLITE_OPEN_READONLY, 0)) {
                /* SELECT * FROM <attach name>.<table name> UNION */
                unioncmdp += SNFORMAT_S(unioncmdp, sizeof(unioncmd) - (unioncmdp - unioncmd), 6,
                                        select->data, select->len,
                                        "'", (size_t) 1,
                                        attachname, attachname_len,
                                        "'.", (size_t) 2,
                                        tablename->data, tablename->len,
                                        " UNION", (size_t) 6);
                rec_count++;
            }
        }
        sqlite3_finalize(res);
    }

    /*
     * close SQL statement using the table in db.db
     * i.e.
     *    ... UNION SELECT * FROM <default table name>;
     *
     * this assumes theres a table already in the db
     */
    unioncmdp += SNFORMAT_S(unioncmdp, sizeof(unioncmd) - (unioncmdp - unioncmd), 3,
                            select->data, select->len,
                            default_table->data, default_table->len,
                            ";", (size_t) 1);

    /* create view */
    char *err = NULL;
    const int rc = sqlite3_exec(db, unioncmd, NULL, NULL, &err);
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    (*query_count)++;
    #endif
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: Create external data view \"%s\" failed: %s: %s\n",
                viewname->data, err, unioncmd);
        sqlite3_free(err);
        return -1;
    }

    return rec_count;
}

typedef struct external_detach_args {
    sqlite3 *db;
    size_t (*func)(char *dst, const size_t dst_size,
                   void *args);
    void *args;
} eda_t;

static int external_detach(void *args, int count, char **data, char **columns) {
    (void) count; (void) data; (void) (columns);

    eda_t *eda = (eda_t *) args;
    char attachname[MAXSQL];

    eda->func(attachname, sizeof(attachname),
              eda->args);

    detachdb(NULL, eda->db, attachname, 0); /* don't check for errors */
    return 0;
}

void external_concatenate_cleanup(sqlite3 *db, const char *drop_view,
                                  const refstr_t *type,
                                  const refstr_t *extra,
                                  size_t (*set_attachname)(char *dst, const size_t dst_size,
                                                           void *args),
                                  void *attachname_args
                                  #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                                  , size_t *query_count
                                  #endif
    )
{
    if (drop_view) {
        char *err = NULL;
        const int rc = sqlite3_exec(db, drop_view, NULL, NULL, &err);
        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        (*query_count)++;
        #endif
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Warning: Could not drop view: %s\n", err);
            sqlite3_free(err);
        }
    }

    if (type) {
        char get_extdbs[MAXSQL];
        external_create_query(get_extdbs, sizeof(get_extdbs),
                              "0", 1,
                              EXTERNAL_DBS, EXTERNAL_DBS_LEN,
                              type,
                              extra);

        eda_t args = {
            .db = db,
            .func = set_attachname,
            .args = attachname_args,
        };

        char *err = NULL;
        const int rc = sqlite3_exec(db, get_extdbs, external_detach, &args, &err);
        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        (*query_count)++;
        #endif
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Warning: Could not detach external database: %s\n", err);
            sqlite3_free(err);
        }
    }
}

size_t external_increment_attachname(char *dst, const size_t dst_size,
                                     void *args) {
    size_t *rec_count = (size_t *) args;
    return SNPRINTF(dst, dst_size, EXTERNAL_ATTACH_PREFIX "%zu", (*rec_count)++);
}

size_t external_decrement_attachname(char *dst, const size_t dst_size,
                                     void *args) {
    size_t *rec_count = (size_t *) args;
    return SNPRINTF(dst, dst_size, EXTERNAL_ATTACH_PREFIX "%zu", --(*rec_count));
}
