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

#include "external.h"
#include "utils.h"

/*
 * mode, uid, and gid are not strictly necessary but can be useful
 */

const char EXTERNAL_DBS_PWD_CREATE[] =
    "DROP TABLE IF EXISTS " EXTERNAL_DBS_PWD ";"
    "CREATE TABLE " EXTERNAL_DBS_PWD "(type TEXT, filename TEXT, attachname TEXT, mode INT64, uid INT64, gid INT64, PRIMARY KEY(type, filename, attachname));";

const char EXTERNAL_DBS_PWD_INSERT[] =
    "INSERT INTO " EXTERNAL_DBS_PWD " VALUES (@type, @filename, @attachname, @mode, @uid, @gid);";

const char EXTERNAL_DBS_ROLLUP_CREATE[] =
    "DROP TABLE IF EXISTS " EXTERNAL_DBS_ROLLUP ";"
    "CREATE TABLE " EXTERNAL_DBS_ROLLUP "(type TEXT, filename TEXT, attachname TEXT, mode INT64, uid INT64, gid INT64, PRIMARY KEY(type, filename, attachname));";

const char EXTERNAL_DBS_ROLLUP_INSERT[] =
    "INSERT INTO " EXTERNAL_DBS_ROLLUP " VALUES (@type, @filename, @attachname, @mode, @uid, @gid);";

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
static size_t create_external_query(char *sql,         const size_t sql_size,
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

int external_insert(sqlite3 *db, const char *type, const char *filename, const char *attachname) {
    char sql[MAXSQL];
    SNPRINTF(sql, sizeof(sql),
             "INSERT INTO " EXTERNAL_DBS_PWD " (type, filename, attachname) "
             "VALUES "
             "('%s', '%s', '%s');",
             type, filename, attachname);

    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "Warning: Could not insert requested external db: %s: %s\n",
                filename, err);
        sqlite3_free(err);
        return 1;
    }

    return 0;
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
                         size_t (*modify_attachname)(char **dst, const size_t dst_size,
                                                   const char *src, const size_t src_len,
                                                   void *args),
                         void *attachname_args
                         #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                         , size_t *query_count
                         #endif
    ) {
    /* Not checking arguments */

    int           rec_count = 0;
    sqlite3_stmt *res = NULL;
    /* not checking if the view exists - if it does, it's an error and will error */
    char          unioncmd[MAXSQL];
    char         *unioncmdp = unioncmd + SNFORMAT_S(unioncmd, sizeof(unioncmd), 3,
                                                    "CREATE TEMP VIEW ", (size_t) 17,
                                                    viewname->data, viewname->len,
                                                    " AS", (size_t) 3);

    /* find external databases of given type */
    char get_mappings[MAXSQL];
    const size_t get_mappings_len = create_external_query(get_mappings, sizeof(get_mappings),
                                                          "filename, attachname", 20,
                                                          EXTERNAL_DBS, EXTERNAL_DBS_LEN,
                                                          type,
                                                          extra);

    /* step through each external file recorded in the main database */
    int rc = sqlite3_prepare_v2(db, get_mappings, get_mappings_len, &res, NULL);
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    (*query_count)++;
    #endif
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: Could not get external file names from table '%s': '%s': err %s (%d)\n",
                EXTERNAL_DBS, get_mappings, sqlite3_errmsg(db), rc);
        return -1;
    }

    while (sqlite3_step(res) == SQLITE_ROW) {
        const char *src_filename   = (const char *) sqlite3_column_text(res, 0);
        const char *src_attachname = (const char *) sqlite3_column_text(res, 1);

        char *filename = (char *) src_filename;
        char dst_filename[MAXSQL];
        if (modify_filename) {
            filename = dst_filename;
            modify_filename(&filename, sizeof(dst_filename),
                            src_filename, strlen(src_filename),
                            filename_args);
        }

        char *attachname = (char *) src_attachname;
        size_t attachname_len = strlen(src_attachname);
        char dst_attachname[MAXSQL];
        if (modify_attachname) {
            attachname = dst_attachname;
            attachname_len = modify_attachname(&attachname, sizeof(dst_attachname),
                                               src_attachname, attachname_len,
                                               attachname_args);
        }

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
    rc = sqlite3_exec(db, unioncmd, NULL, NULL, &err);
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
    size_t (*func)(char **dst, const size_t dst_size,
                   const char *src, const size_t src_len,
                   void *args);
    void *args;
} eda_t;

static int external_detach(void *args, int count, char **data, char **columns) {
    (void) count; (void) (columns);

    eda_t *eda = (eda_t *) args;
    char *attachname = data[0];
    char dst_attachname[MAXSQL];
    if (eda->func) {
        attachname = dst_attachname;
        eda->func(&attachname, sizeof(dst_attachname),
                  data[0], strlen(data[0]),
                  eda->args);
    }

    detachdb(NULL, eda->db, attachname, 0); /* don't check for errors */
    return 0;
}

void external_concatenate_cleanup(sqlite3 *db, const char *drop_view,
                                  const refstr_t *type,
                                  const refstr_t *extra,
                                  size_t (*modify_attachname)(char **dst, const size_t dst_size,
                                                              const char *src, const size_t src_len,
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

    char get_attachnames[MAXSQL];
    create_external_query(get_attachnames, sizeof(get_attachnames),
                          "attachname", 10,
                          EXTERNAL_DBS, EXTERNAL_DBS_LEN,
                          type,
                          extra);

    eda_t args = {
        .db = db,
        .func = modify_attachname,
        .args = attachname_args,
    };

    sqlite3_exec(db, get_attachnames, external_detach, &args, NULL);
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    (*query_count)++;
    #endif
}

static size_t enumerate_attachname(char **dst, const size_t dst_size,
                                   const char *src, const size_t src_len,
                                   void *args) {
    (void) src_len;

    size_t *rec_count = (size_t *) args;

    return SNPRINTF(*dst, dst_size, "%s.%zu", src, (*rec_count)++);
}

int external_with_template(sqlite3 *db,
                           const refstr_t *type,
                           sll_t *eus
                           #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                           , size_t *query_count
                           #endif
    ) {
    /* Not checking arguments */

    int external_count = 0;

    /* for each basename match, attach all databases */
    sll_loop(eus, node) {
        eus_t *user = (eus_t *) sll_node_data(node);

        char basename_comp[MAXSQL];
        const size_t basename_comp_len = SNFORMAT_S(basename_comp, sizeof(basename_comp), 3,
                                                    "basename(filename) == '", (size_t) 23,
                                                    user->basename.data, user->basename.len,
                                                    "'", (size_t) 1);
        const refstr_t basename_comp_ref = {
            .data = basename_comp,
            .len  = basename_comp_len,
        };

        static const refstr_t select = {
            .data = " SELECT * FROM ",
            .len  = 15,
        };

        size_t rec_count = 0; /* reset for each match */
        external_concatenate(db,
                             type,
                             &basename_comp_ref,
                             &user->view,
                             &select,
                             &user->table,
                             &user->template_table,
                             NULL, NULL,
                             enumerate_attachname, &rec_count
                             #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                             , query_count
                             #endif
            );
        external_count += rec_count;
    }

    return external_count;
}

int external_with_template_cleanup(sqlite3 *db,
                                   const refstr_t *type,
                                   sll_t *eus
                                   #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                                   , size_t *query_count
                                   #endif
    ) {
    sll_loop(eus, node) {
        eus_t *user = (eus_t *) sll_node_data(node);

        char drop_view[MAXSQL];
        SNFORMAT_S(drop_view, sizeof(drop_view), 3,
                   "DROP VIEW ", (size_t) 10,
                   user->view.data, user->view.len,
                   ";", (size_t) 1);

        char basename_comp[MAXSQL];
        const size_t basename_comp_len = SNFORMAT_S(basename_comp, sizeof(basename_comp), 3,
                                                    "basename(filename) == '", (size_t) 23,
                                                    user->basename.data, user->basename.len,
                                                    "'", (size_t) 1);

        const refstr_t basename_comp_ref = {
            .data = basename_comp,
            .len = basename_comp_len,
        };

        size_t rec_count = 0;
        external_concatenate_cleanup(db, drop_view,
                                     type,
                                     &basename_comp_ref,
                                     enumerate_attachname, &rec_count
                                     #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                                     , query_count
                                     #endif
            );
    }

    return 0;
}
