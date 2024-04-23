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
    "CREATE TABLE " EXTERNAL_DBS_PWD "(type TEXT, filename TEXT PRIMARY KEY, attachname TEXT, mode INT64, uid INT64, gid INT64);";

const char EXTERNAL_DBS_PWD_INSERT[] =
    "INSERT INTO " EXTERNAL_DBS_PWD " VALUES (@type, @filename, @attachname, @mode, @uid, @gid);";

const char EXTERNAL_DBS_ROLLUP_CREATE[] =
    "DROP TABLE IF EXISTS " EXTERNAL_DBS_ROLLUP ";"
    "CREATE TABLE " EXTERNAL_DBS_ROLLUP "(type TEXT, filename TEXT PRIMARY KEY, attachname TEXT, mode INT64, uid INT64, gid INT64);";

const char EXTERNAL_DBS_ROLLUP_INSERT[] =
    "INSERT INTO " EXTERNAL_DBS_ROLLUP " VALUES (@type, @filename, @attachname, @mode, @uid, @gid);";

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
                                    const char *type,  const size_t type_len,
                                    const char *extra, const size_t extra_len) {
    size_t len = SNFORMAT_S(sql, sql_size, 4,
                            "SELECT ", (size_t) 7,
                            cols, cols_len,
                            " FROM ", (size_t) 6,
                            table, table_len);

    if ((type && type_len) ||
        (extra && extra_len)) {
        len += SNFORMAT_S(sql + len, sql_size - len, 1,
                          " WHERE", 6);
    }

    if (type && type_len) {
        len += SNFORMAT_S(sql + len, sql_size - len, 3,
                          " (type == '", (size_t) 11,
                          type, type_len,
                          "')", (size_t) 2);
    }

    if ((type && type_len) &&
        (extra && extra_len)) {
        len += SNFORMAT_S(sql + len, sql_size - len, 1,
                          " AND ", (size_t) 5);
    }

    if (extra && extra_len) {
        len += SNFORMAT_S(sql + len, sql_size - len, 1,
                          extra, extra_len);
    }

    len += SNFORMAT_S(sql + len, sql_size - len, 1,
                      ";", (size_t) 1);

    return len;
}

int external_insert(sqlite3 *db, const char *type, const char *filename, const char *attachname) {
    char sql[MAXSQL];
    SNPRINTF(sql, sizeof(sql),
             "INSERT INTO " EXTERNAL_DBS_PWD " VALUES ('%s', '%s', '%s', NULL, NULL, NULL);",
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

int external_concatenate(struct work *work, sqlite3 *db,
                         const char *type, const size_t type_len,
                         const char *viewname, const size_t viewname_len,
                         const char *select, const size_t select_len,
                         const char *tablename, const size_t tablename_len,
                         size_t (*modify_filename)(char **dst, const size_t dst_size,
                                                   const char *src, const size_t src_len,
                                                   struct work *work)) {
    /* Not checking arguments */

    int           rec_count = 0;
    sqlite3_stmt *res = NULL;
    /* not checking if the view exists - if it does, it's an error and will error */
    char          unioncmd[MAXSQL];
    char         *unioncmdp = unioncmd + SNFORMAT_S(unioncmd, sizeof(unioncmd), 3,
                                                    "CREATE TEMP VIEW ", (size_t) 17,
                                                    viewname, viewname_len,
                                                    " AS", 3);

    /* pull a subset of */
    char get_mappings[MAXSQL];
    const size_t get_mappings_len = create_external_query(get_mappings, sizeof(get_mappings),
                                                          "filename, attachname", 20,
                                                          EXTERNAL_DBS, sizeof(EXTERNAL_DBS) - 1,
                                                          type, type_len,
                                                          NULL, 0);

    /* step through each external file recorded in the main database */
    int error = sqlite3_prepare_v2(db, get_mappings, get_mappings_len, &res, NULL);
    if (error != SQLITE_OK) {
        fprintf(stderr, "Error: Could not get external file names from table '%s': '%s': err %s (%d)\n",
                EXTERNAL_DBS, get_mappings, sqlite3_errmsg(db), error);
        return -1;
    }

    while (sqlite3_step(res) == SQLITE_ROW) {
        const char *src_filename = (const char *) sqlite3_column_text(res, 0);
        const char *attachname   = (const char *) sqlite3_column_text(res, 1);

        /*
         * modify the stored filename in case it needs changing
         *
         * the full path might have been removed to not store the
         * same prefix repeatedly
         */
        char *attfile = (char *) src_filename;
        char dst_filename[MAXSQL];
        size_t dst_filename_len = strlen(src_filename);
        if (modify_filename) {
            attfile = dst_filename;
            dst_filename_len = modify_filename(&attfile, sizeof(dst_filename),
                                              src_filename, dst_filename_len,
                                              work);
        }

        /* if attach fails, you don't have access to the database - just continue */
        if (attachdb(attfile, db, attachname, SQLITE_OPEN_READONLY, 0)) {
            /* SELECT * FROM <attach name>.xattrs_avail [WHERE type == '<type>'] UNION */
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
     *
     * this assumes theres a table already in the db
     */
    unioncmdp += SNFORMAT_S(unioncmdp, sizeof(unioncmd) - (unioncmdp - unioncmd), 3,
                            select, select_len,
                            tablename, tablename_len,
                            ";", (size_t) 1);

    /* create view */
    char *err = NULL;
    if (sqlite3_exec(db, unioncmd, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "Error: Create external data view \"%s\" failed: %s: %s\n",
                viewname, err, unioncmd);
        sqlite3_free(err);
        return -1;
    }

    return rec_count;
}

static int external_detach(void *args, int count, char **data, char **columns) {
    (void) count; (void) (columns);

    detachdb(NULL, (sqlite3 *) args, data[0], 0); /* don't check for errors */
    return 0;
}

void external_concatenate_cleanup(sqlite3 *db, const char *drop_view,
                                  const char *type, const size_t type_len
                                  #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                                  , size_t *query_count
                                  #endif
    )
{
    if (drop_view) {
        sqlite3_exec(db, drop_view, NULL, NULL, NULL);
        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        (*query_count)++;
        #endif
    }

    char get_attachnames[MAXSQL];
    create_external_query(get_attachnames, sizeof(get_attachnames),
                          "attachname", 10,
                          EXTERNAL_DBS, sizeof(EXTERNAL_DBS) - 1,
                          type, type_len,
                          NULL, 0);

    sqlite3_exec(db, get_attachnames, external_detach, db, NULL);
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    (*query_count)++;
    #endif
}

typedef struct template_query_arg {
    eus_t *user;
    int found;

    char filename[MAXSQL];
    size_t filename_len;

    char attachname[MAXSQL];
    size_t attachname_len;
} tqa_t;

int get_external_user_db(void *args, int count, char **data, char **columns) {
    (void) count;
    (void) columns;

    const char *filename   = data[0];
    const char *attachname = data[1];

    tqa_t *tqa = (tqa_t *) args;

    tqa->found++;

    /* have to copy out and attach later */

    tqa->filename_len = strlen(filename);
    memcpy(tqa->filename, filename, tqa->filename_len);
    tqa->filename[tqa->filename_len] = '\0';

    tqa->attachname_len = strlen(attachname);
    memcpy(tqa->attachname, attachname, tqa->attachname_len);
    tqa->attachname[tqa->attachname_len] = '\0';

    return 0;
}

int external_with_template(struct work *work, sqlite3 *db,
                         const char *type, const size_t type_len,
                         sll_t *eus
                         #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                         , size_t *query_count
                         #endif
) {
    (void) type_len;

    /* Not checking arguments */

    int external_count = 0;
    sll_loop(eus, node) {
        tqa_t tqa;
        tqa.user = (eus_t *) sll_node_data(node);
        tqa.found = 0;

        int rc = SQLITE_OK;
        char *err = NULL;

        /* set up sql to create view with template file */
        char create_user_view[MAXSQL];
        size_t create_user_view_len = SNPRINTF(create_user_view, sizeof(create_user_view),
                                               "CREATE TEMP VIEW %s AS SELECT * FROM %s",
                                               tqa.user->view.data, tqa.user->template_table.data);

        /* find this file in the external database table */
        char find_tracked[MAXSQL];
        SNPRINTF(find_tracked, sizeof(find_tracked),
                 "SELECT filename, attachname "
                 "FROM " ATTACH_NAME "." EXTERNAL_DBS " "
                 "WHERE (type == '%s') AND (basename(filename) == '%s');",
                 type, tqa.user->basename);
        rc = sqlite3_exec(db, find_tracked, get_external_user_db, &tqa, &err);
        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        (*query_count)++;
        #endif
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error: Could not query external database table in '%s': %s\n",
                    work->name, err);
            sqlite3_free(err);
            goto create_view;
        }

        /* external user database with this name was not defined for this directory - create view using just the template */
        if (!tqa.found) {
            goto create_view;
        }
        /* should never reach here */
        else if (tqa.found > 1) {
            fprintf(stderr, "Warning: found %d rows with the name '%s in '%s'. Using last.\n",
                    tqa.found, tqa.user->basename.data, work->name);
        }

        /* attach the external db to the thread db */
        rc = !!attachdb(tqa.filename, db, tqa.attachname, SQLITE_OPEN_READONLY, 1);
        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        (*query_count)++;
        #endif
        if (rc) {
            /* add this external user database to the view */
            create_user_view_len += SNPRINTF(create_user_view + create_user_view_len, sizeof(create_user_view) - create_user_view_len,
                                             " UNION SELECT * FROM %s.%s",
                                             tqa.attachname, tqa.user->table.data);
        }
        else {
            /* found external db, but failed to attach - create view using just the template */
            fprintf(stderr, "Warning: Failed to attach '%s' to '%s'\n",
                    tqa.filename, work->name);
        }

      create_view:
        /* append ; */
        SNPRINTF(create_user_view + create_user_view_len, sizeof(create_user_view) - create_user_view_len, ";");

        rc = sqlite3_exec(db, create_user_view, NULL, NULL, &err);
        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        (*query_count)++;
        #endif
        if (rc == SQLITE_OK) {
            external_count += !!tqa.found;
        }
        else {
            fprintf(stderr, "Error: Could not create user view '%s' at '%s': %s\n",
                    tqa.user->view.data, work->name, err);
            sqlite3_free(err);
        }
    }

    return external_count;
}

int external_with_template_cleanup(struct work *work, sqlite3 *db,
                                 const char *type, const size_t type_len,
                                 sll_t *eus
                                 #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                                 , size_t *query_count
                                 #endif
    ) {
    (void) type_len;

    sll_loop(eus, node) {
        tqa_t tqa;
        tqa.user = (eus_t *) sll_node_data(node);
        tqa.found = 0;

        int rc = SQLITE_OK;
        char *err = NULL;

        /* drop view */
        char sql[MAXSQL];
        SNPRINTF(sql, sizeof(sql), "DROP VIEW %s;", tqa.user->view);
        rc = sqlite3_exec(db, sql, NULL, NULL, &err);
        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        (*query_count)++;
        #endif
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error: Could not drop view '%s' in '%s': %s\n",
                    tqa.user->view.data, work->name, err);
            sqlite3_free(err);
        }

        /* find the file in the external database table */
        SNPRINTF(sql, sizeof(sql),
                 "SELECT filename, attachname "
                 "FROM " ATTACH_NAME "." EXTERNAL_DBS " "
                 "WHERE (type == '%s') AND (basename(filename) == '%s');",
                 type, tqa.user->basename);
        rc = sqlite3_exec(db, sql, get_external_user_db, &tqa, &err);
        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        (*query_count)++;
        #endif
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error: Could not query external database table in '%s': %s\n",
                    work->name, err);
            sqlite3_free(err);
            continue;
        }

        if (!tqa.found) {
            continue;
        }
        /* should never reach here */
        else if (tqa.found > 1) {
            fprintf(stderr, "Warning: found %d rows with the name '%s in '%s'. Using last.\n",
                    tqa.found, tqa.user->basename.data, work->name);
        }

        /* detach external user db */
        detachdb(tqa.filename, db, tqa.attachname, 1);
        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        (*query_count)++;
        #endif
    }

    return 0;
}
