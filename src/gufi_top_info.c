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
#include "print.h"
#include "trie.h"

#define TOP_INFO_NAME "info.db"
#define TOP_INFO_NAME_LEN sizeof(TOP_INFO_NAME) - 1

/* information stored at common index parent */
#define TOP_INFO_TABLE "info"
static const char TOP_INFO_CREATE[] =
    "CREATE TABLE IF NOT EXISTS " TOP_INFO_TABLE "(\n"
    "    name TEXT,\n"      /* subdirectory under common index parent */

    /* indexing info */
    "    start FLOAT,\n"    /* start time (seconds since epoch) */
    "    end FLOAT,\n"      /* end time (seconds since epoch) */
    "    duration FLOAT,\n" /* run time, not necessarily equal to end - start if clock jumped */
    "    threads INT64,\n"  /* threads used to generate this specific index */

    /* source information */
    "    src TEXT,\n"       /* source path */
    "    system TEXT,\n"    /* system name */
    "    OS TEXT,\n"        /* OS of system */
    "    cpu TEXT,\n"       /* cpu info */
    "    memory INT64,\n"   /* available memory (bytes) */
    "    fstype TEXT,\n"    /* filesystem type */
    "    storage TEXT,\n"   /* storage info */

    "    notes TEXT\n"       /* user notes */
    ");";

static int check_summary_row_count(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    size_t *rows = (size_t *) args;
    return !(sscanf(data[0], "%zu", rows) == 1);
}

/*
 * try to make sure the info isn't being placed in a random location in the index
 *
 * returns 0 on good path
 *         1 on bad path/error
 */
static int check_at_top(const refstr_t *path) {
    char dbname[MAXPATH];
    SNFORMAT_S(dbname, sizeof(dbname), 3,
               path->data, path->len,
               "/", (size_t) 1,
               DBNAME, DBNAME_LEN);

    /* make sure there is a db.db file */
    sqlite3 *db = opendb(dbname, SQLITE_OPEN_READWRITE, 0, 0, NULL, NULL);
    if (!db) {
        return 1;
    }

    /* check for existance of rows in summary table */
    size_t count = 0;
    char *err = NULL;
    if (sqlite3_exec(db, "SELECT COUNT(*) FROM " SUMMARY ";",
                     check_summary_row_count, &count, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Failed to get " SUMMARY " table row count in \"%s\": %s\n",
                                  path->data, err);
        closedb(db);
        return 1;
    }

    if (count) {
        fprintf(stderr, "Error: Not at the top: directory \"%s\" has contents in the summary table\n",
                path->data);
        closedb(db);
        return 1;
    }

    closedb(db);
    return 0;
}

static int create_top_info_table(const char *name, sqlite3 *db, void *args) {
    (void) args;

    return (create_table_wrapper(name, db, TOP_INFO_TABLE, TOP_INFO_CREATE) != SQLITE_OK);
}

/* insert 1 row with given key-value pairs */
static int add(sqlite3 *db, int argc, char **argv) {
    if (argc & 1) {
        fprintf(stderr, "Error: add requires key-value pairs\n");
        return 1;
    }

    char *cols = NULL;
    size_t cols_size = 0;
    size_t cols_len = 0;
    char *vals = NULL;
    size_t vals_size = 0;
    size_t vals_len = 0;

    for(int i = 0; i < argc; i += 2) {
        const char *key = argv[i];
        const char *val = argv[i + 1];

        /* let sqlite3 handle duplicate keys */

        write_with_resize(&cols, &cols_size, &cols_len, "%s, ", key);
        write_with_resize(&vals, &vals_size, &vals_len, "'%s', ", val);
    }

    int rc = 0;
    if (argc) {
        /* remove trailing commas */
        cols[cols_len - 2] = '\0';
        vals[vals_len - 2] = '\0';

        char sql[MAXSQL];
        SNPRINTF(sql, sizeof(sql), "INSERT INTO " TOP_INFO_TABLE "(%s) VALUES (%s);" ,
                 cols, vals);

        char *err = NULL;
        if (sqlite3_exec(db, sql, NULL, NULL, &err) == SQLITE_OK) {
            sqlite3_int64 added = sqlite3_last_insert_rowid(db);
            fprintf(stdout, "Inserted row %lld\n", added);
        }
        else {
            sqlite_print_err_and_free(err, stderr, "Error: Failed to insert new row: %s\n", err);
            rc = 1;
        }
    }

    free(vals);
    free(cols);

    return rc;
}

/* for the given row id, update the given columns */
static int set(sqlite3 *db, int argc, char **argv) {
    if ((argc & 1) == 0) {
        fprintf(stderr, "Error: set requires a id followed by key-value pairs\n");
        return 1;
    }

    char *changes = NULL;
    size_t changes_size = 0;
    size_t changes_len = 0;

    for(int i = 1; i < argc; i += 2) {
        const char *key = argv[i];
        const char *val = argv[i + 1];

        write_with_resize(&changes, &changes_size, &changes_len, "%s = '%s', ", key, val);
    }

    argc--; /* remove rowid */

    int rc = 0;
    if (argc) {
        /* remove trailing comma */
        changes[changes_len - 2] = '\0';

        char sql[MAXSQL];
        SNPRINTF(sql, sizeof(sql), "UPDATE " TOP_INFO_TABLE " SET %s WHERE rowid == %s;" ,
                 changes, argv[0]);

        printf("%s\n", sql);

        char *err = NULL;
        if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr, "Error: Failed to update row %s: %s\n", argv[0], err);
            rc = 1;
        }
    }

    free(changes);

    return rc;
}

/* delete each matching id */
static int del(sqlite3 *db, int argc, char **argv) {
    int rc = 0;

    for(int i = 0; i < argc; i++) {
        char sql[MAXSQL];
        SNPRINTF(sql, sizeof(sql), "DELETE FROM " TOP_INFO_TABLE " WHERE rowid == %s;",
                 argv[i]);

        char *err = NULL;
        if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr, "Error: Failed to delete row %s: %s\n", argv[i], err);
            rc = 1;
        }
    }

    return rc;
}

/* for a given id, show the given columns */
static int show(sqlite3 *db, int argc, char **argv) {
    if (argc == 0) {
        fprintf(stderr, "Error: show requires a id followed by keys\n");
        return 1;
    }

    char *cols = NULL;
    size_t cols_size = 0;
    size_t cols_len = 0;

    for(int i = 1; i < argc; i++) {
        write_with_resize(&cols, &cols_size, &cols_len, " %s, ", argv[i]);
    }

    argc--; /* remove rowid */

    int rc = 0;
    if (argc) {
        /* remove trailing comma */
        cols[cols_len - 2] = '\0';

        char sql[MAXSQL];
        SNPRINTF(sql, sizeof(sql), "SELECT %s FROM " TOP_INFO_TABLE " WHERE rowid == %s;" ,
                 cols, argv[0]);

        struct PrintArgs print = {
            .output_buffer = NULL,
            .delim = ',', /* dump csv */
            .mutex = NULL,
            .outfile = stdout,
            .types = NULL,
            .suppress_newline = 0,
        };

        char *err = NULL;
        if (sqlite3_exec(db, sql, print_uncached, &print, &err) != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr, "Error: Failed to get row %s: %s\n", argv[0], err);
            rc = 1;
        }
    }

    free(cols);

    return rc;
}

static int print_headers(void *args, int count, char **data, char **columns) {
    (void) data;

    PrintArgs_t *print = (PrintArgs_t *) args;

    count--;

    for(int i = 0; i < count; i++ ) {
        fprintf(print->outfile, "%s%c", columns[i], print->delim);
    }
    fprintf(print->outfile, "%s\n", columns[count]);

    return 0;
}

/* dump the entire table */
static int dump(sqlite3 *db, int argc, char **argv) {
    (void) argc; (void) argv;

    struct PrintArgs print = {
        .output_buffer = NULL,
        .delim = ',', /* dump csv */
        .mutex = NULL,
        .outfile = stdout,
        .types = NULL,
        .suppress_newline = 0,
    };

    char *err = NULL;
    if (sqlite3_exec(db, "SELECT * FROM " TOP_INFO_TABLE " LIMIT 1;", print_headers, &print, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Failed to print headers \"%s\": %s\n",
                                  sqlite3_filename_database(sqlite3_db_filename(db, "main")), err);
        return 1;
    }

    if (sqlite3_exec(db, "SELECT * FROM " TOP_INFO_TABLE ";", print_uncached, &print, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Failed to dump \"%s\": %s\n",
                                  sqlite3_filename_database(sqlite3_db_filename(db, "main")), err);
        return 1;
    }

    return 0;
}

/* avoid casting function pointers to object pointers */
struct OpWrapper {
    int (*func)(sqlite3 *db, int argc, char **argv);
};

static const struct OpWrapper ADD  = { .func = add  };
static const struct OpWrapper SET  = { .func = set  };
static const struct OpWrapper DEL  = { .func = del  };
static const struct OpWrapper SHOW = { .func = show };
static const struct OpWrapper DUMP = { .func = dump };

static void sub_help(void) {
    printf("GUFI_root        root of the GUFI tree (not index root)\n");
    printf("op:\n");
    printf("    add/insert [key value]...\n");
    printf("    set/modify rowid [key value]...\n");
    printf("    del/delete/remove [rowid ...]\n");
    printf("    show rowid [key]...\n");
    printf("    dump\n");
    printf("\n");
    printf("The keys are the SQL column names of the table:\n");
    printf("\n");
    printf("%s\n", TOP_INFO_CREATE);
    printf("\n");
}

int main(int argc, char *argv[]) {
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION,

        FLAG_END
    };

    struct input in;
    process_args_and_maybe_exit(options, 2, "GUFI_root op [op args]", &in);

    int rc = EXIT_SUCCESS;

    refstr_t path;
    refstr_t op;
    INSTALL_STR(&path, argv[idx++]);
    INSTALL_STR(&op,   argv[idx++]);

    char dbname[MAXPATH];

    if (check_at_top(&path) != 0) {
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    SNFORMAT_S(dbname, sizeof(dbname), 3,
               path.data, path.len,
               "/", (size_t) 1,
               TOP_INFO_NAME, TOP_INFO_NAME_LEN);

    /* create the info file if it does not exist; do not overwrite existing data */
    sqlite3 *db = opendb(dbname, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, 0, 0, create_top_info_table, NULL);
    if (!db) {
        goto cleanup;
    }

    trie_t *ops = trie_alloc();

    /* known operations */
    trie_insert(ops, "add",    3, (void *) &ADD,  NULL);
    trie_insert(ops, "insert", 6, (void *) &ADD,  NULL); /* alias */
    trie_insert(ops, "set",    3, (void *) &SET,  NULL);
    trie_insert(ops, "modify", 6, (void *) &SET,  NULL); /* alias */
    trie_insert(ops, "del",    3, (void *) &DEL,  NULL);
    trie_insert(ops, "delete", 6, (void *) &DEL,  NULL); /* alias */
    trie_insert(ops, "remove", 6, (void *) &DEL,  NULL); /* alias */
    trie_insert(ops, "show",   4, (void *) &SHOW, NULL);
    trie_insert(ops, "dump",   4, (void *) &DUMP, NULL);

    struct OpWrapper *opw = NULL;
    if (trie_search(ops, op.data, op.len, (void **) &opw)) {
        rc = (opw->func(db, argc - idx, argv + idx) == 0)?EXIT_SUCCESS:EXIT_FAILURE;
    }
    else {
        fprintf(stderr, "Error: Unknown op: %s\n", op.data);
        rc = EXIT_FAILURE;
    }

    trie_free(ops);

    closedb(db);

  cleanup:
    input_fini(&in);

    return rc;
}
