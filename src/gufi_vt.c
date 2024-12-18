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



#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "addqueryfuncs.h"
#include "bf.h"
#include "dbutils.h"

/*
 * GUFI Virtual Tables
 *
 * This SQLite3 Module contains the code that allows for users to
 * query GUFI trees as though they were tables via the SQLite3 Virtual
 * Table Mechanism.  These virtual tables act as table-valued
 * functions, so CREATE VIRTUAL TABLE is not necessary.
 *
 * Firstly, the UDFs added to SQLite3 by GUFI that do not require
 * internal state are also added to SQLite3 when this module is
 * loaded.
 *
 * Then, 6 virtual tables are added:
 *    gufi_vt_treesummary
 *    gufi_vt_summary
 *    gufi_vt_entries
 *    gufi_vt_pentries
 *    gufi_vt_vrsummary
 *    gufi_vt_pentries
 *
 * These may be used like so:
 *     SELECT ... USING gufi_vt_*('<indexroot>', ...);
 *
 * The first argument, the index root, is required. The following
 * arguments are all optional, but are positional, and appear in the
 * following order:
 *     - # of threads
 *     - T
 *     - S
 *
 * The schemas of all 6 of the corresponding tables and views are
 * recreated here, and thus all columns are accessible.
 */

typedef struct gufi_query_sql {
    const char *I;
    const char *T;
    const char *S;
    const char *E;
    const char *K;
    const char *J;
    const char *G;
    const char *F;
} gq_sql_t;

typedef struct gufi_vtab {
    sqlite3_vtab base;
    gq_sql_t sql;                 /* not const to allow for T and S to be modified */
} gufi_vtab;

typedef struct gufi_vtab_cursor {
    sqlite3_vtab_cursor base;

    FILE *output;              /* result of popen */
    char *row;                 /* current row */
    ssize_t len;               /* length of current row */

    sqlite_int64 rowid;        /* current row id */
} gufi_vtab_cursor;

/* Calling addqueryfuncs causes SQLite3 to set up incorrectly, leading to a segfault at load time */
static int addvtfuncs(sqlite3 *db) {
    return !(
        (sqlite3_create_function(db,   "uidtouser",           1,   SQLITE_UTF8,
                                 NULL, &uidtouser,                 NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "gidtogroup",          1,   SQLITE_UTF8,
                                 NULL, &gidtogroup,                NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "modetotxt",           1,   SQLITE_UTF8,
                                 NULL, &modetotxt,                 NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "strftime",            2,   SQLITE_UTF8,
                                 NULL, &sqlite3_strftime,          NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "blocksize",           2,   SQLITE_UTF8,
                                 NULL, &blocksize,                 NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "human_readable_size", 1,   SQLITE_UTF8,
                                 NULL, &human_readable_size,       NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "basename",            1,   SQLITE_UTF8,
                                 NULL, &sqlite_basename,           NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "stdevs",              1,   SQLITE_UTF8,
                                 NULL, NULL,  stdev_step,          stdevs_final) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "stdevp",              1,   SQLITE_UTF8,
                                 NULL, NULL,  stdev_step,          stdevp_final) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "median",              1,   SQLITE_UTF8,
                                 NULL, NULL,  median_step,         median_final) == SQLITE_OK)
        );
}

#define delim "\x1E" /* record separator */

/*
 * run gufi_query, aggregating results into a single db file
 *
 * have to fork+exec - cannot link gufi_query in without changing
 * everything to link dynamically
 */
static int gufi_query_aggregate_db(const char *indexroot, const char *threads, const gq_sql_t *sql,
                                   FILE **output, char **errmsg) {
    const char *argv[24] = {
        "gufi_query",
        "-u",
        "-d", delim,
    };

    #define set_argv(argc, argv, flag, value) if (value) { argv[argc++] = flag; argv[argc++] = value; }

    int argc = 4;
    set_argv(argc, argv, "-n", threads);
    set_argv(argc, argv, "-I", sql->I);
    set_argv(argc, argv, "-T", sql->T);
    set_argv(argc, argv, "-S", sql->S);
    set_argv(argc, argv, "-E", sql->E);
    set_argv(argc, argv, "-K", sql->K);
    set_argv(argc, argv, "-J", sql->J);
    set_argv(argc, argv, "-G", sql->G);
    set_argv(argc, argv, "-F", sql->F);

    argv[argc++] = indexroot;
    argv[argc]   = NULL;

    size_t len = 0;
    for(int i = 0; i < argc; i++) {
        len += strlen(argv[i]) + 3; /* + 2 for quotes around each argument + 1 for space between args */
    }

    /* convert array of args to single string */
    char *cmd = malloc(len + 1);
    char *curr = cmd;
    for(int i = 0; i < argc; i++) {
        /* FIXME: this should use single quotes to avoid potentially processing variables, but needs to be double quotes to handle strings in SQLite properly */
        curr += snprintf(curr, len + 1 - (curr - cmd), "\"%s\" ", argv[i]);
    }

    /* pass command to popen */
    FILE *out = popen(cmd, "re");

    free(cmd);

    if (!out) {
        const int err = errno;
        *errmsg = sqlite3_mprintf("popen failed: %s (%d)", strerror(err), err);
        return SQLITE_ERROR;
    }

    *output = out;

    return SQLITE_OK;
}

/* generic connect function */
static int gufi_vtConnect(sqlite3 *db,
                          void *pAux,
                          int argc, const char *const*argv,
                          sqlite3_vtab **ppVtab,
                          char **pzErr,
                          const char *schema,
                          const gq_sql_t *sql) {
    (void) pAux; (void) pzErr;
    (void) argc; (void) argv;

    gufi_vtab *pNew = NULL;
    const int rc = sqlite3_declare_vtab(db, schema);
    if(rc == SQLITE_OK){
        pNew = (gufi_vtab *)sqlite3_malloc( sizeof(*pNew) );
        if( pNew==0 ) return SQLITE_NOMEM;
        memset(pNew, 0, sizeof(*pNew));
        pNew->sql = *sql;
        /* sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY); */
    }

    *ppVtab = &pNew->base;
    return rc;
}

/* positional arguments to virtual table/table-valued function */
#define GUFI_VT_ARGS_INDEXROOT       0
#define GUFI_VT_ARGS_THREADS         1
#define GUFI_VT_ARGS_T               2
#define GUFI_VT_ARGS_S               3
#define GUFI_VT_ARGS_COUNT           4

#define GUFI_VT_ARG_COLUMNS          "indexroot TEXT HIDDEN, threads INT64 HIDDEN, " \
                                     "T TEXT HIDDEN, S TEXT HIDDEN, "

#define GUFI_VT_EXTRA_COLUMNS        "path TEXT, epath TEXT, fpath TEXT, rpath TEXT, "
#define GUFI_VT_EXTRA_COLUMNS_SQL    "path(), epath(), fpath(), path(), "
#define GUFI_VT_EXTRA_COLUMNS_SQL_VR "path(), epath(), fpath(), rpath(sname, sroll), "

#define GUFI_VT_ALL_COLUMNS          GUFI_VT_ARG_COLUMNS \
                                     GUFI_VT_EXTRA_COLUMNS

#define PENTRIES_SCHEMA(name, extra_cols)             \
    "CREATE TABLE " name "(" extra_cols "name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattr_names BLOB, crtime INT64, ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, osstext1 TEXT, osstext2 TEXT, pinode TEXT, ppinode TEXT);"

#define VRSUMMARY_SCHEMA(name, extra_cols)            \
    "CREATE TABLE " name "(" extra_cols "dname TEXT, sname TEXT, sroll INT64, srollsubdirs INT64, name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattr_names BLOB, totfiles INT64, totlinks INT64, minuid INT64, maxuid INT64, mingid INT64, maxgid INT64, minsize INT64, maxsize INT64, totzero INT64, totltk INT64, totmtk INT64, totltm INT64, totmtm INT64, totmtg INT64, totmtt INT64, totsize INT64, minctime INT64, maxctime INT64, minmtime INT64, maxmtime INT64, minatime INT64, maxatime INT64, minblocks INT64, maxblocks INT64, totxattr INT64, depth INT64, mincrtime INT64, maxcrtime INT64, minossint1 INT64, maxossint1 INT64, totossint1 INT64, minossint2 INT64, maxossint2 INT64, totossint2 INT64, minossint3 INT64, maxossint3 INT64, totossint3 INT64, minossint4 INT64, maxossint4 INT64, totossint4 INT64, rectype INT64, pinode TEXT, isroot INT64, rollupscore INT64);"

#define VRPENTRIES_SCHEMA(name, extra_cols)           \
    "CREATE TABLE " name "(" extra_cols "dname TEXT, sname TEXT, dmode INT64, dnlink INT64, duid INT64, dgid INT64, dsize INT64, dblksize INT64, dblocks INT64, datime INT64, dmtime INT64, dctime INT64, dlinkname TEXT, dtotfile INT64, dtotlinks INT64, dminuid INT64, dmaxuid INT64, dmingid INT64, dmaxgid INT64, dminsize INT64, dmaxsize INT64, dtotzero INT64, dtotltk INT64, dtotmtk INT64, totltm INT64, dtotmtm INT64, dtotmtg INT64, dtotmtt INT64, dtotsize INT64, dminctime INT64, dmaxctime INT64, dminmtime INT64, dmaxmtime INT64, dminatime INT64, dmaxatime INT64, dminblocks INT64, dmaxblocks INT64, dtotxattr INT64, ddepth INT64, dmincrtime INT64, dmaxcrtime INT64, sroll INT64, atroot INT64, srollsubdirs INT64, name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattr_names BLOB, crtime INT64, ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, osstext1 TEXT, osstext2 TEXT, pinode TEXT, ppinode TEXT);"

#define INTERMEDIATE "intermediate"
#define AGGREGATE    "aggregate"

#define SELECT_FROM(name, extra_sql)                  \
    (const char *) (                                  \
        "INSERT INTO " INTERMEDIATE " "               \
        "SELECT " GUFI_VT_EXTRA_COLUMNS_SQL "* "      \
        "FROM " name ";" extra_sql                    \
    )

#define SELECT_FROM_VR(name, extra_sql)               \
    (const char *) (                                  \
        "INSERT INTO " INTERMEDIATE " "               \
        "SELECT " GUFI_VT_EXTRA_COLUMNS_SQL_VR "* "   \
        "FROM " name ";" extra_sql                    \
    )

#define INSERT_AGG ("INSERT INTO "   AGGREGATE " SELECT * FROM " INTERMEDIATE ";")
#define SELECT_AGG ("SELECT * FROM " AGGREGATE ";")

/* generate xConnect function for each virtual table */
#define gufi_vt_xConnect(name, abbrev, t, s, e, vr)                            \
    static int gufi_vt_ ##abbrev ##Connect(sqlite3 *db,                        \
                                           void *pAux,                         \
                                           int argc, const char * const *argv, \
                                           sqlite3_vtab **ppVtab,              \
                                           char **pzErr) {                     \
        /* this is what the virtual table looks like */                        \
        static const char schema[] =                                           \
            name ##_SCHEMA(name, GUFI_VT_ALL_COLUMNS);                         \
                                                                               \
        /* this is the actual query */                                         \
        static const gq_sql_t sql = {                                          \
            .I = name ##_SCHEMA(INTERMEDIATE, GUFI_VT_EXTRA_COLUMNS),          \
            .T = t?(SELECT_FROM(name, "SELECT NULL;")):NULL,                   \
            .S = s?(vr?SELECT_FROM_VR(name, ""):SELECT_FROM(name, "")):NULL,   \
            .E = e?(vr?SELECT_FROM_VR(name, ""):SELECT_FROM(name, "")):NULL,   \
            .K = name ##_SCHEMA(AGGREGATE,  GUFI_VT_EXTRA_COLUMNS),            \
            .J = INSERT_AGG,                                                   \
            .G = SELECT_AGG,                                                   \
            .F = NULL,                                                         \
        };                                                                     \
                                                                               \
        return gufi_vtConnect(db, pAux, argc, argv, ppVtab, pzErr,             \
                              schema, &sql);                                   \
     }


/* generate xConnect for each table/view */
gufi_vt_xConnect(TREESUMMARY, T,   1, 0, 0, 0)
gufi_vt_xConnect(SUMMARY,     S,   0, 1, 0, 0)
gufi_vt_xConnect(ENTRIES,     E,   0, 0, 1, 0)
gufi_vt_xConnect(PENTRIES,    P,   0, 0, 1, 0)
gufi_vt_xConnect(VRSUMMARY,   VRS, 0, 1, 0, 1)
gufi_vt_xConnect(VRPENTRIES,  VRP, 0, 0, 1, 1)

/* FIXME: This is probably not correct */
static int gufi_vtBestIndex(sqlite3_vtab *tab,
                            sqlite3_index_info *pIdxInfo) {
    /* gufi_vtab *vtab = (gufi_vtab *) tab; */
    (void) tab;

    int argc = 0;  /* number of input arguments */

    const struct sqlite3_index_constraint *constraint = pIdxInfo->aConstraint;
    for(int i = 0; i < pIdxInfo->nConstraint; i++, constraint++) {
        if (constraint->op != SQLITE_INDEX_CONSTRAINT_EQ) {
            continue;
        }

        if (!constraint->usable) {
            continue;
        }

        if (constraint->iColumn < GUFI_VT_ARGS_COUNT) {
            pIdxInfo->aConstraintUsage[i].argvIndex = constraint->iColumn + 1;
            pIdxInfo->aConstraintUsage[i].omit = 1;
            argc++;
        }
    }

    /* index root not found */
    if (argc == 0) {
        return SQLITE_CONSTRAINT;
    }

    return SQLITE_OK;
}

static int gufi_vtDisconnect(sqlite3_vtab *pVtab) {
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int gufi_vtOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor) {
    (void) p;
    gufi_vtab_cursor *pCur = sqlite3_malloc( sizeof(*pCur) );
    if( pCur==0 ) return SQLITE_NOMEM;
    memset(pCur, 0, sizeof(*pCur));
    *ppCursor = &pCur->base;
    return SQLITE_OK;
}

static int gufi_vtClose(sqlite3_vtab_cursor *cur) {
    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;
    free(pCur->row);
    pCur->row = NULL;
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int gufi_vtFilter(sqlite3_vtab_cursor *cur,
                         int idxNum, const char *idxStr,
                         int argc, sqlite3_value **argv) {
    (void) idxNum; (void) idxStr;
    (void) argc;   (void) argv;

    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;
    gufi_vtab *vtab = (gufi_vtab *) cur->pVtab;

    /* indexroot must be present */
    const char *indexroot     = (const char *) sqlite3_value_text(argv[GUFI_VT_ARGS_INDEXROOT]);
    const char *threads       = NULL;

    if (argc > GUFI_VT_ARGS_THREADS) {
        /* passing NULL in the SQL will result in a NULL pointer */
        if ((threads = (const char *) sqlite3_value_text(argv[GUFI_VT_ARGS_THREADS]))) {

            size_t nthreads = 0;
            if ((sscanf(threads, "%zu", &nthreads) != 1) || (nthreads == 0)) {
                vtab->base.zErrMsg = sqlite3_mprintf("Bad thread count: '%s'", threads);
                return SQLITE_CONSTRAINT;
            }
        }
    }

    #define set_str(argc, argv, idx, dst)                               \
        if (argc > idx) {                                               \
            if (sqlite3_value_bytes(argv[idx]) != 0) {                  \
                dst = (const char *) sqlite3_value_text(argv[idx]);     \
            }                                                           \
        }

    /* -T and -S can be changed */
    set_str(argc, argv, GUFI_VT_ARGS_T, vtab->sql.T);
    set_str(argc, argv, GUFI_VT_ARGS_S, vtab->sql.S);

    /* kick off gufi_query */
    const int rc = gufi_query_aggregate_db(indexroot, threads, &vtab->sql,
                                           &pCur->output, &vtab->base.zErrMsg);
    if (rc != SQLITE_OK) {
        return SQLITE_ERROR;
    }

    pCur->rowid = 0;
    pCur->row = NULL;

    /* wait for first row */
    size_t len = 0;
    pCur->len = getline(&pCur->row, &len, pCur->output);

    if (pCur->len < 0) { /* failed or reached EOF */
        const int err = errno;
        sqlite3_free(cur->pVtab->zErrMsg);
        cur->pVtab->zErrMsg = sqlite3_mprintf("Could not read first result: %s (%d)",
                                              strerror(err), err);
        return SQLITE_ERROR;
    }

    if (pCur->row[pCur->len - 1] == '\n') {
        pCur->row[pCur->len - 1] = '\0';
        pCur->len--;
    }

    return SQLITE_OK;
}

static int gufi_vtNext(sqlite3_vtab_cursor *cur) {
    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;

    size_t len = 0;
    free(pCur->row);
    pCur->row = NULL;
    pCur->len = getline(&pCur->row, &len, pCur->output);

    /* no more to read */
    if (pCur->len == -1) {
        return SQLITE_OK;
    }

    /* remove trailing newline */
    if (pCur->row[pCur->len - 1] == '\n') {
        pCur->len--;
    }

    pCur->rowid++;

    return SQLITE_OK;
}

static int gufi_vtEof(sqlite3_vtab_cursor *cur) {
    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;

    const int eof = (pCur->len < 1);
    if (eof) {
        pclose(pCur->output);
        pCur->output = NULL;
    }

    return eof;
}

static int find_col(const char *str, const size_t len, const char sep, const size_t idx,
                    int *type, const char **col, size_t *col_len) {
    (void) sep;

    if (!str) {
        goto error;
    }

    char *curr = (char *) str;
    size_t i = 0;
    while ((i <= idx) && ((size_t) (curr - str) < len)) {
        /* type */
        *type = *curr;
        curr++;

        /* length */
        /* have to use text instead of binary to distinguish 10 from '\x0a' */
        char buf[5] = {0};
        memcpy(buf, curr, 4);
        if (sscanf(buf, "%zu", col_len) != 1) {
            goto error;
        }
        curr += 4;

        /* value */
        *col = curr;
        curr += *col_len;

        /* delimiter */
        curr++;
        i++;
    }

    if (i != (idx + 1)) {
        goto error;
    }

    return 0;

  error:
    *type = 0;
    *col = NULL;
    *col_len = 0;
    return 1;
}

static int gufi_vtColumn(sqlite3_vtab_cursor *cur,
                         sqlite3_context *ctx,
                         int N) {
    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;

    int type = 0;
    const char *col = NULL;
    size_t len = 0;

    if (find_col(pCur->row, pCur->len, delim[0], N - GUFI_VT_ARGS_COUNT, &type, &col, &len) == 0) {
        switch(type) {
            case SQLITE_INTEGER:
                {
                    int value = 0;
                    if (sscanf(col, "%d", &value) != 1) {
                        const int err = errno;
                        gufi_vtab *vtab = (gufi_vtab *) &pCur->base.pVtab;
                        vtab->base.zErrMsg = sqlite3_mprintf("Could not parse '%.*s' as a double: %s (%d)\n",
                                                             len, col, strerror(err), err);
                        return SQLITE_ERROR;
                    }
                    sqlite3_result_int(ctx, value);
                    break;
                }
            case SQLITE_FLOAT:
                {
                    double value = 0;
                    if (sscanf(col, "%lf", &value) != 1) {
                        const int err = errno;
                        gufi_vtab *vtab = (gufi_vtab *) &pCur->base.pVtab;
                        vtab->base.zErrMsg = sqlite3_mprintf("Could not parse '%.*s' as a double: %s (%d)\n",
                                                             len, col, strerror(err), err);
                        return SQLITE_ERROR;
                    }
                    sqlite3_result_double(ctx, value);
                    break;
                }
            case SQLITE_TEXT:
            case SQLITE_BLOB:
                sqlite3_result_text(ctx, col, len, SQLITE_TRANSIENT);
                break;
            case SQLITE_NULL:
            default:
                sqlite3_result_text(ctx, col, len, SQLITE_TRANSIENT);
                /* sqlite3_result_null(ctx); */
                break;
        }
    }
    else {
        sqlite3_result_null(ctx);
    }

    return SQLITE_OK;
}

static int gufi_vtRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid) {
    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;
    *pRowid = pCur->rowid;
    return SQLITE_OK;
}

static const sqlite3_module gufi_vtModule = {
    0,                         /* iVersion */
    0,                         /* xCreate */
    0,                         /* xConnect */
    gufi_vtBestIndex,          /* xBestIndex */
    0,                         /* xDisconnect */
    0,                         /* xDestroy */
    gufi_vtOpen,               /* xOpen - open a cursor */
    gufi_vtClose,              /* xClose - close a cursor */
    gufi_vtFilter,             /* xFilter - configure scan constraints */
    gufi_vtNext,               /* xNext - advance a cursor */
    gufi_vtEof,                /* xEof - check for end of scan */
    gufi_vtColumn,             /* xColumn - read data */
    gufi_vtRowid,              /* xRowid - read data */
    0,                         /* xUpdate */
    0,                         /* xBegin */
    0,                         /* xSync */
    0,                         /* xCommit */
    0,                         /* xRollback */
    0,                         /* xFindFunction */
    0,                         /* xRename */
    0,                         /* xSavepoint */
    0,                         /* xRelease */
    0,                         /* xRollbackTo */
    0,                         /* xShadowName */
    /* 0                          /\* xIntegrity *\/ */
};

#define create_module(module_name, constructor)                              \
    {                                                                        \
        static sqlite3_module module;                                        \
        memcpy(&module, &gufi_vtModule, sizeof(gufi_vtModule));              \
        module.xCreate = NULL;                                               \
        module.xConnect = constructor;                                       \
        module.xDisconnect = gufi_vtDisconnect;                              \
        module.xDestroy = NULL;                                              \
        const int rc = sqlite3_create_module(db, module_name, &module, 0);   \
        if (rc != SQLITE_OK) {                                               \
            return rc;                                                       \
        }                                                                    \
    }

/* no underscore between gufi and vt for entry point */
int sqlite3_gufivt_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi) {
    (void) pzErrMsg;

    SQLITE_EXTENSION_INIT2(pApi);

    if (addvtfuncs(db) != 0) {
        return SQLITE_ERROR;
    }

    create_module("gufi_vt_treesummary", gufi_vt_TConnect);
    create_module("gufi_vt_summary",     gufi_vt_SConnect);
    create_module("gufi_vt_entries",     gufi_vt_EConnect);
    create_module("gufi_vt_pentries",    gufi_vt_PConnect);
    create_module("gufi_vt_vrsummary",   gufi_vt_VRSConnect);
    create_module("gufi_vt_vrpentries",  gufi_vt_VRPConnect);

    return SQLITE_OK;
}
