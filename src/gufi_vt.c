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

#include "addqueryfuncs.h"
#include "bf.h"
#include "dbutils.h"

/*
 * GUFI virtual tables/table-valued functions
 *
 * This SQLite3 Module contains the code that allows for users to
 * query GUFI trees as though they were tables via the SQLite3 Virtual
 * Table Mechanism. These virtual tables act as table-valued
 * functions, so CREATE VIRTUAL TABLE is not necessary.
 *
 * First, the UDFs added to SQLite3 by GUFI that do not require
 * internal state are added to SQLite3 when this module is loaded.
 *
 * Then, 6 virtual tables are added:

 *    gufi_vt_treesummary
 *    gufi_vt_summary
 *    gufi_vt_entries
 *    gufi_vt_pentries
 *    gufi_vt_vrsummary
 *    gufi_vt_vrpentries
 *
 * These may be used like so:
 *     SELECT ...
 *     FROM gufi_vt_*('<indexroot>', ...)
 *     ... ;
 *
 * The following are positional arguments to each virtual table:
 *
 *     index root
 *     # of threads
 *     T
 *     S
 *
 * The index root argument is required. The remaining are optional.
 * T and S may be used to change tree traversal behavior. Their usage
 * is strange: there must be at least as many columns in the T/S SQL
 * as the highest column index SELECT-ed in the actual query. The
 * columns themselves do not have to match the columns of the table
 * being selected, but should. The results of T and S will also show
 * up in the results of the actual query, and thus need to be filtered
 * out.
 *
 * GUFI user defined functions (UDFs) that do not require gufi_query
 * state may be called. UDFs requiring gufi_query state (path(), epath(),
 * fpath(), and rpath()) can be accessed from the virtual table by using
 * columns with the same names.
 *
 * The schemas of all 6 of the corresponding tables and views are
 * recreated here, and thus all columns are accessible.
 */

typedef struct gufi_query_sql {
    const char *T;
    const char *S;
    const char *E;
} gq_sql_t;

typedef struct gufi_vtab {
    sqlite3_vtab base;
    gq_sql_t sql;                 /* not const to allow for T and S to be modified */
} gufi_vtab;

typedef struct gufi_vtab_cursor {
    sqlite3_vtab_cursor base;

    FILE *output;              /* result of popen */
    char *row;                 /* current row */
    size_t len;                /* length of current row */
    size_t *col_starts;
    int col_count;

    sqlite_int64 rowid;        /* current row id */
} gufi_vtab_cursor;

/*
 * run gufi_query, aggregating results into a single db file
 *
 * have to fork+exec - cannot link gufi_query in without changing
 * everything to link dynamically
 */
static int gufi_query_aggregate_db(const char *indexroot, const char *threads, const gq_sql_t *sql,
                                   FILE **output, char **errmsg) {
    const char *argv[12] = {
        "gufi_query",
        "-u",
    };

    #define set_argv(argc, argv, flag, value) if (value) { argv[argc++] = flag; argv[argc++] = value; }

    int argc = 2;
    set_argv(argc, argv, "-n", threads);
    set_argv(argc, argv, "-T", sql->T);
    set_argv(argc, argv, "-S", sql->S);
    set_argv(argc, argv, "-E", sql->E);

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

/* space taken up by type and length */
static const size_t TL = sizeof(char) + sizeof(size_t);

/* read TLV rows terminated by newline - this only works because type is in the range [1, 5] */
static int gufi_query_read_row(gufi_vtab_cursor *pCur) {
    size_t size = sizeof(int);
    char *buf = malloc(size);
    char *curr = buf;
    ptrdiff_t curr_offset = 0;

    size_t *starts = NULL; /* index of where each column starts in buf */
    int count = 0;         /* number of columns */

    // each row is prefixed with a count
    if (fread(curr, sizeof(char), sizeof(int), pCur->output) != sizeof(int)) {
        goto error;
    }

    count = * (int *) curr;
    starts = malloc(count * sizeof(size_t));

    curr += sizeof(int);

    char *new_buf = NULL;
    for(int i = 0; i < count; i++) {
        starts[i] = curr - buf;

        /* add space for type and length */
        size += TL;

        curr_offset = curr - buf;

        /* reallocate buffer for type and length */
        new_buf = realloc(buf, size);
        if (!new_buf) {
            const int err = errno;
            fprintf(stderr, "Error: Could not resize buffer for reading column type and length. New size: %zu: %s (%d)\n",
                    size, strerror(err), err);
            goto error;
        }

        buf = new_buf;
        curr = buf + curr_offset;

        /* read type and length */
        const size_t tl = fread(curr, sizeof(char), TL, pCur->output);
        if (tl != TL) {
            fprintf(stderr, "Error: Could not read type and length from column %d\n", i);
            goto error;
        }

        const size_t value_len = * (size_t *) (curr + sizeof(char));

        size += value_len;         /* update buffer size with value length */
        curr += TL;                /* to go to end of buffer/start of value */

        curr_offset = curr - buf;

        /* allocate space for value */
        new_buf = realloc(buf, size);
        if (!new_buf) {
            const int err = errno;
            fprintf(stderr, "Error: Could not resize buffer for reading column value. New size: %zu: %s (%d)\n",
                    size, strerror(err), err);
            goto error;
        }

        buf = new_buf;
        curr = buf + curr_offset;

        const size_t v = fread(curr, sizeof(char), value_len, pCur->output);
        if (v != value_len) {
            fprintf(stderr, "Eror: Could not read %zu octets. Got %zu\n", value_len, v);
            goto error;
        }

        curr += value_len;
    }

    pCur->row = buf;
    pCur->len = size;
    pCur->col_starts = starts;
    pCur->col_count = count;

    return 0;

  error:
    free(buf);
    pCur->row = NULL;
    pCur->len = 0;
    free(starts);
    pCur->col_starts = NULL;
    pCur->col_count = 0;
    return 1;
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

#define SELECT_FROM(name)                             \
        "SELECT " GUFI_VT_EXTRA_COLUMNS_SQL "* "      \
        "FROM " name ";"                              \

#define SELECT_FROM_VR(name)                          \
        "SELECT " GUFI_VT_EXTRA_COLUMNS_SQL_VR "* "   \
        "FROM " name ";"                              \

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
        static const char select_from[]    = SELECT_FROM(name);                \
        static const char select_from_vr[] = SELECT_FROM_VR(name);             \
                                                                               \
        /* this is the actual query */                                         \
        static const gq_sql_t sql = {                                          \
            .T = t?select_from:NULL,                                           \
            .S = s?(vr?select_from_vr:select_from):NULL,                       \
            .E = e?(vr?select_from_vr:select_from):NULL,                       \
        };                                                                     \
                                                                               \
        return gufi_vtConnect(db, pAux, argc, argv, ppVtab,                    \
                              pzErr, schema, &sql);                            \
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
    (void) tab;

    int argc = 0;  /* number of input arguments */

    const struct sqlite3_index_constraint *constraint = pIdxInfo->aConstraint;
    for(int i = 0; i < pIdxInfo->nConstraint; i++, constraint++) {
        if (constraint->op != SQLITE_INDEX_CONSTRAINT_EQ) {
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
    free(pCur->col_starts);
    pCur->col_starts = NULL;
    free(pCur->row);
    pCur->row = NULL;
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int gufi_vtFilter(sqlite3_vtab_cursor *cur,
                         int idxNum, const char *idxStr,
                         int argc, sqlite3_value **argv) {
    (void) idxNum; (void) idxStr;

    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;
    gufi_vtab *vtab = (gufi_vtab *) cur->pVtab;

    /* indexroot must be present */
    const char *indexroot = (const char *) sqlite3_value_text(argv[GUFI_VT_ARGS_INDEXROOT]);
    const char *threads   = NULL;

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
    pCur->len = 0;

    /* wait for first row */
    gufi_query_read_row(pCur);

    return SQLITE_OK;
}

static int gufi_vtNext(sqlite3_vtab_cursor *cur) {
    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;

    free(pCur->row);
    pCur->row = NULL;
    pCur->len = 0;
    free(pCur->col_starts);
    pCur->col_starts = NULL;
    pCur->col_count = 0;

    /* no more to read or error */
    if (gufi_query_read_row(pCur) != 0) {
        return SQLITE_OK;
    }

    pCur->rowid++;

    return SQLITE_OK;
}

static int gufi_vtEof(sqlite3_vtab_cursor *cur) {
    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;

    const int eof = (pCur->len <= sizeof(int));
    if (eof) {
        pclose(pCur->output);
        pCur->output = NULL;
    }

    return eof;
}

static int gufi_vtColumn(sqlite3_vtab_cursor *cur,
                         sqlite3_context *ctx,
                         int N) {
    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;

    const char *buf = pCur->row + pCur->col_starts[N - GUFI_VT_ARGS_COUNT];
    const int type = *buf;
    const size_t len = * (size_t *) (buf + 1);
    const char *col = buf + TL;

    switch(type) {
        case SQLITE_INTEGER:
            {
                int value = 0;
                if (sscanf(col, "%d", &value) == 1) {
                    sqlite3_result_int(ctx, value);
                }
                else {
                    sqlite3_result_text(ctx, col, len, SQLITE_TRANSIENT);
                }
                break;
            }
        /* GUFI does not have floating point columns */
        /* case SQLITE_FLOAT: */
        /*     { */
        /*         double value = 0; */
        /*         if (sscanf(col, "%lf", &value) == 1) { */
        /*             sqlite3_result_double(ctx, value); */
        /*         } */
        /*         else { */
        /*             sqlite3_result_text(ctx, col, len, SQLITE_TRANSIENT); */
        /*         } */
        /*         break; */
        /*     } */
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

    if (addqueryfuncs(db) != 0) {
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
