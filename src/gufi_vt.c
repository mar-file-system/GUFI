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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "addqueryfuncs.h"
#include "bf.h"
#include "dbutils.h"
#include "utils.h"

/*
 * GUFI Virtual Tables Module
 *
 * This file contains GUFI virtual tables in the form of
 * gufi_vt_* and gufi_vt.
 *
 * gufi_vt_* is a set of virtual tables with fixed schemas that can be
 * queried directly for ease of use:
 *
 *     SELECT * FROM gufi_vt_pentries('indexroot'[, threads]);
 *
 * gufi_vt is a true virtual table as described by the SQLite Virtual
 * Table documentation (https://www.sqlite.org/vtab.html). It is
 * intended for power users that have some understanding of how
 * gufi_query works and the idiosyncrasies of the gufi_vt module:
 *
 *     CREATE VIRTUAL TABLE temp.gufi
 *     USING gufi_vt(threads=2, E="SELECT * FROM pentries", index=path);
 *
 *     SELECT * FROM gufi;
 *
 * See the respective comments below for details.
 */

typedef struct gufi_query_sql {
    refstr_t I;
    refstr_t T;
    refstr_t S;
    refstr_t E;
    refstr_t K;
    refstr_t J;
    refstr_t G;
    refstr_t F;
} gq_sql_t;

typedef struct gufi_vtab {
    sqlite3_vtab base;
    const char *indexroot;     /* if this is present, CREATE VIRTUAL TABLE was called */
    const char *threads;       /* if this is present, CREATE VIRTUAL TABLE was called, but use indexroot to determine if this is a gufi_vt or gufi_vt_* */
    gq_sql_t sql;
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
static int gufi_query(const char *indexroot, const char *threads, const gq_sql_t *sql,
                      FILE **output, char **errmsg) {
    const char *argv[23] = {
        "gufi_query",
        "-u",
        "-x",
    };

    #define set_argv(argc, argv, flag, value) if (value) { argv[argc++] = flag; argv[argc++] = value; }

    int argc = 3;
    set_argv(argc, argv, "-n", threads);
    set_argv(argc, argv, "-I", sql->I.data);
    set_argv(argc, argv, "-T", sql->T.data);
    set_argv(argc, argv, "-S", sql->S.data);
    set_argv(argc, argv, "-E", sql->E.data);
    set_argv(argc, argv, "-K", sql->K.data);
    set_argv(argc, argv, "-J", sql->J.data);
    set_argv(argc, argv, "-G", sql->G.data);
    set_argv(argc, argv, "-F", sql->F.data);

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
    FILE *out = popen(cmd, "r");

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
    static const size_t ROW_PREFIX = sizeof(size_t) + sizeof(int);
    char *buf = NULL;
    char *curr = buf;

    size_t *starts = NULL; /* index of where each column starts in buf */
    size_t row_len = 0;
    int count = 0;

    /* row length */
    if (fread(&row_len, sizeof(char), sizeof(row_len), pCur->output) != sizeof(row_len)) {
        if (ferror(pCur->output)) { /* eof at other reads are always errors */
            pCur->base.pVtab->zErrMsg = sqlite3_mprintf("Error: Could not read row length");
        }
        goto error;
    }

    /* column count */
    if (fread(&count, sizeof(char), sizeof(count), pCur->output) != sizeof(count)) {
        pCur->base.pVtab->zErrMsg = sqlite3_mprintf("Error: Could not read column count");
        goto error;
    }

    row_len -= ROW_PREFIX;

    /* buf does not contain row prefix */
    buf = malloc(row_len + 1); /* allow for NULL terminator */
    curr = buf;
    starts = malloc(count * sizeof(size_t));

    for(int i = 0; i < count; i++) {
        /* column start points to type */
        starts[i] = curr - buf;

        /* read type and length */
        const size_t tl = fread(curr, sizeof(char), TL, pCur->output);
        if (tl != TL) {
            pCur->base.pVtab->zErrMsg = sqlite3_mprintf("Error: Could not read type and length from column %d",
                                                        i);
            goto error;
        }

        const size_t value_len = * (size_t *) (curr + sizeof(char));

        curr += TL;   /* to go to start of value */

        const size_t v = fread(curr, sizeof(char), value_len, pCur->output);
        if (v != value_len) {
            pCur->base.pVtab->zErrMsg = sqlite3_mprintf("Error: Could not read %zu octets. Got %zu",
                                                        value_len, v);
            goto error;
        }

        curr += value_len;
    }

    pCur->row = buf;
    pCur->len = row_len;
    pCur->col_starts = starts;
    pCur->col_count = count;

    return 0;

  error:
    free(starts);
    pCur->col_starts = NULL;
    pCur->col_count = 0;
    free(buf);
    pCur->row = NULL;
    pCur->len = 0;
    return 1;
}

/* remove starting/ending quotation marks */
static void set_sql(refstr_t *refstr, char *value) {
    refstr->data = value;
    refstr->len = 0;

    if (refstr->data) {
        if ((refstr->data[0] == '\'') ||
            (refstr->data[0] == '"')) {
            refstr->data++;
        }

        refstr->len = strlen(refstr->data);

        if ((refstr->data[refstr->len - 1] == '\'') ||
            (refstr->data[refstr->len - 1] == '"')) {
            ((char *) refstr->data)[--refstr->len] = '\0';
        }
    }
}

/* generic connect function */
static int gufi_vtConnect(sqlite3 *db, void *pAux,
                          int argc, const char *const*argv,
                          sqlite3_vtab **ppVtab,
                          char **pzErr,
                          const char *schema,
                          const char *threads,
                          const gq_sql_t *sql,
                          const char *indexroot) {
    (void) pAux; (void) pzErr;
    (void) argc; (void) argv;

    gufi_vtab *pNew = NULL;
    const int rc = sqlite3_declare_vtab(db, schema);
    if(rc == SQLITE_OK){
        pNew = (gufi_vtab *)sqlite3_malloc( sizeof(*pNew) );
        if( pNew==0 ) return SQLITE_NOMEM;
        memset(pNew, 0, sizeof(*pNew));
        pNew->threads = threads;
        pNew->sql = *sql;
        pNew->indexroot = indexroot;
        /* sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY); */
    }

    *ppVtab = &pNew->base;
    return rc;
}

/* *********************************************************************** */
/*
 * GUFI Eponymous-only virtual tables/table-valued functions
 *
 * This allows for users to query single table names from GUFI trees
 * as though they were tables via the SQLite3 Virtual Table
 * Mechanism. These virtual tables act as table-valued functions, so
 * CREATE VIRTUAL TABLE is not necessary.
 *
 * The schemas of these virtual tables are fixed:
 *     SELECT path(), epath(), fpath(), path()/rpath(), level(), * FROM <table>;
 *
 * First, the UDFs added to SQLite3 by GUFI that do not require
 * internal state are added to SQLite3 when this module is loaded.
 *
 * Then, 6 virtual tables are added:
 *
 *    gufi_vt_treesummary
 *    gufi_vt_summary
 *    gufi_vt_entries
 *    gufi_vt_pentries
 *    gufi_vt_vrsummary
 *    gufi_vt_vrpentries
 *
 * These may be used like so:
 *     SELECT ...
 *     FROM gufi_vt_*('<indexroot>'[, threads])
 *     ... ;
 *
 * The following are positional arguments to each virtual table:
 *
 *     index root
 *     # of threads
 *
 * The index root argument is required. The thread count is optional.
 *
 * GUFI user defined functions (UDFs) that do not require gufi_query
 * state may be called. UDFs requiring gufi_query state (path(), epath(),
 * fpath(), and rpath()) can be accessed from the virtual table by using
 * columns with the same names.
 *
 * The schemas of all 6 of the corresponding tables and views are
 * recreated here, and thus all columns are accessible.
 */

/* positional arguments */
#define GUFI_VT_ARGS_INDEXROOT       0
#define GUFI_VT_ARGS_THREADS         1
#define GUFI_VT_ARGS_COUNT           2

#define GUFI_VT_ARG_COLUMNS          "indexroot TEXT HIDDEN, threads INT64 HIDDEN, "

#define GUFI_VT_EXTRA_COLUMNS        "path TEXT, epath TEXT, fpath TEXT, rpath TEXT, level INT64, "
#define GUFI_VT_EXTRA_COLUMNS_SQL    "path(), epath(), fpath(), path(), level(), "
#define GUFI_VT_EXTRA_COLUMNS_SQL_VR "path(), epath(), fpath(), rpath(sname, sroll), level(), "

#define GUFI_VT_ALL_COLUMNS          GUFI_VT_ARG_COLUMNS      \
                                     GUFI_VT_EXTRA_COLUMNS

#define PENTRIES_SCHEMA(name, extra_cols)                                         \
    "CREATE TABLE " name "(" extra_cols                                           \
    "name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64, "                 \
    "uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, "             \
    "atime INT64, mtime INT64, ctime INT64, "                                     \
    "linkname TEXT, xattr_names BLOB, crtime INT64, "                             \
    "ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, "                \
    "osstext1 TEXT, osstext2 TEXT, pinode TEXT, ppinode TEXT"                     \
    ");"                                                                          \

#define VRSUMMARY_SCHEMA(name, extra_cols)                                        \
    "CREATE TABLE " name "(" extra_cols                                           \
    "dname TEXT, sname TEXT, sroll INT64, srollsubdirs INT64, "                   \
    "name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64, "                 \
    "uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, "             \
    "atime INT64, mtime INT64, ctime INT64, "                                     \
    "linkname TEXT, xattr_names BLOB, "                                           \
    "totfiles INT64, totlinks INT64, "                                            \
    "minuid INT64, maxuid INT64, mingid INT64, maxgid INT64, "                    \
    "minsize INT64, maxsize INT64, totzero INT64, "                               \
    "totltk INT64, totmtk INT64, "                                                \
    "totltm INT64, totmtm INT64, "                                                \
    "totmtg INT64, totmtt INT64, "                                                \
    "totsize INT64, "                                                             \
    "minctime INT64, maxctime INT64, totctime INT64, "                            \
    "minmtime INT64, maxmtime INT64, totmtime INT64, "                            \
    "minatime INT64, maxatime INT64, totatime INT64, "                            \
    "minblocks INT64, maxblocks INT64, totblocks INT64, "                         \
    "totxattr INT64, depth INT64, "                                               \
    "mincrtime INT64, maxcrtime INT64, totcrtime INT64, "                         \
    "minossint1 INT64, maxossint1 INT64, totossint1 INT64, "                      \
    "minossint2 INT64, maxossint2 INT64, totossint2 INT64, "                      \
    "minossint3 INT64, maxossint3 INT64, totossint3 INT64, "                      \
    "minossint4 INT64, maxossint4 INT64, totossint4 INT64, "                      \
    "rectype INT64, pinode TEXT, isroot INT64, rollupscore INT64"                 \
    ");"

#define VRPENTRIES_SCHEMA(name, extra_cols)                                       \
    "CREATE TABLE " name "(" extra_cols                                           \
    "dname TEXT, sname TEXT, dmode INT64, dnlink INT64, "                         \
    "duid INT64, dgid INT64, "                                                    \
    "dsize INT64, dblksize INT64, dblocks INT64, "                                \
    "datime INT64, dmtime INT64, dctime INT64, "                                  \
    "dlinkname TEXT, "                                                            \
    "dtotfile INT64, dtotlinks INT64, "                                           \
    "dminuid INT64, dmaxuid INT64, dmingid INT64, dmaxgid INT64, "                \
    "dminsize INT64, dmaxsize INT64, dtotzero INT64, "                            \
    "dtotltk INT64, dtotmtk INT64, "                                              \
    "totltm INT64, dtotmtm INT64, "                                               \
    "dtotmtg INT64, dtotmtt INT64, "                                              \
    "dtotsize INT64, "                                                            \
    "dminctime INT64, dmaxctime INT64, dtotctime INT64, "                         \
    "dminmtime INT64, dmaxmtime INT64, dtotmtime INT64, "                         \
    "dminatime INT64, dmaxatime INT64, dtotatime INT64, "                         \
    "dminblocks INT64, dmaxblocks INT64, dtotblocks INT64, "                      \
    "dtotxattr INT64, ddepth INT64, "                                             \
    "dmincrtime INT64, dmaxcrtime INT64, dtotcrtime INT64, "                      \
    "sroll INT64, atroot INT64, srollsubdirs INT64, "                             \
    "name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64, "                 \
    "uid INT64, gid INT64, "                                                      \
    "size INT64, blksize INT64, blocks INT64, "                                   \
    "atime INT64, mtime INT64, ctime INT64, "                                     \
    "linkname TEXT, xattr_names BLOB, "                                           \
    "crtime INT64, "                                                              \
    "ossint1 INT64, ossint2 INT64, ""ossint3 INT64, ossint4 INT64, "              \
    "osstext1 TEXT, osstext2 TEXT, "                                              \
    "pinode TEXT, ppinode TEXT"                                                   \
    ");"

#define SELECT_FROM(name)                             \
    "SELECT " GUFI_VT_EXTRA_COLUMNS_SQL "* "          \
    "FROM " name ";"                                  \

#define SELECT_FROM_VR(name)                          \
    "SELECT " GUFI_VT_EXTRA_COLUMNS_SQL_VR "* "       \
    "FROM " name ";"                                  \

/* generate xConnect function for each virtual table */
#define gufi_vt_xConnect(name, abbrev, t, s, e, vr)                            \
    static int gufi_vt_ ##abbrev ##Connect(sqlite3 *db,                        \
                                           void *pAux,                         \
                                           int argc, const char * const *argv, \
                                           sqlite3_vtab **ppVtab,              \
                                           char **pzErr) {                     \
        /* this is what the virtual table looks like */                        \
        /* the schema is fixed for this function */                            \
        const char schema[] =                                                  \
            name ##_SCHEMA(name, GUFI_VT_ALL_COLUMNS);                         \
                                                                               \
        static const char select_from[]    = SELECT_FROM(name);                \
        static const char select_from_vr[] = SELECT_FROM_VR(name);             \
                                                                               \
        /* fixed queries */                                                    \
        gq_sql_t sql;                                                          \
        memset(&sql, 0, sizeof(sql));                                          \
        set_sql(&sql.T, (char *) (t?select_from:NULL));                        \
        set_sql(&sql.S, (char *) (s?(vr?select_from_vr:select_from):NULL));    \
        set_sql(&sql.E, (char *) (e?(vr?select_from_vr:select_from):NULL));    \
                                                                               \
        return gufi_vtConnect(db, pAux, argc, argv, ppVtab, pzErr,             \
                              schema, NULL, &sql, NULL);                       \
     }

/* generate xConnect for each table/view */
gufi_vt_xConnect(TREESUMMARY, T,   1, 0, 0, 0)
gufi_vt_xConnect(SUMMARY,     S,   0, 1, 0, 0)
gufi_vt_xConnect(ENTRIES,     E,   0, 0, 1, 0)
gufi_vt_xConnect(PENTRIES,    P,   0, 0, 1, 0)
gufi_vt_xConnect(VRSUMMARY,   VRS, 0, 1, 0, 1)
gufi_vt_xConnect(VRPENTRIES,  VRP, 0, 0, 1, 1)

/* *********************************************************************** */

/* *********************************************************************** */
/*
 * GUFI Generic Virtual Table
 *
 * This is intended for power users to be able to run arbitrary
 * queries and generate virtual tables with arbitrary result
 * columns. In order to achieve this, users MUST run "CREATE VIRTUAL
 * TABLE" to create the virtual table before querying it. The virtual
 * table should preferably to the temp namespace:
 *
 *     CREATE VIRTUAL TABLE temp.gufi
 *     USING gufi_vt(threads=2, E="SELECT * FROM pentries", index=path);
 *
 * All arguments should be key=value pairs. The allowed keys are:
 *     n or threads
 *     I
 *     T
 *     S
 *     E
 *     K
 *     J
 *     G
 *     F
 *     index
 *
 * Notes:
 *     - Arguments without '=' are considered index paths
 *     - Arguments may appear in any order.
 *     - If there are duplicate arguments, the right-most duplicate will be used
 *     - When determining the virtual table's schema, there are two separate cases:
 *           - If NOT aggregating, the query that is processed latest will be used:
 *                 - If T is passed in, but not S or E, it will be used
 *                 - If S is passed in, but not E, it will be used
 *                 - If E is passed in, it will be used
 *           - If aggregating (K is passed in), the G SQL will be used
 *
 * Then, SELECT from the virtual table
 *
 *    SELECT * FROM gufi;
 *
 * Notes:
 *     - If the query that was used to generate the schema modified a
 *       column WITHOUT aliasing it, the column will be available as
 *       the original string:
 *
 *           CREATE VIRTUAL TABLE temp.gufi
 *           USING gufi_vt(E="SELECT path() || '/' || name FROM pentries", ...);
 *
 *           SELECT "path() || '/' || name" FROM gufi;
 *
 *     - If the query that was used to generate the schema modified a
 *       column AND aliased it, the column will be available as the
 *       alias:
 *
 *           CREATE VIRTUAL TABLE temp.gufi
 *           USING gufi_vt(E="SELECT path() || '/' || name AS fullpath FROM pentries", ...);
 *
 *           SELECT fullpath FROM gufi;
 *
 *     - Due to how the virtual table schema interacts with gufi_query
 *       output, there are behaviors that should be noted:
 *           - If T or S is used when E is used, and the T
 *             or S returns has more columns than E, they will be dropped.
 *
 *                 CREATE VIRTUAL TABLE temp.gufi
 *                 USING gufi_vt(S="SELECT name, size, atime, mtime, ctime FROM summary",
 *                               E="SELECT name, size FROM pentries", ...);
 *
 *                 SELECT * FROM gufi;
 *
 *                 Only name and size for output from S will be printed
 *
 *           - Additionally, the T and S columns will be mapped into E's columns.
 *
 *                 CREATE VIRTUAL TABLE temp.gufi
 *                 USING gufi_vt(S="size, name FROM summary",
 *                               E="SELECT name, size FROM pentries", ...);
 *
 *                 SELECT * FROM gufi;
 *
 *                 Output from S will show integer followed by text, instead
 *                 of text followed by integer
 *
 *           - If selecting a column further right than is available due to
 *             the number of columns in an output row being different, NULL
 *             will be returned:
 *
 *                 CREATE VIRTUAL TABLE temp.gufi
 *                 USING gufi_vt(S="name FROM summary",
 *                               E="SELECT name, size FROM pentries", ...);
 *
 *                 SELECT size FROM gufi;
 *
 *                 Rows from S should print a blank column,
 *                 while rows from E will have both
 */

static int get_cols(sqlite3 *db, refstr_t *sql, int **types,
                    char ***names, size_t **lens, int *cols) {
    int type_cols = 0;
    if ((get_col_types(db, sql, types, &type_cols) != 0) ||
        (get_col_names(db, sql, names, lens, cols) != 0)) {
        return 1;
    }

    /*
     * double check that types and names have the same number of columns
     *
     * this should never be true
     */
    return (type_cols != *cols);
}

static int gufi_vtpu_xConnect(sqlite3 *db,
                              void *pAux,
                              int argc, const char * const *argv,
                              sqlite3_vtab **ppVtab,
                              char **pzErr) {
    static const char ONE[] = "1";
    const char *threads = ONE; /* default value */
    gq_sql_t sql;
    const char *indexroot = NULL;

    memset(&sql, 0, sizeof(sql));

    /* parse args */
    for(int i = 3; i < argc; i++) {
        char *value = NULL;
        char *key   = strtok_r((char *) argv[i], "=", &value);
        const size_t len = strlen(key);

        /* assume this is an index path */
        if (!value || (value == (key + len))) {
            indexroot = argv[i]; /* keep last one */
            continue;
        }

        if (len == 1) {
            switch (*key) {
                case 'n':
                    ;
                    size_t nthreads = 0;
                    if ((sscanf(value, "%zu", &nthreads) != 1) || (nthreads == 0)) {
                        *pzErr = sqlite3_mprintf("Bad thread count: %s", value);
                        return SQLITE_MISUSE;
                    }
                    threads = value;
                    break;
                case 'I':
                    set_sql(&sql.I, value);
                    break;
                case 'T':
                    set_sql(&sql.T, value);
                    break;
                case 'S':
                    set_sql(&sql.S, value);
                    break;
                case 'E':
                    set_sql(&sql.E, value);
                    break;
                case 'K':
                    set_sql(&sql.K, value);
                    break;
                case 'J':
                    set_sql(&sql.J, value);
                    break;
                case 'G':
                    set_sql(&sql.G, value);
                    break;
                case 'F':
                    set_sql(&sql.F, value);
                    break;
                default:
                    break;
            }
        }
        else if (len == 5) {
            if (strncmp(key, "index", 6) == 0) {
                indexroot = value; /* keep last one */
            }
        }
        else if (len == 7) {
            if (strncmp(key, "threads", 8) == 0) {
                size_t nthreads = 0;
                if ((sscanf(value, "%zu", &nthreads) != 1) || (nthreads == 0)) {
                    *pzErr = sqlite3_mprintf("Bad thread count: %s", value);
                    return SQLITE_MISUSE;
                }
                threads = value;
            }
        }
    }

    if (!indexroot) {
        *pzErr = sqlite3_mprintf("Missing indexroot");
        return SQLITE_CONSTRAINT;
    }

    /* remove quotes surrounding indexroot */
    if ((indexroot[0] == '\'') ||
        (indexroot[0] == '"')) {
        indexroot++;
    }

    /* get column types of results from gufi_query */

    /* make sure all setup tables are placed into a different db */
    /* this should never fail */
    sqlite3 *tempdb = opendb(SQLITE_MEMORY, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                             0, 0, create_dbdb_tables, NULL);

    create_xattr_tables(SQLITE_MEMORY, tempdb, NULL);

    struct input in;
    in.process_xattrs = 1;
    struct work work; /* unused */
    memset(&work, 0, sizeof(work));
    size_t count = 0; /* unused */
    setup_xattrs_views(&in, tempdb, &work, &count);

    int cols = 0;
    int *types = NULL;
    char **names = NULL;
    size_t *lens = NULL;

    /* this should never fail */
    addqueryfuncs_with_context(tempdb, &work);

    if (sql.T.len) {
        /* this should never fail */
        create_table_wrapper(SQLITE_MEMORY, tempdb, TREESUMMARY, TREESUMMARY_CREATE);
    }

    /* if not aggregating, get types for T, S, or E */
    if (!sql.K.len) {
        int rc = -1; /* default to error */
        if (sql.T.len && !sql.S.len && !sql.E.len) {
            rc = get_cols(tempdb, &sql.T, &types, &names, &lens, &cols);
        }
        else if (sql.S.len && !sql.E.len) {
            rc = get_cols(tempdb, &sql.S, &types, &names, &lens, &cols);
        }
        else if (sql.E.len) {
            rc = get_cols(tempdb, &sql.E, &types, &names, &lens, &cols);
        }
        if (rc != 0) {
            goto error;
        }
    }
    /* types for G */
    else {
        /* run -K so -G can pull the final columns */
        char *err = NULL;
        if (sqlite3_exec(tempdb, sql.K.data, NULL, NULL, &err) != SQLITE_OK) {
            *pzErr = sqlite3_mprintf("-K SQL failed while getting columns types: %s", err);
            sqlite3_free(err);
            goto error;
        }
        if (get_cols(tempdb, &sql.G, &types, &names, &lens, &cols) != 0) {
            goto error;
        }
    }

    static const char *SQL_TYPES[] = {
        NULL,
        "INTEGER",
        "FLOAT",
        "TEXT",
        "BLOB",
        "NULL"
    };

    /* construct the schema for the virtual table */
    char schema[MAXSQL];
    char *ptr = schema + SNPRINTF(schema, sizeof(schema), "CREATE TABLE x(");
    for(int c = 0; c < cols; c++) {
        ptr += SNPRINTF(ptr, sizeof(schema) - (ptr - schema), "\"%s\" %s, ",
                        names[c], SQL_TYPES[types[c]]);
        free(names[c]);
    }
    ptr -= 2; /* remove trailing ", " */
    SNPRINTF(ptr, sizeof(schema) - (ptr - schema), ");");

    free(lens);
    free(names);
    free(types);

    closedb(tempdb);

    const size_t indexroot_len = strlen(indexroot);
    if ((indexroot[indexroot_len - 1] == '\'') ||
        (indexroot[indexroot_len - 1] == '"')) {
        ((char *) indexroot)[indexroot_len - 1] = '\0';
    }

    return gufi_vtConnect(db, pAux, argc, argv, ppVtab, pzErr,
                          schema, threads, &sql, indexroot);
  error:
    free(lens);
    if (names) {
        for(int c = 0; c < cols; c++) {
            free(names[c]);
        }
        free(names);
    }
    free(types);

    closedb(tempdb);
    return SQLITE_ERROR;
}

/* different address to get real virtual table */
static int gufi_vtpu_xCreate(sqlite3 *db,
                             void *pAux,
                             int argc, const char * const *argv,
                             sqlite3_vtab **ppVtab,
                             char **pzErr) {
    return gufi_vtpu_xConnect(db, pAux, argc, argv,
                              ppVtab, pzErr);
}
/* *********************************************************************** */

/* FIXME: This is probably not correct */
static int gufi_vtBestIndex(sqlite3_vtab *tab,
                            sqlite3_index_info *pIdxInfo) {
    gufi_vtab *vtab = (gufi_vtab *) tab;
    if (vtab->indexroot) {
        pIdxInfo->estimatedCost = 1000000;
    }
    else {
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

    const char *indexroot = vtab->indexroot;
    const char *threads = vtab->threads;

    /* gufi_vt_* */
    if (!indexroot) {
        /* indexroot must be present */
        indexroot = (const char *) sqlite3_value_text(argv[GUFI_VT_ARGS_INDEXROOT]);

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
    }

    /* kick off gufi_query */
    const int rc = gufi_query(indexroot, threads, &vtab->sql,
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

    gufi_vtab *pVtab = (gufi_vtab *) cur->pVtab;

    /*
     * if this row came from a query that was not the query that was
     * used to generate the virtual table's schema, and the position
     * of the selected column is past what is available in this row,
     * return NULL
     */
    if (N >= pCur->col_count) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    const char *buf = pCur->row + pCur->col_starts[N - (pVtab->indexroot?0:GUFI_VT_ARGS_COUNT)];
    const char type = *buf;
    const size_t len = * (size_t *) (buf + 1);
    const char *col = buf + TL;

    switch(type) {
        case SQLITE_INTEGER:
            {
                const char orig = col[len];
                ((char *)col)[len] = '\0';

                int value = 0;
                if (sscanf(col, "%d", &value) == 1) {
                    sqlite3_result_int(ctx, value);
                }
                else {
                    sqlite3_result_text(ctx, col, len, SQLITE_TRANSIENT);
                }
                ((char *)col)[len] = orig;
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

#define create_module(module_name, create, connect)                          \
    {                                                                        \
        static sqlite3_module module;                                        \
        memcpy(&module, &gufi_vtModule, sizeof(gufi_vtModule));              \
        module.xCreate = create;                                             \
        module.xConnect = connect;                                           \
        module.xDisconnect = gufi_vtDisconnect;                              \
        module.xDestroy = gufi_vtDisconnect;                                 \
        const int rc = sqlite3_create_module(db, module_name, &module, 0);   \
        if (rc != SQLITE_OK) {                                               \
            return rc;                                                       \
        }                                                                    \
    }

/* no underscore between gufi and vt for entry point */
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_gufivt_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi) {
    (void) pzErrMsg;

    SQLITE_EXTENSION_INIT2(pApi);

    if (addqueryfuncs(db) != 0) {
        return SQLITE_ERROR;
    }

    /* fixed schemas - SELECT directly from these */
    create_module("gufi_vt_treesummary",   NULL,                gufi_vt_TConnect);
    create_module("gufi_vt_summary",       NULL,                gufi_vt_SConnect);
    create_module("gufi_vt_entries",       NULL,                gufi_vt_EConnect);
    create_module("gufi_vt_pentries",      NULL,                gufi_vt_PConnect);
    create_module("gufi_vt_vrsummary",     NULL,                gufi_vt_VRSConnect);
    create_module("gufi_vt_vrpentries",    NULL,                gufi_vt_VRPConnect);

    /* dynamic schema - requires CREATE VIRTUAL TABLE */
    create_module("gufi_vt",               gufi_vtpu_xCreate,   gufi_vtpu_xConnect);

    return SQLITE_OK;
}
