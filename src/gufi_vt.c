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

#include "bf.h"
#include "dbutils.h"

#define INTERMEDIATE "intermediate"
#define AGGREGATE    "aggregate"

typedef struct gufi_query_args {
    const char *I;
    const char *T;
    const char *S;
    const char *E;
    const char *K;
    const char *J;
    const char *G;
    const char *F;
} gq_args_t;

/*
 * run gufi_query, aggregating results into a single db file
 *
 * have to fork+exec - cannot link gufi_query in without changing
 * everything to link dynamically
 */
static int gufi_query_aggregate_db(const char *indexroot, const size_t nthreads, const gq_args_t *gqa,
                                   const char *prefix, char **outdb, char **errmsg) {
    /* TODO: allow for aggregate databases to be placed anywhere */
    const size_t outdb_len = (prefix?strlen(prefix):0) + 14;
    *outdb = sqlite3_malloc(outdb_len + 1);
    if (prefix) {
        snprintf(*outdb, outdb_len + 1, "%s/gufi_vt.XXXXXX", prefix);
    }
    else {
        snprintf(*outdb, outdb_len + 1, "gufi_vt.XXXXXX");
    }

    const int fd = mkstemp(*outdb);
    if (fd < 0) {
        const int err = errno;
        *errmsg = sqlite3_mprintf("mkstemp('%s') failed: %s (%d)", *outdb, strerror(err), err);
        sqlite3_free(*outdb);
        *outdb = NULL;
        return EXIT_FAILURE;
    }
    close(fd);

    /* ceil(log10(1<<64)) = 20 */
    char *nthreads_str = malloc(21);
    snprintf(nthreads_str, 21, "%zu", nthreads);

    const char *argv[23] = {
        "gufi_query",
        "-n", nthreads_str,
        "-O", *outdb,
    };

    #define set_argv(argc, argv, flag, value) if (value) { argv[argc++] = flag; argv[argc++] = value; }

    size_t argc = 5;
    set_argv(argc, argv, "-I", gqa->I);
    set_argv(argc, argv, "-T", gqa->T);
    set_argv(argc, argv, "-S", gqa->S);
    set_argv(argc, argv, "-E", gqa->E);
    set_argv(argc, argv, "-K", gqa->K);
    set_argv(argc, argv, "-J", gqa->J);
    set_argv(argc, argv, "-G", gqa->G);
    set_argv(argc, argv, "-F", gqa->F);

    argv[argc++] = indexroot;
    argv[argc]   = NULL;

    const pid_t pid = fork();
    if (pid == -1) {
        const int err = errno;
        *errmsg = sqlite3_mprintf("Failed to fork: %s (%d)", strerror(err), err);
        return 0;
    }

    /* child */
    if (pid == 0) {
        /* using execvp to allow for gufi_query executable to be selected by PATH */
        const int rc = execvp("gufi_query", (char * const *) argv);
        if (rc != 0) {
            const int err = errno;
            fprintf(stderr, "Failed to start gufi_query: %s (%d)\n", strerror(err), err);
            free(nthreads_str);
            return EXIT_FAILURE;
        }
        /* can't get here */
    }

    free(nthreads_str);

    /* parent */
    int status = EXIT_SUCCESS;
    const int rc = waitpid(pid, &status, 0);
    if (rc != pid) {
        const int err = errno;
        fprintf(stderr, "Failed to wait for gufi_query: %s (%d)\n", strerror(err), err);
        return EXIT_FAILURE;
    }

    if (status != EXIT_SUCCESS) {
        return SQLITE_ERROR;
    }

    return SQLITE_OK;
}

typedef struct gufi_vtab {
    sqlite3_vtab base;
    const char *indexroot;
    size_t nthreads;
    gq_args_t gqa; /* not const to allow for changes */

    const char *output_prefix;

    /* TODO: track multiple aggregate result files to not rerun queries unnecessarily */
    /*       hash(sql) -> file name */
    char *dbname; /* keep track of aggregate db file name to delete upon unload */
} gufi_vtab;

typedef struct gufi_vtab_cursor {
    sqlite3_vtab_cursor base;
    sqlite3 *db;               /* the aggregated results database file */
    sqlite3_stmt *stmt;        /* compiled SQL pulling from aggregate database */
    sqlite_int64 rowid;        /* current row id */
    int res;                   /* previous sqlite3_step return code */
} gufi_vtab_cursor;

/* generic connect function */
static int gufivtConnect(sqlite3 *db,
                         void *pAux,
                         int argc, const char *const*argv,
                         sqlite3_vtab **ppVtab,
                         char **pzErr,
                         const char *schema,
                         const gq_args_t *gqa) {
    (void) pAux;

    if (argc < 4) {
        *pzErr = sqlite3_mprintf("gufi_vt constructor requires at least one argument");
        return SQLITE_ERROR;
    }

    gufi_vtab *pNew = NULL;
    const int rc = sqlite3_declare_vtab(db, schema);
    if(rc == SQLITE_OK){
        pNew = (gufi_vtab *)sqlite3_malloc( sizeof(*pNew) );
        if( pNew==0 ) return SQLITE_NOMEM;
        memset(pNew, 0, sizeof(*pNew));

        pNew->indexroot = argv[3];
        pNew->nthreads = 1;
        if (argc > 4) {
            if (sscanf(argv[4], "%zu", &pNew->nthreads) != 1) {
                *pzErr = sqlite3_mprintf("gufi_vt invalid thread count: '%s'", argv[4]);
                sqlite3_free(pNew);
                return SQLITE_ERROR;
            }
        }

        if (argc > 5) {
            pNew->output_prefix = argv[5];
        }

        pNew->gqa = *gqa;

        /* sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY); */
    }

    *ppVtab = &pNew->base;
    return rc;
}

#define PENTRIES_SCHEMA(name) \
    "CREATE TABLE " name "(name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattr_names BLOB, crtime INT64, ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, osstext1 TEXT, osstext2 TEXT, pinode TEXT, ppinode TEXT);"

#define VRSUMMARY_SCHEMA(name) \
    "CREATE TABLE " name "(dname TEXT, sname TEXT, sroll INT64, srollsubdirs INT64, name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattr_names BLOB, totfiles INT64, totlinks INT64, minuid INT64, maxuid INT64, mingid INT64, maxgid INT64, minsize INT64, maxsize INT64, totzero INT64, totltk INT64, totmtk INT64, totltm INT64, totmtm INT64, totmtg INT64, totmtt INT64, totsize INT64, minctime INT64, maxctime INT64, minmtime INT64, maxmtime INT64, minatime INT64, maxatime INT64, minblocks INT64, maxblocks INT64, totxattr INT64, depth INT64, mincrtime INT64, maxcrtime INT64, minossint1 INT64, maxossint1 INT64, totossint1 INT64, minossint2 INT64, maxossint2 INT64, totossint2 INT64, minossint3 INT64, maxossint3 INT64, totossint3 INT64, minossint4 INT64, maxossint4 INT64, totossint4 INT64, rectype INT64, pinode TEXT, isroot INT64, rollupscore INT64);"

#define VRPENTRIES_SCHEMA(name) \
    "CREATE TABLE " name "(dname TEXT, sname TEXT, dmode INT64, dnlink INT64, duid INT64, dgid INT64, dsize INT64, dblksize INT64, dblocks INT64, datime INT64, dmtime INT64, dctime INT64, dlinkname TEXT, dtotfile INT64, dtotlinks INT64, dminuid INT64, dmaxuid INT64, dmingid INT64, dmaxgid INT64, dminsize INT64, dmaxsize INT64, dtotzero INT64, dtotltk INT64, dtotmtk INT64, totltm INT64, dtotmtm INT64, dtotmtg INT64, dtotmtt INT64, dtotsize INT64, dminctime INT64, dmaxctime INT64, dminmtime INT64, dmaxmtime INT64, dminatime INT64, dmaxatime INT64, dminblocks INT64, dmaxblocks INT64, dtotxattr INT64, ddepth INT64, dmincrtime INT64, dmaxcrtime INT64, sroll INT64, atroot INT64, srollsubdirs INT64, name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattr_names BLOB, crtime INT64, ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, osstext1 TEXT, osstext2 TEXT, pinode TEXT, ppinode TEXT);"

#define SELECT_FROM(name) ("INSERT INTO " INTERMEDIATE " SELECT * FROM " name ";")

/* generate xConnect function for each virtual table */
#define gufivtXConnect(name, abbrev, t, s, e)                                 \
    static int gufivt ##abbrev ##Connect(sqlite3 *db,                         \
                                         void *pAux,                          \
                                         int argc, const char * const *argv,  \
                                         sqlite3_vtab **ppVtab,               \
                                         char **pzErr) {                      \
        static const char schema[] =                                          \
            name ##_SCHEMA(name);                                             \
                                                                              \
        static const gq_args_t gqa = {                                        \
            .I = name ##_SCHEMA(INTERMEDIATE),                                \
            .T = t?SELECT_FROM(name):NULL,                                    \
            .S = s?SELECT_FROM(name):NULL,                                    \
            .E = e?SELECT_FROM(name):NULL,                                    \
            .K = name ##_SCHEMA(AGGREGATE),                                   \
            .J = "INSERT INTO " AGGREGATE " SELECT * FROM " INTERMEDIATE ";", \
            .G = NULL,                                                        \
            .F = NULL,                                                        \
        };                                                                    \
                                                                              \
        return gufivtConnect(db, pAux, argc, argv, ppVtab, pzErr,             \
                             schema, &gqa);                                   \
    }

gufivtXConnect(TREESUMMARY, T,   1, 0, 0)
gufivtXConnect(SUMMARY,     S,   0, 1, 0)
gufivtXConnect(ENTRIES,     E,   0, 0, 1)
gufivtXConnect(PENTRIES,    P,   0, 0, 1)
gufivtXConnect(VRSUMMARY,   VRS, 0, 1, 0)
gufivtXConnect(VRPENTRIES,  VRP, 0, 0, 1)

static int gufivtBestIndex(sqlite3_vtab *tab,
                           sqlite3_index_info *pIdxInfo) {
    (void) tab;
    (void) pIdxInfo;
    return SQLITE_OK;
}

static int gufivtDisconnect(sqlite3_vtab *pVtab) {
    gufi_vtab *vtab = (gufi_vtab *) pVtab;
    remove(vtab->dbname); /* check for error? */
    sqlite3_free(vtab->dbname);
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int gufivtOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor) {
    gufi_vtab *vtab = (gufi_vtab *) p;

    /* run gufi_query to get aggregate results */
    int rc = gufi_query_aggregate_db(vtab->indexroot, vtab->nthreads, &vtab->gqa,
                                     vtab->output_prefix, &vtab->dbname, &p->zErrMsg);

    if (rc != EXIT_SUCCESS) {
        sqlite3_free(vtab->dbname);
        vtab->dbname = NULL;
        return SQLITE_ERROR;
    }

    gufi_vtab_cursor *pCur = sqlite3_malloc( sizeof(*pCur) );
    if( pCur==0 ) return SQLITE_NOMEM;
    memset(pCur, 0, sizeof(*pCur));

    /* open the aggregate db file */
    if (sqlite3_open_v2(vtab->dbname, &pCur->db, SQLITE_OPEN_READONLY, GUFI_SQLITE_VFS) != SQLITE_OK) {
        sqlite3_free(p->zErrMsg);
        p->zErrMsg = sqlite3_mprintf("Could not open aggregate db %s", vtab->dbname);
        sqlite3_free(vtab->dbname);
        vtab->dbname = NULL;
        return SQLITE_ERROR;
    }

    *ppCursor = &pCur->base;

    return SQLITE_OK;
}

static int gufivtClose(sqlite3_vtab_cursor *cur) {
    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;
    sqlite3_finalize(pCur->stmt);
    sqlite3_close(pCur->db);
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int gufivtFilter(sqlite3_vtab_cursor *cur,
                        int idxNum, const char *idxStr,
                        int argc, sqlite3_value **argv) {
    (void) idxNum; (void) idxStr;
    (void) argc;   (void) argv;

    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;

    /* set up SQL for retreiving data from results table */
    static const char SELECT_AGG[] = "SELECT * FROM " AGGREGATE ";";
    if (sqlite3_prepare_v2(pCur->db, SELECT_AGG, sizeof(SELECT_AGG), &pCur->stmt, NULL) != SQLITE_OK) {
        sqlite3_free(cur->pVtab->zErrMsg);
        cur->pVtab->zErrMsg = sqlite3_mprintf("Could not prepare SQL for aggregate db: %s", sqlite3_errmsg(pCur->db));
        return SQLITE_ERROR;
    }

    pCur->rowid = 0;
    pCur->res = sqlite3_step(pCur->stmt); /* go to first result */

    return SQLITE_OK;
}

static int gufivtNext(sqlite3_vtab_cursor *cur) {
    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;
    pCur->res = sqlite3_step(pCur->stmt);
    if ((pCur->res != SQLITE_ROW) && (pCur->res != SQLITE_DONE)) {
        return SQLITE_ERROR;
    }
    if (pCur->res == SQLITE_ROW) {
        pCur->rowid++;
    }
    return SQLITE_OK;
}

static int gufivtEof(sqlite3_vtab_cursor *cur) {
    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;
    const int eof = (pCur->res != SQLITE_ROW);
    if (eof) {
        sqlite3_reset(pCur->stmt);
        pCur->res = SQLITE_DONE;
    }
    return eof;
}

static int gufivtColumn(sqlite3_vtab_cursor *cur,
                        sqlite3_context *ctx,
                        int i) {
    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;
    switch (sqlite3_column_type(pCur->stmt, i)) {
        case SQLITE_INTEGER:
            {
                const int col = sqlite3_column_int(pCur->stmt, i);
                sqlite3_result_int(ctx, col);
            }
            break;
        case SQLITE_FLOAT:
            {
                const double col = sqlite3_column_int(pCur->stmt, i);
                sqlite3_result_double(ctx, col);
            }
            break;
        case SQLITE_TEXT:
            {
                const char *col = (char *) sqlite3_column_text(pCur->stmt, i);
                const int bytes = sqlite3_column_bytes(pCur->stmt, i);
                sqlite3_result_text(ctx, col, bytes, SQLITE_TRANSIENT);
            }
            break;
        case SQLITE_BLOB:
            {
                const void *col = sqlite3_column_blob(pCur->stmt, i);
                const int bytes = sqlite3_column_bytes(pCur->stmt, i);
                sqlite3_result_blob(ctx, col, bytes, SQLITE_TRANSIENT);
            }
            break;
        case SQLITE_NULL:
        default:
            sqlite3_result_null(ctx);
            break;
    }

    return SQLITE_OK;
}

static int gufivtRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid) {
    gufi_vtab_cursor *pCur = (gufi_vtab_cursor *) cur;
    *pRowid = pCur->rowid;
    return SQLITE_OK;
}

/* fill in xCreate and xConnect with the same function */
static const sqlite3_module gufivtModule = {
    0,                         /* iVersion */
    0,                         /* xCreate */
    0,                         /* xConnect */
    gufivtBestIndex,           /* xBestIndex */
    gufivtDisconnect,          /* xDisconnect */
    gufivtDisconnect,          /* xDestroy */
    gufivtOpen,                /* xOpen - open a cursor */
    gufivtClose,               /* xClose - close a cursor */
    gufivtFilter,              /* xFilter - configure scan constraints */
    gufivtNext,                /* xNext - advance a cursor */
    gufivtEof,                 /* xEof - check for end of scan */
    gufivtColumn,              /* xColumn - read data */
    gufivtRowid,               /* xRowid - read data */
    0,                         /* xUpdate */
    0,                         /* xBegin */
    0,                         /* xSync */
    0,                         /* xCommit */
    0,                         /* xRollback */
    0,                         /* xFindMethod */
    0,                         /* xRename */
    0,                         /* xSavepoint */
    0,                         /* xRelease */
    0,                         /* xRollbackTo */
    0,                         /* xShadowName */
    /* 0                          /\* xIntegrity *\/ */
};

#define create_module(module_name, constructor)                     \
    {                                                               \
        static sqlite3_module module;                               \
        memcpy(&module, &gufivtModule, sizeof(gufivtModule));       \
        module.xCreate  = constructor;                              \
        module.xConnect = constructor;                              \
        rc = sqlite3_create_module(db, module_name, &module, 0);    \
        if (rc != SQLITE_OK) {                                      \
            return rc;                                              \
        }                                                           \
    }

int sqlite3_gufivt_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi) {
    (void) pzErrMsg;

    SQLITE_EXTENSION_INIT2(pApi);
    int rc = SQLITE_OK;

    create_module("gufi_vt_treesummary", gufivtTConnect);
    create_module("gufi_vt_summary",     gufivtSConnect);
    create_module("gufi_vt_entries",     gufivtEConnect);
    create_module("gufi_vt_pentries",    gufivtPConnect);
    create_module("gufi_vt_vrsummary",   gufivtVRSConnect);
    create_module("gufi_vt_vrpentries",  gufivtVRPConnect);

    return SQLITE_OK;
}
