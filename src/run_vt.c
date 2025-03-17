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

/*
 * Run Virtual Table
 *
 * This is intended for power users to be able to run arbitrary
 * commands and generate virtual tables with aribtrary result
 * columns. In order to achieve this, users MUST run "CREATE VIRTUAL
 * TABLE" to create the virtual table before querying it. The virtual
 * table should preferrably be created in the temp namespace:
 *
 *     CREATE VIRTUAL TABLE temp.output
 *     USING run_vt(cmd='find -name "*.c" -printf "%f %s\n"', cols='name, size');
 *
 * All arguments should be key=value pairs. The allowed keys are:
 *     cmd     - the command string
 *     cols    - a comma delimited list of column names (no types)
 *     d or D  - column delimiter (default: space)
 *
 * Notes:
 *     - Arguments may appear in any order.
 *     - If there are duplicate arguments, the right-most duplicate will be used
 *
 * Then, SELECT from the virtual table
 *
 *    SELECT * FROM output;
 */

typedef struct refstr {
    const char *data;
    size_t len;
} refstr_t;

typedef struct run_args {
    refstr_t cmd;
    const char *delim;
    size_t col_count;
} run_args_t;

typedef struct run_vtab {
    sqlite3_vtab base;
    run_args_t args;
} run_vtab;

typedef struct run_vtab_cursor {
    sqlite3_vtab_cursor base;

    FILE *output;              /* result of popen */
    char *row;                 /* current row */
    size_t row_alloc;          /* space allocated for row */
    size_t len;                /* length of current row */
    refstr_t *cols;

    sqlite_int64 rowid;        /* current row id */
} run_vtab_cursor;

/* run provided command in provided shell */
static int run(run_args_t *args,
               FILE **output, char **errmsg) {
    /* pass command to popen */
    FILE *out = popen(args->cmd.data, "r");
    if (!out) {
        const int err = errno;
        *errmsg = sqlite3_mprintf("popen failed: %s (%d)", strerror(err), err);
        return SQLITE_ERROR;
    }

    *output = out;

    return SQLITE_OK;
}

static int run_read_row(run_vtab_cursor *pCur) {
    run_vtab *pVtab = (run_vtab *) pCur->base.pVtab;

    const ssize_t len = getline(&pCur->row, &pCur->row_alloc, pCur->output);
    if (len < 1) {
        return 1;
    }

    pCur->row[len - 1] = '\0';
    pCur->len = len - 1; /* remove newline */
    if (pCur->row[pCur->len - 1] == '\r') {
        pCur->row[len - 1] = '\0';
        pCur->len--;     /* remove carriage return */
    }

    /* using col_count to not scan row to get column count */
    pCur->cols = calloc(pVtab->args.col_count, sizeof(refstr_t));

    char *saveptr = NULL;
    char *ptr = strtok_r(pCur->row, pVtab->args.delim, &saveptr);
    pCur->cols[0].data = ptr;
    pCur->cols[0].len  = strlen(ptr);

    for(size_t c = 1; c < pVtab->args.col_count; c++) {
        ptr = strtok_r(NULL, pVtab->args.delim, &saveptr);
        if (!ptr) {
            return 1;
        }
        pCur->cols[c].data = ptr;
        pCur->cols[c].len  = strlen(ptr);
    }

    return 0;
}

/* remove ', ", and leading/trailing spaces */
static void cleanup_arg(refstr_t *value) {
    if (!value->data || !value->len) {
        return;
    }

    if ((value->data[0] == '\'') ||
        (value->data[0] == '"')) {
        value->data++;
        value->len--;
    }

    while (value->data[0] == ' ') {
        value->data++;
        value->len--;
    }

    if ((value->data[value->len - 1] == '\'') ||
        (value->data[value->len - 1] == '"')) {
        value->len--;
        ((char *) (value->data))[value->len] = '\0';
    }

    while (value->data[value->len - 1] == ' ') {
        value->len--;
        ((char *) (value->data))[value->len] = '\0';
    }
}

static int run_vtConnect(sqlite3 *db,
                         void *pAux,
                         int argc, const char * const *argv,
                         sqlite3_vtab **ppVtab,
                         char **pzErr) {
    (void) pAux;
    static const char DEFAULT_DELIM[] = " ";

    run_args_t args;
    memset(&args, 0, sizeof(args));
    args.delim = DEFAULT_DELIM;

    refstr_t cols;
    memset(&cols, 0, sizeof(cols));

    /* parse args */
    for(int i = 3; i < argc; i++) {
        char *saveptr = NULL;
        char *key   = strtok_r((char *) argv[i], "=", &saveptr);
        char *value = strtok_r(NULL, "=", &saveptr);

        /* assume this is an index path */
        if (!value) {
            *pzErr = sqlite3_mprintf("Input arg missing '=': %s", argv[i]);
            return SQLITE_CONSTRAINT;
        }

        const size_t len = strlen(key);

        if (len == 1) {
            if ((*key == 'd') || (*key == 'D')) {
                args.delim = value;
            }
        }
        else if (len == 3) {
            if (strncmp(key, "cmd", 4) == 0) {
                args.cmd.data = value;
                args.cmd.len = strlen(value);

                cleanup_arg(&args.cmd);
            }
        }
        else if (len == 4) {
            if (strncmp(key, "cols", 5) == 0) {
                cols.data = value;
                cols.len = strlen(value);

                cleanup_arg(&cols);
            }
        }
        else {
            *pzErr = sqlite3_mprintf("Unknown input arg: %s", argv[i]);
            return SQLITE_CONSTRAINT;
        }
    }

    if (!args.cmd.data || !args.cmd.len) {
        *pzErr = sqlite3_mprintf("Missing command to run");
        return SQLITE_CONSTRAINT;
    }

    if (!cols.data || !cols.len) {
        *pzErr = sqlite3_mprintf("Missing output columns");
        return SQLITE_CONSTRAINT;
    }

    /* count number of columns (not messing with db) */
    args.col_count = 0;
    for(size_t i = 0; i < cols.len; i++) {
        args.col_count += (cols.data[i] == ',');
    }
    if (cols.data[cols.len - 1] != ',') {
        args.col_count++;
    }

    /* create schema from args.cols */
    char schema[4096];
    if (snprintf(schema, sizeof(schema),
                 "CREATE TABLE X(%.*s);", /* SQL injection should fail due to columns being processed by sqlite3_declare_vtab */
                 (int) cols.len, cols.data) >= (ssize_t) sizeof(schema)) {
        *pzErr = sqlite3_mprintf("Cols argument too long: %s", cols);
        return SQLITE_CONSTRAINT;
    }

    /* set up the virtual table */
    run_vtab *pNew = NULL;
    const int rc = sqlite3_declare_vtab(db, schema);
    if(rc == SQLITE_OK){
        pNew = (run_vtab *)sqlite3_malloc( sizeof(*pNew) );
        if( pNew==0 ) return SQLITE_NOMEM;
        memset(pNew, 0, sizeof(*pNew));
        pNew->args = args;
        /* sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY); */
    }

    *ppVtab = &pNew->base;
    return rc;
}

/* different address to get real virtual table */
static int run_vtCreate(sqlite3 *db,
                        void *pAux,
                        int argc, const char * const *argv,
                        sqlite3_vtab **ppVtab,
                        char **pzErr) {
    return run_vtConnect(db, pAux, argc, argv,
                         ppVtab, pzErr);
}
/* *********************************************************************** */

/* FIXME: This is probably not correct */
static int run_vtBestIndex(sqlite3_vtab *tab,
                           sqlite3_index_info *pIdxInfo) {
    (void ) tab;
    pIdxInfo->estimatedCost = 1000000;
    return SQLITE_OK;
}

static int run_vtDisconnect(sqlite3_vtab *pVtab) {
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int run_vtOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor) {
    (void) p;
    run_vtab_cursor *pCur = sqlite3_malloc( sizeof(*pCur) );
    if( pCur==0 ) return SQLITE_NOMEM;
    memset(pCur, 0, sizeof(*pCur));
    *ppCursor = &pCur->base;
    return SQLITE_OK;
}

static int run_vtClose(sqlite3_vtab_cursor *cur) {
    run_vtab_cursor *pCur = (run_vtab_cursor *) cur;
    free(pCur->cols);
    pCur->cols = NULL;
    free(pCur->row);
    pCur->row = NULL;
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int run_vtFilter(sqlite3_vtab_cursor *cur,
                        int idxNum, const char *idxStr,
                        int argc, sqlite3_value **argv) {
    (void) idxNum; (void) idxStr;
    (void) argc;   (void) argv;

    run_vtab_cursor *pCur = (run_vtab_cursor *) cur;
    run_vtab *vtab = (run_vtab *) cur->pVtab;

    /* run command */
    if (run(&vtab->args, &pCur->output, &vtab->base.zErrMsg) != SQLITE_OK) {
        return SQLITE_ERROR;
    }

    pCur->rowid = 0;
    pCur->row = NULL;
    pCur->row_alloc = 0;
    pCur->len = 0;
    pCur->cols = NULL;

    /* wait for first row */
    run_read_row(pCur);

    return SQLITE_OK;
}

static int run_vtNext(sqlite3_vtab_cursor *cur) {
    run_vtab_cursor *pCur = (run_vtab_cursor *) cur;

    free(pCur->row);
    pCur->row = NULL;
    pCur->row_alloc = 0;
    pCur->len = 0;
    free(pCur->cols);
    pCur->cols = NULL;

    /* no more to read or error */
    if (run_read_row(pCur) != 0) {
        return SQLITE_OK;
    }

    pCur->rowid++;

    return SQLITE_OK;
}

static int run_vtEof(sqlite3_vtab_cursor *cur) {
    run_vtab_cursor *pCur = (run_vtab_cursor *) cur;

    const int eof = (pCur->len < 1);
    if (eof) {
        pclose(pCur->output);
        pCur->output = NULL;
    }

    return eof;
}

static int run_vtColumn(sqlite3_vtab_cursor *cur,
                        sqlite3_context *ctx,
                        int N) {
    run_vtab_cursor *pCur = (run_vtab_cursor *) cur;

    /* don't know types, so everything is initially TEXT */
    refstr_t *col = &pCur->cols[N];
    sqlite3_result_text(ctx, col->data, col->len, SQLITE_TRANSIENT);

    return SQLITE_OK;
}

static int run_vtRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid) {
    run_vtab_cursor *pCur = (run_vtab_cursor *) cur;
    *pRowid = pCur->rowid;
    return SQLITE_OK;
}

static const sqlite3_module run_vtModule = {
    0,                         /* iVersion */
    0,                         /* xCreate */
    0,                         /* xConnect */
    run_vtBestIndex,           /* xBestIndex */
    0,                         /* xDisconnect */
    0,                         /* xDestroy */
    run_vtOpen,                /* xOpen - open a cursor */
    run_vtClose,               /* xClose - close a cursor */
    run_vtFilter,              /* xFilter - configure scan constraints */
    run_vtNext,                /* xNext - advance a cursor */
    run_vtEof,                 /* xEof - check for end of scan */
    run_vtColumn,              /* xColumn - read data */
    run_vtRowid,               /* xRowid - read data */
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

/* no underscore between run and vt for entry point */
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_runvt_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi) {
    (void) pzErrMsg;

    SQLITE_EXTENSION_INIT2(pApi);

    static sqlite3_module module;
    memcpy(&module, &run_vtModule, sizeof(run_vtModule));
    module.xCreate = run_vtCreate;
    module.xConnect = run_vtConnect;
    module.xDisconnect = run_vtDisconnect;
    module.xDestroy = run_vtDisconnect;

    return sqlite3_create_module(db, "run_vt", &module, 0);
}
