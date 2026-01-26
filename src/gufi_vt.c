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
#include <unistd.h>

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "SinglyLinkedList.h"
#include "addqueryfuncs.h"
#include "bf.h"
#include "dbutils.h"
#include "popen_argv.h"
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
 *     SELECT * FROM gufi_vt_pentries('index'[, ...]);
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

typedef struct gufi_query_cmd {
    refstr_t remote_cmd;   /* command to send this run to a remote host (i.e. ssh); prefixes gufi_query command */
    sll_t remote_args;     /* list of refstr_t that are single arguments to remote_cmd (i.e. user@remote) */
    int verbose;           /* print gufi_query command */

    refstr_t threads;      /* number of threads in string form to avoid converting back and forth */
    refstr_t min_level;    /* defaults to 0; set to non-0 if index root should be used with path list */
    refstr_t max_level;    /* defaults to (uint64_t) -1 */
    refstr_t dir_match_uid;
    refstr_t dir_match_gid;

    /* sql */

    /*
     * SQL to set up temporary single table that does not exist in
     * default GUFI to allow for result column types to be discovered
     */
    refstr_t setup_res_col_types;

    refstr_t I;
    refstr_t T;
    refstr_t S;
    refstr_t E;
    refstr_t K;
    refstr_t J;
    refstr_t G;
    refstr_t F;

    refstr_t path_list;    /* list of paths to process; if min-level is 0, these should be full paths/relative to pwd */
    refstr_t p;            /* source path */
    refstr_t plugin;       /* gufi_query plugin library path */

    sll_t indexroots;      /* list of index roots to pass to gufi_query */
} gq_cmd_t;

static void gq_cmd_init(gq_cmd_t *cmd) {
    memset(cmd, 0, sizeof(*cmd));
    sll_init(&cmd->remote_args);
    sll_init(&cmd->indexroots);
}

static void gq_cmd_destroy(gq_cmd_t *cmd) {
    sll_destroy(&cmd->indexroots, NULL);  /* list of references to argv[i] */
    sll_destroy(&cmd->remote_args, free); /* list of allocated refstr_t */
    /* not freeing cmd here */
}

typedef struct gufi_vtab {
    sqlite3_vtab base;
    gq_cmd_t cmd;
    int fixed_schema;      /* fixed schema (gufi_vt_*) or not (gufi_vt)? */
} gufi_vtab;

typedef struct gufi_vtab_cursor {
    sqlite3_vtab_cursor base;

    popen_argv_t *output;
    char *row;             /* current row */
    size_t len;            /* length of current row */
    size_t *col_starts;
    int col_count;

    sqlite_int64 rowid;    /* current row id */
} gufi_vtab_cursor;

static const char ONE_THREAD[] = "1";

/*
 * run gufi_query
 *
 * have to fork+exec - cannot link gufi_query in without changing
 * everything to link dynamically
 */
static int gufi_query(const gq_cmd_t *cmd, popen_argv_t **output, char **errmsg) {
    size_t max_argc = 0;
    int argc = 0;
    const char **argv = NULL;
    char *flat = NULL; /* flattened command string that needs to be freed */
    char *dir_match_uid = NULL;
    char *dir_match_gid = NULL;

    if (cmd->remote_cmd.len) {
        /* need to combine entire gufi_query command into one argument */
        max_argc = 1 + sll_get_size(&cmd->remote_args) + 1;

        argv = calloc(max_argc + 1, sizeof(cmd[0]));

        argv[argc++] = cmd->remote_cmd.data;
        sll_loop(&cmd->remote_args, node) {
            refstr_t *remote_arg = (refstr_t *) sll_node_data(node);
            argv[argc++] = remote_arg->data;
        }

        size_t size = 0;
        size_t len = 0;

        write_with_resize(&flat, &size, &len,
                          "gufi_query ");
        write_with_resize(&flat, &size, &len,
                          "--print-tlv ");
        write_with_resize(&flat, &size, &len,
                          "-x ");
        write_with_resize(&flat, &size, &len,
                          "--threads %s ", cmd->threads.data);

        /* flatten the entire gufi_query command into a single string */
        #define flatten_argv(argc, argv, flag, refstr)               \
            if (refstr.len) {                                        \
                write_with_resize(&flat, &size, &len,                \
                                  "%s \"%s\" ", flag, refstr.data);  \
            }

        /* construct the rest of the gufi_query command */
        flatten_argv(argc, argv, "--min-level", cmd->min_level);
        flatten_argv(argc, argv, "--max-level", cmd->max_level);
        flatten_argv(argc, argv, "--path-list", cmd->path_list);

        if (cmd->dir_match_uid.len) {
            write_with_resize(&flat, &size, &len,
                              "--dir-match-uid=%s ", cmd->dir_match_uid.data);
        }

        if (cmd->dir_match_gid.len) {
            write_with_resize(&flat, &size, &len,
                              "--dir-match-gid=%s ", cmd->dir_match_gid.data);
        }

        flatten_argv(argc, argv, "-I",          cmd->I);
        flatten_argv(argc, argv, "-T",          cmd->T);
        flatten_argv(argc, argv, "-S",          cmd->S);
        flatten_argv(argc, argv, "-E",          cmd->E);
        flatten_argv(argc, argv, "-K",          cmd->K);
        flatten_argv(argc, argv, "-J",          cmd->J);
        flatten_argv(argc, argv, "-G",          cmd->G);
        flatten_argv(argc, argv, "-F",          cmd->F);
        flatten_argv(argc, argv, "-p",          cmd->p);
        flatten_argv(argc, argv, "--plugin",    cmd->plugin);

        sll_loop(&cmd->indexroots, node) {
            const char *indexroot = (char *) sll_node_data(node);
            write_with_resize(&flat, &size, &len,
                              "%s ", indexroot);
        }

        argv[argc++] = flat;
    }
    else {
        /* can keep arguments separate */

        max_argc = 35; /* 5 fixed args, 14 pairs of flags, 2 single argv flags */
        max_argc += sll_get_size(&cmd->indexroots);

        argv = calloc(max_argc + 1, sizeof(argv[0]));

        argv[argc++] = "gufi_query";
        argv[argc++] = "--print-tlv";
        argv[argc++] = "-x";

        /* keep this immediately before other key-value flags to make printing easier */
        argv[argc++] ="--threads";
        argv[argc++] = cmd->threads.data; /* always set */

        #define set_argv(argc, argv, flag, refstr)  \
            if (refstr.len) {                       \
                argv[argc++] = flag;                \
                argv[argc++] = refstr.data;         \
            }

        /* construct the rest of the gufi_query command */
        set_argv(argc, argv, "--min-level", cmd->min_level);
        set_argv(argc, argv, "--max-level", cmd->max_level);
        set_argv(argc, argv, "--path-list", cmd->path_list);

        if (cmd->dir_match_uid.len) {
            size_t size = 0;
            size_t len = 0;
            write_with_resize(&dir_match_uid, &size, &len,
                              "--dir-match-uid=%s", cmd->dir_match_uid.data);
            argv[argc++] = dir_match_uid;
        }

        if (cmd->dir_match_gid.len) {
            size_t size = 0;
            size_t len = 0;
            write_with_resize(&dir_match_gid, &size, &len,
                              "--dir-match-gid=%s", cmd->dir_match_gid.data);
            argv[argc++] = dir_match_gid;
        }

        set_argv(argc, argv, "--setup-res-col-type",  cmd->setup_res_col_types);
        set_argv(argc, argv, "-I",                    cmd->I);
        set_argv(argc, argv, "-T",                    cmd->T);
        set_argv(argc, argv, "-S",                    cmd->S);
        set_argv(argc, argv, "-E",                    cmd->E);
        set_argv(argc, argv, "-K",                    cmd->K);
        set_argv(argc, argv, "-J",                    cmd->J);
        set_argv(argc, argv, "-G",                    cmd->G);
        set_argv(argc, argv, "-F",                    cmd->F);
        set_argv(argc, argv, "-p",                    cmd->p);
        set_argv(argc, argv, "--plugin",              cmd->plugin);

        sll_loop(&cmd->indexroots, node) {
            const char *indexroot = (char *) sll_node_data(node);
            argv[argc++] = indexroot;
        }
    }

    if (cmd->verbose) {
        fprintf(stderr, "Each line is one argv[i]:\n");

        argc--; /* argc is not used after outside of here */

        fprintf(stderr, "    %s \\\n", argv[0]);
        for(int i = 1; i < argc; i++) {
            fprintf(stderr, "        %s \\\n", argv[i]);
        }
        fprintf(stderr, "        %s\n", argv[argc]);
    }

    /* pass command to popen */
    popen_argv_t *out = popen_argv(argv);

    free(dir_match_gid);
    free(dir_match_uid);
    free(flat);
    free(argv);

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

    const int fd = popen_argv_fd(pCur->output);

    /* row length */
    if (read_size(fd, &row_len, sizeof(row_len)) != sizeof(row_len)) {
        const int err = errno;
        pCur->base.pVtab->zErrMsg = sqlite3_mprintf("Error: Could not read row length: %s (%d)",
                                                    strerror(err), err);
        goto error;
    }

    /* column count */
    if (read_size(fd, &count, sizeof(count)) != sizeof(count)) {
        const int err = errno;
        pCur->base.pVtab->zErrMsg = sqlite3_mprintf("Error: Could not read column count: %s (%d)",
                                                    strerror(err), err);
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
        const size_t tl = read_size(fd, curr, TL);
        if (tl != TL) {
            const int err = errno;
            pCur->base.pVtab->zErrMsg = sqlite3_mprintf("Error: Could not read type and length from column %d: %s (%d)",
                                                        i, strerror(err), err);
            goto error;
        }

        const size_t value_len = * (size_t *) (curr + sizeof(char));

        curr += TL;   /* to go to start of value */

        const size_t v = read_size(fd, curr, value_len);
        if (v != value_len) {
            const int err = errno;
            pCur->base.pVtab->zErrMsg = sqlite3_mprintf("Error: Could not read %zu octets. Got %zu: %s (%d)",
                                                        value_len, v, strerror(err), err);
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
static void set_refstr(refstr_t *refstr, char *value) {
    if (value) {
        /* don't wipe old value if new value is NULL */
        memset(refstr, 0, sizeof(*refstr));

        while ((value[0] == '\'') ||
               (value[0] == '"')) {
            value++;
        }

        refstr->data = value;
        refstr->len = strlen(value);

        while (refstr->len &&
               ((value[refstr->len - 1] == '\'') ||
                (value[refstr->len - 1] == '"'))) {
            value[--refstr->len] = '\0';
        }

        if (refstr->len == 0) {
            refstr->data = NULL;
        }
    }
}

/* generic connect function */
static int gufi_vtConnect(sqlite3 *db, void *pAux,
                          int argc, const char *const*argv,
                          sqlite3_vtab **ppVtab,
                          char **pzErr,
                          const char *schema,
                          const gq_cmd_t *cmd,  /* reference to struct of pointers */
                          const int fixed_schema) {
    (void) pAux; (void) pzErr;
    (void) argc; (void) argv;

    gufi_vtab *pNew = NULL;
    const int rc = sqlite3_declare_vtab(db, schema);
    if(rc == SQLITE_OK){
        pNew = (gufi_vtab *)sqlite3_malloc( sizeof(*pNew) );
        if( pNew==0 ) return SQLITE_NOMEM;
        memset(pNew, 0, sizeof(*pNew));
        pNew->cmd = *cmd; /* ownership is transferred */
        pNew->fixed_schema = fixed_schema;
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
 *     FROM gufi_vt_*('<indexroot>'[, threads, min_level, max_level, path_list, verbose, remote_cmd, remote_args])
 *     ... ;
 *
 * The following are positional arguments to each virtual table:
 *
 *     indexroots        (string; space separated paths)
 *     # of threads      (positive integer)
 *     min_level         (non-negative integer)
 *     max_level         (non-negative integer)
 *     path_list         (file path string)
 *     verbose           (0 or 1)
 *     remote_cmd        (string)
 *     remote_args       (string; space separated args)
 *
 * The index root argument is required. All of the others are
 * optional. The remote args shoud be passed in one at a time, not in
 * one string/arg i.e. 'ssh user@remote -p 2222' should be passed in
 * as gufi_vt_*(..., ssh, user@remote -p 2222). Note that the
 * remote_args will be split on space, so be careful when passing in
 * remote_args that contain space that are not arg separators.
 *
 * GUFI user defined functions (UDFs) that do not require gufi_query
 * state may be called. UDFs requiring gufi_query state (path(),
 * epath(), fpath(), and rpath()) can be accessed from the virtual
 * table by using columns with the same names.
 *
 * The schemas of all 6 of the corresponding tables and views are
 * recreated here, and thus all columns are accessible.
 */

/* positional arguments */
#define GUFI_VT_ARGS_INDEXROOT       0
#define GUFI_VT_ARGS_THREADS         1
#define GUFI_VT_ARGS_MIN_LEVEL       2
#define GUFI_VT_ARGS_MAX_LEVEL       3
#define GUFI_VT_ARGS_PATH_LIST       4
#define GUFI_VT_ARGS_VERBOSE         5
#define GUFI_VT_ARGS_REMOTE_CMD      6
#define GUFI_VT_ARGS_REMOTE_ARGS     7
#define GUFI_VT_ARGS_COUNT           8

#define GUFI_VT_ARG_COLUMNS          "indexroot TEXT HIDDEN,   " \
                                     "threads INT64 HIDDEN,    " \
                                     "min_level INT64 HIDDEN,  " \
                                     "max_level INT64 HIDDEN,  " \
                                     "path_list TEXT HIDDEN,   " \
                                     "verbose BOOLEAN HIDDEN,  " \
                                     "remote_cmd TEXT HIDDEN,  " \
                                     "remote_args TEXT HIDDEN, "

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

#define SELECT_FROM(name)                                                         \
    "SELECT " GUFI_VT_EXTRA_COLUMNS_SQL "* "                                      \
    "FROM " name ";"                                                              \

#define SELECT_FROM_VR(name)                                                      \
    "SELECT " GUFI_VT_EXTRA_COLUMNS_SQL_VR "* "                                   \
    "FROM " name ";"                                                              \

/* generate xConnect function for each virtual table */
#define gufi_vt_xConnect(name, abbrev, t, s, e, vr)                               \
    static int gufi_vt_ ##abbrev ##Connect(sqlite3 *db,                           \
                                           void *pAux,                            \
                                           int argc, const char * const *argv,    \
                                           sqlite3_vtab **ppVtab,                 \
                                           char **pzErr) {                        \
        /* this is what the virtual table looks like */                           \
        /* the schema is fixed for this function */                               \
        const char schema[] =                                                     \
            name ##_SCHEMA(name, GUFI_VT_ALL_COLUMNS);                            \
                                                                                  \
        static const char select_from[]    = SELECT_FROM(name);                   \
        static const char select_from_vr[] = SELECT_FROM_VR(name);                \
                                                                                  \
        /* fixed queries */                                                       \
        gq_cmd_t cmd;                                                             \
        gq_cmd_init(&cmd);                                                        \
        set_refstr(&cmd.T, (char *) (t?select_from:NULL));                        \
        set_refstr(&cmd.S, (char *) (s?(vr?select_from_vr:select_from):NULL));    \
        set_refstr(&cmd.E, (char *) (e?(vr?select_from_vr:select_from):NULL));    \
                                                                                  \
        return gufi_vtConnect(db, pAux, argc, argv, ppVtab, pzErr,                \
                              schema, &cmd, 1);                                   \
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
 *     USING gufi_vt(threads=2, E='SELECT * FROM pentries;', index=path);
 *
 * Most arguments should be key=value pairs. The allowed keys are:
 *     remote_cmd              = '<command to forward this run of gufi_query to a remote (e.g. ssh)>'
 *     remote_arg              = '<single argv[i] passed to remote_cmd before gufi_query (e.g. user@remote)>'
 *     n or threads            =  <positive integer>
 *     min_level               =  <non-negative integer>
 *     max_level               =  <non-negative integer>
 *     dir_match_uid           =  <uid>
 *     dir_match_gid           =  <gid>
 *     setup_res_col_types     = '<SQL>' (set up single temporary table to make columns available for getting result column types)
 *     I                       = '<SQL>'
 *     T                       = '<SQL>'
 *     S                       = '<SQL>'
 *     E                       = '<SQL>'
 *     K                       = '<SQL>'
 *     J                       = '<SQL>'
 *     G                       = '<SQL>'
 *     F                       = '<SQL>'
 *     path_list               = '<file path>'
 *     p                       = '<source path for use with spath()>'
 *     plugin                  = '<gufi_query plugin library path>'
 *     index                   = '<path>' (can also pass in without the key)
 *     verbose/VERBOSE         =  <0|1>
 *
 * Notes:
 *     - Arguments without '=' are considered index paths
 *     - Arguments may appear in any order.
 *     - String arguments do not have to be quoted, but should be.
 *       String arguments can be quoted with single or double
 *       quotes. Single quotes are recommended in order to make sure
 *       SQLite3 does not interpret them as column names by accident.
 *     - If there are duplicate arguments, the right-most duplicate will be used
 *           - The following are exceptions and all values will be used in the order they appear:
 *                 - remote_arg
 *                 - index
 *     - At least one of T, S, or E must be passed in
 *     - When determining the virtual table's schema, there are two separate cases:
 *           - If NOT aggregating, the query that is processed latest will be used:
 *                 - If T is passed in, but not S or E, it will be used
 *                 - If S is passed in, but not E, it will be used
 *                 - If E is passed in, it will be used
 *           - If aggregating (K is passed in), the G SQL will be used
 *           - If using plugin generated tables/views, aggregation must be used
 *             because those tables/views don't exist when column types are checked
 *     - Most bad values are not checked until the first SELECT is
 *       performed, which is when the arguments are passed to gufi_query
 *     - xattr processing is always enabled
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
    gq_cmd_t cmd;
    gq_cmd_init(&cmd);

    set_refstr(&cmd.threads, (char *) ONE_THREAD);

    /* parse args */
    for(int i = 3; i < argc; i++) {
        char *value = NULL;
        char *key   = strtok_r((char *) argv[i], "=", &value);
        const size_t len = strlen(key);

        /* assume this is an index path */
        if (!value || (value == (key + len))) {
            /* clean up indexroot */
            refstr_t ir;
            set_refstr(&ir, (char *) argv[i]);
            sll_push(&cmd.indexroots, (char *) ir.data);
            continue;
        }

        if (len == 1) {
            switch (*key) {
                case 'n':
                    /* let gufi_query check value */
                    set_refstr(&cmd.threads, value);
                    break;
                case 'I':
                    set_refstr(&cmd.I, value);
                    break;
                case 'T':
                    set_refstr(&cmd.T, value);
                    break;
                case 'S':
                    set_refstr(&cmd.S, value);
                    break;
                case 'E':
                    set_refstr(&cmd.E, value);
                    break;
                case 'K':
                    set_refstr(&cmd.K, value);
                    break;
                case 'J':
                    set_refstr(&cmd.J, value);
                    break;
                case 'G':
                    set_refstr(&cmd.G, value);
                    break;
                case 'F':
                    set_refstr(&cmd.F, value);
                    break;
                case 'p':
                    set_refstr(&cmd.p, value);
                default:
                    break;
            }
        }
        else if (strncmp(key, "index", 6) == 0) {
            /* clean up indexroot */
            refstr_t ir;
            set_refstr(&ir, value);
            sll_push(&cmd.indexroots, (char *) ir.data);
        }
        else if (strncmp(key, "plugin", 7) == 0) {
            set_refstr(&cmd.plugin, value);
        }
        else if (strncmp(key, "threads", 8) == 0) {
            /* let gufi_query check value */
            set_refstr(&cmd.threads, value);
        }
        else if ((strncmp(key, "verbose", 8) == 0) ||
                 (strncmp(key, "VERBOSE", 8) == 0)) {
            if (sscanf(value, "%d", &cmd.verbose) != 1) {
                *pzErr = sqlite3_mprintf("Bad verbose value: %s", value);
                gq_cmd_destroy(&cmd);
                return SQLITE_MISUSE;
            }
        }
        else if (strncmp(key, "min_level", 10) == 0) {
            /* let gufi_query check value */
            set_refstr(&cmd.min_level, value);
        }
        else if (strncmp(key, "max_level", 10) == 0) {
            /* let gufi_query check value */
            set_refstr(&cmd.max_level, value);
        }
        else if (strncmp(key, "path_list", 10) == 0) {
            set_refstr(&cmd.path_list, value);
        }
        else if (strncmp(key, "remote_cmd", 11) == 0) {
            set_refstr(&cmd.remote_cmd, value);
        }
        else if (strncmp(key, "remote_arg", 11) == 0) {
            refstr_t *remote_arg = calloc(1, sizeof(*remote_arg));
            set_refstr(remote_arg, value);
            sll_push(&cmd.remote_args, remote_arg);
        }
        else if (strncmp(key, "dir_match_uid", 14) == 0) {
            /* let gufi_query check value */
            set_refstr(&cmd.dir_match_uid, value);
        }
        else if (strncmp(key, "dir_match_gid", 14) == 0) {
            /* let gufi_query check value */
            set_refstr(&cmd.dir_match_gid, value);
        }
        else if (strncmp(key, "setup_res_col_types", 20) == 0) {
            set_refstr(&cmd.setup_res_col_types, value);
        }
        else {
            *pzErr = sqlite3_mprintf("Unknown key: %s", key);
            gq_cmd_destroy(&cmd);
            return SQLITE_CONSTRAINT;
        }
    }

    if (!sll_get_size(&cmd.indexroots) && (!cmd.path_list.data && !cmd.path_list.len)) {
        *pzErr = sqlite3_mprintf("Missing indexroot or path_list");
        gq_cmd_destroy(&cmd);
        return SQLITE_CONSTRAINT;
    }

    /* get column types of results from gufi_query */

    /* make sure all setup tables are placed into a different db */
    /* this should never fail */
    sqlite3 *tempdb = opendb(SQLITE_MEMORY, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                             0, 0, create_dbdb_tables, NULL);

    create_xattr_tables(SQLITE_MEMORY, tempdb, NULL);

    /* before getting the column types, set up tables that don't exist yet in this temporary space */
    if (cmd.setup_res_col_types.data && cmd.setup_res_col_types.len) {
        char *err = NULL;
        if (sqlite3_exec(tempdb, cmd.setup_res_col_types.data, NULL, NULL, &err) != SQLITE_OK) {
            *pzErr = sqlite3_mprintf("Setting up results table failed: %s", err);
            gq_cmd_destroy(&cmd);
            sqlite3_free(err);
            return SQLITE_CONSTRAINT;
        }
    }

    struct input in;
    in.process_xattrs = 1;
    in.source_prefix = cmd.p;
    struct work work; /* unused */
    memset(&work, 0, sizeof(work));
    size_t count = 0; /* unused */
    setup_xattrs_views(&in, tempdb, &work, &count);

    int cols = 0;
    int *types = NULL;
    char **names = NULL;
    size_t *lens = NULL;

    /* this should never fail */
    aqfctx_t ctx = {
        .in = &in,
        .work = &work,
    };
    addqueryfuncs(tempdb);
    addqueryfuncs_with_context(tempdb, &ctx);

    if (cmd.T.len) {
        /* this should never fail */
        create_table_wrapper(SQLITE_MEMORY, tempdb, TREESUMMARY, TREESUMMARY_CREATE);
    }

    /* if not aggregating, get types for T, S, or E */
    if (!cmd.K.len) {
        int rc = -1; /* get_cols returns 0 or 1, so -1 means SQL was not provided */
        if (cmd.T.len && !cmd.S.len && !cmd.E.len) {
            rc = get_cols(tempdb, &cmd.T, &types, &names, &lens, &cols);
        }
        else if (cmd.S.len && !cmd.E.len) {
            rc = get_cols(tempdb, &cmd.S, &types, &names, &lens, &cols);
        }
        else if (cmd.E.len) {
            rc = get_cols(tempdb, &cmd.E, &types, &names, &lens, &cols);
        }

        if (rc == -1) {
            *pzErr = sqlite3_mprintf("Need at least one of T/S/E when not aggregating");
            goto error;
        }
        else if (rc == 1) {
            *pzErr = sqlite3_mprintf("Could not get column types");
            goto error;
        }
    }
    /* types for G */
    else {
        /* run -K so -G can pull the final columns */
        char *err = NULL;
        if (sqlite3_exec(tempdb, cmd.K.data, NULL, NULL, &err) != SQLITE_OK) {
            *pzErr = sqlite3_mprintf("-K SQL failed while getting columns types: %s", err);
            sqlite3_free(err);
            goto error;
        }
        if (get_cols(tempdb, &cmd.G, &types, &names, &lens, &cols) != 0) {
            *pzErr = sqlite3_mprintf("Failed to get column types of -G");
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

    return gufi_vtConnect(db, pAux, argc, argv, ppVtab, pzErr,
                          schema, &cmd, 0);
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
    gq_cmd_destroy(&cmd);
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
    if (!vtab->fixed_schema) {
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

        /* both index root and path list are not found */
        if ((argc == 0) && !vtab->cmd.path_list.len) {
            return SQLITE_CONSTRAINT;
        }
    }

    return SQLITE_OK;
}

static int gufi_vtDisconnect(sqlite3_vtab *pVtab) {
    gufi_vtab *vtab = (gufi_vtab *) pVtab;
    gq_cmd_destroy(&vtab->cmd);
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

    /* gufi_vt_* */
    if (vtab->fixed_schema) {
        /* at least one index must be present */
        char *indexroots = (char *) sqlite3_value_text(argv[GUFI_VT_ARGS_INDEXROOT]);
        if (!indexroots) {
            vtab->base.zErrMsg = sqlite3_mprintf("Missing index");
            return SQLITE_CONSTRAINT;
        }

        /* push space delimited indexroots */
        {
            char *saveptr = NULL;
            char *indexroot = strtok_r(indexroots, " ", &saveptr); /* skip multiple contiguous spaces */
            while (indexroot) {
                refstr_t ir;
                set_refstr(&ir, indexroot);
                sll_push(&vtab->cmd.indexroots, (char *) ir.data);

                indexroot = strtok_r(NULL, " ", &saveptr);
            }
        }
        /* not checking for empty string/indexroot list; 0 indexes is valid gufi_query input */

        /* default thread count */
        set_refstr(&vtab->cmd.threads, (char *) ONE_THREAD);
        if (argc > GUFI_VT_ARGS_THREADS) {
            /* passing NULL in the SQL will result in a NULL pointer */
            char *threads = NULL;
            if ((threads = (char *) sqlite3_value_text(argv[GUFI_VT_ARGS_THREADS]))) {
                /* let gufi_query check value */
                set_refstr(&vtab->cmd.threads, threads);
            }

            if (argc > GUFI_VT_ARGS_MIN_LEVEL) {
                /* passing NULL in the SQL will result in a NULL pointer */
                char *min_level_str = NULL;
                if ((min_level_str = (char *) sqlite3_value_text(argv[GUFI_VT_ARGS_MIN_LEVEL]))) {
                    /* let gufi_query check value */
                    set_refstr(&vtab->cmd.min_level, min_level_str);
                }

                if (argc > GUFI_VT_ARGS_MAX_LEVEL) {
                    /* passing NULL in the SQL will result in a NULL pointer */
                    char *max_level_str = NULL;
                    if ((max_level_str = (char *) sqlite3_value_text(argv[GUFI_VT_ARGS_MAX_LEVEL]))) {
                        /* let gufi_query check value */
                        set_refstr(&vtab->cmd.max_level, max_level_str);
                    }

                    if (argc > GUFI_VT_ARGS_PATH_LIST) {
                        set_refstr(&vtab->cmd.path_list, (char *) sqlite3_value_text(argv[GUFI_VT_ARGS_PATH_LIST]));

                        if (argc > GUFI_VT_ARGS_VERBOSE) {
                            /* passing NULL in the SQL will result in a NULL pointer */
                            char *verbose_str = NULL;
                            if ((verbose_str = (char *) sqlite3_value_text(argv[GUFI_VT_ARGS_VERBOSE]))) {
                                int verbose = 0;
                                if (sscanf(verbose_str, "%d", &verbose) != 1) {
                                    vtab->base.zErrMsg = sqlite3_mprintf("Bad verbose value: '%s'", verbose_str);
                                    return SQLITE_CONSTRAINT;
                                }

                                vtab->cmd.verbose = verbose;
                            }

                            if (argc > GUFI_VT_ARGS_REMOTE_CMD) {
                                set_refstr(&vtab->cmd.remote_cmd, (char *) sqlite3_value_text(argv[GUFI_VT_ARGS_REMOTE_CMD]));

                                if (argc > GUFI_VT_ARGS_REMOTE_ARGS) {
                                    char *remote_args = (char *) sqlite3_value_text(argv[GUFI_VT_ARGS_REMOTE_ARGS]);
                                    char *saveptr = NULL;
                                    char *remote_arg = strtok_r(remote_args, " ", &saveptr); /* skip multiple contiguous spaces */
                                    while (remote_arg) {
                                        refstr_t *ra = calloc(1, sizeof(*ra));
                                        set_refstr(ra, remote_arg);
                                        sll_push(&vtab->cmd.remote_args, ra);

                                        remote_arg = strtok_r(NULL, " ", &saveptr);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /* kick off gufi_query */
    const int rc = gufi_query(&vtab->cmd, &pCur->output, &vtab->base.zErrMsg);
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
        popen_argv_close(pCur->output);
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

    const char *buf = pCur->row + pCur->col_starts[N - (pVtab->fixed_schema?GUFI_VT_ARGS_COUNT:0)];
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
