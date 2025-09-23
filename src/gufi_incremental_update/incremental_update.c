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
#include <string.h>
#include <unistd.h>

#include "dbutils.h"
#include "debug.h"

#include "gufi_incremental_update/incremental_update.h"

#define ERRNO_NOT_ERR(op, msg, ...)                         \
    if (op != 0) {                                          \
        const int err = errno;                              \
        fprintf(stderr, msg ": %s (%d)\n",                  \
                __VA_ARGS__, strerror(err), err);           \
        return 0; /* do not stop query from continuing */   \
    }

#define INDEX_ATTACH "indexdb"
#define TREE_ATTACH  "treedb"

#define INDEX "gufi" /* can't use 'index' because that is a keyword */
#define TREE  "tree"

#define UNCHANGED_RENAMED_DELETED "urd"
static const char UNCHANGED_RENAMED_DELETED_CREATE[] =
    DROP_TABLE(UNCHANGED_RENAMED_DELETED)
    "CREATE TABLE " UNCHANGED_RENAMED_DELETED " AS "
    /*
     * keep index data + matching tree data to get
     *     unchanged directories
     *     renamed directories
     *     deleted directories (index, but no tree)
     */
    "SELECT "
    "       " INDEX ".path    AS " INDEX "path, "
    "       " INDEX ".type    AS " INDEX "type, "
    "       " INDEX ".inode   AS " INDEX "inode, "
    "       " INDEX ".pinode  AS " INDEX "pinode, "
    "       " INDEX ".depth   AS " INDEX "depth, "
    "       " INDEX ".suspect AS " INDEX "suspect, "
    "       " TREE  ".path    AS " TREE  "path, "
    "       " TREE  ".type    AS " TREE  "type, "
    "       " TREE  ".inode   AS " TREE  "inode, "
    "       " TREE  ".pinode  AS " TREE  "pinode, "
    "       " TREE  ".depth   AS " TREE  "depth, "
    "       " TREE  ".suspect AS " TREE  "suspect "
    "FROM " INDEX_ATTACH "." READDIRPLUS " AS " INDEX " "
    "LEFT OUTER JOIN " TREE_ATTACH "." READDIRPLUS " AS " TREE " ON " INDEX ".inode == " TREE ".inode "
    ";";

#define CREATED "created"
const char CREATED_CREATE[] =
    DROP_TABLE(CREATED)
    "CREATE TABLE " CREATED " AS "
    /*
     * keep tree data + matching index data to get
     *     create directories (tree, but no index)
     *
     * have to do LEFT JOIN with flipped tables because
     * sqlite3 does not have a RIGHT JOIN
     */
    "SELECT "
    "       " INDEX ".path    AS " INDEX "path, "
    "       " INDEX ".type    AS " INDEX "type, "
    "       " INDEX ".inode   AS " INDEX "inode, "
    "       " INDEX ".pinode  AS " INDEX "pinode, "
    "       " INDEX ".depth   AS " INDEX "depth, "
    "       " INDEX ".suspect AS " INDEX "suspect, "
    "       " TREE  ".path    AS " TREE  "path, "
    "       " TREE  ".type    AS " TREE  "type, "
    "       " TREE  ".inode   AS " TREE  "inode, "
    "       " TREE  ".pinode  AS " TREE  "pinode, "
    "       " TREE  ".depth   AS " TREE  "depth, "
    "       " TREE  ".suspect AS " TREE  "suspect "
    "FROM " TREE_ATTACH "." READDIRPLUS " AS " TREE " "
    "LEFT OUTER JOIN " INDEX_ATTACH "." READDIRPLUS " AS " INDEX " ON " TREE ".inode == " INDEX ".inode "
    "WHERE " INDEX ".inode IS NULL " /* remove all unchanged/renames, leaving only new directories */
    ";";

#define ALL_MATCHES "jt"
static const char ALL_MATCHES_CREATE[] =
    "CREATE TEMPORARY VIEW " ALL_MATCHES " AS "
    "SELECT * FROM " UNCHANGED_RENAMED_DELETED "." UNCHANGED_RENAMED_DELETED " "
    "UNION ALL "
    "SELECT * FROM " CREATED "." CREATED " "
    ";";

#define INCR_DIFF "diff"
static const char INCR_DIFF_CREATE[] =
    DROP_TABLE(INCR_DIFF)
    "CREATE TABLE " INCR_DIFF " AS "
    "SELECT "
    "       * "
    "FROM " ALL_MATCHES " "
    /* only keep changes */
    "WHERE (" INDEX "pinode != " TREE "pinode) "
    "  OR  (" INDEX "path   != " TREE "path) "
    "  OR  (" INDEX "inode IS NULL) "
    "  OR  (" TREE  "inode IS NULL) "
    "  OR  (" TREE  "suspect == 1) "
    ";";

/* find directories that were deleted or renamed */
static const char GET_DELETES_AND_MOVE_OUTS[] =
    "SELECT "
    "       " INDEX "path, "
    /* "       " TREE  "path, " */
    "       " TREE  "inode, "
    "       CASE WHEN " TREE "path IS NULL THEN 1 ELSE 0 END "
    "FROM " INCR_DIFF " "
    "WHERE (" TREE  "path IS NULL)"
    "  OR  (" INDEX "pinode != " TREE "pinode) "
    "  OR  (" INDEX "path   != " TREE "path) "
    "ORDER BY " INDEX "depth DESC " /* go up tree to avoid deleting moved directories from a parent */
    ";";

/* find directories that were created or renamed */
static const char GET_CREATES_AND_MOVE_INS[] =
    "SELECT "
    /* "       " INDEX "path, " */
    "       " TREE  "path, "
    "       " TREE  "inode, "
    "       CASE WHEN " INDEX "path IS NULL THEN 1 ELSE 0 END "
    "FROM " INCR_DIFF " "
    "WHERE (" INDEX "path IS NULL) "
    "  OR  (" INDEX "pinode != " TREE "pinode) "
    "  OR  (" INDEX "path   != " TREE "path) "
    "ORDER BY " TREE "depth ASC " /* go down tree to avoid creating new directories under non-existent parents */
    ";";

/* find directories that were changed */
static const char GET_UPDATES[] =
    "SELECT "
    /* "       " INDEX "path, " */
    "       " TREE  "path, "
    /* "       CASE WHEN " INDEX "path == " TREE "path THEN 0 ELSE 1 END, " */
    "       " TREE  "inode "
    /* "       " TREE  "pinode, " */
    /* "       CASE WHEN " INDEX "pinode == " TREE "pinode THEN 0 ELSE 1 END " */
    "FROM " INCR_DIFF " "
    "WHERE " TREE "path IS NOT NULL "
    /* "ORDER BY " TREE "depth ASC " */
    ";";

#define URD_DB_EXT "urd"

static size_t gen_urd_name(struct PoolArgs *pa, char *name, const size_t name_size) {
    return SNFORMAT_S(name, name_size, 3,
                      pa->in.outname.data, pa->in.outname.len,
                      ".", (size_t) 1,
                      URD_DB_EXT, sizeof(URD_DB_EXT) - 1);
}

static int get_urd(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    (void) ctx; (void) id; (void) data;

    struct PoolArgs *pa = (struct PoolArgs *) args;

    char dbname[MAXPATH];
    gen_urd_name(pa, dbname, sizeof(dbname));

    sqlite3 *db = opendb(dbname, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, 0, 0, NULL, NULL);
    if (!db) {
        return 1;
    }

    /* ground truth */
    char index_snapshot_name[MAXPATH];
    gen_index_snapshot_name(pa, index_snapshot_name, sizeof(index_snapshot_name));

    /* tree with changes */
    char tree_snapshot_name[MAXPATH];
    gen_tree_snapshot_name(pa, tree_snapshot_name, sizeof(tree_snapshot_name));

    char *err = NULL;

    /* make snapshots available to this database instance */
    if (!attachdb(index_snapshot_name, db, INDEX_ATTACH, SQLITE_OPEN_READONLY, 1) ||
        !attachdb(tree_snapshot_name,  db, TREE_ATTACH,  SQLITE_OPEN_READONLY, 1)) {
        goto cleanup;
    }

    /* get directories that were unchanged, renamed, or deleted */
    if (sqlite3_exec(db, UNCHANGED_RENAMED_DELETED_CREATE, NULL, NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Could not get unchanged/changed/deleted: %s\n", err);
    }

  cleanup:
    closedb(db);

    return !!err;
}

#define CREATED_DB_EXT "created"

static size_t gen_created_name(struct PoolArgs *pa, char *name, const size_t name_size) {
    return SNFORMAT_S(name, name_size, 3,
                      pa->in.outname.data, pa->in.outname.len,
                      ".", (size_t) 1,
                      CREATED_DB_EXT, sizeof(CREATED_DB_EXT) - 1);
}

static int get_created(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    (void) ctx; (void) id; (void) data;

    struct PoolArgs *pa = (struct PoolArgs *) args;

    char dbname[MAXPATH];
    gen_created_name(pa, dbname, sizeof(dbname));

    sqlite3 *db = opendb(dbname, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, 0, 0, NULL, NULL);
    if (!db) {
        return 1;
    }

    /* ground truth */
    char index_snapshot_name[MAXPATH];
    gen_index_snapshot_name(pa, index_snapshot_name, sizeof(index_snapshot_name));

    /* tree with changes */
    char tree_snapshot_name[MAXPATH];
    gen_tree_snapshot_name(pa, tree_snapshot_name, sizeof(tree_snapshot_name));

    char *err = NULL;

    /* make snapshots available to this database instance */
    if (!attachdb(index_snapshot_name, db, INDEX_ATTACH, SQLITE_OPEN_READONLY, 1) ||
        !attachdb(tree_snapshot_name,  db, TREE_ATTACH,  SQLITE_OPEN_READONLY, 1)) {
        goto cleanup;
    }

    /* get directories that were created */
    if (sqlite3_exec(db, CREATED_CREATE, NULL, NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Could not get created: %s\n", err);
    }

  cleanup:
    closedb(db);

    return !!err;
}

/* create db file containing the differences between the index and current state of the tree */
#define DIFF_SNAPSHOT_EXT "diff"
static int get_diff(struct PoolArgs *pa, char *diff_dbname, const size_t diff_dbname_size) {
    /* get matches in parallel */
    QPTPool_wait(pa->pool);
    QPTPool_enqueue(pa->pool, 0, get_urd,     NULL);
    QPTPool_enqueue(pa->pool, 0, get_created, NULL);
    QPTPool_wait(pa->pool);

    int rc = 0;

    char *err = NULL;

    /* unchanged, renamed, deleted */
    char urd_dbname[MAXPATH];
    gen_urd_name(pa, urd_dbname, sizeof(urd_dbname));

    /* created */
    char created_dbname[MAXPATH];
    gen_created_name(pa, created_dbname, sizeof(created_dbname));

    /* generate the name of the diff database (<snapshotdb>.diff; not deleted afterwards for debugging) */
    SNFORMAT_S(diff_dbname, diff_dbname_size, 3,
               pa->in.outname.data, pa->in.outname.len,
               ".", (size_t) 1,
               DIFF_SNAPSHOT_EXT, sizeof(DIFF_SNAPSHOT_EXT) - 1);

    sqlite3 *db = opendb(diff_dbname, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, 0, 0, NULL, NULL);
    if (!db) {
        return 1;
    }

    /* make pieces of partial changes available to this database instance */
    if (!attachdb(urd_dbname,     db, UNCHANGED_RENAMED_DELETED, SQLITE_OPEN_READONLY, 1) ||
        !attachdb(created_dbname, db, CREATED,                   SQLITE_OPEN_READONLY, 1)) {
        rc = 1;
        goto cleanup;
    }

    /* create a view of all changes between the index and the tree */
    if (sqlite3_exec(db, ALL_MATCHES_CREATE, NULL, NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Could not create all matches view: %s\n", err);
        rc = 1;
        goto cleanup;
    }

    /* write reduced diff to db file */
    if (sqlite3_exec(db, INCR_DIFF_CREATE, NULL, NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Could not create diff table: %s\n", err);
        rc = 1;
    }

  cleanup:
    closedb(db);

    return rc;
}

static int apply_removes_and_move_outs_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    struct PoolArgs *pa = (struct PoolArgs *) args;

    /* const char *indexpath = data[0]; */
    /* const char *treepath  = data[1]; */
    /* const char *treeinode = data[2]; */
    /* const char  del       = data[3][0]; */

    const char *indexpath = data[0];
    const char *treeinode = data[1];
    const char  del       = data[2][0];

    char path[MAXPATH];
    const size_t path_len = SNFORMAT_S(path, sizeof(path), 2,
                                       pa->index.path.data, pa->index.parent_len, /* parent comes with trailing slash */
                                       indexpath, strlen(indexpath));

    /* delete index directory */
    if (del == '1') {
        char dbname[MAXPATH];
        SNFORMAT_S(dbname, sizeof(dbname), 3,
                   path, path_len,
                   "/", (size_t) 1,
                   DBNAME, DBNAME_LEN);

        ERRNO_NOT_ERR(unlink(dbname), "    Error: Could not delete " DBNAME " in \"%s\"", path);
        ERRNO_NOT_ERR(rmdir(path),    "    Error: Could not rmdir \"%s\"", path);

        fprintf(stdout, "    Deleted directory \"%s\"\n", path);
    }
    /* move index directory into parking lot */
    else {
        char plname[MAXPATH];
        SNFORMAT_S(plname, sizeof(plname), 4,
                   pa->parking_lot.data, pa->parking_lot.len,
                   "/",  (size_t) 1,
                   "d.", (size_t) 2,
                   treeinode, strlen(treeinode));

        ERRNO_NOT_ERR(rename(path, plname), "    Error: Could not move \"%s\" to parking lot \"%s\"",
                      path, plname);

        fprintf(stdout, "    Moved directory \"%s\" into parking lot \"%s\"\n", path, plname);
    }

    return 0;
}

static int apply_removes_and_move_outs(struct PoolArgs *pa, sqlite3 *db) {
    fprintf(stdout, "Start deleting directories and moving directories to the parking lot\n");

    struct start_end timer;
    clock_gettime(CLOCK_MONOTONIC, &timer.start);

    char *err = NULL;
    if (sqlite3_exec(db, GET_DELETES_AND_MOVE_OUTS,
                     apply_removes_and_move_outs_callback,
                     pa, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Could not get removes and move outs from diff table: %s\n", err);
        return 1;
    }

    clock_gettime(CLOCK_MONOTONIC, &timer.end);
    const long double processtime = sec(nsec(&timer));

    fprintf(stdout, "Time spent deleting directories and moving directories to the parking lot: %.2Lfs\n", processtime);
    fflush(stdout);

    return 0;
}

static int apply_creates_and_move_ins_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    struct PoolArgs *pa = (struct PoolArgs *) args;

    /* const char *indexpath = data[0]; */
    /* const char *treepath  = data[1]; */
    /* const char *treeinode = data[2]; */
    /* const char  new       = data[3][0]; */

    const char *treepath  = data[0];
    const char *treeinode = data[1];
    const char  new       = data[2][0];

    char path[MAXPATH];
    SNFORMAT_S(path, sizeof(path), 2,
               pa->index.path.data, pa->index.parent_len, /* parent comes with trailing slash */
               treepath, strlen(treepath));

    /* create index directory */
    if (new == '1') {
        ERRNO_NOT_ERR(mkdir(path, 0775), "    Error: Could not mkdir \"%s\"", path);

        /* db.db is not created here */

        fprintf(stdout, "    Created directory \"%s\"\n", path);
    }
    /* move index directory from parking lot */
    else {
        char plname[MAXPATH];
        SNFORMAT_S(plname, sizeof(plname), 4,
                   pa->parking_lot.data, pa->parking_lot.len,
                   "/",  (size_t) 1,
                   "d.", (size_t) 2,
                   treeinode, strlen(treeinode));

        ERRNO_NOT_ERR(rename(plname, path), "    Error: Could not move from parking lot \"%s\" to \"%s\"", plname, path);

        fprintf(stdout, "    Moved directory \"%s\" from parking lot to \"%s\"\n", plname, path);
    }

    return 0;
}

static int apply_creates_and_move_ins(struct PoolArgs *pa, sqlite3 *db) {
    fprintf(stdout, "Start creating directories and moving directories back from the parking lot\n");

    struct start_end timer;
    clock_gettime(CLOCK_MONOTONIC, &timer.start);

    char *err = NULL;
    if (sqlite3_exec(db, GET_CREATES_AND_MOVE_INS,
                     apply_creates_and_move_ins_callback,
                     pa, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Could not get creates and move ins from diff table: %s\n", err);
        return 1;
    }

    clock_gettime(CLOCK_MONOTONIC, &timer.end);
    const long double processtime = sec(nsec(&timer));

    fprintf(stdout, "Time spent creating directories and moving directories back from the parking lot: %.2Lfs\n", processtime);
    fflush(stdout);

    return 0;
}

struct UpdateDir {
    str_t treepath;
    str_t treeinode;
};

/* update a single directory by moving the <parking lot>/<dir inode> database file into the correct path */
static int apply_update(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    (void ) ctx; (void) id;

    struct PoolArgs *pa = (struct PoolArgs *) args;
    struct UpdateDir *ud = (struct UpdateDir *) data;

    char path[MAXPATH];
    const size_t path_len = SNFORMAT_S(path, sizeof(path), 2,
                                       pa->index.path.data, pa->index.parent_len, /* parent comes with trailing slash */
                                       ud->treepath.data, ud->treepath.len);

    /* move database file from parking lot */
    char plname[MAXPATH];
    SNFORMAT_S(plname, sizeof(plname), 3,
               pa->parking_lot.data, pa->parking_lot.len,
               "/", (size_t) 1,
               ud->treeinode.data, ud->treeinode.len);

    /*
     * this might fail if a directory was renamed, due to a parent
     * directory being renamed, but otherwise being unchanged, meaning
     * it was not suspect, and thus not reindexed
     */
    struct stat st;
    ERRNO_NOT_ERR(stat(plname, &st), "    Warning: Could not stat update " DBNAME " in parking lot \"%s\"", plname);

    /* destination of the database file */
    char dbname[MAXPATH];
    SNFORMAT_S(dbname, sizeof(dbname), 3,
               path, path_len,
               "/", (size_t) 1,
               DBNAME, DBNAME_LEN);

    ERRNO_NOT_ERR(rename(plname, dbname),
                  "    Error: Could not move updated db.db from parking lot \"%s\" to \"%s\"\n",
                  plname, path);

    fprintf(stdout, "    Update db \"%s\" moved to \"%s\"\n",
            plname, dbname);

    /* open the updated db */
    sqlite3 *db = opendb(dbname, SQLITE_OPEN_READONLY, 0, 0, NULL, NULL);
    if (!db) {
        return 0; /* do not stop query from continuing */
    }

    struct Permissions perms;
    char *err = NULL;
    if (sqlite3_exec(db, "SELECT mode, uid, gid FROM " SUMMARY ";",
                     get_permissions_callback, &perms, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Could not get permissions from \"%s\": %s\n", dbname, err);
        closedb(db);
        return 0; /* do not stop query from continuing */
    }

    closedb(db);

    ERRNO_NOT_ERR(chmod(path, perms.mode & (S_IRWXU | S_IRWXG | S_IRWXO)),
                  "    Error: Could not change permission of directory \"%s\" to %3o",
                  path, perms.mode & (S_IRWXU | S_IRWXG | S_IRWXO));

    ERRNO_NOT_ERR(chown(path, perms.uid, perms.gid),
                  "    Error: Could not change owners of directory \"%s\" to %" STAT_uid ":%" STAT_gid "\n",
                  path, perms.uid, perms.gid);

    ERRNO_NOT_ERR(chmod(dbname, perms.mode & (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)),
                  "    Error: Could not change permission of database \"%s\" to %3o",
                  dbname, perms.mode & (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH));

    ERRNO_NOT_ERR(chown(dbname, perms.uid, perms.gid),
                  "    Error: Could not change owners of directory \"%s\" to %" STAT_uid ":%" STAT_gid "\n",
                  path, perms.uid, perms.gid);

    fprintf(stdout, "    Updated permissions of \"%s\"\n", path);

    str_free_existing(&ud->treeinode);
    str_free_existing(&ud->treepath);
    free(ud);

    return 0;
}

static int apply_updates_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    struct PoolArgs *pa = (struct PoolArgs *) args;

    /* const char *indexpath  = data[0]; */
    /* const char *treepath   = data[1]; */
    /* const char renamed     = data[2][0]; */
    /* const char *treeinode  = data[3]; */
    /* const char *treepinode = data[4]; */
    /* const char  moved      = data[5][0]; */

    const char *treepath   = data[0];
    const char *treeinode  = data[1];

    struct UpdateDir *ud = calloc(1, sizeof(*ud));

    str_alloc_existing(&ud->treepath, strlen(treepath));
    memcpy(ud->treepath.data, treepath, ud->treepath.len);

    str_alloc_existing(&ud->treeinode, strlen(treeinode));
    memcpy(ud->treeinode.data, treeinode, ud->treeinode.len);

    QPTPool_enqueue(pa->pool, 0, apply_update, ud);

    return 0;
}

static int apply_updates(struct PoolArgs *pa, sqlite3 *db) {
    fprintf(stdout, "Start updating databases and directories\n");
    fflush(stdout);

    char *err = NULL;
    if (sqlite3_exec(db, GET_UPDATES,
                     apply_updates_callback,
                     pa, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Could not get updates from diff table: %s\n", err);
        return 1;
    }

    QPTPool_wait(pa->pool);

    return 0;
}

int incremental_update(struct PoolArgs *pa) {
    char diff_dbname[MAXPATH];

    /* generate the diff database */
    if (get_diff(pa, diff_dbname, sizeof(diff_dbname)) != 0) {
        return 1;
    }

    /* reopen diff database in read-only mode */
    sqlite3 *db = opendb(diff_dbname, SQLITE_OPEN_READONLY, 0, 0, NULL, NULL);
    if (!db) {
        return 1;
    }

    /* apply changes */
    const int ret = !(
        (apply_removes_and_move_outs(pa, db) == 0) &&
        (apply_creates_and_move_ins(pa, db) == 0) &&
        (apply_updates(pa, db) == 0)
    );

    closedb(db);

    return ret;
}
