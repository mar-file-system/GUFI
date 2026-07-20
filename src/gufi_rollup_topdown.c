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



#include <dirent.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "bf.h"
#include "debug.h"
#include "dbutils.h"
#include "rollup.h"
#include "str.h"
#include "template_db.h"
#include "utils.h"

typedef struct Path {
    str_t orig; /* normal filesystem path for traversal */
    str_t uri;  /* uri for ATTACH-ing in SQLite 3 */
} Path_t;

/* void * for sll_destroy */
static void Path_free(void *ptr) {
    Path_t *path = (Path_t *) ptr;
    str_free_existing(&path->uri);
    str_free_existing(&path->orig);
    free(path);
}

typedef struct ThreadDB {
    char *dbname;
    char *uri;
    sqlite3 *db;
} ThreadDB_t;

typedef struct Subtree {
    struct work *root;          /* top of subtree that can be rolled up */

    /* parallel_walk */
    pthread_mutex_t *mutex;     /* used by parallel_walk and copy_to_top */
    Path_t *curr;               /* the directory currently being processed */
    sll_t *subdirs;             /* per-thread list of directory Path_t *s */

    /* rollup_copy */
    ThreadDB_t *tdbs;           /* array of target dbs to copy to */
    size_t threads_used;        /* number of threads used for processing this subtree */
    size_t remaining;           /* when remaining hits 0, call rollup_merge */

    struct start_end *se;       /* initially owned by subtree, transferred to stats */
} Subtree_t;

static int Subtree_cmp(const void *lhs, const void *rhs) {
    Subtree_t **l = (Subtree_t **) lhs;
    Subtree_t **r = (Subtree_t **) rhs;
    return ((*l)->remaining < (*r)->remaining);
}

static void Subtree_free(Subtree_t *subtree, const size_t threads) {
    free(subtree->se);
    free(subtree->tdbs);    /* elements should have already been freed */

    Path_free(subtree->curr);
    pthread_mutex_destroy(subtree->mutex);
    free(subtree->mutex);
    for(size_t i = 0; i < threads; i++) {
        sll_destroy(&subtree->subdirs[i], Path_free);
    }
    free(subtree->subdirs);
    free(subtree->root);
    free(subtree);
}

struct PoolArgs {
    struct input in;
    struct template_db dbdb_template;
    struct template_db xattr_template;

    sll_t *subtrees;            /* per-thread list of Subtree_t * */

    pthread_mutex_t mutex;
    pthread_cond_t cond;        /* signal this to have main enqueue another subtree */
    size_t active_dbs;

    /* per-thread stats */
    struct {
        /* find directories to roll up */
        size_t not_rolledup;    /* could not roll up for some reason (includes below counts) */
        size_t cannot_rollup;   /* bad permissions */
        size_t too_many_before; /* already past limit before rollup */
        size_t too_many_after;  /* past limit after rollup */

        /* selected directories */
        size_t top;

        /* during rollup operation */
        sll_t subdirs;          /* counts (size_t *) */
        sll_t runtimes;         /* time to roll up into each subtree root (struct start_end *) */
                                /* does not include timestamps for errors */
        size_t failures;        /* selected for rollup and failed */
    } *stats;
};

/* put immediate subdirs into a list */
static void ls_dirs(const Path_t *path,
                    DIR *dir, struct PoolArgs *pa,
                    sll_t *subdirs) {
    struct dirent *entry = NULL;
    while ((entry = readdir(dir))) {
        const size_t len = strlen(entry->d_name);

        const int skip = (
            /* skip ., .., and anything else in skip_names */
            trie_search(pa->in.skip, entry->d_name, len, NULL) ||
            /* skip db.db */
            ((len == DBNAME_LEN) && (strncmp(entry->d_name, DBNAME, DBNAME_LEN) == 0))
        );

        if (skip) {
            continue;
        }

        const size_t child_path_len = path->orig.len + 1 + len;
        char *child_path = NULL;

        if ((entry->d_type == DT_DIR) ||
            (entry->d_type == DT_UNKNOWN)) {
            child_path = malloc(child_path_len + 1);
            SNFORMAT_S(child_path, child_path_len + 1, 3,
                       path->orig.data, path->orig.len,
                       "/", (size_t) 1,
                       entry->d_name, len);
        }
        else {
            continue;
        }

        if (entry->d_type == DT_UNKNOWN) {
            struct stat st;
            time_t crtime;                            /* unused */
            StatCalled stat_called = STAT_NOT_CALLED; /* unused */
            if (lstat_wrapper(child_path, &st, &crtime,
                              &stat_called, 1, NULL) != 0) {
                free(child_path);
                continue;
            }

            if (!S_ISDIR(st.st_mode)) {
                free(child_path);
                continue;
            }
        }

        Path_t *child = malloc(sizeof(*child));
        child->orig.data = child_path;
        child->orig.len = child_path_len;
        child->orig.free = free;

        const size_t uri_size = path->uri.len + 1 + 3 * len + 1;
        child->uri.data = malloc(uri_size);
        child->uri.len = 0;
        child->uri.free = free;

        child->uri.len = SNFORMAT_S(child->uri.data, uri_size, 2,
                                    path->uri.data, path->uri.len,
                                    "/", (size_t) 1);

        size_t used_len = len;
        child->uri.len += sqlite_uri_path(child->uri.data + child->uri.len, uri_size - child->uri.len,
                                          entry->d_name, &used_len);
        child->uri.data[child->uri.len] = '\0';

        sll_push_back(subdirs, child);
    }
}

static int parallel_walk(QPTPool_ctx_t *ctx, void *data) {
    struct PoolArgs *pa = NULL;
    QPTPool_get_args(ctx, (void **) &pa);
    const size_t id = QPTPool_get_id(ctx);
    Subtree_t *subtree = (Subtree_t *) data;

    DIR *dir = opendir_wrapper(subtree->curr->orig.data, NULL);
    if (!dir) {
        pa->stats[id].not_rolledup++;
        return 1;
    }

    /* get this directory's immediate subdirs */
    sll_t subdirs; /* Path_t * */
    sll_init(&subdirs);
    ls_dirs(subtree->curr, dir, pa, &subdirs);

    /* queue up immediate subdirs for recursion */
    sll_loop(&subdirs, node) {
        Subtree_t *subdir = calloc(1, sizeof(*subdir));

        /* references owned by subtree top */
        subdir->root    = subtree->root;
        subdir->mutex   = subtree->mutex;
        subdir->curr    = (Path_t *) sll_node_data(node);
        subdir->subdirs = subtree->subdirs;

        QPTPool_enqueue(ctx, parallel_walk, subdir);
    }

    /* push subdirs into subtree list (take ownership) */
    sll_move_append(&subtree->subdirs[id], &subdirs);
    sll_destroy(&subdirs, NULL);

    closedir(dir);
    free(subtree);

    return 0;
}

/*
 * given a subtree root directory, continue parallel tree walk,
 * recording the subdirectories with the root
 */
static int prepare_top_down_rollup(struct PoolArgs *pa, QPTPool_ctx_t *ctx, struct work *work) {
    const size_t id = QPTPool_get_id(ctx);

    /*
     * top owns all of its members; other Subtree_t
     * instances only hold references
     */
    Subtree_t *top = malloc(sizeof(*top));

    top->root = work;       /* take ownership */

    top->mutex = malloc(sizeof(*top->mutex));
    pthread_mutex_init(top->mutex, NULL);

    top->curr = NULL;       /* not set for top */

    /* set up per-thread lists to subdirs */
    top->subdirs = calloc(pa->in.maxthreads, sizeof(*top->subdirs));
    for(size_t i = 0; i < pa->in.maxthreads; i++) {
        sll_init(&top->subdirs[i]);
    }

    /* convert path to URI for SQLite 3 */
    Path_t *top_path = malloc(sizeof(*top_path));
    top_path->orig = (str_t) REFSTR(work->name, work->name_len);
    str_alloc_existing(&top_path->uri, 3 * work->name_len);

    size_t used_len = work->name_len;
    top_path->uri.len = sqlite_uri_path(top_path->uri.data, top_path->uri.len,
                                        work->name, &used_len);
    top_path->uri.data[top_path->uri.len] = '\0'; /* need to NULL terminate because uri is used in SNPRINTF */

    top->curr = top_path;  /* take ownership at top only, rest go into top->subdirs */

    /* record this subtree in the pool state */
    sll_push_back(&pa->subtrees[id], top);

    /* ************************************************************ */

    /*
     * walk the subtree in parallel and get all subdirectories
     * that need to be copied to the top
     */

    Subtree_t *subtree = calloc(1, sizeof(*subtree));
    subtree->root    = work;                 /* reference */
    subtree->subdirs = top->subdirs;         /* reference */
    subtree->curr    = top_path;             /* reference */
    subtree->mutex   = top->mutex;           /* reference */

    QPTPool_enqueue(ctx, parallel_walk, subtree);

    return 0;
}

/* get number of entries in the tree */
static int get_entries(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    return !(sscanf(data[0], "%zu", (size_t *) args) == 1);
}

static int find_top(QPTPool_ctx_t *ctx, void *data);

/*
 * return -1: error
 *         0: roll up this directory
 *         1: this directory has/will have too many entries
 */
static int check_entries_count(struct PoolArgs *pa, struct work *work, size_t *counter,
                               DIR *dir, QPTPool_ctx_t *ctx, const size_t id,
                               sqlite3 *db, const char *sql) {
    char *err = NULL;

    size_t entries = 0;
    if (sqlite3_exec(db, sql, get_entries, &entries, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Could not get entries count from \"%s\": %s\n",
                                  work->name, err);
        closedb(db);
        pa->stats[id].not_rolledup++;
        return -1;
    }

    /*
     * if this directory has/will have too many entries, do not
     * roll up this directory and instead keep going down
     */
    if (entries > pa->in.rollup.entries_limit) {
        closedb(db);
        (*counter)++;
        pa->stats[id].not_rolledup++;

        descend(ctx,
                &pa->in, work,
                dir, 1,
                find_top, NULL, NULL,
                NULL);

        return 1;
    }

    return 0;
}

/* get both canrolledup and isrolled up */
static int get_rollup(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    int **rollup = (int **) args;
    return !((sscanf(data[0], "%d", rollup[0]) == 1) &&
             (sscanf(data[1], "%d", rollup[1]) == 1));
}

/*
 * descend until a rollup-able directory is found
 * then complete the tree walk to get a list of directories to copy into the top
 */
static int find_top(QPTPool_ctx_t *ctx, void *data) {
    struct PoolArgs *pa = NULL;
    QPTPool_get_args(ctx, (void **) &pa);
    const size_t id = QPTPool_get_id(ctx);

    struct work *work = (struct work *) data;

    DIR *dir = opendir_wrapper(work->name, NULL);
    if (!dir) {
        pa->stats[id].not_rolledup++;
        goto free_work;
    }

    /* not deep enough - descend */
    if (work->level < pa->in.min_level) {
        pa->stats[id].not_rolledup++;
        descend(ctx,
                &pa->in, work,
                dir, 1,
                find_top, NULL, NULL,
                NULL);
        goto close_dir;
    }

    const size_t dbname_len = work->name_len + 1 + DBNAME_LEN;
    char *dbname = malloc(dbname_len + 1);
    SNFORMAT_S(dbname, dbname_len + 1, 3,
               work->name, work->name_len,
               "/", (size_t) 1,
               DBNAME, DBNAME_LEN);

    int canrollup = 0;
    int isrolledup = 0;

    sqlite3 *db = opendb(dbname, SQLITE_OPEN_READONLY, 1, 0, NULL, NULL);

    /*
     * if the database file does not exist, this directory cannot rollup
     *
     * opendb will print an error, but keep going
     */
    if (db) {
        char *err = NULL;

        /* get whether or not this directory CAN roll up */
        int *arr[] = { &canrollup, &isrolledup };
        if (sqlite3_exec(db, "SELECT canrollup, isrolledup FROM " SUMMARY " WHERE isroot == 1;",
                         get_rollup, arr, &err) != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr, "Error: Could not get rollup data from \"%s\": %s\n",
                                      work->name, err);
            closedb(db);
            pa->stats[id].not_rolledup++;
            goto free_dbname;
        }

        /* check whether or not this directory SHOULD roll up */
        if (pa->in.rollup.entries_limit) {
            /*
             * if this directory started with too many entries, do not
             * roll up this directory and instead keep going down
             */
            if (check_entries_count(pa, work, &pa->stats[id].too_many_before,
                                    dir, ctx, id, db,
                                    "SELECT totfiles + totlinks "
                                    "FROM " SUMMARY " "
                                    "WHERE isroot == 1;") != 0) {
                closedb(db);
                goto free_dbname;
            }

            /*
             * if rolling up into this directory would result in
             * too many entries, do not roll up this directory
             * and instead keep going down
             */
            if (check_entries_count(pa, work, &pa->stats[id].too_many_after,
                                    dir, ctx, id, db,
                                    "SELECT " TREESUMMARY ".totfiles + " TREESUMMARY ".totlinks "
                                    "FROM " TREESUMMARY " JOIN " SUMMARY " ON " TREESUMMARY ".inode == " SUMMARY ".inode "
                                    "WHERE " SUMMARY ".isroot == 1;") != 0) {
                closedb(db);
                goto free_dbname;
            }
        }

        closedb(db); /* readonly db is not needed any more */
    }

    /* found top - walk the subtree */
    if (canrollup == 1) {
        pa->stats[id].top++;

        if (pa->in.dry_run ||
            (isrolledup && pa->in.dont_reprocess)) {
            /* don't do anything */
        }
        else {
            prepare_top_down_rollup(pa, ctx, work);
            work = NULL; /* don't free subtree root work */
        }
    }
    else {
        pa->stats[id].not_rolledup++;
        pa->stats[id].cannot_rollup++;

        /* cannot roll up, so descend */
        descend(ctx,
                &pa->in, work,
                dir, 1,
                find_top, NULL, NULL,
                NULL);
    }

  free_dbname:
    free(dbname);
  close_dir:
    closedir(dir);
  free_work:
    free(work);

    return 0;
}

/*
 * merge per-thread dbs into main db
 * tdbs[0] is db.db
 *
 * called when subtree has been processed
 */
static void rollup_merge(const size_t id, struct PoolArgs *pa, Subtree_t *subtree) {
    if (subtree->tdbs) {
        /* merge per-thread data into db.db */
        for(size_t i = 1; i < subtree->threads_used; i++) {
            if (!subtree->tdbs[i].db) {
                continue;
            }

            closedb(subtree->tdbs[i].db);

            if (attachdb(subtree->tdbs[i].uri, subtree->tdbs[0].db, ROLLUP_SUBDIR_ATTACH_NAME, SQLITE_OPEN_READONLY, 1, NULL)) {
                char *err = NULL;
                if (sqlite3_exec(subtree->tdbs[0].db,
                                 "INSERT INTO " SUMMARY             " SELECT * FROM " ROLLUP_SUBDIR_ATTACH_NAME "." SUMMARY "; "
                                 "INSERT INTO " PENTRIES_ROLLUP     " SELECT * FROM " ROLLUP_SUBDIR_ATTACH_NAME "." PENTRIES "; "
                                 "INSERT INTO " XATTRS_ROLLUP       " SELECT * FROM " ROLLUP_SUBDIR_ATTACH_NAME "." XATTRS_AVAIL "; "
                                 "INSERT INTO " EXTERNAL_DBS_ROLLUP " SELECT * FROM " ROLLUP_SUBDIR_ATTACH_NAME "." EXTERNAL_DBS "; ",
                                 NULL, NULL, &err) != SQLITE_OK) {
                    sqlite_print_err_and_free(err, stderr, "Error: Could not merge rollup data from thread %zu into %s: %s\n",
                                              i, subtree->root->name, err);

                    /* keep going */
                }

                detachdb(subtree->tdbs[i].uri, subtree->tdbs[0].db, ROLLUP_SUBDIR_ATTACH_NAME, 1, NULL);
            }

            free(subtree->tdbs[i].uri);
            if (remove(subtree->tdbs[i].dbname) != 0) {
                const int err = errno;
                fprintf(stderr, "Warning: Could not remove \"%s\": %s (%d)\n",
                        subtree->tdbs[i].dbname, strerror(err), err);
            }
            free(subtree->tdbs[i].dbname);
        }

        closedb(subtree->tdbs[0].db);
        free(subtree->tdbs[0].dbname);
    }

    /* cleanup */

    struct start_end *se = subtree->se;
    subtree->se = NULL;
    const size_t threads_used = subtree->threads_used;

    Subtree_free(subtree, 0);

    clock_gettime(CLOCK_MONOTONIC, &se->end);
    sll_push_back(&pa->stats[id].runtimes, se);

    pthread_mutex_lock(&pa->mutex);
    pa->active_dbs -= threads_used;
    pthread_cond_broadcast(&pa->cond);
    pthread_mutex_unlock(&pa->mutex);
}

struct ThreadRollUp {
    Subtree_t *subtree;
    ThreadDB_t *tdb;       /* target to copy to */
    Path_t *path;          /* subdirectory path being operated on */
};

/* copy one subdirectory into a per-thread database */
static int copy_to_top(QPTPool_ctx_t *ctx, void *data) {
    struct PoolArgs *pa = NULL;
    QPTPool_get_args(ctx, (void **) &pa);
    const size_t id = QPTPool_get_id(ctx);
    struct ThreadRollUp *tru = (struct ThreadRollUp *) data;
    Subtree_t *subtree = tru->subtree;

    int rc = 0;

    /* roll up one subdirectory into one of the root's per-thread db file */

    /* attach the subdirectory's db.db in READONLY mode to the per-thread database */
    char *child_dbname = rollup_child_attach(tru->tdb->db, &tru->path->uri, SQLITE_OPEN_READONLY);
    if (!child_dbname) {
        rc = -1;
        goto cleanup;
    }

    rexa_t rexa = {
        .xattr       = {
            .temp    = &pa->xattr_template,
            .mutex   = subtree->mutex,
        },
        .parent      = subtree->root->name,
        .parent_len  = subtree->root->name_len,
        .child       = tru->path->orig.data,
        .child_len   = tru->path->orig.len,
    };

    /* get the correct rolled up path */
    const char *rolledup_path = rexa.child + rexa.parent_len - subtree->root->basename_len;
    const size_t rolledup_path_len = rexa.child_len - rexa.parent_len + subtree->root->basename_len;

    /* SQL for doing rollup */
    const size_t rollup_one_subdir_len =
        sizeof(ROLLUP_ONE_SUBDIR_FRONT) +
        1 + rolledup_path_len + 1 +
        sizeof(ROLLUP_ONE_SUBDIR_BACK);
    char *rollup_one_subdir = malloc(rollup_one_subdir_len + 1);
    SNFORMAT_S(rollup_one_subdir, rollup_one_subdir_len + 1, 5,
               ROLLUP_ONE_SUBDIR_FRONT, sizeof(ROLLUP_ONE_SUBDIR_FRONT) - 1,
               "'", (size_t) 1,
               rolledup_path, rolledup_path_len,
               "'", (size_t) 1,
               ROLLUP_ONE_SUBDIR_BACK, sizeof(ROLLUP_ONE_SUBDIR_BACK) - 1);

    /*
     * roll up the subdirectory into this directory
     *
     * xattrs that were not rolled in are directly rolled up into the
     * target user/group db, not into per-thread user/group dbs
     */
    if (rollup_child_process(tru->tdb->db, rollup_one_subdir, &rexa) != 0) {
        rc = -1;
    }

    free(rollup_one_subdir);

    /* always detach */
    rollup_child_detach(tru->tdb->db, child_dbname);

  cleanup:
    Path_free(tru->path);
    free(tru);

    pthread_mutex_lock(subtree->mutex);
    const size_t remaining = --subtree->remaining;
    pthread_mutex_unlock(subtree->mutex);

    if (!remaining) {
        rollup_merge(id, pa, subtree);
    }

    return rc;
}

/* set up and kick off copies into per-thread databases placed in the target directory */
static int rollup_copy(QPTPool_ctx_t *ctx, void *data) {
    struct PoolArgs *pa = NULL;
    QPTPool_get_args(ctx, (void **) &pa);
    Subtree_t *subtree = (Subtree_t *) data;
    struct work *work = subtree->root;
    Path_t *path = subtree->curr;

    /* setup */

    subtree->se = calloc(1, sizeof(*subtree->se));
    clock_gettime(CLOCK_MONOTONIC, &subtree->se->start);

    /* <path>/db.db */
    const size_t dbname_len = work->name_len + 1 + DBNAME_LEN;
    char *dbname = malloc(dbname_len + 1);
    SNFORMAT_S(dbname, dbname_len + 1, 3,
               work->name, work->name_len,
               "/", (size_t) 1,
               DBNAME, DBNAME_LEN);

    /* reopen the db in readwrite mode */
    sqlite3 *topdb = opendb(dbname, SQLITE_OPEN_READWRITE, 1, 0, NULL, NULL);
    if (!topdb) {
        pa->stats[pa->in.maxthreads].failures++;
        free(dbname);
        Subtree_free(subtree, pa->in.maxthreads);
        return -1;
    }

    /* clear out old rollup data */
    str_t name = REFSTR(work->name, work->name_len);
    if (rollup_init(topdb, &name) != 0) {
        pa->stats[pa->in.maxthreads].failures++;
        closedb(topdb);
        free(dbname);
        Subtree_free(subtree, pa->in.maxthreads);
        return -1;
    }

    /* get number of subdirectories to process */
    size_t *subdir_count = calloc(1, sizeof(*subdir_count));
    for(size_t i = 0; i < pa->in.maxthreads; i++) {
        *subdir_count += sll_get_size(&subtree->subdirs[i]);
    }
    subtree->remaining = *subdir_count;

    /* save subdirectory count in stats */
    sll_push_back(&pa->stats[pa->in.maxthreads].subdirs, subdir_count);

    /* no need to open maxthreads dbs for < maxthreads directories */
    const size_t threads_used = min(*subdir_count, pa->in.maxthreads);
    subtree->threads_used = threads_used;

    /* single directory subtree - just clean up */
    if (threads_used == 0) {
        subtree->tdbs = NULL;
        closedb(topdb);
        free(dbname);

        pthread_mutex_unlock(&pa->mutex);
        rollup_merge(pa->in.maxthreads, pa, subtree);
        pthread_mutex_lock(&pa->mutex);

        return 0;
    }

    pa->active_dbs += threads_used; /* locked by caller */

    ThreadDB_t *tdbs = calloc(threads_used, sizeof(*tdbs));
    subtree->tdbs = tdbs;

    /* thread 0 writes directly into the target db */
    tdbs[0].dbname = dbname;         /* owned */
    tdbs[0].uri    = path->uri.data; /* reference */
    tdbs[0].db     = topdb;          /* owned */

    /* open per-thread dbs */
    const size_t tdbname_len = dbname_len + 1 + UINT64_DIGITS;                 /* <path>/db.db.<thread id> */
    const size_t uri_len = path->uri.len + 1 + DBNAME_LEN + 1 + UINT64_DIGITS; /* <uri>/db.db.<thread id> */
    for(size_t i = 1; i < threads_used; i++) {
        tdbs[i].dbname = malloc(tdbname_len + 1);
        SNPRINTF(tdbs[i].dbname, tdbname_len + 1, "%s.%zu", dbname, i);
        tdbs[i].uri = malloc(uri_len + 1);
        SNPRINTF(tdbs[i].uri, uri_len + 1, "%s/" DBNAME ".%zu", path->uri.data, i);
        tdbs[i].db = template_to_db(&pa->dbdb_template, tdbs[i].dbname,
                                    geteuid(), getegid(), NULL);
    }

    /* ************************************************************ */

    if (*subdir_count) {
        size_t enqueue_offset = 0;

        /* does not need to be carried around any more */
        sll_t *subdirs = subtree->subdirs;
        subtree->subdirs = NULL;

        /* enqueue subdirectories for copying into per-thread database files */
        for(size_t i = 0; i < pa->in.maxthreads; i++) {
            for(Path_t *child_path = sll_pop_front(&subdirs[i]);
                child_path;
                child_path = sll_pop_front(&subdirs[i])) {

                struct ThreadRollUp *tru = malloc(sizeof(*tru));
                tru->subtree = subtree;             /* reference */
                tru->tdb = &tdbs[enqueue_offset];   /* reference */
                tru->path = child_path;             /* take ownership */

                QPTPool_enqueue(ctx, copy_to_top, tru);

                enqueue_offset = (enqueue_offset + 1) % threads_used;
            }

            sll_destroy(&subdirs[i], NULL); /* nothing should be in this list */
        }

        free(subdirs);
    }

    return 0;
}

static int compare_subdirs(const void *lhs, const void *rhs) {
    const size_t *l = (size_t *) lhs;
    const size_t *r = (size_t *)rhs;

    if (*l < *r) {
        return -1;
    }
    if (*l > *r) {
        return 1;
    }

    return 0;
}

static int compare_runtimes(const void *lhs, const void *rhs) {
    return (* (long double *) lhs) - (* (long double *) rhs);
}

static void compute_stats(struct PoolArgs *pa) {
    size_t not_rolledup = 0;
    size_t cannot_rollup = 0;
    size_t too_many_before = 0;
    size_t too_many_after = 0;
    size_t top = 0;
    size_t failures = 0;
    for(size_t i = 0; i < pa->in.maxthreads; i++) {
        not_rolledup += pa->stats[i].not_rolledup;
        cannot_rollup += pa->stats[i].cannot_rollup;
        too_many_before += pa->stats[i].too_many_before;
        too_many_after += pa->stats[i].too_many_after;
        top += pa->stats[i].top;
        failures += pa->stats[i].failures;
    }

    size_t *subdirs = calloc(top, sizeof(*subdirs));
    size_t subdir_sum = 0;

    long double *runtimes = calloc(top, sizeof(*runtimes));
    long double runtime_sum = 0;
    size_t idx[] = {0, 0};
    for(size_t i = 0; i < pa->in.maxthreads + 1; i++) {
        sll_loop(&pa->stats[i].subdirs, node) {
            const size_t sd = * (size_t *) sll_node_data(node);
            subdir_sum += sd;
            subdirs[idx[0]++] = sd;
        }

        sll_loop(&pa->stats[i].runtimes, node) {
            struct start_end *se = (struct start_end *) sll_node_data(node);
            const long double rt = sec(nsec(se));
            runtime_sum += rt;
            runtimes[idx[1]++] = rt;
        }
    }

    qsort(subdirs,  top, sizeof(*subdirs),  compare_subdirs);
    qsort(runtimes, top, sizeof(*runtimes), compare_runtimes);

    const long double subdir_mean = ((long double) subdir_sum) / top;
    long double subdir_var  = 0;
    const long double runtime_mean = runtime_sum / top;
    long double runtime_var = 0;
    for(size_t i = 0; i < top; i++) {
        {
            const long double diff = subdirs[i] - subdir_mean;
            subdir_var += diff * diff;
        }
        {
            const long double diff = runtimes[i] - runtime_mean;
            runtime_var += diff * diff;
        }
    }
    if (top) {
         /* variance of population (the directories that were rolled up) */
        subdir_var  /= top;
        runtime_var /= top;
    }
    const long double subdir_stdev  = sqrt(subdir_var);
    const long double runtime_stdev = sqrt(runtime_var);

    fprintf(stderr, "%zu directories could not rollup\n", not_rolledup);
    fprintf(stderr, "    %zu directories are not allowed to rollup\n", cannot_rollup);
    fprintf(stderr, "    %zu directories started with too many entries\n", too_many_before);
    fprintf(stderr, "    %zu directories would have ended up with too many entries\n", too_many_after);
    fprintf(stderr, "Rolled up into %zu top-level directories\n", top);
    fprintf(stderr, "    Subdir stats:\n");
    fprintf(stderr, "        Rolled up: %zu\n", subdir_sum);
    fprintf(stderr, "        Min:   %zu\n", top?subdirs[0]:0);
    fprintf(stderr, "        Max:   %zu\n", top?subdirs[top - 1]:0);
    fprintf(stderr, "        Mean:  %.2Lf\n", top?subdir_mean:NAN);
    fprintf(stderr, "        Stdev: %.2Lf\n", top?subdir_stdev:NAN);
    fprintf(stderr, "    Time to do rollup copies stats:\n");
    fprintf(stderr, "        Min:   %.6Lfs\n", top?runtimes[0]:NAN);
    fprintf(stderr, "        Max:   %.6Lfs\n", top?runtimes[top - 1]:NAN);
    fprintf(stderr, "        Mean:  %.6Lfs\n", top?runtime_mean:NAN);
    fprintf(stderr, "        Stdev: %.6Lfs\n", top?runtime_stdev:NAN);
    fprintf(stderr, "    %zu failed rollup copies\n", failures);
    fprintf(stderr, "Total remaining directories to traverse: %zu\n", not_rolledup + top);

    free(runtimes);
    free(subdirs);
}

static void sub_help(void) {
   printf("GUFI_tree         GUFI tree to roll up\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_THREADS,

        /* processing/tree walk flags */
        FLAG_MIN_LEVEL, FLAG_ROLLUP_LIMIT, FLAG_DRY_RUN,

        FLAG_END
    };

    struct PoolArgs pa;
    process_args_and_maybe_exit(options, 1, "GUFI_tree ...", &pa.in);

    int rc = 0;

    const str_t HERE = REFSTR(".", 1);
    init_template_db(&pa.dbdb_template);
    if (create_rollup_template(&pa.dbdb_template, &HERE) != 0) {
        fprintf(stderr, "Could not create template file\n");
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    init_template_db(&pa.xattr_template);
    if (create_xattrs_template(&pa.xattr_template, NULL) != 0) {
        fprintf(stderr, "Error: Could not create xattr template file\n");
        rc = 1;
        goto close_dbdb_template;
    }

    pa.subtrees = calloc(pa.in.maxthreads, sizeof(*pa.subtrees));
    if (!pa.subtrees) {
        fprintf(stderr, "Error: Could not allocate subtree root lists\n");
        rc = 1;
        goto close_xattr_template;
    }

    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        sll_init(&pa.subtrees[i]);
    }

    pa.stats = calloc(pa.in.maxthreads + 1, sizeof(*pa.stats));
    if (!pa.stats) {
        fprintf(stderr, "Error: Could not allocate stat counters\n");
        rc = 1;
        goto free_subtrees;
    }

    for(size_t i = 0; i < pa.in.maxthreads + 1; i++) {
        sll_init(&pa.stats[i].subdirs);
        sll_init(&pa.stats[i].runtimes);
    }

    QPTPool_ctx_t *pool = QPTPool_init(pa.in.maxthreads, &pa);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        rc = 1;
        goto free_stats;
    }

    struct start_end runtime;
    clock_gettime(CLOCK_MONOTONIC, &runtime.start);

    /* enqueue each input argument to search for tops of subtrees that can roll up */
    for(int i = 0; i < pa.in.pos.argc; i++) {
        const char *path = pa.in.pos.argv[i];
        struct work *work = new_work_with_name(NULL, 0, path, strlen(path));
        /* need actual basename length, not the entire length of the starting path */
        work->basename_len = work->name_len - dirname_len(work->name, work->name_len);
        QPTPool_enqueue(pool, find_top, work);
    }

    /*
     * wait for top of all subtrees to roll up have been found and
     * their subdirectories recorded
     */
    QPTPool_wait(pool);

    struct timespec done_finding_subtrees;
    clock_gettime(CLOCK_MONOTONIC, &done_finding_subtrees);

    fprintf(stderr, "Finished Finding Top of Subtrees to Roll Up in %.2Lfs\n",
            sec(since_epoch(&done_finding_subtrees) - since_epoch(&runtime.start)));

    /*
     * do the actual rollup using the discovered subtrees
     *
     * Subtrees are pushed into the thread pool for parallel
     * processing up until all threads are active (oversubscription is
     * allowed). The remaining subtrees are pushed in as subtrees exit
     * the thread pool.
     */

    size_t subtree_count = 0;
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        subtree_count += sll_get_size(&pa.subtrees[i]);
    }

    Subtree_t **subtrees = malloc(subtree_count * sizeof(*subtrees));

    size_t j = 0;
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        for(Subtree_t *subtree = sll_pop_front(&pa.subtrees[i]);
            subtree; subtree = sll_pop_front(&pa.subtrees[i])) {
            subtrees[j++] = subtree;
        }
    }

    /* sort subtrees to process smaller ones first */
    qsort(subtrees, subtree_count, sizeof(*subtrees), Subtree_cmp);

    pthread_mutex_init(&pa.mutex, NULL);
    pthread_cond_init(&pa.cond, NULL);
    pa.active_dbs = 0;

    for(size_t i = 0; i < subtree_count; i++) {
        pthread_mutex_lock(&pa.mutex);

        /* wait until number of active copies drops */
        while (pa.active_dbs >= pa.in.maxthreads) { /* can oversubscribe */
            pthread_cond_wait(&pa.cond, &pa.mutex);
        }

        /* kick off subtree copying */
        rollup_copy(pool, subtrees[i]);
        pthread_mutex_unlock(&pa.mutex);
    }

    free(subtrees);

    QPTPool_stop(pool);

    pthread_mutex_destroy(&pa.mutex);
    pthread_cond_destroy(&pa.cond);

    clock_gettime(CLOCK_MONOTONIC, &runtime.end);

    QPTPool_destroy(pool);

    /* cleanup */
    fprintf(stderr, "Finished Rolling Up in %.2Lfs\n",
            sec(since_epoch(&runtime.end) - since_epoch(&done_finding_subtrees)));

    compute_stats(&pa);
    fprintf(stderr, "Took %.2Lf seconds\n", sec(nsec(&runtime)));

  free_stats:
    for(size_t i = 0; i < pa.in.maxthreads + 1; i++) {
        sll_destroy(&pa.stats[i].runtimes, free);
        sll_destroy(&pa.stats[i].subdirs, free);
    }
    free(pa.stats);

  free_subtrees:
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        sll_destroy(&pa.subtrees[i], NULL); /* each subtree list should be empty at this point */
    }
    free(pa.subtrees);

  close_xattr_template:
    close_template_db(&pa.xattr_template);

  close_dbdb_template:
    close_template_db(&pa.dbdb_template);

  cleanup:
    input_fini(&pa.in);

    return rc?EXIT_FAILURE:EXIT_SUCCESS;
}
