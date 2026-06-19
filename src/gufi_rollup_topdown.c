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

static void Path_free(void *ptr) {
    Path_t *path = (Path_t *) ptr;
    str_free_existing(&path->uri);
    str_free_existing(&path->orig);
    free(path);
}

typedef struct Subtree {
    struct work *root; /* top of subtree that can be rolled up */
    sll_t *subdirs;    /* per-thread list of directory Path_t *s */
    Path_t *curr;      /* the directory currently being processed */
} Subtree_t ;

struct PoolArgs {
    struct input in;
    struct template_db dbdb_template;
    struct template_db xattr_template;

    sll_t *subtrees;            /* per-thread list of Subtree_t * */

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
        sll_t subdirs;
        sll_t runtimes;         /* list of time to roll up into each subtree root (struct start_end) */
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
        Subtree_t *work = malloc(sizeof(*work));

        /* references owned by subtree top */
        work->root = subtree->root;
        work->subdirs = subtree->subdirs;
        work->curr = (Path_t *) sll_node_data(node);

        QPTPool_enqueue(ctx, parallel_walk, work);
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

    Subtree_t *subtree = malloc(sizeof(*subtree));
    subtree->root    = work;         /* reference */
    subtree->subdirs = top->subdirs; /* reference */
    subtree->curr    = top_path;     /* reference */

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

    /* found top - do rollup */
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

/* pulls double duty as bookkeeping and work item */
struct ThreadDB {
    struct work *work;
    char *dbname;
    char *uri;
    sqlite3 *db;
};

struct ThreadRollUp {
    struct ThreadDB *tdbs;
    Path_t *path; /* subdirectory path being operated on */
};

/* copy one subdirectory into a per-thread database */
static int copy_to_top(QPTPool_ctx_t *ctx, void *data) {
    struct PoolArgs *pa = NULL;
    QPTPool_get_args(ctx, (void **) &pa);
    const size_t id = QPTPool_get_id(ctx);
    struct ThreadRollUp *tru = (struct ThreadRollUp *) data;

    /* roll up one subdirectory into one of the root's per-thread db file */

    /* attach the subdirectory's db.db in READONLY mode to the per-thread database */
    char *child_dbname = rollup_child_attach(tru->tdbs[id].db, &tru->path->uri, SQLITE_OPEN_READONLY);
    if (!child_dbname) {
        return -1;
    }

    pthread_mutex_t xattr_mutex = PTHREAD_MUTEX_INITIALIZER;
    rexa_t rexa = {
        .xattr       = {
            .temp    = &pa->xattr_template,
            .mutex   = &xattr_mutex,
        },
        .parent      = tru->tdbs->work->name,
        .parent_len  = tru->tdbs->work->name_len,
        .child       = tru->path->orig.data,
        .child_len   = tru->path->orig.len,
    };

    /* get the correct rolled up path */
    const char *rolledup_path = rexa.child + rexa.parent_len - tru->tdbs->work->basename_len;
    const size_t rolledup_path_len = rexa.child_len - rexa.parent_len + tru->tdbs->work->basename_len;

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

    int rc = 0;

    /*
     * roll up the subdirectory into this directory
     *
     * xattrs that were not rolled in are directly rolled up into the
     * target user/group db, not into per-thread user/group dbs
     */
    if (rollup_child_process(tru->tdbs[id].db, rollup_one_subdir, &rexa) != 0) {
        rc = -1;
    }

    free(rollup_one_subdir);

    /* always detach */
    rollup_child_detach(tru->tdbs[id].db, child_dbname);

    Path_free(tru->path);
    free(tru);

    return rc;
}

/* set up and kick off copies into per-thread databases placed in the target directory */
static struct ThreadDB *parallel_copy(QPTPool_ctx_t *ctx, void *data) {
    struct PoolArgs *pa = NULL;
    QPTPool_get_args(ctx, (void **) &pa);
    const size_t id = QPTPool_get_id(ctx);
    Subtree_t *subtree = (Subtree_t *) data;
    struct work *work = subtree->root;
    Path_t *path = subtree->curr;

    /* setup */

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
        pa->stats[id].failures++;
        free(dbname);
        return NULL;
    }

    /* clear out old rollup data */
    str_t name = REFSTR(work->name, work->name_len);
    if (rollup_init(topdb, &name) != 0) {
        pa->stats[id].failures++;
        closedb(topdb);
        free(dbname);
        return NULL;
    }

    struct ThreadDB *tdbs = calloc(pa->in.maxthreads, sizeof(*tdbs));

    /* thread 0 writes directly into the target db */
    tdbs[0].work   = work;           /* reference (still owned by subtree) */
    tdbs[0].dbname = dbname;
    tdbs[0].uri    = path->uri.data; /* reference */
    tdbs[0].db     = topdb;

    /* open per-thread dbs */
    const size_t tdbname_len = dbname_len + 1 + 20;                 /* <path>/db.db.<thread id> */
    const size_t uri_len = path->uri.len + 1 + DBNAME_LEN + 1 + 20; /* <uri>/db.db.<thread id> */
    for(size_t i = 1; i < pa->in.maxthreads; i++) {
        tdbs[i].work = work; /* reference */
        tdbs[i].dbname = malloc(tdbname_len + 1);
        SNPRINTF(tdbs[i].dbname, tdbname_len + 1, "%s.%zu", dbname, i);
        tdbs[i].uri = malloc(uri_len + 1);
        SNPRINTF(tdbs[i].uri, uri_len + 1, "%s/" DBNAME ".%zu", path->uri.data, i);
        tdbs[i].db = template_to_db(&pa->dbdb_template, tdbs[i].dbname,
                                    geteuid(), getegid(), NULL);
        /* TODO: move this to main to only do CREATE once */
        create_treesummary_tables(tdbs[i].dbname, tdbs[i].db, NULL);
    }

    size_t *subdir_count = calloc(1, sizeof(*subdir_count));
    sll_push_back(&pa->stats[id].subdirs, subdir_count);

    /* ************************************************************ */

    /* enqueue subtree paths for copying into per-thread database files */
    for(size_t i = 0; i < pa->in.maxthreads; i++) {
        *subdir_count += sll_get_size(&subtree->subdirs[i]);

        for(Path_t *child_path = sll_pop_front(&subtree->subdirs[i]);
            child_path;
            child_path = sll_pop_front(&subtree->subdirs[i])) {

            struct ThreadRollUp *tru = malloc(sizeof(*tru));
            tru->tdbs = tdbs;       /* reference */
            tru->path = child_path; /* take ownership */

            QPTPool_enqueue(ctx, copy_to_top, tru);
        }

        sll_destroy(&subtree->subdirs[i], NULL); /* nothing should be in this list */
    }

    QPTPool_wait(ctx);

    return tdbs;
}

/* do rollups with discovered subtrees */
static void do_rollup(struct PoolArgs *pa, Subtree_t *subtree,
                      QPTPool_ctx_t *ctx) {
    struct start_end *se = malloc(sizeof(*se));
    clock_gettime(CLOCK_MONOTONIC, &se->start);

    /* copy subtree contents in parallel into per-thread db files */
    struct ThreadDB *tdbs = parallel_copy(ctx, subtree);

    /* merge per-thread data into db.db */
    for(size_t i = 1; i < pa->in.maxthreads; i++) {
        closedb(tdbs[i].db);

        if (attachdb(tdbs[i].uri, tdbs[0].db, ROLLUP_SUBDIR_ATTACH_NAME, SQLITE_OPEN_READONLY, 1, NULL)) {
            char *err = NULL;
            if (sqlite3_exec(tdbs[0].db,
                             "INSERT INTO " SUMMARY             " SELECT * FROM " ROLLUP_SUBDIR_ATTACH_NAME "." SUMMARY "; "
                             "INSERT INTO " PENTRIES_ROLLUP     " SELECT * FROM " ROLLUP_SUBDIR_ATTACH_NAME "." PENTRIES "; "
                             "INSERT INTO " XATTRS_ROLLUP       " SELECT * FROM " ROLLUP_SUBDIR_ATTACH_NAME "." XATTRS_AVAIL "; "
                             "INSERT INTO " EXTERNAL_DBS_ROLLUP " SELECT * FROM " ROLLUP_SUBDIR_ATTACH_NAME "." EXTERNAL_DBS "; ",
                             NULL, NULL, &err) != SQLITE_OK) {
                sqlite_print_err_and_free(err, stderr, "Error: Could not merge rollup data from thread %zu into %s: %s\n",
                                          i, subtree->root->name, err);

                /* keep going */
            }

            detachdb(tdbs[i].uri, tdbs[0].db, ROLLUP_SUBDIR_ATTACH_NAME, 1, NULL);
        }

        free(tdbs[i].uri);
        remove(tdbs[i].dbname);
        free(tdbs[i].dbname);
    }

    /* cleanup */

    closedb(tdbs[0].db);
    free(tdbs[0].dbname);

    free(tdbs);

    Path_free(subtree->curr);
    free(subtree->subdirs);
    free(subtree->root);
    free(subtree);

    clock_gettime(CLOCK_MONOTONIC, &se->end);
    sll_push_back(&pa->stats[0].runtimes, se);
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
    for(size_t i = 0; i < pa->in.maxthreads; i++) {
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
    if (create_dbdb_template(&pa.dbdb_template, &HERE) != 0) {
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

    pa.stats = calloc(pa.in.maxthreads, sizeof(*pa.stats));
    if (!pa.stats) {
        fprintf(stderr, "Error: Could not allocate stat counters\n");
        rc = 1;
        goto free_subtrees;
    }

    for(size_t i = 0; i < pa.in.maxthreads; i++) {
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

    /*
     * do the actual rollup using the discovered subtrees
     *
     * each subtree is processed sequentially (for now) to not have
     * explosive numbers of databases (pa.in.maxthreads * number of
     * subtrees) open at once
     */
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        for(Subtree_t *subtree = sll_pop_front(&pa.subtrees[i]);
            subtree; subtree = sll_pop_front(&pa.subtrees[i])) {
            do_rollup(&pa, subtree, pool);
        }
    }

    QPTPool_stop(pool);

    clock_gettime(CLOCK_MONOTONIC, &runtime.end);

    QPTPool_destroy(pool);

    /* cleanup */

    compute_stats(&pa);
    fprintf(stderr, "Took %.2Lf seconds\n", sec(nsec(&runtime)));

  free_stats:
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        sll_destroy(&pa.stats[i].runtimes, free);
        sll_destroy(&pa.stats[i].subdirs, free);
    }
    free(pa.stats);

  free_subtrees:
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        sll_destroy(&pa.subtrees[i], Path_free);
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
