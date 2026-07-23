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

struct PoolArgs {
    struct input in;
    struct template_db dbdb_template;
    struct template_db xattr_template;

    sll_t *tops;                /* per-thread list of top directories of each subtree (Subdir_t *) */

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

typedef struct Path {
    str_t orig;                 /* normal filesystem path for traversal */
    str_t uri;                  /* uri for ATTACH-ing in SQLite 3 */
} Path_t;

/* void * for sll_destroy */
static void Path_free(void *ptr) {
    Path_t *path = (Path_t *) ptr;
    str_free_existing(&path->uri);
    str_free_existing(&path->orig);
    free(path);
}

/* used for parallel_walk */
typedef struct Subdir {
    struct work *root;          /* top of subtree that can be rolled up */
    Path_t *path;               /* the directory currently being processed */
    sll_t *subdirs;             /* array of lists of subdirectories in this subtree */
} Subdir_t;

/* per-thread staging areas before merging into db.db */
typedef struct ThreadDB {
    char *dbname;               /* needed to remove per-thread db */
    char *uri;                  /* needed to attach to db.db */
    sqlite3 *db;
} ThreadDB_t;

/*
 * used for rollup copy operations
 *
 * only owns path; rest of members are references until remaining reaches 0
 */
typedef struct Subtree {
    struct work *root;          /* top of subtree that can be rolled up */
    Path_t *path;               /* the directory currently being processed */
    pthread_mutex_t *mutex;     /* lock for remaining */
    size_t *remaining;          /* when remaining reaches 0, call rollup_merge */
    size_t threads_used;        /* number of threads used for processing this subtree */
    ThreadDB_t *tdbs;           /* array of target dbs to copy to (count: threads_used) */
    ThreadDB_t *tdb;            /* db this work item will write to */
    struct start_end *se;       /* ownership transferred to stats */
} Subtree_t;

static void Subdir_free(Subdir_t *subdir, const size_t threads) {
    for(size_t i = 0; i < threads; i++) {
        for(Subdir_t *subsubdir = sll_pop_front(&subdir->subdirs[i]);
            subsubdir; subsubdir = sll_pop_front(&subdir->subdirs[i])) {
            /* subsubdir->subdirs is the same as subdir->subdirs */
            Path_free(subsubdir->path);
            /* subsubdir->root is the same as subdir->root */
            free(subsubdir);
        }
        sll_destroy(&subdir->subdirs[i], NULL);
    }
    free(subdir->subdirs);
    Path_free(subdir->path);
    free(subdir->root);
    free(subdir);
}

static ThreadDB_t *tdbs_create(struct template_db *dbdb,
                               Subdir_t *top,
                               const size_t threads_used,
                               char *thread0_dbname,
                               sqlite3 *thread0_db) {
    ThreadDB_t *tdbs = calloc(threads_used, sizeof(*tdbs));

    /* thread 0 writes directly into the target db */
    tdbs[0].dbname = thread0_dbname;         /* owned */
    tdbs[0].uri    = NULL;
    tdbs[0].db     = thread0_db;             /* owned */

    /* open per-thread dbs */
    const size_t orig_len = top->path->orig.len + 1 + DBNAME_LEN + 1 + UINT64_DIGITS; /* <path>/db.db.<thread id> */
    const size_t uri_len  = top->path->uri.len  + 1 + DBNAME_LEN + 1 + UINT64_DIGITS; /* <uri>/db.db.<thread id> */
    for(size_t i = 1; i < threads_used; i++) {
        tdbs[i].dbname = malloc(orig_len + 1);
        SNPRINTF(tdbs[i].dbname, orig_len + 1, "%s/" DBNAME ".%zu", top->path->orig.data, i);

        tdbs[i].uri = malloc(uri_len + 1);
        SNPRINTF(tdbs[i].uri,    uri_len + 1,  "%s/" DBNAME ".%zu", top->path->uri.data, i);

        tdbs[i].db = template_to_db(dbdb, tdbs[i].dbname,
                                    geteuid(), getegid(), NULL);
    }

    return tdbs;
}

static void tdbs_destroy(ThreadDB_t *tdbs, const size_t threads_used) {
    for(size_t i = 1; i < threads_used; i++) {
        closedb(tdbs[i].db);
        free(tdbs[i].uri);
        if (remove(tdbs[i].dbname) != 0) {
            const int err = errno;
            fprintf(stderr, "Warning: Could not remove \"%s\": %s (%d)\n",
                    tdbs[i].dbname, strerror(err), err);
        }
        free(tdbs[i].dbname);
    }

    closedb(tdbs[0].db);
    free(tdbs[0].dbname);
    free(tdbs);
}

static void Subtree_free(void *ptr) {
    Subtree_t *subtree = (Subtree_t *) ptr;
    free(subtree->se);
    if (subtree->tdbs) {
        tdbs_destroy(subtree->tdbs, subtree->threads_used);
    }
    free(subtree->remaining);
    if (subtree->mutex) {
        pthread_mutex_destroy(subtree->mutex);
        free(subtree->mutex);
    }
    free(subtree->root);
    free(subtree);
}

/* for totsubdirs and totfiles + totlinks */
static int get_size_t(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    return !((sscanf(data[0], "%zu", (size_t *) args) == 1));
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
    if (sqlite3_exec(db, sql, get_size_t, &entries, &err) != SQLITE_OK) {
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

/*
 * merge per-thread dbs into main db
 * tdbs[0] is db.db
 *
 * called when subtree has been processed
 */
static void rollup_merge(QPTPool_ctx_t *ctx, Subtree_t *subtree) {
    struct PoolArgs *pa = QPTPool_get_args_internal(ctx);
    const size_t id = QPTPool_get_id(ctx);

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

        free(subtree->tdbs);
        subtree->tdbs = NULL;
    }

    /* cleanup */

    struct start_end *se = subtree->se;
    subtree->se = NULL;
    const size_t threads_used = subtree->threads_used;

    Subtree_free(subtree);

    clock_gettime(CLOCK_MONOTONIC, &se->end);

    pthread_mutex_lock(&pa->mutex);
    pa->active_dbs -= threads_used;
    sll_push_back(&pa->stats[id].runtimes, se);
    pthread_cond_broadcast(&pa->cond);
    pthread_mutex_unlock(&pa->mutex);
}

static int copy_to_top(QPTPool_ctx_t *ctx, void *data);

/* get immediate subdirectories and enqueue them for copying to top */
static int ls_dirs(QPTPool_ctx_t *ctx,
                   Subdir_t *curr,
                   sll_t *subdirs) {
    struct PoolArgs *pa = QPTPool_get_args_internal(ctx);
    const size_t id = QPTPool_get_id(ctx);

    DIR *dir = opendir_wrapper(curr->path->orig.data, NULL);
    if (!dir) {
        pa->stats[id].not_rolledup++;
        return 1;
    }

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

        const size_t child_path_len = curr->path->orig.len + 1 + len;
        char *child_path = NULL;

        if ((entry->d_type == DT_DIR) ||
            (entry->d_type == DT_UNKNOWN)) {
            child_path = malloc(child_path_len + 1);
            SNFORMAT_S(child_path, child_path_len + 1, 3,
                       curr->path->orig.data, curr->path->orig.len,
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

        /* child must be a directory */

        Path_t *child = malloc(sizeof(*child));
        {
            child->orig.data = child_path;
            child->orig.len  = child_path_len;
            child->orig.free = free;

            const size_t uri_size = curr->path->uri.len + 1 + 3 * len + 1;
            child->uri.data  = malloc(uri_size);
            child->uri.len   = SNFORMAT_S(child->uri.data, uri_size, 2,
                                          curr->path->uri.data, curr->path->uri.len,
                                          "/", (size_t) 1);
            child->uri.free  = free;

            size_t used_len  = len;
            child->uri.len  += sqlite_uri_path(child->uri.data + child->uri.len, uri_size - child->uri.len,
                                               entry->d_name, &used_len);
            child->uri.data[child->uri.len] = '\0';
        }

        Subdir_t *subdir  = calloc(1, sizeof(*subdir));
        subdir->root      = curr->root;
        subdir->path      = child;
        subdir->subdirs   = curr->subdirs;

        sll_push_back(subdirs, subdir);
    }

    closedir(dir);
    return 0;
}

static int parallel_walk(QPTPool_ctx_t *ctx, void *data) {
    const size_t id = QPTPool_get_id(ctx);
    Subdir_t *curr = (Subdir_t *) data;

    /* get this directory's immediate subdirs */
    sll_t subdirs; /* Path_t * */
    sll_init(&subdirs);
    const int rc = ls_dirs(ctx, curr, &subdirs);

    /* queue up immediate subdirs for recursion */
    sll_loop(&subdirs, node) {
        QPTPool_enqueue(ctx, parallel_walk, sll_node_data(node));
    }

    /* push immediate subdirs into subdir list (take ownership) */
    sll_move_append(&curr->subdirs[id], &subdirs);
    sll_destroy(&subdirs, NULL);

    /* curr is not owned by this function, so don't free */

    return rc;
}

/*
 * copy current directory into a per-thread database
 *
 * if this was the last directory, merge the per-thread databases
 */
static int copy_to_top(QPTPool_ctx_t *ctx, void *data) {
    struct PoolArgs *pa = QPTPool_get_args_internal(ctx);

    Subtree_t *subtree = (Subtree_t *) data;

    int rc = 0;

    /* roll up one subdirectory into one of the root's per-thread db file */

    /* attach the subdirectory's db.db in READONLY mode to the per-thread database */
    char *child_dbname = rollup_child_attach(subtree->tdb->db, &subtree->path->uri, SQLITE_OPEN_READONLY);
    if (!child_dbname) {
        rc = 1;
        goto cleanup;
    }

    rexa_t rexa = {
        .xattr       = {
            .temp    = &pa->xattr_template,
            .mutex   = subtree->mutex,
        },
        .parent      = subtree->root->name,
        .parent_len  = subtree->root->name_len,
        .child       = subtree->path->orig.data,
        .child_len   = subtree->path->orig.len,
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
    if (rollup_child_process(subtree->tdb->db, rollup_one_subdir, &rexa) != 0) {
        rc = 1;
    }

    free(rollup_one_subdir);

    /* always detach */
    rollup_child_detach(subtree->tdb->db, child_dbname);

  cleanup:
    Path_free(subtree->path);
    subtree->path = NULL;

    pthread_mutex_lock(subtree->mutex);
    const size_t remaining = --(*subtree->remaining);
    pthread_mutex_unlock(subtree->mutex);

    if (remaining) {
        free(subtree); /* rollup_merge will free pointers */
    }
    else {
        rollup_merge(ctx, subtree);
    }

    return rc;
}

/* enqueue subtree for processing */
static int enqueue_rollup_copy(QPTPool_ctx_t *ctx, Subdir_t *top) {
    struct PoolArgs *pa = QPTPool_get_args_internal(ctx);

    /*
     * overwrite old start value because actual processing starts here
     *
     * previous value is only applicable when the subtree has 0
     * subdirectories and did not need further processing
     */

    struct start_end *se = calloc(1, sizeof(*se));
    clock_gettime(CLOCK_MONOTONIC, &se->start);

    const size_t dbname_len = top->root->name_len + 1 + DBNAME_LEN;
    char *dbname = malloc(dbname_len + 1);
    SNFORMAT_S(dbname, dbname_len + 1, 3,
               top->root->name, top->root->name_len,
               "/", (size_t) 1,
               DBNAME, DBNAME_LEN);

    sqlite3 *topdb = opendb(dbname, SQLITE_OPEN_READWRITE, 1, 0, NULL, NULL);
    if (!topdb) {
        pa->stats[pa->in.maxthreads].failures++;
        free(dbname);
        free(se);
        Subdir_free(top, pa->in.maxthreads);
        return 1;
    }

    /* clear out old rollup data (does not delete treesummary data) */
    str_t name = REFSTR(dbname, dbname_len);
    if (rollup_init(topdb, &name) != 0) {
        pa->stats[pa->in.maxthreads].failures++;
        closedb(topdb);
        free(dbname);
        free(se);
        Subdir_free(top, pa->in.maxthreads);
        return 1;
    }

    /* when this hits 0, merge per-thread staging dbs */
    size_t *subdir_count = calloc(1, sizeof(*subdir_count));
    for(size_t i = 0; i < pa->in.maxthreads; i++) {
        *subdir_count += sll_get_size(&top->subdirs[i]);
    }

    sll_push_back(&pa->stats[pa->in.maxthreads].subdirs, subdir_count);

    if (!*subdir_count) {
        closedb(topdb);
        free(dbname);
        Subdir_free(top, pa->in.maxthreads);
        clock_gettime(CLOCK_MONOTONIC, &se->end);
        sll_push_back(&pa->stats[pa->in.maxthreads].runtimes, se);
        return 0;
    }

    /* mutex for remaining */
    pthread_mutex_t *mutex = malloc(sizeof(*mutex));
    pthread_mutex_init(mutex, NULL);

    size_t *remaining = malloc(sizeof(*remaining));
    *remaining = *subdir_count;

    /* no need to open maxthreads dbs for < maxthreads directories */
    const size_t threads_used = min(*subdir_count, pa->in.maxthreads);

    /* ************************************************** */

    /* create per-thread staging dbs for threads to write to in parallel */
    ThreadDB_t *tdbs = tdbs_create(&pa->dbdb_template, top, threads_used, dbname, topdb);

    /* ************************************************** */

    pa->active_dbs += threads_used; /* locked by caller */

    /*
     * enqueue all subdirs from one thread, ensuring that each
     * thread only ever gets work for one db enqueued and thus
     * never races
     */
    sll_t *subdirs = top->subdirs;
    size_t db_offset = 0;
    for(size_t i = 0; i < pa->in.maxthreads; i++) {
        for(Subdir_t *subdir = sll_pop_front(&subdirs[i]);
            subdir; subdir = sll_pop_front(&subdirs[i])) {

            Subtree_t *subtree    = calloc(1, sizeof(*subtree));
            subtree->root         = top->root;
            subtree->path         = subdir->path;
            subtree->mutex        = mutex;
            subtree->remaining    = remaining;
            subtree->threads_used = threads_used;
            subtree->tdbs         = tdbs;
            subtree->tdb          = &tdbs[db_offset];
            subtree->se           = se;

            QPTPool_enqueue(ctx, copy_to_top, subtree);

            db_offset = (db_offset + 1) % threads_used;

            free(subdir);
        }

        sll_destroy(&subdirs[i], NULL);
    }

    top->root = NULL; /* used for copying upwards */
    Subdir_free(top, pa->in.maxthreads);

    return 0;
}

/* get both canrolledup and isrolled up */
static int get_rollup(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    int **rollup = (int **) args;
    return !((sscanf(data[0], "%d", rollup[0]) == 1) &&
             (sscanf(data[1], "%d", rollup[1]) == 1));
}

/* convert a directory work item into a subtree work item */
static int prepare_rollup_copy(QPTPool_ctx_t *ctx, void *data) {
    struct PoolArgs *pa = QPTPool_get_args_internal(ctx);
    const size_t id = QPTPool_get_id(ctx);

    Subdir_t *top = calloc(1, sizeof(*top));
    top->root = (struct work *) data;

    top->subdirs = calloc(pa->in.maxthreads, sizeof(*top->subdirs));
    for(size_t i = 0; i < pa->in.maxthreads; i++) {
        sll_init(&top->subdirs[i]);
    }

    {
        /* create path for walking this directory */
        top->path = malloc(sizeof(*top->path));
        top->path->orig = (str_t) REFSTR(top->root->name, top->root->name_len);
        str_alloc_existing(&top->path->uri, 3 * top->root->name_len);

        /* convert path to URI for SQLite 3 */
        size_t used_len = top->root->name_len;
        top->path->uri.len = sqlite_uri_path(top->path->uri.data, top->path->uri.len,
                                                 top->root->name, &used_len);
        top->path->uri.data[top->path->uri.len] = '\0'; /* need to NULL terminate because uri is used in SNPRINTF */

        /* top->path is freed after tdbs_create */
    }

    /* store the top of this subtree in the global list */
    sll_push_back(&pa->tops[id], top);

    /* walk top of subtree to get subdirs */
    return parallel_walk(ctx, top);
}

/*
 * descend until a rollup-able directory is found
 * then complete the tree walk to get a list of directories to copy into the top
 */
static int find_top(QPTPool_ctx_t *ctx, void *data) {
    struct PoolArgs *pa = QPTPool_get_args_internal(ctx);
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
            QPTPool_enqueue(ctx, prepare_rollup_copy, work);
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

    pa.tops = calloc(pa.in.maxthreads, sizeof(*pa.tops));
    if (!pa.tops) {
        fprintf(stderr, "Error: Could not allocate subtree root lists\n");
        rc = 1;
        goto close_xattr_template;
    }

    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        sll_init(&pa.tops[i]);
    }

    pa.stats = calloc(pa.in.maxthreads + 1, sizeof(*pa.stats));
    if (!pa.stats) {
        fprintf(stderr, "Error: Could not allocate stat counters\n");
        rc = 1;
        goto free_tops;
    }

    for(size_t i = 0; i < pa.in.maxthreads + 1; i++) {
        sll_init(&pa.stats[i].subdirs);
        sll_init(&pa.stats[i].runtimes);
    }

    pthread_mutex_init(&pa.mutex, NULL);
    pthread_cond_init(&pa.cond, NULL);
    pa.active_dbs = 0;

    QPTPool_ctx_t *pool = QPTPool_init(pa.in.maxthreads, &pa);
    QPTPool_set_steal(pool, 0, 0); /* work stealing is not allowed */
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
     * Subtrees are pushed into the thread pool for rolling up until
     * all threads are active (oversubscription is allowed). The
     * remaining subtrees are pushed in as subtrees exit the thread
     * pool.
     */
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        for(Subdir_t *top = sll_pop_front(&pa.tops[i]);
            top; top = sll_pop_front(&pa.tops[i])) {
            pthread_mutex_lock(&pa.mutex);

            /* wait until number of active copies drops */
            while (pa.active_dbs >= pa.in.maxthreads) { /* can oversubscribe */
                pthread_cond_wait(&pa.cond, &pa.mutex);
            }

            /* kick off subtree copying */
            rc |= enqueue_rollup_copy(pool, top);

            pthread_mutex_unlock(&pa.mutex);
        }
    }

    QPTPool_stop(pool);

    clock_gettime(CLOCK_MONOTONIC, &runtime.end);

    QPTPool_destroy(pool);

    /* cleanup */

    pthread_cond_destroy(&pa.cond);
    pthread_mutex_destroy(&pa.mutex);

    compute_stats(&pa);
    fprintf(stderr, "Took %.2Lf seconds\n", sec(nsec(&runtime)));

  free_stats:
    for(size_t i = 0; i < pa.in.maxthreads + 1; i++) {
        sll_destroy(&pa.stats[i].runtimes, free);
        sll_destroy(&pa.stats[i].subdirs, free);
    }
    free(pa.stats);

  free_tops:
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        sll_destroy(&pa.tops[i], NULL); /* each subtree list should be empty at this point */
    }
    free(pa.tops);

  close_xattr_template:
    close_template_db(&pa.xattr_template);

  close_dbdb_template:
    close_template_db(&pa.dbdb_template);

  cleanup:
    input_fini(&pa.in);

    return rc?EXIT_FAILURE:EXIT_SUCCESS;
}
