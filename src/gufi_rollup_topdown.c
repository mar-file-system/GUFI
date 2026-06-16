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
    struct template_db xattr_template;

    /* per-thread stats */
    struct {
        size_t not_rolledup;
        size_t top;
        size_t subdir;
    } *stats;
};

typedef struct Path {
    str_t orig; /* normal filesystem path for traversal */
    str_t uri;  /* uri for ATTACH-ing in SQLite 3 */
} Path_t;

void Path_free(void *ptr) {
    Path_t *path = (Path_t *) ptr;
    str_free_existing(&path->uri);
    str_free_existing(&path->orig);
    free(path);
}

/* put immeidate subdirs into a list */
static void ls_dirs(const Path_t *path,
                    DIR *dir, struct PoolArgs *pa, sll_t *subdirs) {
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
                              &stat_called, 1, 1) != 0) {
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

/*
 * roll up one subdirectory directly into the top directory
 *
 * @return -1 - error
 *          0 - success
*/
static int rollup_child(const Path_t *top_path,
                        const size_t basename_len,
                        sqlite3 *topdb,
                        const Path_t *child_path,
                        struct template_db *xattr_template) {
    /* Not checking arguments */

    /* process child */
    int rc = 0;

    char *child_dbname = rollup_child_attach(topdb, &child_path->uri, SQLITE_OPEN_READONLY);
    if (!child_dbname) {
        return -1;
    }

    size_t xattr_count = 0;
    rexa_t rexa = {
        .xattr       = xattr_template,
        .parent      = top_path->orig.data,
        .parent_len  = top_path->orig.len,
        .child       = child_path->orig.data,
        .child_len   = child_path->orig.len,
        .count       = &xattr_count,
    };

    /* get the correct path */
    const char *rolledup_path = child_path->orig.data + top_path->orig.len - basename_len;
    const size_t rolledup_path_len = child_path->orig.len - top_path->orig.len + basename_len;

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

    /* roll up the subdir into this dir */
    if (rollup_child_process(topdb, rollup_one_subdir, &rexa) != 0) {
        rc = -1;
    }

    free(rollup_one_subdir);

    /* always detach subdir */
    rollup_child_detach(topdb, child_dbname);

    return rc;
}

/*
 * roll up everything under the current directory into the current directory
 *
 * descent is done serially because writing into the target database
 * has to be serialized in any case
 *
 * return -1  topdir error
 *         0  success
 *         1+ # of subdir errors
 */
static int top_down_rollup(struct PoolArgs *pa, const size_t id,
                           struct work *work,
                           const char *topdb_name, DIR *topdir) {
    /* reopen the db in readwrite mode */
    sqlite3 *topdb = opendb(topdb_name, SQLITE_OPEN_READWRITE, 1, 0, NULL, NULL);
    if (!topdb) {
        return -1;
    }

    /* convert path to URI for SQLite 3 */
    Path_t *top_path = malloc(sizeof(*top_path));
    top_path->orig.data = work->name;
    top_path->orig.len = work->name_len;
    top_path->orig.free = NULL;

    const size_t uri_max_len = 3 * work->name_len;
    top_path->uri.data = malloc(uri_max_len + 1);
    top_path->uri.len = 0;
    top_path->uri.free = free;

    size_t used_len = work->name_len;
    top_path->uri.len = sqlite_uri_path(top_path->uri.data, uri_max_len,
                                        work->name, &used_len);

    /* clear out old rollup data */
    if (rollup_init(topdb, &top_path->orig) != 0) {
        closedb(topdb);
        Path_free(top_path);
        return -1;
    }

    int failures = 0;

    /* descend + copy */
    sll_t subdirs; /* Path_t * */
    sll_init(&subdirs);

    /* have to do this outside of loop to not copy self as rollup data */
    ls_dirs(top_path, topdir, pa, &subdirs);

    while (sll_get_size(&subdirs)) {
        Path_t *child_path = (Path_t *) sll_pop_front(&subdirs);
        pa->stats[id].subdir++;

        /* enqueue child directories */
        {
            DIR *dir = opendir_wrapper(child_path->orig.data, 1);
            if (!dir) {
                failures++;
                Path_free(child_path);
                continue;
            }

            ls_dirs(child_path, dir, pa, &subdirs);
            closedir(dir);
        }

        /* roll up this one subdirectory into the parent */
        if (rollup_child(top_path, work->basename_len,
                         topdb, child_path,
                         &pa->xattr_template) != 0) {
            failures++;
        }

        Path_free(child_path);
    }

    sll_destroy(&subdirs, Path_free);

    /* mark this directory as rolled up */
    char *err = NULL;
    if (sqlite3_exec(topdb, "UPDATE " SUMMARY " SET isrolledup = 1 WHERE isroot == 1;",
                     NULL, NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Could not mark %s: %s\n",
                                  work->name, err);
        /* fall through */
    }

    closedb(topdb);
    Path_free(top_path);

    return failures;
}

/* get number of entries in the tree */
static int get_entries(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    return !(sscanf(data[0], "%zu", (size_t *) args) == 1);
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
 *
 * then enqueue the function to do the actual rollup
 */
static int find_top(QPTPool_ctx_t *ctx, void *data) {
    struct PoolArgs *pa = NULL;
    QPTPool_get_args(ctx, (void **) &pa);

    struct work *work = (struct work *) data;

    DIR *dir = opendir_wrapper(work->name, 1);
    if (!dir) {
        goto free_work;
    }

    /* not deep enough - descend */
    if (work->level <= pa->in.min_level) {
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

        if (pa->in.rollup.entries_limit) {
            size_t entries = 0;
            if (sqlite3_exec(db,
                             "SELECT " TREESUMMARY ".totfiles + " TREESUMMARY ".totlinks "
                             "FROM " TREESUMMARY " JOIN " SUMMARY " ON " TREESUMMARY ".inode == " SUMMARY ".inode "
                             "WHERE " SUMMARY ".isroot == 1;",
                             get_entries, &entries, &err) != SQLITE_OK) {
                sqlite_print_err_and_free(err, stderr, "Error: Could not get entries count from \"%s\": %s\n",
                                          work->name, err);
                closedb(db);
                goto free_dbname;
            }

            /*
             * if rolling up into this directory would result in
             * too many entries, do not roll up this directory
             * and instead keep going down
             */
            if (entries > pa->in.rollup.entries_limit) {
                closedb(db);

                descend(ctx,
                        &pa->in, work,
                        dir, 1,
                        find_top, NULL, NULL,
                        NULL);

                goto free_dbname;
            }
        }

        int *arr[] = { &canrollup, &isrolledup };
        if (sqlite3_exec(db, "SELECT canrollup, isrolledup FROM " SUMMARY " WHERE isroot == 1;",
                         get_rollup, arr, &err) != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr, "Error: Could not get rollup data from \"%s\": %s\n",
                                      work->name, err);
            closedb(db);
            goto free_dbname;
        }

        closedb(db); /* readonly db is not needed any more */
    }

    const size_t id = QPTPool_get_id(ctx);

    /* found top - do rollup */
    if (canrollup == 1) {
        pa->stats[id].top++;

        if (pa->in.dry_run ||
            (isrolledup && pa->in.dont_reprocess)) {
            /* don't do anything */
        }
        else {
            top_down_rollup(pa, id, work, dbname, dir);
        }
    }
    else {
        pa->stats[id].not_rolledup++;

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

    init_template_db(&pa.xattr_template);
    if (create_xattrs_template(&pa.xattr_template, NULL) != 0) {
        fprintf(stderr, "Error: Could not create xattr template file\n");
        rc = 1;
        goto cleanup;
    }

    pa.stats = calloc(pa.in.maxthreads, sizeof(*pa.stats));
    if (!pa.stats) {
        fprintf(stderr, "Error: Could not allocate stat counters\n");
        rc = 1;
        goto free_stats;
    }

    QPTPool_ctx_t *pool = QPTPool_init(pa.in.maxthreads, &pa);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        rc = 1;
        goto free_stats;
    }

    struct start_end runtime;
    clock_gettime(CLOCK_MONOTONIC, &runtime.start);

    for(int i = 0; i < pa.in.pos.argc; i++) {
        const char *path = pa.in.pos.argv[i];
        struct work *work = new_work_with_name(NULL, 0, path, strlen(path));
        /* need actual basename length, not the entire length of the starting path */
        work->basename_len = work->name_len - dirname_len(work->name, work->name_len);
        QPTPool_enqueue(pool, find_top, work);
    }

    QPTPool_stop(pool);

    clock_gettime(CLOCK_MONOTONIC, &runtime.end);

    QPTPool_destroy(pool);

    /* cleanup */

    size_t not_rolledup = 0;
    size_t top = 0;
    size_t subdir = 0;
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        not_rolledup += pa.stats[i].not_rolledup;
        top += pa.stats[i].top;
        subdir += pa.stats[i].subdir;
    }

    fprintf(stderr, "%zu directories could not rollup\n", not_rolledup);
    fprintf(stderr, "Rolled up %zu subdirectories into %zu top-level directories\n", subdir, top);
    fprintf(stderr, "Total remaining directories to traverse: %zu\n", not_rolledup + top);
    fprintf(stderr, "Took %.2Lf seconds\n", sec(nsec(&runtime)));

  free_stats:
    free(pa.stats);

    close_template_db(&pa.xattr_template);

  cleanup:
    input_fini(&pa.in);

    return rc?EXIT_FAILURE:EXIT_SUCCESS;
}
