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
#include <fcntl.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

/* GUFI headers */
#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "bf.h"
#include "dbutils.h"
#include "debug.h"
#include "print.h"
#include "str.h"
#include "trie.h"
#include "utils.h"

/* stats headers */
#include "DirData.h"
#include "handle_columns.h"

#define OUTLIERS_TABLE  "outliers"
#define OUTLIERS_CREATE                                     \
    "DROP TABLE IF EXISTS " OUTLIERS_TABLE ";"              \
    "CREATE TABLE " OUTLIERS_TABLE " ("                     \
    "path TEXT, level INT64, "                              \
    "outlier_type TEXT, "                                   \
    "column TEXT, "                                         \
    "t_val DOUBLE, t_mean DOUBLE, t_stdev DOUBLE, "         \
    "s_val DOUBLE, s_mean DOUBLE, s_stdev DOUBLE "          \
    ");"
#define OUTLIERS_INSERT "INSERT INTO " OUTLIERS_TABLE " VALUES ("   \
    "@path, @level, "                                               \
    "@outlier_type, "                                               \
    "@column, "                                                     \
    "@t_val, @t_mean, @t_stdev, "                                   \
    "@s_val, @s_mean, @s_stdev "                                    \
    ");"

#define INTERMEDIATE_DBNAME_FORMAT "file:memory%zu?mode=memory&cache=shared" GUFI_SQLITE_VFS_URI

struct PoolArgs {
    struct input in;
    sqlite3 **dbs;   /* per-thread dbs */

    /* runtime stats */
    size_t *opendbs; /* per-thread number of attempted opendbs */
};

typedef struct {
    str_t path;
    size_t level;
    refstr_t col;
    const ColHandler_t *handler;  /* functions for handling the column type */
    const str_t *query;           /* SQL for pulling data from index; reference to allocation in main */
    int is_outlier;
    int reported;                 /* has this path already been reported? */

    Stats_t t;
    Stats_t s;
} OutlierWork_t;

static OutlierWork_t *OutlierWork_create(const str_t *path, const size_t level,
                                         const refstr_t col,
                                         const ColHandler_t *handler, const str_t *query,
                                         const int is_outlier, const int reported,
                                         const Stats_t *t, const Stats_t *s) {
    OutlierWork_t *ow = calloc(1, sizeof(OutlierWork_t));
    ow->path.data = malloc(path->len + 1);
    ow->path.len = SNFORMAT_S(ow->path.data, path->len + 1, 1,
                              path->data, path->len);

    ow->level = level;
    ow->col = col;
    ow->handler = handler;
    ow->query = query;
    ow->is_outlier = is_outlier;
    ow->reported = reported;
    ow->t = *t;
    ow->s = *s;

    return ow;
}

static void OutlierWork_free(OutlierWork_t *ow) {
    str_free_existing(&ow->path);
    free(ow);
}

static int is_subdir(const char *path, struct dirent *entry) {
    switch (entry->d_type) {
        case DT_DIR:
            return 1;
            break;
        case DT_LNK:
        case DT_REG:
        case DT_FIFO:
        case DT_SOCK:
        case DT_CHR:
        case DT_BLK:
            return 0;
            break;
        case DT_UNKNOWN:
        default:
            ;
            /* some filesystems don't support d_type - fall back to calling lstat */
            #if HAVE_STATX
            struct statx stx;
            if (statx(AT_FDCWD, path,
                      AT_SYMLINK_NOFOLLOW | AT_STATX_DONT_SYNC,
                      STATX_ALL, &stx) != 0) {
                const int err = errno;
                fprintf(stderr, "Error: Could not statx \"%s\": %s (%d)\n",
                        path, strerror(err), err);
                return -1;
            }

            return S_ISDIR(stx.stx_mode);
            #else
            struct stat st;
            if (lstat(path, &st) != 0) {
                const int err = errno;
                fprintf(stderr, "Error: Could not lstat \"%s\": %s (%d)\n",
                        path, strerror(err), err);
                return -1;
            }

            return S_ISDIR(st.st_mode);
            #endif
            break;
    }

    return 0; /* shoud never get here */
}

/* go down one level */
static void get_subdirs(OutlierWork_t *ow, sll_t *subdirs, size_t *opendbs) {
    DIR *dir = opendir(ow->path.data);
    if (!dir) {
        const int err = errno;
        fprintf(stderr, "Error: Could not open directory \"%s\": %s (%d)\n", ow->path.data, strerror(err), err);
        return;
    }

    sll_init(subdirs);

    struct dirent *entry = NULL;
    while ((entry = readdir(dir))) {
        const size_t len = strlen(entry->d_name);

        const int skip = (
            /* skip . and .. ; not using skip_names */
            ((len == 1) && (strncmp(entry->d_name, ".",  1) == 0)) ||
            ((len == 2) && (strncmp(entry->d_name, "..", 2) == 0)) ||
            /* skip *.db */
            ((len >= 3) && (strncmp(entry->d_name + len - 3, ".db", 3) == 0))
            );

        if (skip) {
            continue;
        }

        struct DirData *dd = DirData_create(&ow->path, entry->d_name, len);
        if (is_subdir(dd->path.data, entry) != 1) {
            DirData_free(dd);
            continue;
        }

        /* create the db.db path */
        const size_t dbname_len = dd->path.len + 1 + DBNAME_LEN;
        char *dbname = malloc(dbname_len + 1);
        SNFORMAT_S(dbname, dbname_len + 1, 3,
                   dd->path.data, dd->path.len,
                   "/", (size_t) 1,
                   DBNAME, DBNAME_LEN);

        (*opendbs)++;

        /* the db must exist or else it is not included */
        sqlite3 *db = opendb(dbname, SQLITE_OPEN_READONLY, 0, 0, NULL, NULL);
        if (!db) {
            free(dbname);
            DirData_free(dd);
            continue;
        }

        /* get data from db */
        char *err = NULL;
        if (sqlite3_exec(db, ow->query->data, ow->handler->sqlite_callback, dd, &err) == SQLITE_OK) {
            /* save this subdir */
            sll_push(subdirs, dd);
        }
        else {
            fprintf(stderr, "Error: Could not get column from \"%s\": \"%s\": %s\n", dbname, ow->query->data, err);
            sqlite3_free(err);
            DirData_free(dd);
        }

        free(dbname);

        closedb(db);
    }

    closedir(dir);
}

static int insert_outlier(sqlite3_stmt *stmt, const str_t *path, const size_t level,
                          const char *outlier_type, const refstr_t *col, const Stats_t *t, const Stats_t *s) {
    sqlite3_bind_text(stmt,    1, path->data,   path->len, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt,   2, level);

    sqlite3_bind_text(stmt,    3, outlier_type, -1,        SQLITE_STATIC); /* static value */
    sqlite3_bind_text(stmt,    4, col->data,    col->len,  SQLITE_STATIC); /* reference to argv[i] */

    sqlite3_bind_double(stmt,  5, t->value);
    sqlite3_bind_double(stmt,  6, t->mean);
    sqlite3_bind_double(stmt,  7, t->stdev);

    sqlite3_bind_double(stmt,  8, s->value);
    sqlite3_bind_double(stmt,  9, s->mean);
    sqlite3_bind_double(stmt, 10, s->stdev);

    const int ret = sqlite3_step(stmt);

    sqlite3_reset(stmt);

    if (ret == SQLITE_DONE) {
        sqlite3_clear_bindings(stmt); /* NULL will cause invalid read here */
    }
    else {
        sqlite_print_err_and_free(NULL, stderr, "SQL insert outlier step: %s error %d err %s\n",
                                  path->data, ret, sqlite3_errstr(ret));
        return 1;
    }

    return 0;
}

/*
 * reasons to arrive at current node
 *     root - no choice / current level had no outliers
 *     contains an outlier
 *     parent found no outliers
 *
 * subdirectories
 *     0
 *     some outliers
 *     no outliers
 */
static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    struct PoolArgs *pa = (struct PoolArgs *) args;
    OutlierWork_t *ow = (OutlierWork_t *) data;

    int rc = 0;

    sll_t subdirs;
    get_subdirs(ow, &subdirs, &pa->opendbs[id]);

    const uint64_t n = sll_get_size(&subdirs);

    /* nothing under here */
    if (n == 0) {
        /* if this directory was an outlier, print this directory */
        if (ow->is_outlier && !ow->reported) {
            sqlite3_stmt *stmt = insertdbprep(pa->dbs[id], OUTLIERS_INSERT);
            insert_outlier(stmt, &ow->path, ow->level,
                           "no-subdirs", &ow->col, &ow->t, &ow->s);
            insertdbfin(stmt);
        }
        sll_destroy(&subdirs, NULL);
    }
    else {
        sll_t *work = NULL;

        sll_t outliers;
        sll_init(&outliers);

        const size_t next_level = ow->level + 1;

        if (n == 1) {
            /* not enough samples to get standard deviation, so just go down a level */
            work = &subdirs;
        }
        else {
            /* compute mean and standard deviation */
            double t_mean = 0, t_stdev = 0;
            double s_mean = 0, s_stdev = 0;
            ow->handler->compute_mean_stdev(&subdirs,
                                            &t_mean, &t_stdev,
                                            &s_mean, &s_stdev);

            /* get 3 sigma values */
            const double t_limit = 3 * t_stdev;
            const double t_low   = t_mean - t_limit;
            const double t_high  = t_mean + t_limit;

            const double s_limit = 3 * s_stdev;
            const double s_low   = s_mean - s_limit;
            const double s_high  = s_mean + s_limit;

            sqlite3_stmt *stmt   = insertdbprep(pa->dbs[id], OUTLIERS_INSERT);

            /* find outliers */
            sll_loop(&subdirs, node) {
                DirData_t *dd = (DirData_t *) sll_node_data(node);
                dd->t.mean   = t_mean;
                dd->t.stdev  = t_stdev;
                dd->s.mean   = s_mean;
                dd->s.stdev  = s_stdev;

                /* if the subtree is an outlier, queue it up for procesing */
                if ((dd->t.value < t_low) || (t_high < dd->t.value)) {
                    sll_push(&outliers, dd);
                }

                /* if the subdirectory is an outlier, report it now */
                if ((dd->s.value < s_low) || (s_high < dd->s.value)) {
                    insert_outlier(stmt, &dd->path, next_level,
                                   "directory", &ow->col, &dd->t, &dd->s);
                    dd->reported = 1;
                }
            }

            insertdbfin(stmt);

            /*
             * if there is at least one subtree outlier, process only outliers
             * otherwise, process all subdirs/subtrees
             */
            work = (sll_get_size(&outliers) == 0)?&subdirs:&outliers;
        }

        const int sub_outlier = (work == &outliers); /* all outliers or all not outliers */

        /*
         * current directory is the outlier because the subtree
         * contains an outlier but none of the subdirectory
         * subtrees have outliers
         */
        if (ow->is_outlier && !sub_outlier) {
            if (!ow->reported) {
                sqlite3_stmt *stmt = insertdbprep(pa->dbs[id], OUTLIERS_INSERT);
                insert_outlier(stmt, &ow->path, ow->level,
                               "subdirs-not-outliers", &ow->col, &ow->t, &ow->s);
                insertdbfin(stmt);
            }
        }
        /* current subtree is NOT an outlier: descend with subdirs/outliers */
        else {
            sll_loop(work, node) {
                DirData_t *dd = (DirData_t *) sll_node_data(node);
                OutlierWork_t *new_ow = OutlierWork_create(&dd->path, next_level,
                                                           ow->col,
                                                           ow->handler, ow->query,
                                                           sub_outlier, dd->reported,
                                                           &dd->t, &dd->s);

                QPTPool_enqueue(ctx, id, processdir, new_ow);
            }
        }

        sll_destroy(&outliers, NULL); /* references to subdirs, so don't clean up */
        sll_destroy(&subdirs, DirData_free);
    }

    OutlierWork_free(ow);

    return rc;
}

static void intermediate_dbs_fini(struct PoolArgs *pa) {
    for(size_t i = 0; i < pa->in.maxthreads; i++) {
        sqlite3_close(pa->dbs[i]);
    }
    free(pa->dbs);
}

static sqlite3 *db_init(const char *dbname, const char *sql) {
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(dbname, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE| SQLITE_OPEN_URI,
                        GUFI_SQLITE_VFS) != SQLITE_OK) {
        sqlite_print_err_and_free(NULL, stderr, "Cannot open per-thread database\n");
        sqlite3_close(db); /* close db even if it didn't open to avoid memory leaks */
        return NULL;
    }

    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Cannot initialize per-thread database: %s\n",
                                  err);
        sqlite3_close(db); /* close db even if it didn't open to avoid memory leaks */
        return NULL;
    }

    return db;
}

static int intermediate_dbs_init(struct PoolArgs *pa) {
    pa->dbs = calloc(pa->in.maxthreads, sizeof(sqlite3 *));
    if (!pa->dbs) {
        const int err = errno;
        fprintf(stderr, "Error: Could not allocate space for %zu db pointers: %s (%d)\n",
                pa->in.maxthreads, strerror(err), err);
        return -1;
    }

    for(size_t i = 0; i < pa->in.maxthreads; i++) {
        char dbname[MAXSQL];
        SNPRINTF(dbname, sizeof(dbname), INTERMEDIATE_DBNAME_FORMAT, i);

        pa->dbs[i] = db_init(dbname, OUTLIERS_CREATE);
        if (!pa->dbs[i]) {
            intermediate_dbs_fini(pa);
            return -1;
        }
    }

    return 0;
}

static int print_uncached(void *args, int count, char **data, char **columns) {
    (void) columns;

    PrintArgs_t *pa = (PrintArgs_t *) args;

    count--;

    for(int i = 0; i < count; i++ ) {
        fprintf(pa->outfile, "%s%c", data[i], pa->delim);
    }

    fprintf(pa->outfile, "%s\n", data[count]);

    pa->rows++;

    return 0;
}

static int num_rows(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    return !(sscanf(data[0], "%zu", (size_t *) args) == 1);
}

static void write_results(struct PoolArgs *pa, size_t *rows) {
    *rows = 0;

    if (pa->in.output == OUTDB) {
        #define attachname "intermediate"

        /* combine results into one database file */
        sqlite3 *db = db_init(pa->in.outname.data, OUTLIERS_CREATE);
        for(size_t i = 0; i < pa->in.maxthreads; i++) {
            char dbname[MAXSQL];
            SNPRINTF(dbname, sizeof(dbname), INTERMEDIATE_DBNAME_FORMAT, i);

            if (!attachdb_raw(dbname, db, attachname, 1)) {
                continue;
            }

            char *err = NULL;
            if (sqlite3_exec(db, "INSERT INTO " OUTLIERS_TABLE " SELECT * FROM " attachname "." OUTLIERS_TABLE ";",
                             NULL, NULL, &err) != SQLITE_OK) {
                sqlite_print_err_and_free(err, stderr, "Error: Could not move intermediate results into final database: %s\n",
                                          err);
            }

            detachdb(dbname, db, attachname, 1);
        }

        char *err = NULL;
        if (sqlite3_exec(db, "SELECT COUNT(*) FROM " OUTLIERS_TABLE ";",
                         num_rows, rows, &err) != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr, "Error: Could not get number of rows in final database: %s\n",
                                      err);
        }

        sqlite3_close(db);
    }
    else {
        /* just print per-thread results */
        PrintArgs_t print = {
            .delim = pa->in.delim,
            .outfile = stdout,
            .rows = 0,
        };

        for(size_t i = 0; i < pa->in.maxthreads; i++) {
            char *err = NULL;
            if (sqlite3_exec(pa->dbs[i], "SELECT * FROM " OUTLIERS_TABLE ";",
                             print_uncached, &print, &err) != SQLITE_OK) {
                fprintf(stderr, "Error: Could not print intermediate results: %s\n", err);
                sqlite3_free(err);
            }
        }

        *rows = print.rows;
    }
}

static void sub_help(void) {
   printf("input_dir         walk this tree to find directories with outliers\n");
   printf("column            column to look at\n");
   printf("\n");
   printf("Make sure every directory under the starting path has a db.db file.\n");
   printf("Make sure treesummary tables have been generated for every directory.\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    struct PoolArgs pa;
    process_args_and_maybe_exit("hHvn:d:O:", 2, "input_dir column ...", &pa.in);

    int rc = EXIT_SUCCESS;

    if ((argc - idx) & 1) {
        fprintf(stderr, "Error: Input should be pairs of paths and columns\n");
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    if (intermediate_dbs_init(&pa) != 0) {
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    QPTPool_t *pool = QPTPool_init(pa.in.maxthreads, &pa);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        rc = EXIT_FAILURE;
        goto cleanup_dbs;
    }

    pa.opendbs = calloc(pa.in.maxthreads, sizeof(size_t));

    /* set up known column names to function mappings */
    trie_t *col_funcs = setup_column_functions();

    const size_t pairs = (argc - idx) / 2;

    sll_t queries;
    sll_init(&queries);

    Stats_t root_stats = {
        .value = NAN,
        .mean  = NAN,
        .stdev = NAN,
    };

    struct start_end after_init;
    clock_gettime(CLOCK_MONOTONIC, &after_init.start);

    for(size_t i = 0; i < pairs; i++) {
        const size_t j = 2 * i + idx;
        const char *path = argv[j];
        const char *user_col = argv[j + 1];
        const size_t user_col_len = strlen(user_col);

        struct stat st;
        if (stat(path, &st) != 0) {
            const int err = errno;
            fprintf(stderr, "Error: Could not stat root directory %s: %s (%d)\n",
                    path, strerror(err), err);
            continue;
        }

        const refstr_t user_cols[] = {
            { user_col,  user_col_len, },
            { NULL,      0,            },
        };

        /* check if processing multiple columns */
        const refstr_t *groups = handle_group(user_col, user_col_len);
        if (!groups) {
            groups = user_cols;
        }

        size_t g = 0;
        while (groups[g].data) {
            const refstr_t c = groups[g++];

            /* find column type handler */
            ColHandler_t *handler = NULL;
            if (trie_search(col_funcs, c.data, c.len, (void **) &handler) != 1) {
                fprintf(stderr, "Error: Could not find handler for column %s\n", c.data);
                continue;
            }

            /* this is actually a reference */
            str_t p = {
                .data = (char *) path,
                .len = strlen(path),
            };

            str_t *query = calloc(1, sizeof(*query));
            sll_push(&queries, query);

            handler->gen_sql(query, c.data);
            char *data = realloc(query->data, query->len + 1);
            if (data) {
                query->data = data;
            }

            OutlierWork_t *ow = OutlierWork_create(&p, 0, c, handler, query,
                                                   0,  /*
                                                        * starting path cannot be an outlier because
                                                        * there is only one directory at that level
                                                        * (even if the path is not root and has
                                                        * siblings, we do not know that)
                                                        */
                                                   0, &root_stats, &root_stats);
            QPTPool_enqueue(pool, i % pa.in.maxthreads, processdir, ow);
        }
    }

    QPTPool_stop(pool);

    /* write results to stdout or a single database file */
    size_t rows = 0;
    write_results(&pa, &rows);

    clock_gettime(CLOCK_MONOTONIC, &after_init.end);
    const long double processtime = sec(nsec(&after_init));

    const uint64_t thread_count = QPTPool_threads_completed(pool);

    size_t opendbs = 0;
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        opendbs += pa.opendbs[i];
    }

    fprintf(stderr, "Ran %" PRIu64 " threads\n", thread_count);
    fprintf(stderr, "Ran opendb %zu times\n", opendbs);
    fprintf(stderr, "Found %zu outliers\n", rows);
    fprintf(stderr, "Time Spent Processing: %.2Lfs\n", processtime);

    /* cleanup */
    sll_destroy(&queries, (void (*)(void *)) str_free);
    free(pa.opendbs);
    trie_free(col_funcs);
    QPTPool_destroy(pool);

  cleanup_dbs:
    intermediate_dbs_fini(&pa);

  cleanup:
    input_fini(&pa.in);

    dump_memory_usage();

    return rc;
}
