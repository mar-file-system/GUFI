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
#include <sys/types.h>
#include <unistd.h>

#include "BottomUp.h"
#include "SinglyLinkedList.h"
#include "bf.h"
#include "dbutils.h"
#include "debug.h"
#include "external_attach.h"
#include "rollup.h"
#include "template_db.h"
#include "utils.h"

/* statistics stored when processing each directory */
/* this is the type stored in the RollUpStats sll_t variables */
struct DirStats {
    char path[MAXPATH];

    uint64_t level;           /* level this directory is at */

    size_t subdir_count;      /* unrolled up count from readdir, not summary */
    size_t subnondir_count;   /* rolled up count from entries and subdir.pentries */

    size_t too_many_before;   /* current directory is too big to roll up */
    size_t too_many_after;    /* rolling up would result in too many rows in pentries */
    int score;                /* roll up score regardless of success or failure */
    int success;              /* whether or not the roll up succeeded */
};

/* per thread stats */
struct RollUpStats {
    sll_t not_processed;
    sll_t not_rolled_up;
    sll_t rolled_up;

    uint64_t remaining;       /* children that remain after rolling up */
};

struct PoolArgs {
    struct input in;
    struct template_db xattr_template;
    struct RollUpStats *stats;
};

/* compare function for qsort */
int compare_size_t(const void *lhs, const void *rhs) {
    return (*(size_t *) lhs - *(size_t *) rhs);
}

#define sll_dir_stats(name, all, var, width)                                     \
do {                                                                             \
    const size_t count = sll_get_size(all);                                      \
    if (count == 0) {                                                            \
        break;                                                                   \
    }                                                                            \
                                                                                 \
    size_t *array = malloc(count * sizeof(size_t));                              \
    size_t min = (size_t) -1;                                                    \
    size_t max = 0;                                                              \
    size_t sum = 0;                                                              \
    size_t i = 0;                                                                \
    sll_loop(all, node) {                                                        \
        struct DirStats *ds = (struct DirStats *) sll_node_data(node);           \
        if (ds->var < min) {                                                     \
            min = ds->var;                                                       \
        }                                                                        \
                                                                                 \
        if (ds->var > max) {                                                     \
            max = ds->var;                                                       \
        }                                                                        \
                                                                                 \
        sum += ds->var;                                                          \
        array[i++] = ds->var;                                                    \
    }                                                                            \
                                                                                 \
    qsort(array, count, sizeof(size_t), compare_size_t);                         \
    const size_t half = count / 2;                                               \
    double median = array[half];                                                 \
    if (count % 2 == 0) {                                                        \
        median += array[half - 1];                                               \
        median /= 2;                                                             \
    }                                                                            \
                                                                                 \
    const double avg = ((double) sum) / count;                                   \
                                                                                 \
    fprintf(stdout, "    %s:\n", name);                                          \
    fprintf(stdout, "        min:        %" #width "zu\n",  min);                \
    fprintf(stdout, "        max:        %" #width "zu\n",  max);                \
    fprintf(stdout, "        median:     %" #width ".2f\n", median);             \
    fprintf(stdout, "        sum:        %" #width "zu\n",  sum);                \
    fprintf(stdout, "        average:    %" #width ".2f\n",  avg);               \
                                                                                 \
    free(array);                                                                 \
} while (0)

#define print_too_many(name, all, var, width)                                    \
do {                                                                             \
    const size_t count = sll_get_size(all);                                      \
    if (count == 0) {                                                            \
        break;                                                                   \
    }                                                                            \
                                                                                 \
    size_t *array = malloc(count * sizeof(size_t));                              \
    size_t min = (size_t) -1;                                                    \
    size_t max = 0;                                                              \
    size_t sum = 0;                                                              \
    size_t i = 0;                                                                \
    sll_loop(all, node) {                                                        \
        struct DirStats *ds = (struct DirStats *) sll_node_data(node);           \
        if (ds->var == 0) {                                                      \
            continue;                                                            \
        }                                                                        \
                                                                                 \
        if (ds->var < min) {                                                     \
            min = ds->var;                                                       \
        }                                                                        \
                                                                                 \
        if (ds->var > max) {                                                     \
            max = ds->var;                                                       \
        }                                                                        \
                                                                                 \
        sum += ds->var;                                                          \
        array[i++] = ds->var;                                                    \
    }                                                                            \
                                                                                 \
    fprintf(stdout, "    %s %9zu\n", name, i);                                   \
    if (i) {                                                                     \
        qsort(array, i, sizeof(size_t), compare_size_t);                         \
        const size_t half = i / 2;                                               \
        double median = array[half];                                             \
        if (i % 2 == 0) {                                                        \
            median += array[half - 1];                                           \
            median /= 2;                                                         \
        }                                                                        \
                                                                                 \
        const double avg = ((double) sum) / i;                                   \
                                                                                 \
        fprintf(stdout, "        min:        %" #width "zu\n",  min);            \
        fprintf(stdout, "        max:        %" #width "zu\n",  max);            \
        fprintf(stdout, "        median:     %" #width ".2f\n", median);         \
        fprintf(stdout, "        sum:        %" #width "zu\n",  sum);            \
        fprintf(stdout, "        average:    %" #width ".2f\n", avg);            \
    }                                                                            \
                                                                                 \
    free(array);                                                                 \
} while (0)

static void print_stanza(const char *name, sll_t *stats) {
    fprintf(stdout, "%s %*" PRIu64 "\n", name, (int) (29 - strlen(name)), sll_get_size(stats));
    sll_dir_stats("Subdirectories", stats, subdir_count,    10);
    sll_dir_stats("Files/Links",    stats, subnondir_count, 10);
    sll_dir_stats("Level",          stats, level,           10);
}

/* this function moves the sll stats up and deallocates them */
static void print_stats(char **paths, const int path_count,
                        struct input *in, struct RollUpStats *stats) {
    fprintf(stdout, "Roots:\n");
    for(int i = 0; i < path_count; i++) {
        fprintf(stdout, "    %s\n", paths[i]);
    }
    fprintf(stdout, "\n");
    fprintf(stdout, "Thread Pool Size: %12zu\n",  in->maxthreads);
    fprintf(stdout, "Files/Links Limit: %11zu\n", in->rollup.entries_limit);
    fprintf(stdout, "\n");

    /* per-thread stats together */
    sll_t not_processed;
    sll_init(&not_processed);

    sll_t not_rolled_up;
    sll_init(&not_rolled_up);

    sll_t rolled_up;
    sll_init(&rolled_up);

    size_t remaining = 0;

    for(size_t i = 0; i < in->maxthreads; i++) {
        sll_move_append(&not_processed, &stats[i].not_processed);
        sll_move_append(&not_rolled_up, &stats[i].not_rolled_up);
        sll_move_append(&rolled_up,     &stats[i].rolled_up);
        remaining += stats[i].remaining;
    }

    /* get distribution of roll up scores */
    size_t successful = 0;
    size_t failed = 0;
    sll_loop(&rolled_up, node) {
        struct DirStats *ds = (struct DirStats *) sll_node_data(node);
        if (ds->success) {
            successful++;
        }
        else {
            failed++;
        }
    }

    /* count empty directories */

    /* do not count not_processed, since they do not have databases to scan */
    size_t not_processed_nondirs = 0;
    size_t not_processed_empty = 0;

    size_t not_rolled_up_nondirs = 0;
    size_t not_rolled_up_empty = 0;
    sll_loop(&not_rolled_up, node) {
        struct DirStats *ds = (struct DirStats *) sll_node_data(node);
        not_rolled_up_nondirs += ds->subnondir_count;
        not_rolled_up_empty += !ds->subnondir_count;
    }

    size_t rolled_up_nondirs = 0;
    size_t rolled_up_empty = 0;
    sll_loop(&rolled_up, node) {
        struct DirStats *ds = (struct DirStats *) sll_node_data(node);
        rolled_up_nondirs += ds->subnondir_count;
        rolled_up_empty += !ds->subnondir_count;
    }

    const size_t total_nondirs = not_processed_nondirs +
                                 not_rolled_up_nondirs +
                                 rolled_up_nondirs;

    const size_t empty = not_processed_empty +
                         not_rolled_up_empty +
                         rolled_up_empty;

    const size_t total_dirs = sll_get_size(&not_processed) +
                              sll_get_size(&not_rolled_up) +
                              sll_get_size(&rolled_up);

    /* print stats of each type of directory */
    print_stanza("Not Processed:",   &not_processed);
    print_stanza("Cannot Roll Up:",  &not_rolled_up);
    print_too_many("Too Many Before:", &not_rolled_up, too_many_before, 10);
    print_too_many("Too Many After: ", &not_rolled_up, too_many_after,  10);
    print_stanza("Can Roll Up:",     &rolled_up);
    fprintf(stdout, "    Successful: %14zu\n", successful);
    fprintf(stdout, "    Failed:     %14zu\n", failed);
    fprintf(stdout, "Files/Links:    %zu\n", total_nondirs);
    fprintf(stdout, "Directories:    %zu (%zu empty)\n", total_dirs, empty);
    fprintf(stdout, "Total:          %zu\n", total_nondirs + total_dirs);
    fprintf(stdout, "Remaining Dirs: %zu (%.2Lf%%)\n", remaining, (100 * (long double) remaining) / total_dirs);
    sll_destroy(&rolled_up, free);
    sll_destroy(&not_rolled_up, free);
    sll_destroy(&not_processed, free);
}

/* main data being passed around during walk */
struct RollUp {
    struct BottomUp data;
    int rolledup;
};

static int rollup_descend(void *args, int *keep_going) {
    struct RollUp *dir = (struct RollUp *) args;

    char dbname[MAXPATH];
    SNFORMAT_S(dbname, sizeof(dbname), 2,
               dir->data.name, dir->data.name_len,
               "/" DBNAME, DBNAME_LEN + 1);

    sqlite3 *db = opendb(dbname, SQLITE_OPEN_READONLY, 1, 0, NULL, NULL);

    int rc = !db;
    if (db) {
        /* check if the current directory is rolled up - if it is, don't descend */
        if (get_isrolledup(db, &dir->rolledup) == 0) {
            if (dir->rolledup) {
                *keep_going = 0;
            }
        }
        else {
            rc = 1;
        }
    }

    closedb(db);

    return rc;
}

/* ************************************** */
/* get permissions from directory entries */
const char PERM_SQL[] = "SELECT mode, uid, gid, (SELECT COUNT(*) FROM pentries) "
                        "FROM " SUMMARY " WHERE isroot == 1;";

struct ChildData {
    struct Permissions *perms;
    size_t count;
};

static int get_permissions_and_count(void *args, int count, char **data, char **columns) {
    struct ChildData *child = (struct ChildData *) args;
    const int rc = get_permissions_callback(child->perms, count, data, columns);
    if (rc != 0) {
        return rc;
    }
    return !(sscanf(data[3], "%zu", &child->count) == 1);
}

static int add_entries_count(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    size_t *total = (size_t *) args;

    /* handle index root, which does not have summary data */
    if (!data[0]) {
        return 0;
    }

    if (sscanf(data[0], "%zu", total) != 1) {
        return 1;
    }

    return 0;
}

/* get number of non-dirs in this directory (including rolled up non-dirs) */
static int get_nondirs(const char *name, sqlite3 *dst, size_t *subnondir_count) {
    *subnondir_count = 0;

    /* reduce number of times add_entries_count is called to 1 per directory by using SUM() */
    char *err = NULL;
    const int exec_rc = sqlite3_exec(dst, "SELECT SUM(totfiles + totlinks) FROM summary;",
                                     add_entries_count, subnondir_count, &err);
    if (exec_rc != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Warning: Failed to get non-directory count from \"%s\": %s\n",
                                  name, err);
    }
    return exec_rc;
}

/*
 * Check if a directory can roll up all of its children without violating permissions
 *
 *  0 children -> automatically can roll up since there are no possible conflicts
 * >0 children -> compare each child's permissions with this directory's permissions
 *
 * Previously returned 1 - 5 to indicate which rollup rule was
 * followed because all children had to follow the same rule (other
 * than leaves). Now only returns 1 because rollup can occur so long
 * as every child meets any of the rules.
 *
 * @return -1 - failed to open a database
 *          0 - do not roll up
 *          1 - all permissions pass
 */
int can_rollup(sqlite3 *dst, struct RollUp *rollup,
               size_t *total_child_entries) { /* extra data to grab while getting permissions */
    const size_t child_count = sll_get_size(&rollup->data.subdirs);

    /* no children -> can roll up */
    if (child_count == 0) {
        return 1;
    }

    {
        /*
         * get how many children were rolled up using in-memory
         * values memory instead of reading from databases
         */
        size_t rolledup = 0;
        sll_loop(&rollup->data.subdirs, node) {
            struct RollUp *child = (struct RollUp *) sll_node_data(node);
            rolledup += child->rolledup;
        }

        if (child_count != rolledup) {
            return 0;
        }
    }

    /* get permissions of the current directory */
    struct Permissions perms = {0};
    if (get_permissions(dst, rollup->data.name, &perms) != 0) {
        return -1;
    }

    struct Permissions *child_perms =
        malloc(sizeof(struct Permissions) * sll_get_size(&rollup->data.subdirs));

    /* get permissions of each child (not using values from bu->st) */
    size_t idx = 0;
    sll_loop(&rollup->data.subdirs, node) {
        struct BottomUp *child = (struct BottomUp *) sll_node_data(node);

        char dbname[MAXPATH] = {0};
        SNFORMAT_S(dbname, MAXPATH, 3,
                   child->name, child->name_len,
                   "/", (size_t) 1,
                   DBNAME, DBNAME_LEN);

        sqlite3 *db = opendb(dbname, SQLITE_OPEN_READONLY, 1, 0, NULL, NULL);
        if (!db) {
            break;
        }

        /* get the child directory's permissions and pentries count */
        struct ChildData data;
        data.perms = &child_perms[idx];
        data.count = 0;
        char *err = NULL;
        const int rc = sqlite3_exec(db, PERM_SQL, get_permissions_and_count, &data, &err);

        closedb(db);

        if (rc != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr, "Error: Could not get permissions of child directory \"%s\": %s\n",
                                      child->name, err);
            break;
        }

        /* rolled up size is checked by caller */
        *total_child_entries += data.count;

        idx++;
    }

    /* not every child has good permission */
    if (child_count != idx) {
        free(child_perms);
        return -1;
    }

    size_t legal = 0;
    for(size_t i = 0; i < child_count; i++) {
        legal += check_pair_permissions(&perms, &child_perms[i]);
    }

    free(child_perms);

    /* need all children to have good permissions */
    return (legal == child_count);
}
/* ************************************** */

/*
 * Just because a directory can roll up without violating permissions
 * does not mean it should
 *
 * @return   0 - should NOT rollup
 *           1 - should rollup
 */
static int should_rollup(struct input *in,
                         struct RollUp *rollup, struct DirStats *ds,
                         sqlite3 *dst) {
    /* Not checking arguments */

    /* not deep enough */
    if (rollup->data.level < in->min_level) {
        return 0;
    }

    /* get count of number of non-directories in the current directory */
    get_nondirs(rollup->data.name, dst, &ds->subnondir_count);

    /* the current directory has too many immediate files/links, don't roll up */
    if (in->rollup.entries_limit && (ds->subnondir_count > in->rollup.entries_limit)) {
        ds->too_many_before = ds->subnondir_count;
        return 0;
    }

    /* check if the permissions of this directory and its children match */
    size_t total_child_entries = 0;
    const int legal = can_rollup(dst, rollup, &total_child_entries);

    /*
     * even if this directory can be rolled up, don't if
     * doing so would result in too many rows in pentries
     */
    if (legal == 1) {
        const size_t total_pentries = ds->subnondir_count + total_child_entries;
        if (in->rollup.entries_limit && (total_pentries > in->rollup.entries_limit)) {
            ds->too_many_after = total_pentries;
            return 0;
        }
    }

    return legal;
}

/* if subdirectory is not keeping rollup data, delete it */
static const char DELETE_SUBDIR_ROLLUP[] =
    /* remove rolled up directories */
    "DELETE FROM " ROLLUP_SUBDIR_ATTACH_NAME "." SUMMARY " WHERE isroot != 1;"

    /* unset isrolledup */
    "UPDATE "      ROLLUP_SUBDIR_ATTACH_NAME "." SUMMARY " SET isrolledup = 0;" /* leave canrollup unchanged */

    /* remove rolled up entries */
    "DELETE FROM " ROLLUP_SUBDIR_ATTACH_NAME "." PENTRIES_ROLLUP ";"

    /* delete rolled in+up xattrs */
    "DELETE FROM " ROLLUP_SUBDIR_ATTACH_NAME "." XATTRS_ROLLUP ";"

    /*
     * get filenames of external xattr dbs created by the subdirectory
     * so that subsubdir xattrs that were rolled up into an existing db
     * can be removed
     */
    "SELECT filename, 1 FROM " ROLLUP_SUBDIR_ATTACH_NAME "." EXTERNAL_DBS_PWD " WHERE type == '" EXTERNAL_TYPE_XATTR_NAME "';"

    /* get filenames of rolled up external xattr dbs and remove(3) them (user dbs should not be touched) */
    "SELECT filename, 0 FROM " ROLLUP_SUBDIR_ATTACH_NAME "." EXTERNAL_DBS_ROLLUP " WHERE type == '" EXTERNAL_TYPE_XATTR_NAME "';"

    /* remove rolled up external xattr db and user db records */
    "DELETE FROM " ROLLUP_SUBDIR_ATTACH_NAME "." EXTERNAL_DBS_ROLLUP ";"
    ;

/*
 * remove rolled up xattrs from the current
 * directory's external xattr database file
 */
static int rollup_external_xattrs_delete(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    rexa_t        *rexa         = (rexa_t *) args;
    const char    *filename     = data[0];
    const size_t   filename_len = strlen(filename);
    const char     existed      = data[1][0] - '0';

    /* parent xattr db filename */
    char xattr_db_name[MAXPATH];
    SNFORMAT_S(xattr_db_name, MAXPATH, 3,
               rexa->child, rexa->child_len,
               "/", (size_t) 1,
               filename, filename_len);

    /* this is a external xattr db that was created by the subdirectory before rollups */
    if (existed == 1) {
        /* open parent per-user/per-group xattr db file */
        sqlite3 *xattr_db = opendb(xattr_db_name, SQLITE_OPEN_READWRITE, 0, 0, NULL, NULL);
        if (!xattr_db) {
            return 1;
        }

        char *err = NULL;
        if (sqlite3_exec(xattr_db, "DELETE FROM " XATTRS_ROLLUP ";", NULL, NULL, &err) != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr, "Warning: Failed to delete rolled up xattrs from \"%s\": %s\n",
                                      xattr_db_name, err);
        }

        closedb(xattr_db);
    }
    /* this external xattr db was created by rollups and now does not need to exist */
    else if (existed == 0) {
        if (remove(xattr_db_name) != 0) {
            const int err = errno;
            sqlite_print_err_and_free(NULL, stderr, "Warning: Failed to delete rolled up xattrs file \"%s\": %s (%d)\n",
                                      xattr_db_name, strerror(err), err);
        }
    }

    return 0;
}

/* copy one subdir/db.db table into the current directory */
static const char ROLLUP_ONE_SUBDIR[] =
    ROLLUP_ONE_SUBDIR_FRONT "s.name || '/' || sub.name" ROLLUP_ONE_SUBDIR_BACK;

/*
@return -1 - error
         0 - success
*/
static int do_rollup(struct RollUp *rollup,
                     sqlite3 *dst,
                     struct template_db *xattr_template) {
    /* assume that this directory can be rolled up */
    /* should_rollup should have been called earlier  */

    /* Not checking arguments */

    struct PoolArgs *pa = (struct PoolArgs *) rollup->data.extra_args;

    /* clear out old rollup data */
    {
        str_t name = REFSTR(rollup->data.name,
                            rollup->data.name_len);
        if (rollup_init(dst, &name) != 0) {
            return -1;
        }
    }

    /* process each child */
    int rc = 0;
    size_t xattr_count = 0;
    sll_loop(&rollup->data.subdirs, node) {
        struct BottomUp *child = (struct BottomUp *) sll_node_data(node);

        str_t child_name = REFSTR(child->alt_name, child->alt_name_len);
        char *child_dbname = rollup_child_attach(dst, &child_name, pa->in.rollup.attach_flag);
        if (!child_dbname) {
            rc = -1;
            break;
        }

        rexa_t rexa = {
            .xattr      = xattr_template,
            .parent     = rollup->data.name,
            .parent_len = rollup->data.name_len,
            .child      = child->name,
            .child_len  = child->name_len,
            .count      = &xattr_count,
        };

        /* roll up the subdir into this dir */
        if (rollup_child_process(dst, ROLLUP_ONE_SUBDIR, &rexa) != 0) {
            rc = -1;
        }

        if (rc == 0){
            /* if the current level is at or below the delete_below value, drop the subdir data */
            char *err = NULL;
            if ((pa->in.rollup.attach_flag == SQLITE_OPEN_READWRITE) &&
                (rollup->data.level >= pa->in.rollup.delete_below)) {
                if (sqlite3_exec(dst, DELETE_SUBDIR_ROLLUP, rollup_external_xattrs_delete, &rexa, &err) != SQLITE_OK) {
                    sqlite_print_err_and_free(err, stderr,
                                              "Error: Failed to delete data from subdir \"%s\": %s\n",
                                              child->name, err);
                    /* not setting rc because failing to delete is ok */
                }
            }
        }

        /* always detach subdir */
        rollup_child_detach(dst, child_dbname);

        if (rc) {
            break;
        }
    }

    /*
     * generate treesummary table
     *
     * might be slightly faster if bottomup_collect_treesummary
     * were integrated into above loop, but that would add a lot
     * of complexity
     */
    if (rc == 0) {
        if (rollup->data.stat_called == STAT_NOT_CALLED) {
            time_t crtime = 0; /* unused */
            if (lstat_wrapper(rollup->data.name, &rollup->data.st, &crtime,
                              &rollup->data.stat_called, 1, 1) != 0) {
                return 1;
            }
        }

        bottomup_collect_treesummary(dst, rollup->data.name, &rollup->data.subdirs, ISROLLEDUP_KNOWN_YES,
                                     rollup->data.st.st_uid, rollup->data.st.st_uid, NULL);
    }

    return rc;
}

static int rollup_ascend(void *args) {
    struct RollUp *dir = (struct RollUp *) args;

    const size_t id = dir->data.tid.up;

    char dbname[MAXPATH];
    SNFORMAT_S(dbname, MAXPATH, 3,
               dir->data.name, dir->data.name_len,
               "/", (size_t) 1,
               DBNAME, DBNAME_LEN);

    struct PoolArgs *pa = (struct PoolArgs *) dir->data.extra_args;
    struct input *in = &pa->in;
    struct RollUpStats *stats = pa->stats;

    /* get statistics out of BottomUp */
    struct DirStats *ds = malloc(sizeof(struct DirStats));
    SNFORMAT_S(ds->path, MAXPATH, 1, dir->data.name, dir->data.name_len);
    ds->level = dir->data.level;
    ds->subdir_count = sll_get_size(&dir->data.subdirs);
    ds->subnondir_count = 0;
    ds->too_many_before = 0;
    ds->too_many_after = 0;
    ds->score = 0;
    ds->success = 1;

    int openflag = SQLITE_OPEN_READWRITE;
    modifydb_func modifydb = create_treesummary_tables;
    if (in->dry_run) {
        openflag = SQLITE_OPEN_READONLY;
        modifydb = NULL;
    }

    /*
     * open the database file here to reduce number of open calls
     *
     * always create the treesummary table
     */
    sqlite3 *dst = opendb(dbname, openflag, 1, 0, modifydb, NULL);
    if (!dst) {
        sll_push_back(&stats[id].not_processed, ds); /* did not check if can roll up */
        stats[id].remaining++;
        return 0;
    }

    /* can attempt to roll up */

    /* if completing partial rollup and this directory is already rolled up, skip */
    if (!in->dont_reprocess || !dir->rolledup) {
        /* check if rollup is allowed */
        ds->score = should_rollup(in, dir, ds, dst);

        /* if can roll up */
        if (ds->score > 0) {
            /* do the roll up */
            if (in->dry_run) {
                ds->success = 1;
            }
            else {
                ds->success = (do_rollup(dir, dst, &pa->xattr_template) == 0);

                /* index summary.inode to make rollup views faster to query */
                if (ds->success) {
                    char *err = NULL;
                    if (sqlite3_exec(dst,
                                     "CREATE INDEX " SUMMARY "_idx ON " SUMMARY "(inode);",
                                     NULL, NULL, &err) != SQLITE_OK) {
                        sqlite_print_err_and_free(err, stderr, "Warning: Could not create roll up index at \"%s\": %s\n", dir->data.name, err);
                    }
                }
            }

            dir->rolledup = ds->success;
            sll_push_back(&stats[id].rolled_up, ds);

            /* root directory will always remain */
            if (!dir->data.parent) {
                stats[id].remaining++;
            }
        }
        else if (ds->score == 0) {
            /* is not allowed to roll up */
            sll_push_back(&stats[id].not_rolled_up, ds);

            /* count this directory */
            stats[id].remaining++;

            /* count only children that were rolled up */
            /* in order to not double count */
            sll_loop(&dir->data.subdirs, node) {
                struct RollUp *child = (struct RollUp *) sll_node_data(node);
                if (child->rolledup) {
                    stats[id].remaining++;
                }
            }

            if (!pa->in.dry_run) {
                if (dir->data.stat_called == STAT_NOT_CALLED) {
                    time_t crtime = 0; /* unused */
                    if (lstat_wrapper(dir->data.name, &dir->data.st, &crtime,
                                      &dir->data.stat_called, 1, 1) != 0) {
                        return 1;
                    }
                }

                bottomup_collect_treesummary(dst, dir->data.name, &dir->data.subdirs, ISROLLEDUP_KNOWN_NO,
                                             dir->data.st.st_uid, dir->data.st.st_gid, NULL);
            }
        }
    }
    else if (in->dont_reprocess && dir->rolledup) {
        ds->score = 1;
        ds->success = 1;
        sll_push_back(&stats[id].rolled_up, ds);
    }

    closedb(dst);

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
        FLAG_MIN_LEVEL, FLAG_MAX_LEVEL, FLAG_PATH_LIST, /* max-level is only used for distributed processing */
        FLAG_ROLLUP_LIMIT, FLAG_ROLLUP_DELETE_BELOW,
        FLAG_DRY_RUN, FLAG_DONT_REPROCESS,

        FLAG_END
    };

    struct start_end runtime;
    clock_gettime(CLOCK_MONOTONIC, &runtime.start);

    struct PoolArgs pa;
    process_args_and_maybe_exit(options, 1, "GUFI_tree ...", &pa.in);

    init_template_db(&pa.xattr_template);
    if (create_xattrs_template(&pa.xattr_template, NULL) != 0) {
        fprintf(stderr, "Could not create xattr template file\n");
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    pa.stats = calloc(pa.in.maxthreads, sizeof(struct RollUpStats));
    if (!pa.stats) {
        fprintf(stderr, "Could not allocate %zu stat buffers\n", pa.in.maxthreads);
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        sll_init(&pa.stats[i].not_processed);
        sll_init(&pa.stats[i].not_rolled_up);
        sll_init(&pa.stats[i].rolled_up);
        pa.stats[i].remaining = 0;
    }

    BU_descend_f desc = pa.in.dont_reprocess?rollup_descend:NULL;

    const int rc = parallel_bottomup(pa.in.pos.argv, pa.in.pos.argc,
                                     pa.in.min_level, pa.in.max_level,
                                     &pa.in.path_list,
                                     pa.in.maxthreads,
                                     sizeof(struct RollUp),
                                     desc, rollup_ascend,
                                     0,
                                     1,
                                     &pa);

    print_stats(pa.in.pos.argv, pa.in.pos.argc, &pa.in, pa.stats);

    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        sll_destroy(&pa.stats[i].rolled_up, 0);
        sll_destroy(&pa.stats[i].not_rolled_up, 0);
        sll_destroy(&pa.stats[i].not_processed, 0);
    }
    free(pa.stats);
    close_template_db(&pa.xattr_template);

    clock_gettime(CLOCK_MONOTONIC, &runtime.end);
    fprintf(stderr, "Took %.2Lf seconds\n", sec(nsec(&runtime)));

    input_fini(&pa.in);

    return rc?EXIT_FAILURE:EXIT_SUCCESS;
}
