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
#include "external.h"
#include "template_db.h"
#include "utils.h"

#define SUBDIR_ATTACH_NAME "subdir"

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

    #ifdef DEBUG
    struct OutputBuffers *print_buffers;
    #endif
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
    fprintf(stdout, "Files/Links Limit: %11zu\n", in->max_in_dir);
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

/* ************************************** */
/* get permissions from directory entries */
const char PERM_SQL[] = "SELECT mode, uid, gid, (SELECT COUNT(*) FROM pentries) "
                        "FROM " SUMMARY " WHERE isroot == 1;";

struct Permissions {
    mode_t mode;
    uid_t uid;
    gid_t gid;
};

static int get_permissions(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    struct Permissions *perms = (struct Permissions *) args;
    perms->mode = atoi(data[0]);
    perms->uid  = atoi(data[1]);
    perms->gid  = atoi(data[2]);
    return 0;
}

struct ChildData {
    struct Permissions *perms;
    size_t count;
};

static int get_permissions_and_count(void *args, int count, char **data, char **columns) {
    struct ChildData *child = (struct ChildData *) args;
    get_permissions(child->perms, count, data, columns);
    child->count = atoi(data[3]);
    return 0;
}

static int add_entries_count(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    size_t *total = (size_t *) args;
    size_t rows = 0;
    if (sscanf(data[0], "%zu", &rows) != 1) {
        return 1;
    }
    *total += rows;
    return 0;
}

/* get number of non-dirs in this directory from the pentries table */
static int get_nondirs(const char *name, sqlite3 *dst, size_t *subnondir_count) {
    char *err = NULL;
    const int exec_rc = sqlite3_exec(dst, "SELECT COUNT(*) FROM pentries",
                                     add_entries_count, subnondir_count, &err);
    if (exec_rc != SQLITE_OK) {
        fprintf(stderr, "Warning: Failed to get entries row count from \"%s\": %s\n", name, err);
        sqlite3_free(err);
    }
    return exec_rc;
}

/*
@return -1 - failed to open a database
         0 - do not roll up
         1 - all permissions pass
*/
static int check_children(struct RollUp *rollup, struct Permissions *curr,
                          const size_t child_count, size_t *total_child_entries
                          timestamp_sig) {
    if (child_count == 0) {
        return 1;
    }

    struct Permissions *child_perms =
        malloc(sizeof(struct Permissions) * sll_get_size(&rollup->data.subdirs));

    /* get permissions of each child */
    size_t idx = 0;
    sll_loop(&rollup->data.subdirs, node) {
        struct RollUp *child = (struct RollUp *) sll_node_data(node);

        char dbname[MAXPATH] = {0};
        SNFORMAT_S(dbname, MAXPATH, 3,
                   child->data.name, child->data.name_len,
                   "/", (size_t) 1,
                   DBNAME, DBNAME_LEN);

        timestamp_create_start(open_child_db);
        sqlite3 *db = opendb(dbname, SQLITE_OPEN_READONLY, 1, 0, NULL, NULL);
        timestamp_end_print(timestamp_buffers, rollup->data.tid.up, "open_child_db", open_child_db);

        if (!db) {
            break;
        }

        /* get the child directory's permissions and pentries count */
        timestamp_create_start(get_child_data);
        struct ChildData data;
        data.perms = &child_perms[idx];
        data.count = 0;
        char *err = NULL;
        const int rc = sqlite3_exec(db, PERM_SQL, get_permissions_and_count, &data, &err);
        timestamp_end_print(timestamp_buffers, rollup->data.tid.up, "get_child_data", get_child_data);

        timestamp_create_start(close_child_db);
        closedb(db);
        timestamp_end_print(timestamp_buffers, rollup->data.tid.up, "close_child_db", close_child_db);

        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error: Could not get permissions of child directory \"%s\": %s\n", child->data.name, err);

            sqlite3_free(err);
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
        /* so long as one condition is true, child[i] can be rolled up into the current directory */
        legal += (
            /* self and child are o+rx */
            (((curr->mode          & 0005) == 0005) &&
             ((child_perms[i].mode & 0005) == 0005) &&
              (curr->uid == child_perms[i].uid) &&
              (curr->gid == child_perms[i].gid)) ||

            /* self and child have same user, group, and others permissions, uid, and gid */
            (((curr->mode & 0555) == (child_perms[i].mode & 0555)) &&
              (curr->uid == child_perms[i].uid) &&
              (curr->gid == child_perms[i].gid)) ||

            /* self and child have same user and group permissions, uid, and gid, and top is o-rx */
            (((curr->mode & 0550) == (child_perms[i].mode & 0550)) &&
              (curr->uid == child_perms[i].uid) &&
              (curr->gid == child_perms[i].gid)) ||

            /* self and child have same user permissions, go-rx, uid */
            (((curr->mode & 0500) == (child_perms[i].mode & 0500)) &&
              (curr->uid == child_perms[i].uid))
        );
    }

    free(child_perms);

    /* need all children to have good permissions */
    return (legal == child_count);
}
/* ************************************** */

/*
 * Check if the current directory can be rolled up.
 *
 * Previously returned 1 - 5 to indicate which rollup rule was
 * followed because all children had to follow the same rule (other
 * than leaves). Now only returns 1 because rollup can occur so long
 * as every child meets any of the rules.
 *
 * @return   0 - cannot rollup
 *           1 - can rollup
 */
static int can_rollup(struct input *in,
                      struct RollUp *rollup,
                      struct DirStats *ds,
                      sqlite3 *dst
                      timestamp_sig) {
    /* Not checking arguments */

    char *err = NULL;

    timestamp_create_start(can_roll_up);

    /* default to cannot roll up */
    int legal = 0;

    /* get count of number of non-directories in the current directory */
    timestamp_create_start(nondir_count);
    ds->subnondir_count = 0;
    get_nondirs(rollup->data.name, dst, &ds->subnondir_count);
    timestamp_end_print(timestamp_buffers, rollup->data.tid.up, "nondir_count", nondir_count);

    /* the current directory has too many immediate files/links, don't roll up */
    if (in->max_in_dir && (ds->subnondir_count > in->max_in_dir)) {
        ds->too_many_before = ds->subnondir_count;
        goto end_can_rollup;
    }

    /* all children are expected to pass */
    size_t total_subdirs = 0;

    /* check if ALL children have been rolled up */
    timestamp_create_start(check_subdirs_rolledup);
    size_t rolledup = 0;
    sll_loop(&rollup->data.subdirs, node) {
        struct RollUp *child = (struct RollUp *) sll_node_data(node);
        rolledup += child->rolledup;
        total_subdirs++;
    }
    timestamp_end_print(timestamp_buffers, rollup->data.tid.up, "check_subdirs_rolledup", check_subdirs_rolledup);

    /* quick check to see if all chilren were rolled up */
    if (total_subdirs != rolledup) {
        goto end_can_rollup;
    }

    /* get permissions of the current directory */
    timestamp_create_start(get_perms);
    struct Permissions perms;
    const int exec_rc = sqlite3_exec(dst, PERM_SQL, get_permissions, &perms, &err);
    timestamp_end_print(timestamp_buffers, rollup->data.tid.up, "get_perms", get_perms);

    if (exec_rc != SQLITE_OK) {
        fprintf(stderr, "Error: Could not get permissions of current directory \"%s\": %s\n", rollup->data.name, err);
        legal = 0;
        goto end_can_rollup;
    }

    /* check if the permissions of this directory and its children match */
    timestamp_create_start(check_perms);
    size_t total_child_entries = 0;
    legal = check_children(rollup, &perms, total_subdirs, &total_child_entries timestamp_args);
    timestamp_end_print(timestamp_buffers, rollup->data.tid.up, "check_perms", check_perms);

    /*
     * even if this directory can be rolled up, don't
     * let it if doing so would result in too many
     * rows in pentries
     */
    if (legal) {
        const size_t total_pentries = ds->subnondir_count + total_child_entries;
        if (in->max_in_dir && (total_pentries > in->max_in_dir)) {
            ds->too_many_after = total_pentries;
            legal = 0;
        }
    }

end_can_rollup:
    timestamp_end_print(timestamp_buffers, rollup->data.tid.up, "can_rollup", can_roll_up);

    return legal;
}

static const char rollup_subdir[] =
    /* copy subdir/db.db tables into current db.db */
    "INSERT INTO " PENTRIES_ROLLUP " SELECT * FROM " SUBDIR_ATTACH_NAME "." PENTRIES ";"
    "INSERT INTO " SUMMARY " SELECT s.name || '/' || sub.name, sub.type, sub.inode, sub.mode, sub.nlink, sub.uid, sub.gid, sub.size, sub.blksize, sub.blocks, sub.atime, sub.mtime, sub.ctime, sub.linkname, sub.xattr_names, sub.totfiles, sub.totlinks, sub.minuid, sub.maxuid, sub.mingid, sub.maxgid, sub.minsize, sub.maxsize, sub.totzero, sub.totltk, sub.totmtk, sub.totltm, sub.totmtm, sub.totmtg, sub.totmtt, sub.totsize, sub.minctime, sub.maxctime, sub.minmtime, sub.maxmtime, sub.minatime, sub.maxatime, sub.minblocks, sub.maxblocks, sub.totxattr, sub.depth + 1, sub.mincrtime, sub.maxcrtime, sub.minossint1, sub.maxossint1, sub.totossint1, sub.minossint2, sub.maxossint2, sub.totossint2, sub.minossint3, sub.maxossint3, sub.totossint3, sub.minossint4, sub.maxossint4, sub.totossint4, sub.rectype, sub.pinode, 0, sub.rollupscore FROM " SUMMARY " AS s, " SUBDIR_ATTACH_NAME "." SUMMARY " AS sub WHERE s.isroot == 1;"
    /*
     * do not copy treesummary upwards - just calculate current
     * treesummary since the treesummary covers all subtree
     * summaries
     */
    "INSERT INTO " XATTRS_ROLLUP " SELECT * FROM " SUBDIR_ATTACH_NAME "." XATTRS_AVAIL ";"
    "INSERT OR IGNORE INTO " EXTERNAL_DBS_ROLLUP " SELECT * FROM " SUBDIR_ATTACH_NAME "." EXTERNAL_DBS ";"

    /* select subdir external xattrs_avail for copying to current external xattrs_rollup via callback */
    "SELECT filename, uid, gid FROM " SUBDIR_ATTACH_NAME "." EXTERNAL_DBS " WHERE type == '" EXTERNAL_TYPE_XATTR_NAME "';";

/* rollup_external_xattrs callback args */
struct CallbackArgs {
    struct template_db *xattr;
    char *parent;
    size_t parent_len;
    char *child;
    size_t child_len;
    size_t *count;
};

/* use this callback to create xattr db files and insert filenames */
/* db.db doesn't need to be modified since the SQL statement already did it */
static int rollup_external_xattrs(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    struct CallbackArgs *ca     = (struct CallbackArgs *) args;
    const char  *filename       = data[0];
    const size_t filename_len   = strlen(filename);
    const char  *uid_str        = data[1];
    const char  *gid_str        = data[2];
    uid_t uid;
    gid_t gid;

    sscanf(uid_str, "%" STAT_uid, &uid); /* skip checking for failure */
    sscanf(gid_str, "%" STAT_gid, &gid); /* skip checking for failure */

    /* parent xattr db filename */
    char xattr_db_name[MAXPATH];
    SNFORMAT_S(xattr_db_name, MAXPATH, 3,
               ca->parent, ca->parent_len,
               "/", (size_t) 1,
               filename, filename_len);

    /* check if the parent per-user/per-group xattr db file exists */
    struct stat st;
    if (stat(xattr_db_name, &st) != 0) {
        const int err = errno;
        if (err != ENOENT) {
            fprintf(stderr, "Error: Cannot access xattr db file %s: %s (%d)\n",
                    xattr_db_name, strerror(err), err);
            return 1;
        }

        /* copy the template file */
        if (copy_template(ca->xattr, xattr_db_name, uid, gid)) {
            return 1;
        }
    }

    /* open parent per-user/per-group xattr db file */
    sqlite3 *xattr_db = opendb(xattr_db_name, SQLITE_OPEN_READWRITE, 0, 0, NULL, NULL);
    if (!xattr_db) {
        return 1;
    }

    char child_xattr_db_name[MAXPATH];
    SNFORMAT_S(child_xattr_db_name, MAXPATH, 3,
               ca->child, ca->child_len,
               "/", (size_t) 1,
               filename, filename_len);

    char attachname[MAXPATH];
    SNPRINTF(attachname, sizeof(attachname),
             EXTERNAL_ATTACH_PREFIX "%zu", (*ca->count)++);

    if (!attachdb(child_xattr_db_name, xattr_db, attachname, SQLITE_OPEN_READONLY, 1)) {
        closedb(xattr_db);
        return 1;
    }

    char insert[MAXSQL];
    SNPRINTF(insert, sizeof(insert),
             /* copy child external xattrs_avail to external xattrs_rollup */
             "INSERT INTO " XATTRS_ROLLUP " SELECT * FROM %s." XATTRS_AVAIL ";",
             attachname);
    char *err = NULL;
    int rc = sqlite3_exec(xattr_db, insert, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: Failed to copy \"%s\" xattrs into " XATTRS_ROLLUP " of %s: %s\n",
                child_xattr_db_name, xattr_db_name, err);
        sqlite3_free(err);
    }

    closedb(xattr_db);
    return 0;
}

/*
@return -1 - could not move entries into pentries
         0 - success
         1 - at least one child failed to be moved
*/
static int do_rollup(struct RollUp *rollup,
                     struct DirStats *ds,
                     sqlite3 *dst,
                     struct template_db *xattr_template
                     timestamp_sig) {
    /* assume that this directory can be rolled up */
    /* can_rollup should have been called earlier  */

    /* Not checking arguments */

    timestamp_create_start(do_roll_up);

    #ifdef DEBUG
    #ifdef PER_THREAD_STATS
    const size_t id = rollup->data.tid.up;
    #endif
    #endif

    int rc = 0;
    char *err = NULL;
    int exec_rc = SQLITE_OK;

    char setup[] =
        /* clear out old rollup data */
        "DELETE FROM " PENTRIES_ROLLUP ";"
        "DELETE FROM " SUMMARY " WHERE isroot != 1;"
        "DELETE FROM " TREESUMMARY ";"
        "DELETE FROM " XATTRS_ROLLUP ";"

        /* select all external xattr db names for cleanup in callback */
        "SELECT filename FROM " EXTERNAL_DBS " WHERE type == '" EXTERNAL_TYPE_XATTR_NAME "';"

        "DELETE FROM " EXTERNAL_DBS_ROLLUP ";"

        /* set the rollup score in the SQL statement */
        "UPDATE " SUMMARY " SET rollupscore = 0;";

    setup[sizeof(setup) - sizeof("0;")] += ds->score;

    refstr_t name = {
        .data = rollup->data.name,
        .len  = rollup->data.name_len,
    };

    timestamp_create_start(setup);
    exec_rc = sqlite3_exec(dst, setup, xattrs_rollup_cleanup, &name, &err);
    timestamp_end_print(timestamp_buffers, id, "setup", setup);

    if (exec_rc != SQLITE_OK) {
        fprintf(stderr, "Error: Failed to set up database for rollup: \"%s\": %s\n",
                rollup->data.name, err);
        sqlite3_free(err);
        rc = -1;
        goto end_rollup;
    }

    /* process each child */
    timestamp_create_start(rollup_subdirs);

    size_t xattr_count = 0;
    sll_loop(&rollup->data.subdirs, node) {
        timestamp_create_start(rollup_subdir);

        struct BottomUp *child = (struct BottomUp *) sll_node_data(node);

        char child_dbname[MAXPATH];
        SNFORMAT_S(child_dbname, sizeof(child_dbname), 3,
                   child->alt_name, child->alt_name_len,
                   "/", (size_t) 1,
                   DBNAME, DBNAME_LEN);

        /* attach subdir database file as 'SUBDIR_ATTACH_NAME' */
        rc = !attachdb(child_dbname, dst, SUBDIR_ATTACH_NAME, SQLITE_OPEN_READONLY, 1);

        /* roll up the subdir into this dir */
        if (!rc) {
            timestamp_create_start(rollup_subdir);
            struct CallbackArgs ca = {
                .xattr      = xattr_template,
                .parent     = rollup->data.name,
                .parent_len = rollup->data.name_len,
                .child      = child->name,
                .child_len  = child->name_len,
                .count      = &xattr_count,
            };

            exec_rc = sqlite3_exec(dst, rollup_subdir, rollup_external_xattrs, &ca, &err);
            timestamp_end_print(timestamp_buffers, id, "rollup_subdir", rollup_subdir);
            if (exec_rc != SQLITE_OK) {
                fprintf(stderr, "Error: Failed to copy subdir \"%s\" into current database: %s\n",
                        child->name, err);
                sqlite3_free(err);
            }
        }

        /* always detach subdir */
        detachdb(child_dbname, dst, SUBDIR_ATTACH_NAME, 1);

        timestamp_end_print(timestamp_buffers, id, "rollup_subdir", rollup_subdir);

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
    if (!rc) {
        timestamp_create_start(rollup_treesummary);
        bottomup_collect_treesummary(dst, rollup->data.name, &rollup->data.subdirs, ROLLUPSCORE_KNOWN_YES);
        timestamp_end_print(timestamp_buffers, id, "rollup_treesummary", rollup_treesummary);
    }

    timestamp_end_print(timestamp_buffers, id, "rollup_subdirs", rollup_subdirs);

end_rollup:
    timestamp_end_print(timestamp_buffers, id, "do_rollup", do_roll_up);
    return rc;
}

static int rollup(void *args timestamp_sig) {
    timestamp_create_start(setup);

    struct RollUp *dir = (struct RollUp *) args;
    dir->rolledup = 0;

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
    ds->subdir_count = dir->data.subdir_count;
    ds->subnondir_count = 0;
    ds->too_many_before = 0;
    ds->too_many_after = 0;
    ds->score = 0;
    ds->success = 1;

    timestamp_end_print(timestamp_buffers, id, "setup", setup);

    /*
     * open the database file here to reduce number of open calls
     *
     * always create the treesummary table
     */
    timestamp_create_start(open_curr_db);
    sqlite3 *dst = opendb(dbname, SQLITE_OPEN_READWRITE, 1, 0, create_treesummary_tables, NULL);
    timestamp_end_print(timestamp_buffers, id, "opendb", open_curr_db);

    /* can attempt to roll up */
    if (dst) {
        /* check if rollup is allowed */
        ds->score = can_rollup(in, dir, ds, dst timestamp_args);

        /* if can roll up */
        if (ds->score > 0) {
            /* do the roll up */
            if (in->dry_run) {
                ds->success = 1;
            }
            else {
                ds->success = (do_rollup(dir, ds, dst, &pa->xattr_template timestamp_args) == 0);
            }

            dir->rolledup = ds->success;
            sll_push(&stats[id].rolled_up, ds);

            /* root directory will always remain */
            if (!dir->data.parent) {
                stats[id].remaining++;
            }
        }
        else if (ds->score == 0) {
            /* is not allowed to roll up */
            sll_push(&stats[id].not_rolled_up, ds);

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

            timestamp_create_start(not_rolledup_treesummary);
            bottomup_collect_treesummary(dst, dir->data.name, &dir->data.subdirs, ROLLUPSCORE_KNOWN_NO);
            timestamp_end_print(timestamp_buffers, id, "not_rolledup_treesummary", not_rolledup_treesummary);
        }
    }
    else {
        /* did not check if can roll up */
        sll_push(&stats[id].not_processed, ds);

        stats[id].remaining++;
    }

    closedb(dst);

    return 0;
}

static void sub_help(void) {
   printf("GUFI_index        GUFI index to roll up\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    epoch = since_epoch(NULL);

    timestamp_create_start_raw(runtime);

    struct PoolArgs pa;
    int idx = parse_cmd_line(argc, argv, "hHn:L:X", 1, "GUFI_index ...", &pa.in);
    if (pa.in.helped)
        sub_help();
    if (idx < 0) {
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    init_template_db(&pa.xattr_template);
    if (create_xattrs_template(&pa.xattr_template) != 0) {
        fprintf(stderr, "Could not create xattr template file\n");
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    #ifdef DEBUG
    pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;

    #ifdef PER_THREAD_STATS
    timestamp_print_init(timestamp_buffers, pa.in.maxthreads + 1, 1024 * 1024, &print_mutex);
    #endif
    #endif

    pa.stats = calloc(pa.in.maxthreads, sizeof(struct RollUpStats));
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        sll_init(&pa.stats[i].not_processed);
        sll_init(&pa.stats[i].not_rolled_up);
        sll_init(&pa.stats[i].rolled_up);
        pa.stats[i].remaining = 0;
    }

    #ifdef DEBUG
    struct OutputBuffers print_buffers;
    OutputBuffers_init(&print_buffers, pa.in.maxthreads, 1024 * 1024, &print_mutex);

    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        pa.stats[i].print_buffers = &print_buffers;
    }
    #endif

    argv += idx;
    argc -= idx;

    const int rc = parallel_bottomup(argv, argc,
                                     pa.in.maxthreads,
                                     sizeof(struct RollUp),
                                     NULL, rollup,
                                     0,
                                     1,
                                     &pa
                                     #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                     , timestamp_buffers
                                     #endif
        );

    #ifdef DEBUG
    OutputBuffers_flush_to_single(&print_buffers, stderr);
    OutputBuffers_destroy(&print_buffers);

    #ifdef PER_THREAD_STATS
    timestamp_print_destroy(timestamp_buffers);
    #endif

    #endif

    print_stats(argv, argc, &pa.in, pa.stats);

    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        #ifdef DEBUG
        pa.stats[i].print_buffers = NULL;
        #endif
        sll_destroy(&pa.stats[i].rolled_up, 0);
        sll_destroy(&pa.stats[i].not_rolled_up, 0);
        sll_destroy(&pa.stats[i].not_processed, 0);
    }
    free(pa.stats);
    close_template_db(&pa.xattr_template);

    timestamp_set_end_raw(runtime);
    fprintf(stderr, "Took %.2Lf seconds\n", sec(nsec(&runtime)));

    input_fini(&pa.in);

    return rc?EXIT_FAILURE:EXIT_SUCCESS;
}
