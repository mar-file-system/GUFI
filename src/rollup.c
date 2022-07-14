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

#include "bf.h"
#include "BottomUp.h"
#include "dbutils.h"
#include "debug.h"
#include "SinglyLinkedList.h"
#include "template_db.h"
#include "utils.h"

extern int errno;

#define SUBDIR_ATTACH_NAME "subdir"

/* statistics stored when processing each directory */
/* this is the type stored in the RollUpStats struct sll variables */
struct DirStats {
    char path[MAXPATH];

    size_t level;           /* level this directory is at */

    size_t subdir_count;    /* unrolled up count from readdir, not summary */
    size_t subnondir_count; /* rolled up count from entries and subdir.pentries */

    size_t too_many_before; /* current directory is too big to roll up */
    size_t too_many_after;  /* rolling up would result in too many rows in pentries */
    int score;              /* roll up score regardless of success or failure */
    int success;            /* whether or not the roll up succeeded */
};

/* per thread stats */
struct RollUpStats {
    struct sll not_processed;
    struct sll not_rolled_up;
    struct sll rolled_up;

    size_t remaining;       /* children that remain after rolling up */

    #ifdef DEBUG
    struct OutputBuffers *print_buffers;
    #endif
};

struct ThreadArgs {
    struct template_db xattr_template;
    struct RollUpStats *stats;
};

/* compare function for qsort */
int compare_size_t(const void *lhs, const void *rhs) {
    return (*(size_t *) lhs - *(size_t *) rhs);
}

#define sll_dir_stats(name, all, var, width)                             \
do {                                                                     \
    const size_t count = sll_get_size(all);                              \
    if (count == 0) {                                                    \
        break;                                                           \
    }                                                                    \
                                                                         \
    size_t *array = malloc(count * sizeof(size_t));                      \
    size_t min = (size_t) -1;                                            \
    size_t max = 0;                                                      \
    size_t sum = 0;                                                      \
    size_t i = 0;                                                        \
    sll_loop(all, node) {                                                \
        struct DirStats *ds = (struct DirStats *) sll_node_data(node);   \
        if (ds->var < min) {                                             \
            min = ds->var;                                               \
        }                                                                \
                                                                         \
        if (ds->var > max) {                                             \
            max = ds->var;                                               \
        }                                                                \
                                                                         \
        sum += ds->var;                                                  \
        array[i++] = ds->var;                                            \
    }                                                                    \
                                                                         \
    qsort(array, count, sizeof(size_t), compare_size_t);                 \
    const size_t half = count / 2;                                       \
    double median = array[half];                                         \
    if (count % 2 == 0) {                                                \
        median += array[half - 1];                                       \
        median /= 2;                                                     \
    }                                                                    \
                                                                         \
    const double avg = ((double) sum) / count;                           \
                                                                         \
    fprintf(stdout, "    %s:\n", name);                                  \
    fprintf(stdout, "        min:        %" #width "zu\n",  min);        \
    fprintf(stdout, "        max:        %" #width "zu\n",  max);        \
    fprintf(stdout, "        median:     %" #width ".2f\n", median);     \
    fprintf(stdout, "        sum:        %" #width "zu\n",  sum);        \
    fprintf(stdout, "        average:    %" #width ".2f\n", avg);        \
                                                                         \
    free(array);                                                         \
} while (0)

#define print_too_many(name, all, var, width)                            \
do {                                                                     \
    const size_t count = sll_get_size(all);                              \
    if (count == 0) {                                                    \
        break;                                                           \
    }                                                                    \
                                                                         \
    size_t *array = malloc(count * sizeof(size_t));                      \
    size_t min = (size_t) -1;                                            \
    size_t max = 0;                                                      \
    size_t sum = 0;                                                      \
    size_t i = 0;                                                        \
    sll_loop(all, node) {                                                \
        struct DirStats *ds = (struct DirStats *) sll_node_data(node);   \
        if (ds->var == 0) {                                              \
            continue;                                                    \
        }                                                                \
                                                                         \
        if (ds->var < min) {                                             \
            min = ds->var;                                               \
        }                                                                \
                                                                         \
        if (ds->var > max) {                                             \
            max = ds->var;                                               \
        }                                                                \
                                                                         \
        sum += ds->var;                                                  \
        array[i++] = ds->var;                                            \
    }                                                                    \
                                                                         \
    fprintf(stdout, "    %s %9zu\n", name, i);                           \
    if (i) {                                                             \
        qsort(array, i, sizeof(size_t), compare_size_t);                 \
        const size_t half = i / 2;                                       \
        double median = array[half];                                     \
        if (i % 2 == 0) {                                                \
            median += array[half - 1];                                   \
            median /= 2;                                                 \
        }                                                                \
                                                                         \
        const double avg = ((double) sum) / i;                           \
                                                                         \
        fprintf(stdout, "        min:        %" #width "zu\n",  min);    \
        fprintf(stdout, "        max:        %" #width "zu\n",  max);    \
        fprintf(stdout, "        median:     %" #width ".2f\n", median); \
        fprintf(stdout, "        sum:        %" #width "zu\n",  sum);    \
        fprintf(stdout, "        average:    %" #width ".2f\n", avg);    \
    }                                                                    \
                                                                         \
    free(array);                                                         \
} while (0)

void print_stanza(const char *name, struct sll *stats) {
    fprintf(stdout, "%s %*zu\n", name, (int) (29 - strlen(name)), sll_get_size(stats));
    sll_dir_stats("Subdirectories", stats, subdir_count,    10);
    sll_dir_stats("Files/Links",    stats, subnondir_count, 10);
    sll_dir_stats("Level",          stats, level,           10);
}

/* this function moves the sll stats up and deallocates them */
void print_stats(char **paths, const int path_count, struct RollUpStats *stats, const size_t threads) {
    fprintf(stdout, "Roots:\n");
    for(int i = 0; i < path_count; i++) {
        fprintf(stdout, "    %s\n", paths[i]);
    }
    fprintf(stdout, "\n");
    fprintf(stdout, "Thread Pool Size: %12d\n",  in.maxthreads);
    fprintf(stdout, "Files/Links Limit: %11d\n", (int) in.max_in_dir);
    fprintf(stdout, "\n");

    /* per-thread stats together */
    struct sll not_processed;
    sll_init(&not_processed);

    struct sll not_rolled_up;
    sll_init(&not_rolled_up);

    struct sll rolled_up;
    sll_init(&rolled_up);

    size_t remaining = 0;

    for(size_t i = 0; i < threads; i++) {
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
const char PERM_SQL[] = "SELECT "
    "(SELECT mode FROM summary WHERE isroot == 1), "
    "(SELECT uid  FROM summary WHERE isroot == 1), "
    "(SELECT gid  FROM summary WHERE isroot == 1), "
    "(SELECT COUNT(*) FROM pentries)";

struct Permissions {
    mode_t mode;
    uid_t uid;
    gid_t gid;
};

int get_permissions(void *args, int count, char **data, char **columns) {
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

int get_permissions_and_count(void *args, int count, char **data, char **columns) {
    struct ChildData *child = (struct ChildData *) args;
    get_permissions(child->perms, count, data, columns);
    child->count = atoi(data[3]);
    return 0;
}

int add_entries_count(void *args, int count, char **data, char **columns) {
    size_t *total = (size_t *) args;
    size_t rows = 0;
    if (sscanf(data[0], "%zu", &rows) != 1) {
        return 1;
    }
    *total += rows;
    return 0;
}

/* get number of non-dirs in this directory from the pentries table */
int get_nondirs(const char *name, sqlite3 *dst, size_t *subnondir_count) {
    char *err = NULL;
    const int exec_rc = sqlite3_exec(dst, "SELECT COUNT(*) FROM pentries",
                                     add_entries_count, subnondir_count, &err);
    if (exec_rc != SQLITE_OK) {
        fprintf(stderr, "Warning: Failed to get entries row count from \"%s\": %s\n", name, err);
    }
    sqlite3_free(err);
    return exec_rc;
}

/*
@return -1 - failed to open a database
         0 - do not roll up
         1 - all permissions pass
*/
int check_children(struct RollUp *rollup, struct Permissions *curr,
                   const size_t child_count, size_t *total_child_entries
                   timestamp_sig) {
    if (child_count == 0) {
        return 1;
    }

    timestamp_create_buffer(4096);

    struct Permissions *child_perms =
        malloc(sizeof(struct Permissions) *sll_get_size(&rollup->data.subdirs));

    /* get permissions of each child */
    size_t idx = 0;
    sll_loop(&rollup->data.subdirs, node) {
        struct RollUp *child = (struct RollUp *) sll_node_data(node);

        char dbname[MAXPATH] = {0};
        SNPRINTF(dbname, MAXPATH, "%s/" DBNAME, child->data.name);

        timestamp_start(open_child_db);
        sqlite3 *db = opendb(dbname, SQLITE_OPEN_READONLY, 1, 0
                             , NULL, NULL
                             #if defined(DEBUG) && defined(PER_THREAD_STATS)
                             , NULL, NULL
                             , NULL, NULL
                             #endif
            );
        timestamp_end(timestamp_buffers, rollup->data.tid.up, "open_child_db", open_child_db);

        if (!db) {
            break;
        }

        /* get the child directory's permissions and pentries count */
        timestamp_start(get_child_data);
        struct ChildData data;
        data.perms = &child_perms[idx];
        data.count = 0;
        char *err = NULL;
        const int rc = sqlite3_exec(db, PERM_SQL, get_permissions_and_count, &data, &err);
        timestamp_end(timestamp_buffers, rollup->data.tid.up, "get_child_data", get_child_data);

        timestamp_start(close_child_db);
        closedb(db);
        timestamp_end(timestamp_buffers, rollup->data.tid.up, "close_child_db", close_child_db);

        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error: Could not get permissions of child directory \"%s\": %s\n", child->data.name, err);

            sqlite3_free(err);
            break;
        }

        sqlite3_free(err);

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

/* check if the current directory can be rolled up */
/*
@return   0 - cannot rollup
        > 0 - rollup score
*/
int can_rollup(struct RollUp *rollup,
               struct DirStats *ds,
               sqlite3 *dst
               timestamp_sig) {
    /* if (!rollup || !ds || !dst) { */
    /*     return -1; */
    /* } */

    char *err = NULL;

    timestamp_create_buffer(4096);
    timestamp_start(can_roll_up);

    /* default to cannot roll up */
    int legal = 0;

    /* get count of number of non-directories in the current directory */
    timestamp_start(nondir_count);
    ds->subnondir_count = 0;
    get_nondirs(rollup->data.name, dst, &ds->subnondir_count);
    timestamp_end(timestamp_buffers, rollup->data.tid.up, "nondir_count", nondir_count);

    /* the current directory has too many immediate files/links, don't roll up */
    if (ds->subnondir_count > in.max_in_dir) {
        ds->too_many_before = ds->subnondir_count;
        goto end_can_rollup;
    }

    /* all children are expected to pass */
    size_t total_subdirs = 0;

    /* check if ALL children have been rolled up */
    timestamp_start(check_subdirs_rolledup);
    size_t rolledup = 0;
    sll_loop(&rollup->data.subdirs, node) {
        struct RollUp *child = (struct RollUp *) sll_node_data(node);
        rolledup += child->rolledup;
        total_subdirs++;
    }
    timestamp_end(timestamp_buffers, rollup->data.tid.up, "check_subdirs_rolledup", check_subdirs_rolledup);

    /* quick check to see if all chilren were rolled up */
    if (total_subdirs != rolledup) {
        goto end_can_rollup;
    }

    /* get permissions of the current directory */
    timestamp_start(get_perms);
    struct Permissions perms;
    const int exec_rc = sqlite3_exec(dst, PERM_SQL, get_permissions, &perms, &err);
    timestamp_end(timestamp_buffers, rollup->data.tid.up, "get_perms", get_perms);

    if (exec_rc != SQLITE_OK) {
        fprintf(stderr, "Error: Could not get permissions of current directory \"%s\": %s\n", rollup->data.name, err);
        legal = 0;
        goto end_can_rollup;
    }

    /* check if the permissions of this directory and its children match */
    timestamp_start(check_perms);
    size_t total_child_entries = 0;
    legal = check_children(rollup, &perms, total_subdirs, &total_child_entries timestamp_args);
    timestamp_end(timestamp_buffers, rollup->data.tid.up, "check_perms", check_perms);

    /*
     * even if this directory can be rolled up, don't
     * let it if doing so would result in too many
     * rows in pentries
     */
    if (legal) {
        const size_t total_pentries = ds->subnondir_count + total_child_entries;
        if (total_pentries > in.max_in_dir) {
            ds->too_many_after = total_pentries;
            legal = 0;
        }
    }

end_can_rollup:
    sqlite3_free(err);

    timestamp_end(timestamp_buffers, rollup->data.tid.up, "can_rollup", can_roll_up);

    return legal;
}

/* copy child pentries into pentries_rollup */
/* copy child summary into summary */
/* copy child xattrs_avail to xattrs_rollup */
/* copy child xattr_files to xattr_files_rollup */
static const char rollup_subdir[] =
    "INSERT INTO " PENTRIES_ROLLUP " SELECT * FROM " SUBDIR_ATTACH_NAME "." PENTRIES ";"
    "INSERT INTO " SUMMARY " SELECT s.name || '/' || sub.name, sub.type, sub.inode, sub.mode, sub.nlink, sub.uid, sub.gid, sub.size, sub.blksize, sub.blocks, sub.atime, sub.mtime, sub.ctime, sub.linkname, sub.xattr_names, sub.totfiles, sub.totlinks, sub.minuid, sub.maxuid, sub.mingid, sub.maxgid, sub.minsize, sub.maxsize, sub.totltk, sub.totmtk, sub.totltm, sub.totmtm, sub.totmtg, sub.totmtt, sub.totsize, sub.minctime, sub.maxctime, sub.minmtime, sub.maxmtime, sub.minatime, sub.maxatime, sub.minblocks, sub.maxblocks, sub.totxattr, sub.depth + 1, sub.mincrtime, sub.maxcrtime, sub.minossint1, sub.maxossint1, sub.totossint1, sub.minossint2, sub.maxossint2, sub.totossint2, sub.minossint3, sub.maxossint3, sub.totossint3, sub.minossint4, sub.maxossint4, sub.totossint4, sub.rectype, sub.pinode, 0, sub.rollupscore FROM " SUMMARY " AS s, " SUBDIR_ATTACH_NAME "." SUMMARY " AS sub WHERE s.isroot == 1;"
    "INSERT INTO " XATTRS_ROLLUP      " SELECT * FROM " SUBDIR_ATTACH_NAME "." XATTRS_AVAIL ";"
    "INSERT INTO " XATTR_FILES_ROLLUP " SELECT * FROM " SUBDIR_ATTACH_NAME "." XATTR_FILES  ";"
    "SELECT * FROM " SUBDIR_ATTACH_NAME "." XATTR_FILES ";";

struct CallbackArgs {
    struct template_db *xattr;
    char *parent;
    size_t parent_len;
    char *child;
    size_t child_len;
};

/* use this callback to create xattr db files and insert filenames */
/* db.db doesn't need to be modified since the SQL statement already did it */
static int rollup_xattr_dbs_callback(void *args, int count, char **data, char **columns) {
    struct CallbackArgs *ca     = (struct CallbackArgs *) args;
    const char  *uid_str        = data[0];
    const char  *gid_str        = data[1];
    const char  *filename       = data[2];
    const size_t filename_len   = strlen(filename);
    const char  *attachname     = data[3];
    uid_t uid;
    gid_t gid;

    sscanf(uid_str, "%d", &uid); /* skip checking for failure */
    sscanf(gid_str, "%d", &gid); /* skip checking for failure */

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
    sqlite3 *xattr_db = opendb(xattr_db_name, SQLITE_OPEN_READWRITE, 0, 0,
                               NULL, NULL
                               #if defined(DEBUG) && defined(PER_THREAD_STATS)
                               , NULL, NULL
                               , NULL, NULL
                               #endif
        );
    if (!xattr_db) {
        return 1;
    }

    char child_xattr_db_name[MAXPATH];
    SNFORMAT_S(child_xattr_db_name, MAXPATH, 3,
               ca->child, ca->child_len,
               "/", (size_t) 1,
               filename, filename_len);

    if (!attachdb(child_xattr_db_name, xattr_db, attachname, SQLITE_OPEN_READONLY, 1)) {
        closedb(xattr_db);
        return 1;
    }

    char insert[MAXSQL];
    SNPRINTF(insert, MAXSQL, "INSERT INTO %s SELECT * FROM %s.%s;",
             XATTRS_ROLLUP, attachname, XATTRS_AVAIL);
    char *err = NULL;
    int rc = sqlite3_exec(xattr_db, insert, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: Failed to copy \"%s\" xattrs into xattrs_rollup of %s: %s\n",
                               child_xattr_db_name, xattr_db_name, err);
    }

    sqlite3_free(err);
    closedb(xattr_db);
    return 0;
}

/*
@return -1 - could not move entries into pentries
         0 - success
         1 - at least one child failed to be moved
*/
int do_rollup(struct RollUp *rollup,
              struct DirStats *ds,
              sqlite3 *dst,
              struct template_db *xattr_template
              timestamp_sig) {
    /* assume that this directory can be rolled up */
    /* can_rollup should have been called earlier  */

    /* if (!rollup || !dst) { */
    /*     return -1; */
    /* } */

    timestamp_create_buffer(4096);
    timestamp_start(do_roll_up);

    #ifdef DEBUG
    #ifdef PER_THREAD_STATS
    const size_t id = rollup->data.tid.up;
    #endif
    #endif

    int rc = 0;
    char *err = NULL;
    int exec_rc = SQLITE_OK;

    /* set the rollup score in the SQL statement */
    char rollup_current_dir[] = "UPDATE " SUMMARY " SET rollupscore = 0;";
    rollup_current_dir[sizeof(rollup_current_dir) - sizeof("0;")] += ds->score;

    timestamp_start(rollup_current_dir);
    exec_rc = sqlite3_exec(dst, rollup_current_dir, NULL, NULL, &err);
    timestamp_end(timestamp_buffers, id, "rollup_current_dir", rollup_current_dir);

    if (exec_rc != SQLITE_OK) {
        fprintf(stderr, "Error: Failed to copy \"%s\" entries into pentries table: %s\n", rollup->data.name, err);
        rc = -1;
        goto end_rollup;
    }

    /* process each child */
    timestamp_start(rollup_subdirs);

    sll_loop(&rollup->data.subdirs, node) {
        timestamp_start(rollup_subdir);

        struct BottomUp *child = (struct BottomUp *) sll_node_data(node);

        char child_dbname[MAXPATH];
        SNFORMAT_S(child_dbname, MAXPATH, 3, child->name, child->name_len, "/", 1, DBNAME, DBNAME_LEN);

        /* attach subdir database file as 'SUBDIR_ATTACH_NAME' */
        rc = !attachdb(child_dbname, dst, SUBDIR_ATTACH_NAME, SQLITE_OPEN_READONLY, 1);

        /* roll up the subdir into this dir */
        if (!rc) {
            timestamp_start(rollup_subdir);
            struct CallbackArgs ca = {
                .xattr      = xattr_template,
                .parent     = rollup->data.name,
                .parent_len = rollup->data.name_len,
                .child      = child->name,
                .child_len  = child->name_len,
            };

            exec_rc = sqlite3_exec(dst, rollup_subdir, rollup_xattr_dbs_callback, &ca, &err);
            timestamp_end(timestamp_buffers, id, "rollup_subdir", rollup_subdir);
            if (exec_rc != SQLITE_OK) {
                fprintf(stderr, "Error: Failed to copy \"%s\" subdir pentries into pentries table: %s\n", child->name, err);
            }
        }

        /* always detach subdir */
        detachdb(child_dbname, dst, SUBDIR_ATTACH_NAME);

        timestamp_end(timestamp_buffers, id, "rollup_subdir", rollup_subdir);

        if (rc) {
            break;
        }
    }

    timestamp_end(timestamp_buffers, id, "rollup_subdirs", rollup_subdirs);

end_rollup:
    sqlite3_free(err);

    timestamp_end(timestamp_buffers, id, "do_rollup", do_roll_up);
    return rc;
}

void rollup(void *args timestamp_sig) {
    timestamp_create_buffer(4096);

    timestamp_start(setup);

    struct RollUp *dir = (struct RollUp *) args;
    dir->rolledup = 0;

    const size_t id = dir->data.tid.up;

    char dbname[MAXPATH];
    SNFORMAT_S(dbname, MAXPATH, 3,
               dir->data.name, dir->data.name_len,
               "/", (size_t) 1,
               DBNAME, DBNAME_LEN);

    struct ThreadArgs *ta = dir->data.extra_args;
    struct RollUpStats *stats = ta->stats;

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

    timestamp_end(timestamp_buffers, id, "setup", setup);

    /* open the database file here to reduce number of open calls */
    timestamp_start(open_curr_db);
    sqlite3 *dst = opendb(dbname, SQLITE_OPEN_READWRITE, 1, 0
                           , NULL, NULL
                           #if defined(DEBUG) && defined(PER_THREAD_STATS)
                           , NULL, NULL
                           , NULL, NULL
                           #endif
        );
    timestamp_end(timestamp_buffers, id, "opendb", open_curr_db);

    /* can attempt to roll up */
    if (dst) {
        /* check if rollup is allowed */
        ds->score = can_rollup(dir, ds, dst timestamp_args);

        /* if can roll up */
        if (ds->score > 0) {
            /* do the roll up */
            if (in.dry_run) {
                ds->success = 1;
            }
            else {
                ds->success = (do_rollup(dir, ds, dst, &ta->xattr_template timestamp_args) == 0);
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
        }
    }
    else {
        /* did not check if can roll up */
        sll_push(&stats[id].not_processed, ds);

        stats[id].remaining++;
    }

    closedb(dst);
}

void sub_help() {
   printf("GUFI_index        GUFI index to roll up\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    epoch = since_epoch(NULL);

    timestamp_start_raw(runtime);

    int idx = parse_cmd_line(argc, argv, "hHn:L:X", 1, "GUFI_index ...", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;

    struct ThreadArgs ta;
    init_template_db(&ta.xattr_template);
    if (create_xattrs_template(&ta.xattr_template) != 0) {
        fprintf(stderr, "Could not create xattr template file\n");
        return -1;
    }

    #ifdef DEBUG
    pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;

    #ifdef PER_THREAD_STATS
    timestamp_init(timestamp_buffers, in.maxthreads + 1, 1024 * 1024, &print_mutex);
    #endif
    #endif

    ta.stats = calloc(in.maxthreads, sizeof(struct RollUpStats));
    for(int i = 0; i < in.maxthreads; i++) {
        sll_init(&ta.stats[i].not_processed);
        sll_init(&ta.stats[i].not_rolled_up);
        sll_init(&ta.stats[i].rolled_up);
        ta.stats[i].remaining = 0;
    }

    #ifdef DEBUG
    struct OutputBuffers print_buffers;
    OutputBuffers_init(&print_buffers, in.maxthreads, 1024 * 1024, &print_mutex);

    for(int i = 0; i < in.maxthreads; i++) {
        ta.stats[i].print_buffers = &print_buffers;
    }
    #endif

    argv += idx;
    argc -= idx;

    const int rc = parallel_bottomup(argv, argc,
                                     in.maxthreads,
                                     sizeof(struct RollUp),
                                     NULL, rollup,
                                     0,
                                     &ta
                                     #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                     , timestamp_buffers
                                     #endif
        );

    #ifdef DEBUG
    OutputBuffers_flush_to_single(&print_buffers, stderr);
    OutputBuffers_destroy(&print_buffers);

    #ifdef PER_THREAD_STATS
    timestamp_destroy(timestamp_buffers);
    #endif

    #endif

    print_stats(argv, argc, ta.stats, in.maxthreads);

    for(int i = 0; i < in.maxthreads; i++) {
        #ifdef DEBUG
        ta.stats[i].print_buffers = NULL;
        #endif
        sll_destroy(&ta.stats[i].rolled_up, 0);
        sll_destroy(&ta.stats[i].not_rolled_up, 0);
        sll_destroy(&ta.stats[i].not_processed, 0);
    }
    free(ta.stats);
    close_template_db(&ta.xattr_template);

    timestamp_set_end_raw(runtime);
    fprintf(stderr, "Took %.2Lf seconds\n", sec(nsec(&runtime)));

    return rc;
}
