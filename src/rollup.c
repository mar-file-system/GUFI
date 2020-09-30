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

    int    score;           /* roll up score regardless of success or failure */
    int    success;         /* whether or not the roll up succeeded */
};

/* per thread stats */
struct RollUpStats {
    struct sll not_processed;
    struct sll not_rolled_up;
    struct sll rolled_up;

    size_t remaining;       /* subdirectories that remain after rolling up */

    #ifdef DEBUG
    struct OutputBuffers * print_buffers;
    #endif
};

size_t rollup_distribution(const char * name, size_t * distribution) {
    size_t total = 0;
    fprintf(stderr, "    %s:\n", name);
    for(size_t i = 1; i < 6; i++) {
        fprintf(stderr, "        %zu: %19zu\n", i, distribution[i]);
        total += distribution[i];
    }
    fprintf(stderr, "        Total: %15zu\n", total);
    return total;
}

/* compare function for qsort */
int compare_size_t(const void * lhs, const void * rhs) {
    return (*(size_t *) lhs - *(size_t *) rhs);
}

#define sll_dir_stats(name, all, var, threads, width)                   \
do {                                                                    \
    const size_t count = sll_get_size(all);                             \
    if (count == 0) {                                                   \
        break;                                                          \
    }                                                                   \
                                                                        \
    size_t * array = malloc(count * sizeof(size_t));                    \
    size_t min = (size_t) -1;                                           \
    size_t max = 0;                                                     \
    size_t sum = 0;                                                     \
    size_t i = 0;                                                       \
    sll_loop(all, node) {                                               \
        struct DirStats * ds = (struct DirStats *) sll_node_data(node); \
        if (ds->var < min) {                                            \
            min = ds->var;                                              \
        }                                                               \
                                                                        \
        if (ds->var > max) {                                            \
            max = ds->var;                                              \
        }                                                               \
                                                                        \
        sum += ds->var;                                                 \
        array[i++] = ds->var;                                           \
    }                                                                   \
                                                                        \
    qsort(array, count, sizeof(size_t), compare_size_t);                \
    const size_t half = count / 2;                                      \
    double median = array[half];                                        \
    if (count % 2 == 0) {                                               \
        median += array[half - 1];                                      \
        median /= 2;                                                    \
    }                                                                   \
                                                                        \
    const double avg = ((double) sum) / count;                          \
                                                                        \
    fprintf(stderr, "    %s:\n", name);                                 \
    fprintf(stderr, "        min:        %" #width "zu\n",  min);       \
    fprintf(stderr, "        max:        %" #width "zu\n",  max);       \
    fprintf(stderr, "        median:     %" #width ".2f\n", median);    \
    fprintf(stderr, "        sum:        %" #width "zu\n",  sum);       \
    fprintf(stderr, "        average:    %" #width ".2f\n", avg);       \
                                                                        \
    free(array);                                                        \
} while (0)

void print_stanza(const char * name, struct sll * stats, const size_t threads) {
    fprintf(stderr, "%s %*zu\n", name, (int) (29 - strlen(name)), sll_get_size(stats));
    sll_dir_stats("Subdirectories", stats, subdir_count,    threads, 10);
    sll_dir_stats("Files/Links",    stats, subnondir_count, threads, 10);
    sll_dir_stats("Level",          stats, level,           threads, 10);
    fprintf(stderr, "\n");
}

/* this function moves the sll stats up and deallocates them */
void print_stats(char ** paths, const int path_count, struct RollUpStats * stats, const size_t threads) {
    fprintf(stderr, "Roots:\n");
    for(int i = 0; i < path_count; i++) {
        fprintf(stderr, "    %s\n", paths[i]);
    }

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

    /* print stats of each type of directory */
    print_stanza("Not Processed:", &not_processed, threads);
    print_stanza("Not Rolled Up:", &not_rolled_up, threads);
    print_stanza("Rolled Up:",     &rolled_up,     threads);

    /* get distribution of roll up scores */
    size_t successful[6] = {0};
    size_t failed[6] = {0};
    sll_loop(&rolled_up, node) {
        struct DirStats * ds = (struct DirStats *) sll_node_data(node);
        (ds->success?successful:failed)[ds->score]++;
    }

    /* count empty directories */

    /* do not count not_processed, since they do not have databases to scan */
    size_t not_processed_nondirs = 0;
    size_t not_processed_empty = 0;

    size_t not_rolled_up_nondirs = 0;
    size_t not_rolled_up_empty = 0;
    sll_loop(&not_rolled_up, node) {
        struct DirStats * ds = (struct DirStats *) sll_node_data(node);
        not_rolled_up_nondirs += ds->subnondir_count;
        not_rolled_up_empty += !ds->subnondir_count;
    }

    size_t rolled_up_nondirs = 0;
    size_t rolled_up_empty = 0;
    sll_loop(&rolled_up, node) {
        struct DirStats * ds = (struct DirStats *) sll_node_data(node);
        rolled_up_nondirs += ds->subnondir_count;
        rolled_up_empty += !ds->subnondir_count;
    }

    rollup_distribution("Successful", successful);
    rollup_distribution("Failed",     failed);

    const size_t total_nondirs = not_processed_nondirs +
                                 not_rolled_up_nondirs +
                                 rolled_up_nondirs;

    const size_t empty = not_processed_empty +
                         not_rolled_up_empty +
                         rolled_up_empty;

    const size_t total_dirs = sll_get_size(&not_processed) +
                              sll_get_size(&not_rolled_up) +
                              sll_get_size(&rolled_up);

    fprintf(stderr, "Files/Links:    %zu\n", total_nondirs);
    fprintf(stderr, "Directories:    %zu (%zu empty)\n", total_dirs, empty);
    fprintf(stderr, "Total:          %zu\n", total_nondirs + total_dirs);
    fprintf(stderr, "Remaining Dirs: %zu (%.2Lf%%)\n", remaining, (100 * (long double) remaining) / total_dirs);

    sll_destroy(&rolled_up, free);
    sll_destroy(&not_rolled_up, free);
    sll_destroy(&not_processed, free);
}

/* print the result of a single roll up */
/* name level score success */
#if defined(DEBUG) && defined(PRINT_ROLLUP_SCORE)
static inline
void print_result(struct OutputBuffers * obufs, const size_t id,
                  const char * name, const size_t level,
                  const int score, const size_t success) {
    char result[MAXSQL];
    const size_t result_len = SNPRINTF(result, MAXSQL, " %zu 0 0\n", level);

    struct OutputBuffer * obuf = &(obufs->buffers[id]);

    const size_t name_len = strlen(name);
    const size_t len = name_len + result_len;

    /* not enough space remaining, so clear out buffer */
    if ((obuf->capacity - obuf->filled) < len) {
        pthread_mutex_lock(&obufs->mutex);
        fwrite(obuf->buf, sizeof(char), obuf->filled, stderr);
        obuf->filled = 0;
        pthread_mutex_unlock(&obufs->mutex);
    }

    /* not enough space in entire buffer, so write directly */
    if (len >= obuf->capacity) {
        pthread_mutex_lock(&obufs->mutex);
        fwrite(obuf->buf, sizeof(char), obuf->filled, stderr);
        pthread_mutex_unlock(&obufs->mutex);
    }

    /* print name */
    memcpy(obuf->buf + obuf->filled, name, name_len);

    /* set results string */
    result[result_len - 4] += score;
    result[result_len - 2] += success;

    /* print score and success */
    memcpy(obuf->buf + obuf->filled + name_len, result, result_len);

    obuf->filled += len;
}
#endif

/* main data being passed around during walk */
struct RollUp {
    struct BottomUp data;
    int rolledup;
};

/* ************************************** */
/* get permissions from directory entries */
const char PERM_SQL[] = "SELECT mode, uid, gid FROM summary WHERE isroot == 1";

struct Permissions {
    mode_t mode;
    uid_t uid;
    gid_t gid;
};

int get_permissions(void * args, int count, char ** data, char ** columns) {
    if (count != 3) {
        return 1;
    }

    struct Permissions * perms = (struct Permissions *) args;

    perms->mode = atoi(data[0]);
    perms->uid  = atoi(data[1]);
    perms->gid  = atoi(data[2]);

    return 0;
}

/*
@return -1 - failed to open a database
         0 - do not roll up
         1 - self and subdirectories are all o+rx
         2 - self and subdirectories have same user and group permissions, uid, and gid, and top is o-rx
         3 - self and subdirectories have same user permissions, go-rx, uid
         4 - self and subdirectories have same user, group, and others permissions, uid, and gid
         5 - leaf directory
*/
int check_permissions(struct Permissions * curr, const size_t child_count, struct sll * child_list, const size_t id timestamp_sig) {
    if (child_count == 0) {
        return 5;
    }

    timestamp_create_buffer(4096);

    struct Permissions * child_perms = malloc(sizeof(struct Permissions) * sll_get_size(child_list));

    /* get permissions of each child */
    size_t idx = 0;
    sll_loop(child_list, node) {
        struct RollUp * child = (struct RollUp *) sll_node_data(node);

        char dbname[MAXPATH] = {0};
        SNPRINTF(dbname, MAXPATH, "%s/" DBNAME, child->data.name);

        timestamp_start(open_child_db);
        sqlite3 * db = opendb(dbname, SQLITE_OPEN_READONLY, 1, 0
                              , NULL, NULL
                              #if defined(DEBUG) && defined(PER_THREAD_STATS)
                              , NULL, NULL
                              , NULL, NULL
                              #endif
            );
        timestamp_end(timestamp_buffers, id, "open_child_db", open_child_db);

        if (!db) {
            break;
        }

        /* get the child directory's permissions */
        timestamp_start(get_child_perms);
        char * err = NULL;
        const int exec_rc = sqlite3_exec(db, PERM_SQL, get_permissions, &child_perms[idx], &err);
        timestamp_end(timestamp_buffers, id, "get_child_perms", get_child_perms);

        timestamp_start(close_child_db);
        closedb(db);
        timestamp_end(timestamp_buffers, id, "close_child_db", close_child_db);

        if (exec_rc != SQLITE_OK) {
            fprintf(stderr, "Error: Could not get permissions of child directory \"%s\": %s\n", child->data.name, err);

            sqlite3_free(err);
            break;
        }

        sqlite3_free(err);

        idx++;
    }

    if (child_count != idx) {
        free(child_perms);
        return -1;
    }

    size_t o_plus_rx = 0;
    size_t ugo = 0;
    size_t ug = 0;
    size_t u = 0;
    for(size_t i = 0; i < child_count; i++) {
        /* self and subdirectories are all o+rx */
        o_plus_rx += (((curr->mode          & 0005) == 0005) &&
                      ((child_perms[i].mode & 0005) == 0005) &&
                      (curr->uid == child_perms[i].uid) &&
                      (curr->gid == child_perms[i].gid));

        /* self and subdirectories have same user, group, and others permissions, uid, and gid */
        ugo += (((curr->mode & 0555) == (child_perms[i].mode & 0555)) &&
                (curr->uid == child_perms[i].uid) &&
                (curr->gid == child_perms[i].gid));

        /* self and subdirectories have same user and group permissions, uid, and gid, and top is o-rx */
        ug += (((curr->mode & 0550) == (child_perms[i].mode & 0550)) &&
                (curr->uid == child_perms[i].uid) &&
                (curr->gid == child_perms[i].gid));

        /* self and subdirectories have same user permissions, go-rx, uid */
        u += (((curr->mode & 0500) == (child_perms[i].mode & 0500)) &&
              (curr->uid == child_perms[i].uid));
    }

    free(child_perms);

    if (o_plus_rx == child_count) {
        return 1;
    }

    if (ugo == child_count) {
        return 4;
    }

    if (ug == child_count) {
        return 2;
    }

    if (u == child_count) {
        return 3;
    }

    return 0;
}
/* ************************************** */

/* check if the current directory can be rolled up */
/*
@return   0 - cannot rollup
        > 0 - rollup score
*/
int can_rollup(struct RollUp * rollup,
               struct DirStats * ds,
               sqlite3 * dst
               timestamp_sig) {
    /* if (!rollup || !ds || !dst) { */
    /*     return -1; */
    /* } */

    char * err = NULL;

    timestamp_create_buffer(4096);
    timestamp_start(can_roll_up);

    /* default to cannot roll up */
    int legal = 0;

    /* if a directory has too many immediate files/links, don't roll up */
    if (ds->subnondir_count >= in.max_in_dir) {
        goto end_can_rollup;
    }

    /* all subdirectories are expected to pass */
    size_t total_subdirs = 0;

    /* check if ALL subdirectories have been rolled up */
    timestamp_start(check_subdirs_rolledup);
    size_t rolledup = 0;
    sll_loop(&rollup->data.subdirs, node) {
        struct RollUp * child = (struct RollUp *) sll_node_data(node);
        rolledup += child->rolledup;
        total_subdirs++;
    }
    timestamp_end(timestamp_buffers, rollup->data.tid.up, "check_subdirs_rolledup", check_subdirs_rolledup);

    /* not all subdirectories were rolled up, so cannot roll up */
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

    /* check if the permissions of this directory and its subdirectories match */
    timestamp_start(check_perms);
    legal = check_permissions(&perms, total_subdirs, &rollup->data.subdirs, rollup->data.tid.up timestamp_args);
    timestamp_end(timestamp_buffers, rollup->data.tid.up, "check_perms", check_perms);

end_can_rollup:
    sqlite3_free(err);

    timestamp_end(timestamp_buffers, rollup->data.tid.up, "can_rollup", can_roll_up);

    return legal;
}

int add_entries_count(void * args, int count, char ** data, char ** columns) {
    size_t * rows = (size_t *) args;
    *rows += atoi(data[0]);
    return 0;
}

/* drop pentries view */
/* create pentries table */
/* copy entries + pinode into pentries */
/* define here to be able to duplicate the SQL at compile time */
#define ROLLUP_CURRENT_DIR \
    "DROP VIEW IF EXISTS pentries;" \
    "CREATE TABLE pentries AS SELECT entries.*, summary.inode AS pinode FROM summary, entries;" /* "CREATE TABLE AS" already inserts rows, so no need to explicitly insert rows */ \
    "UPDATE summary SET rollupscore = 0;"

/* location of the 0 in ROLLUP_CURRENT_DIR */
static const size_t rollup_score_offset = sizeof(ROLLUP_CURRENT_DIR) - sizeof("0;");

/* copy subdirectory pentries into pentries */
/* copy subdirectory summary into summary */
static const char rollup_subdir[] =
    "INSERT INTO pentries SELECT * FROM " SUBDIR_ATTACH_NAME ".pentries;"
    "INSERT INTO summary  SELECT NULL, s.name || '/' || sub.name, sub.type, sub.inode, sub.mode, sub.nlink, sub.uid, sub.gid, sub.size, sub.blksize, sub.blocks, sub.atime, sub.mtime, sub.ctime, sub.linkname, sub.xattrs, sub.totfiles, sub.totlinks, sub.minuid, sub.maxuid, sub.mingid, sub.maxgid, sub.minsize, sub.maxsize, sub.totltk, sub.totmtk, sub.totltm, sub.totmtm, sub.totmtg, sub.totmtt, sub.totsize, sub.minctime, sub.maxctime, sub.minmtime, sub.maxmtime, sub.minatime, sub.maxatime, sub.minblocks, sub.maxblocks, sub.totxattr, sub.depth, sub.mincrtime, sub.maxcrtime, sub.minossint1, sub.maxossint1, sub.totossint1, sub.minossint2, sub.maxossint2, sub.totossint2, sub.minossint3, sub.maxossint3, sub.totossint3, sub.minossint4, sub.maxossint4, sub.totossint4, sub.rectype, sub.pinode, 0, sub.rollupscore FROM summary as s, " SUBDIR_ATTACH_NAME ".summary as sub WHERE s.isroot == 1;";

/* get number of non-dirs in this directory from the entries table */
int get_nondirs(struct RollUp * rollup, struct DirStats * ds, sqlite3 * dst timestamp_sig) {
    char * err = NULL;
    const int exec_rc = sqlite3_exec(dst, "SELECT COUNT(*) FROM entries", add_entries_count, &ds->subnondir_count, &err);

    if (exec_rc != SQLITE_OK) {
        fprintf(stderr, "Warning: Failed to get entries row count from \"%s\": %s\n", rollup->data.name, err);
    }
    sqlite3_free(err);
    return exec_rc;
}

/*
@return -1 - could not move entries into pentries
         0 - success
         1 - at least one subdirectory failed to be moved
*/
int do_rollup(struct RollUp * rollup,
              struct DirStats * ds,
              sqlite3 * dst
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
    char * err = NULL;
    int exec_rc = SQLITE_OK;

    /* set the rollup score in the SQL statement */
    char rollup_current_dir[] = ROLLUP_CURRENT_DIR;
    rollup_current_dir[rollup_score_offset] += ds->score;

    timestamp_start(rollup_current_dir);
    exec_rc = sqlite3_exec(dst, rollup_current_dir, NULL, NULL, &err);
    timestamp_end(timestamp_buffers, id, "rollup_current_dir", rollup_current_dir);

    if (exec_rc != SQLITE_OK) {
        fprintf(stderr, "Error: Failed to copy \"%s\" entries into pentries table: %s\n", rollup->data.name, err);
        rc = -1;
        goto end_rollup;
    }

    /* process each subdirectory */
    timestamp_start(rollup_subdirs);

    sll_loop(&rollup->data.subdirs, node) {
        timestamp_start(rollup_subdir);

        struct BottomUp * child = (struct BottomUp *) sll_node_data(node);

        char child_dbname[MAXPATH];
        SNFORMAT_S(child_dbname, MAXPATH, 3, child->name, strlen(child->name), "/", 1, DBNAME, DBNAME_LEN);

        /* attach subdir database file as 'SUBDIR_ATTACH_NAME' */
        rc = !attachdb(child_dbname, dst, SUBDIR_ATTACH_NAME, SQLITE_OPEN_READONLY);

        /* roll up the subdir into this dir */
        if (!rc) {
            timestamp_start(rollup_subdir);
            exec_rc = sqlite3_exec(dst, rollup_subdir, NULL, NULL, &err);
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

void rollup(void * args timestamp_sig) {
    timestamp_create_buffer(4096);

    timestamp_start(setup);

    struct RollUp * dir = (struct RollUp *) args;
    dir->rolledup = 0;

    const size_t id = dir->data.tid.up;
    const size_t name_len = strlen(dir->data.name);

    char dbname[MAXPATH];
    SNPRINTF(dbname, MAXPATH, "%s/" DBNAME, dir->data.name);

    struct RollUpStats * stats = (struct RollUpStats *) dir->data.extra_args;

    /* get statistics out of BottomUp */
    struct DirStats * ds = malloc(sizeof(struct DirStats));
    SNFORMAT_S(ds->path, MAXPATH, 1, dir->data.name, name_len);
    ds->level = dir->data.level;
    ds->subdir_count = dir->data.subdir_count;
    ds->subnondir_count = 0;
    ds->score = 0;
    ds->success = 1;

    timestamp_end(timestamp_buffers, id, "setup", setup);

    /* open the database file here to reduce number of open calls */
    timestamp_start(open_curr_db);
    sqlite3 * dst = opendb(dbname, SQLITE_OPEN_READWRITE, 1, 0
                           , NULL, NULL
                           #if defined(DEBUG) && defined(PER_THREAD_STATS)
                           , NULL, NULL
                           , NULL, NULL
                           #endif
        );
    timestamp_end(timestamp_buffers, id, "opendb", open_curr_db);

    /* can attempt to roll up */
    if (dst) {
        /* get count of number of non-directories that will be affected */
        timestamp_start(nondir_count);
        get_nondirs(dir, ds, dst timestamp_args);
        timestamp_end(timestamp_buffers, id, "nondir_count", nondir_count);

        /* check if rollup is allowed */
        ds->score = can_rollup(dir, ds, dst timestamp_args);

        /* if can roll up */
        if (ds->score > 0) {
            /* do the roll up */
            if (in.dry_run) {
                ds->success = 1;
            }
            else {
                ds->success = (do_rollup(dir, ds, dst timestamp_args) == 0);
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

            /* count only subdirectories that were rolled up */
            /* in order to not double count */
            sll_loop(&dir->data.subdirs, node) {
                struct RollUp * child = (struct RollUp *) sll_node_data(node);
                if (child->rolledup) {
                    stats[id].remaining++;
                }
            }
        }

        #if defined(DEBUG) && defined(PRINT_ROLLUP_SCORE)
        print_result(stats->print_buffers, id,
                     dir->data.name, ds->level,
                     ds->score, ds->success);
        #endif
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

int main(int argc, char * argv[]) {
    epoch = since_epoch(NULL);

    timestamp_start_raw(runtime);

    int idx = parse_cmd_line(argc, argv, "hHn:L:X", 1, "GUFI_index ...", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;

    #ifdef DEBUG
    pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;

    #ifdef PER_THREAD_STATS
    timestamp_init(timestamp_buffers, in.maxthreads + 1, 1024 * 1024, &print_mutex);
    #endif
    #endif

    struct RollUpStats * stats = calloc(in.maxthreads, sizeof(struct RollUpStats));
    for(int i = 0; i < in.maxthreads; i++) {
        sll_init(&stats[i].not_processed);
        sll_init(&stats[i].not_rolled_up);
        sll_init(&stats[i].rolled_up);
        stats[i].remaining = 0;
    }

    #ifdef DEBUG
    struct OutputBuffers print_buffers;
    OutputBuffers_init(&print_buffers, in.maxthreads, 1024 * 1024, &print_mutex);

    for(int i = 0; i < in.maxthreads; i++) {
        stats[i].print_buffers = &print_buffers;
    }
    #endif

    argv += idx;
    argc -= idx;

    const int rc = parallel_bottomup(argv, argc,
                                     in.maxthreads,
                                     sizeof(struct RollUp), rollup,
                                     0,
                                     stats
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

    print_stats(argv, argc, stats, in.maxthreads);

    for(int i = 0; i < in.maxthreads; i++) {
        #ifdef DEBUG
        stats[i].print_buffers = NULL;
        #endif
        sll_destroy(&stats[i].rolled_up, 0);
        sll_destroy(&stats[i].not_rolled_up, 0);
        sll_destroy(&stats[i].not_processed, 0);
    }
    free(stats);

    timestamp_set_end_raw(runtime);
    fprintf(stderr, "Took %.2Lf seconds\n", elapsed(&runtime));

    return rc;
}
