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
#include <stddef.h>
#include <string.h>

#include "BottomUp.h"
#include "dbutils.h"
#include "external_attach.h"
#include "rollup.h"

int get_permissions_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    struct Permissions *perms = (struct Permissions *) args;
    return !(
        (sscanf(data[0], "%" STAT_mode, &perms->mode) == 1) &&
        (sscanf(data[1], "%" STAT_uid,  &perms->uid)  == 1) &&
        (sscanf(data[2], "%" STAT_gid,  &perms->gid)  == 1)
    );
}

int get_permissions(sqlite3 *db, const char *name, struct Permissions *perms) {
    char *err = NULL;
    if (sqlite3_exec(db,
                     "SELECT mode, uid, gid "
                     "FROM " SUMMARY, get_permissions_callback, perms, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Could not get permissions of \"%s\": %s\n",
                                  name, err);
        return -1;
    }
    return 0;
}

/*
 * compare the permission of the parent directory and a single child
 *
 * still need to make sure ALL children can roll up, not just one -
 * this is just a primitive
 *
 * before calling this function, make sure the child itself can roll
 * up or is already rolled up
 */
size_t check_pair_permissions(struct Permissions *parent,
                              struct Permissions *child) {
    /* so long as one condition is true, child can be rolled up into the parent */
    return (
        /* parent and child are o+rx */
        (((parent->mode & 0005) == 0005) &&
         ((child->mode  & 0005) == 0005) &&
         (parent->uid == child->uid) &&
         (parent->gid == child->gid)) ||

        /* parent and child have same user, group, and others permissions, uid, and gid */
        (((parent->mode & 0555) == (child->mode & 0555)) &&
         (parent->uid == child->uid) &&
         (parent->gid == child->gid)) ||

        /* parent and child have same user and group permissions, uid, and gid, and top is o-rx */
        (((parent->mode & 0550) == (child->mode & 0550)) &&
         (parent->uid == child->uid) &&
         (parent->gid == child->gid)) ||

        /* parent and child have same user permissions, go-rx, uid */
        (((parent->mode & 0500) == (child->mode & 0500)) &&
         (parent->uid == child->uid))
    );
}

struct PermCanRollup {
    struct Permissions perms;
    int canrollup;
};

static int get_permissions_and_canrollup_callback(void *args, int count, char **data, char **columns) {
    struct PermCanRollup *pcr = (struct PermCanRollup *) args;
    const int rc = get_permissions_callback(&pcr->perms, count, data, columns);
    if (rc != 0) {
        return rc;
    }
    return !(sscanf(data[3], "%d", &pcr->canrollup) == 1);
}

static int get_permissions_canrollup(sqlite3 *db, const char *name, struct PermCanRollup *pcr) {
    char *err = NULL;
    if (sqlite3_exec(db,
                     "SELECT mode, uid, gid, canrollup "
                     "FROM " SUMMARY " WHERE isroot == 1;",
                     get_permissions_and_canrollup_callback, pcr, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Could not get permissions of \"%s\": %s\n",
                                  name, err);
        return -1;
    }
    return 0;
}

const char ROLLUP_CLEANUP[] =
    "DROP INDEX IF EXISTS " SUMMARY "_idx; "
    "DELETE FROM " PENTRIES_ROLLUP "; "
    "DELETE FROM " SUMMARY " WHERE isroot != 1; "
    "DELETE FROM " XATTRS_ROLLUP "; "
    "SELECT filename FROM " EXTERNAL_DBS_ROLLUP " WHERE type == '" EXTERNAL_TYPE_XATTR_NAME "'; "
    "DELETE FROM " EXTERNAL_DBS_ROLLUP "; "
    /* "VACUUM; " */
    "UPDATE " SUMMARY " SET canrollup = 1, isrolledup = 0;"; /* keep this last to allow it to be modified easily */
                                                             /* set canrollup = 1 because this SQL should only be used on directories that have been rolled up */

const size_t ROLLUP_CLEANUP_SIZE = sizeof(ROLLUP_CLEANUP);

static int count_pwd(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    size_t *xattr_count = (size_t *) args;
    sscanf(data[0], "%zu", xattr_count); /* skip check */
    return 0;
}

/* Delete all entries in the XATTR_ROLLUP table */
int xattrs_rollup_cleanup(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    str_t *name = (str_t *) args;

    char *relpath = data[0];
    const size_t relpath_len = strlen(relpath);

    const size_t fullpath_len = name->len + 1 + relpath_len;
    char *fullpath = malloc(fullpath_len + 1);
    SNFORMAT_S(fullpath, fullpath_len + 1, 3,
               name->data, name->len,
               "/", (size_t) 1,
               relpath, relpath_len);

    /* if the file is missing, return ok */
    struct stat st;
    if (stat(fullpath, &st) != 0) {
        free(fullpath);
        return (errno != ENOENT);
    }

    int rc = 0;

    sqlite3 *db = opendb(fullpath, SQLITE_OPEN_READWRITE, 0, 0, NULL, NULL);

    if (db) {
        char *err_msg = NULL;
        size_t xattr_count = 0;
        if (sqlite3_exec(db,
                         "DELETE FROM " XATTRS_ROLLUP "; "
                         "SELECT COUNT(*) FROM " XATTRS_PWD "; ",
                         count_pwd, &xattr_count, &err_msg) == SQLITE_OK) {
             /* remove empty per-user/per-group xattr db files */
             if (xattr_count == 0) {
                 if (remove(fullpath) != 0) {
                     const int err = errno;
                     fprintf(stderr, "Warning: Failed to remove empty xattr db file %s: %s (%d)\n",
                             fullpath, strerror(err), err);
                     rc = 1;
                 }
             }
        }
        else {
            sqlite_print_err_and_free(err_msg, stderr, "Warning: Failed to clear out rolled up xattr data from %s: %s\n", fullpath, err_msg);
            rc = 1;
        }
    }

    closedb(db);
    free(fullpath);

    return rc;
}

/* remove any existing roll up data for the target db */
int rollup_init(sqlite3 *db, const str_t *path) {
    /* Not checking arguments */

    /*
     * treesummary table is not cleared here
     * it was either already cleared or set by gufi_treesummary_all
     */
    char rollup_cleanup[ROLLUP_CLEANUP_SIZE];
    memcpy(rollup_cleanup, ROLLUP_CLEANUP, ROLLUP_CLEANUP_SIZE);

    /*
     * set isrolledup here instead of running 2 SQL statements
     * setting it to 0 here and then to 1 during copying
     */
    rollup_cleanup[sizeof(rollup_cleanup) - sizeof("1, isrolledup = 0;")] = '1';
    rollup_cleanup[sizeof(rollup_cleanup) - sizeof("0;")] = '1';

    char *err = NULL;
    if (sqlite3_exec(db, rollup_cleanup, xattrs_rollup_cleanup, (void *) path, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Failed to set up database for rollup: \"%s\": %s\n",
                                  path->data, err);
        return 1;
    }

    return 0;
}

char *rollup_child_attach(sqlite3 *db, const str_t *child_path, const int attach_flag) {
    const size_t child_dbname_len = child_path->len + 1 + DBNAME_LEN;
    char *child_dbname = malloc(child_dbname_len + 1);
    SNFORMAT_S(child_dbname, child_dbname_len + 1, 3,
               child_path->data, child_path->len,
               "/", (size_t) 1,
               DBNAME, DBNAME_LEN);

    /* attach subdir database file as 'SUBDIR_ATTACH_NAME' */
    if (attachdb(child_dbname, db, ROLLUP_SUBDIR_ATTACH_NAME, attach_flag, 1, NULL)) {
        return child_dbname;
    }

    free(child_dbname);
    return NULL;
}

/* use this callback to create xattr db files and insert filenames */
/* db.db doesn't need to be modified since the SQL statement already did it */
static int rollup_external_xattrs(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    rexa_t      *rexa           = (rexa_t *) args;
    const char  *filename       = data[0];
    const size_t filename_len   = strlen(filename);
    const char  *uid_str        = data[1];
    const char  *gid_str        = data[2];
    uid_t uid;
    gid_t gid;

    sscanf(uid_str, "%" STAT_uid, &uid); /* skip checking for failure */
    sscanf(gid_str, "%" STAT_gid, &gid); /* skip checking for failure */

    /* parent xattr db filename */
    const size_t xattr_db_name_len = rexa->parent_len + 1 + filename_len;
    char *xattr_db_name = malloc(xattr_db_name_len + 1);
    SNFORMAT_S(xattr_db_name, xattr_db_name_len + 1, 3,
               rexa->parent, rexa->parent_len,
               "/", (size_t) 1,
               filename, filename_len);

    /* check if the parent per-user/per-group xattr db file exists */
    struct stat st;
    if (stat(xattr_db_name, &st) != 0) {
        const int err = errno;
        if (err != ENOENT) {
            fprintf(stderr, "Error: Cannot access xattr db file %s: %s (%d)\n",
                    xattr_db_name, strerror(err), err);
            free(xattr_db_name);
            return 1;
        }

        /* copy the template file */
        if (copy_template(rexa->xattr, xattr_db_name, uid, gid, NULL)) {
            free(xattr_db_name);
            return 1;
        }
    }

    /* open parent per-user/per-group xattr db file */
    sqlite3 *xattr_db = opendb(xattr_db_name, SQLITE_OPEN_READWRITE, 0, 0, NULL, NULL);
    if (!xattr_db) {
        free(xattr_db_name);
        return 1;
    }

    const size_t child_xattr_db_name_len = rexa->child_len + 1 + filename_len;
    char *child_xattr_db_name = malloc(child_xattr_db_name_len + 1);
    SNFORMAT_S(child_xattr_db_name, child_xattr_db_name_len + 1, 3,
               rexa->child, rexa->child_len,
               "/", (size_t) 1,
               filename, filename_len);

    char attachname[MAXSQL];
    SNPRINTF(attachname, sizeof(attachname),
             EXTERNAL_ATTACH_PREFIX "%zu", (*rexa->count)++);

    if (!attachdb(child_xattr_db_name, xattr_db, attachname, SQLITE_OPEN_READONLY, 1, NULL)) {
        free(child_xattr_db_name);
        closedb(xattr_db);
        free(xattr_db_name);
        return 1;
    }

    /* copy child external xattrs_avail to external xattrs_rollup */
    char insert[MAXSQL];
    SNPRINTF(insert, sizeof(insert),
             "INSERT INTO " XATTRS_ROLLUP " SELECT * FROM %s." XATTRS_AVAIL ";",
             attachname);
    char *err = NULL;
    if (sqlite3_exec(xattr_db, insert, NULL, NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Failed to copy \"%s\" xattrs into " XATTRS_ROLLUP " of %s: %s\n",
                                  child_xattr_db_name, xattr_db_name, err);
    }

    free(child_xattr_db_name);
    closedb(xattr_db);
    free(xattr_db_name);
    return 0;
}

/* roll up the attached subdir into this dir */
int rollup_child_process(sqlite3 *db, const char *sql, rexa_t *rexa) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, rollup_external_xattrs, rexa, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr,
                                  "Error: Failed to copy subdir \"%s\" into \"%s\": %s\n",
                                  rexa->child, rexa->parent, err);
        return 1;
    }

    return 0;
}

int rollup_child_detach(sqlite3 *db, char *child_dbname) {
    detachdb(child_dbname, db, ROLLUP_SUBDIR_ATTACH_NAME, 1, NULL);

    free(child_dbname);

    return 0;
}

static int get_isrolledup_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    /* make sure canrollup is set before checking isrolledup */
    int canrollup = 0;
    if (sscanf(data[0], "%d", &canrollup) != 1) {
        return 1;
    }

    int *isrolledup = (int *) args;
    *isrolledup = 0;

    if (canrollup == 0) {
        return 0;
    }

    return !(sscanf(data[1], "%d", isrolledup) == 1);
}

int get_isrolledup(sqlite3 *db, int *isrolledup) {
    char *err = NULL;
    if (sqlite3_exec(db, "SELECT canrollup, isrolledup FROM summary WHERE isroot == 1;",
                     get_isrolledup_callback, isrolledup, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Could not get isrolledup: %s\n", err);
        return 1;
    }

    return 0;
}

int bottomup_collect_treesummary(sqlite3 *db, const char *dirname, sll_t *subdirs,
                                 const enum CheckIsRolledUp check_isrolledup,
                                 const uid_t uid, const gid_t gid, int *canrollup) {
    int isrolledup = 0;
    switch(check_isrolledup) {
        case ISROLLEDUP_CHECK:
            get_isrolledup(db, &isrolledup);
            break;
        case ISROLLEDUP_KNOWN_YES:
            isrolledup = 1;
            break;
        case ISROLLEDUP_DONT_CHECK:
        case ISROLLEDUP_KNOWN_NO:
        default:
            break;
    }

    char *err = NULL;

    /* delete any old treesummary rows that exist for this directory */
    if (sqlite3_exec(db, "DELETE FROM " TREESUMMARY " WHERE inode == (SELECT inode FROM " SUMMARY " WHERE isroot == 1);",
                     NULL, NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Failed to delete old treesummary for \"%s\": %s\n",
                                  dirname, err);
        return 1;
    }

    if (isrolledup != 0) {
        if (check_isrolledup != ISROLLEDUP_KNOWN_YES) {
            *canrollup = 1;
        }

        /*
         * this directory has been rolled up, so all information is
         * available here: compute the treesummary, no need to go
         * further down
         *
         * don't bother copying it out of sqlite only to put it back in
         */
        static const char TREESUMMARY_ROLLUP_COMPUTE_INSERT[] =
            /*
             * recompute treesummary from summary tables since
             * all summary tables are immediately available
             */
            "INSERT INTO " TREESUMMARY " SELECT (SELECT "
            "inode FROM " SUMMARY " WHERE isroot == 1), "
            "(SELECT pinode FROM " SUMMARY " WHERE isroot == 1), "
            "COUNT(*) - 1, " /* a directory is not a subdirectory of itself */
            "MAX(totfiles), MAX(totlinks), MAX(size), TOTAL(totfiles), TOTAL(totlinks), "
            "MIN(minuid), MAX(maxuid), MIN(mingid), MAX(maxgid), "
            "MIN(minsize), MAX(maxsize), TOTAL(totzero), "
            "TOTAL(totltk), TOTAL(totmtk), "
            "TOTAL(totltm), TOTAL(totmtm), "
            "TOTAL(totmtg), TOTAL(totmtt), "
            "TOTAL(totsize), "
            "MIN(minctime),   MAX(maxctime),   TOTAL(totctime),  "
            "MIN(minmtime),   MAX(maxmtime),   TOTAL(totmtime),  "
            "MIN(minatime),   MAX(maxatime),   TOTAL(totatime),  "
            "MIN(minblocks),  MAX(maxblocks),  TOTAL(totblocks), "
            "TOTAL(totxattr), TOTAL(depth), "
            "MIN(mincrtime),  MAX(maxcrtime),  TOTAL(totcrtime),  "
            "MIN(minossint1), MAX(maxossint1), TOTAL(totossint1), "
            "MIN(minossint2), MAX(maxossint2), TOTAL(totossint2), "
            "MIN(minossint3), MAX(maxossint3), TOTAL(totossint3), "
            "MIN(minossint4), MAX(maxossint4), TOTAL(totossint4), "
            "(SELECT COUNT(*) FROM " EXTERNAL_DBS "), rectype, "
            "(SELECT uid FROM " SUMMARY " WHERE isroot == 1), "
            "(SELECT gid FROM " SUMMARY " WHERE isroot == 1) "
            "FROM " SUMMARY " GROUP BY rectype"
            ";";

        if (sqlite3_exec(db, TREESUMMARY_ROLLUP_COMPUTE_INSERT, NULL, NULL, &err) != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr, "Error: Failed to compute treesummary for \"%s\": %s\n",
                                      dirname, err);
            return 1;
        }

        return 0;
    }

    if (check_isrolledup == ISROLLEDUP_CHECK) {
        *canrollup = 0;
    }

    size_t canrollup_children = 0;

    /* get this directory's permissions */
    struct Permissions parent_perms = {0};
    if (get_permissions(db, dirname, &parent_perms) != 0) {
        return 1;
    }

    /*
     * this directory has not rolled up, so have to use child
     * directories to get information
     *
     * every child is either
     *     - a leaf, and thus all the data is available in the summary
     *       table to use to create the treesummary table, or
     *
     *     - has a treesummary table because BottomUp is coming back up
     *
     * reopen child dbs to collect data
     *     - not super efficient
     *     - doing in-place update results in a massive query
     */
    struct sum tsum;
    zeroit(&tsum);

    struct sum sum;
    sll_loop(subdirs, node) {
        struct BottomUp *subdir = (struct BottomUp *) sll_node_data(node);

        const size_t child_dbname_len = subdir->name_len + 1 + DBNAME_LEN;
        char *child_dbname = malloc(child_dbname_len + 1);
        SNFORMAT_S(child_dbname, child_dbname_len + 1, 2,
                   subdir->name, subdir->name_len,
                   "/" DBNAME, DBNAME_LEN + 1);

        sqlite3 *child_db = opendb(child_dbname, SQLITE_OPEN_READONLY, 1, 0, NULL, NULL);
        if (!child_db) {
            free(child_dbname);
            continue;
        }

        /*
         * make sure child treesummary table exists and pull data from
         * it into intermediate treesummary for this directory
         */
        int trecs = 0;
        if (sqlite3_exec(child_db, TREESUMMARY_EXISTS,
                         treesummary_exists_callback, &trecs, &err) == SQLITE_OK) {
            zeroit(&sum);
            querytsdb(dirname, &sum, child_db, !(trecs < 1));
            tsumit(&sum, &tsum);
        }
        else {
            sqlite_print_err_and_free(err, stderr,
                                      "Warning: Failed to check for existence of treesummary table in child \"%s\": %s\n",
                                      subdir->name, err);
            err = NULL;
        }

        /* get subdirectory's permission from the database, not from bu->st */
        struct PermCanRollup pcr = {0};
        if (get_permissions_canrollup(child_db, subdir->name, &pcr) != 0) {
            closedb(child_db);
            free(child_dbname);
            return 1;
        }

        closedb(child_db);
        free(child_dbname);

        /* make sure the subdirectory can be rolled up to begin with */
        if (pcr.canrollup) {
            /* check if subdirectory can roll up into parent */
            canrollup_children += check_pair_permissions(&parent_perms, &pcr.perms);
        }
    }

    if (check_isrolledup == ISROLLEDUP_CHECK) {
        *canrollup = (canrollup_children == sll_get_size(subdirs));
    }

    /* add summary data from this directory */
    zeroit(&sum);
    querytsdb(dirname, &tsum, db, 0);
    tsumit(&sum, &tsum);
    tsum.totsubdirs--;

    return inserttreesumdb(dirname, db, &tsum, 0, uid, gid);
}
