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



#ifndef GUFI_ROLLUP_H
#define GUFI_ROLLUP_H

#include <sys/types.h>

#include <sqlite3.h>

#include "external_attach.h"
#include "str.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ******************************************************************* */
/* functions for figuring out whether or not rollups are allowed */

struct Permissions {
    mode_t mode;
    uid_t uid;
    gid_t gid;
};

/* SELECT mode, uid, gid FROM summary; */
int get_permissions_callback(void *args, int count, char **data, char **columns);
int get_permissions(sqlite3 *db, const char *name, struct Permissions *perms);

/*
 * compare the permission of the parent directory and a single child
 *
 * does not include checking if the child is already rolled up
 */
size_t check_pair_permissions(struct Permissions *parent,
                              struct Permissions *child);
/* ******************************************************************* */

/* ******************************************************************* */
/* primitives for core "do rollup" operations */

/* used for rollup and unrollup */
extern const char   ROLLUP_CLEANUP[];
extern const size_t ROLLUP_CLEANUP_SIZE;
int xattrs_rollup_cleanup(void *args, int count, char **data, char **columns);

/* remove any existing roll up data for the target db */
int rollup_init(sqlite3 *db, const str_t *path);

/* child db.db are attached one at a time with this name */
#define ROLLUP_SUBDIR_ATTACH_NAME "subdir"

/* attach one child db to the target db and return the child db name */
char *rollup_child_attach(sqlite3 *db, const str_t *child_path, const int attach_flag);

/* SQLite3 callback arg for handling externally stored xattrs */
typedef struct RollupExternalXattrsArgs {
    struct template_db *xattr;
    char *parent;
    size_t parent_len;
    char *child;
    size_t child_len;
    size_t *count;
} rexa_t;

/*
 * SQL parts to copy one subdir/db.db table into the current directory
 *
 * split to be able to swap out name column without doing unnecessary memcpys
 */

/* common front portion of SQL */
#define ROLLUP_ONE_SUBDIR_FRONT                                         \
    "INSERT INTO " PENTRIES_ROLLUP " "                                  \
    "SELECT * "                                                         \
    "FROM " ROLLUP_SUBDIR_ATTACH_NAME "." PENTRIES ";"                  \
                                                                        \
    "INSERT INTO " SUMMARY " "                                          \
                                                                        \
    /* directory data */                                                \
    "SELECT "                                                           \

/* common back portion of SQL */
#define ROLLUP_ONE_SUBDIR_BACK                                          \
    ", "                                                                \
    "sub.type, sub.inode, sub.mode, sub.nlink, "                        \
    "sub.uid, sub.gid, sub.size, sub.blksize, sub.blocks, "             \
    "sub.atime, sub.mtime, sub.ctime, "                                 \
    "sub.linkname, sub.xattr_names, sub.crtime, "                       \
                                                                        \
    /* summary data */                                                  \
    "sub.totfiles, sub.totlinks, "                                      \
    "sub.minuid, sub.maxuid, sub.mingid, sub.maxgid, "                  \
    "sub.minsize, sub.maxsize, sub.totzero, "                           \
    "sub.totltk, sub.totmtk, "                                          \
    "sub.totltm, sub.totmtm, "                                          \
    "sub.totmtg, sub.totmtt, "                                          \
    "sub.totsize, "                                                     \
    "sub.minctime,   sub.maxctime,  sub.totctime,  "                    \
    "sub.minmtime,   sub.maxmtime,  sub.totmtime,  "                    \
    "sub.minatime,   sub.maxatime,  sub.totatime,  "                    \
    "sub.minblocks,  sub.maxblocks, sub.totblocks, "                    \
    "sub.totxattr,   sub.depth + 1, "                                   \
    "sub.mincrtime,  sub.maxcrtime,  sub.totcrtime,  "                  \
    "sub.minossint1, sub.maxossint1, sub.totossint1, "                  \
    "sub.minossint2, sub.maxossint2, sub.totossint2, "                  \
    "sub.minossint3, sub.maxossint3, sub.totossint3, "                  \
    "sub.minossint4, sub.maxossint4, sub.totossint4, "                  \
                                                                        \
    /* internal data */                                                 \
    "sub.rectype, sub.pinode, 0, " /* isroot */                         \
    "1, 1 " /* canrollup, isrolledup */                                 \
    "FROM " SUMMARY " AS s, "                                           \
    ROLLUP_SUBDIR_ATTACH_NAME "." SUMMARY " AS sub "                    \
    "WHERE s.isroot == 1;"                                              \
                                                                        \
    "INSERT INTO " TREESUMMARY " "                                      \
    "SELECT * "                                                         \
    "FROM " ROLLUP_SUBDIR_ATTACH_NAME "." TREESUMMARY ";"               \
                                                                        \
    "INSERT INTO " XATTRS_ROLLUP " "                                    \
    "SELECT * "                                                         \
    "FROM " ROLLUP_SUBDIR_ATTACH_NAME "." XATTRS_AVAIL ";"              \
                                                                        \
    "INSERT OR IGNORE INTO " EXTERNAL_DBS_ROLLUP " "                    \
    "SELECT * "                                                         \
    "FROM " ROLLUP_SUBDIR_ATTACH_NAME "." EXTERNAL_DBS ";"              \
                                                                        \
    /* select subdir external xattrs_avail for copying to current */    \
    /* external xattrs_rollup via callback */                           \
    "SELECT filename, uid, gid "                                        \
    "FROM " ROLLUP_SUBDIR_ATTACH_NAME "." EXTERNAL_DBS " "              \
    "WHERE type == '" EXTERNAL_TYPE_XATTR_NAME "';"                     \

/*
 * roll up the ATTACHed subdir into the target db
 *
 * construct SQL from ROLLUP_ONE_SUBDIR_FRONT and ROLLUP_ONE_SUBDIR_BACK
 */
int rollup_child_process(sqlite3 *db, const char *sql, rexa_t *rexa);

/* detach the child and free the child db name */
int rollup_child_detach(sqlite3 *db, char *child_dbname);
/* ******************************************************************* */

/* for querying */
int get_isrolledup(sqlite3 *db, int *isrolledup);

enum CheckIsRolledUp {
    ISROLLEDUP_CHECK,
    ISROLLEDUP_DONT_CHECK,
    ISROLLEDUP_KNOWN_YES,
    ISROLLEDUP_KNOWN_NO,
};

int bottomup_collect_treesummary(sqlite3 *db, const char *dirname, sll_t *subdirs,
                                 const enum CheckIsRolledUp check_isrolledup,
                                 const uid_t uid, const gid_t gid, int *canrollup);

#ifdef __cplusplus
}
#endif

#endif
