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
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "dbutils.h"
#include "template_db.h"
#include "utils.h"
#include "xattrs.h"

int init_template_db(struct template_db *tdb) {
    /* Not checking argument */

    tdb->fd = -1;
    tdb->size = -1;
    return 0;
}

/* each db.db, per-user db, and per-group db will have these tables */
static int create_xattr_tables(const char *name, sqlite3 *db, void *args) {
    (void) args;

    return ((create_table_wrapper(name, db, XATTRS_PWD,         XATTRS_PWD_CREATE)         != SQLITE_OK) ||
            (create_table_wrapper(name, db, XATTRS_ROLLUP,      XATTRS_ROLLUP_CREATE)      != SQLITE_OK) ||
            (create_table_wrapper(name, db, XATTRS_AVAIL,       XATTRS_AVAIL_CREATE)       != SQLITE_OK));

}

int create_dbdb_tables(const char *name, sqlite3 *db, void *args) {
    return ((create_table_wrapper(name, db, ENTRIES,             ENTRIES_CREATE)             != SQLITE_OK) ||
            (create_table_wrapper(name, db, SUMMARY,             SUMMARY_CREATE)             != SQLITE_OK) ||
            (create_table_wrapper(name, db, VRSUMMARY,           VRSUMMARY_CREATE)           != SQLITE_OK) ||
            (create_table_wrapper(name, db, PENTRIES_ROLLUP,     PENTRIES_ROLLUP_CREATE)     != SQLITE_OK) ||
            (create_table_wrapper(name, db, PENTRIES,            PENTRIES_CREATE)            != SQLITE_OK) ||
            (create_table_wrapper(name, db, VRPENTRIES,          VRPENTRIES_CREATE)          != SQLITE_OK) ||
            (create_table_wrapper(name, db, "vssqldir",          vssqldir)                   != SQLITE_OK) ||
            (create_table_wrapper(name, db, "vssqluser",         vssqluser)                  != SQLITE_OK) ||
            (create_table_wrapper(name, db, "vssqlgroup",        vssqlgroup)                 != SQLITE_OK) ||
            (create_table_wrapper(name, db, SUMMARYLONG,         SUMMARYLONG_CREATE)         != SQLITE_OK) ||
            (create_table_wrapper(name, db, VRSUMMARYLONG,       VRSUMMARYLONG_CREATE)       != SQLITE_OK) ||
            (create_table_wrapper(name, db, EXTERNAL_DBS_PWD,    EXTERNAL_DBS_PWD_CREATE)    != SQLITE_OK) ||
            (create_table_wrapper(name, db, EXTERNAL_DBS_ROLLUP, EXTERNAL_DBS_ROLLUP_CREATE) != SQLITE_OK) ||
            (create_table_wrapper(name, db, EXTERNAL_DBS,        EXTERNAL_DBS_CREATE)        != SQLITE_OK) ||
            create_xattr_tables(name, db, args));
}

int close_template_db(struct template_db *tdb) {
    /* Not checking argument */

    close(tdb->fd);
    return init_template_db(tdb);
}

/* create the database file to copy from */
int create_template(struct template_db *tdb, int (*create_tables)(const char *, sqlite3 *, void *),
                    const char *name) {
    if (!tdb || (tdb->fd != -1) ||
        !create_tables || !name) {
        return -1;
    }

    sqlite3 *db = opendb(name, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 0, 0
                         , create_tables, NULL
                         #if defined(DEBUG) && defined(PER_THREAD_STATS)
                         , NULL, NULL
                         , NULL, NULL
                         #endif
                         );

    /*
     * open before sqlite3_close to prevent potential race
     * condition where file is deleted before being reopened
     */
    tdb->fd = open(name, O_RDONLY);

    sqlite3_close(db);

    /* no need for the file to remain on the filesystem */
    remove(name);

    if (tdb->fd == -1) {
        fprintf(stderr, "Could not open template file\n");
        return -1;
    }

    tdb->size = lseek(tdb->fd, 0, SEEK_END);
    return !tdb->size;
}

/* create the initial xattrs database file to copy from */
int create_xattrs_template(struct template_db *tdb) {
    static const char name[] = "xattrs_tmp.db";
    return create_template(tdb, create_xattr_tables, name);
}

/* create the initial main database file to copy from */
int create_dbdb_template(struct template_db *tdb) {
    static const char name[] = "tmp.db";
    return create_template(tdb, create_dbdb_tables, name);
}

/* copy the template file instead of creating a new database and new tables for each work item */
/* the ownership and permissions are set too */
int copy_template(struct template_db *tdb, const char *dst, uid_t uid, gid_t gid) {
    /* Not checking arguments */

    int err = 0;
    const int src_db = dup(tdb->fd);
    err = err?err:errno;
    const int dst_db = open(dst, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    err = err?err:errno;
    const ssize_t sf = copyfd(src_db, 0, dst_db, 0, tdb->size);
    err = err?err:errno;
    fchown(dst_db, uid, gid);
    err = err?err:errno;
    close(src_db);
    err = err?err:errno;
    close(dst_db);
    err = err?err:errno;

    if (sf == -1) {
        fprintf(stderr, "Could not copy template file (%d) to %s (%d): %s (%d)\n",
                src_db, dst, dst_db, strerror(err), err);
        remove(dst);
        return -1;
    }

    return 0;
}

sqlite3 *template_to_db(struct template_db *tdb, const char *dst, uid_t uid, gid_t gid) {
    if (copy_template(tdb, dst, uid, gid)) {
        return NULL;
    }

    return opendb(dst, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 1, 0
                  , NULL, NULL
                  #if defined(DEBUG) && defined(PER_THREAD_STATS)
                  , NULL, NULL
                  , NULL, NULL
                  #endif
        );
}

/* create db.db with empty tables at the given directory (and leave it on the filesystem) */
int create_empty_dbdb(struct template_db *tdb, refstr_t *dst, uid_t uid, gid_t gid) {
    char dbname[MAXPATH];
    SNFORMAT_S(dbname, sizeof(dbname), 3,
               dst->data, dst->len,
               "/", (size_t) 1,
               DBNAME, DBNAME_LEN);

    /* if database file already exists, assume it's good */
    struct stat st;
    if (stat(dbname, &st) == 0) {  /* following links */
        return -!S_ISREG(st.st_mode);
    }

    const int err = errno;

    /* if the parent's database file is not found, create it */
    if (err != ENOENT) {
        fprintf(stderr, "Error: Empty db.db path '%s': %s (%d)\n",
                dst->data, strerror(err), err);
        return -1;
    }

    return copy_template(tdb, dbname, uid, gid);
}
