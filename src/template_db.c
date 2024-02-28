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
#include "external.h"
#include "template_db.h"
#include "utils.h"
#include "xattrs.h"

int init_template_db(struct template_db *tdb) {
    /* Not checking argument */

    tdb->buf = NULL;
    tdb->size = 0;
    return 0;
}

/* each db.db, per-user db, and per-group db will have these tables */
int create_xattr_tables(const char *name, sqlite3 *db, void *args) {
    (void) args;

    return ((create_table_wrapper(name, db, XATTRS_PWD,      XATTRS_PWD_CREATE)      != SQLITE_OK) ||
            (create_table_wrapper(name, db, XATTRS_ROLLUP,   XATTRS_ROLLUP_CREATE)   != SQLITE_OK) ||
            (create_table_wrapper(name, db, XATTRS_AVAIL,    XATTRS_AVAIL_CREATE)    != SQLITE_OK));

}

int create_dbdb_tables(const char *name, sqlite3 *db, void *args) {
    return ((create_table_wrapper(name, db, ENTRIES,         ENTRIES_CREATE)         != SQLITE_OK) ||
            (create_table_wrapper(name, db, SUMMARY,         SUMMARY_CREATE)         != SQLITE_OK) ||
            (create_table_wrapper(name, db, VRSUMMARY,       VRSUMMARY_CREATE)       != SQLITE_OK) ||
            (create_table_wrapper(name, db, PENTRIES_ROLLUP, PENTRIES_ROLLUP_CREATE) != SQLITE_OK) ||
            (create_table_wrapper(name, db, PENTRIES,        PENTRIES_CREATE)        != SQLITE_OK) ||
            (create_table_wrapper(name, db, VRPENTRIES,      VRPENTRIES_CREATE)      != SQLITE_OK) ||
            (create_table_wrapper(name, db, "vssqldir",      vssqldir)               != SQLITE_OK) ||
            (create_table_wrapper(name, db, "vssqluser",     vssqluser)              != SQLITE_OK) ||
            (create_table_wrapper(name, db, "vssqlgroup",    vssqlgroup)             != SQLITE_OK) ||
            (create_table_wrapper(name, db, SUMMARYLONG,     SUMMARYLONG_CREATE)     != SQLITE_OK) ||
            (create_table_wrapper(name, db, VRSUMMARYLONG,   VRSUMMARYLONG_CREATE)   != SQLITE_OK) ||
            create_external_tables(name, db, args) ||
            create_xattr_tables(name, db, args));
}

int close_template_db(struct template_db *tdb) {
    /* Not checking argument */

    sqlite3_free(tdb->buf);
    return init_template_db(tdb);
}

/* create the database file to copy from */
int create_template(struct template_db *tdb, int (*create_tables)(const char *, sqlite3 *, void *)) {
    /* Not checking arguments */

    sqlite3 *db = opendb(":memory:", SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 0, 0, create_tables, NULL);
    tdb->buf = sqlite3_serialize(db, "main", &tdb->size, 0);
    sqlite3_close(db);

    return -!tdb->buf;
}

/* create the initial xattrs database file to copy from */
int create_xattrs_template(struct template_db *tdb) {
    return create_template(tdb, create_xattr_tables);
}

/* create the initial main database file to copy from */
int create_dbdb_template(struct template_db *tdb) {
    return create_template(tdb, create_dbdb_tables);
}

sqlite3 *template_to_mem_db(struct template_db *tdb) {
    /* create in-memory db for overwriting */
    sqlite3 *db = opendb(":memory:", SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 1, 0, NULL, NULL);

    /* copy the template to a new buffer */
    unsigned char *serialized = sqlite3_malloc64(tdb->size);
    memcpy(serialized, tdb->buf, tdb->size);

    /* takes ownership of serialized */
    if (sqlite3_deserialize(db, "main", serialized, tdb->size, tdb->size,
                            SQLITE_DESERIALIZE_RESIZEABLE | SQLITE_DESERIALIZE_FREEONCLOSE) != SQLITE_OK) {
        sqlite3_free(serialized);
        return NULL;
    }

    return db;
}

/* wrapper for template_to_mem_db + mem_db_to_file */
int template_to_file(struct template_db *tdb, char *dst, uid_t uid, gid_t gid) {
    sqlite3 *db = template_to_mem_db(tdb);
    if (!db) {
        return -1;
    }

    const int rc = mem_db_to_file(db, dst, uid, gid);
    closedb(db);
    return rc;
}

int create_empty_dbdb(struct template_db *tdb, refstr_t *path, uid_t uid, gid_t gid) {
    /* Not checking arguments */

    char dbname[MAXPATH];
    SNFORMAT_S(dbname, sizeof(dbname), 3,
               path->data, path->len,
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
                dbname, strerror(err), err);
        return -1;
    }

    return template_to_file(tdb, dbname, uid, gid);
}
