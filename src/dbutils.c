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
#include <grp.h>
#include <math.h>
#include <pwd.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "pcre.h"

#include "BottomUp.h"
#include "dbutils.h"
#include "external.h"
#include "histogram.h"

const char READDIRPLUS_CREATE[] =
    DROP_TABLE(READDIRPLUS)
    "CREATE TABLE " READDIRPLUS "(path TEXT, type TEXT, inode TEXT PRIMARY KEY, pinode TEXT, suspect INT64);";

const char READDIRPLUS_INSERT[] =
    "INSERT INTO " READDIRPLUS " VALUES (@path, @type, @inode, @pinode, @suspect);";

const char ENTRIES_CREATE[] =
    DROP_TABLE(ENTRIES)
    "CREATE TABLE " ENTRIES "(name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattr_names BLOB, crtime INT64, ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, osstext1 TEXT, osstext2 TEXT);";

const char ENTRIES_INSERT[] =
    "INSERT INTO " ENTRIES " VALUES (@name, @type, @inode, @mode, @nlink, @uid, @gid, @size, @blksize, @blocks, @atime, @mtime, @ctime, @linkname, @xattr_names, @crtime, @ossint1, @ossint2, @ossint3, @ossint4, @osstext1, @osstext2);";

const char SUMMARY_CREATE[] =
    DROP_TABLE(SUMMARY)
    "CREATE TABLE " SUMMARY "(name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattr_names BLOB, totfiles INT64, totlinks INT64, minuid INT64, maxuid INT64, mingid INT64, maxgid INT64, minsize INT64, maxsize INT64, totzero INT64, totltk INT64, totmtk INT64, totltm INT64, totmtm INT64, totmtg INT64, totmtt INT64, totsize INT64, minctime INT64, maxctime INT64, minmtime INT64, maxmtime INT64, minatime INT64, maxatime INT64, minblocks INT64, maxblocks INT64, totxattr INT64, depth INT64, mincrtime INT64, maxcrtime INT64, minossint1 INT64, maxossint1 INT64, totossint1 INT64, minossint2 INT64, maxossint2 INT64, totossint2 INT64, minossint3 INT64, maxossint3 INT64, totossint3 INT64, minossint4 INT64, maxossint4 INT64, totossint4 INT64, rectype INT64, pinode TEXT, isroot INT64, rollupscore INT64);";

static const char SUMMARY_INSERT[] =
    "INSERT INTO " SUMMARY " VALUES (@name, @type, @inode, @mode, @nlink, @uid, @gid, @size, @blksize, @blocks, @atime, @mtime, @ctime, @linkname, @xattr_names, @totfiles, @totlinks, @minuid, @maxuid, @mingid, @maxgid, @minsize, @maxsize, @totzero, @totltk, @totmtk, @totltm, @totmtm, @totmtg, @totmtt, @totsize, @minctime, @maxctime, @minmtime, @maxmtime, @minatime, @maxatime, @minblocks, @maxblocks, @totxattr, @depth, @mincrtime, @maxcrtime, @minossint1, @maxossint1, @totossint1, @minossint2, @maxossint2, @totossint2, @minossint3, @maxossint3, @totossint3, @minossint4, @maxossint4, @totossint4, @rectype, @pinode, @isroot, @rollupscore);";

const char VRSUMMARY_CREATE[] =
    DROP_VIEW(VRSUMMARY)
    "CREATE VIEW " VRSUMMARY " AS SELECT REPLACE(" SUMMARY ".name, RTRIM(" SUMMARY ".name, REPLACE(" SUMMARY ".name, '/', '')), '') AS dname, " SUMMARY ".name AS sname, " SUMMARY ".rollupscore AS sroll, (SELECT COUNT(*) FROM " SUMMARY " AS c WHERE c.pinode == " SUMMARY ".inode) AS srollsubdirs, " SUMMARY ".* FROM " SUMMARY ";";

const char PENTRIES_ROLLUP_CREATE[] =
    DROP_TABLE(PENTRIES_ROLLUP)
    "CREATE TABLE " PENTRIES_ROLLUP "(name TEXT, type TEXT, inode TEXT, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattr_names BLOB, crtime INT64, ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, osstext1 TEXT, osstext2 TEXT, pinode TEXT, ppinode TEXT);";

const char PENTRIES_ROLLUP_INSERT[] =
    "INSERT INTO " PENTRIES_ROLLUP " VALUES (@name, @type, @inode, @mode, @nlink, @uid, @gid, @size, @blksize, @blocks, @atime, @mtime, @ctime, @linkname, @xattr_names, @crtime, @ossint1, @ossint2, @ossint3, @ossint4, @osstext1, @osstext2, @pinode, @ppinode);";

const char PENTRIES_CREATE[] =
    DROP_VIEW(PENTRIES)
    "CREATE VIEW " PENTRIES " AS SELECT " ENTRIES ".*, " SUMMARY ".inode AS pinode, " SUMMARY ".pinode AS ppinode FROM " ENTRIES ", " SUMMARY " WHERE isroot == 1 UNION SELECT * FROM " PENTRIES_ROLLUP ";";

/* vrentries is not created because rolled up entries tables are not correct */

const char VRPENTRIES_CREATE[] =
    DROP_VIEW(VRPENTRIES)
    "CREATE VIEW " VRPENTRIES " AS SELECT REPLACE(" VRSUMMARY ".name, RTRIM(" VRSUMMARY ".name, REPLACE(" VRSUMMARY ".name, '/', '')), '') AS dname, " VRSUMMARY ".name AS sname, " VRSUMMARY ".mode AS dmode, " VRSUMMARY ".nlink AS dnlink, " VRSUMMARY ".uid AS duid, " VRSUMMARY ".gid AS dgid, " VRSUMMARY ".size AS dsize, " VRSUMMARY ".blksize AS dblksize, " VRSUMMARY ".blocks AS dblocks, " VRSUMMARY ".atime AS datime, " VRSUMMARY ".mtime AS dmtime, " VRSUMMARY ".ctime AS dctime, " VRSUMMARY ".linkname AS dlinkname, " VRSUMMARY ".totfiles AS dtotfile, " VRSUMMARY ".totlinks AS dtotlinks, " VRSUMMARY ".minuid AS dminuid, " VRSUMMARY ".maxuid AS dmaxuid, " VRSUMMARY ".mingid AS dmingid, " VRSUMMARY ".maxgid AS dmaxgid, " VRSUMMARY ".minsize AS dminsize, " VRSUMMARY ".maxsize AS dmaxsize, " VRSUMMARY ".totzero AS dtotzero, " VRSUMMARY ".totltk AS dtotltk, " VRSUMMARY ".totmtk AS dtotmtk, " VRSUMMARY ".totltm AS totltm, " VRSUMMARY ".totmtm AS dtotmtm, " VRSUMMARY ".totmtg AS dtotmtg, " VRSUMMARY ".totmtt AS dtotmtt, " VRSUMMARY ".totsize AS dtotsize, " VRSUMMARY ".minctime AS dminctime, " VRSUMMARY ".maxctime AS dmaxctime, " VRSUMMARY ".minmtime AS dminmtime, " VRSUMMARY ".maxmtime AS dmaxmtime, " VRSUMMARY ".minatime AS dminatime, " VRSUMMARY ".maxatime AS dmaxatime, " VRSUMMARY ".minblocks AS dminblocks, " VRSUMMARY ".maxblocks AS dmaxblocks, " VRSUMMARY ".totxattr AS dtotxattr, " VRSUMMARY ".depth AS ddepth, " VRSUMMARY ".mincrtime AS dmincrtime, " VRSUMMARY ".maxcrtime AS dmaxcrtime, " VRSUMMARY ".rollupscore AS sroll, " VRSUMMARY ".isroot AS atroot, " VRSUMMARY ".srollsubdirs AS srollsubdirs, " PENTRIES ".* FROM " VRSUMMARY ", " PENTRIES " WHERE " VRSUMMARY ".inode == " PENTRIES ".pinode;";

const char TREESUMMARY_EXISTS[] =
    "SELECT name FROM sqlite_master WHERE (type == 'table') AND (name == '" TREESUMMARY "');";

/* summary and tsummary views grouped by uid and gid */
#define vssql(name, value)                                                                             \
    const char vssql##name[] =                                                                         \
        "DROP VIEW IF EXISTS vsummary" #name ";"                                                       \
        "CREATE VIEW vsummary" #name " AS SELECT * FROM " SUMMARY " WHERE rectype == " #value ";";     \
                                                                                                       \
    const char vtssql##name[] =                                                                        \
        "DROP VIEW IF EXISTS vtsummary" #name ";"                                                      \
        "CREATE VIEW vtsummary" #name " AS SELECT * FROM " TREESUMMARY " WHERE rectype == " #value ";" \

vssql(dir,   0);
vssql(user,  1);
vssql(group, 2);

/*
 * use LONG_STATS at end of LONG_CREATE
 * to add columns from more tables
 *
 * newtable must have an inode column
 */
#define LONG_STATS(newtable) " LEFT JOIN " newtable " ON " SUMMARY ".inode == " newtable ".inode "
#define LONG_CREATE(source)                                         \
    const char source##LONG_CREATE[] =                              \
        DROP_VIEW(source##LONG)                                     \
        "CREATE VIEW " source##LONG " AS SELECT * FROM " source ";" \

LONG_CREATE(SUMMARY);
LONG_CREATE(VRSUMMARY);

static sqlite3 *attachdb_internal(const char *attach, sqlite3 *db, const char *dbn, const int print_err) {
    char *err = NULL;
    if (sqlite3_exec(db, attach, NULL, NULL, print_err?(&err):NULL) != SQLITE_OK) {
        if (print_err) {
            fprintf(stderr, "Cannot attach database as \"%s\": %s\n", dbn, err);
            sqlite3_free(err);
        }
        return NULL;
    }

    return db;
}

sqlite3 *attachdb_raw(const char *name, sqlite3 *db, const char *dbn, const int print_err) {
    /*
     * create ATTACH statement here to prevent double copy that
     * would be done by generating name first and passing it to
     * attachdb_internal for SQL statement generation
     */
    char attach[MAXSQL];
    sqlite3_snprintf(sizeof(attach), attach, "ATTACH %Q AS %Q;", name, dbn);

    return attachdb_internal(attach, db, dbn, print_err);
}

sqlite3 *attachdb(const char *name, sqlite3 *db, const char *dbn, const int flags, const int print_err) {
    char ow = '?';
    if (flags & SQLITE_OPEN_READONLY) {
        ow = 'o';
    }
    else if (flags & SQLITE_OPEN_READWRITE) {
        ow = 'w';
    }

    /*
     * create ATTACH statement here to prevent double copy that
     * would be done by generating name first and passing it to
     * attachdb_internal for SQL statement generation
     */
    char attach[MAXSQL];
    sqlite3_snprintf(sizeof(attach), attach, "ATTACH 'file:%q?mode=r%c" GUFI_SQLITE_VFS_URI "' AS %Q;",
                     name, ow, dbn);

    return attachdb_internal(attach, db, dbn, print_err);
}

sqlite3 *detachdb_cached(const char *name, sqlite3 *db, const char *sql, const int print_err) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, print_err?(&err):NULL) != SQLITE_OK) {
        if (print_err) {
            fprintf(stderr, "Cannot detach database: %s %s\n", name, err);
            sqlite3_free(err);
        }
        return NULL;
    }

    return db;
}

sqlite3 *detachdb(const char *name, sqlite3 *db, const char *dbn, const int print_err) {
    /* cannot check for sqlite3_snprintf errors except by finding the null terminator, so skipping */
    char detach[MAXSQL];
    sqlite3_snprintf(MAXSQL, detach, "DETACH %Q;", dbn);

    return detachdb_cached(name, db, detach, print_err);
}

int create_table_wrapper(const char *name, sqlite3 *db, const char *sql_name, const char *sql) {
    char *err = NULL;
    const int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "%s:%d SQL error while executing '%s' on %s: '%s' (%d)\n",
                __FILE__, __LINE__, sql_name, name, err, rc);
        sqlite3_free(err);
    }
    return rc;
}

int create_treesummary_tables(const char *name, sqlite3 *db, void *args) {
    (void) args;

    return ((create_table_wrapper(name, db, "tsql",        TREESUMMARY_CREATE) != SQLITE_OK) ||
            (create_table_wrapper(name, db, "vtssqldir",   vtssqldir)          != SQLITE_OK) ||
            (create_table_wrapper(name, db, "vtssqluser",  vtssqluser)         != SQLITE_OK) ||
            (create_table_wrapper(name, db, "vtssqlgroup", vtssqlgroup)        != SQLITE_OK));
}

int set_db_pragmas(sqlite3 *db) {
    /* https://www.sqlite.org/pragma.html */
    /* no errors will be returned by the PRAGMA itself */

    return !(sqlite3_exec(db,
                        /* https://www.sqlite.org/pragma.html#pragma_journal_mode */
                        "PRAGMA journal_mode = OFF;"

                        /* https://www.sqlite.org/pragma.html#pragma_page_size */
                        /* The page size must be a power of two between 512 and 65536 inclusive. */
                        "PRAGMA page_size = 65536;"

                        ,
                        NULL, NULL, NULL) == SQLITE_OK);
}

sqlite3 *opendb(const char *name, int flags, const int setpragmas, const int load_extensions,
                int (*modifydb_func)(const char *name, sqlite3 *db, void *args), void *modifydb_args) {
    sqlite3 *db = NULL;

    if (sqlite3_open_v2(name, &db, flags | SQLITE_OPEN_URI, GUFI_SQLITE_VFS) != SQLITE_OK) {
        if (!(flags & SQLITE_OPEN_CREATE)) {
            fprintf(stderr, "Cannot open database: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db));
        }
        sqlite3_close(db); /* close db even if it didn't open to avoid memory leaks */
        return NULL;
    }

    if (setpragmas) {
        /* ignore errors */
        set_db_pragmas(db);
    }

    if (load_extensions) {
        if ((sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, NULL) != SQLITE_OK) || /* enable loading of extensions */
            (sqlite3_extension_init(db, NULL, NULL)                                != SQLITE_OK)) { /* load the sqlite3-pcre extension */
            fprintf(stderr, "Unable to load regex extension\n");
            sqlite3_close(db);
            return NULL;
        }
    }

    if (!(flags & SQLITE_OPEN_READONLY) && modifydb_func) {
        if (modifydb_func(name, db, modifydb_args) != 0) {
            fprintf(stderr, "Cannot modify database: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db));
            sqlite3_close(db);
            return NULL;
        }
    }

    return db;
}

int querytsdb(const char *name, struct sum *sum, sqlite3 *db, int ts) {
    static const char *ts_str[] = {
        "SELECT totfiles, totlinks, minuid, maxuid, mingid, maxgid, minsize, maxsize, totzero, totltk, totmtk, totltm, totmtm, totmtg, totmtt, totsize, minctime, maxctime, minmtime, maxmtime, minatime, maxatime, minblocks, maxblocks, totxattr, mincrtime, maxcrtime, minossint1, maxossint1, totossint1, minossint2, maxossint2, totossint2, minossint3, maxossint3, totossint3, minossint4, maxossint4, totossint4, (SELECT COUNT(*) FROM " EXTERNAL_DBS ") "
        "FROM " SUMMARY " WHERE rectype == 0;",
        "SELECT t.totfiles, t.totlinks, t.minuid, t.maxuid, t.mingid, t.maxgid, t.minsize, t.maxsize, t.totzero, t.totltk, t.totmtk, t.totltm, t.totmtm, t.totmtg, t.totmtt, t.totsize, t.minctime, t.maxctime, t.minmtime, t.maxmtime, t.minatime, t.maxatime, t.minblocks, t.maxblocks, t.totxattr, t.mincrtime, t.maxcrtime, t.minossint1, t.maxossint1, t.totossint1, t.minossint2, t.maxossint2, t.totossint2, t.minossint3, t.maxossint3, t.totossint3, t.minossint4, t.maxossint4, t.totossint4, t.totextdbs, t.totsubdirs, t.maxsubdirfiles, t.maxsubdirlinks, t.maxsubdirsize "
        "FROM " TREESUMMARY " AS t, " SUMMARY " AS s WHERE (s.isroot == 1) AND (s.inode == t.inode) AND (t.rectype == 0);",
    };

    const char *sqlstmt = ts_str[ts];
    sqlite3_stmt *res = NULL;
    if (sqlite3_prepare_v2(db, sqlstmt, MAXSQL, &res, NULL) != SQLITE_OK) {
        fprintf(stderr, "SQL error on query: %s, name: %s, err: %s\n",
                sqlstmt,name,sqlite3_errmsg(db));
        return 1;
    }

    int rows = 0;
    while (sqlite3_step(res) != SQLITE_DONE) {
        struct sum curr;
        zeroit(&curr);

        curr.totfiles   = sqlite3_column_int64(res, 0);
        curr.totlinks   = sqlite3_column_int64(res, 1);
        curr.minuid     = sqlite3_column_int64(res, 2);
        curr.maxuid     = sqlite3_column_int64(res, 3);
        curr.mingid     = sqlite3_column_int64(res, 4);
        curr.maxgid     = sqlite3_column_int64(res, 5);
        curr.minsize    = sqlite3_column_int64(res, 6);
        curr.maxsize    = sqlite3_column_int64(res, 7);
        curr.totzero    = sqlite3_column_int64(res, 8);
        curr.totltk     = sqlite3_column_int64(res, 9);
        curr.totmtk     = sqlite3_column_int64(res, 10);
        curr.totltm     = sqlite3_column_int64(res, 11);
        curr.totmtm     = sqlite3_column_int64(res, 12);
        curr.totmtg     = sqlite3_column_int64(res, 13);
        curr.totmtt     = sqlite3_column_int64(res, 14);
        curr.totsize    = sqlite3_column_int64(res, 15);
        curr.minctime   = sqlite3_column_int64(res, 16);
        curr.maxctime   = sqlite3_column_int64(res, 17);
        curr.minmtime   = sqlite3_column_int64(res, 18);
        curr.maxmtime   = sqlite3_column_int64(res, 19);
        curr.minatime   = sqlite3_column_int64(res, 20);
        curr.maxatime   = sqlite3_column_int64(res, 21);
        curr.minblocks  = sqlite3_column_int64(res, 22);
        curr.maxblocks  = sqlite3_column_int64(res, 23);
        curr.totxattr   = sqlite3_column_int64(res, 24);

        curr.mincrtime  = sqlite3_column_int64(res, 25);
        curr.maxcrtime  = sqlite3_column_int64(res, 26);
        curr.minossint1 = sqlite3_column_int64(res, 27);
        curr.maxossint1 = sqlite3_column_int64(res, 28);
        curr.totossint1 = sqlite3_column_int64(res, 29);
        curr.minossint2 = sqlite3_column_int64(res, 30);
        curr.maxossint2 = sqlite3_column_int64(res, 31);
        curr.totossint2 = sqlite3_column_int64(res, 32);
        curr.minossint3 = sqlite3_column_int64(res, 33);
        curr.maxossint3 = sqlite3_column_int64(res, 34);
        curr.totossint3 = sqlite3_column_int64(res, 35);
        curr.minossint4 = sqlite3_column_int64(res, 36);
        curr.maxossint4 = sqlite3_column_int64(res, 37);
        curr.totossint4 = sqlite3_column_int64(res, 38);

        curr.totextdbs  = sqlite3_column_int64(res, 39);

        if (ts) {
            curr.totsubdirs     = sqlite3_column_int64(res, 40);
            curr.maxsubdirfiles = sqlite3_column_int64(res, 41);
            curr.maxsubdirlinks = sqlite3_column_int64(res, 42);
            curr.maxsubdirsize  = sqlite3_column_int64(res, 43);
        }
        else {
            curr.totsubdirs     = 0;
            curr.maxsubdirfiles = 0;
            curr.maxsubdirlinks = 0;
            curr.maxsubdirsize  = 0;
        }

        tsumit(&curr, sum);
        sum->totsubdirs--;
        rows++;
    }

    sqlite3_finalize(res);

    return rows;
}

int startdb(sqlite3 *db)
{
    char *err = NULL;
    if (sqlite3_exec(db, "BEGIN TRANSACTION", NULL , NULL, &err) != SQLITE_OK) {
        printf("begin transaction issue %s\n", err);
        sqlite3_free(err);
    }
    return !err;
}

int stopdb(sqlite3 *db)
{
    char *err = NULL;
    if (sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &err) != SQLITE_OK) {
        printf("end transaction issue %s\n", err);
        sqlite3_free(err);
    }
    return !err;
}

void closedb(sqlite3 *db)
{
    sqlite3_close(db);
}

void insertdbfin(sqlite3_stmt *res)
{
    sqlite3_finalize(res);
}

sqlite3_stmt *insertdbprep(sqlite3 *db, const char *sqli)
{
    const char *tail = NULL;
    int error = SQLITE_OK;
    sqlite3_stmt *reso = NULL;

    // WARNING: passing length-arg that is longer than SQL text
    error = sqlite3_prepare_v2(db, sqli, MAXSQL, &reso, &tail);
    if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on insertdbprep: error %d %s err %s\n",
                  error, sqli, sqlite3_errmsg(db));
          return NULL;
    }
    return reso;
}

int insertdbgo(struct work *pwork, struct entry_data *ed,
               sqlite3_stmt *res)
{
    /* Not checking arguments */

    int rc = 0;

    char *zname     = sqlite3_mprintf("%q", pwork->name + pwork->name_len - pwork->basename_len);
    char *ztype     = sqlite3_mprintf("%c", ed->type);
    char *zino      = sqlite3_mprintf("%" PRIu64, ed->statuso.st_ino);
    char *zlinkname = sqlite3_mprintf("%q", ed->linkname);
    char *zosstext1 = sqlite3_mprintf("%q", ed->osstext1);
    char *zosstext2 = sqlite3_mprintf("%q", ed->osstext2);
    char *zpino     = sqlite3_mprintf("%" PRIu64, pwork->pinode);
    sqlite3_bind_text(res,  1,  zname, -1, SQLITE_STATIC);
    sqlite3_bind_text(res,  2,  ztype, -1, SQLITE_STATIC);
    sqlite3_bind_text(res,  3,  zino,  -1, SQLITE_STATIC);
    sqlite3_bind_int64(res, 4,  ed->statuso.st_mode);
    sqlite3_bind_int64(res, 5,  ed->statuso.st_nlink);
    sqlite3_bind_int64(res, 6,  ed->statuso.st_uid);
    sqlite3_bind_int64(res, 7,  ed->statuso.st_gid);
    sqlite3_bind_int64(res, 8,  ed->statuso.st_size);
    sqlite3_bind_int64(res, 9,  ed->statuso.st_blksize);
    sqlite3_bind_int64(res, 10, ed->statuso.st_blocks);
    sqlite3_bind_int64(res, 11, ed->statuso.st_atime);
    sqlite3_bind_int64(res, 12, ed->statuso.st_mtime);
    sqlite3_bind_int64(res, 13, ed->statuso.st_ctime);
    sqlite3_bind_text(res,  14, zlinkname, -1, SQLITE_STATIC);

    /* only insert xattr names */
    char xattr_names[MAXXATTR];
    ssize_t xattr_names_len = xattr_get_names(&ed->xattrs, xattr_names, sizeof(xattr_names), XATTRDELIM);
    if (xattr_names_len < 0) {
         xattr_names_len = 0;
    }
    sqlite3_bind_blob64(res, 15, xattr_names, xattr_names_len, SQLITE_STATIC);

    sqlite3_bind_int64(res, 16, ed->crtime);
    sqlite3_bind_int64(res, 17, ed->ossint1);
    sqlite3_bind_int64(res, 18, ed->ossint2);
    sqlite3_bind_int64(res, 19, ed->ossint3);
    sqlite3_bind_int64(res, 20, ed->ossint4);
    sqlite3_bind_text(res,  21, zosstext1, -1, SQLITE_STATIC);
    sqlite3_bind_text(res,  22, zosstext2, -1, SQLITE_STATIC);
    sqlite3_bind_text(res,  23, zpino,     -1, SQLITE_STATIC);

    const int error = sqlite3_step(res);
    if (error != SQLITE_DONE) {
        fprintf(stderr, "SQL insertdbgo step: %s error %d err %s\n",
                pwork->name, error, sqlite3_errstr(error));
        rc = 1;
    }

    sqlite3_free(zpino);
    sqlite3_free(zosstext2);
    sqlite3_free(zosstext1);
    sqlite3_free(zlinkname);
    sqlite3_free(zino);
    sqlite3_free(ztype);
    sqlite3_free(zname);
    sqlite3_reset(res);
    sqlite3_clear_bindings(res); /* NULL will cause invalid read here */

    return rc;
}

int insertdbgo_xattrs_avail(struct entry_data *ed, sqlite3_stmt *res)
{
    /* Not checking arguments */

    int error = SQLITE_DONE;
    for(size_t i = 0; (i < ed->xattrs.count) && (error == SQLITE_DONE); i++) {
        struct xattr *xattr = &ed->xattrs.pairs[i];

        sqlite3_bind_int64(res, 1, ed->statuso.st_ino);
        sqlite3_bind_blob(res,  2, xattr->name,  xattr->name_len,  SQLITE_STATIC);
        sqlite3_bind_blob(res,  3, xattr->value, xattr->value_len, SQLITE_STATIC);

        error = sqlite3_step(res);
        sqlite3_reset(res);
    }

    return error;
}

int insertdbgo_xattrs(struct input *in, struct stat *dir,
                      struct work *work, struct entry_data *ed,
                      sll_t *xattr_db_list, struct template_db *xattr_template,
                      const char *topath, const size_t topath_len,
                      sqlite3_stmt *xattrs_res, sqlite3_stmt *xattr_files_res) {
    /*
     * permissions that roll in - skip processing 0 xattrs
     *
     * permissions that can't roll in - don't create external
     * xattrs db just because there's a file with different
     * permissions
     */
    if (ed->xattrs.count == 0) {
        return 0;
    }

    /* insert into the xattrs_avail table inside db.db */
    const int rollin_score = xattr_can_rollin(dir, &ed->statuso);
    if (rollin_score) {
        insertdbgo_xattrs_avail(ed, xattrs_res);
    }
    /* can't roll in, so need external dbs */
    else {
        struct xattr_db *xattr_uid_db = NULL;
        struct xattr_db *xattr_gid_db = NULL;

        /* linear search since there shouldn't be too many users/groups that have xattrs */
        sll_loop(xattr_db_list, node) {
            struct xattr_db *xattr_db = (struct xattr_db *) sll_node_data(node);
            if (xattr_db->st.st_uid == ed->statuso.st_uid) {
                xattr_uid_db = xattr_db;

                if (xattr_gid_db) {
                    break;
                }
            }

            if ((xattr_db->st.st_gid == ed->statuso.st_gid) &&
                ((xattr_db->st.st_mode & 0040) == (ed->statuso.st_mode & 0040))) {
                xattr_gid_db = xattr_db;

                if (xattr_uid_db) {
                    break;
                }
            }
        }

        /* user id/permission mismatch */
        if (!xattr_uid_db) {
            xattr_uid_db = create_xattr_db(xattr_template,
                                           topath, topath_len,
                                           in,
                                           work->pinode,
                                           ed->statuso.st_uid,
                                           in->nobody.gid,
                                           ed->statuso.st_mode,
                                           xattr_files_res);
            if (!xattr_uid_db) {
                return 1;
            }
            sll_push(xattr_db_list, xattr_uid_db);
        }

        /* group id/permission mismatch */
        if (!xattr_gid_db) {
            xattr_gid_db = create_xattr_db(xattr_template,
                                           topath, topath_len,
                                           in,
                                           work->pinode,
                                           in->nobody.uid,
                                           ed->statuso.st_gid,
                                           ed->statuso.st_mode,
                                           xattr_files_res);
            if (!xattr_gid_db) {
                return 1;
            }
            sll_push(xattr_db_list, xattr_gid_db);
        }

        /* insert into per-user and per-group xattr dbs */
        insertdbgo_xattrs_avail(ed, xattr_uid_db->res);
        insertdbgo_xattrs_avail(ed, xattr_gid_db->res);
    }

    return 0;
}

int insertsumdb(sqlite3 *sdb, const char *path, struct work *pwork, struct entry_data *ed, struct sum *su)
{
    sqlite3_stmt *res = insertdbprep(sdb, SUMMARY_INSERT);
    if (!res) {
        return 1;
    }

    char *zname     = sqlite3_mprintf("%q", path);
    char *ztype     = sqlite3_mprintf("%c", ed->type);
    char *zino      = sqlite3_mprintf("%" PRIu64, ed->statuso.st_ino);
    char *zlinkname = sqlite3_mprintf("%q", ed->linkname);
    char *zpino     = sqlite3_mprintf("%" PRIu64, pwork->pinode);

    char xattrnames[MAXXATTR] = "\x00";
    xattr_get_names(&ed->xattrs, xattrnames, sizeof(xattrnames), XATTRDELIM);

    char *zxattrnames = sqlite3_mprintf("%q", xattrnames);

    sqlite3_bind_text(res,   1,  zname, -1, SQLITE_STATIC);
    sqlite3_bind_text(res,   2,  ztype, -1, SQLITE_STATIC);
    sqlite3_bind_text(res,   3,  zino,  -1, SQLITE_STATIC);
    sqlite3_bind_int64(res,  4,  ed->statuso.st_mode);
    sqlite3_bind_int64(res,  5,  ed->statuso.st_nlink);
    sqlite3_bind_int64(res,  6,  ed->statuso.st_uid);
    sqlite3_bind_int64(res,  7,  ed->statuso.st_gid);
    sqlite3_bind_int64(res,  8,  ed->statuso.st_size);
    sqlite3_bind_int64(res,  9,  ed->statuso.st_blksize);
    sqlite3_bind_int64(res,  10, ed->statuso.st_blocks);
    sqlite3_bind_int64(res,  11, ed->statuso.st_atime);
    sqlite3_bind_int64(res,  12, ed->statuso.st_mtime);
    sqlite3_bind_int64(res,  13, ed->statuso.st_ctime);
    sqlite3_bind_text(res,   14, zlinkname, -1, SQLITE_STATIC);
    sqlite3_bind_blob64(res, 15, zxattrnames, strlen(zxattrnames), SQLITE_STATIC);
    sqlite3_bind_int64(res,  16, su->totfiles);
    sqlite3_bind_int64(res,  17, su->totlinks);
    sqlite3_bind_int64(res,  18, su->minuid);
    sqlite3_bind_int64(res,  19, su->maxuid);
    sqlite3_bind_int64(res,  20, su->mingid);
    sqlite3_bind_int64(res,  21, su->maxgid);
    sqlite3_bind_int64(res,  22, su->minsize);
    sqlite3_bind_int64(res,  23, su->maxsize);
    sqlite3_bind_int64(res,  24, su->totzero);
    sqlite3_bind_int64(res,  25, su->totltk);
    sqlite3_bind_int64(res,  26, su->totmtk);
    sqlite3_bind_int64(res,  27, su->totltm);
    sqlite3_bind_int64(res,  28, su->totmtm);
    sqlite3_bind_int64(res,  29, su->totmtg);
    sqlite3_bind_int64(res,  30, su->totmtt);
    sqlite3_bind_int64(res,  31, su->totsize);
    sqlite3_bind_int64(res,  32, su->minctime);
    sqlite3_bind_int64(res,  33, su->maxctime);
    sqlite3_bind_int64(res,  34, su->minmtime);
    sqlite3_bind_int64(res,  35, su->maxmtime);
    sqlite3_bind_int64(res,  36, su->minatime);
    sqlite3_bind_int64(res,  37, su->maxatime);
    sqlite3_bind_int64(res,  38, su->minblocks);
    sqlite3_bind_int64(res,  39, su->maxblocks);
    sqlite3_bind_int64(res,  40, su->totxattr);
    sqlite3_bind_int64(res,  41, 0); /* depth */
    sqlite3_bind_int64(res,  42, su->mincrtime);
    sqlite3_bind_int64(res,  43, su->maxcrtime);
    sqlite3_bind_int64(res,  44, su->minossint1);
    sqlite3_bind_int64(res,  45, su->maxossint1);
    sqlite3_bind_int64(res,  46, su->totossint1);
    sqlite3_bind_int64(res,  47, su->minossint2);
    sqlite3_bind_int64(res,  48, su->maxossint2);
    sqlite3_bind_int64(res,  49, su->totossint2);
    sqlite3_bind_int64(res,  50, su->minossint3);
    sqlite3_bind_int64(res,  51, su->maxossint3);
    sqlite3_bind_int64(res,  52, su->totossint3);
    sqlite3_bind_int64(res,  53, su->minossint4);
    sqlite3_bind_int64(res,  54, su->maxossint4);
    sqlite3_bind_int64(res,  55, su->totossint4);
    sqlite3_bind_int64(res,  56, 0); /* rectype */
    sqlite3_bind_text(res,   57, zpino, -1, SQLITE_STATIC);
    sqlite3_bind_int64(res,  58, 1); /* isroot */
    sqlite3_bind_int64(res,  59, 0); /* rollupscore */

    sqlite3_step(res);
    sqlite3_reset(res);
    sqlite3_clear_bindings(res);

    sqlite3_free(zxattrnames);
    sqlite3_free(zpino);
    sqlite3_free(zlinkname);
    sqlite3_free(zino);
    sqlite3_free(ztype);
    sqlite3_free(zname);

    insertdbfin(res);
    return 0;
}

static int get_str_callback(void *args, int count, char **data, char **columns) {
    (void) count;
    (void) columns;

    char **str = (char **) args;

    if (data[0]) {
        const size_t len = strlen(data[0]) + 2; /* + 2 for quotation marks */
        *str = malloc(len + 1);
        SNFORMAT_S(*str, len + 1, 3,
                   "'", (size_t) 1,
                   data[0], len - 2,
                   "'", (size_t) 1);
    }
    else {
        *str = malloc(5);
        snprintf(*str, 5, "NULL");
    }

    return 0;
}

int inserttreesumdb(const char *name, sqlite3 *sdb, struct sum *su,int rectype,int uid,int gid)
{
    int depth = 0;
    for(const char *c = name; *c; c++) {
        depth += (*c == '/');
    }

    char *err = NULL;

    char *inode = NULL;
    if (sqlite3_exec(sdb, "SELECT inode FROM " SUMMARY " WHERE isroot == 1;",
                     get_str_callback, &inode, &err) != SQLITE_OK ) {
        fprintf(stderr, "Could not get inode from %s: %s\n", SUMMARY, err);
        sqlite3_free(err);
        return 1;
    }

    char *pinode = NULL;
    if (sqlite3_exec(sdb, "SELECT pinode FROM " SUMMARY " WHERE isroot == 1;",
                     get_str_callback, &pinode, &err) != SQLITE_OK ) {
        fprintf(stderr, "Could not get inode from %s: %s\n", SUMMARY, err);
        free(inode);
        sqlite3_free(err);
        return 1;
    }

    char sqlstmt[MAXSQL];
    SNPRINTF(sqlstmt, MAXSQL, "INSERT INTO " TREESUMMARY " VALUES (%s, %s, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %d, %d);",
             inode, pinode, su->totsubdirs, su->maxsubdirfiles, su->maxsubdirlinks, su->maxsubdirsize, su->totfiles, su->totlinks, su->minuid, su->maxuid, su->mingid, su->maxgid, su->minsize, su->maxsize, su->totzero, su->totltk, su->totmtk, su->totltm, su->totmtm, su->totmtg, su->totmtt, su->totsize, su->minctime, su->maxctime, su->minmtime, su->maxmtime, su->minatime, su->maxatime, su->minblocks, su->maxblocks, su->totxattr, depth, su->mincrtime, su->maxcrtime, su->minossint1, su->maxossint1, su->totossint1, su->minossint2, su->maxossint2, su->totossint2, su->minossint3, su->maxossint3, su->totossint3, su->minossint4, su->maxossint4, su->totossint4, su->totextdbs, rectype, uid, gid);

    free(pinode);
    free(inode);

    err = NULL;
    if (sqlite3_exec(sdb, sqlstmt, 0, 0, &err) != SQLITE_OK ) {
        fprintf(stderr, "SQL error on insert (treesummary): %s %s\n", err, sqlstmt);
        sqlite3_free(err);
    }

    return !!err;
}

/* return the directory you are currently in */
static void path(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc; (void) argv;
    struct work *work = (struct work *) sqlite3_user_data(context);
    size_t user_dirname_len = work->orig_root.len + work->name_len - work->root_parent.len - work->root_basename_len;
    char *user_dirname = malloc(user_dirname_len + 1);

    SNFORMAT_S(user_dirname, user_dirname_len + 1, 2,
               work->orig_root.data, work->orig_root.len,
               work->name + work->root_parent.len + work->root_basename_len, work->name_len - work->root_parent.len - work->root_basename_len);

    sqlite3_result_text(context, user_dirname, user_dirname_len, free);
}

/* return the basename of the directory you are currently in */
static void epath(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc; (void) argv;
    struct work *work = (struct work *) sqlite3_user_data(context);

    sqlite3_result_text(context, work->name + work->name_len - work->basename_len,
                        work->basename_len, SQLITE_STATIC);
}

/* return the fullpath of the directory you are currently in */
static void fpath(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc; (void) argv;
    struct work *work = (struct work *) sqlite3_user_data(context);

    if (!work->fullpath) {
        work->fullpath = realpath(work->name, NULL);
        work->fullpath_len = strlen(work->fullpath);
    }

    sqlite3_result_text(context, work->fullpath, work->fullpath_len, SQLITE_STATIC);
}

/*
 * Usage:
 *     SELECT rpath(sname, sroll)
 *     FROM vrsummary;
 *
 *     SELECT rpath(sname, sroll) || "/" || name
 *     FROM vrpentries;
 */
static void rpath(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc;

    /* work->name contains the current directory being operated on */
    struct work *work = (struct work *) sqlite3_user_data(context);
    const int rollupscore = sqlite3_value_int(argv[1]);

    size_t user_dirname_len = 0;
    char *user_dirname = NULL;

    const size_t root_len = work->root_parent.len + work->root_basename_len;

    if (rollupscore == 0) { /* use work->name */
        user_dirname_len = work->orig_root.len + work->name_len - root_len;
        user_dirname = malloc(user_dirname_len + 1);

        SNFORMAT_S(user_dirname, user_dirname_len + 1, 2,
                   work->orig_root.data, work->orig_root.len,
                   work->name + root_len, work->name_len - root_len);
    }
    else { /* reconstruct full path out of argv[0] */
        refstr_t input;
        input.data = (char *) sqlite3_value_text(argv[0]);
        input.len  = strlen(input.data);

        /*
         * fullpath = work->name[:-work->basename_len] + input
         */
        const size_t fullpath_len = work->name_len - work->basename_len + input.len;
        char *fullpath = malloc(fullpath_len + 1);
        SNFORMAT_S(fullpath, fullpath_len + 1, 2,
                   work->name, work->name_len - work->basename_len,
                   input.data, input.len);

        /*
         * replace fullpath prefix with original user input
         */
        user_dirname_len = work->orig_root.len + fullpath_len - root_len;
        user_dirname = malloc(user_dirname_len + 1);
        SNFORMAT_S(user_dirname, user_dirname_len + 1, 2,
                   work->orig_root.data, work->orig_root.len,
                   fullpath + root_len, fullpath_len - root_len);

        free(fullpath);
    }

    sqlite3_result_text(context, user_dirname, user_dirname_len, free);
}

static void uidtouser(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc;

    const char *text = (char *) sqlite3_value_text(argv[0]);

    const int fuid = atoi(text);
    struct passwd *fmypasswd = getpwuid(fuid);
    const char *show = fmypasswd?fmypasswd->pw_name:text;

    sqlite3_result_text(context, show, -1, SQLITE_TRANSIENT);
}

static void gidtogroup(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc;

    const char *text = (char *) sqlite3_value_text(argv[0]);

    const int fgid = atoi(text);
    struct group *fmygroup = getgrgid(fgid);
    const char *show = fmygroup?fmygroup->gr_name:text;

    sqlite3_result_text(context, show, -1, SQLITE_TRANSIENT);
}

static void modetotxt(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc;
    int fmode;
    char tmode[64];
    fmode = sqlite3_value_int(argv[0]);
    modetostr(tmode, sizeof(tmode), fmode);
    sqlite3_result_text(context, tmode, -1, SQLITE_TRANSIENT);
}

static void sqlite3_strftime(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc;

    const char *fmt = (char *) sqlite3_value_text(argv[0]); /* format    */
    const time_t t = sqlite3_value_int64(argv[1]);          /* timestamp */

    char buf[MAXPATH];
    #ifdef LOCALTIME_R
    struct tm tm;
    strftime(buf, sizeof(buf), fmt, localtime_r(&t, &tm));
    #else
    strftime(buf, sizeof(buf), fmt, localtime(&t));
    #endif
    sqlite3_result_text(context, buf, -1, SQLITE_TRANSIENT);
}

/* uint64_t goes up to E */
static const char SIZE[] = {'K', 'M', 'G', 'T', 'P', 'E'};

/*
 * Returns the number of blocks required to store a given size
 * Unfilled blocks count as one full block (round up)
 *
 * This function attempts to replicate ls output and is mainly
 * intended for gufi_ls, so use with caution.
 *
 * blocksize(1024, "K")    -> 1K
 * blocksize(1024, "1K")   -> 1
 * blocksize(1024, "KB")   -> 2KB
 * blocksize(1024, "1KB")  -> 2
 * blocksize(1024, "KiB")  -> 1K
 * blocksize(1024, "1KiB") -> 1
 */
static void blocksize(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    const char *size_s = (const char *) sqlite3_value_text(argv[0]);
    const char *unit_s = (const char *) sqlite3_value_text(argv[1]);
    const size_t unit_s_len = strlen(unit_s);

    uint64_t size = 0;
    if (sscanf(size_s, "%" PRIu64, &size) != 1) {
        sqlite3_result_error(context, "Bad blocksize size", -1);
        return;
    }

    /* whether or not a coefficent was found - affects printing */
    uint64_t unit_size = 0;
    const int coefficient_found = sscanf(unit_s, "%" PRIu64, &unit_size);
    if (coefficient_found == 1) {
        if (unit_size == 0) {
            sqlite3_result_error(context, "Bad blocksize unit", -1);
            return;
        }
    }
    else {
        /* if there were no numbers, default to 1 */
        unit_size = 1;
    }

    /*
     * get block size suffix i.e. 1KB -> KB
     */
    const char *unit = unit_s;
    {
        /*
         * find first non-numerical character
         * decimal points are not accepted, and will break this loop
         */
        size_t offset = 0;
        while ((offset < unit_s_len) &&
               (('0' <= unit[offset]) && (unit[offset] <= '9'))) {
            offset++;
        }

        unit += offset;
    }

    const size_t len = strlen(unit);

    /* suffix is too long */
    if (len > 3) {
        sqlite3_result_error(context, "Bad blocksize unit", -1);
        return;
    }

    /* suffix is optional */
    if (len) {
        if ((len > 1) && (unit[len - 1] != 'B')) {
            sqlite3_result_error(context, "Bad blocksize unit", -1);
            return;
        }

        uint64_t multiplier = 1024;
        if (len == 2) {
            multiplier = 1000;
        }
        else if (len == 3) {
            if (unit[1] != 'i') {
                sqlite3_result_error(context, "Bad blocksize unit", -1);
                return;
            }
        }

        int found = 0;
        for(size_t i = 0; i < sizeof(SIZE); i++) {
            unit_size *= multiplier;
            if (unit[0] == SIZE[i]) {
                found = 1;
                break;
            }
        }

        if (!found) {
            sqlite3_result_error(context, "Bad blocksize unit", -1);
            return;
        }
    }

    const uint64_t blocks = (size / unit_size) + (!!(size % unit_size));

    char buf[MAXPATH];
    size_t buf_len = snprintf(buf, sizeof(buf), "%" PRIu64, blocks);

    /* add unit to block count */
    if (!coefficient_found) {
        buf_len += snprintf(buf + buf_len, sizeof(buf) - buf_len, "%s", unit);
    }

    sqlite3_result_text(context, buf, buf_len, SQLITE_TRANSIENT);
}

/* Returns a string containg the size with as large of a unit as reasonable */
static void human_readable_size(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    char buf[MAXPATH];

    const char *size_s = (const char *) sqlite3_value_text(argv[0]);
    double size = 0;

    if (sscanf(size_s, "%lf", &size) != 1) {
        sqlite3_result_error(context, "Bad size", -1);
        return;
    }

    size_t unit_index = 0;
    while (size >= 1024) {
        size /= 1024;
        unit_index++;
    }

    if (unit_index == 0) {
        snprintf(buf, sizeof(buf), "%.1f", size);
    }
    else {
        snprintf(buf, sizeof(buf), "%.1f%c", size, SIZE[unit_index - 1]);
    }

    sqlite3_result_text(context, buf, -1, SQLITE_TRANSIENT);
}

static void relative_level(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc; (void) argv;

    size_t level = (size_t) (uintptr_t) sqlite3_user_data(context);
    sqlite3_result_int64(context, level);
}

static void starting_point(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc; (void) argv;

    refstr_t *root = (refstr_t *) sqlite3_user_data(context);
    sqlite3_result_text(context, root->data, root->len, SQLITE_STATIC);
}

static void sqlite_basename(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    char *path = (char *) sqlite3_value_text(argv[0]);

    if (!path) {
        sqlite3_result_text(context, "", 0, SQLITE_TRANSIENT);
        return;
    }

    const size_t path_len = strlen(path);

    /* remove trailing slashes */
    const size_t trimmed_len = trailing_non_match_index(path, path_len, "/", 1);
    if (!trimmed_len) {
        sqlite3_result_text(context, "/", 1, SQLITE_STATIC);
        return;
    }

    /* basename(work->name) will be the same as the first part of the input path, so remove it */
    const size_t offset = trailing_match_index(path, trimmed_len, "/", 1);

    const size_t bn_len = trimmed_len - offset;
    char *bn = path + offset;

    sqlite3_result_text(context, bn, bn_len, SQLITE_STATIC);
}

/*
 * One pass standard deviation (sample)
 * https://mathcentral.uregina.ca/QQ/database/QQ.09.06/h/murtaza1.html
 */
typedef struct {
    double sum;
    double sum_sq;
    uint64_t count;
} stdev_t;

static void stdev_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;
    stdev_t *data = (stdev_t *) sqlite3_aggregate_context(context, sizeof(*data));
    const double value = sqlite3_value_double(argv[0]);

    data->sum += value;
    data->sum_sq += value * value;
    data->count++;
}

static void stdevs_final(sqlite3_context *context) {
    stdev_t *data = (stdev_t *) sqlite3_aggregate_context(context, sizeof(*data));

    if (data->count < 2) {
        sqlite3_result_null(context);
    }
    else {
        const double variance = ((data->count * data->sum_sq) - (data->sum * data->sum)) / (data->count * (data->count - 1));
        sqlite3_result_double(context, sqrt(variance));
    }
}

static void stdevp_final(sqlite3_context *context) {
    stdev_t *data = (stdev_t *) sqlite3_aggregate_context(context, sizeof(*data));

    if (data->count < 2) {
        sqlite3_result_null(context);
    }
    else {
        const double variance = ((data->count * data->sum_sq) - (data->sum * data->sum)) / (data->count * data->count);
        sqlite3_result_double(context, sqrt(variance));
    }
}

static void median_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;
    sll_t *data = (sll_t *) sqlite3_aggregate_context(context, sizeof(*data));
    if (sll_get_size(data) == 0) {
        sll_init(data);
    }

    const double value = sqlite3_value_double(argv[0]);
    sll_push(data, (void *) (uintptr_t) value);
}

/* /\* */
/*  * find kth largest element */
/*  * */
/*  * Adapted from code by Russell Cohen */
/*  * https://rcoh.me/posts/linear-time-median-finding/ */
/*  *\/ */
/* static double quickselect(sll_t *sll, uint64_t count, uint64_t k) { */
/*     /\* cache unused values here since partitioning destroys the original list *\/ */
/*     sll_t cache; */
/*     sll_init(&cache); */

/*     sll_t lt, eq, gt; */
/*     sll_init(&lt); */
/*     sll_init(&eq); */
/*     sll_init(&gt); */

/*     while (count > 1) { */
/*         /\* TODO: Better pivot selection *\/ */
/*         const uint64_t pivot_idx = (rand() * rand()) % count; */
/*         double pivot = 0; */
/*         size_t i = 0; */
/*         sll_loop(sll, node) { */
/*             if (i == pivot_idx) { */
/*                 pivot = (double) (uintptr_t) sll_node_data(node); */
/*                 break; */
/*             } */
/*             i++; */
/*         } */

/*         sll_node_t *node = NULL; */
/*         while ((node = sll_head_node(sll))) { */
/*             const double value = (double) (uint64_t) sll_node_data(node); */
/*             if (value < pivot) { */
/*                 sll_move_append_first(&lt, sll, 1); */
/*             } */
/*             else if (value > pivot) { */
/*                 sll_move_append_first(&gt, sll, 1); */
/*             } */
/*             else { */
/*                 sll_move_append_first(&eq, sll, 1); */
/*             } */
/*         } */

/*         /\* sll is empty at this point *\/ */

/*         const uint64_t lt_size = sll_get_size(&lt); */
/*         const uint64_t eq_size = sll_get_size(&eq); */

/*         if (k < lt_size) { */
/*             sll_move_append(sll,    &lt); */
/*             sll_move_append(&cache, &eq); */
/*             sll_move_append(&cache, &gt); */
/*         } */
/*         else if (k < (lt_size + eq_size)) { */
/*             sll_move_append(&cache, &lt); */
/*             sll_move_append(sll,    &eq); */
/*             sll_move_append(&cache, &gt); */
/*             break; */
/*         } */
/*         else { */
/*             k -= lt_size + eq_size; */
/*             sll_move_append(&cache, &lt); */
/*             sll_move_append(&cache, &eq); */
/*             sll_move_append(sll,    &gt); */
/*         } */

/*         count = sll_get_size(sll); */
/*     } */

/*     /\* restore original list's contents (different order) *\/ */
/*     sll_move_append(sll, &cache); */

/*     return (double) (uintptr_t) sll_node_data(sll_head_node(sll)); */
/* } */

static int cmp_double(const void *lhs, const void *rhs) {
    return * (double *) lhs - * (double *) rhs;
}

static void median_final(sqlite3_context *context) {
    sll_t *data = (sll_t *) sqlite3_aggregate_context(context, sizeof(*data));

    const uint64_t count = sll_get_size(data);
    double median = 0;

    /* skip some mallocs */
    if (count == 0) {
        sqlite3_result_null(context);
        goto cleanup;
    }
    else if (count == 1) {
        median = (double) (uintptr_t) sll_node_data(sll_head_node(data));
        goto ret_median;
    }
    else if (count == 2) {
        median = ((double) (uintptr_t) sll_node_data(sll_head_node(data)) +
                  (double) (uintptr_t) sll_node_data(sll_tail_node(data))) / 2.0;
        goto ret_median;
    }

    const uint64_t half = count / 2;

    double *arr = malloc(count * sizeof(double));
    size_t i = 0;
    sll_loop(data, node) {
        arr[i++] = (double) (uintptr_t) sll_node_data(node);
    }

    qsort(arr, count, sizeof(double), cmp_double);

    median = arr[half];
    if (!(count & 1)) {
        median += arr[half - 1];
        median /= 2.0;
    }
    free(arr);

    /* median = quickselect(data, count, half); */
    /* if (!(count & 1)) { */
    /*     median += quickselect(data, count, half - 1); */
    /*     median /= 2.0; */
    /* } */

  ret_median:
    sqlite3_result_double(context, median);

  cleanup:
    sll_destroy(data, NULL);
}

int addqueryfuncs(sqlite3 *db) {
    return !(
        (sqlite3_create_function(db,   "uidtouser",           1,   SQLITE_UTF8,
                                 NULL, &uidtouser,                 NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "gidtogroup",          1,   SQLITE_UTF8,
                                 NULL, &gidtogroup,                NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "modetotxt",           1,   SQLITE_UTF8,
                                 NULL, &modetotxt,                 NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "strftime",            2,   SQLITE_UTF8,
                                 NULL, &sqlite3_strftime,          NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "blocksize",           2,   SQLITE_UTF8,
                                 NULL, &blocksize,                 NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "human_readable_size", 1,   SQLITE_UTF8,
                                 NULL, &human_readable_size,       NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "basename",            1,   SQLITE_UTF8,
                                 NULL, &sqlite_basename,           NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "stdevs",              1,   SQLITE_UTF8,
                                 NULL, NULL,  stdev_step,          stdevs_final) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "stdevp",              1,   SQLITE_UTF8,
                                 NULL, NULL,  stdev_step,          stdevp_final) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "median",              1,   SQLITE_UTF8,
                                 NULL, NULL,  median_step,         median_final) == SQLITE_OK) &&
        addhistfuncs(db)
        );
}

int addqueryfuncs_with_context(sqlite3 *db, struct work *work) {
    return !(
        (sqlite3_create_function(db,  "path",                      0, SQLITE_UTF8,
                                 work,                             &path,           NULL, NULL) == SQLITE_OK) &&
        (sqlite3_create_function(db,  "epath",                     0, SQLITE_UTF8,
                                 work,                             &epath,          NULL, NULL) == SQLITE_OK) &&
        (sqlite3_create_function(db,  "fpath",                     0, SQLITE_UTF8,
                                 work,                             &fpath,          NULL, NULL) == SQLITE_OK) &&
        (sqlite3_create_function(db,  "rpath",                     2, SQLITE_UTF8,
                                 work,                             &rpath,          NULL, NULL) == SQLITE_OK) &&
        (sqlite3_create_function(db,  "starting_point",            0,  SQLITE_UTF8,
                                 (void *) &work->orig_root,        &starting_point, NULL, NULL) == SQLITE_OK) &&
        (sqlite3_create_function(db,  "level",                     0,  SQLITE_UTF8,
                                 (void *) (uintptr_t) work->level, &relative_level, NULL, NULL) == SQLITE_OK)
        );
}

struct xattr_db *create_xattr_db(struct template_db *tdb,
                                 const char *path, const size_t path_len,
                                 struct input *in,
                                 long long int pinode,
                                 uid_t uid, gid_t gid, mode_t mode,
                                 sqlite3_stmt *file_list) {
    /* /\* make sure only one of uid or gid is set *\/ */
    /* if (!((uid == in->nobody.uid) ^ (gid == in->nobody.gid)))) { */
    /*     return NULL; */
    /* } */

    struct xattr_db *xdb = malloc(sizeof(struct xattr_db));
    mode_t xattr_db_mode = 0;

    /* set the relative path in xdb */
    if (uid != in->nobody.uid) {
        xdb->filename_len = SNPRINTF(xdb->filename, MAXPATH,
                                     XATTR_UID_FILENAME_FORMAT, uid);
        xattr_db_mode = 0600;
    }
    else if (gid != in->nobody.gid) {
        /* g+r */
        if ((mode & 0040) == 0040) {
            xdb->filename_len = SNPRINTF(xdb->filename, MAXPATH,
                                         XATTR_GID_W_READ_FILENAME_FORMAT, gid);
            xattr_db_mode = 0040;
        }
        /* g-r */
        else {
            xdb->filename_len = SNPRINTF(xdb->filename, MAXPATH,
                                         XATTR_GID_WO_READ_FILENAME_FORMAT, gid);
            xattr_db_mode = 0000;
        }
    }

    xdb->pinode = pinode;

    /* store full path here */
    char filename[MAXPATH];
    SNFORMAT_S(filename, MAXPATH, 3,
               path, path_len,
               "/", (size_t) 1,
               xdb->filename, xdb->filename_len);

    xdb->db  = NULL;
    xdb->res = NULL;
    xdb->file_list = file_list;
    xdb->st.st_mode = xattr_db_mode;
    xdb->st.st_uid = uid;
    xdb->st.st_gid = gid;

    if (copy_template(tdb, filename, xdb->st.st_uid, xdb->st.st_gid) != 0) {
        destroy_xattr_db(xdb);
        return NULL;
    }

    if (chmod(filename, xdb->st.st_mode) != 0) {
        const int err = errno;
        fprintf(stderr, "Warning: Unable to set permissions for %s: %d\n", filename, err);
    }

    xdb->db = opendb(filename, SQLITE_OPEN_READWRITE, 0, 0, NULL, NULL);

    if (!xdb->db) {
        destroy_xattr_db(xdb);
        return NULL;
    }

    xdb->res = insertdbprep(xdb->db, XATTRS_PWD_INSERT);
    return xdb;
}

void destroy_xattr_db(void *ptr) {
    struct xattr_db *xdb = (struct xattr_db *) ptr;

    /* write out per-user/per-group xattrs */
    sqlite3_finalize(xdb->res);
    closedb(xdb->db);

    /* add this xattr db to the list of dbs to open when creating view */
    sqlite3_bind_text( xdb->file_list, 1, EXTERNAL_TYPE_XATTR.data, EXTERNAL_TYPE_XATTR.len, SQLITE_STATIC);
    sqlite3_bind_int64(xdb->file_list, 2, xdb->pinode);
    sqlite3_bind_text( xdb->file_list, 3, xdb->filename, xdb->filename_len,                  SQLITE_STATIC);
    sqlite3_bind_int64(xdb->file_list, 4, xdb->st.st_mode);
    sqlite3_bind_int64(xdb->file_list, 5, xdb->st.st_uid);
    sqlite3_bind_int64(xdb->file_list, 6, xdb->st.st_gid);

    sqlite3_step(xdb->file_list);
    sqlite3_reset(xdb->file_list);
    sqlite3_clear_bindings(xdb->file_list);

    free(xdb);
}

static int count_pwd(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    size_t *xattr_count = (size_t *) args;
    sscanf(data[0], "%zu", xattr_count); /* skip check */
    return 0;
}

/* Delete all entries in the XATTR_ROLLUP table */
int xattrs_rollup_cleanup(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    refstr_t *name = (refstr_t *) args;

    char *relpath = data[0];
    char fullpath[MAXPATH];
    SNFORMAT_S(fullpath, sizeof(fullpath), 3,
               name->data, name->len,
               "/", (size_t) 1,
               relpath, strlen(relpath));

    /* if the file is missing, return ok */
    struct stat st;
    if (stat(fullpath, &st) != 0) {
        return (errno != ENOENT);
    }

    int rc = 0;

    sqlite3 *db = opendb(fullpath, SQLITE_OPEN_READWRITE, 0, 0, NULL, NULL);

    if (db) {
        char *err_msg = NULL;
        size_t xattr_count = 0;
        if (sqlite3_exec(db,
                         "DELETE FROM " XATTRS_ROLLUP ";"
                         "SELECT COUNT(*) FROM " XATTRS_PWD ";",
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
            fprintf(stderr, "Warning: Failed to clear out rolled up xattr data from %s: %s\n",
                    fullpath, err_msg);
            sqlite3_free(err_msg);
            rc = 1;
        }
    }

    closedb(db);

    return rc;
}

/*
 * some characters cannot be used directly
 * in sqlite and need to be converted
 *
 * this function does not take in an sqlite
 * instance, but is here because it is only
 * applicable to sqlite
 */
size_t sqlite_uri_path(char *dst, size_t dst_size,
                       const char *src, size_t *src_len) {
    struct convert {
        char src;
        char *dst;
        size_t dst_len;
    };

    static const struct convert need_convert[] = {
        /* https://www.sqlite.org/uri.html */
        {'#', "%23", 3},
        {'?', "%3f", 3},

        /* percent signs need to be replaced too */
        {'%', "%25", 3},
    };

    static const size_t need_convert_count = sizeof(need_convert) / sizeof(need_convert[0]);

    size_t d = 0;
    size_t s = 0;
    while ((d < dst_size) && (s < *src_len)) {
        int converted = 0;
        for(size_t i = 0; (d < dst_size) && (i < need_convert_count); i++) {
            const struct convert *ep = &need_convert[i];
            if (src[s] == ep->src) {
                for(size_t j = 0; (d < dst_size) && (j < ep->dst_len); j++) {
                    dst[d++] = ep->dst[j];
                }
                converted = 1;
                break;
            }
        }

        if (!converted) {
            dst[d++] = src[s];
        }

        s++;
    }

    *src_len = s;
    return d;
}

static int get_rollupscore_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    return !(sscanf(data[0], "%d", (int *) args) == 1);
}

int get_rollupscore(sqlite3 *db, int *rollupscore) {
    char *err = NULL;
    if (sqlite3_exec(db, "SELECT rollupscore FROM summary WHERE isroot == 1;",
                     get_rollupscore_callback, rollupscore, &err) != SQLITE_OK) {
        fprintf(stderr, "Could not get rollupscore: %s\n", err);
        sqlite3_free(err);
        return 1;
    }

    return 0;
}

int treesummary_exists_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) data; (void) columns;
    int *trecs = (int *) args;
    (*trecs)++;
    return 0;
}

int bottomup_collect_treesummary(sqlite3 *db, const char *dirname, sll_t *subdirs,
                                 const enum CheckRollupScore check_rollupscore) {
    if (!db) {
        return 1;
    }

    int rollupscore = 0;
    switch(check_rollupscore) {
        case ROLLUPSCORE_CHECK:
            get_rollupscore(db, &rollupscore);
            break;
        case ROLLUPSCORE_KNOWN_YES:
            rollupscore = 1;
            break;
        case ROLLUPSCORE_DONT_CHECK:
        case ROLLUPSCORE_KNOWN_NO:
        default:
            break;
    }

    if (rollupscore != 0) {
        /*
         * this directory has been rolled up, so all information is
         * available here: compute the treesummary, no need to go
         * further down
         *
         * don't bother copying it out of sqlite only to put it back in
         *
         * this ignores all treesummary tables since all summary
         * table rows are immediately available
         *
         * -1 because a directory is not a subdirectory of itself
         */
        static const char TREESUMMARY_ROLLUP_COMPUTE_INSERT[] =
            "INSERT INTO " TREESUMMARY " SELECT (SELECT inode FROM " SUMMARY " WHERE isroot == 1), (SELECT pinode FROM " SUMMARY " WHERE isroot == 1), COUNT(*) - 1, MAX(totfiles), MAX(totlinks), MAX(size), TOTAL(totfiles), TOTAL(totlinks), MIN(minuid), MAX(maxuid), MIN(mingid), MAX(maxgid), MIN(minsize), MAX(maxsize), TOTAL(totzero), TOTAL(totltk), TOTAL(totmtk), TOTAL(totltm), TOTAL(totmtm), TOTAL(totmtg), TOTAL(totmtt), TOTAL(totsize), MIN(minctime), MAX(maxctime), MIN(minmtime), MAX(maxmtime), MIN(minatime), MAX(maxatime), MIN(minblocks), MAX(maxblocks), TOTAL(totxattr), TOTAL(depth), MIN(mincrtime), MAX(maxcrtime), MIN(minossint1), MAX(maxossint1), TOTAL(totossint1), MIN(minossint2), MAX(maxossint2), TOTAL(totossint2), MIN(minossint3), MAX(maxossint3), TOTAL(totossint3), MIN(minossint4), MAX(maxossint4), TOTAL(totossint4), (SELECT COUNT(*) FROM " EXTERNAL_DBS "), rectype, uid, gid FROM " SUMMARY ";";

        char *err = NULL;
        if (sqlite3_exec(db, TREESUMMARY_ROLLUP_COMPUTE_INSERT, NULL, NULL, &err) != SQLITE_OK) {
            fprintf(stderr, "Error: Failed to compute treesummary for \"%s\": %s\n",
                    dirname, err);
            sqlite3_free(err);
            return 1;
        }

        return 0;
    }

    /*
     * this directory has not rolled up, so have to use child
     * directories to get information
     *
     * every child is either
     *     - a leaf, and thus all the data is available in the summary table, or
     *     - has a treesummary table because BottomUp is comming back up
     *
     * reopen child dbs to collect data
     *     - not super efficient, but should not happen too often
     */

    struct sum tsum;
    zeroit(&tsum);

    struct sum sum;
    sll_loop(subdirs, node) {
        struct BottomUp *subdir = (struct BottomUp *) sll_node_data(node);

        char child_dbname[MAXPATH];
        SNFORMAT_S(child_dbname, sizeof(child_dbname), 2,
                   subdir->name, subdir->name_len,
                   "/" DBNAME, DBNAME_LEN + 1);

        sqlite3 *child_db = opendb(child_dbname, SQLITE_OPEN_READONLY, 1, 0, NULL, NULL);
        if (!child_db) {
            continue;
        }

        char *err = NULL;
        int trecs = 0;
        if (sqlite3_exec(child_db, TREESUMMARY_EXISTS,
            treesummary_exists_callback,
            &trecs, &err) == SQLITE_OK) {
            zeroit(&sum);

            querytsdb(dirname, &sum, child_db, !(trecs < 1));

            /* aggregate subdirectory summaries */
            tsumit(&sum, &tsum);
        }
        else {
            fprintf(stderr, "Warning: Failed to check for existance of treesummary table in child \"%s\": %s\n",
                    subdir->name, err);
            sqlite3_free(err);
        }

        closedb(child_db);
    }

    /* add summary data from this directory */
    zeroit(&sum);
    querytsdb(dirname, &tsum, db, 0);
    tsumit(&sum, &tsum);
    tsum.totsubdirs--;

    return inserttreesumdb(dirname, db, &tsum, 0, 0, 0);
}
