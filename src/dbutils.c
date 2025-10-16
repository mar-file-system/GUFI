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
#include "trie.h"

const char READDIRPLUS_CREATE[] =
    DROP_TABLE(READDIRPLUS)
    READDIRPLUS_SCHEMA(READDIRPLUS);

const char READDIRPLUS_INSERT[] =
    "INSERT INTO " READDIRPLUS " VALUES (@path, @type, @inode, @pinode, @depth, @suspect);";

const char ENTRIES_CREATE[] =
    DROP_TABLE(ENTRIES)
    ENTRIES_SCHEMA(ENTRIES, "");

const char ENTRIES_INSERT[] =
    "INSERT INTO " ENTRIES " VALUES ("
    "@name, @type, @inode, @mode, @nlink, @uid, @gid, @size, "
    "@blksize, @blocks, @atime, @mtime, @ctime, @linkname, @xattr_names, @crtime, "
    "@ossint1, @ossint2, @ossint3, @ossint4, @osstext1, @osstext2"
    ");";

const char SUMMARY_CREATE[] =
    DROP_TABLE(SUMMARY)
    SUMMARY_SCHEMA(SUMMARY, "");

static const char SUMMARY_INSERT[] =
    "INSERT INTO " SUMMARY " VALUES ("

    /* directory */
    "@name, @type, @inode, @mode, @nlink, @uid, @gid, @size, "
    "@blksize, @blocks, @atime, @mtime, @ctime, @linkname, @xattr_names, @crtime, "

    /* summary */
    "@totfiles, @totlinks, "
    "@minuid, @maxuid, @mingid, @maxgid, "
    "@minsize, @maxsize, @totzero, "
    "@totltk, @totmtk, "
    "@totltm, @totmtm, "
    "@totmtg, @totmtt, "
    "@totsize, "
    "@minctime,   @maxctime,   @totctime,  "
    "@minmtime,   @maxmtime,   @totmtime,  "
    "@minatime,   @maxatime,   @totatime,  "
    "@minblocks,  @maxblocks,  @totblocks, "
    "@totxattr,   @depth, "
    "@mincrtime,  @maxcrtime,  @totcrtime,  "
    "@minossint1, @maxossint1, @totossint1, "
    "@minossint2, @maxossint2, @totossint2, "
    "@minossint3, @maxossint3, @totossint3, "
    "@minossint4, @maxossint4, @totossint4, "

    /* extra info */
    "@rectype,    @pinode,     @isroot,     @rollupscore"
    ");";

const char VRSUMMARY_CREATE[] =
    DROP_VIEW(VRSUMMARY)
    "CREATE VIEW " VRSUMMARY " AS SELECT "
    "REPLACE(" SUMMARY ".name, RTRIM(" SUMMARY ".name, REPLACE(" SUMMARY ".name, '/', '')), '') AS dname, "
    SUMMARY ".name AS sname, "
    SUMMARY ".rollupscore AS sroll, "
    "(SELECT COUNT(*) FROM " SUMMARY " AS c WHERE c.pinode == " SUMMARY ".inode) AS srollsubdirs, "
    SUMMARY ".* "
    "FROM " SUMMARY
    ";";

const char PENTRIES_ROLLUP_CREATE[] =
    DROP_TABLE(PENTRIES_ROLLUP)
    PENTRIES_ROLLUP_SCHEMA(PENTRIES_ROLLUP);

const char PENTRIES_ROLLUP_INSERT[] =
    "INSERT INTO " PENTRIES_ROLLUP " VALUES ("
    "@name, @type, @inode, @mode, @nlink, @uid, @gid, @size, "
    "@blksize, @blocks, @atime, @mtime, @ctime, @linkname, @xattr_names, @crtime, "
    "@ossint1, @ossint2, @ossint3, @ossint4, @osstext1, @osstext2, "
    "@pinode, @ppinode"
    ");";

const char PENTRIES_CREATE[] =
    DROP_VIEW(PENTRIES)
    "CREATE VIEW " PENTRIES " AS SELECT "
    ENTRIES ".*, " SUMMARY ".inode AS pinode, "
    SUMMARY ".pinode AS ppinode "
    "FROM " ENTRIES ", " SUMMARY " "
    "WHERE isroot == 1 "
    "UNION "
    "SELECT * FROM " PENTRIES_ROLLUP
    ";";

/* vrentries is not created because rolled up entries tables are not correct */

const char VRPENTRIES_CREATE[] =
    DROP_VIEW(VRPENTRIES)
    "CREATE VIEW " VRPENTRIES " AS SELECT "
    "REPLACE(" VRSUMMARY ".name, RTRIM("      VRSUMMARY ".name, REPLACE("    VRSUMMARY ".name, '/', '')), '') AS dname, "
    VRSUMMARY ".name         AS sname, "      VRSUMMARY ".mode        AS dmode, "
    VRSUMMARY ".nlink        AS dnlink, "
    VRSUMMARY ".uid          AS duid, "       VRSUMMARY ".gid         AS dgid, "
    VRSUMMARY ".size         AS dsize, "
    VRSUMMARY ".blksize      AS dblksize, "   VRSUMMARY ".blocks      AS dblocks, "
    VRSUMMARY ".atime        AS datime, "
    VRSUMMARY ".mtime        AS dmtime, "
    VRSUMMARY ".ctime        AS dctime, "
    VRSUMMARY ".linkname     AS dlinkname, "
    VRSUMMARY ".totfiles     AS dtotfiles, "  VRSUMMARY ".totlinks    AS dtotlinks, "
    VRSUMMARY ".minuid       AS dminuid, "    VRSUMMARY ".maxuid      AS dmaxuid, "
    VRSUMMARY ".mingid       AS dmingid, "    VRSUMMARY ".maxgid      AS dmaxgid, "
    VRSUMMARY ".minsize      AS dminsize, "   VRSUMMARY ".maxsize     AS dmaxsize, "
    VRSUMMARY ".totzero      AS dtotzero, "
    VRSUMMARY ".totltk       AS dtotltk, "    VRSUMMARY ".totmtk      AS dtotmtk, "
    VRSUMMARY ".totltm       AS totltm, "     VRSUMMARY ".totmtm      AS dtotmtm, "
    VRSUMMARY ".totmtg       AS dtotmtg, "    VRSUMMARY ".totmtt      AS dtotmtt, "
    VRSUMMARY ".totsize      AS dtotsize, "
    VRSUMMARY ".minctime     AS dminctime, "  VRSUMMARY ".maxctime    AS dmaxctime, "
    VRSUMMARY ".totctime     AS dtotctime, "
    VRSUMMARY ".minmtime     AS dminmtime, "  VRSUMMARY ".maxmtime    AS dmaxmtime, "
    VRSUMMARY ".totmtime     AS dtotmtime, "
    VRSUMMARY ".minatime     AS dminatime, "  VRSUMMARY ".maxatime    AS dmaxatime, "
    VRSUMMARY ".totatime     AS dtotatime, "
    VRSUMMARY ".minblocks    AS dminblocks, " VRSUMMARY ".maxblocks   AS dmaxblocks, "
    VRSUMMARY ".totblocks    AS dtotblocks, "
    VRSUMMARY ".totxattr     AS dtotxattr, "  VRSUMMARY ".depth       AS ddepth, "
    VRSUMMARY ".mincrtime    AS dmincrtime, " VRSUMMARY ".maxcrtime   AS dmaxcrtime, "
    VRSUMMARY ".totcrtime    AS dtotcrtime, "
    VRSUMMARY ".rollupscore  AS sroll, "      VRSUMMARY ".isroot      AS atroot, "
    VRSUMMARY ".srollsubdirs AS srollsubdirs, "
    PENTRIES ".* "
    "FROM " VRSUMMARY ", " PENTRIES " "
    "WHERE " VRSUMMARY ".inode == " PENTRIES ".pinode;";

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

static sqlite3 *attachdb_internal(const char *name, const char *attach, sqlite3 *db, const char *dbn, const int print_err) {
    char *err = NULL;
    if (sqlite3_exec(db, attach, NULL, NULL, print_err?(&err):NULL) != SQLITE_OK) {
        if (print_err) {
            sqlite_print_err_and_free(err, stderr, "Cannot attach database \"%s\" as \"%s\": %s\n", name, dbn, err);
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

    return attachdb_internal(name, attach, db, dbn, print_err);
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

    return attachdb_internal(name, attach, db, dbn, print_err);
}

sqlite3 *detachdb_cached(const char *name, sqlite3 *db, const char *sql, const int print_err) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, print_err?(&err):NULL) != SQLITE_OK) {
        if (print_err) {
            sqlite_print_err_and_free(err, stderr, "Cannot detach database \"%s\": %s\n", name, err);
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
        sqlite_print_err_and_free(err, stderr, "%s:%d SQL error while executing '%s' on %s: '%s' (%d)\n", __FILE__, __LINE__, sql_name, name, err, rc);
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
                modifydb_func modifydb, void *modifydb_args) {
    sqlite3 *db = NULL;

    if (sqlite3_open_v2(name, &db, flags | SQLITE_OPEN_URI, GUFI_SQLITE_VFS) != SQLITE_OK) {
        if (!(flags & SQLITE_OPEN_CREATE)) {
            sqlite_print_err_and_free(NULL, stderr, "Cannot open database: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db));
        }
        sqlite3_close(db); /* close db even if it didn't open to avoid memory leaks */
        return NULL;
    }

    if (setpragmas) {
        /* ignore errors */
        set_db_pragmas(db);
    }

    if (load_extensions) {
        /* not handling errors: if loading extensions fails, there are bigger issues */

        /* enable loading of extensions */
        sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, NULL);

        /* load the sqlite3-pcre extension */
        sqlite3_pcre2_init(db, NULL, NULL);
    }

    if (modifydb) {
        if (flags & SQLITE_OPEN_READONLY) {
            fprintf(stderr, "Database %s opened in READONLY mode, but a modifydb function was provided\n", name);
            sqlite3_close(db);
            return NULL;
        }
        else {
            if (modifydb(name, db, modifydb_args) != 0) {
                sqlite_print_err_and_free(NULL, stderr, "Cannot modify database: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db));
                sqlite3_close(db);
                return NULL;
            }
        }
    }

    return db;
}

int querytsdb(const char *name, struct sum *sum, sqlite3 *db, int ts) {
    static const char *ts_str[] = {
        "SELECT "
        "totfiles, totlinks, "
        "minuid, maxuid, mingid, maxgid, "
        "minsize, maxsize, totzero, "
        "totltk, totmtk, "
        "totltm, totmtm, "
        "totmtg, totmtt, "
        "totsize, "
        "minctime,   maxctime,   totctime,  "
        "minmtime,   maxmtime,   totmtime,  "
        "minatime,   maxatime,   totatime,  "
        "minblocks,  maxblocks,  totblocks, "
        "totxattr, "
        "mincrtime,  maxcrtime,  totcrtime,  "
        "minossint1, maxossint1, totossint1, "
        "minossint2, maxossint2, totossint2, "
        "minossint3, maxossint3, totossint3, "
        "minossint4, maxossint4, totossint4, "
        "(SELECT COUNT(*) FROM " EXTERNAL_DBS ") "
        "FROM " SUMMARY " "
        "WHERE rectype == 0;",
        /* ************************************** */
        "SELECT "
        "t.totfiles, t.totlinks, "
        "t.minuid, t.maxuid, t.mingid, t.maxgid, "
        "t.minsize, t.maxsize, t.totzero, "
        "t.totltk, t.totmtk, "
        "t.totltm, t.totmtm, "
        "t.totmtg, t.totmtt, "
        "t.totsize, "
        "t.minctime,   t.maxctime,   t.totctime,  "
        "t.minmtime,   t.maxmtime,   t.totmtime,  "
        "t.minatime,   t.maxatime,   t.totatime,  "
        "t.minblocks,  t.maxblocks,  t.totblocks, "
        "t.totxattr, "
        "t.mincrtime,  t.maxcrtime,  t.totcrtime,  "
        "t.minossint1, t.maxossint1, t.totossint1, "
        "t.minossint2, t.maxossint2, t.totossint2, "
        "t.minossint3, t.maxossint3, t.totossint3, "
        "t.minossint4, t.maxossint4, t.totossint4, "
        "t.totextdbs, t.totsubdirs, t.maxsubdirfiles, t.maxsubdirlinks, t.maxsubdirsize "
        "FROM " TREESUMMARY " AS t, " SUMMARY " AS s "
        "WHERE (s.isroot == 1) AND (s.inode == t.inode) AND (t.rectype == 0);",
    };

    const char *sqlstmt = ts_str[ts];
    sqlite3_stmt *res = NULL;
    if (sqlite3_prepare_v2(db, sqlstmt, MAXSQL, &res, NULL) != SQLITE_OK) {
        sqlite_print_err_and_free(NULL, stderr, "SQL error on query: %s: %s, err: %s\n",
                                  sqlstmt, name, sqlite3_errmsg(db));
        return 1;
    }

    int rows = 0;
    while (sqlite3_step(res) != SQLITE_DONE) {
        struct sum curr;
        zeroit(&curr);

        curr.totfiles     = sqlite3_column_int64(res,   0);
        curr.totlinks     = sqlite3_column_int64(res,   1);
        curr.minuid       = sqlite3_column_int64(res,   2);
        curr.maxuid       = sqlite3_column_int64(res,   3);
        curr.mingid       = sqlite3_column_int64(res,   4);
        curr.maxgid       = sqlite3_column_int64(res,   5);
        curr.minsize      = sqlite3_column_int64(res,   6);
        curr.maxsize      = sqlite3_column_int64(res,   7);
        curr.totzero      = sqlite3_column_int64(res,   8);
        curr.totltk       = sqlite3_column_int64(res,   9);
        curr.totmtk       = sqlite3_column_int64(res,  10);
        curr.totltm       = sqlite3_column_int64(res,  11);
        curr.totmtm       = sqlite3_column_int64(res,  12);
        curr.totmtg       = sqlite3_column_int64(res,  13);
        curr.totmtt       = sqlite3_column_int64(res,  14);
        curr.totsize      = sqlite3_column_int64(res,  15);
        curr.minctime     = sqlite3_column_int64(res,  16);
        curr.maxctime     = sqlite3_column_int64(res,  17);
        curr.totctime     = sqlite3_column_int64(res,  18);
        curr.minmtime     = sqlite3_column_int64(res,  19);
        curr.maxmtime     = sqlite3_column_int64(res,  20);
        curr.totmtime     = sqlite3_column_int64(res,  21);
        curr.minatime     = sqlite3_column_int64(res,  22);
        curr.maxatime     = sqlite3_column_int64(res,  23);
        curr.totatime     = sqlite3_column_int64(res,  24);
        curr.minblocks    = sqlite3_column_int64(res,  25);
        curr.maxblocks    = sqlite3_column_int64(res,  26);
        curr.totblocks    = sqlite3_column_int64(res,  27);
        curr.totxattr     = sqlite3_column_int64(res,  28);

        curr.mincrtime    = sqlite3_column_int64(res,  29);
        curr.maxcrtime    = sqlite3_column_int64(res,  30);
        curr.totcrtime    = sqlite3_column_int64(res,  31);
        curr.minossint1   = sqlite3_column_int64(res,  32);
        curr.maxossint1   = sqlite3_column_int64(res,  33);
        curr.totossint1   = sqlite3_column_int64(res,  34);
        curr.minossint2   = sqlite3_column_int64(res,  35);
        curr.maxossint2   = sqlite3_column_int64(res,  36);
        curr.totossint2   = sqlite3_column_int64(res,  37);
        curr.minossint3   = sqlite3_column_int64(res,  38);
        curr.maxossint3   = sqlite3_column_int64(res,  39);
        curr.totossint3   = sqlite3_column_int64(res,  40);
        curr.minossint4   = sqlite3_column_int64(res,  41);
        curr.maxossint4   = sqlite3_column_int64(res,  42);
        curr.totossint4   = sqlite3_column_int64(res,  43);

        curr.totextdbs    = sqlite3_column_int64(res,  44);

        if (ts) {
            curr.totsubdirs     = sqlite3_column_int64(res, 45);
            curr.maxsubdirfiles = sqlite3_column_int64(res, 46);
            curr.maxsubdirlinks = sqlite3_column_int64(res, 47);
            curr.maxsubdirsize  = sqlite3_column_int64(res, 48);
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
    if (sqlite3_exec(db, "BEGIN TRANSACTION;", NULL , NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "begin transaction issue %s\n", err);
    }
    return !err;
}

int stopdb(sqlite3 *db)
{
    char *err = NULL;
    if (sqlite3_exec(db, "END TRANSACTION;", NULL, NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "end transaction issue %s\n", err);
    }
    return !err;
}

void closedb(sqlite3 *db)
{
    sqlite3_close(db);
}

int copy_columns_callback(void *args, int count, char **data, char **columns) {
    (void) columns;

    /*
     * strs should have space for at least count columns
     *
     * make sure to initialize strs[i] to NULL or good values
     */
    char **strs = (char **) args;

    for(int i = 0; i < count; i++) {
        if (!data[i]) {
            continue;
        }

        const size_t len = strlen(data[i]);

        if (!strs[i]) {
            strs[i] = malloc(len + 1);
        }

        SNFORMAT_S(strs[i], len + 1, 1,
                   data[i], len);
    }

    return 0;
}

void insertdbfin(sqlite3_stmt *res)
{
    sqlite3_finalize(res);
}

sqlite3_stmt *insertdbprep(sqlite3 *db, const char *sqli)
{
    const char *tail = NULL;
    sqlite3_stmt *reso = NULL;

    // WARNING: passing length-arg that is longer than SQL text
    const int error = sqlite3_prepare_v2(db, sqli, MAXSQL, &reso, &tail);
    if (error != SQLITE_OK) {
        sqlite_print_err_and_free(NULL, stderr, "SQL error on insertdbprep: error %d %s err %s\n",
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
    char *zino      = sqlite3_mprintf("%" PRIu64, pwork->statuso.st_ino);

    char *zlinkname = sqlite3_mprintf("%q", ed->linkname);
    char *zosstext1 = sqlite3_mprintf("%q", ed->osstext1);
    char *zosstext2 = sqlite3_mprintf("%q", ed->osstext2);
    char *zpino     = sqlite3_mprintf("%" PRIu64, pwork->pinode);
    sqlite3_bind_text(res,  1,  zname, -1, SQLITE_STATIC);
    sqlite3_bind_text(res,  2,  ztype, -1, SQLITE_STATIC);
    sqlite3_bind_text(res,  3,  zino,  -1, SQLITE_STATIC);
    sqlite3_bind_int64(res, 4,  pwork->statuso.st_mode);
    sqlite3_bind_int64(res, 5,  pwork->statuso.st_nlink);
    sqlite3_bind_int64(res, 6,  pwork->statuso.st_uid);
    sqlite3_bind_int64(res, 7,  pwork->statuso.st_gid);
    sqlite3_bind_int64(res, 8,  pwork->statuso.st_size);
    sqlite3_bind_int64(res, 9,  pwork->statuso.st_blksize);
    sqlite3_bind_int64(res, 10, pwork->statuso.st_blocks);
    sqlite3_bind_int64(res, 11, pwork->statuso.st_atime);
    sqlite3_bind_int64(res, 12, pwork->statuso.st_mtime);
    sqlite3_bind_int64(res, 13, pwork->statuso.st_ctime);
    sqlite3_bind_text(res,  14, zlinkname, -1, SQLITE_STATIC);

    /* only insert xattr names */
    char xattr_names[MAXXATTR];
    ssize_t xattr_names_len = xattr_get_names(&ed->xattrs, xattr_names, sizeof(xattr_names), XATTRDELIM);
    if (xattr_names_len < 0) {
         xattr_names_len = 0;
    }
    sqlite3_bind_blob64(res, 15, xattr_names, xattr_names_len, SQLITE_STATIC);

    sqlite3_bind_int64(res, 16, pwork->crtime);
    sqlite3_bind_int64(res, 17, ed->ossint1);
    sqlite3_bind_int64(res, 18, ed->ossint2);
    sqlite3_bind_int64(res, 19, ed->ossint3);
    sqlite3_bind_int64(res, 20, ed->ossint4);
    sqlite3_bind_text(res,  21, zosstext1, -1, SQLITE_STATIC);
    sqlite3_bind_text(res,  22, zosstext2, -1, SQLITE_STATIC);
    sqlite3_bind_text(res,  23, zpino,     -1, SQLITE_STATIC);

    const int error = sqlite3_step(res);
    if (error != SQLITE_DONE) {
        sqlite_print_err_and_free(NULL, stderr, "SQL insertdbgo step: %s error %d err %s\n",
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
    if (error == SQLITE_DONE) {
        sqlite3_clear_bindings(res); /* NULL will cause invalid read here */
    }

    return rc;
}

int insertdbgo_xattrs_avail(struct work *pwork, struct entry_data *ed, sqlite3_stmt *res)
{
    /* Not checking arguments */

    int error = SQLITE_DONE;
    for(size_t i = 0; (i < ed->xattrs.count) && (error == SQLITE_DONE); i++) {
        struct xattr *xattr = &ed->xattrs.pairs[i];

        sqlite3_bind_int64(res, 1, pwork->statuso.st_ino);
        sqlite3_bind_blob(res,  2, xattr->name,  xattr->name_len,  SQLITE_STATIC);
        sqlite3_bind_blob(res,  3, xattr->value, xattr->value_len, SQLITE_STATIC);

        error = sqlite3_step(res);
        sqlite3_reset(res);
    }

    return error;
}

int insertdbgo_xattrs(struct input *in, struct stat *dir,
                      struct work *pwork, struct entry_data *ed,
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
    const int rollin_score = xattr_can_rollin(dir, &pwork->statuso);
    if (rollin_score) {
        insertdbgo_xattrs_avail(pwork, ed, xattrs_res);
    }
    /* can't roll in, so nepwork external dbs */
    else {
        struct xattr_db *xattr_uid_db = NULL;
        struct xattr_db *xattr_gid_db = NULL;

        /* linear search since there shouldn't be too many users/groups that have xattrs */
        sll_loop(xattr_db_list, node) {
            struct xattr_db *xattr_db = (struct xattr_db *) sll_node_data(node);
            if (xattr_db->st.st_uid == pwork->statuso.st_uid) {
                xattr_uid_db = xattr_db;

                if (xattr_gid_db) {
                    break;
                }
            }

            if ((xattr_db->st.st_gid == pwork->statuso.st_gid) &&
                ((xattr_db->st.st_mode & 0040) == (pwork->statuso.st_mode & 0040))) {
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
                                           pwork->pinode,
                                           pwork->statuso.st_uid,
                                           in->nobody.gid,
                                           pwork->statuso.st_mode,
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
                                           pwork->pinode,
                                           in->nobody.uid,
                                           pwork->statuso.st_gid,
                                           pwork->statuso.st_mode,
                                           xattr_files_res);
            if (!xattr_gid_db) {
                return 1;
            }
            sll_push(xattr_db_list, xattr_gid_db);
        }

        /* insert into per-user and per-group xattr dbs */
        insertdbgo_xattrs_avail(pwork, ed, xattr_uid_db->res);
        insertdbgo_xattrs_avail(pwork, ed, xattr_gid_db->res);
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
    char *zino      = sqlite3_mprintf("%" PRIu64, pwork->statuso.st_ino);
    char *zlinkname = sqlite3_mprintf("%q", ed->linkname);
    char *zpino     = sqlite3_mprintf("%" PRIu64, pwork->pinode);

    char xattrnames[MAXXATTR] = "\x00";
    xattr_get_names(&ed->xattrs, xattrnames, sizeof(xattrnames), XATTRDELIM);

    char *zxattrnames = sqlite3_mprintf("%q", xattrnames);

    sqlite3_bind_text(res,   1,  zname, -1, SQLITE_STATIC);
    sqlite3_bind_text(res,   2,  ztype, -1, SQLITE_STATIC);
    sqlite3_bind_text(res,   3,  zino,  -1, SQLITE_STATIC);
    sqlite3_bind_int64(res,  4,  pwork->statuso.st_mode);
    sqlite3_bind_int64(res,  5,  pwork->statuso.st_nlink);
    sqlite3_bind_int64(res,  6,  pwork->statuso.st_uid);
    sqlite3_bind_int64(res,  7,  pwork->statuso.st_gid);
    sqlite3_bind_int64(res,  8,  pwork->statuso.st_size);
    sqlite3_bind_int64(res,  9,  pwork->statuso.st_blksize);
    sqlite3_bind_int64(res,  10, pwork->statuso.st_blocks);
    sqlite3_bind_int64(res,  11, pwork->statuso.st_atime);
    sqlite3_bind_int64(res,  12, pwork->statuso.st_mtime);
    sqlite3_bind_int64(res,  13, pwork->statuso.st_ctime);
    sqlite3_bind_text(res,   14, zlinkname, -1, SQLITE_STATIC);
    sqlite3_bind_blob64(res, 15, zxattrnames, strlen(zxattrnames), SQLITE_STATIC);
    sqlite3_bind_int64(res,  16, pwork->crtime);
    sqlite3_bind_int64(res,  17, su->totfiles);
    sqlite3_bind_int64(res,  18, su->totlinks);
    sqlite3_bind_int64(res,  19, su->minuid);
    sqlite3_bind_int64(res,  20, su->maxuid);
    sqlite3_bind_int64(res,  21, su->mingid);
    sqlite3_bind_int64(res,  22, su->maxgid);
    sqlite3_bind_int64(res,  23, su->minsize);
    sqlite3_bind_int64(res,  24, su->maxsize);
    sqlite3_bind_int64(res,  25, su->totzero);
    sqlite3_bind_int64(res,  26, su->totltk);
    sqlite3_bind_int64(res,  27, su->totmtk);
    sqlite3_bind_int64(res,  28, su->totltm);
    sqlite3_bind_int64(res,  29, su->totmtm);
    sqlite3_bind_int64(res,  30, su->totmtg);
    sqlite3_bind_int64(res,  31, su->totmtt);
    sqlite3_bind_int64(res,  32, su->totsize);
    sqlite3_bind_int64(res,  33, su->minctime);
    sqlite3_bind_int64(res,  34, su->maxctime);
    sqlite3_bind_int64(res,  35, su->totctime);
    sqlite3_bind_int64(res,  36, su->minmtime);
    sqlite3_bind_int64(res,  37, su->maxmtime);
    sqlite3_bind_int64(res,  38, su->totmtime);
    sqlite3_bind_int64(res,  39, su->minatime);
    sqlite3_bind_int64(res,  40, su->maxatime);
    sqlite3_bind_int64(res,  41, su->totatime);
    sqlite3_bind_int64(res,  42, su->minblocks);
    sqlite3_bind_int64(res,  43, su->maxblocks);
    sqlite3_bind_int64(res,  44, su->totblocks);
    sqlite3_bind_int64(res,  45, su->totxattr);
    sqlite3_bind_int64(res,  46, 0); /* depth */
    sqlite3_bind_int64(res,  47, su->mincrtime);
    sqlite3_bind_int64(res,  48, su->maxcrtime);
    sqlite3_bind_int64(res,  49, su->totcrtime);
    sqlite3_bind_int64(res,  50, su->minossint1);
    sqlite3_bind_int64(res,  51, su->maxossint1);
    sqlite3_bind_int64(res,  52, su->totossint1);
    sqlite3_bind_int64(res,  53, su->minossint2);
    sqlite3_bind_int64(res,  54, su->maxossint2);
    sqlite3_bind_int64(res,  55, su->totossint2);
    sqlite3_bind_int64(res,  56, su->minossint3);
    sqlite3_bind_int64(res,  57, su->maxossint3);
    sqlite3_bind_int64(res,  58, su->totossint3);
    sqlite3_bind_int64(res,  59, su->minossint4);
    sqlite3_bind_int64(res,  60, su->maxossint4);
    sqlite3_bind_int64(res,  61, su->totossint4);
    sqlite3_bind_int64(res,  62, 0); /* rectype */
    sqlite3_bind_text(res,   63, zpino, -1, SQLITE_STATIC);
    sqlite3_bind_int64(res,  64, 1); /* isroot */
    sqlite3_bind_int64(res,  65, 0); /* rollupscore */

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

int inserttreesumdb(const char *name, sqlite3 *sdb, struct sum *su,int rectype,int uid,int gid)
{
    int depth = 0;
    for(const char *c = name; *c; c++) {
        depth += (*c == '/');
    }

    char *err = NULL;

    char *ip[2] = {NULL, NULL};
    if (sqlite3_exec(sdb, "SELECT inode, pinode FROM " SUMMARY " WHERE isroot == 1;",
                     copy_columns_callback, ip, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Could not get inode from %s: %s\n", SUMMARY, err);
        return 1;
    }

    char *inode  = ip[0];
    char *pinode = ip[1];

    char sqlstmt[MAXSQL];
    SNPRINTF(sqlstmt, MAXSQL, "INSERT INTO " TREESUMMARY " VALUES ("
             "'%s', '%s', "               /* inode, pinode, */
             "%lld, "                     /* su->totsubdirs, */
             "%lld, %lld, %lld, "         /* su->maxsubdirfiles, su->maxsubdirlinks, su->maxsubdirsize, */
             "%lld, %lld, "               /* su->totfiles, su->totlinks, */
             "%lld, %lld, "               /* su->minuid,   su->maxuid, */
             "%lld, %lld, "               /* su->mingid,   su->maxgid, */
             "%lld, %lld, "               /* su->minsize,  su->maxsize, */
             "%lld, "                     /* su->totzero, */
             "%lld, %lld, "               /* su->totltk,  su->totmtk, */
             "%lld, %lld, "               /* su->totltm,  su->totmtm, */
             "%lld, %lld, "               /* su->totmtg,  su->totmtt, */
             "%lld, "                     /* su->totsize, */
             "%lld, %lld, %lld, "         /* su->minctime,  su->maxctime,  su->totctime,  */
             "%lld, %lld, %lld, "         /* su->minmtime,  su->maxmtime,  su->totmtime,  */
             "%lld, %lld, %lld, "         /* su->minatime,  su->maxatime,  su->totatime,  */
             "%lld, %lld, %lld, "         /* su->minblocks, su->maxblocks, su->totblocks, */
             "%lld, "                     /* su->totxattr, */
             "%d, "                       /* depth, */
             "%lld, %lld, %lld, "         /* su->mincrtime,  su->maxcrtime,  su->totcrtime,  */
             "%lld, %lld, %lld, "         /* su->minossint1, su->maxossint1, su->totossint1, */
             "%lld, %lld, %lld, "         /* su->minossint2, su->maxossint2, su->totossint2, */
             "%lld, %lld, %lld, "         /* su->minossint3, su->maxossint3, su->totossint3, */
             "%lld, %lld, %lld, "         /* su->minossint4, su->maxossint4, su->totossint4, */
             "%lld, "                     /* su->totextdbs, */
             "%d, %d, %d"                 /* rectype, uid, gid, */
             ");",
             inode, pinode,
             su->totsubdirs,
             su->maxsubdirfiles, su->maxsubdirlinks, su->maxsubdirsize,
             su->totfiles, su->totlinks,
             su->minuid,   su->maxuid,
             su->mingid,   su->maxgid,
             su->minsize,  su->maxsize,
             su->totzero,
             su->totltk,  su->totmtk,
             su->totltm,  su->totmtm,
             su->totmtg,  su->totmtt,
             su->totsize,
             su->minctime,  su->maxctime,  su->totctime,
             su->minmtime,  su->maxmtime,  su->totmtime,
             su->minatime,  su->maxatime,  su->totatime,
             su->minblocks, su->maxblocks, su->totblocks,
             su->totxattr,
             depth,
             su->mincrtime,  su->maxcrtime,  su->totcrtime,
             su->minossint1, su->maxossint1, su->totossint1,
             su->minossint2, su->maxossint2, su->totossint2,
             su->minossint3, su->maxossint3, su->totossint3,
             su->minossint4, su->maxossint4, su->totossint4,
             su->totextdbs,
             rectype, uid, gid);

    free(pinode);
    free(inode);

    err = NULL;
    if (sqlite3_exec(sdb, sqlstmt, NULL, NULL, &err) != SQLITE_OK ) {
        sqlite_print_err_and_free(err, stderr, "SQL error on insert (treesummary): %s %s\n", err, sqlstmt);
    }

    return !!err;
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

    struct xattr_db *xdb = calloc(1, sizeof(struct xattr_db));
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
        fprintf(stderr, "Warning: Unable to set permissions for %s: %s (%d)\n", filename, strerror(err), err);
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

const char ROLLUP_CLEANUP[] =
    "DROP INDEX IF EXISTS " SUMMARY "_idx;"
    "DELETE FROM " PENTRIES_ROLLUP ";"
    "DELETE FROM " SUMMARY " WHERE isroot != 1;"
    "DELETE FROM " XATTRS_ROLLUP ";"
    "SELECT filename FROM " EXTERNAL_DBS_ROLLUP " WHERE type == '" EXTERNAL_TYPE_XATTR_NAME "';"
    "DELETE FROM " EXTERNAL_DBS_ROLLUP ";"
    "VACUUM;"
    "UPDATE " SUMMARY " SET rollupscore = 0;"; /* keep this last to allow it to be modified easily */

const size_t ROLLUP_CLEANUP_SIZE = sizeof(ROLLUP_CLEANUP);

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
            sqlite_print_err_and_free(err_msg, stderr, "Warning: Failed to clear out rolled up xattr data from %s: %s\n", fullpath, err_msg);
            rc = 1;
        }
    }

    closedb(db);

    return rc;
}

static size_t xattr_modify_filename(char **dst, const size_t dst_size,
                                    const char *src, const size_t src_len,
                                    void *args) {
    struct work *work = (struct work *) args;

    if (src[0] == '/') {
        *dst = (char *) src;
        return src_len;
    }

    return SNFORMAT_S(*dst, dst_size, 3,
                      work->name, work->name_len,
                      "/", (size_t) 1,
                      src, src_len);
}

static int create_view(const char *name, sqlite3 *db, const char *query) {
    char *err = NULL;
    const int rc = sqlite3_exec(db, query, NULL, NULL, &err);

    if (rc != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Create %s view failed: %s\n", name, err);
        return 0;
    }

    return 1;
}

static int xattr_create_views(sqlite3 *db) {
    /* create LEFT JOIN views (all rows, with and without xattrs) */
    /* these should run once, and be no-ops afterwards since the backing data of the views get swapped out */

    return !(
        /* entries, pentries, summary */
        create_view(XENTRIES, db, "CREATE TEMP VIEW IF NOT EXISTS " XENTRIES " AS SELECT " ENTRIES ".*, " XATTRS ".name as xattr_name, " XATTRS ".value as xattr_value FROM " ENTRIES " LEFT JOIN " XATTRS " ON " ENTRIES ".inode == " XATTRS ".inode;") &&

        create_view(XPENTRIES, db, "CREATE TEMP VIEW IF NOT EXISTS " XPENTRIES " AS SELECT " PENTRIES ".*, " XATTRS ".name as xattr_name, " XATTRS ".value as xattr_value FROM " PENTRIES " LEFT JOIN " XATTRS " ON " PENTRIES ".inode == " XATTRS ".inode;") &&

        create_view(XSUMMARY, db, "CREATE TEMP VIEW IF NOT EXISTS " XSUMMARY " AS SELECT " SUMMARY ".*, " XATTRS ".name as xattr_name, " XATTRS ".value as xattr_value FROM " SUMMARY " LEFT JOIN " XATTRS " ON " SUMMARY ".inode == " XATTRS ".inode;") &&

        /* vrpentries and vrsummary */
        /* vrentries is not available because rolled up entries tables are not correct */
        create_view(VRXPENTRIES, db, "CREATE TEMP VIEW IF NOT EXISTS " VRXPENTRIES " AS SELECT " VRPENTRIES ".*, " XATTRS ".name as xattr_name, " XATTRS ".value as xattr_value FROM " VRPENTRIES " LEFT JOIN " XATTRS " ON " VRPENTRIES ".inode == " XATTRS ".inode;") &&

        create_view(VRXSUMMARY, db, "CREATE TEMP VIEW IF NOT EXISTS " VRXSUMMARY " AS SELECT " VRSUMMARY ".*, " XATTRS ".name as xattr_name, " XATTRS ".value as xattr_value FROM " VRSUMMARY " LEFT JOIN " XATTRS " ON " VRSUMMARY ".inode == " XATTRS ".inode;")
        );
}

void setup_xattrs_views(struct input *in, sqlite3 *db,
                        struct work *work, size_t *extdb_count) {
    static const refstr_t XATTRS_REF = {
        .data = XATTRS,
        .len  = sizeof(XATTRS) - 1,
    };

    #define XATTRS_COLS " SELECT inode, name, value FROM "
    static const refstr_t XATTRS_COLS_REF = {
        .data = XATTRS_COLS,
        .len  = sizeof(XATTRS_COLS) - 1,
    };

    static const refstr_t XATTRS_AVAIL_REF = {
        .data = XATTRS_AVAIL,
        .len  = sizeof(XATTRS_AVAIL) - 1,
    };

    static const refstr_t XATTRS_TEMPLATE_REF = {
        .data = XATTRS_TEMPLATE,
        .len  = sizeof(XATTRS_TEMPLATE) - 1,
    };

    /* always set up xattrs view */
    external_concatenate(db,
                         in->process_xattrs?&EXTERNAL_TYPE_XATTR:NULL,
                         NULL,
                         &XATTRS_REF,
                         &XATTRS_COLS_REF,
                         &XATTRS_AVAIL_REF,
                         in->process_xattrs?&XATTRS_AVAIL_REF:&XATTRS_TEMPLATE_REF,
                         in->process_xattrs?xattr_modify_filename:NULL, work,
                         external_increment_attachname, extdb_count);

    /* set up xattr variant views */
    xattr_create_views(db);
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

void sqlite_print_err_and_free(char *err, FILE *stream, char *format, ...) {
    va_list args;
    va_start(args, format);
    vfprintf(stream, format, args);
    va_end(args);

    sqlite3_free(err);
}

static int get_rollupscore_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    return !(sscanf(data[0], "%d", (int *) args) == 1);
}

int get_rollupscore(sqlite3 *db, int *rollupscore) {
    char *err = NULL;
    if (sqlite3_exec(db, "SELECT rollupscore FROM summary WHERE isroot == 1;",
                     get_rollupscore_callback, rollupscore, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Could not get rollupscore: %s\n", err);
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
         *   - cannot use treesummary rows that were copied upwards
         *     because leaf directories will not have rows, resulting
         *     in no treesummary rows when joining with current
         *     directory's summary data
         *
         * -1 because a directory is not a subdirectory of itself
         */
        static const char TREESUMMARY_ROLLUP_COMPUTE_INSERT[] =
            "INSERT INTO " TREESUMMARY " SELECT (SELECT "
            "inode FROM " SUMMARY " WHERE isroot == 1), "
            "(SELECT pinode FROM " SUMMARY " WHERE isroot == 1), "
            "COUNT(*) - 1, MAX(totfiles), MAX(totlinks), MAX(size), TOTAL(totfiles), TOTAL(totlinks), "
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
            "(SELECT COUNT(*) FROM " EXTERNAL_DBS "), rectype, uid, gid "
            "FROM " SUMMARY
            ";";

        char *err = NULL;
        if (sqlite3_exec(db, TREESUMMARY_ROLLUP_COMPUTE_INSERT, NULL, NULL, &err) != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr, "Error: Failed to compute treesummary for \"%s\": %s\n",
                                      dirname, err);
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
            sqlite_print_err_and_free(err, stderr, "Warning: Failed to check for existance of treesummary table in child \"%s\": %s\n", subdir->name, err);
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

/*
 * subset of known string to SQLite type conversions
 *
 * https://www.sqlite.org/datatype3.html
 * https://www.sqlite.org/c3ref/c_blob.html
 */
static trie_t *sqlite3_types(void) {
    trie_t *types = trie_alloc();

    trie_insert(types, "INT",     3, (void *) (uintptr_t) SQLITE_INTEGER, NULL);
    trie_insert(types, "INTEGER", 7, (void *) (uintptr_t) SQLITE_INTEGER, NULL);
    trie_insert(types, "INT64",   5, (void *) (uintptr_t) SQLITE_INTEGER, NULL);

    trie_insert(types, "FLOAT",   5, (void *) (uintptr_t) SQLITE_FLOAT,   NULL);
    trie_insert(types, "DOUBLE",  6, (void *) (uintptr_t) SQLITE_FLOAT,   NULL);
    trie_insert(types, "REAL",    4, (void *) (uintptr_t) SQLITE_FLOAT,   NULL);

    trie_insert(types, "TEXT",    4, (void *) (uintptr_t) SQLITE_TEXT,    NULL);

    trie_insert(types, "BLOB",    4, (void *) (uintptr_t) SQLITE_BLOB,    NULL);

    trie_insert(types, "NULL",    4, (void *) (uintptr_t) SQLITE_NULL,    NULL);

    return types;
}

int get_col_types(sqlite3 *db, const refstr_t *sql, int **types, int *cols) {
    /* parse sql */
    sqlite3_stmt *stmt = NULL;
    const int rc = sqlite3_prepare_v2(db, sql->data, sql->len, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: Could not prepare '%s' for getting column types: %s (%d)\n",
                sql->data, sqlite3_errstr(rc), rc);
        return -1;
    }

    /* /\* */
    /*  * need to step if calling sqlite3_column_type, but requires */
    /*  * that the table has at least 1 row of actual values */
    /*  *\/ */
    /* rc = sqlite3_step(stmt); */
    /* if (rc != SQLITE_ROW) { */
    /*     fprintf(stderr, "Error: Failed to evaluate SQL statement '%s': %s (%d)\n", */
    /*             sql->data, sqlite3_errstr(rc), rc); */
    /*     return NULL; */
    /* } */

    /* get column count */
    *cols = sqlite3_column_count(stmt);
    if (*cols == 0) {
        fprintf(stderr, "Error: '%s' returns no data\n", sql->data);
        sqlite3_finalize(stmt);
        return -1;
    }

    trie_t *str2type = sqlite3_types();

    /* get each column's type */
    *types = malloc(*cols * sizeof(int));
    for(int i = 0; i < *cols; i++) {
        const char *type = sqlite3_column_decltype(stmt, i);
        if (!type) {
            (*types)[i] = SQLITE_NULL;
            continue;
        }

        const size_t type_len = strlen(type);

        void *sql_type = NULL;
        if (trie_search(str2type, type, type_len, &sql_type) == 1) {
            (*types)[i] = (uintptr_t) sql_type;
        }
        else {
            (*types)[i] = 0; /* unknown type */
        }
    }

    trie_free(str2type);

    sqlite3_finalize(stmt);
    return 0;
}

int get_col_names(sqlite3 *db, const refstr_t *sql, char ***names, size_t **lens, int *cols) {
    int rc = SQLITE_OK;

    /* parse sql */
    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(db, sql->data, sql->len, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: Could not prepare '%s' for getting column names: %s (%d)\n",
                sql->data, sqlite3_errstr(rc), rc);
        return -1;
    }

    /* get column count */
    *cols = sqlite3_column_count(stmt);
    if (*cols == 0) {
        fprintf(stderr, "Error: '%s' returns no data\n", sql->data);
        sqlite3_finalize(stmt);
        return -1;
    }

    /* get each column's type */
    *names = malloc(*cols * sizeof(char *));
    *lens = malloc(*cols * sizeof(size_t));
    for(int i = 0; i < *cols; i++) {
        const char *name = sqlite3_column_name(stmt, i);
        if (!name) {
            (*names)[i] = NULL;
            (*lens)[i] = 0;
            continue;
        }

        const size_t name_len = strlen(name);

        (*names)[i] = malloc(name_len + 1);
        memcpy((*names)[i], name, name_len);
        (*names)[i][name_len] = '\0';

        (*lens)[i] = name_len;
    }

    sqlite3_finalize(stmt);
    return 0;
}

/* SELECT mode, uid, gid FROM <table>; */
int get_permissions_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    struct Permissions *perms = (struct Permissions *) args;
    return !(
        (sscanf(data[0], "%" STAT_mode, &perms->mode) == 1) &&
        (sscanf(data[1], "%" STAT_uid,  &perms->uid)  == 1) &&
        (sscanf(data[2], "%" STAT_gid,  &perms->gid)  == 1)
    );
}
