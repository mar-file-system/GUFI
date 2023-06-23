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
#include <pwd.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "pcre.h"

#include "BottomUp.h"
#include "dbutils.h"

const char READDIRPLUS_CREATE[] =
    DROP_TABLE(READDIRPLUS)
    "CREATE TABLE " READDIRPLUS "(path TEXT, type TEXT, inode INT64 PRIMARY KEY, pinode INT64, suspect INT64);";

const char READDIRPLUS_INSERT[] =
    "INSERT INTO " READDIRPLUS " VALUES (@path, @type, @inode, @pinode, @suspect);";

const char ENTRIES_CREATE[] =
    DROP_TABLE(ENTRIES)
    "CREATE TABLE " ENTRIES "(name TEXT, type TEXT, inode INT64, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattr_names BLOB, crtime INT64, ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, osstext1 TEXT, osstext2 TEXT);";

const char ENTRIES_INSERT[] =
    "INSERT INTO " ENTRIES " VALUES (@name, @type, @inode, @mode, @nlink, @uid, @gid, @size, @blksize, @blocks, @atime, @mtime, @ctime, @linkname, @xattr_names, @crtime, @ossint1, @ossint2, @ossint3, @ossint4, @osstext1, @osstext2);";

const char SUMMARY_CREATE[] =
    DROP_TABLE(SUMMARY)
    "CREATE TABLE " SUMMARY "(name TEXT, type TEXT, inode INT64, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattr_names BLOB, totfiles INT64, totlinks INT64, minuid INT64, maxuid INT64, mingid INT64, maxgid INT64, minsize INT64, maxsize INT64, totltk INT64, totmtk INT64, totltm INT64, totmtm INT64, totmtg INT64, totmtt INT64, totsize INT64, minctime INT64, maxctime INT64, minmtime INT64, maxmtime INT64, minatime INT64, maxatime INT64, minblocks INT64, maxblocks INT64, totxattr INT64, depth INT64, mincrtime INT64, maxcrtime INT64, minossint1 INT64, maxossint1 INT64, totossint1 INT64, minossint2 INT64, maxossint2 INT64, totossint2 INT64, minossint3 INT64, maxossint3 INT64, totossint3 INT64, minossint4 INT64, maxossint4 INT64, totossint4 INT64, rectype INT64, pinode INT64, isroot INT64, rollupscore INT64);";

static const char SUMMARY_INSERT[] =
    "INSERT INTO " SUMMARY " VALUES (@name, @type, @inode, @mode, @nlink, @uid, @gid, @size, @blksize, @blocks, @atime, @mtime, @ctime, @linkname, @xattr_names, @totfiles, @totlinks, @minuid, @maxuid, @mingid, @maxgid, @minsize, @maxsize, @totltk, @totmtk, @totltm, @totmtm, @totmtg, @totmtt, @totsize, @minctime, @maxctime, @minmtime, @maxmtime, @minatime, @maxatime, @minblocks, @maxblocks, @totxattr, @depth, @mincrtime, @maxcrtime, @minossint1, @maxossint1, @totossint1, @minossint2, @maxossint2, @totossint2, @minossint3, @maxossint3, @totossint3, @minossint4, @maxossint4, @totossint4, @rectype, @pinode, @isroot, @rollupscore);";

const char VRSUMMARY_CREATE[] =
    DROP_VIEW(VRSUMMARY)
    "CREATE VIEW " VRSUMMARY " AS SELECT REPLACE(" SUMMARY ".name, RTRIM(" SUMMARY ".name, REPLACE(" SUMMARY ".name, \"/\", \"\")), \"\") AS dname, " SUMMARY ".name AS sname, " SUMMARY ".rollupscore AS sroll, " SUMMARY ".* FROM " SUMMARY ";";

const char PENTRIES_ROLLUP_CREATE[] =
    DROP_TABLE(PENTRIES_ROLLUP)
    "CREATE TABLE " PENTRIES_ROLLUP "(name TEXT, type TEXT, inode INT64, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattr_names BLOB, crtime INT64, ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, osstext1 TEXT, osstext2 TEXT, pinode INT64);";

const char PENTRIES_ROLLUP_INSERT[] =
    "INSERT INTO " PENTRIES_ROLLUP " VALUES (@name, @type, @inode, @mode, @nlink, @uid, @gid, @size, @blksize, @blocks, @atime, @mtime, @ctime, @linkname, @xattr_names, @crtime, @ossint1, @ossint2, @ossint3, @ossint4, @osstext1, @osstext2, pinode INT64);";

const char PENTRIES_CREATE[] =
    DROP_VIEW(PENTRIES)
    "CREATE VIEW " PENTRIES " AS SELECT " ENTRIES ".*, " SUMMARY ".inode AS pinode FROM " ENTRIES ", " SUMMARY " WHERE isroot == 1 UNION SELECT * FROM " PENTRIES_ROLLUP ";";

const char VRPENTRIES_CREATE[] =
    DROP_VIEW(VRPENTRIES)
    "CREATE VIEW " VRPENTRIES " AS SELECT REPLACE(" SUMMARY ".name, RTRIM(" SUMMARY ".name, REPLACE(" SUMMARY ".name, \"/\", \"\")), \"\") AS dname, " SUMMARY ".name AS sname, " SUMMARY ".mode AS dmode, " SUMMARY ".nlink AS dnlink, " SUMMARY ".uid AS duid, " SUMMARY ".gid AS dgid, " SUMMARY ".size AS dsize, " SUMMARY ".blksize AS dblksize, " SUMMARY ".blocks AS dblocks, " SUMMARY ".atime AS datime, " SUMMARY ".mtime AS dmtime, " SUMMARY ".ctime AS dctime, " SUMMARY ".linkname AS dlinkname, " SUMMARY ".totfiles AS dtotfile, " SUMMARY ".totlinks AS dtotlinks, " SUMMARY ".minuid AS dminuid, " SUMMARY ".maxuid AS dmaxuid, " SUMMARY ".mingid AS dmingid, " SUMMARY ".maxgid AS dmaxgidI, " SUMMARY ".minsize AS dminsize, " SUMMARY ".maxsize AS dmaxsize, " SUMMARY ".totltk AS dtotltk, " SUMMARY ".totmtk AS dtotmtk, " SUMMARY ".totltm AS totltm, " SUMMARY ".totmtm AS dtotmtm, " SUMMARY ".totmtg AS dtotmtg, " SUMMARY ".totmtt AS dtotmtt, " SUMMARY ".totsize AS dtotsize, " SUMMARY ".minctime AS dminctime, " SUMMARY ".maxctime AS dmaxctime, " SUMMARY ".minmtime AS dminmtime, " SUMMARY ".maxmtime AS dmaxmtime, " SUMMARY ".minatime AS dminatime, " SUMMARY ".maxatime AS dmaxatime, " SUMMARY ".minblocks AS dminblocks, " SUMMARY ".maxblocks AS dmaxblocks, " SUMMARY ".totxattr AS dtotxattr, " SUMMARY ".depth AS ddepth, " SUMMARY ".mincrtime AS dmincrtime, " SUMMARY ".maxcrtime AS dmaxcrtime, " SUMMARY ".rollupscore AS sroll, " SUMMARY ".isroot as atroot, " PENTRIES ".* FROM " SUMMARY ", " PENTRIES " WHERE " SUMMARY ".inode == " PENTRIES ".pinode;";

const char TREESUMMARY_EXISTS[] =
    "SELECT name FROM sqlite_master WHERE (type == 'table') AND (name == '" TREESUMMARY "');";

/* summary and tsummary views */
#define vssql(name, value)                                                                           \
    const char vssql##name[] =                                                                       \
        "DROP VIEW IF EXISTS vsummary" #name ";"                                                     \
        "CREATE VIEW vsummary" #name " AS SELECT * FROM " SUMMARY " WHERE rectype=" #value ";";      \
                                                                                                     \
    const char vtssql##name[] =                                                                      \
        "DROP VIEW IF EXISTS vtsummary" #name ";"                                                    \
        "CREATE VIEW vtsummary" #name " AS SELECT * FROM " TREESUMMARY " WHERE rectype=" #value ";"  \

vssql(dir,   0);
vssql(user,  1);
vssql(group, 2);

sqlite3 *attachdb(const char *name, sqlite3 *db, const char *dbn, const int flags, const int print_err)
{
  /* cannot check for sqlite3_snprintf errors except by finding the null terminator, so skipping */
  char attach[MAXSQL];
  if (flags & SQLITE_OPEN_READONLY) {
      sqlite3_snprintf(MAXSQL, attach, "ATTACH 'file:%q?mode=ro" GUFI_SQLITE_VFS_URI "' AS %Q", name, dbn);
  }
  else if (flags & SQLITE_OPEN_READWRITE) {
      sqlite3_snprintf(MAXSQL, attach, "ATTACH %Q AS %Q", name, dbn);
  }

  char *err = NULL;
  if (sqlite3_exec(db, attach, NULL, NULL, print_err?(&err):NULL) != SQLITE_OK) {
      if (print_err) {
          fprintf(stderr, "Cannot attach database as \"%s\": %s\n", dbn, err);
      }
      sqlite3_free(err);
      return NULL;
  }

  return db;
}

sqlite3 *detachdb(const char *name, sqlite3 *db, const char *dbn, const int print_err)
{
  /* cannot check for sqlite3_snprintf errors except by finding the null terminator, so skipping */
  char detach[MAXSQL];
  sqlite3_snprintf(MAXSQL, detach, "DETACH %Q", dbn);

  if (sqlite3_exec(db, detach, NULL, NULL, NULL) != SQLITE_OK) {
      if (print_err) {
          fprintf(stderr, "Cannot detach database: %s %s\n", name, sqlite3_errmsg(db));
      }
      return NULL;
  }

  return db;
}

int create_table_wrapper(const char *name, sqlite3 *db, const char *sql_name, const char *sql) {
    char *err_msg = NULL;
    const int rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "%s:%d SQL error while executing '%s' on %s: '%s' (%d)\n",
                __FILE__, __LINE__, sql_name, name, err_msg, rc);
        sqlite3_free(err_msg);
    }
    return rc;
}

int create_treesummary_tables(const char *name, sqlite3 *db, void *args) {
    if ((create_table_wrapper(name, db, "tsql",        TREESUMMARY_CREATE) != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vtssqldir",   vtssqldir)          != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vtssqluser",  vtssqluser)         != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vtssqlgroup", vtssqlgroup)        != SQLITE_OK)) {
        return -1;
    }

    return 0;
}

int set_db_pragmas(sqlite3 *db) {
    int rc = 0;

    // try to turn sychronization off
    if (sqlite3_exec(db, "PRAGMA synchronous = OFF", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "Could not turn off synchronization\n");
        rc = 1;
    }

    // try to turn journaling off
    if (sqlite3_exec(db, "PRAGMA journal_mode = OFF", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "Could not turn off journaling\n");
        rc = 1;
    }

    // try to store temp_store in memory
    if (sqlite3_exec(db, "PRAGMA temp_store = 2", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "Could not set temporary storage to in-memory\n");
        rc = 1;
    }

    // try to increase the page size
    if (sqlite3_exec(db, "PRAGMA page_size = 16777216", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "Could not set page size\n");
        rc = 1;
    }

    // try to increase the cache size
    if (sqlite3_exec(db, "PRAGMA cache_size = 16777216", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "Could not set cache size\n");
        rc = 1;
    }

    // try to get an exclusive lock
    if (sqlite3_exec(db, "PRAGMA locking_mode = EXCLUSIVE", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "Could not set locking mode\n");
        rc = 1;
    }

    return rc;
}

#if defined(DEBUG) && defined(PER_THREAD_STATS)
#define check_set_start(name)            \
    if (name) {                          \
        timestamp_set_start_raw(*name);  \
    }

#define check_set_end(name)              \
    if (name) {                          \
        timestamp_set_end_raw(*name);    \
    }
#else
#define check_set_start(name)
#define check_set_end(name)
#endif

sqlite3 *opendb(const char *name, int flags, const int setpragmas, const int load_extensions,
                 int (*modifydb_func)(const char *name, sqlite3 *db, void *args), void *modifydb_args
                 #if defined(DEBUG) && defined(PER_THREAD_STATS)
                 , struct start_end *sqlite3_open,   struct start_end *set_pragmas
                 , struct start_end *load_extension, struct start_end *modify_db
                 #endif
    ) {
    sqlite3 *db = NULL;

    check_set_start(sqlite3_open);
    if (sqlite3_open_v2(name, &db, flags | SQLITE_OPEN_URI, GUFI_SQLITE_VFS) != SQLITE_OK) {
        check_set_end(sqlite3_open);
        if (!(flags & SQLITE_OPEN_CREATE)) {
            fprintf(stderr, "Cannot open database: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db));
        }
        sqlite3_close(db); /* close db even if it didn't open to avoid memory leaks */
        return NULL;
    }
    check_set_end(sqlite3_open);

    check_set_start(set_pragmas);
    if (setpragmas) {
        /* ignore errors */
        set_db_pragmas(db);
    }
    check_set_end(set_pragmas);

    check_set_start(load_extension);
    if (load_extensions) {
        if ((sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, NULL) != SQLITE_OK) || /* enable loading of extensions */
            (sqlite3_extension_init(db, NULL, NULL)                                != SQLITE_OK)) { /* load the sqlite3-pcre extension */
            check_set_end(load_extension);
            fprintf(stderr, "Unable to load regex extension\n");
            sqlite3_close(db);
            return NULL;
        }
    }
    check_set_end(load_extension);

    check_set_start(modify_db);
    if (!(flags & SQLITE_OPEN_READONLY) && modifydb_func) {
        if (modifydb_func(name, db, modifydb_args) != 0) {
            check_set_end(modify_db);
            fprintf(stderr, "Cannot modify database: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db));
            sqlite3_close(db);
            return NULL;
        }
    }
    check_set_end(modify_db);

    return db;
}

int querytsdb(const char *name, struct sum *sum, sqlite3 *db, int ts) {
    static const char *ts_str[] = {
        "SELECT totfiles, totlinks, minuid, maxuid, mingid, maxgid, minsize, maxsize, totltk, totmtk, totltm, totmtm, totmtg, totmtt, totsize, minctime, maxctime, minmtime, maxmtime, minatime, maxatime, minblocks, maxblocks, totxattr, mincrtime, maxcrtime, minossint1, maxossint1, totossint1, minossint2, maxossint2, totossint2, minossint3, maxossint3, totossint3, minossint4, maxossint4, totossint4 "
        "FROM summary WHERE rectype == 0;",
        "SELECT totfiles, totlinks, minuid, maxuid, mingid, maxgid, minsize, maxsize, totltk, totmtk, totltm, totmtm, totmtg, totmtt, totsize, minctime, maxctime, minmtime, maxmtime, minatime, maxatime, minblocks, maxblocks, totxattr, mincrtime, maxcrtime, minossint1, maxossint1, totossint1, minossint2, maxossint2, totossint2, minossint3, maxossint3, totossint3, minossint4, maxossint4, totossint4, totsubdirs, maxsubdirfiles, maxsubdirlinks, maxsubdirsize "
        "FROM treesummary WHERE rectype == 0;",
    };

    const char *sqlstmt = ts_str[ts];
    sqlite3_stmt *res = NULL;
    if (sqlite3_prepare_v2(db, sqlstmt, MAXSQL, &res, NULL) != SQLITE_OK) {
        fprintf(stderr, "SQL error on query: %s, name: %s, err: %s\n",
                sqlstmt,name,sqlite3_errmsg(db));
        return -1;
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
        curr.totltk     = sqlite3_column_int64(res, 8);
        curr.totmtk     = sqlite3_column_int64(res, 9);
        curr.totltm     = sqlite3_column_int64(res, 10);
        curr.totmtm     = sqlite3_column_int64(res, 11);
        curr.totmtg     = sqlite3_column_int64(res, 12);
        curr.totmtt     = sqlite3_column_int64(res, 13);
        curr.totsize    = sqlite3_column_int64(res, 14);
        curr.minctime   = sqlite3_column_int64(res, 15);
        curr.maxctime   = sqlite3_column_int64(res, 16);
        curr.minmtime   = sqlite3_column_int64(res, 17);
        curr.maxmtime   = sqlite3_column_int64(res, 18);
        curr.minatime   = sqlite3_column_int64(res, 19);
        curr.maxatime   = sqlite3_column_int64(res, 20);
        curr.minblocks  = sqlite3_column_int64(res, 21);
        curr.maxblocks  = sqlite3_column_int64(res, 22);
        curr.totxattr   = sqlite3_column_int64(res, 23);

        curr.mincrtime  = sqlite3_column_int64(res, 24);
        curr.maxcrtime  = sqlite3_column_int64(res, 25);
        curr.minossint1 = sqlite3_column_int64(res, 26);
        curr.maxossint1 = sqlite3_column_int64(res, 27);
        curr.totossint1 = sqlite3_column_int64(res, 28);
        curr.minossint2 = sqlite3_column_int64(res, 29);
        curr.maxossint2 = sqlite3_column_int64(res, 30);
        curr.totossint2 = sqlite3_column_int64(res, 31);
        curr.minossint3 = sqlite3_column_int64(res, 32);
        curr.maxossint3 = sqlite3_column_int64(res, 33);
        curr.totossint3 = sqlite3_column_int64(res, 34);
        curr.minossint4 = sqlite3_column_int64(res, 35);
        curr.maxossint4 = sqlite3_column_int64(res, 36);
        curr.totossint4 = sqlite3_column_int64(res, 37);

        if (ts) {
            curr.totsubdirs     = sqlite3_column_int64(res, 38);
            curr.maxsubdirfiles = sqlite3_column_int64(res, 39);
            curr.maxsubdirlinks = sqlite3_column_int64(res, 40);
            curr.maxsubdirsize  = sqlite3_column_int64(res, 41);
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
    char *err_msg = NULL;
    if (sqlite3_exec(db, "BEGIN TRANSACTION", NULL , NULL, &err_msg) != SQLITE_OK) printf("begin transaction issue %s\n",sqlite3_errmsg(db));
    sqlite3_free(err_msg);
    return 0;
}

int stopdb(sqlite3 *db)
{
    char *err_msg = NULL;
    sqlite3_exec(db,"END TRANSACTION",NULL, NULL, &err_msg);
    sqlite3_free(err_msg);
    return 0;
}

int closedb(sqlite3 *db)
{
    if (sqlite3_close(db) != SQLITE_OK) {
      printf("closedb issue %s\n", sqlite3_errmsg(db));
      exit(9);
    }
    return 0;
}

int insertdbfin(sqlite3_stmt *res)
{
    sqlite3_finalize(res);
    return 0;
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
               sqlite3 *db, sqlite3_stmt *res)
{
    int error;
    char *zname;
    char *ztype;
    char *zlinkname;
    char *zosstext1;
    char *zosstext2;
    int len=0;;
    const char *shortname;
    int found;
    int cnt;

    shortname=pwork->name;
    len=strlen(pwork->name);
    cnt=0;
    found=0;
    while (len > 0) {
       if (!memcmp(shortname,"/",1)) {
          found=cnt;
       }
       cnt++;
       len--;
       shortname++;
    }
    if (found > 0) {
      shortname=pwork->name+found+1;
    } else {
      shortname=pwork->name;
    }

    zname = sqlite3_mprintf("%q",shortname);
    ztype = sqlite3_mprintf("%c",ed->type);
    zlinkname = sqlite3_mprintf("%q",ed->linkname);
    zosstext1 = sqlite3_mprintf("%q",ed->osstext1);
    zosstext2 = sqlite3_mprintf("%q",ed->osstext2);
    error=sqlite3_bind_text(res,1,zname,-1,SQLITE_STATIC);
    if (error != SQLITE_OK) fprintf(stderr, "SQL insertdbgo bind name: %s error %d err %s\n",pwork->name,error,sqlite3_errmsg(db));
    sqlite3_bind_text(res,2,ztype,-1,SQLITE_STATIC);
    sqlite3_bind_int64(res,3,ed->statuso.st_ino);
    sqlite3_bind_int64(res,4,ed->statuso.st_mode);
    sqlite3_bind_int64(res,5,ed->statuso.st_nlink);
    sqlite3_bind_int64(res,6,ed->statuso.st_uid);
    sqlite3_bind_int64(res,7,ed->statuso.st_gid);
    sqlite3_bind_int64(res,8,ed->statuso.st_size);
    sqlite3_bind_int64(res,9,ed->statuso.st_blksize);
    sqlite3_bind_int64(res,10,ed->statuso.st_blocks);
    sqlite3_bind_int64(res,11,ed->statuso.st_atime);
    sqlite3_bind_int64(res,12,ed->statuso.st_mtime);
    sqlite3_bind_int64(res,13,ed->statuso.st_ctime);
    sqlite3_bind_text(res,14,zlinkname,-1,SQLITE_STATIC);

    /* only insert xattr names */
    char xattr_names[MAXXATTR];
    ssize_t xattr_names_len = xattr_get_names(&ed->xattrs, xattr_names, sizeof(xattr_names), XATTRDELIM);
    if (xattr_names_len < 0) {
         xattr_names_len = 0;
    }
    sqlite3_bind_blob64(res, 15, xattr_names, xattr_names_len, SQLITE_STATIC);

    sqlite3_bind_int64(res,16,ed->crtime);
    sqlite3_bind_int64(res,17,ed->ossint1);
    sqlite3_bind_int64(res,18,ed->ossint2);
    sqlite3_bind_int64(res,19,ed->ossint3);
    sqlite3_bind_int64(res,20,ed->ossint4);
    error=sqlite3_bind_text(res,21,zosstext1,-1,SQLITE_STATIC);
    error=sqlite3_bind_text(res,22,zosstext2,-1,SQLITE_STATIC);
    sqlite3_bind_int64(res,23,pwork->pinode);
    error=sqlite3_step(res);
    sqlite3_free(zname);
    sqlite3_free(ztype);
    sqlite3_free(zlinkname);
    sqlite3_free(zosstext1);
    sqlite3_free(zosstext2);
    sqlite3_reset(res);

    return 0;
}

int insertdbgo_xattrs_avail(struct entry_data *ed, sqlite3_stmt *res)
{
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

int insertdbgo_xattrs(struct input *in, struct stat *dir, struct entry_data *ed,
                      sll_t *xattr_db_list, struct template_db *xattr_template,
                      const char *topath, const size_t topath_len,
                      sqlite3_stmt *xattrs_res, sqlite3_stmt *xattr_files_res) {
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
                                           ed->statuso.st_uid,
                                           in->nobody.gid,
                                           ed->statuso.st_mode,
                                           xattr_files_res);
            if (!xattr_uid_db) {
                return -1;
            }
            sll_push(xattr_db_list, xattr_uid_db);
        }

        /* group id/permission mismatch */
        if (!xattr_gid_db) {
            xattr_gid_db = create_xattr_db(xattr_template,
                                           topath, topath_len,
                                           in,
                                           in->nobody.uid,
                                           ed->statuso.st_gid,
                                           ed->statuso.st_mode,
                                           xattr_files_res);
            if (!xattr_gid_db) {
                return -1;
            }
            sll_push(xattr_db_list, xattr_gid_db);
        }

        /* insert into per-user and per-group xattr dbs */
        insertdbgo_xattrs_avail(ed, xattr_uid_db->res);
        insertdbgo_xattrs_avail(ed, xattr_gid_db->res);
    }

    return 0;
}

int insertdbgor(struct work *pwork, struct entry_data *ed, sqlite3 *db, sqlite3_stmt *res)
{
    int error;
    char *zname;
    char *ztype;

    zname = sqlite3_mprintf("%q",pwork->name);
    ztype = sqlite3_mprintf("%q",ed->type);
    error=sqlite3_bind_text(res,1,zname,-1,SQLITE_TRANSIENT);
    if (error != SQLITE_OK) fprintf(stderr, "SQL insertdbgor bind name: %s error %d err %s\n",pwork->name,error,sqlite3_errmsg(db));
    sqlite3_bind_text(res,2,ztype,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int64(res,3,ed->statuso.st_ino);
    sqlite3_bind_int64(res,4,pwork->pinode);
    sqlite3_bind_int64(res,5,ed->suspect);

    error = sqlite3_step(res);
    sqlite3_free(zname);
    sqlite3_free(ztype);
    sqlite3_clear_bindings(res);
    sqlite3_reset(res);

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
    char *zlinkname = sqlite3_mprintf("%q", ed->linkname);

    char xattrnames[MAXXATTR] = "\x00";
    xattr_get_names(&ed->xattrs, xattrnames, sizeof(xattrnames), XATTRDELIM);

    char *zxattrnames = sqlite3_mprintf("%q", xattrnames);

    sqlite3_bind_text(res,   1,  zname, -1, SQLITE_STATIC);
    sqlite3_bind_text(res,   2,  ztype, -1, SQLITE_STATIC);
    sqlite3_bind_int64(res,  3,  ed->statuso.st_ino);
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
    sqlite3_bind_int64(res,  24, su->totltk);
    sqlite3_bind_int64(res,  25, su->totmtk);
    sqlite3_bind_int64(res,  26, su->totltm);
    sqlite3_bind_int64(res,  27, su->totmtm);
    sqlite3_bind_int64(res,  28, su->totmtg);
    sqlite3_bind_int64(res,  29, su->totmtt);
    sqlite3_bind_int64(res,  30, su->totsize);
    sqlite3_bind_int64(res,  31, su->minctime);
    sqlite3_bind_int64(res,  32, su->maxctime);
    sqlite3_bind_int64(res,  33, su->minmtime);
    sqlite3_bind_int64(res,  34, su->maxmtime);
    sqlite3_bind_int64(res,  35, su->minatime);
    sqlite3_bind_int64(res,  36, su->maxatime);
    sqlite3_bind_int64(res,  37, su->minblocks);
    sqlite3_bind_int64(res,  38, su->maxblocks);
    sqlite3_bind_int64(res,  39, su->totxattr);
    sqlite3_bind_int64(res,  40, 0); /* depth */
    sqlite3_bind_int64(res,  41, su->mincrtime);
    sqlite3_bind_int64(res,  42, su->maxcrtime);
    sqlite3_bind_int64(res,  43, su->minossint1);
    sqlite3_bind_int64(res,  44, su->maxossint1);
    sqlite3_bind_int64(res,  45, su->totossint1);
    sqlite3_bind_int64(res,  46, su->minossint2);
    sqlite3_bind_int64(res,  47, su->maxossint2);
    sqlite3_bind_int64(res,  48, su->totossint2);
    sqlite3_bind_int64(res,  49, su->minossint3);
    sqlite3_bind_int64(res,  50, su->maxossint3);
    sqlite3_bind_int64(res,  51, su->totossint3);
    sqlite3_bind_int64(res,  52, su->minossint4);
    sqlite3_bind_int64(res,  53, su->maxossint4);
    sqlite3_bind_int64(res,  54, su->totossint4);
    sqlite3_bind_int64(res,  55, 0); /* rectype */
    sqlite3_bind_int64(res,  56, pwork->pinode);
    sqlite3_bind_int64(res,  57, 1); /* isroot */
    sqlite3_bind_int64(res,  58, 0); /* rollupscore */

    sqlite3_step(res);
    sqlite3_clear_bindings(res);
    sqlite3_reset(res);

    sqlite3_free(zxattrnames);
    sqlite3_free(zlinkname);
    sqlite3_free(ztype);
    sqlite3_free(zname);

    return insertdbfin(res);
}

int inserttreesumdb(const char *name, sqlite3 *sdb, struct sum *su,int rectype,int uid,int gid)
{
    int depth = 0;
    for(const char *c = name; *c; c++) {
        depth += (*c == '/');
    }

    char sqlstmt[MAXSQL];
    SNPRINTF(sqlstmt, MAXSQL, "INSERT INTO treesummary VALUES (%lld, %lld, %lld, %lld,%lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %d, %d);",
       su->totsubdirs, su->maxsubdirfiles, su->maxsubdirlinks, su->maxsubdirsize,su->totfiles, su->totlinks, su->minuid, su->maxuid, su->mingid, su->maxgid, su->minsize, su->maxsize, su->totltk, su->totmtk, su->totltm, su->totmtm, su->totmtg, su->totmtt, su->totsize, su->minctime, su->maxctime, su->minmtime, su->maxmtime, su->minatime, su->maxatime, su->minblocks, su->maxblocks,su->totxattr,depth,su->mincrtime, su->maxcrtime, su->minossint1, su->maxossint1, su->totossint1, su->minossint2, su->maxossint2, su->totossint2, su->minossint3, su->maxossint3, su->totossint3, su->minossint4,su->maxossint4, su->totossint4, rectype, uid, gid);

    char *err_msg = NULL;
    if (sqlite3_exec(sdb, sqlstmt, 0, 0, &err_msg) != SQLITE_OK ) {
        fprintf(stderr, "SQL error on insert (treesummary): %s\n", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    return 0;
}

/* return the directory you are currently in */
static void path(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc; (void) argv;
    struct work *work = (struct work *) sqlite3_user_data(context);

    sqlite3_result_text(context, work->name, work->name_len, SQLITE_STATIC);
    return;
}

/* return the basename of the directory you are currently in */
static void epath(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc; (void) argv;
    struct work *work = (struct work *) sqlite3_user_data(context);

    sqlite3_result_text(context, work->name + work->name_len - work->basename_len,
                        work->basename_len, SQLITE_STATIC);
    return;
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

    return;
}

/*
 * Usage:
 *     SELECT rpath(summary.name, summary.rollupscore) || "/" || pentries.name
 *     FROM summary, pentries
 *     WHERE summary.inode == pentries.pinode;
 *
 * rpath = work->name - work->root (prefix)
 * // rolled up
 * if summary.rollupscore != 0
 *     path += summary.name - common path (prefix)
 * return path
 */
static void rpath(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc;

    /* work->name contains the current directory being operated on */
    struct work *work = (struct work *) sqlite3_user_data(context);
    const int rollupscore = sqlite3_value_int(argv[1]);

    refstr_t dirname;

    /* remove parent from current directory name */
    dirname.data = work->name + work->root_parent.len;
    dirname.len  = work->name_len - work->root_parent.len;
    while (dirname.len && (dirname.data[0] == '/')) {
        dirname.data++;
        dirname.len--;
    }

    if (rollupscore == 0) {
        sqlite3_result_text(context, dirname.data, dirname.len, SQLITE_STATIC);
    }
    else {
        char fullpath[MAXPATH];
        size_t fullpath_len = SNFORMAT_S(fullpath, MAXPATH, 1,
            dirname.data, dirname.len);

        refstr_t input;
        input.data = (char *) sqlite3_value_text(argv[0]);
        input.len  = strlen(input.data);
        size_t input_offset = 0;

        /* remove first path segment, including / */
        while ((input_offset < input.len) &&
               (input.data[input_offset] != '/')) {
            input_offset++;
        }

        while ((input_offset < input.len) &&
               (input.data[input_offset] == '/')) {
            input_offset++;
        }

        const size_t input_remaining = input.len - input_offset;
        if (input_remaining) {
            fullpath_len += SNFORMAT_S(fullpath + fullpath_len, MAXPATH, 2,
                "/", 1, input.data + input_offset, input_remaining);
        }

        sqlite3_result_text(context, fullpath, fullpath_len, SQLITE_TRANSIENT);
    }

    return;
}

static void uidtouser(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc;

    const char *text = (char *) sqlite3_value_text(argv[0]);

    const int fuid = atoi(text);
    struct passwd *fmypasswd = getpwuid(fuid);
    const char *show = fmypasswd?fmypasswd->pw_name:text;

    sqlite3_result_text(context, show, -1, SQLITE_TRANSIENT);

    return;
}

static void gidtogroup(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc;

    const char *text = (char *) sqlite3_value_text(argv[0]);

    const int fgid = atoi(text);
    struct group *fmygroup = getgrgid(fgid);
    const char *show = fmygroup?fmygroup->gr_name:text;

    sqlite3_result_text(context, show, -1, SQLITE_TRANSIENT);

    return;
}

static void modetotxt(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    int fmode;
    char tmode[64];
    fmode = sqlite3_value_int(argv[0]);
    modetostr(tmode, sizeof(tmode), fmode);
    sqlite3_result_text(context, tmode, -1, SQLITE_TRANSIENT);
    return;
}

/* uint64_t goes up to E */
static const char SIZE[] = {'K', 'M', 'G', 'T', 'P', 'E'};

/* Returns the number of blocks required to store a given size */
/* Unfilled blocks count as one full block (round up)          */
static void blocksize(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    const char *size_s = (const char *) sqlite3_value_text(argv[0]);
    const char *unit   = (const char *) sqlite3_value_text(argv[1]);
    uint64_t size = 0;
    if (sscanf(size_s, "%" PRIu64, &size) != 1) {
        sqlite3_result_error(context, "Bad blocksize size", -1);
        return;
    }

    const size_t len = strlen(unit);

    uint64_t unit_size = 1;
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

    char buf[MAXPATH];
    const size_t buf_len = snprintf(buf, sizeof(buf), "%" PRIu64 "%s",
                                    (size / unit_size) + (!!(size % unit_size)),
                                    unit);

    sqlite3_result_text(context, buf, buf_len, SQLITE_TRANSIENT);

    return;
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

    return;
}

static void relative_level(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc; (void) argv;

    size_t level = (size_t) (uintptr_t) sqlite3_user_data(context);
    sqlite3_result_int64(context, level);
    return;
}

static void starting_point(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc; (void) argv;

    sqlite3_result_text(context, sqlite3_user_data(context), -1, SQLITE_TRANSIENT);
    return;
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
    const size_t trimmed_len = trailing_match_index(path, path_len, "/", 1);
    if (!trimmed_len) {
        sqlite3_result_text(context, "/", 1, SQLITE_TRANSIENT);
        return;
    }

    /* basename(work->name) will be the same as the first part of the input path, so remove it */
    const size_t offset = trailing_non_match_index(path, trimmed_len, "/", 1);

    const size_t bn_len = trimmed_len - offset;
    char *bn = path + offset;

    sqlite3_result_text(context, bn, bn_len, SQLITE_TRANSIENT);

    return;
}

int addqueryfuncs_common(sqlite3 *db) {
    return !((sqlite3_create_function(db,  "uidtouser",           1,   SQLITE_UTF8,
                                      NULL,                       &uidtouser,           NULL, NULL) == SQLITE_OK) &&
             (sqlite3_create_function(db,  "gidtogroup",          1,   SQLITE_UTF8,
                                      NULL,                       &gidtogroup,          NULL, NULL) == SQLITE_OK) &&
             (sqlite3_create_function(db,  "modetotxt",           1,   SQLITE_UTF8,
                                      NULL,                       &modetotxt,           NULL, NULL) == SQLITE_OK) &&
             (sqlite3_create_function(db,  "blocksize",           2,   SQLITE_UTF8,
                                      NULL,                       &blocksize,           NULL, NULL) == SQLITE_OK) &&
             (sqlite3_create_function(db,  "human_readable_size", 1,   SQLITE_UTF8,
                                      NULL,                       &human_readable_size, NULL, NULL) == SQLITE_OK) &&
             (sqlite3_create_function(db,  "basename",            1,   SQLITE_UTF8,
                                      NULL,                       &sqlite_basename,     NULL, NULL) == SQLITE_OK));
}

int addqueryfuncs_with_context(sqlite3 *db, struct work *work) {
    /* only available if work is valid */
    if (work) {
        void *lvl = (void *) (uintptr_t) work->level;
        if (!((sqlite3_create_function(db,  "path",                     0, SQLITE_UTF8,
                                       work,                            &path,               NULL, NULL) == SQLITE_OK) &&
              (sqlite3_create_function(db,  "epath",                    0, SQLITE_UTF8,
                                       work,                            &epath,              NULL, NULL) == SQLITE_OK) &&
              (sqlite3_create_function(db,  "fpath",                    0, SQLITE_UTF8,
                                       work,                            &fpath,              NULL, NULL) == SQLITE_OK) &&
              (sqlite3_create_function(db,  "rpath",                    2, SQLITE_UTF8,
                                       work,                            &rpath,              NULL, NULL) == SQLITE_OK) &&
              (sqlite3_create_function(db,  "starting_point",           0,  SQLITE_UTF8,
                                       (void *) work->root_parent.data, &starting_point,     NULL, NULL) == SQLITE_OK) &&
              (sqlite3_create_function(db,  "level",                    0,  SQLITE_UTF8,
                                       lvl,                             &relative_level,     NULL, NULL) == SQLITE_OK))) {
            return 1;
        }
    }

    return 0;
}

int addqueryfuncs(sqlite3 *db, size_t id, struct work *work) {
    (void) id;
    return !((addqueryfuncs_common(db) == 0) &&
             (addqueryfuncs_with_context(db, work) == 0));
}

struct xattr_db *create_xattr_db(struct template_db *tdb,
                                 const char *path, const size_t path_len,
                                 struct input *in,
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
        xdb->attach_len   = SNPRINTF(xdb->attach, MAXPATH,
                                     XATTR_UID_ATTACH_FORMAT, uid);
        xattr_db_mode = 0600;
    }
    else if (gid != in->nobody.gid) {
        /* g+r */
        if ((mode & 0040) == 0040) {
            xdb->filename_len = SNPRINTF(xdb->filename, MAXPATH,
                                         XATTR_GID_W_READ_FILENAME_FORMAT, gid);
            xdb->attach_len   = SNPRINTF(xdb->attach, MAXPATH,
                                         XATTR_GID_W_READ_ATTACH_FORMAT, gid);
            xattr_db_mode = 0040;
        }
        /* g-r */
        else {
            xdb->filename_len = SNPRINTF(xdb->filename, MAXPATH,
                                         XATTR_GID_WO_READ_FILENAME_FORMAT, gid);
            xdb->attach_len   = SNPRINTF(xdb->attach, MAXPATH,
                                         XATTR_GID_WO_READ_ATTACH_FORMAT, gid);
            xattr_db_mode = 0000;
        }
    }

    /* store full path here */
    char filename[MAXPATH];
    SNFORMAT_S(filename, MAXPATH, 3,
               path, path_len,
               "/", (size_t) 1,
               xdb->filename, xdb->filename_len);

    xdb->db  = NULL;
    xdb->res = NULL;
    xdb->file_list = file_list;
    xdb->st.st_uid = uid;
    xdb->st.st_gid = gid;
    xdb->st.st_mode = xattr_db_mode;

    if (copy_template(tdb, filename, xdb->st.st_uid, xdb->st.st_gid) != 0) {
        destroy_xattr_db(xdb);
        return NULL;
    }

    if (chmod(filename, xdb->st.st_mode) != 0) {
        const int err = errno;
        fprintf(stderr, "Warning: Unable to set permissions for %s: %d\n", filename, err);
    }

    xdb->db = opendb(filename, SQLITE_OPEN_READWRITE, 0, 0
                     , NULL, NULL
                     #if defined(DEBUG) && defined(PER_THREAD_STATS)
                     , NULL, NULL
                     , NULL, NULL
                     #endif
                     );

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
    sqlite3_bind_text( xdb->file_list, 1, xdb->filename, xdb->filename_len, SQLITE_STATIC);
    sqlite3_bind_text( xdb->file_list, 2, xdb->attach, xdb->attach_len, SQLITE_STATIC);
    sqlite3_bind_int64(xdb->file_list, 3, xdb->st.st_mode);
    sqlite3_bind_int64(xdb->file_list, 4, xdb->st.st_uid);
    sqlite3_bind_int64(xdb->file_list, 5, xdb->st.st_gid);

    sqlite3_step(xdb->file_list);
    sqlite3_reset(xdb->file_list);

    free(xdb);
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

    int *rollupscore = (int *) args;
    *rollupscore = atoi(data[0]);
    return 0;
}

int get_rollupscore(sqlite3 *db, int *rollupscore) {
    char *err = NULL;
    if (sqlite3_exec(db, "SELECT rollupscore FROM summary WHERE isroot == 1",
                     get_rollupscore_callback, rollupscore, &err) != SQLITE_OK) {
        sqlite3_free(err);
        return -1;
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
            "INSERT INTO " TREESUMMARY " SELECT COUNT(*) - 1, MAX(totfiles), MAX(totlinks), MAX(size), TOTAL(totfiles), TOTAL(totlinks), MIN(minuid), MAX(maxuid), MIN(mingid), MAX(maxgid), MIN(minsize), MAX(maxsize), TOTAL(totltk), TOTAL(totmtk), TOTAL(totltm), TOTAL(totmtm), TOTAL(totmtg), TOTAL(totmtt), TOTAL(totsize), MIN(minctime), MAX(maxctime), MIN(minmtime), MAX(maxmtime), MIN(minatime), MAX(maxatime), MIN(minblocks), MAX(maxblocks), TOTAL(totxattr), TOTAL(depth), MIN(mincrtime), MAX(maxcrtime), MIN(minossint1), MAX(maxossint1), TOTAL(totossint1), MIN(minossint2), MAX(maxossint2), TOTAL(totossint2), MIN(minossint3), MAX(maxossint3), TOTAL(totossint3), MIN(minossint4), MAX(maxossint4), TOTAL(totossint4), rectype, uid, gid FROM " SUMMARY ";";

        char *err = NULL;
        if (sqlite3_exec(db, TREESUMMARY_ROLLUP_COMPUTE_INSERT, NULL, NULL, &err) != SQLITE_OK) {
            fprintf(stderr, "Error: Failed to compute treesummary for \"%s\": %s\n",
                    dirname, err);
            sqlite3_free(err);
            return 1;
        }
    }
    else {
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

            sqlite3 *child_db = opendb(child_dbname, SQLITE_OPEN_READONLY, 1, 0, NULL, NULL
                                       #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                       , NULL, NULL
                                       , NULL, NULL
                                       #endif
                );
            if (!child_db) {
                 continue;
            }

            char *err = NULL;
            int trecs = 0;
            if (sqlite3_exec(child_db, TREESUMMARY_EXISTS,
                             treesummary_exists_callback,
                             &trecs, &err) == SQLITE_OK) {
                zeroit(&sum);
                if (trecs < 1) {
                    /* add summary data from this child */
                    querytsdb(dirname, &sum, child_db, 0);
                } else {
                    /* add treesummary data from this child */
                    querytsdb(dirname, &sum, child_db, 1);
                }

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

        inserttreesumdb(dirname, db, &tsum, 0, 0, 0);
    }

    return 0;
}
