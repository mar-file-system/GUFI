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



#include "pcre.h"
#include <errno.h>
#include <grp.h>
#include <libgen.h>
#include <pwd.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "config.h"
#include "dbutils.h"
#include "template_db.h"

extern int errno;

static const char GUFI_SQLITE_VFS[] = "unix-none";

#define DROP_TABLE(name) "DROP TABLE IF EXISTS " #name ";"
#define DROP_VIEW(name)  "DROP VIEW  IF EXISTS " #name ";"

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

const char PENTRIES_ROLLUP_CREATE[] =
    DROP_TABLE(PENTRIES_ROLLUP)
    "CREATE TABLE " PENTRIES_ROLLUP "(name TEXT, type TEXT, inode INT64, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattr_names BLOB, crtime INT64, ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, osstext1 TEXT, osstext2 TEXT, pinode INT64);";

const char PENTRIES_ROLLUP_INSERT[] =
    "INSERT INTO " PENTRIES_ROLLUP " VALUES (@name, @type, @inode, @mode, @nlink, @uid, @gid, @size, @blksize, @blocks, @atime, @mtime, @ctime, @linkname, @xattr_names, @crtime, @ossint1, @ossint2, @ossint3, @ossint4, @osstext1, @osstext2, pinode INT64);";

const char PENTRIES_CREATE[] =
    DROP_VIEW(PENTRIES)
    "CREATE VIEW " PENTRIES " AS SELECT " ENTRIES ".*, " SUMMARY ".inode AS pinode FROM " ENTRIES ", " SUMMARY " WHERE isroot == 1 UNION SELECT * FROM " PENTRIES_ROLLUP ";";

const char tsql[] =
    DROP_TABLE(TREESUMMARY)
    "CREATE TABLE " TREESUMMARY "(totsubdirs INT64, maxsubdirfiles INT64, maxsubdirlinks INT64, maxsubdirsize INT64, totfiles INT64, totlinks INT64, minuid INT64, maxuid INT64, mingid INT64, maxgid INT64, minsize INT64, maxsize INT64, totltk INT64, totmtk INT64, totltm INT64, totmtm INT64, totmtg INT64, totmtt INT64, totsize INT64, minctime INT64, maxctime INT64, minmtime INT64, maxmtime INT64, minatime INT64, maxatime INT64, minblocks INT64, maxblocks INT64, totxattr INT64, depth INT64, mincrtime INT64, maxcrtime INT64, minossint1 INT64, maxossint1 INT64, totossint1 INT64, minossint2 INT64, maxossint2 INT64, totossint2 INT64, minossint3 INT64, maxossint3 INT64, totossint3 INT64, minossint4 INT64, maxossint4 INT64, totossint4 INT64, rectype INT64, uid INT64, gid INT64);";

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
  char attach[MAXSQL];
  if (flags & SQLITE_OPEN_READONLY) {
      if (!sqlite3_snprintf(MAXSQL, attach, "ATTACH 'file:%q?mode=ro' AS %Q", name, dbn)) {
          if (print_err) {
              fprintf(stderr, "Cannot create ATTACH command\n");
          }
          return NULL;
      }
  }
  else if (flags & SQLITE_OPEN_READWRITE) {
      if (!sqlite3_snprintf(MAXSQL, attach, "ATTACH %Q AS %Q", name, dbn)) {
          if (print_err) {
              fprintf(stderr, "Cannot create ATTACH command\n");
          }
          return NULL;
      }
  }

  if (sqlite3_exec(db, attach, NULL, NULL, NULL) != SQLITE_OK) {
      if (print_err) {
          fprintf(stderr, "Cannot attach database as \"%s\": %s\n", dbn, sqlite3_errmsg(db));
      }
      return NULL;
  }

  return db;
}

sqlite3 *detachdb(const char *name, sqlite3 *db, const char *dbn, const int print_err)
{
  char detach[MAXSQL];
  if (!sqlite3_snprintf(MAXSQL, detach, "DETACH %Q", dbn)) {
      if (print_err) {
          fprintf(stderr, "Cannot create DETACH command\n");
      }
      return NULL;
  }

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

int rawquerydb(const char *name,
               int         isdir,        // (unused)
               sqlite3    *db,
               char       *sqlstmt,
               int         printpath,    // (unused)
               int         printheader,
               int         printrows,
               int         ptid)         // pthread-ID
{
     sqlite3_stmt *res;
     int           error     = 0;
     int           rec_count = 0;
     const char   *tail;
     //char   lsqlstmt[MAXSQL];
     //char   prefix[MAXPATH];
     //char   shortname[MAXPATH];
     //char  *pp;

     FILE *        out;
     char          ffielddelim[2];

     if (!sqlstmt) {
        fprintf(stderr, "SQL was empty\n");
        return -1;
     }

     out = stdout;
     if (in.output == OUTFILE)
        out = gts.outfd[ptid];

     if (in.dodelim == 0)
       SNPRINTF(ffielddelim,2,"|");

     if (in.dodelim == 1)
       SNPRINTF(ffielddelim,2,"%s",fielddelim);

     if (in.dodelim == 2)
       SNPRINTF(ffielddelim,2,"%s",in.delim);

     while (*sqlstmt) {
       // WARNING: passing length-arg that is longer than SQL text
       error = sqlite3_prepare_v2(db, sqlstmt, MAXSQL, &res, &tail);
       if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on query: %s name %s err %s\n",
                  sqlstmt,name,sqlite3_errmsg(db));
          return -1;
       }

       //printf("running on %s query %s printpath %d tail %s\n",name,sqlstmt,printpath,tail);
       if (*tail) {
         sqlite3_step(res);
         //sqlite3_finalize(res);
         sqlite3_reset(res);
       }
       sqlstmt = (char*)tail;
     }

     // NOTE: if <sqlstmt> actually contained multiple statments, then this
     //       loop only runs with the final statement.

     if (sqlite3_step(res) == SQLITE_ROW) {
         const int ncols = sqlite3_column_count(res);

         // maybe print column-names as a header (once)
         if (printheader) {
             for(int cnt = 0; cnt < ncols; cnt++) {
                 fprintf(out,"%s%s", sqlite3_column_name(res,cnt),in.delim);
             }
             fprintf(out,"\n");
         }

         do {
             // maybe print result-row values
             if (printrows) {
                 for(int cnt = 0; cnt < ncols; cnt++) {
                     fprintf(out,"%s%s", sqlite3_column_text(res,cnt),in.delim);
                 }
                 fprintf(out,"\n");
             }

             // count of rows in query-result
             rec_count++;
        } while (sqlite3_step(res) == SQLITE_ROW);
     }

    //printf("We received %d records.\n", rec_count);
    // sqlite3_reset(res);
    sqlite3_finalize(res);
    return(rec_count);
}

int querytsdb(const char *name, struct sum *sumin, sqlite3 *db, int *recs, int ts)
{
     static const char *ts_str[] = {
         "select totfiles,totlinks,minuid,maxuid,mingid,maxgid,minsize,maxsize,totltk,totmtk,totltm,totmtm,totmtg,totmtt,totsize,minctime,maxctime,minmtime,maxmtime,minatime,maxatime,minblocks,maxblocks,totxattr,mincrtime,maxcrtime,minossint1,maxossint1,totossint1,minossint2,maxossint2,totossint2,minossint3,maxossint3,totossint3,minossint4,maxossint4,totossint4 "
         "from summary where rectype=0;",
         "select totfiles,totlinks,minuid,maxuid,mingid,maxgid,minsize,maxsize,totltk,totmtk,totltm,totmtm,totmtg,totmtt,totsize,minctime,maxctime,minmtime,maxmtime,minatime,maxatime,minblocks,maxblocks,totxattr,mincrtime,maxcrtime,minossint1,maxossint1,totossint1,minossint2,maxossint2,totossint2,minossint3,maxossint3,totossint3,minossint4,maxossint4,totossint4,totsubdirs,maxsubdirfiles,maxsubdirlinks,maxsubdirsize "
         "from treesummary where rectype=0;"
     };

     const char *sqlstmt = ts_str[ts];
     sqlite3_stmt *res = NULL;
     if (sqlite3_prepare_v2(db, sqlstmt, MAXSQL, &res, NULL) != SQLITE_OK) {
          fprintf(stderr, "SQL error on query: %s, name: %s, err: %s\n",
                  sqlstmt,name,sqlite3_errmsg(db));
          return -1;
     }

     sqlite3_step(res);

     //sumin->totfiles = atoll((const char *)sqlite3_column_text(res, 0));
     sumin->totfiles   = sqlite3_column_int64(res, 0);
     sumin->totlinks   = sqlite3_column_int64(res, 1);
     sumin->minuid     = sqlite3_column_int64(res, 2);
     sumin->maxuid     = sqlite3_column_int64(res, 3);
     sumin->mingid     = sqlite3_column_int64(res, 4);
     sumin->maxgid     = sqlite3_column_int64(res, 5);
     sumin->minsize    = sqlite3_column_int64(res, 6);
     sumin->maxsize    = sqlite3_column_int64(res, 7);
     sumin->totltk     = sqlite3_column_int64(res, 8);
     sumin->totmtk     = sqlite3_column_int64(res, 9);
     sumin->totltm     = sqlite3_column_int64(res, 10);
     sumin->totmtm     = sqlite3_column_int64(res, 11);
     sumin->totmtg     = sqlite3_column_int64(res, 12);
     sumin->totmtt     = sqlite3_column_int64(res, 13);
     sumin->totsize    = sqlite3_column_int64(res, 14);
     sumin->minctime   = sqlite3_column_int64(res, 15);
     sumin->maxctime   = sqlite3_column_int64(res, 16);
     sumin->minmtime   = sqlite3_column_int64(res, 17);
     sumin->maxmtime   = sqlite3_column_int64(res, 18);
     sumin->minatime   = sqlite3_column_int64(res, 19);
     sumin->maxatime   = sqlite3_column_int64(res, 20);
     sumin->minblocks  = sqlite3_column_int64(res, 21);
     sumin->maxblocks  = sqlite3_column_int64(res, 22);
     sumin->totxattr   = sqlite3_column_int64(res, 23);

     sumin->mincrtime  = sqlite3_column_int64(res, 24);
     sumin->maxcrtime  = sqlite3_column_int64(res, 25);
     sumin->minossint1 = sqlite3_column_int64(res, 26);
     sumin->maxossint1 = sqlite3_column_int64(res, 27);
     sumin->totossint1 = sqlite3_column_int64(res, 28);
     sumin->minossint2 = sqlite3_column_int64(res, 29);
     sumin->maxossint2 = sqlite3_column_int64(res, 30);
     sumin->totossint2 = sqlite3_column_int64(res, 31);
     sumin->minossint3 = sqlite3_column_int64(res, 32);
     sumin->maxossint3 = sqlite3_column_int64(res, 33);
     sumin->totossint3 = sqlite3_column_int64(res, 34);
     sumin->minossint4 = sqlite3_column_int64(res, 35);
     sumin->maxossint4 = sqlite3_column_int64(res, 36);
     sumin->totossint4 = sqlite3_column_int64(res, 37);

     if (ts) {
       sumin->totsubdirs     = sqlite3_column_int64(res,38);
       sumin->maxsubdirfiles = sqlite3_column_int64(res,39 );
       sumin->maxsubdirlinks = sqlite3_column_int64(res,40 );
       sumin->maxsubdirsize  = sqlite3_column_int64(res,41 );
     }
     else {
       sumin->totsubdirs     = 0;
       sumin->maxsubdirfiles = 0;
       sumin->maxsubdirlinks = 0;
       sumin->maxsubdirsize  = 0;
     }

     //printf("tsdb: totfiles %d totlinks %d minuid %d\n",
     //       sumin->totfiles,sumin->totlinks,sumin->minuid);
     sqlite3_finalize(res);

     return 0;
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
    sqlite3_stmt *reso;

    // WARNING: passing length-arg that is longer than SQL text
    error = sqlite3_prepare_v2(db, sqli, MAXSQL, &reso, &tail);
    if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on insertdbprep: error %d %s err %s\n",
                  error, sqli, sqlite3_errmsg(db));
          return NULL;
    }
    return reso;
}

int insertdbgo(struct work *pwork, sqlite3 *db, sqlite3_stmt *res)
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
    ztype = sqlite3_mprintf("%q",pwork->type);
    zlinkname = sqlite3_mprintf("%q",pwork->linkname);
    zosstext1 = sqlite3_mprintf("%q",pwork->osstext1);
    zosstext2 = sqlite3_mprintf("%q",pwork->osstext2);
    error=sqlite3_bind_text(res,1,zname,-1,SQLITE_STATIC);
    if (error != SQLITE_OK) fprintf(stderr, "SQL insertdbgo bind name: %s error %d err %s\n",pwork->name,error,sqlite3_errmsg(db));
    sqlite3_bind_text(res,2,ztype,-1,SQLITE_STATIC);
    sqlite3_bind_int64(res,3,pwork->statuso.st_ino);
    sqlite3_bind_int64(res,4,pwork->statuso.st_mode);
    sqlite3_bind_int64(res,5,pwork->statuso.st_nlink);
    sqlite3_bind_int64(res,6,pwork->statuso.st_uid);
    sqlite3_bind_int64(res,7,pwork->statuso.st_gid);
    sqlite3_bind_int64(res,8,pwork->statuso.st_size);
    sqlite3_bind_int64(res,9,pwork->statuso.st_blksize);
    sqlite3_bind_int64(res,10,pwork->statuso.st_blocks);
    sqlite3_bind_int64(res,11,pwork->statuso.st_atime);
    sqlite3_bind_int64(res,12,pwork->statuso.st_mtime);
    sqlite3_bind_int64(res,13,pwork->statuso.st_ctime);
    sqlite3_bind_text(res,14,zlinkname,-1,SQLITE_STATIC);

    /* only insert xattr names */
    char xattr_names[MAXXATTR];
    ssize_t xattr_names_len = xattr_get_names(&pwork->xattrs, xattr_names, sizeof(xattr_names), XATTRDELIM);
    if (xattr_names_len < 0) {
         xattr_names_len = 0;
    }
    sqlite3_bind_blob64(res, 15, xattr_names, xattr_names_len, SQLITE_STATIC);

    sqlite3_bind_int64(res,16,pwork->crtime);
    sqlite3_bind_int64(res,17,pwork->ossint1);
    sqlite3_bind_int64(res,18,pwork->ossint2);
    sqlite3_bind_int64(res,19,pwork->ossint3);
    sqlite3_bind_int64(res,20,pwork->ossint4);
    error=sqlite3_bind_text(res,21,zosstext1,-1,SQLITE_STATIC);
    error=sqlite3_bind_text(res,22,zosstext2,-1,SQLITE_STATIC);
    sqlite3_bind_int64(res,23,pwork->pinode);
    error=sqlite3_step(res);
    sqlite3_free(zname);
    sqlite3_free(ztype);
    sqlite3_free(zlinkname);
    sqlite3_free(zosstext1);
    sqlite3_free(zosstext2);
    /* if (error != SQLITE_ROW)  { */
          /* fprintf(stderr, "SQL error on insertdbgo: error %d err %s\n",error,sqlite3_errmsg(db)); */
          //return 0;
    /* } */
    /* sqlite3_clear_bindings(res); */
    sqlite3_reset(res);

    return 0;
}

int insertdbgo_xattrs_avail(struct work *pwork, sqlite3_stmt *res)
{
    int error = SQLITE_DONE;
    for(size_t i = 0; (i < pwork->xattrs.count) && (error == SQLITE_DONE); i++) {
        struct xattr *xattr = &pwork->xattrs.pairs[i];

        sqlite3_bind_int64(res, 1, pwork->statuso.st_ino);
        sqlite3_bind_blob(res,  2, xattr->name,  xattr->name_len,  SQLITE_STATIC);
        sqlite3_bind_blob(res,  3, xattr->value, xattr->value_len, SQLITE_STATIC);

        error = sqlite3_step(res);
        sqlite3_reset(res);
    }

    return error;
}

int insertdbgo_xattrs(struct stat *dir, struct work *entry,
                      struct sll *xattr_db_list,
                      struct template_db *xattr_template,
                      const char *topath, const size_t topath_len,
                      sqlite3_stmt *xattrs_res,
                      sqlite3_stmt *xattr_files_res) {
    /* insert into the xattrs_avail table inside db.db */
    const int rollin_score = xattr_can_rollin(dir, &entry->statuso);
    if (rollin_score) {
        insertdbgo_xattrs_avail(entry, xattrs_res);
    }
    /* can't roll in, so need external dbs */
    else {
        struct xattr_db *xattr_uid_db = NULL;
        struct xattr_db *xattr_gid_db = NULL;

        /* linear search since there shouldn't be too many users/groups that have xattrs */
        sll_loop(xattr_db_list, node) {
            struct xattr_db *xattr_db = (struct xattr_db *) sll_node_data(node);
            if (xattr_db->st.st_uid == entry->statuso.st_uid) {
                xattr_uid_db = xattr_db;

                if (xattr_gid_db) {
                    break;
                }
            }

            if ((xattr_db->st.st_gid == entry->statuso.st_gid) &&
                ((xattr_db->st.st_mode & 0040) == (entry->statuso.st_mode & 0040))) {
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
                                           entry->statuso.st_uid,
                                           in.xattrs.nobody.gid,
                                           entry->statuso.st_mode,
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
                                           in.xattrs.nobody.uid,
                                           entry->statuso.st_gid,
                                           entry->statuso.st_mode,
                                           xattr_files_res);
            if (!xattr_gid_db) {
                return -1;
            }
            sll_push(xattr_db_list, xattr_gid_db);
        }

        /* insert into per-user and per-group xattr dbs */
        insertdbgo_xattrs_avail(entry, xattr_uid_db->res);
        insertdbgo_xattrs_avail(entry, xattr_gid_db->res);
    }

    return 0;
}

int insertdbgor(struct work *pwork, sqlite3 *db, sqlite3_stmt *res)
{
    int error;
    char *zname;
    char *ztype;

    zname = sqlite3_mprintf("%q",pwork->name);
    ztype = sqlite3_mprintf("%q",pwork->type);
    error=sqlite3_bind_text(res,1,zname,-1,SQLITE_TRANSIENT);
    if (error != SQLITE_OK) fprintf(stderr, "SQL insertdbgor bind name: %s error %d err %s\n",pwork->name,error,sqlite3_errmsg(db));
    sqlite3_bind_text(res,2,ztype,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int64(res,3,pwork->statuso.st_ino);
    sqlite3_bind_int64(res,4,pwork->pinode);
    sqlite3_bind_int64(res,5,pwork->suspect);

    error = sqlite3_step(res);
    sqlite3_free(zname);
    sqlite3_free(ztype);
    if (error != SQLITE_ROW) {
          //fprintf(stderr, "SQL error on insertdbgor: error %d err %s\n",error,sqlite3_errmsg(db));
          //return 0;
    }
    sqlite3_clear_bindings(res);
    sqlite3_reset(res);

    return 0;
}

int insertsumdb(sqlite3 *sdb, struct work *pwork, struct sum *su)
{
    sqlite3_stmt *res = insertdbprep(sdb, SUMMARY_INSERT);
    if (!res) {
        return 1;
    }

    /* get the basename */
    char nameout[MAXPATH];
    char shortname[MAXPATH];
    shortpath(pwork->name, nameout, shortname);

    char *zname     = sqlite3_mprintf("%q", shortname);
    char *ztype     = sqlite3_mprintf("%q", pwork->type);
    char *zlinkname = sqlite3_mprintf("%q", pwork->linkname);

    char xattrnames[MAXXATTR] = "\x00";
    xattr_get_names(&pwork->xattrs, xattrnames, sizeof(xattrnames), XATTRDELIM);

    char *zxattrnames = sqlite3_mprintf("%q", xattrnames);

    sqlite3_bind_text(res,   1, zname, -1, SQLITE_STATIC);
    sqlite3_bind_text(res,   2, ztype, -1, SQLITE_STATIC);
    sqlite3_bind_int64(res,  3, pwork->statuso.st_ino);
    sqlite3_bind_int64(res,  4, pwork->statuso.st_mode);
    sqlite3_bind_int64(res,  5, pwork->statuso.st_nlink);
    sqlite3_bind_int64(res,  6, pwork->statuso.st_uid);
    sqlite3_bind_int64(res,  7, pwork->statuso.st_gid);
    sqlite3_bind_int64(res,  8, pwork->statuso.st_size);
    sqlite3_bind_int64(res,  9, pwork->statuso.st_blksize);
    sqlite3_bind_int64(res,  10, pwork->statuso.st_blocks);
    sqlite3_bind_int64(res,  11, pwork->statuso.st_atime);
    sqlite3_bind_int64(res,  12, pwork->statuso.st_mtime);
    sqlite3_bind_int64(res,  13, pwork->statuso.st_ctime);
    sqlite3_bind_text(res,   14, zlinkname, -1, SQLITE_STATIC);
    sqlite3_bind_blob64(res, 15, zxattrnames, -1, SQLITE_STATIC);
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
    char *err_msg = 0;
    char sqlstmt[MAXSQL];
    int rc;
    int depth;
    size_t i;

    depth=0;
    i=0;
    while (i < strlen(name)) {
      if (!strncmp(name+i,"/",1)) depth++;
      i++;
    }
    SNPRINTF(sqlstmt,MAXSQL,"INSERT INTO treesummary VALUES (%lld, %lld, %lld, %lld,%lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %d, %d);",
       su->totsubdirs, su->maxsubdirfiles, su->maxsubdirlinks, su->maxsubdirsize,su->totfiles, su->totlinks, su->minuid, su->maxuid, su->mingid, su->maxgid, su->minsize, su->maxsize, su->totltk, su->totmtk, su->totltm, su->totmtm, su->totmtg, su->totmtt, su->totsize, su->minctime, su->maxctime, su->minmtime, su->maxmtime, su->minatime, su->maxatime, su->minblocks, su->maxblocks,su->totxattr,depth,su->mincrtime, su->maxcrtime, su->minossint1, su->maxossint1, su->totossint1, su->minossint2, su->maxossint2, su->totossint2, su->minossint3, su->maxossint3, su->totossint3, su->minossint4,su->maxossint4, su->totossint4, rectype, uid, gid);

    rc = sqlite3_exec(sdb, sqlstmt, 0, 0, &err_msg);
    if (rc != SQLITE_OK ) {
        fprintf(stderr, "SQL error on insert (treesummary): %s\n", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    return 0;
}

/*
 * path(name (optional), remove root (optional))
 *
 * 0 args - get current directory: work->name (real path of index)
 * 1 arg  - dirname(work->name) + "/" + input arg
 * 2 args - int: if set to non-zero, remove work->root
 */
static void path(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    /* work->name contains the current directory being operated on */
    struct work *work = (struct work *) sqlite3_user_data(context);

    /* no args - return current directory */
    if (argc == 0) {
        sqlite3_result_text(context, work->name, -1, SQLITE_STATIC);
    }
    /* use first arg - combine with pwd to get full path */
    else {
        const char *name = (char *) sqlite3_value_text(argv[0]);
        int remove_root = 0;
        if (argc > 1) {
            remove_root = sqlite3_value_int(argv[1]);
        }

        char *path = work->name;
        size_t path_len = work->name_len;

        if (remove_root) {
            path += work->root_len;
            path_len -= work->root_len;
        }

        /* remove trailing '/' */
        while (path_len && (path[path_len - 1] == '/')) {
            path_len--;
        }

        /* basename(work->name) will be the same as the first part of the input path, so remove it */
        while (path_len && (path[path_len - 1] != '/')) {
            path_len--;
        }

        char fullpath[MAXPATH];

        /* do not strip trailing '/' or explicitly add an '/' */
        const size_t len = SNFORMAT_S(fullpath, MAXPATH, 2,
                                      path, path_len,
                                      name, strlen(name));

        sqlite3_result_text(context, fullpath, len, SQLITE_STATIC);
    }

    return;
}

static void fpath(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    const size_t id = (size_t) (uintptr_t) sqlite3_user_data(context);
    sqlite3_result_text(context, gps[id].gfpath, -1, SQLITE_TRANSIENT);
    return;
}

static void epath(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    const size_t id = (size_t) (uintptr_t) sqlite3_user_data(context);
    sqlite3_result_text(context, gps[id].gepath, -1, SQLITE_TRANSIENT);
    return;
}

static void uidtouser(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    const char *text = (char *) sqlite3_value_text(argv[0]);
    const size_t width = sqlite3_value_int64(argv[1]);

    static const char FORMAT[] = "%%%zus";
    char format[256];
    SNPRINTF(format, 256, FORMAT, width);

    const int fuid = atoi(text);
    struct passwd *fmypasswd = getpwuid(fuid);
    const char *show = fmypasswd?fmypasswd->pw_name:text;

    char fname[256];
    SNPRINTF(fname, 256, format, text);
    sqlite3_result_text(context, show, -1, SQLITE_TRANSIENT);

    return;
}

static void gidtogroup(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    const char *text = (char *) sqlite3_value_text(argv[0]);
    const size_t width = sqlite3_value_int64(argv[1]);

    static const char FORMAT[] = "%%%zus";
    char format[256];
    SNPRINTF(format, 256, FORMAT, width);

    const int fgid = atoi(text);
    struct group *fmygroup = getgrgid(fgid);
    const char *show = fmygroup?fmygroup->gr_name:text;

    char fgroup[256];
    SNPRINTF(fgroup, 256, format, show);
    sqlite3_result_text(context, fgroup, -1, SQLITE_TRANSIENT);

    return;
}

static void modetotxt(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    int fmode;
    char tmode[64];
    if (argc == 1) {
        fmode = sqlite3_value_int(argv[0]);
        modetostr(tmode, sizeof(tmode), fmode);
        sqlite3_result_text(context, tmode, -1, SQLITE_TRANSIENT);
        return;
    }
    sqlite3_result_null(context);
}

static void sqlite3_strftime(sqlite3_context *context, int argc, sqlite3_value **argv) {
    const char *fmt = (char *) sqlite3_value_text(argv[0]); /* format    */
    const time_t t = sqlite3_value_int64(argv[1]);          /* timestamp */

    char buf[MAXPATH];
    strftime(buf, sizeof(buf), fmt, localtime(&t));
    sqlite3_result_text(context, buf, -1, SQLITE_TRANSIENT);

    return;
}

static const char SIZE[] = {'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'};

/* Returns the number of blocks required to store a given size */
/* Unfilled blocks count as one full block (round up)          */
static void blocksize(sqlite3_context *context, int argc, sqlite3_value **argv) {
    const size_t size  = sqlite3_value_int64(argv[0]);
    const char *unit  = (char *) sqlite3_value_text(argv[1]);
    const int    align = sqlite3_value_int(argv[2]);

    const size_t len = strlen(unit);

    size_t unit_size = 1;
    if (len) {
        if ((len > 1) && (unit[len - 1] != 'B')) {
            sqlite3_result_error(context, "Bad blocksize unit", 0);
            return;
        }

        size_t multiplier = 1024;
        if (len == 2) {
            multiplier = 1000;
        }

        if (len == 3) {
            if (unit[1] != 'i') {
                sqlite3_result_error(context, "Bad blocksize unit", 0);
                return;
            }
        }

        for(size_t i = 0; i < sizeof(SIZE); i++) {
            unit_size *= multiplier;
            if (unit[0] == SIZE[i]) {
                break;
            }
        }
    }

    char fmt[MAXPATH];
    SNPRINTF(fmt, sizeof(fmt), "%%%dzu%s", align, unit);

    char buf[MAXPATH];
    snprintf(buf, sizeof(buf), fmt, size / unit_size + (!!(size % unit_size)));

    sqlite3_result_text(context, buf, -1, SQLITE_TRANSIENT);

    return;
}

/* Returns a string containg the size with as large of a unit as reasonable */
static void human_readable_size(sqlite3_context *context, int argc, sqlite3_value **argv) {
    const int align = sqlite3_value_int(argv[1]);
    char format[MAXPATH];
    char buf[MAXPATH];

    double size = sqlite3_value_double(argv[0]);
    if (size) {
        size_t unit_index = 0;
        while (size >= 1024) {
            size /= 1024;
            unit_index++;
        }

        if (unit_index == 0) {
            snprintf(format, sizeof(format), "%%%d.1f", align);
            snprintf(buf, sizeof(buf), format, size);
        }
        else {
            snprintf(format, sizeof(format), "%%%d.1f%%c", align);
            snprintf(buf, sizeof(buf), format, size, SIZE[unit_index - 1]);
        }
    }
    else {
        snprintf(format, sizeof(format), "%%%dd", align);
        snprintf(buf, sizeof(buf), format, 0);
    }

    sqlite3_result_text(context, buf, -1, SQLITE_TRANSIENT);

    return;
}

static void relative_level(sqlite3_context *context, int argc, sqlite3_value **argv) {
    size_t level = (size_t) (uintptr_t) sqlite3_user_data(context);
    sqlite3_result_int64(context, level);
    return;
}

static void starting_point(sqlite3_context *context, int argc, sqlite3_value **argv) {
    sqlite3_result_text(context, sqlite3_user_data(context), -1, SQLITE_TRANSIENT);
    return;
}

static void sqlite_basename(sqlite3_context *context, int argc, sqlite3_value **argv) {
    char *path = (char *) sqlite3_value_text(argv[0]);

    if (!path) {
        sqlite3_result_text(context, "", 0, SQLITE_TRANSIENT);
        return;
    }

    const size_t path_len = strlen(path);

    /* remove trailing '/' */
    size_t trimmed_len = path_len;
    while (trimmed_len && (path[trimmed_len - 1] == '/')) {
        trimmed_len--;
    }

    if (!trimmed_len) {
        sqlite3_result_text(context, "/", 1, SQLITE_TRANSIENT);
        return;
    }

    /* basename(work->name) will be the same as the first part of the input path, so remove it */
    size_t offset = trimmed_len;
    while (offset && (path[offset - 1] != '/')) {
        offset--;
    }

    const size_t bn_len = trimmed_len - offset;
    char *bn = path + offset;

    sqlite3_result_text(context, bn, bn_len, SQLITE_TRANSIENT);

    return;
}

int addqueryfuncs_common(sqlite3 *db) {
    return !((sqlite3_create_function(db,  "uidtouser",           2,   SQLITE_UTF8,
                                      NULL,                       &uidtouser,           NULL, NULL) == SQLITE_OK) &&
             (sqlite3_create_function(db,  "gidtogroup",          2,   SQLITE_UTF8,
                                      NULL,                       &gidtogroup,          NULL, NULL) == SQLITE_OK) &&
             (sqlite3_create_function(db,  "modetotxt",           1,   SQLITE_UTF8,
                                      NULL,                       &modetotxt,           NULL, NULL) == SQLITE_OK) &&
             (sqlite3_create_function(db,  "strftime",            2,   SQLITE_UTF8,
                                      NULL,                       &sqlite3_strftime,    NULL, NULL) == SQLITE_OK) &&
             (sqlite3_create_function(db,  "blocksize",           3,   SQLITE_UTF8,
                                      NULL,                       &blocksize,           NULL, NULL) == SQLITE_OK) &&
             (sqlite3_create_function(db,  "human_readable_size", 2,   SQLITE_UTF8,
                                      NULL,                       &human_readable_size, NULL, NULL) == SQLITE_OK) &&
             (sqlite3_create_function(db,  "basename",            1,   SQLITE_UTF8,
                                      NULL,                       &sqlite_basename,     NULL, NULL) == SQLITE_OK));
}

int addqueryfuncs_with_context(sqlite3 *db, size_t id, struct work *work) {
    /* only available if work is valid */
    if (work) {
        void *lvl = (void *) (uintptr_t) work->level;
        void *id_ptr = (void *) (uintptr_t) id;
        if (!((sqlite3_create_function(db,  "path",                -1, SQLITE_UTF8,
                                       work,                       &path,               NULL, NULL) == SQLITE_OK) &&
              (sqlite3_create_function(db,  "fpath",               0,  SQLITE_UTF8,
                                       id_ptr,                     &fpath,              NULL, NULL) == SQLITE_OK) &&
              (sqlite3_create_function(db,  "epath",               0,  SQLITE_UTF8,
                                       id_ptr,                     &epath,              NULL, NULL) == SQLITE_OK) &&
              (sqlite3_create_function(db,  "starting_point",      0,  SQLITE_UTF8,
                                       (void *) work->root,        &starting_point,     NULL, NULL) == SQLITE_OK) &&
              (sqlite3_create_function(db,  "level",               0,  SQLITE_UTF8,
                                       lvl,                        &relative_level,     NULL, NULL) == SQLITE_OK))) {
            return 1;
        }
    }

    return 0;
}

int addqueryfuncs(sqlite3 *db, size_t id, struct work *work) {
    return !((addqueryfuncs_common(db) == 0) &&
             (addqueryfuncs_with_context(db, id, work) == 0));
}

size_t print_results(sqlite3_stmt *res, FILE *out, const int printpath, const int printheader, const int printrows, const char *delim) {
    size_t rec_count = 0;

    // NOTE: if <sqlstmt> actually contained multiple statments, then this
    //       loop only runs with the final statement.
    int onetime=0;
    while (sqlite3_step(res) == SQLITE_ROW) {
        //printf("looping through rec_count %ds\n",rec_count);

        // find the column whose name is "name"
        int name_col = -1; // default to -1 if not found
        int ncols=sqlite3_column_count(res);
        for(int cnt = 0; cnt < ncols; cnt++) {
            const char *col_name = sqlite3_column_name(res, cnt);
            if ((strlen(col_name) == 4) &&
                (strncmp(col_name, "name", 4) == 0)) {
                name_col = cnt;
                break;
            }
        }

        // maybe print column-names as a header (once)
        if (printheader) {
            if (onetime == 0) {
                int cnt=0;
                while (ncols > 0) {
                    if (cnt==0) {
                        //if (printpath) fprintf(out,"path/%s",delim);
                    }
                    fprintf(out,"%s%s", sqlite3_column_name(res,cnt),delim);
                    //fprintf(out,"%s%s", sqlite3_column_decltype(res,cnt),delim);
                    ncols--;
                    cnt++;
                }
                fprintf(out,"\n");
                onetime++;
            }
        }

        // maybe print result-row values
        if (printrows) {
            //if (printpath) printf("%s/", name);
            int cnt=0;
            while (ncols > 0) {
                if (cnt==0) {
                    //if (printpath) fprintf(out,"%s/%s",shortname,delim);
                }
                if (cnt == name_col) {
                    fprintf(out,"%s%s", sqlite3_column_text(res,cnt),delim);
                }
                else {
                    fprintf(out,"%s%s", sqlite3_column_text(res,cnt),delim);
                }
                ncols--;
                cnt++;
            }
            fprintf(out,"\n");
        }

        // count of rows in query-result
        rec_count++;
    }

    return rec_count;
}

struct xattr_db *create_xattr_db(struct template_db *tdb,
                                 const char *path, const size_t path_len,
                                 uid_t uid, gid_t gid, mode_t mode,
                                 sqlite3_stmt *file_list) {
    /* /\* make sure only one of uid or gid is set *\/ */
    /* if (!((uid == in.xattrs.nobody.uid) ^ (gid == in.xattrs.nobody.gid)))) { */
    /*     return NULL; */
    /* } */

    struct xattr_db *xdb = malloc(sizeof(struct xattr_db));
    mode_t xattr_db_mode = 0;

    /* set the relative path in xdb */
    if (uid != in.xattrs.nobody.uid) {
        xdb->filename_len = SNPRINTF(xdb->filename, MAXPATH,
                                     XATTR_UID_FILENAME_FORMAT, uid);
        xdb->attach_len   = SNPRINTF(xdb->attach, MAXPATH,
                                     XATTR_UID_ATTACH_FORMAT, uid);
        xattr_db_mode = 0600;
    }
    else if (gid != in.xattrs.nobody.gid) {
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
    sqlite3_bind_int64(xdb->file_list, 1, xdb->st.st_uid);
    sqlite3_bind_int64(xdb->file_list, 2, xdb->st.st_gid);
    sqlite3_bind_text( xdb->file_list, 3, xdb->filename, xdb->filename_len, SQLITE_STATIC);
    sqlite3_bind_text( xdb->file_list, 4, xdb->attach, xdb->attach_len, SQLITE_STATIC);

    sqlite3_step(xdb->file_list);
    sqlite3_reset(xdb->file_list);

    free(xdb);
}

static const char XATTR_GET_DB_LIST[] = "SELECT filename, attachname FROM " XATTR_FILES ";";

/*
 * Attach the files listed in the xattr_files table to db.db and
 * create the xattrs view of all xattrs accessible by the caller.
 *
 * Then, create the xentries, xpentries, and xsummary views by doing
 * a LEFT JOIN with the xattrs view. These views contain all rows,
 * whether or not they have xattrs.
 */
int xattrprep(const char *path, const size_t path_len, sqlite3 *db
              #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
              , size_t *query_count
              #endif
    )
{
    static const char XATTR_COLS[] = " SELECT inode, name, value FROM ";
    static const size_t XATTR_COLS_LEN = sizeof(XATTR_COLS) - 1;
    static const size_t XATTRS_AVAIL_LEN = sizeof(XATTRS_AVAIL) - 1;

    int           rec_count = 0;
    sqlite3_stmt *res = NULL;
    /* not checking if the view exists - if it does, it's an error and will error */
    char          unioncmd[MAXSQL] = "CREATE TEMP VIEW " XATTRS " AS";
    char         *unioncmdp = unioncmd + strlen(unioncmd);

    /* step through each xattr db file */
    int error = sqlite3_prepare_v2(db, XATTR_GET_DB_LIST, MAXSQL, &res, NULL);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    (*query_count)++;
    #endif

    if (error != SQLITE_OK) {
        fprintf(stderr, "xattrprep Error: %s: Could not get filenames from table %s: %d err %s\n",
               path, XATTR_FILES, error, sqlite3_errmsg(db));
        return -1;
    }

    while (sqlite3_step(res) == SQLITE_ROW) {
        /* const int ncols = sqlite3_column_count(res); */
        /* if (ncols != 2) { */
        /*     fprintf(stderr, "Error: Searching xattr file list returned bad column count: %d (expected 2)\n", ncols); */
        /*     continue; */
        /* } */

        const char *filename   = (const char *) sqlite3_column_text(res, 0);
        const char *attachname = (const char *) sqlite3_column_text(res, 1);
        const size_t attachname_len = strlen(attachname);

        /* ATTACH <path>/<per-user/group db> AS <attach name> */
        char attcmd[MAXSQL];
        SNFORMAT_S(attcmd, sizeof(attcmd), 6,
                   "ATTACH \'", (size_t) 8,
                   path, path_len,
                   "/", (size_t) 1,
                   filename, strlen(filename),
                   "\' AS ", (size_t) 5,
                   attachname, attachname_len);

        /* if attach fails, you don't have access to the xattrs - just continue */
        if (sqlite3_exec(db, attcmd, NULL, NULL, NULL) == SQLITE_OK) {
            rec_count++;
            /* SELECT inode, name, value FROM <attach name>.xattrs_avail UNION */
            unioncmdp += SNFORMAT_S(unioncmdp, sizeof(unioncmd) - (unioncmdp - unioncmd), 5,
                                    XATTR_COLS, XATTR_COLS_LEN,
                                    attachname, attachname_len,
                                    ".", (size_t) 1,
                                    XATTRS_AVAIL, XATTRS_AVAIL_LEN,
                                    " UNION", (size_t) 6);
        }
    }
    sqlite3_finalize(res);

    SNFORMAT_S(unioncmdp, sizeof(unioncmd) - (unioncmdp - unioncmd), 2,
               XATTR_COLS, XATTR_COLS_LEN,
               XATTRS_AVAIL, XATTRS_AVAIL_LEN);

    /* create xattrs view */
    int rc = sqlite3_exec(db, unioncmd, NULL, NULL, NULL);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    (*query_count)++;
    #endif

    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: Create xattrs view failed: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    /* create LEFT JOIN views (all rows, with and without xattrs) */
    /* these should run once, and be no-ops afterwards since the backing data of the views get swapped out */

    rc = sqlite3_exec(db, "CREATE TEMP VIEW IF NOT EXISTS " XENTRIES " AS SELECT " ENTRIES ".*, " XATTRS ".name as xattr_name, " XATTRS ".value as xattr_value FROM " ENTRIES " LEFT JOIN " XATTRS " ON " ENTRIES ".inode == " XATTRS ".inode;", NULL, NULL, NULL);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    (*query_count)++;
    #endif

    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: Create xentries view failed: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    rc = sqlite3_exec(db, "CREATE TEMP VIEW IF NOT EXISTS " XPENTRIES " AS SELECT " PENTRIES ".*, " XATTRS ".name as xattr_name, " XATTRS ".value as xattr_value FROM " PENTRIES " LEFT JOIN " XATTRS " ON " PENTRIES ".inode == " XATTRS ".inode;", NULL, NULL, NULL);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    (*query_count)++;
    #endif

    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: Create xpentries view failed: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    rc = sqlite3_exec(db, "CREATE TEMP VIEW IF NOT EXISTS " XSUMMARY " AS SELECT " SUMMARY ".*, " XATTRS ".name as xattr_name, " XATTRS ".value as xattr_value FROM " SUMMARY " LEFT JOIN " XATTRS " ON " SUMMARY ".inode == " XATTRS ".inode;", NULL, NULL, NULL);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    (*query_count)++;
    #endif

    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: create xsummary view failed: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    return rec_count;
}

static int detach_xattr_db(void *args, int count, char **data, char **columns) {
    detachdb(NULL, (sqlite3 *) args, data[1], 0); /* don't check for errors */
    return 0;
}

void xattrdone(sqlite3 *db
               #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
               , size_t *query_count
               #endif
    )
{
    /* no need to drop xentries, xpentries, and xsummary */

    sqlite3_exec(db, "DROP VIEW IF EXISTS " XATTRS ";", NULL, NULL, NULL);
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    (*query_count)++;
    #endif

    sqlite3_exec(db, XATTR_GET_DB_LIST, detach_xattr_db, db, NULL);
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    (*query_count)++;
    #endif
}
