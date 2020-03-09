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
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <pwd.h>
#include <grp.h>
#include "pcre.h"

#include "dbutils.h"

extern int errno;

static const char GUFI_SQLITE_VFS[] = "unix-none";

char *rsql = // "DROP TABLE IF EXISTS readdirplus;"
            "CREATE TABLE readdirplus(path TEXT, type TEXT, inode INT64 PRIMARY KEY, pinode INT64, suspect INT64);";

char *rsqli = "INSERT INTO readdirplus VALUES (@path,@type,@inode,@pinode,@suspect);";

char *esql = // "DROP TABLE IF EXISTS entries;"
            "CREATE TABLE entries(id INTEGER PRIMARY KEY, name TEXT, type TEXT, inode INT64, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattrs TEXT, crtime INT64, ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, osstext1 TEXT, osstext2 TEXT);";

char *esqli = "INSERT INTO entries VALUES (NULL,@name,@type,@inode,@mode,@nlink,@uid,@gid,@size,@blksize,@blocks,@atime,@mtime, @ctime,@linkname,@xattrs,@crtime,@ossint1,@ossint2,@ossint3,@ossint4,@osstext1,@osstext2);";

char *ssql = "DROP TABLE IF EXISTS summary;"
             "CREATE TABLE summary(id INTEGER PRIMARY KEY, name TEXT, type TEXT, inode INT64, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattrs TEXT, totfiles INT64, totlinks INT64, minuid INT64, maxuid INT64, mingid INT64, maxgid INT64, minsize INT64, maxsize INT64, totltk INT64, totmtk INT64, totltm INT64, totmtm INT64, totmtg INT64, totmtt INT64, totsize INT64, minctime INT64, maxctime INT64, minmtime INT64, maxmtime INT64, minatime INT64, maxatime INT64, minblocks INT64, maxblocks INT64, totxattr INT64,depth INT64, mincrtime INT64, maxcrtime INT64, minossint1 INT64, maxossint1 INT64, totossint1 INT64, minossint2 INT64, maxossint2 INT64, totossint2 INT64, minossint3 INT64, maxossint3 INT64, totossint3 INT64,minossint4 INT64, maxossint4 INT64, totossint4 INT64, rectype INT64, pinode INT64);";

char *tsql = "DROP TABLE IF EXISTS treesummary;"
             "CREATE TABLE treesummary(totsubdirs INT64, maxsubdirfiles INT64, maxsubdirlinks INT64, maxsubdirsize INT64, totfiles INT64, totlinks INT64, minuid INT64, maxuid INT64, mingid INT64, maxgid INT64, minsize INT64, maxsize INT64, totltk INT64, totmtk INT64, totltm INT64, totmtm INT64, totmtg INT64, totmtt INT64, totsize INT64, minctime INT64, maxctime INT64, minmtime INT64, maxmtime INT64, minatime INT64, maxatime INT64, minblocks INT64, maxblocks INT64, totxattr INT64,depth INT64, mincrtime INT64, maxcrtime INT64, minossint1 INT64, maxossint1 INT64, totossint1 INT64, minossint2 INT64, maxossint2 INT64, totossint2 INT64, minossint3 INT64, maxossint3 INT64, totossint3 INT64, minossint4 INT64, maxossint4 INT64, totossint4 INT64,rectype INT64, uid INT64, gid INT64);";

char *vesql = "DROP VIEW IF EXISTS pentries;"
              "create view pentries as select entries.*, summary.inode as pinode from entries, summary where rectype=0;";


char *vssqldir   = "DROP VIEW IF EXISTS vsummarydir;"
                   "create view vsummarydir as select * from summary where rectype=0;";
char *vssqluser  = "DROP VIEW IF EXISTS vsummaryuser;"
                   "create view vsummaryuser as select * from summary where rectype=1;";
char *vssqlgroup = "DROP VIEW IF EXISTS vsummarygroup;"
                   "create view vsummarygroup as select * from summary where rectype=2;";

char *vtssqldir   = "DROP VIEW IF EXISTS vtsummarydir;"
                    "create view vtsummarydir as select * from treesummary where rectype=0;";
char *vtssqluser  = "DROP VIEW IF EXISTS vtsummaryuser;"
                    "create view vtsummaryuser as select * from treesummary where rectype=1;";
char *vtssqlgroup = "DROP VIEW IF EXISTS vtsummarygroup;"
                    "create view vtsummarygroup as select * from treesummary where rectype=2;";




sqlite3 * attachdb(const char *name, sqlite3 *db, const char *dbn, const OpenMode mode)
{
  char attach[MAXSQL];
  if (mode == RDONLY) {
      if (!sqlite3_snprintf(MAXSQL, attach, "ATTACH 'file:%q?mode=ro' AS %Q", name, dbn)) {
          fprintf(stderr, "Cannot create ATTACH command\n");
          return NULL;
      }
  }
  else if (mode == RDWR) {
      if (!sqlite3_snprintf(MAXSQL, attach, "ATTACH %Q AS %Q", name, dbn)) {
          fprintf(stderr, "Cannot create ATTACH command\n");
          return NULL;
      }
  }

  if (sqlite3_exec(db, attach, NULL, NULL, NULL) != SQLITE_OK) {
      fprintf(stderr, "Cannot attach database \"%s\" as \"%s\": %s\n", name, dbn, sqlite3_errmsg(db));
      return NULL;
  }

  return db;
}

sqlite3 * detachdb(const char *name, sqlite3 *db, const char *dbn)
{
  char detach[MAXSQL];
  if (!sqlite3_snprintf(MAXSQL, detach, "DETACH %Q", dbn)) {
      fprintf(stderr, "Cannot create DETACH command\n");
      return NULL;
  }

  if (sqlite3_exec(db, detach, NULL, NULL, NULL) != SQLITE_OK) {
      fprintf(stderr, "Cannot detach database: %s %s\n", name, sqlite3_errmsg(db));
      return NULL;
  }

  return db;
}

int create_table_wrapper(const char *name, sqlite3 * db, const char * sql_name, const char * sql, int (*callback)(void*,int,char**,char**), void * args) {
    char *err_msg = NULL;
    const int rc = sqlite3_exec(db, sql, callback, args, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "%s:%d SQL error while executing '%s' on %s: '%s' (%d)\n",
                __FILE__, __LINE__, sql_name, name, err_msg, rc);
        sqlite3_free(err_msg);
    }
    return rc;
}

static int set_pragmas(sqlite3 * db) {
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

sqlite3 * opendb(const char * name, const OpenMode mode, const int setpragmas, const int load_extensions,
                 int (*modifydb)(const char * name, sqlite3 * db, void * args), void * modifydb_args
                 #ifdef DEBUG
                 , struct timespec * sqlite3_open_start
                 , struct timespec * sqlite3_open_end
                 , struct timespec * set_pragmas_start
                 , struct timespec * set_pragmas_end
                 , struct timespec * load_extension_start
                 , struct timespec * load_extension_end
                 , struct timespec * modifydb_start
                 , struct timespec * modifydb_end
                 #endif
    ) {
    sqlite3 * db = NULL;

    #ifdef DEBUG
    if (sqlite3_open_start) {
        clock_gettime(CLOCK_MONOTONIC, sqlite3_open_start);
    }
    #endif

    int flags = SQLITE_OPEN_URI;
    if (mode == RDONLY) {
        flags |= SQLITE_OPEN_READONLY;
    }
    else {
        flags |= SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE;
    }

    if (sqlite3_open_v2(name, &db, flags, GUFI_SQLITE_VFS) != SQLITE_OK) {
        #ifdef DEBUG
        if (sqlite3_open_end) {
            clock_gettime(CLOCK_MONOTONIC, sqlite3_open_end);
        }
        #endif
        /* fprintf(stderr, "Cannot open database: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db)); */
        sqlite3_close(db); /* close db even if it didn't open to avoid memory leaks */
        return NULL;
    }

    #ifdef DEBUG
    if (sqlite3_open_end) {
        clock_gettime(CLOCK_MONOTONIC, sqlite3_open_end);
    }
    #endif

    #ifdef DEBUG
    if (set_pragmas_start) {
        clock_gettime(CLOCK_MONOTONIC, set_pragmas_start);
    }
    #endif

    if (setpragmas) {
        /* ignore errors */
        set_pragmas(db);
    }

    #ifdef DEBUG
    if (set_pragmas_end) {
        clock_gettime(CLOCK_MONOTONIC, set_pragmas_end);
    }
    #endif

    #ifdef DEBUG
    if (load_extension_start) {
        clock_gettime(CLOCK_MONOTONIC, load_extension_start);
    }
    #endif

    if (load_extensions) {
        if ((sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, NULL) != SQLITE_OK) || /* enable loading of extensions */
            (sqlite3_extension_init(db, NULL, NULL)                                != SQLITE_OK)) { /* load the sqlite3-pcre extension */
            #ifdef DEBUG
            if (load_extension_end) {
                clock_gettime(CLOCK_MONOTONIC, load_extension_end);
            }
            #endif
            fprintf(stderr, "Unable to load regex extension\n");
            sqlite3_close(db);
            return NULL;
        }
    }

    #ifdef DEBUG
    if (load_extension_end) {
        clock_gettime(CLOCK_MONOTONIC, load_extension_end);
    }
    #endif

    #ifdef DEBUG
    if (modifydb_start) {
        clock_gettime(CLOCK_MONOTONIC, modifydb_start);
    }
    #endif

    if (modifydb) {
        if (modifydb(name, db, modifydb_args) != 0) {
            #ifdef DEBUG
            if (modifydb_end) {
                clock_gettime(CLOCK_MONOTONIC, modifydb_end);
            }
            #endif
            fprintf(stderr, "Cannot modify database: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db));
            sqlite3_close(db);
            return NULL;
        }
    }

    #ifdef DEBUG
    if (modifydb_end) {
        clock_gettime(CLOCK_MONOTONIC, modifydb_end);
    }
    #endif

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
     if (in.outfile > 0)
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

sqlite3_stmt * insertdbprep(sqlite3 *db)
{
    const char *tail;
    int error = SQLITE_OK;
    sqlite3_stmt *reso;

    // WARNING: passing length-arg that is longer than SQL text
    error= sqlite3_prepare_v2(db, esqli, MAXSQL, &reso, &tail);
    if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on insertdbprep: error %d %s err %s\n",
                  error,esqli,sqlite3_errmsg(db));
          return NULL;
    }
    return reso;
}

sqlite3_stmt * insertdbprepr(sqlite3 *db)
{
    const char *tail;
    int error = SQLITE_OK;
    sqlite3_stmt *reso;

    // WARNING: passing length-arg that is longer than SQL text
    error= sqlite3_prepare_v2(db, rsqli, MAXSQL, &reso, &tail);
    if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on insertdbprepr: error %d %s err %s\n",
                  error,rsqli,sqlite3_errmsg(db));
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
    char *zxattr;
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
    zxattr = sqlite3_mprintf("%q",pwork->xattr);
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
    error=sqlite3_bind_text(res,15,zxattr,-1,SQLITE_STATIC);
    if (error != SQLITE_OK) printf("insertdbgo bind xattr: error %d %s %s %s %s\n",error,pwork->name,pwork->type,pwork->xattr,sqlite3_errmsg(db));
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
    sqlite3_free(zxattr);
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

int insertsumdb(sqlite3 *sdb, struct work *pwork,struct sum *su)
{
    char *err_msg = 0;
    char sqlstmt[MAXSQL];
    int rc;
    int depth = 0;
    int rectype = 0;

    /* remove trailing path separators (pwork->name is a directory, not a file) */
    for(size_t i = strlen(pwork->name) - 1; i && (pwork->name[i] == '/'); i--) {
        pwork->name[i] = '\0';
    }

    /* get the basename */
    char nameout[MAXPATH];
    char shortname[MAXPATH];
    shortpath(pwork->name, nameout, shortname);

/*
CREATE TABLE summary(name TEXT PRIMARY KEY, type TEXT, inode INT, mode INT, nlink INT, uid INT, gid INT, size INT, blksize INT, blocks INT, atime INT, mtime INT, ctime INT, linkname TEXT, xattrs TEXT, totfiles INT, totlinks INT, minuid INT, maxuid INT, mingid INT, maxgid INT, minsize INT, maxsize INT, totltk INT, totmtk INT, totltm INT, totmtm INT, totmtg INT, totmtt INT, totsize INT, minctime INT, maxctime INT, minmtime INT, maxmtime INT, minatime INT, maxatime INT, minblocks INT, maxblocks INT, totxattr INT,depth INT, mincrtime INT, maxcrtime INT, minossint1 INT, maxossint1 INT, totossint1 INT, minossint2 INT, maxossint2, totossint2 INT, minossint3 INT, maxossint3, totossint3 INT,minossint4 INT, maxossint4 INT, totossint4 INT, rectype INT, pinode INT);
*/
    char *zname     = sqlite3_mprintf("%q",shortname);
    char *ztype     = sqlite3_mprintf("%q",pwork->type);
    char *zlinkname = sqlite3_mprintf("%q",pwork->linkname);
    char *zxattr    = sqlite3_mprintf("%q",pwork->xattr);

    SNPRINTF(sqlstmt,MAXSQL,"INSERT INTO summary VALUES "
            "(NULL, \'%s\', \'%s\', "
            "%"STAT_ino", %d, %"STAT_nlink", %d, %d, %"STAT_size", %"STAT_bsize", %"STAT_blocks", %ld, %ld, %ld, "
            "\'%s\', \'%s\', "
            "%lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %lld);",
            zname,
            ztype,

            pwork->statuso.st_ino,      // STAT_ino
            pwork->statuso.st_mode,
            pwork->statuso.st_nlink,    // STAT_nlink
            pwork->statuso.st_uid,
            pwork->statuso.st_gid,
            pwork->statuso.st_size,     // STAT_size
            pwork->statuso.st_blksize,  // STAT_bsize
            pwork->statuso.st_blocks,   // STAT_blocks
            pwork->statuso.st_atime,
            pwork->statuso.st_mtime,
            pwork->statuso.st_ctime,

            zlinkname,
            zxattr,

            su->totfiles,
            su->totlinks,
            su->minuid,
            su->maxuid,
            su->mingid,
            su->maxgid,
            su->minsize,
            su->maxsize,
            su->totltk,
            su->totmtk,
            su->totltm,
            su->totmtm,
            su->totmtg,
            su->totmtt,
            su->totsize,
            su->minctime,
            su->maxctime,
            su->minmtime,
            su->maxmtime,
            su->minatime,
            su->maxatime,
            su->minblocks,
            su->maxblocks,
            su->totxattr,
            depth,
            su->mincrtime,
            su->maxcrtime,
            su->minossint1,
            su->maxossint1,
            su->totossint1,
            su->minossint2,
            su->maxossint2,
            su->totossint2,
            su->minossint3,
            su->maxossint3,
            su->totossint3,
            su->minossint4,
            su->maxossint4,
            su->totossint4,
            rectype,
            pwork->pinode);

    sqlite3_free(zname);
    sqlite3_free(ztype);
    sqlite3_free(zlinkname);
    sqlite3_free(zxattr);

    rc = sqlite3_exec(sdb, sqlstmt, 0, 0, &err_msg);
    if (rc != SQLITE_OK ) {
        fprintf(stderr, "SQL error on insert (summary): %s\n", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    return 0;
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

static void path(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    const size_t id = (size_t) (uintptr_t) sqlite3_user_data(context);
    sqlite3_result_text(context, gps[id].gpath, -1, SQLITE_TRANSIENT);
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
    struct group * fmygroup = getgrgid(fgid);
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
        modetostr(tmode, fmode);
        sqlite3_result_text(context, tmode, -1, SQLITE_TRANSIENT);
        return;
    }
    sqlite3_result_null(context);
}

static void sqlite3_strftime(sqlite3_context *context, int argc, sqlite3_value **argv) {
    const char * fmt = (char *) sqlite3_value_text(argv[0]); /* format    */
    const time_t t = sqlite3_value_int64(argv[1]);           /* timestamp */

    char buf[MAXPATH];
    strftime(buf, sizeof(buf), fmt, localtime(&t));
    sqlite3_result_text(context, buf, -1, SQLITE_TRANSIENT);

    return;
}

static const char SIZE[] = {' ', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'};

/* Returns the number of blocks required to store a given size */
/* Unfilled blocks count as one full block (round up)          */
static void blocksize(sqlite3_context *context, int argc, sqlite3_value **argv) {
    const size_t size  = sqlite3_value_int64(argv[0]);
    const char * unit  = (char *) sqlite3_value_text(argv[1]);
    const int    align = sqlite3_value_int(argv[2]);

    size_t unit_size = 1;

    const size_t len = strlen(unit);
    if (len) {
        for(size_t i = 1; i < sizeof(SIZE); i++) {
            if (len == 1)
                unit_size *= 1024;
            if ((len == 2) && (unit[1] == 'B'))
                unit_size *= 1000;
            if ((len == 3) && (unit[1] == 'i') && (unit[2] == 'B'))
                unit_size *= 1024;

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
        while (size > 1024) {
            size /= 1024;
            unit_index++;
        }

        if (unit_index == 0) {
            snprintf(format, sizeof(format), "%%%d.1f", align);
            snprintf(buf, sizeof(buf), format, size);
        }
        else {
            snprintf(format, sizeof(format), "%%%d.1f%%c", align - 1);
            snprintf(buf, sizeof(buf), format, size, SIZE[unit_index]);
        }
    }
    else {
        snprintf(format, sizeof(format), "%%%dd", align);
        snprintf(buf, sizeof(buf), format, 0);
    }

    sqlite3_result_text(context, buf, -1, SQLITE_TRANSIENT);

    return;
}

static void level(sqlite3_context *context, int argc, sqlite3_value **argv) {
    size_t level = (size_t) (uintptr_t) sqlite3_user_data(context);
    sqlite3_result_int64(context, level);
    return;
}

int addqueryfuncs(sqlite3 *db, size_t id, size_t lvl) {
    return ((sqlite3_create_function(db, "path",                0, SQLITE_UTF8, (void *) (uintptr_t) id,  &path,                NULL, NULL) == SQLITE_OK) &&
            (sqlite3_create_function(db, "fpath",               0, SQLITE_UTF8, (void *) (uintptr_t) id,  &fpath,               NULL, NULL) == SQLITE_OK) &&
            (sqlite3_create_function(db, "epath",               0, SQLITE_UTF8, (void *) (uintptr_t) id,  &epath,               NULL, NULL) == SQLITE_OK) &&
            (sqlite3_create_function(db, "uidtouser",           2, SQLITE_UTF8, NULL,                     &uidtouser,           NULL, NULL) == SQLITE_OK) &&
            (sqlite3_create_function(db, "gidtogroup",          2, SQLITE_UTF8, NULL,                     &gidtogroup,          NULL, NULL) == SQLITE_OK) &&
            (sqlite3_create_function(db, "modetotxt",           1, SQLITE_UTF8, NULL,                     &modetotxt,           NULL, NULL) == SQLITE_OK) &&
            (sqlite3_create_function(db, "strftime",            2, SQLITE_UTF8, NULL,                     &sqlite3_strftime,    NULL, NULL) == SQLITE_OK) &&
            (sqlite3_create_function(db, "blocksize",           3, SQLITE_UTF8, NULL,                     &blocksize,           NULL, NULL) == SQLITE_OK) &&
            (sqlite3_create_function(db, "human_readable_size", 2, SQLITE_UTF8, NULL,                     &human_readable_size, NULL, NULL) == SQLITE_OK) &&
            (sqlite3_create_function(db, "level",               0, SQLITE_UTF8, (void *) (uintptr_t) lvl, &level,               NULL, NULL) == SQLITE_OK))?0:1;
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
