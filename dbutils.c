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

-----
NOTE:
-----

GUFI uses the C-Thread-Pool library.  The original version, written by
Johan Hanssen Seferidis, is found at
https://github.com/Pithikos/C-Thread-Pool/blob/master/LICENSE, and is
released under the MIT License.  LANS, LLC added functionality to the
original work.  The original work, plus LANS, LLC added functionality is
found at https://github.com/jti-lanl/C-Thread-Pool, also under the MIT
License.  The MIT License can be found at
https://opensource.org/licenses/MIT.


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



#include <string.h>
#include <stdlib.h>

#include <pwd.h>
#include <grp.h>
#include <uuid/uuid.h>

#include "dbutils.h"

#include "bf.h"

char *rsql = // "DROP TABLE IF EXISTS readdirplus;"
            "CREATE TABLE readdirplus(path TEXT, type TEXT, inode INT64 PRIMARY KEY, pinode INT64, suspect INT64);";

char *rsqli = "INSERT INTO readdirplus VALUES (@path,@type,@inode,@pinode,@suspect);";

char *esql = // "DROP TABLE IF EXISTS entries;"
            "CREATE TABLE entries(name TEXT PRIMARY KEY, type TEXT, inode INT64, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattrs TEXT, crtime INT64, ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, osstext1 TEXT, osstext2 TEXT);";

char *esqli = "INSERT INTO entries VALUES (@name,@type,@inode,@mode,@nlink,@uid,@gid,@size,@blksize,@blocks,@atime,@mtime, @ctime,@linkname,@xattrs,@crtime,@ossint1,@ossint2,@ossint3,@ossint4,@osstext1,@osstext2);";

char *ssql = "DROP TABLE IF EXISTS summary;"
             "CREATE TABLE summary(name TEXT PRIMARY KEY, type TEXT, inode INT64, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattrs TEXT, totfiles INT64, totlinks INT64, minuid INT64, maxuid INT64, mingid INT64, maxgid INT64, minsize INT64, maxsize INT64, totltk INT64, totmtk INT64, totltm INT64, totmtm INT64, totmtg INT64, totmtt INT64, totsize INT64, minctime INT64, maxctime INT64, minmtime INT64, maxmtime INT64, minatime INT64, maxatime INT64, minblocks INT64, maxblocks INT64, totxattr INT64,depth INT64, mincrtime INT64, maxcrtime INT64, minossint1 INT64, maxossint1 INT64, totossint1 INT64, minossint2 INT64, maxossint2 INT64, totossint2 INT64, minossint3 INT64, maxossint3 INT64, totossint3 INT64,minossint4 INT64, maxossint4 INT64, totossint4 INT64, rectype INT64, pinode INT64);";

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




sqlite3 *  attachdb(const char *name, sqlite3 *db, char *dbn)
{
  char *err_msg = 0;
  int rc;
  char sqlat[MAXSQL];
  char *zname;

  zname = sqlite3_mprintf("%q",name);
  sprintf(sqlat,"ATTACH \'%s/%s\' as %s",zname,DBNAME,dbn);
  sqlite3_free(zname);
  rc=sqlite3_exec(db, sqlat,0, 0, &err_msg);
  if (rc != SQLITE_OK) {
      fprintf(stderr, "Cannot attach database: %s/%s %s\n", name,DBNAME, sqlite3_errmsg(db));
      sqlite3_close(db);
      return NULL;
  }
  sqlite3_free(err_msg);
 
  return 0;
}

sqlite3 *  detachdb(const char *name, sqlite3 *db, char *dbn)
{
  char *err_msg = 0;
  int rc;
  char sqldet[MAXSQL];

  sprintf(sqldet,"DETACH %s",dbn);
  rc=sqlite3_exec(db, sqldet,0, 0, &err_msg);
  if (rc != SQLITE_OK) {
      fprintf(stderr, "Cannot detach database: %s/%s %s\n", name,DBNAME, sqlite3_errmsg(db));
      sqlite3_close(db);
      return NULL;
  }
  sqlite3_free(err_msg);
 
  return 0;
}


// sqlite3_exec(db, tsql, 0, 0, &err_msg);
#define SQLITE3_EXEC(DB, SQL, FN, ARG, ERRMSG)              \
   do {                                                     \
      rc = sqlite3_exec(DB, SQL, FN, ARG, ERRMSG);          \
      if (rc != SQLITE_OK ) {                               \
         printf("%s:%d SQL error [%s]: '%s'\nSQL: %s",      \
                __FILE__, __LINE__, #SQL, *(ERRMSG), SQL);  \
         sqlite3_free(*(ERRMSG));                           \
         sqlite3_close(DB);                                 \
                                                            \
         /* Gary debugging */                               \
         printf("trying  %s\n",dbn);                        \
         printf("trying  %s\n",vssqldir);                   \
         sprintf(deb,"ls -l %s/%s",in.nameto,name);         \
         system(deb);                                       \
         exit(9);                                           \
                                                            \
         return NULL;                                       \
      }                                                     \
   } while (0)
   

sqlite3 *  opendb(const char *name, sqlite3 *db, int openwhat, int createtables)
{
    char *err_msg = 0;
    char dbn[MAXPATH];
    int rc;
    char deb[MAXPATH];

    sprintf(dbn,"%s/%s", name,DBNAME);
    if (createtables) {
        if (openwhat != 3)
          sprintf(dbn,"%s/%s/%s", in.nameto,name,DBNAME);
       if (openwhat==7)
          sprintf(dbn,"%s", name);
    }
    else {
       if (openwhat == 6)
          sprintf(dbn,"%s/%s/%s", in.nameto,name,DBNAME);
       if (openwhat==5)
          sprintf(dbn,"%s", name);
    }


    //    // The current codebase is not intended to handle incremental updates.
    //    // When initializing a DB, we always expect it not to already exist,
    //    // and populate from scratch, from the source tree.
    //    if (createtables) {
    //       struct stat st;
    //       if (! lstat(dbn, &st)) {
    //          fprintf(stderr, "Database: %s already exists\n", dbn);
    //          return NULL;
    //       }
    //    }
    //printf("sqlite3_open %s\n",dbn);
    rc = sqlite3_open(dbn, &db);
    if (rc != SQLITE_OK) {
       sleep(2);
       rc = sqlite3_open(dbn, &db);
    }
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s %s rc %d\n", dbn, sqlite3_errmsg(db),rc);
        sqlite3_free(err_msg);
        sqlite3_close(db);
printf("trying to open %s\n",dbn);
sprintf(deb,"ls -l %s/%s",in.nameto,name);
system(deb);
exit(9);
        return NULL;
    }

    //printf("sqlite3_open %s openwhat %d\n",dbn,openwhat);
    if (createtables) {
       //printf("sqlite3_open %s openwhat %d creating tables\n",dbn,openwhat);
       if (openwhat==1 || openwhat==4)
          //printf("esql: %s \n", dbn);
          SQLITE3_EXEC(db, esql, 0, 0, &err_msg);
          //printf("esql: %s %s \n", dbn, sqlite3_errmsg(db));

       if (openwhat==3) {
          SQLITE3_EXEC(db, tsql, 0, 0, &err_msg);
          SQLITE3_EXEC(db, vtssqldir, 0, 0, &err_msg);
          SQLITE3_EXEC(db, vtssqluser, 0, 0, &err_msg);
          SQLITE3_EXEC(db, vtssqlgroup, 0, 0, &err_msg);
       }
       if (openwhat==2 || openwhat==4) {
          SQLITE3_EXEC(db, ssql, 0, 0, &err_msg);
          SQLITE3_EXEC(db, vssqldir, 0, 0, &err_msg);
          SQLITE3_EXEC(db, vssqluser, 0, 0, &err_msg);
          SQLITE3_EXEC(db, vssqlgroup, 0, 0, &err_msg);
          SQLITE3_EXEC(db, vesql, 0, 0, &err_msg);
          //printf("vesql: %s %s \n", dbn, sqlite3_errmsg(db));
       }
       if (openwhat==7) {
          SQLITE3_EXEC(db, rsql, 0, 0, &err_msg);
       }
    }

    //printf("returning from opendb ok\n");
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
     const char   *errMSG;
     const char   *tail;
     int           ncols;
     int           cnt;
     int           lastsql;
     int           onetime;
     //char   lsqlstmt[MAXSQL];
     //char   prefix[MAXPATH];
     //char   shortname[MAXPATH];
     //char  *pp;

     int           i;
     FILE *        out;
     char          ffielddelim[2];
 
     if (! sqlstmt) {
        fprintf(stderr, "SQL was empty\n");
        return -1;
     }

     out = stdout;
     if (in.outfile > 0)
        out = gts.outfd[ptid];

     if (in.dodelim == 0)
       sprintf(ffielddelim,"|");

     if (in.dodelim == 1)
       sprintf(ffielddelim,"%s",fielddelim);

     if (in.dodelim == 2)
       sprintf(ffielddelim,"%s",in.delim);


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
     onetime=0;
     while (sqlite3_step(res) == SQLITE_ROW) {
        //printf("looping through rec_count %ds\n",rec_count);

        // maybe print column-names as a header (once)
        if (printheader) {
           if (onetime == 0) {
              cnt=0;
              ncols=sqlite3_column_count(res);
              while (ncols > 0) {
                 if (cnt==0) {
                    //if (printpath) fprintf(out,"path/%s",ffielddelim);
                 }
                 fprintf(out,"%s%s", sqlite3_column_name(res,cnt),ffielddelim);
                 //fprintf(out,"%s%s", sqlite3_column_decltype(res,cnt),ffielddelim);
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
           cnt=0;
           ncols=sqlite3_column_count(res);
           while (ncols > 0) {
              if (cnt==0) {
                 //if (printpath) fprintf(out,"%s/%s",shortname,ffielddelim);
              }
              fprintf(out,"%s%s", sqlite3_column_text(res,cnt),ffielddelim);
              ncols--;
              cnt++;
           }
           fprintf(out,"\n");
        }

        // count of rows in query-result
        rec_count++;
     }

    //printf("We received %d records.\n", rec_count);
    // sqlite3_reset(res);
    sqlite3_finalize(res);
    return(rec_count);
}

int querytsdb(const char *name, struct sum *sumin, sqlite3 *db, int *recs, int ts)
{
     sqlite3_stmt    *res;
     int     error = 0;
     const char      *errMSG;
     const char      *tail;
     char sqlstmt[MAXSQL];
     //struct sum sumin;

     if (ts) {
       sprintf(sqlstmt,"select totfiles,totlinks,minuid,maxuid,mingid,maxgid,minsize,maxsize,totltk,totmtk,totltm,totmtm,totmtg,totmtt,totsize,minctime,maxctime,minmtime,maxmtime,minatime,maxatime,minblocks,maxblocks,totxattr,mincrtime,maxcrtime,minossint1,maxossint1,totossint1,minossint2,maxossint2,totossint2,minossint3,maxossint3,totossint3,minossint4,maxossint4,totossint4,totsubdirs,maxsubdirfiles,maxsubdirlinks,maxsubdirsize "
               "from treesummary where rectype=0;");
     } else {
       sprintf(sqlstmt,"select totfiles,totlinks,minuid,maxuid,mingid,maxgid,minsize,maxsize,totltk,totmtk,totltm,totmtm,totmtg,totmtt,totsize,minctime,maxctime,minmtime,maxmtime,minatime,maxatime,minblocks,maxblocks,totxattr,mincrtime,maxcrtime,minossint1,maxossint1,totossint1,minossint2,maxossint2,totossint2,minossint3,maxossint3,totossint3,minossint4,maxossint4,totossint4 "
               "from summary where rectype=0;;");
     }

     // WARNING: passing length-arg that is longer than SQL text
     error = sqlite3_prepare_v2(db, sqlstmt, MAXSQL, &res, &tail);
     if (error != SQLITE_OK) {
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

     //printf("tsdb: totfiles %d totlinks %d minuid %d\n",
     //       sumin->totfiles,sumin->totlinks,sumin->minuid);
     sqlite3_finalize(res);

     return 0;
}

int startdb(sqlite3 *db)
{
    char *err_msg = 0;
    int rc;
    rc = sqlite3_exec(db, "BEGIN TRANSACTION", NULL , NULL, &err_msg);
    if (rc != SQLITE_OK) printf("begin transaction issue %s\n",sqlite3_errmsg(db));
    sqlite3_free(err_msg);
    return 0;
}

int stopdb(sqlite3 *db)
{
    char *err_msg = 0;
    int rc;
    sqlite3_exec(db,"END TRANSACTION",NULL, NULL, &err_msg);
    sqlite3_free(err_msg);
    return 0;
}

int closedb(sqlite3 *db)
{
    char *err_msg = 0;
    int rc;
    sqlite3_free(err_msg);
    rc = sqlite3_close(db);
    if (rc != SQLITE_OK) {
      printf("closedb issue %s\n",sqlite3_errmsg(db));
      exit(9);
    }
    return 0;
}

int insertdbfin(sqlite3 *db,sqlite3_stmt *res)
{
    char *err_msg = 0;
    int rc;
    
    sqlite3_finalize(res);

    return 0;
}

sqlite3_stmt * insertdbprep(sqlite3 *db,sqlite3_stmt *res)
{
    char *err_msg = 0;
    // char sqlstmt[MAXSQL];
    int rc;
    const char *tail;
    int error;
    sqlite3_stmt *reso;

    /*******  this is a bit dangerous, as all the records need to be written
              to the db, plus some time for the OS to write to disk ****/
    error=sqlite3_exec(db, "PRAGMA synchronous = OFF", NULL, NULL, &err_msg);
    if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on insertdbprep setting sync off: error %d err %s\n",
                  error,sqlite3_errmsg(db));
    }

    // WARNING: passing length-arg that is longer than SQL text
    error= sqlite3_prepare_v2(db, esqli, MAXSQL, &reso, &tail);
    if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on insertdbprep: error %d %s err %s\n",
                  error,esqli,sqlite3_errmsg(db));
          return NULL;
    }
    return reso;
}

sqlite3_stmt * insertdbprepr(sqlite3 *db,sqlite3_stmt *res)
{
    char *err_msg = 0;
    // char sqlstmt[MAXSQL];
    int rc;
    const char *tail;
    int error;
    sqlite3_stmt *reso;

    /*******  this is a bit dangerous, as all the records need to be written
              to the db, plus some time for the OS to write to disk ****/
    error=sqlite3_exec(db, "PRAGMA synchronous = OFF", NULL, NULL, &err_msg);
    if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on insertdbprep setting sync off: error %d err %s\n",
                  error,sqlite3_errmsg(db));
    }

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
    char *err_msg = 0;
    char sqlstmt[MAXSQL];
    int rc;
    int error;
    char *zname;
    char *ztype;
    char *zlinkname;
    char *zxattr;
    char *zosstext1;
    char *zosstext2;
    int len=0;;
    const char *shortname;
    int i;
    int found;
    int cnt;


//    sprintf(sqlstmt,"INSERT INTO entries VALUES (\'%s\', \'%s\',%lld, %d, %d, %d, %d, %lld, %d, %lld, %ld, %ld, %ld, \'%s\', \'%s\');",zname,ztype,st->st_ino,st->st_mode,st->st_nlink,st->st_uid,st->st_gid,st->st_size,st->st_blksize,st->st_blocks,st->st_atime,st->st_mtime,st->st_ctime,zlinkname,zbufo);
/*
CREATE TABLE entries(name TEXT PRIMARY KEY, type TEXT, inode INT, mode INT, nlink INT, uid INT, gid INT, size INT, blksize INT, blocks INT, atime INT, mtime INT, ctime INT, linkname TEXT, xattrs TEXT, crtime INT, ossint1 INT, ossint2 INT, ossint3 INT, ossint4 INT, osstext1 TEXT, osstext2 TEXT);";
char *esqli = "INSERT INTO entries VALUES (@name,@type,@inode,@mode,@nlink,@uid,@gid,@size,@blksize,@blocks,@atime,@mtime, @ctime,@linkname,@xattrs,@crtime,@ossint1,@ossint2,@ossint3,@ossint4,@osstext1,@osstext2);";
*/
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
    error=sqlite3_bind_text(res,1,zname,-1,SQLITE_TRANSIENT);
    if (error != SQLITE_OK) fprintf(stderr, "SQL insertdbgo bind name: %s error %d err %s\n",pwork->name,error,sqlite3_errmsg(db));
    sqlite3_bind_text(res,2,ztype,-1,SQLITE_TRANSIENT);
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
    sqlite3_bind_text(res,14,zlinkname,-1,SQLITE_TRANSIENT);
    error=sqlite3_bind_text(res,15,zxattr,-1,SQLITE_TRANSIENT);
    if (error != SQLITE_OK) printf("insertdbgo bind xattr: error %d %s %s %s %s\n",error,pwork->name,pwork->type,pwork->xattr,sqlite3_errmsg(db));
    sqlite3_bind_int64(res,16,pwork->crtime);
    sqlite3_bind_int64(res,17,pwork->ossint1);
    sqlite3_bind_int64(res,18,pwork->ossint2);
    sqlite3_bind_int64(res,19,pwork->ossint3);
    sqlite3_bind_int64(res,20,pwork->ossint4);
    error=sqlite3_bind_text(res,21,zosstext1,-1,SQLITE_TRANSIENT);
    error=sqlite3_bind_text(res,22,zosstext1,-1,SQLITE_TRANSIENT);
    sqlite3_free(zname);
    sqlite3_free(ztype);
    sqlite3_free(zlinkname);
    sqlite3_free(zxattr);
    sqlite3_free(zosstext1);
    sqlite3_free(zosstext2);
    error = sqlite3_step(res);
    if (error != SQLITE_ROW) {
          //fprintf(stderr, "SQL error on insertdbgo: error %d err %s\n",error,sqlite3_errmsg(db));
          //return 0;
    }
    sqlite3_clear_bindings(res);
    sqlite3_reset(res); 

    return 0;
}

int insertdbgor(struct work *pwork, sqlite3 *db, sqlite3_stmt *res)
{
    char *err_msg = 0;
    char sqlstmt[MAXSQL];
    int rc;
    int error;
    char *zname;
    char *ztype;
    int i;

    zname = sqlite3_mprintf("%q",pwork->name);
    ztype = sqlite3_mprintf("%q",pwork->type);
    error=sqlite3_bind_text(res,1,zname,-1,SQLITE_TRANSIENT);
    if (error != SQLITE_OK) fprintf(stderr, "SQL insertdbgor bind name: %s error %d err %s\n",pwork->name,error,sqlite3_errmsg(db));
    sqlite3_bind_text(res,2,ztype,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int64(res,3,pwork->statuso.st_ino);
    sqlite3_bind_int64(res,4,pwork->pinode);
    sqlite3_bind_int64(res,5,pwork->suspect);

    sqlite3_free(zname);
    sqlite3_free(ztype);
    error = sqlite3_step(res);
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
    int cnt;
    int found;
    char *err_msg = 0;
    char sqlstmt[MAXSQL];
    int rc;
    int len=0;;
    const char *shortname;
    int depth;
    int i;
    int rectype;

    rectype=0; // directory summary record type
    depth=0;
    i=0;
    while (i < strlen(pwork->name)) {
      if (!strncmp(pwork->name+i,"/",1)) depth++;
      i++;
    } 
    //printf("dbutil insertsum name %s depth %d\n",pwork->name,depth);
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
    if (strlen(shortname) < 1) printf("***** shortname is < 1 %s\n",pwork->name);

/*
CREATE TABLE summary(name TEXT PRIMARY KEY, type TEXT, inode INT, mode INT, nlink INT, uid INT, gid INT, size INT, blksize INT, blocks INT, atime INT, mtime INT, ctime INT, linkname TEXT, xattrs TEXT, totfiles INT, totlinks INT, minuid INT, maxuid INT, mingid INT, maxgid INT, minsize INT, maxsize INT, totltk INT, totmtk INT, totltm INT, totmtm INT, totmtg INT, totmtt INT, totsize INT, minctime INT, maxctime INT, minmtime INT, maxmtime INT, minatime INT, maxatime INT, minblocks INT, maxblocks INT, totxattr INT,depth INT, mincrtime INT, maxcrtime INT, minossint1 INT, maxossint1 INT, totossint1 INT, minossint2 INT, maxossint2, totossint2 INT, minossint3 INT, maxossint3, totossint3 INT,minossint4 INT, maxossint4 INT, totossint4 INT, rectype INT, pinode INT);
*/
    char *zname = sqlite3_mprintf("%q",shortname);
    char *ztype = sqlite3_mprintf("%q",pwork->type);
    char *zlinkname = sqlite3_mprintf("%q",pwork->linkname);
    char *zxattr = sqlite3_mprintf("%q",pwork->xattr);
    sprintf(sqlstmt,"INSERT INTO summary VALUES (\'%s\', \'%s\',%lld, %d, %d, %d, %d, %lld, %d, %lld, %ld, %ld, %ld, \'%s\', \'%s\', %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %lld);",
       zname, ztype, pwork->statuso.st_ino,pwork->statuso.st_mode,pwork->statuso.st_nlink,pwork->statuso.st_uid,pwork->statuso.st_gid,pwork->statuso.st_size,pwork->statuso.st_blksize,pwork->statuso.st_blocks,pwork->statuso.st_atime,pwork->statuso.st_mtime,pwork->statuso.st_ctime,zlinkname,zxattr,su->totfiles, su->totlinks, su->minuid, su->maxuid, su->mingid, su->maxgid, su->minsize, su->maxsize, su->totltk, su->totmtk, su->totltm, su->totmtm, su->totmtg, su->totmtt, su->totsize, su->minctime, su->maxctime, su->minmtime, su->maxmtime, su->minatime, su->maxatime, su->minblocks, su->maxblocks,su->totxattr,depth, su->mincrtime, su->maxcrtime, su->minossint1, su->maxossint1, su->totossint1, su->minossint2, su->maxossint2, su->totossint2, su->minossint3, su->maxossint3, su->totossint3, su->minossint4,su->maxossint4, su->totossint4, rectype, pwork->pinode);
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
    int cnt;
    int found;
    char *err_msg = 0;
    char sqlstmt[MAXSQL];
    int rc;
    int len=0;;
    int depth;
    int i;

    depth=0;
    i=0;
    while (i < strlen(name)) {
      if (!strncmp(name+i,"/",1)) depth++;
      i++;
    } 
    sprintf(sqlstmt,"INSERT INTO treesummary VALUES (%lld, %lld, %lld, %lld,%lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %d, %d);",
       su->totsubdirs, su->maxsubdirfiles, su->maxsubdirlinks, su->maxsubdirsize,su->totfiles, su->totlinks, su->minuid, su->maxuid, su->mingid, su->maxgid, su->minsize, su->maxsize, su->totltk, su->totmtk, su->totltm, su->totmtm, su->totmtg, su->totmtt, su->totsize, su->minctime, su->maxctime, su->minmtime, su->maxmtime, su->minatime, su->maxatime, su->minblocks, su->maxblocks,su->totxattr,depth,su->mincrtime, su->maxcrtime, su->minossint1, su->maxossint1, su->totossint1, su->minossint2, su->maxossint2, su->totossint2, su->minossint3, su->maxossint3, su->totossint3, su->minossint4,su->maxossint4, su->totossint4, rectype, uid, gid);

    rc = sqlite3_exec(sdb, sqlstmt, 0, 0, &err_msg);
    if (rc != SQLITE_OK ) {
        fprintf(stderr, "SQL error on insert (treesummary): %s\n", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    return 0;
}

static void uidtouser(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    struct passwd *fmypasswd;
    int fuid;
    char fname[256];
    if (argc == 1) {
        const unsigned char *text = sqlite3_value_text(argv[0]);
        //printf("uidtouser %d %s\n",argc,text);
        sprintf(fname,"%s",text);
        fuid=atoi(fname);
        fmypasswd = getpwuid(fuid);
        if (fmypasswd != NULL) {
          sqlite3_result_text(context, fmypasswd->pw_name, -1, SQLITE_TRANSIENT);
          return;
        }
    }
    sqlite3_result_null(context);
}

static void gidtogroup(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    struct group *fmygroup;
    int fgid;
    char fgroup[256];
    if (argc == 1) {
        const unsigned char *text = sqlite3_value_text(argv[0]);
        //printf("gidtouser %d %s\n",argc,text);
        sprintf(fgroup,"%s",text);
        fgid=atoi(fgroup);
        fmygroup = getgrgid(fgid);
        if (fmygroup != NULL) {
          sqlite3_result_text(context, fmygroup->gr_name, -1, SQLITE_TRANSIENT);
          return;
        }
    }
    sqlite3_result_null(context);
}

static void path(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    int mytid;
 
    mytid=gettid(); 
    sqlite3_result_text(context, gps[mytid].gpath, -1, SQLITE_TRANSIENT);
    return;
}

static void epath(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    int mytid;
 
    mytid=gettid(); 
    sqlite3_result_text(context, gps[mytid].gepath, -1, SQLITE_TRANSIENT);
    return;
}

static void modetotxt(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    int fmode;
    char tmode[64];
    if (argc == 1) {
        const unsigned char *text = sqlite3_value_text(argv[0]);
        sprintf(tmode,"%s",text);
        fmode=atoi(tmode);
        sprintf(tmode,"----------");
        if (fmode &  S_IFDIR) tmode[0] = 'd';
        if (fmode &  S_IRUSR) tmode[1] = 'r';
        if (fmode &  S_IWUSR) tmode[2] = 'w';
        if (fmode &  S_IXUSR) tmode[3] = 'x';
        if (fmode &  S_IRUSR) tmode[4] = 'r';
        if (fmode &  S_IWUSR) tmode[5] = 'w';
        if (fmode &  S_IXUSR) tmode[6] = 'x';
        if (fmode &  S_IRUSR) tmode[7] = 'r';
        if (fmode &  S_IWUSR) tmode[8] = 'w';
        if (fmode &  S_IXUSR) tmode[9] = 'x';
        sqlite3_result_text(context, tmode, -1, SQLITE_TRANSIENT);
        return;
    }
    sqlite3_result_null(context);
}

int addqueryfuncs(sqlite3 *db) {
       sqlite3_create_function(db, "uidtouser", 1, SQLITE_UTF8, NULL, &uidtouser, NULL, NULL);
       sqlite3_create_function(db, "gidtogroup", 1, SQLITE_UTF8, NULL, &gidtogroup, NULL, NULL);
       sqlite3_create_function(db, "path", 0, SQLITE_UTF8, NULL, &path, NULL, NULL);
       sqlite3_create_function(db, "epath", 0, SQLITE_UTF8, NULL, &epath, NULL, NULL);
       sqlite3_create_function(db, "modetotxt", 1, SQLITE_UTF8, NULL, &modetotxt, NULL, NULL);
       return 0;
}
