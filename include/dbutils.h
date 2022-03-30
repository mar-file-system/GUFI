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



#ifndef DBUTILS_H
#define DBUTILS_H

#include <sys/stat.h>
#include <sqlite3.h>

#include "debug.h"
#include "utils.h"

extern char *rsql;
extern char *rsqli;

extern char *esql;
extern char *esqli;

extern char *ssql;
extern char *tsql;

extern char *vesql;

extern char *vssqldir;
extern char *vssqluser;
extern char *vssqlgroup;

extern char *vtssqldir;
extern char *vtssqluser;
extern char *vtssqlgroup;

sqlite3 * attachdb(const char *name, sqlite3 *db, const char *dbn, const int flags);

sqlite3 * detachdb(const char *name, sqlite3 *db, const char *dbn);

int create_table_wrapper(const char *name, sqlite3 * db, const char * sql_name, const char * sql, int (*callback)(void*,int,char**,char**), void * args);

int set_db_pragmas(sqlite3 * db);

sqlite3 * opendb(const char * name, int flags, const int setpragmas, const int load_extensions,
                 int (*modifydb_func)(const char * name, sqlite3 * db, void * args), void * modifydb_args
                 #if defined(DEBUG) && defined(PER_THREAD_STATS)
                 , struct start_end * sqlite3_open,   struct start_end * set_pragmas
                 , struct start_end * load_extension, struct start_end * modify_db
                 #endif
                 );

int rawquerydb(const char *name, int isdir, sqlite3 *db, char *sqlstmt,
               int printpath, int printheader, int printing, int ptid);

int querytsdb(const char *name, struct sum *sumin, sqlite3 *db, int *recs,int ts);

int startdb(sqlite3 *db);

int stopdb(sqlite3 *db);

int closedb(sqlite3 *db);

int insertdbfin(sqlite3_stmt *res);

sqlite3_stmt * insertdbprep(sqlite3 *db);
sqlite3_stmt * insertdbprepr(sqlite3 *db);

int insertdbgo(struct work *pwork, sqlite3 *db, sqlite3_stmt *res);
int insertdbgor(struct work *pwork, sqlite3 *db, sqlite3_stmt *res);

int insertsumdb(sqlite3 *sdb, struct work *pwork,struct sum *su);

int inserttreesumdb(const char *name, sqlite3 *sdb, struct sum *su,int rectype,int uid,int gid);

int addqueryfuncs(sqlite3 *db, size_t id, size_t lvl, char * starting_dir);

size_t print_results(sqlite3_stmt *res, FILE *out, const int printpath, const int printheader, const int printrows, const char *delim);

int get_rollupscore(const char *name, sqlite3 *db, int *rollupscore);

#endif
