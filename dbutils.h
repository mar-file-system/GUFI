#ifndef DBUTILS_H
#define DBUTILS_H

#include <sqlite3.h>
#include <sys/stat.h>

#include "utils.h"


sqlite3 *  attachdb(const char *name, sqlite3 *db, char *dbn);

sqlite3 *  detachdb(const char *name, sqlite3 *db, char *dbn);

sqlite3 *  opendb(const char *name, sqlite3 *db, int openwhat, int createtables);

int rawquerydb(const char *name, int isdir, sqlite3 *db, char *sqlstmt,int printpath, int printheader, int printing, int ptid);

int querytsdb(const char *name, struct sum *sumin, sqlite3 *db, int *recs,int ts);

int startdb(sqlite3 *db);

int stopdb(sqlite3 *db);

int closedb(sqlite3 *db);

int insertdbfin(sqlite3 *db,sqlite3_stmt *res);

sqlite3_stmt * insertdbprep(sqlite3 *db,sqlite3_stmt *res);

int insertdbgo(struct work *pwork, sqlite3 *db, sqlite3_stmt *res);

int insertsumdb(sqlite3 *sdb, struct work *pwork,struct sum *su);

int inserttreesumdb(const char *name, sqlite3 *sdb, struct sum *su,int rectype,int uid,int gid);


#endif
