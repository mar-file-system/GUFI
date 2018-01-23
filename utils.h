#ifndef UTILS_H
#define UTILS_H

#include <sys/stat.h>
#include <pthread.h>
#include <stdio.h>
#include <sqlite3.h>

#include "bf.h"
#include "C-Thread-Pool/thpool.h"

extern threadpool mythpool;

// global variable to hold per thread state goes here
struct globalthreadstate {
   FILE*    outfd[MAXPTHREAD];
   sqlite3* outdbd[MAXPTHREAD];
};
extern struct globalthreadstate gts;

extern struct sum sumout;




int printits(struct work *pwork,int ptid);


int pullxattrs( const char *name, char *bufx);

int zeroit(struct sum *summary);

int sumit (struct sum *summary,struct work *pwork);

int tsumit (struct sum *sumin,struct sum *smout);

// given a possibly-multi-level path of directories (final component is
// also a dir), create the parent dirs all the way down.
// 
int mkpath(char* file_path, mode_t mode);

int dupdir(struct work *pwork);

int incrthread();

int decrthread();

int getqent();

int pushdir( void  * qqwork);

int gettid();

int shortpath(const char *name, char *nameout, char *endname);

int printit(const char *name, const struct stat *status, char *type, char *linkname, int xattrs, char * xattr,int printing, long long pinode);

// NOTE: returns void, not void*, because threadpool threads
//       do not return values outside the API.
typedef void(DirFunc)(void*);

int processdirs(DirFunc dir_fn);


#endif
