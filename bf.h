#ifndef BF_H
#define BF_H

#include <unistd.h>
#include <sys/stat.h>
#include <pthread.h>            // thpool.h expects us to do this

#define MAXPATH 1024
#define MAXXATTR 1024
#define MAXSQL 1024
#define MAXRECS 100000
#define MAXPTHREAD 100


struct sum {
  long long int totfiles;
  long long int totlinks;
  long long int minuid;
  long long int maxuid;
  long long int mingid;
  long long int maxgid;
  long long int minsize;
  long long int maxsize;
  long long int totltk;
  long long int totmtk;
  long long int totltm;
  long long int totmtm;
  long long int totmtg;
  long long int totmtt;
  long long int totsize;
  long long int minctime;
  long long int maxctime;
  long long int minmtime;
  long long int maxmtime;
  long long int minatime;
  long long int maxatime;
  long long int minblocks;
  long long int maxblocks;
  long long int totxattr;
  long long int setit;
  long long int totsubdirs;
  long long int maxsubdirfiles;
  long long int maxsubdirlinks;
  long long int maxsubdirsize;
  long long int mincrtime;
  long long int maxcrtime;
  long long int minossint1;
  long long int maxossint1;
  long long int totossint1;
  long long int minossint2;
  long long int maxossint2;
  long long int totossint2;
  long long int minossint3;
  long long int maxossint3;
  long long int totossint3;
  long long int minossint4;
  long long int maxossint4;
  long long int totossint4;
};

struct input {
   char name[MAXPATH];
   char nameto[MAXPATH];
   char sqltsum[MAXSQL];
   char sqlsum[MAXSQL];
   char sqlent[MAXSQL];
   int  printdir;
   int  andor;
   int  printing;
   int  dodelim;
   char delim[2];
   int  doxattrs;
   int  buildindex;
   int  maxthreads;
   int  writetsum;
   int  outfile;
   char outfilen[MAXPATH];
   int  outdb;
   char outdbn[MAXPATH];
   char sqlinit[MAXSQL];
   char sqlfin[MAXSQL];
};
extern struct input in;

struct work {
   char          name[MAXPATH];
   char          type[2];
   char          nameto[MAXPATH];
   char          linkname[MAXPATH];
   struct stat   statuso;
   long long int pinode;
   long long int offset;
   int           xattrs;
   char          xattr[MAXXATTR];
   void*         freeme;
   int           crtime;
   int           ossint1;
   int           ossint2;
   int           ossint3;
   int           ossint4;
   char          osstext1[MAXXATTR];
   char          osstext2[MAXXATTR];
};

extern char xattrdelim[];
extern char fielddelim[];

extern char *esql;
extern char *esqli;

extern char *ssql;
extern char *tsql;

extern char *vesql;

extern char *vssqldir;
extern char *vssqluser;
extern char *vtssqldir;
extern char *vtssqluser;


#endif
