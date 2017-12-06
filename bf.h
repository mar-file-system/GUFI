#include <unistd.h>

#define MAXPATH 1024
#define MAXXATTR 1024
#define MAXSQL 1024
#define MAXRECS 100000
#define MAXPTHREAD 100

#include "thpool.h"
#include "thpool.c"
threadpool mythpool;

volatile int runningthreads = 0;
pthread_mutex_t running_mutex = PTHREAD_MUTEX_INITIALIZER;
volatile int queuelock = 0;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
volatile int sumlock = 0;
struct sum sumout;
pthread_mutex_t sum_mutex = PTHREAD_MUTEX_INITIALIZER;

// global variable to hold per thread state goes here
struct globalthreadstate {
FILE *outfd[MAXPTHREAD];
sqlite3 *outdbd[MAXPTHREAD];
} gts;

struct globalpathstate {
  char gpath[MAXPATH];
} gps[MAXPTHREAD];

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
int printdir;
int andor;
int printing;
int dodelim;
char delim[2];
int doxattrs;
int buildindex;
int maxthreads;
int writetsum;
int outfile;
char outfilen[MAXPATH];
int outdb;
char outdbn[MAXPATH];
char sqlinit[MAXSQL];
char sqlfin[MAXSQL];
} in;

struct work {
char name[MAXPATH];
char type[2];
char nameto[MAXPATH];
char linkname[MAXPATH];
struct stat statuso;
long long int pinode;
long long int offset;
int xattrs;
char xattr[MAXXATTR];
void * freeme;
int crtime;
int ossint1;
int ossint2;
int ossint3;
int ossint4;
char osstext1[MAXXATTR];
char osstext2[MAXXATTR];
char pinodec[128];
};

char xattrdelim[] = "\x1F";
char fielddelim[] = "\x1E";

char *esql = "DROP TABLE IF EXISTS entries;"
   "CREATE TABLE entries(name TEXT PRIMARY KEY, type TEXT, inode INT64, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattrs TEXT, crtime INT64, ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, osstext1 TEXT, osstext2 TEXT);";
char *esqli = "INSERT INTO entries VALUES (@name,@type,@inode,@mode,@nlink,@uid,@gid,@size,@blksize,@blocks,@atime,@mtime, @ctime,@linkname,@xattrs,@crtime,@ossint1,@ossint2,@ossint3,@ossint4,@osstext1,@osstext2);";

char *ssql = "DROP TABLE IF EXISTS summary;"
   "CREATE TABLE summary(name TEXT PRIMARY KEY, type TEXT, inode INT64, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattrs TEXT, totfiles INT64, totlinks INT64, minuid INT64, maxuid INT64, mingid INT64, maxgid INT64, minsize INT64, maxsize INT64, totltk INT64, totmtk INT64, totltm INT64, totmtm INT64, totmtg INT64, totmtt INT64, totsize INT64, minctime INT64, maxctime INT64, minmtime INT64, maxmtime INT64, minatime INT64, maxatime INT64, minblocks INT64, maxblocks INT64, totxattr INT64,depth INT64, mincrtime INT64, maxcrtime INT64, minossint1 INT64, maxossint1 INT64, totossint1 INT64, minossint2 INT64, maxossint2 INT64, totossint2 INT64, minossint3 INT64, maxossint3 INT64, totossint3 INT64,minossint4 INT64, maxossint4 INT64, totossint4 INT64, rectype INT64, pinode INT64);";

char *tsql = "DROP TABLE IF EXISTS treesummary;"
   "CREATE TABLE treesummary(totsubdirs INT64, maxsubdirfiles INT64, maxsubdirlinks INT64, maxsubdirsize INT64, totfiles INT64, totlinks INT64, minuid INT64, maxuid INT64, mingid INT64, maxgid INT64, minsize INT64, maxsize INT64, totltk INT64, totmtk INT64, totltm INT64, totmtm INT64, totmtg INT64, totmtt INT64, totsize INT64, minctime INT64, maxctime INT64, minmtime INT64, maxmtime INT64, minatime INT64, maxatime INT64, minblocks INT64, maxblocks INT64, totxattr INT64,depth INT64, mincrtime INT64, maxcrtime INT64, minossint1 INT64, maxossint1 INT64, totossint1 INT64, minossint2 INT64, maxossint2 INT64, totossint2 INT64, minossint3 INT64, maxossint3 INT64, totossint3 INT64, minossint4 INT64, maxossint4 INT64, totossint4 INT64,rectype INT64, uid INT64, gid INT64);";

char *vesql = "create view pentries as select entries.*, summary.inode as pinode from entries, summary where rectype=0;";

char *vssqldir = "create view vsummarydir as select * from summary where rectype=0;";
char *vssqluser = "create view vsummaryuser as select * from summary where rectype=1;";
char *vtssqldir = "create view vtsummarydir as select * from treesummary where rectype=0;";
char *vtssqluser = "create view vtsummaryuser as select * from treesummary where rectype=1;";

