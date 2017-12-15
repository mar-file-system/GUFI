#ifndef BF_H
#define BF_H

#include <unistd.h>
#include <sys/stat.h>
#include <pthread.h>            // thpool.h expects us to do this

#define MAXPATH 1024
#define MAXXATTR 1024
#define MAXSQL 2048 
#define MAXRECS 100000
#define MAXPTHREAD 100


struct globalpathstate {
  char gpath[MAXPATH];
  char gepath[MAXPATH];
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
   int  printdir;
   int  printing;
   int  printheader;
   int  printrows;
   int  helped;                 // support parsing of per-app sub-options
   int  dodelim;
   char delim[2];
   int  doxattrs;
   int  buildindex;
   int  maxthreads;
   int  writetsum;
   int  outfile;
   char outfilen[MAXPATH];
   int  andor;
   int  outdb;
   char outdbn[MAXPATH];
   char sqlinit[MAXSQL];
   char sqlfin[MAXSQL];
};
extern struct input in;


void print_help(const char* prog_name,
                const char* getopt_str,
                const char* positional_args_help_str);

// DEBUGGING
void show_input(struct input* in, int retval);

int parse_cmd_line(int         argc,
                   char*       argv[],
                   const char* getopt_str,
                   int         n_positional,
                   const char* positional_args_help_str);

// help for parsing a cmd-line string-argument into a fixed-size array.
// NOTE: This assumes you have a variable <retval> in scope.
#define INSTALL_STR(VAR, SOURCE, MAX, ARG_NAME)                         \
   do {                                                                 \
      const char* src = (SOURCE); /* might be "argv[idx++]" */          \
      if (strlen(src) >= (MAX)) {                                    \
         fprintf(stderr, "argument '%s' exceeds max allowed (%d)\n",    \
                 (ARG_NAME), (MAX));                                    \
         retval = -1;                                                   \
      }                                                                 \
      strncpy((VAR), (src), (MAX));                                     \
      (VAR)[(MAX)-1] = 0;                                               \
   } while (0)


#define INSTALL_INT(VAR, SOURCE, MIN, MAX, ARG_NAME)                    \
   do {                                                                 \
      (VAR) = atoi(SOURCE); /* might be "argv[idx++]" */                \
      if (((VAR) < (MIN)) || ((VAR) > (MAX))) {                         \
         fprintf(stderr, "argument '%s' not in range [%d,%d]\n",        \
                 (ARG_NAME), (MIN), (MAX));                             \
         retval = -1;                                                   \
      }                                                                 \
   } while (0)


// TBD: this would replace gotos
// local impls of processdir() could use this to manage clean-up tasks.
typedef enum {
   DO_FREE   = 0x01,
   CLOSE_DIR = 0x02,
   CLOSE_DB  = 0x04,
} CleanUpTasks;


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
   char          pinodec[128];
};

extern char xattrdelim[];
extern char fielddelim[];


#endif
