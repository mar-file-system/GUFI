#include "bf.h"

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>


char xattrdelim[] = "\x1F";     // ASCII Unit Separator
char fielddelim[] = "\x1E";     // ASCII Record Separator


struct input in = {0};


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







void print_help(const char* prog_name, const char* getopt_str) {

   // "hxpPin:d:o:"
   const char* opt = getopt_str;
   if (! opt)
      return;
   
   printf("Usage: %s [options]\n", prog_name);
   printf("options:\n");

   int ch;
   while ((ch = *opt++)) {
      switch (ch) {
      case ':': continue;
      case 'h': printf("  -h              help\n"); break;
      case 'H': printf("  -H              show assigned input values (debugging)?\n"); break;
      case 'x': printf("  -x              handle xattrs?\n"); break;
      case 'p': printf("  -p              print file diagnostics?\n"); break;
      case 'P': printf("  -P              print directories?\n"); break;
      case 's': printf("  -s              generate tree-summary DB?\n"); break;
      case 'b': printf("  -b              build index?\n"); break;
      case 'a': printf("  -a              AND/OR? (SQL query combination)\n"); break;
      case 'n': printf("  -n <threads>    number of threads\n"); break;
      case 'd': printf("  -d <delim>      delimiter (one char)\n"); break;
      case 'i': printf("  -i <input_dir>  input directory path\n"); break;
      case 'o': printf("  -o <out_fname>  output file\n"); break;
      case 'O': printf("  -O <out_DB>     output DB\n"); break;
      case 't': printf("  -t <name_to>    name to\n"); break;
      case 'I': printf("  -I <SQL_init>   SQL init\n"); break;
      case 'T': printf("  -T <SQL_tsum>   SQL for tree-summary table\n"); break;
      case 'S': printf("  -S <SQL_sum>    SQL for summary table\n"); break;
      case 'E': printf("  -E <SQL_ent>    SQL for entries table\n"); break;
      case 'F': printf("  -F <SQL_fin>    SQL cleanup\n"); break;
      case 'r': printf("  -r <robin_in>   ? (see bfmi.c)\n"); break;

      default: printf("print_help(): unrecognized option '%c'\n", (char)ch);
      }
   }
   printf("\n");
}

// DEBUGGING
void show_input(struct input* in, int retval) {
   printf("in.doxattrs   = %d\n",   in->doxattrs);
   printf("in.printing   = %d\n",   in->printing);
   printf("in.printdir   = %d\n",   in->printdir);
   printf("in.writetsum  = %d\n",   in->writetsum);
   printf("in.buildindex = %d\n",   in->buildindex);
   printf("in.maxthreads = %d\n",   in->maxthreads);
   printf("in.dodelim    = %d\n",   in->dodelim);
   printf("in.delim      = '%s'\n", in->delim);
   printf("in.name       = '%s'\n", in->name);
   printf("in.outfile    = %d\n",   in->outfile);
   printf("in.outfilen   = '%s'\n", in->outfilen);
   printf("in.outdb      = %d\n",   in->outdb);
   printf("in.outdbn     = '%s'\n", in->outdbn);
   printf("in.nameto     = '%s'\n", in->nameto);
   printf("in.andor      = %d\n",   in->andor);
   printf("in.sqlinit    = '%s'\n", in->sqlinit);
   printf("in.sqltsum    = '%s'\n", in->sqltsum);
   printf("in.sqlsum     = '%s'\n", in->sqlsum);
   printf("in.sqlent     = '%s'\n", in->sqlent);
   printf("in.sqlfin     = '%s'\n", in->sqlfin);
   printf("in.robinin    = '%s'\n", in->robinin);
   printf("\n");
   printf("retval        = %d\n", retval);
   printf("\n");
}


// this is where we process input variables
// return 0 for success.
int processin(int argc, char* argv[], const char* getopt_str) {

     char outfn[MAXPATH];
     int i;

     // <in> defaults to all-zeros.
     in.maxthreads = 1;         // don't default to zero threads

     int show = 0;
     int retval = 0;
     int ch;
     while ( (ch = getopt(argc, argv, getopt_str)) != -1) {
        switch (ch) {

        case 'h':               // help
           print_help(argv[0], getopt_str);
           retval = -1;
           break;

        case 'H':               // show parsed inputs
           show = 1;
           retval = -1;
           break;

        case 'x':               // xattrs
           in.doxattrs = 1;
           break;

        case 'p':               // print?
           in.printing = 1;
           break;

        case 'P':               // (cap P)  print dirs?
           in.printdir = 1;
           break;

        case 's':               // generate tree-summary DB?
           in.writetsum = 1;
           break;

        case 'b':               // build index?
           in.buildindex = 1;
           break;

        case 'n':
           in.maxthreads = atoi(optarg);
           break;

        case 'd':
           in.dodelim = 1;
           in.delim[0] = optarg[0];
           break;

        case 'o':
           in.outfile = 1;
           strncpy(in.outfilen, optarg, MAXPATH);
           break;

        case 'O':
           in.outdb = 1;
           strncpy(in.outdbn, optarg, MAXPATH);
           break;

        case 't':
           strncpy(in.nameto, optarg, MAXPATH);
           break;

        case 'i':
           strncpy(in.name, optarg, MAXPATH);
           break;

        case 'I':               // SQL initializations
           strncpy(in.sqlinit, optarg, MAXSQL);
           break;

        case 'T':               // SQL for tree-summary
           strncpy(in.sqltsum, optarg, MAXSQL);
           break;

        case 'S':               // SQL for summary
           strncpy(in.sqlsum, optarg, MAXSQL);
           break;

        case 'E':               // SQL for entries
           strncpy(in.sqlent, optarg, MAXSQL);
           break;

        case 'F':               // SQL clean-up
           strncpy(in.sqlfin, optarg, MAXSQL);
           break;

        case 'r':               // SQL clean-up
           strncpy(in.robinin, optarg, MAXPATH);
           break;

        case 'a':               // and/or
           in.andor = 1;
           break;


        case '?':
            // getopt returns '?' when there is a problem.  In this case it
            // also prints, e.g. "getopt_test: illegal option -- z"
           fprintf(stderr, "unrecognized option '%s'\n", argv[optind -1]);
           retval = -1;
           break;

        default:
           retval = -1;
           fprintf(stderr, "?? getopt returned character code 0%o ??\n", ch);
        };
     }

     // DEBUGGING:
     if (show)
        show_input(&in, retval);
     
     return retval;
}
