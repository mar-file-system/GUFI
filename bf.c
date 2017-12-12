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
char *vssqlgroup = "create view vsummarygroup as select * from summary where rectype=2;";
char *vtssqldir = "create view vtsummarydir as select * from treesummary where rectype=0;";
char *vtssqluser = "create view vtsummaryuser as select * from treesummary where rectype=1;";
char *vtssqlgroup = "create view vtsummarygroup as select * from treesummary where rectype=2;";







void print_help(const char* prog_name,
                const char* getopt_str,
                const char* positional_args_help_str) {

   // "hxpPin:d:o:"
   const char* opt = getopt_str;
   if (! opt)
      return;
   
   printf("Usage: %s [options] %s\n", prog_name, positional_args_help_str);
   printf("options:\n");

   int ch;
   while ((ch = *opt++)) {
      switch (ch) {
      case ':': continue;
      case 'h': printf("  -h              help\n"); break;
      case 'H': printf("  -H              show assigned input values (debugging)\n"); break;
      case 'x': printf("  -x              pull xattrs from source file-sys into GUFI\n"); break;
      case 'p': printf("  -p              print files as they are encountered\n"); break;
      case 'P': printf("  -P              print directories as they are encountered\n"); break;
      case 's': printf("  -s              generate tree-summary DB\n"); break;
      case 'b': printf("  -b              build GUFI index tree\n"); break;
      case 'a': printf("  -a              AND/OR (SQL query combination)\n"); break;
      case 'n': printf("  -n <threads>    number of threads\n"); break;
      case 'd': printf("  -d <delim>      delimiter (one char)\n"); break;
      case 'i': printf("  -i <input_dir>  input directory path\n"); break;
      case 't': printf("  -t <to_dir>     dir where GUFI-tree should be built\n"); break;
      case 'o': printf("  -o <out_fname>  output file (one-per-thread, with thread-id suffix)\n"); break;
      case 'O': printf("  -O <out_DB>     output DB\n"); break;
      case 'I': printf("  -I <SQL_init>   SQL init\n"); break;
      case 'T': printf("  -T <SQL_tsum>   SQL for tree-summary table\n"); break;
      case 'S': printf("  -S <SQL_sum>    SQL for summary table\n"); break;
      case 'E': printf("  -E <SQL_ent>    SQL for entries table\n"); break;
      case 'F': printf("  -F <SQL_fin>    SQL cleanup\n"); break;
      case 'r':
         printf("  -r <robin_in>   custom RobinHood parameters\n");
         printf("                     example contents:\n");
         printf("                     /top - top directory pathname - rh doesnt have a name for the root\n");
         printf("                     0x200004284:0x11:0x0 - fid of the root\n");
         printf("                     20004284110 -  inode of root\n");
         printf("                     16877 - mode of root\n");
         printf("                     1500000000  - atime=mtime=ctime of root\n");
         printf("                     localhost - host of mysql\n");
         printf("                     ggrider - user of mysql\n");
         printf("                     ggriderpw1 - password of mysql\n");
         printf("                     institutes - name of db of mysql\n");
         break;

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


// process command-line options
//
// <positional_args_help_str> is a string like "<input_dir> <output_dir>"
//    which is placed after "[options]" in the output produced for the '-h'
//    option.  In other words, you end up with a picture of the
//    command-line, for the help text.
//
// return: -1 for error.  Otherwise, we return the index of the end of
//    options in <argv>.  This allows the caller to treat remaining
//    arguments as required (positional) args.
//   
int processin(int         argc,
              char*       argv[],
              const char* getopt_str,
              int         n_positional,
              const char* positional_args_help_str) {

#define INSTALL_STR(VAR, SOURCE, MAX, OPT_LETTER)                       \
     do {                                                                 \
        if (strlen(optarg) >= (MAX)) {                                  \
           fprintf(stderr, "argument to '-%c' exceeds max allowed (%d)\n", (OPT_LETTER), (MAX)); \
           retval = -1;                                                 \
        }                                                               \
        strncpy((VAR), optarg, (MAX));                                  \
        (VAR)[(MAX)-1] = 0;                                             \
     } while (0)

     char outfn[MAXPATH];
     int i;

     //bzero(in.sqlent,sizeof(in.sqlent));
     // <in> defaults to all-zeros.
     in.maxthreads = 1;         // don't default to zero threads

     int show = 0;
     int retval = 0;
     int helped = 0;
     int ch;
     while ( (ch = getopt(argc, argv, getopt_str)) != -1) {
        switch (ch) {

        case 'h':               // help
           print_help(argv[0], getopt_str, positional_args_help_str);
           helped = 1;
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
           // strncpy(in.outfilen, optarg, MAXPATH);
           INSTALL_STR(in.outfilen, optarg, MAXPATH, 'o');
           break;

        case 'O':
           in.outdb = 1;
           // strncpy(in.outdbn, optarg, MAXPATH);
           INSTALL_STR(in.outdbn, optarg, MAXPATH, 'O');
           break;

        case 't':
           INSTALL_STR(in.nameto, optarg, MAXPATH, 't');
           break;

        case 'i':
           INSTALL_STR(in.name, optarg, MAXPATH, 'i');
           break;

        case 'I':               // SQL initializations
           INSTALL_STR(in.sqlinit, optarg, MAXSQL, 'I');
           break;

        case 'T':               // SQL for tree-summary
           INSTALL_STR(in.sqltsum, optarg, MAXSQL, 'T');
           break;

        case 'S':               // SQL for summary
           INSTALL_STR(in.sqlsum, optarg, MAXSQL, 'S');
           break;

        case 'E':               // SQL for entries
           INSTALL_STR(in.sqlent, optarg, MAXSQL, 'E');
           break;

        case 'F':               // SQL clean-up
           INSTALL_STR(in.sqlfin, optarg, MAXSQL, 'F');
           break;

        case 'r':               // robinhood mysql input control file
           INSTALL_STR(in.robinin, optarg, MAXPATH, 'r');
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

     // caller requires given number of positional args, after the options.
     // <optind> is the number of argv[] values that were recognized as options.
     if ((argc - optind) < n_positional) {
        if (! helped) {
           print_help(argv[0], getopt_str, positional_args_help_str);
           helped = 1;
        }
        retval = -1;
     }


     // DEBUGGING:
     if (show)
        show_input(&in, retval);

     return (retval ? retval : optind);

#undef INSTALL_STR

}
