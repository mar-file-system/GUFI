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



#include <pwd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "bf.h"
#include "utils.h"

extern char *optarg;
extern int optind, opterr, optopt;

char fielddelim[] = "\x1E";     // ASCII Record Separator

struct globalpathstate gps[MAXPTHREAD] = {};

struct input in = {};



void print_help(const char* prog_name,
                const char* getopt_str,
                const char* positional_args_help_str) {

   // "hxpPin:d:o:"
   const char* opt = getopt_str;
   if (! opt)
      return;

   printf("usage: %s [options] %s\n", prog_name, positional_args_help_str);
   printf("options:\n");

   int ch;
   while ((ch = *opt++)) {
      switch (ch) {
      case ':': continue;
      case 'h': printf("  -h                     help"); break;
      case 'H': printf("  -H                     show assigned input values (debugging)"); break;
      case 'x': printf("  -x                     enable external database processing"); break;
      case 'p': printf("  -p                     print file-names"); break;
      case 'P': printf("  -P                     print directories as they are encountered"); break;
      case 'N': printf("  -N                     print column-names (header) for DB results"); break;
      case 'V': printf("  -V                     print column-values (rows) for DB results"); break;
      case 's': printf("  -s                     generate tree-summary table (in top-level DB)"); break;
      case 'b': printf("  -b                     build GUFI index tree"); break;
      case 'a': printf("  -a                     AND/OR (SQL query combination)"); break;
      case 'n': printf("  -n <threads>           number of threads"); break;
      case 'd': printf("  -d <delim>             delimiter (one char)  [use 'x' for 0x%02X]", (uint8_t)fielddelim[0]); break;
      case 'i': printf("  -i <input_dir>         input directory path"); break;
      case 't': printf("  -t <to_dir>            build GUFI index (under) here"); break;
      case 'o': printf("  -o <out_fname>         output file (one-per-thread, with thread-id suffix)"); break;
      case 'O': printf("  -O <out_DB>            output DB"); break;
      case 'I': printf("  -I <SQL_init>          SQL init"); break;
      case 'T': printf("  -T <SQL_tsum>          SQL for tree-summary table"); break;
      case 'S': printf("  -S <SQL_sum>           SQL for summary table"); break;
      case 'E': printf("  -E <SQL_ent>           SQL for entries table"); break;
      case 'F': printf("  -F <SQL_fin>           SQL cleanup"); break;
      case 'r': printf("  -r                     insert files and links into db (for bfwreaddirplus2db"); break;
      case 'R': printf("  -R                     insert dires into db (for bfwreaddirplus2db"); break;
      case 'D': printf("  -D                     don't descend the tree"); break;
      case 'Y': printf("  -Y                     default to all directories suspect"); break;
      case 'Z': printf("  -Z                     default to all files/links suspect"); break;
      case 'W': printf("  -W <INSUSPECT>         suspect input file"); break;
      case 'A': printf("  -A <suspectmethod>     suspect method (0 no suspects, 1 suspect file_dfl, 2 suspect stat d and file_fl, 3 suspect stat_dfl"); break;
      case 'g': printf("  -g <stridesize>        stride size for striping inodes"); break;
      case 'c': printf("  -c <suspecttime>       time in seconds since epoch for suspect comparision"); break;
      case 'u': printf("  -u                     input mode is from a file so input is a file not a dir"); break;
      case 'y': printf("  -y <min level>         minimum level to go down"); break;
      case 'z': printf("  -z <max level>         maximum level to go down"); break;
      case 'J': printf("  -J <SQL_interm>        SQL for intermediate results"); break;
      case 'K': printf("  -K <create aggregate>  SQL to create the final aggregation table"); break;
      case 'G': printf("  -G <SQL_aggregate>     SQL for aggregated results"); break;
      case 'm': printf("  -m                     Keep mtime and atime same on the database files"); break;
      case 'B': printf("  -B <buffer size>       size of each thread's output buffer in bytes"); break;
      case 'w': printf("  -w                     open the database files in read-write mode instead of read only mode"); break;
      case 'f': printf("  -f <FORMAT>            use the specified FORMAT instead of the default; output a newline after each use of FORMAT"); break;
      case 'j': printf("  -j                     print the information in terse form"); break;
      case 'X': printf("  -X                     Dry run"); break;
      case 'L': printf("  -L <count>             Highest number of files/links in a directory allowed to be rolled up"); break;
      case 'k': printf("  -k <filename>          file containing directory names to skip"); break;

      default: printf("print_help(): unrecognized option '%c'", (char)ch);
      }
      printf("\n");
   }
   printf("\n");
}

// DEBUGGING
void show_input(struct input* in, int retval) {
   printf("in.printing           = %d\n",    in->printing);
   printf("in.printdir           = %d\n",    in->printdir);
   printf("in.printheader        = %d\n",    in->printheader);
   printf("in.printrows          = %d\n",    in->printrows);
   printf("in.writetsum          = %d\n",    in->writetsum);
   printf("in.buildindex         = %d\n",    in->buildindex);
   printf("in.maxthreads         = %d\n",    in->maxthreads);
   printf("in.dodelim            = %d\n",    in->dodelim);
   printf("in.delim              = '%s'\n",  in->delim);
   printf("in.name               = '%s'\n",  in->name);
   printf("in.name_len           = '%zu'\n", in->name_len);
   printf("in.nameto             = '%s'\n",  in->nameto);
   printf("in.andor              = %d\n",    in->andor);
   printf("in.external_enabled   = %d\n",    in->external_enabled);
   printf("in.nobody.uid         = %d\n",    (int) in->nobody.uid);
   printf("in.nobody.gid         = %d\n",    (int) in->nobody.gid);
   printf("in.sql.init           = '%s'\n",  in->sql.init);
   printf("in.sql.tsum           = '%s'\n",  in->sql.tsum);
   printf("in.sql.sum            = '%s'\n",  in->sql.sum);
   printf("in.sql.ent            = '%s'\n",  in->sql.ent);
   printf("in.sql.fin            = '%s'\n",  in->sql.fin);
   printf("in.insertdir          = '%d'\n",  in->insertdir);
   printf("in.insertfl           = '%d'\n",  in->insertfl);
   printf("in.dontdescend        = '%d'\n",  in->dontdescend);
   printf("in.suspectd           = '%d'\n",  in->suspectd);
   printf("in.suspectfl          = '%d'\n",  in->suspectfl);
   printf("in.insuspect          = '%s'\n",  in->insuspect);
   printf("in.suspectfile        = '%d'\n",  in->suspectfile);
   printf("in.suspectmethod      = '%d'\n",  in->suspectmethod);
   printf("in.suspecttime        = '%d'\n",  in->suspecttime);
   printf("in.stride             = '%d'\n",  in->stride);
   printf("in.infile             = '%d'\n",  in->infile);
   printf("in.min_level          = %zu\n",   in->min_level);
   printf("in.max_level          = %zu\n",   in->max_level);
   printf("in.sql.intermediate   = '%s'\n",  in->sql.intermediate);
   printf("in.sql.init_agg       = '%s'\n",  in->sql.init_agg);
   printf("in.sql.agg            = '%s'\n",  in->sql.agg);
   printf("in.keep_matime        = %d\n",    in->keep_matime);
   printf("in.output_buffer_size = %zu\n",   in->output_buffer_size);
   printf("in.open_flags         = %d\n",    in->open_flags);
   printf("in.format_set         = %d\n",    in->format_set);
   printf("in.format             = '%s'\n",  in->format);
   printf("in.terse              = %d\n",    in->terse);
   printf("in.dry_run            = %d\n",    in->dry_run);
   printf("in.max_in_dir         = %zu\n",   in->max_in_dir);
   printf("in.skip               = %s\n",    in->skip);
   printf("\n");
   printf("retval                = %d\n",    retval);
   printf("\n");
}

// process command-line options
//
// <positional_args_help_str> is a string like "input_dir to_dir"
//    which is placed after "[options]" in the output produced for the '-h'
//    option.  In other words, you end up with a picture of the
//    command-line, for the help text.
//
// return: -1 for error.  Otherwise, we return the index of the first
//    positional argument in <argv>.  This allows the caller to do a custom
//    parse of the remaining arguments as required (positional) args.
//
int parse_cmd_line(int         argc,
                   char*       argv[],
                   const char* getopt_str,
                   int         n_positional,
                   const char* positional_args_help_str,
                   struct input *in) {
   memset(in, 0, sizeof(*in));
   in->maxthreads         = 1;                      // don't default to zero threads
   SNPRINTF(in->delim, sizeof(in->delim), fielddelim);
   in->max_level          = -1;                     // default to all the way down
   in->nobody.uid  = 65534;
   in->nobody.gid  = 65534;
   struct passwd *passwd = getpwnam("nobody");
   if (passwd) {
       in->nobody.uid = passwd->pw_uid;
       in->nobody.gid = passwd->pw_gid;
   }
   in->output             = STDOUT;
   in->output_buffer_size = 4096;
   in->open_flags         = SQLITE_OPEN_READONLY;   // default to read-only opens
   in->max_in_dir         = (size_t) -1;

   int show   = 0;
   int retval = 0;
   int ch;
   optind = 1; // reset to 1, not 0 (man 3 getopt)
   while ( (ch = getopt(argc, argv, getopt_str)) != -1) {
      switch (ch) {

      case 'h':               // help
         print_help(argv[0], getopt_str, positional_args_help_str);
         in->helped = 1;
         retval = -1;
         break;

      case 'H':               // show parsed inputs
         show = 1;
         retval = -1;
         break;

      case 'x':               // enable external database processing
         in->external_enabled = 1;
         break;

      case 'p':               // print file name/path?
         in->printing = 1;
         break;

      case 'P':               // print dirs?
         in->printdir = 1;
         break;

      case 'N':               // print DB-result column-names?  (i.e. header)
         in->printheader = 1;
         break;

      case 'V':               // print DB-result row-values?
         in->printrows = 1;
         break;

      case 's':               // generate tree-summary table?
         in->writetsum = 1;
         break;

      case 'b':               // build index?
         in->buildindex = 1;
         break;

      case 'a':               // and/or
         in->andor = 1;
         break;

      case 'n':
         INSTALL_INT(in->maxthreads, optarg, 1, MAXPTHREAD, "-n");
         break;

      case 'd':
         if (optarg[0] == 'x') {
             in->dodelim = 2;
             in->delim[0] = fielddelim[0];
         }
         else {
             in->dodelim = 1;
             in->delim[0] = optarg[0];
         }
         break;

      case 't':
         INSTALL_STR(in->nameto, optarg, MAXPATH, "-t");
         in->nameto_len = strlen(in->nameto);
         break;

      case 'i':
         INSTALL_STR(in->name, optarg, MAXPATH, "-i");
         in->name_len = strlen(in->name);
         break;

      case 'o':
         in->output = OUTFILE;
         INSTALL_STR(in->outname, optarg, MAXPATH, "-o");
         in->outname_len = strlen(in->outname);
         break;

      case 'O':
         in->output = OUTDB;
         INSTALL_STR(in->outname, optarg, MAXPATH, "-O");
         in->outname_len = strlen(in->outname);
         break;

      case 'I':               // SQL initializations
         INSTALL_STR(in->sql.init, optarg, MAXSQL, "-I");
         in->sql.init_len = strlen(in->sql.init);
         break;

      case 'T':               // SQL for tree-summary
         INSTALL_STR(in->sql.tsum, optarg, MAXSQL, "-T");
         in->sql.tsum_len = strlen(in->sql.tsum);
         break;

      case 'S':               // SQL for summary
         INSTALL_STR(in->sql.sum, optarg, MAXSQL, "-S");
         in->sql.sum_len = strlen(in->sql.sum);
         break;

      case 'E':               // SQL for entries
         INSTALL_STR(in->sql.ent, optarg, MAXSQL, "-E");
         in->sql.ent_len = strlen(in->sql.ent);
         break;

      case 'F':               // SQL clean-up
         INSTALL_STR(in->sql.fin, optarg, MAXSQL, "-F");
         in->sql.fin_len = strlen(in->sql.fin);
         break;

      case 'r':               // insert files and links into db for bfwreaddirplus2db
         in->insertfl = 1;
         break;

      case 'R':               // insert dirs into db for bfwreaddirplus2db
         in->insertdir = 1;
         break;

      case 'D':               // default is 0
         in->dontdescend = 1;
         break;

      case 'Y':               // default is 0
         in->suspectd = 1;
         break;

      case 'Z':               // default is 0
         in->suspectfl = 1;
         break;

      case 'W':               // SQL clean-up
         INSTALL_STR(in->insuspect, optarg, MAXPATH, "-W");
         in->suspectfile = 1;
         break;

      case 'A':
         INSTALL_INT(in->suspectmethod, optarg, 1, 4, "-A");
         break;

      case 'g':
         INSTALL_INT(in->stride, optarg, 1, MAXSTRIDE, "-g");
         break;

      case 'c':
         INSTALL_INT(in->suspecttime, optarg, 1, 2147483646, "-c");
         break;

      case 'u':
         in->infile = 1;
         break;

      case 'y':
         INSTALL_UINT(in->min_level, optarg, (size_t) 0, (size_t) -1, "-y");
         break;

      case 'z':
         INSTALL_UINT(in->max_level, optarg, (size_t) 0, (size_t) -1, "-z");
         break;

      case 'J':
         INSTALL_STR(in->sql.intermediate, optarg, MAXSQL, "-J");
         in->sql.intermediate_len = strlen(in->sql.intermediate);
         break;

      case 'K':
         INSTALL_STR(in->sql.init_agg, optarg, MAXSQL, "-K");
         in->sql.init_agg_len = strlen(in->sql.init_agg);
         break;

      case 'G':
         INSTALL_STR(in->sql.agg, optarg, MAXSQL, "-G");
         in->sql.agg_len = strlen(in->sql.agg);
         break;

      case 'm':
         in->keep_matime = 1;
         break;

      case 'B':
         INSTALL_UINT(in->output_buffer_size, optarg, (size_t) 0, (size_t) -1, "-z");
         break;

      case 'w':
         in->open_flags = SQLITE_OPEN_READWRITE;
         break;

      case 'f':
          INSTALL_STR(in->format, optarg, MAXPATH, "-f");
          in->format_set = 1;
          break;

      case 'j':
          in->terse = 1;
          break;

      case 'X':
          in->dry_run = 1;
          break;

      case 'L':
          INSTALL_UINT(in->max_in_dir, optarg, (size_t) 0, (size_t) -1, "-L");
          break;

      case 'k':
          INSTALL_STR(in->skip, optarg, MAXPATH, "-k");
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

   // if there were no other errors,
   // make sure min_level <= max_level
   if (retval == 0) {
       retval = -(in->min_level > in->max_level);
   }

   // caller requires given number of positional args, after the options.
   // <optind> is the number of argv[] values that were recognized as options.
   // NOTE: caller may have custom options ovf their own, so returning a
   //       value > n_positional is okay (?)
   if (retval
       || ((argc - optind) < n_positional)) {
      if (! in->helped) {
         print_help(argv[0], getopt_str, positional_args_help_str);
         in->helped = 1;
      }
      retval = -1;
   }

   // DEBUGGING:
   if (show)
      show_input(in, retval);

   return (retval ? retval : optind);
}
