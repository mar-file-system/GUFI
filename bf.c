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



#include "bf.h"

#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>


char xattrdelim[] = "\x1F";     // ASCII Unit Separator
char fielddelim[] = "\x1E";     // ASCII Record Separator


struct input in = {0};





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
      case 'p': printf("  -p              print file-names\n"); break;
      case 'P': printf("  -P              print directories as they are encountered\n"); break;
      case 'N': printf("  -N              print column-names (header) for DB results\n"); break;
      case 'V': printf("  -V              print column-values (rows) for DB results\n"); break;
      case 's': printf("  -s              generate tree-summary table (in top-level DB)\n"); break;
      case 'b': printf("  -b              build GUFI index tree\n"); break;
      case 'a': printf("  -a              AND/OR (SQL query combination)\n"); break;
      case 'n': printf("  -n <threads>    number of threads\n"); break;
      case 'd': printf("  -d <delim>      delimiter (one char)  [use 'x' for 0x%02X]\n", (uint8_t)fielddelim[0]); break;
      case 'i': printf("  -i <input_dir>  input directory path\n"); break;
      case 't': printf("  -t <to_dir>     build GUFI-tree (under) here\n"); break;
      case 'o': printf("  -o <out_fname>  output file (one-per-thread, with thread-id suffix)\n"); break;
      case 'O': printf("  -O <out_DB>     output DB\n"); break;
      case 'I': printf("  -I <SQL_init>   SQL init\n"); break;
      case 'T': printf("  -T <SQL_tsum>   SQL for tree-summary table\n"); break;
      case 'S': printf("  -S <SQL_sum>    SQL for summary table\n"); break;
      case 'E': printf("  -E <SQL_ent>    SQL for entries table\n"); break;
      case 'F': printf("  -F <SQL_fin>    SQL cleanup\n"); break;
      case 'r': printf("  -r              insert files and links into db (for bfwreaddirplus2db\n"); break;
      case 'R': printf("  -R              insert dires into db (for bfwreaddirplus2db\n"); break;
      case 'D': printf("  -D              dont descend the tree\n"); break;

      default: printf("print_help(): unrecognized option '%c'\n", (char)ch);
      }
   }
   printf("\n");
}

// DEBUGGING
void show_input(struct input* in, int retval) {
   printf("in.doxattrs    = %d\n",   in->doxattrs);
   printf("in.printing    = %d\n",   in->printing);
   printf("in.printdir    = %d\n",   in->printdir);
   printf("in.printheader = %d\n",   in->printheader);
   printf("in.printrows   = %d\n",   in->printrows);
   printf("in.writetsum   = %d\n",   in->writetsum);
   printf("in.buildindex  = %d\n",   in->buildindex);
   printf("in.maxthreads  = %d\n",   in->maxthreads);
   printf("in.dodelim     = %d\n",   in->dodelim);
   printf("in.delim       = '%s'\n", in->delim);
   printf("in.name        = '%s'\n", in->name);
   printf("in.outfile     = %d\n",   in->outfile);
   printf("in.outfilen    = '%s'\n", in->outfilen);
   printf("in.outdb       = %d\n",   in->outdb);
   printf("in.outdbn      = '%s'\n", in->outdbn);
   printf("in.nameto      = '%s'\n", in->nameto);
   printf("in.andor       = %d\n",   in->andor);
   printf("in.sqlinit     = '%s'\n", in->sqlinit);
   printf("in.sqltsum     = '%s'\n", in->sqltsum);
   printf("in.sqlsum      = '%s'\n", in->sqlsum);
   printf("in.sqlent      = '%s'\n", in->sqlent);
   printf("in.sqlfin      = '%s'\n", in->sqlfin);
   printf("in.insertdir   = '%d'\n", in->insertdir);
   printf("in.insertfl    = '%d'\n", in->insertfl);
   printf("in.dontdescend = '%d'\n", in->dontdescend);
   printf("\n");
   printf("retval         = %d\n", retval);
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
                   const char* positional_args_help_str) {

   char outfn[MAXPATH];
   int i;

   //bzero(in.sqlent,sizeof(in.sqlent));
   // <in> defaults to all-zeros.
   in.maxthreads = 1;         // don't default to zero threads
   in.delim[0]   = '|';
   in.dontdescend = 0;        // default to descend
   in.buildinindir = 0;       // default to not building db in input dir

   int show   = 0;
   int retval = 0;
   int ch;
   while ( (ch = getopt(argc, argv, getopt_str)) != -1) {
      switch (ch) {

      case 'h':               // help
         print_help(argv[0], getopt_str, positional_args_help_str);
         in.helped = 1;
         retval = -1;
         break;

      case 'H':               // show parsed inputs
         show = 1;
         retval = -1;
         break;

      case 'x':               // xattrs
         in.doxattrs = 1;
         break;

      case 'p':               // print file name/path?
         in.printing = 1;
         break;

      case 'P':               // print dirs?
         in.printdir = 1;
         break;

      case 'N':               // print DB-result column-names?  (i.e. header)
         in.printheader = 1;
         break;

      case 'V':               // print DB-result row-values?
         in.printrows = 1;
         break;

      case 's':               // generate tree-summary table?
         in.writetsum = 1;
         break;

      case 'b':               // build index?
         in.buildindex = 1;
         break;

      case 'n':
         INSTALL_INT(in.maxthreads, optarg, 1, MAXPTHREAD, "-n");
         break;

      case 'd':
         if (optarg[0] == 'x') {
            in.dodelim = 2;
            in.delim[0] = fielddelim[0];
         }
         else {
            in.dodelim = 1;
            in.delim[0] = optarg[0];
         }
         break;

      case 'o':
         in.outfile = 1;
         INSTALL_STR(in.outfilen, optarg, MAXPATH, "-o");
         break;

      case 'O':
         in.outdb = 1;
         INSTALL_STR(in.outdbn, optarg, MAXPATH, "-O");
         break;

      case 't':
         INSTALL_STR(in.nameto, optarg, MAXPATH, "-t");
         break;

      case 'i':
         INSTALL_STR(in.name, optarg, MAXPATH, "-i");
         break;

      case 'I':               // SQL initializations
         INSTALL_STR(in.sqlinit, optarg, MAXSQL, "-I");
         break;

      case 'T':               // SQL for tree-summary
         INSTALL_STR(in.sqltsum, optarg, MAXSQL, "-T");
         break;

      case 'S':               // SQL for summary
         INSTALL_STR(in.sqlsum, optarg, MAXSQL, "-S");
         break;

      case 'E':               // SQL for entries
         INSTALL_STR(in.sqlent, optarg, MAXSQL, "-E");
         break;

      case 'F':               // SQL clean-up
         INSTALL_STR(in.sqlfin, optarg, MAXSQL, "-F");
         break;

      case 'a':               // and/or
         in.andor = 1;
         break;

      case 'r':               // insert files and links into db for bfwreaddirplus2db 
         in.insertfl = 1;
         break;

      case 'R':               // insert dirs into db for bfwreaddirplus2db 
         in.insertdir = 1;
         break;

      case 'D':               // default is 0
         in.dontdescend = 1;
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
   // NOTE: caller may have custom options ovf their own, so returning a
   //       value > n_positional is okay (?)
   if (retval
       || ((argc - optind) < n_positional)) {
      if (! in.helped) {
         print_help(argv[0], getopt_str, positional_args_help_str);
         in.helped = 1;
      }
      retval = -1;
   }


   // DEBUGGING:
   if (show)
      show_input(&in, retval);

   return (retval ? retval : optind);
}
