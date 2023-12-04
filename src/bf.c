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
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "bf.h"
#include "utils.h"

#ifdef __CYGWIN__
__declspec(dllimport)
#endif
extern char *optarg;
#ifdef __CYGWIN__
__declspec(dllimport)
#endif
extern int optind, opterr, optopt;

const char fielddelim = '\x1E';     /* ASCII Record Separator */

void print_help(const char* prog_name,
                const char* getopt_str,
                const char* positional_args_help_str) {
   /* Not checking arguments */

   const char* opt = getopt_str;
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
      case 'b': printf("  -b                     build GUFI index tree"); break;
      case 'a': printf("  -a                     AND/OR (SQL query combination)"); break;
      case 'n': printf("  -n <threads>           number of threads"); break;
      case 'd': printf("  -d <delim>             delimiter (one char)  [use 'x' for 0x%02X]", (uint8_t)fielddelim); break;
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
      case 'Y': printf("  -Y                     default to all directories suspect"); break;
      case 'Z': printf("  -Z                     default to all files/links suspect"); break;
      case 'W': printf("  -W <INSUSPECT>         suspect input file"); break;
      case 'A': printf("  -A <suspectmethod>     suspect method (0 no suspects, 1 suspect file_dfl, 2 suspect stat d and file_fl, 3 suspect stat_dfl"); break;
      case 'g': printf("  -g <stridesize>        stride size for striping inodes"); break;
      case 'c': printf("  -c <suspecttime>       time in seconds since epoch for suspect comparision"); break;
      case 'y': printf("  -y <min level>         minimum level to go down"); break;
      case 'z': printf("  -z <max level>         maximum level to go down"); break;
      case 'J': printf("  -J <SQL_interm>        SQL for intermediate results"); break;
      case 'K': printf("  -K <create aggregate>  SQL to create the final aggregation table"); break;
      case 'G': printf("  -G <SQL_aggregate>     SQL for aggregated results"); break;
      case 'm': printf("  -m                     Keep mtime and atime same on the database files"); break;
      case 'B': printf("  -B <buffer size>       size of each thread's output buffer in bytes"); break;
      case 'w': printf("  -w                     open the database files in read-write mode instead of read only mode"); break;
      case 'f': printf("  -f <FORMAT>            use the specified FORMAT instead of the default; output a newline after each use of FORMAT"); break;
      case 'j': printf("  -j                     print the information in terse form"); break; /* output from stat --help */
      case 'X': printf("  -X                     Dry run"); break;
      case 'L': printf("  -L <count>             Highest number of files/links in a directory allowed to be rolled up"); break;
      case 'k': printf("  -k <filename>          file containing directory names to skip"); break;
      case 'M': printf("  -M <bytes>             target memory footprint"); break;
      case 'C': printf("  -C <count>             Number of subdirectories allowed to be enqueued for parallel processing. Any remainders will be processed in-situ"); break;
#if HAVE_ZLIB
      case 'e': printf("  -e                     compress work items"); break;
#endif
      default: printf("print_help(): unrecognized option '%c'", (char)ch);
      }
      printf("\n");
   }
   printf("\n");
}

// DEBUGGING
void show_input(struct input* in, int retval) {
   printf("in.printing                 = %d\n",            in->printing);
   printf("in.printdir                 = %d\n",            in->printdir);
   printf("in.printheader              = %d\n",            in->printheader);
   printf("in.printrows                = %d\n",            in->printrows);
   printf("in.buildindex               = %d\n",            in->buildindex);
   printf("in.maxthreads               = %zu\n",           in->maxthreads);
   printf("in.delim                    = '%c'\n",          in->delim);
   printf("in.name                     = '%s'\n",          in->name.data);
   printf("in.name_len                 = '%zu'\n",         in->name.len);
   printf("in.nameto                   = '%s'\n",          in->nameto.data);
   printf("in.andor                    = %d\n",            (int) in->andor);
   printf("in.external_enabled         = %d\n",            in->external_enabled);
   printf("in.nobody.uid               = %" STAT_uid "\n", in->nobody.uid);
   printf("in.nobody.gid               = %" STAT_gid "\n", in->nobody.gid);
   printf("in.sql.init                 = '%s'\n",          in->sql.init.data);
   printf("in.sql.tsum                 = '%s'\n",          in->sql.tsum.data);
   printf("in.sql.sum                  = '%s'\n",          in->sql.sum.data);
   printf("in.sql.ent                  = '%s'\n",          in->sql.ent.data);
   printf("in.sql.fin                  = '%s'\n",          in->sql.fin.data);
   printf("in.insertdir                = '%d'\n",          in->insertdir);
   printf("in.insertfl                 = '%d'\n",          in->insertfl);
   printf("in.suspectd                 = '%d'\n",          in->suspectd);
   printf("in.suspectfl                = '%d'\n",          in->suspectfl);
   printf("in.insuspect                = '%s'\n",          in->insuspect.data);
   printf("in.suspectfile              = '%d'\n",          in->suspectfile);
   printf("in.suspectmethod            = '%d'\n",          in->suspectmethod);
   printf("in.suspecttime              = '%d'\n",          in->suspecttime);
   printf("in.stride                   = '%d'\n",          in->stride);
   printf("in.min_level                = %zu\n",           in->min_level);
   printf("in.max_level                = %zu\n",           in->max_level);
   printf("in.sql.intermediate         = '%s'\n",          in->sql.intermediate.data);
   printf("in.sql.init_agg             = '%s'\n",          in->sql.init_agg.data);
   printf("in.sql.agg                  = '%s'\n",          in->sql.agg.data);
   printf("in.keep_matime              = %d\n",            in->keep_matime);
   printf("in.output_buffer_size       = %zu\n",           in->output_buffer_size);
   printf("in.open_flags               = %d\n",            in->open_flags);
   printf("in.format_set               = %d\n",            in->format_set);
   printf("in.format                   = '%s'\n",          in->format.data);
   printf("in.terse                    = %d\n",            in->terse);
   printf("in.dry_run                  = %d\n",            in->dry_run);
   printf("in.max_in_dir               = %zu\n",           in->max_in_dir);
   printf("in.skip                     = %s\n",            in->skip.data);
   printf("in.target_memory_footprint  = %" PRIu64 "\n",   in->target_memory_footprint);
   printf("in.subdir_limit             = %zu\n",           in->subdir_limit);
   printf("in.compress                 = %d\n",            in->compress);
   printf("\n");
   printf("retval                      = %d\n",    retval);
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
   in->maxthreads              = 1;                      // don't default to zero threads
   in->delim                   = fielddelim;
   in->andor                   = AND;
   in->max_level               = -1;                     // default to all the way down
   in->nobody.uid              = 65534;
   in->nobody.gid              = 65534;
   struct passwd *passwd       = getpwnam("nobody");
   if (passwd) {
       in->nobody.uid          = passwd->pw_uid;
       in->nobody.gid          = passwd->pw_gid;
   }
   in->output                  = STDOUT;
   in->output_buffer_size      = 4096;
   in->open_flags              = SQLITE_OPEN_READONLY;   // default to read-only opens

   int show                    = 0;
   int retval                  = 0;
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

      case 'b':               // build index?
         in->buildindex = 1;
         break;

      case 'a':               // and/or
         in->andor = OR;
         break;

      case 'n':
          INSTALL_SIZE(&in->maxthreads, optarg, 1, (size_t) -1, "-n", &retval);
         break;

      case 'd':
         if (optarg[0] == 'x') {
             in->delim = fielddelim;
         }
         else {
             in->delim = optarg[0];
         }
         break;

      case 't':
         INSTALL_STR(&in->nameto, optarg);
         break;

      case 'i':
         INSTALL_STR(&in->name, optarg);
         break;

      case 'o':
         in->output = OUTFILE;
         INSTALL_STR(&in->outname, optarg);
         break;

      case 'O':
         in->output = OUTDB;
         INSTALL_STR(&in->outname, optarg);
         break;

      case 'I':               // SQL initializations
         INSTALL_STR(&in->sql.init, optarg);
         break;

      case 'T':               // SQL for tree-summary
         INSTALL_STR(&in->sql.tsum, optarg);
         break;

      case 'S':               // SQL for summary
         INSTALL_STR(&in->sql.sum, optarg);
         break;

      case 'E':               // SQL for entries
         INSTALL_STR(&in->sql.ent, optarg);
         break;

      case 'F':               // SQL clean-up
         INSTALL_STR(&in->sql.fin, optarg);
         break;

      case 'r':               // insert files and links into db for bfwreaddirplus2db
         in->insertfl = 1;
         break;

      case 'R':               // insert dirs into db for bfwreaddirplus2db
         in->insertdir = 1;
         break;

      case 'Y':               // default is 0
         in->suspectd = 1;
         break;

      case 'Z':               // default is 0
         in->suspectfl = 1;
         break;

      case 'W':               // SQL clean-up
         INSTALL_STR(&in->insuspect, optarg);
         in->suspectfile = 1;
         break;

      case 'A':
         INSTALL_INT(&in->suspectmethod, optarg, 0, 3, "-A", &retval);
         break;

      case 'g':
         INSTALL_INT(&in->stride, optarg, 1, MAXSTRIDE, "-g", &retval);
         break;

      case 'c':
         INSTALL_INT(&in->suspecttime, optarg, 1, 2147483646, "-c", &retval);
         break;

      case 'y':
         INSTALL_SIZE(&in->min_level, optarg, (size_t) 0, (size_t) -1, "-y", &retval);
         break;

      case 'z':
         INSTALL_SIZE(&in->max_level, optarg, (size_t) 0, (size_t) -1, "-z", &retval);
         break;

      case 'J':
         INSTALL_STR(&in->sql.intermediate, optarg);
         break;

      case 'K':
         INSTALL_STR(&in->sql.init_agg, optarg);
         break;

      case 'G':
         INSTALL_STR(&in->sql.agg, optarg);
         break;

      case 'm':
         in->keep_matime = 1;
         break;

      case 'B':
         INSTALL_SIZE(&in->output_buffer_size, optarg, (size_t) 0, (size_t) -1, "-z", &retval);
         break;

      case 'w':
         in->open_flags = SQLITE_OPEN_READWRITE;
         break;

      case 'f':
          INSTALL_STR(&in->format, optarg);
          in->format_set = 1;
          break;

      case 'j':
          in->terse = 1;
          break;

      case 'X':
          in->dry_run = 1;
          break;

      case 'L':
          INSTALL_SIZE(&in->max_in_dir, optarg, (size_t) 0, (size_t) -1, "-L", &retval);
          break;

      case 'k':
          INSTALL_STR(&in->skip, optarg);
          break;

      case 'M':
          INSTALL_UINT64(&in->target_memory_footprint, optarg, (uint64_t) 0, (uint64_t) -1, "-M", &retval);
          break;

      case 'C':
          INSTALL_SIZE(&in->subdir_limit, optarg, (size_t) 0, (size_t) -1, "-C", &retval);
          break;

#if HAVE_ZLIB
      case 'e':
          in->compress = 1;
          break;
#endif

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

int INSTALL_STR(refstr_t *VAR, const char *SOURCE) {
    if (!SOURCE) {
        return -1;
    }
    VAR->data = (const char *) (SOURCE);
    VAR->len = strlen(VAR->data);
    return 0;
}

#define INSTALL_NUMBER(name, type, fmt)                                        \
    void INSTALL_##name(type *dst, const char *argv,                           \
                       const type min, const type max,                         \
                       const char *arg_name, int *retval) {                    \
        if (sscanf(argv, fmt, dst) != 1) {                                     \
            fprintf(stderr, "could not parse %s with format %s\n", argv, fmt); \
            *retval = -1;                                                      \
        }                                                                      \
        if ((*dst < min) || (*dst > max)) {                                    \
            fprintf(stderr, "argument '%s' not in range [" fmt ", " fmt "]\n", \
                    arg_name, min, max);                                       \
            *retval = -1;                                                      \
        }                                                                      \
    }

INSTALL_NUMBER(INT, int, "%d")
INSTALL_NUMBER(SIZE, size_t, "%zu")
INSTALL_NUMBER(UINT64, uint64_t, "%" PRIu64)
