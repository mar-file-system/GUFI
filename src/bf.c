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



#include <dlfcn.h>
#include <errno.h>
#include <pwd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "bf.h"
#include "dbutils.h"
#include "external.h"
#include "plugin.h"
#include "utils.h"

#define XSTR(x) #x
#define STR(x) XSTR(x)

#ifdef __CYGWIN__
__declspec(dllimport)
#endif
extern char *optarg;
#ifdef __CYGWIN__
__declspec(dllimport)
#endif
extern int optind, opterr, optopt;

const char fielddelim = '\x1E';     /* ASCII Record Separator */

static const char DEFAULT_SWAP_PREFIX[] = "";

static const struct plugin_operations null_plugin_ops = {
    .db_init = NULL,
    .process_file = NULL,
    .process_dir = NULL,
    .db_exit = NULL,
};

struct input *input_init(struct input *in) {
    if (in) {
        memset(in, 0, sizeof(*in));
        in->maxthreads              = 1;                      // don't default to zero threads
        in->delim                   = fielddelim;
        in->process_sql             = RUN_ON_ROW;
        in->max_level               = -1;                     // default to all the way down
        in->nobody.uid              = 65534;
        in->nobody.gid              = 65534;
        struct passwd *passwd       = getpwnam("nobody");
        if (passwd) {
            in->nobody.uid          = passwd->pw_uid;
            in->nobody.gid          = passwd->pw_gid;
        }
        sll_init(&in->sql_format.tsum);
        sll_init(&in->sql_format.sum);
        sll_init(&in->sql_format.ent);
        in->output                  = STDOUT;
        in->output_buffer_size      = 4096;
        in->open_flags              = SQLITE_OPEN_READONLY;   // default to read-only opens
        in->skip                    = trie_alloc();

        /* always skip . and .. */
        trie_insert(in->skip, ".",  1, NULL, NULL);
        trie_insert(in->skip, "..", 2, NULL, NULL);

        sll_init(&in->external_attach);

        in->swap_prefix.data = DEFAULT_SWAP_PREFIX;
        in->swap_prefix.len  = 0;

        in->plugin_ops = &null_plugin_ops;
        in->plugin_handle = NULL;
    }

    return in;
}

void input_fini(struct input *in) {
    if (in) {
        sll_destroy(&in->sql_format.ent,  free);
        sll_destroy(&in->sql_format.sum,  free);
        sll_destroy(&in->sql_format.tsum, free);
        free(in->types.agg);
        free(in->types.ent);
        free(in->types.sum);
        free(in->types.tsum);
        sll_destroy(&in->external_attach, free);
        trie_free(in->skip);
        if (in->plugin_handle) {
            dlclose(in->plugin_handle);
        }
    }
}

/*
 * If a plugin library path is given in in->plugin_name,
 * then load that library and add its plugin functions to in->plugin_ops.
 *
 * Returns 0 on success or 1 on failure.
 */
static int load_plugin_library(struct input *in, char *plugin_name) {
    void *lib = dlopen(plugin_name, RTLD_NOW);
    if (!lib) {
        fprintf(stderr, "Could not open plugin library: %s\n", dlerror());
        return 1;
    }

    in->plugin_handle = lib;

    struct plugin_operations *user_plugin_ops = (struct plugin_operations *) dlsym(lib, GUFI_PLUGIN_SYMBOL_STR);
    if (!user_plugin_ops) {
        fprintf(stderr, "Could not find exported operations in the plugin library: %s\n", dlerror());
        return 1;
    }

    in->plugin_ops = user_plugin_ops;

    return 0;
}

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
      case 'v': printf("  -v                     version"); break;
      case 'x': printf("  -x                     index/query xattrs"); break;
      case 'P': printf("  -P                     print directories as they are encountered"); break;
      case 'b': printf("  -b                     build GUFI index tree"); break;
      case 'a': printf("  -a <0|1|2>             0 - if returned row, run next SQL, else stop (continue descent) (default)\n"
                       "                         1 - skip T, run S and E whether or not a row was returned (old -a)\n"
                       "                         2 - run T, S, and E whether or not a row was returned"); break;
      case 'n': printf("  -n <threads>           number of threads"); break;
      case 'd': printf("  -d <delim>             delimiter (one char)  [use 'x' for 0x%02X]", (uint8_t)fielddelim); break;
      case 'o': printf("  -o <out_fname>         output file (one-per-thread, with thread-id suffix)"); break;
      case 'O': printf("  -O <out_DB>            output DB"); break;
      case 'u': printf("  -u                     prefix row with 1 int column count and each column with 1 octet type and 1 size_t length"); break;
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
      case 'e': printf("  -e                     compress work items"); break;
      case 'q': printf("  -q                     check that external databases are valid before tracking during indexing"); break;
      case 'Q': printf("  -Q <basename>\n"
                       "     <table>\n"
                       "     <template>.<table>\n"
                       "     <view>              External database file basename, per-attach table name, template + table name, and the resultant view"); break;
      case 's': printf("  -s <path>              File name prefix for swap files"); break;
      case 'p': printf("  -p <path>              Source path prefix for %%s in SQL"); break;
      case 'D': printf("  -D <filename>          File containing paths at single level to index (not including starting path). Must also use -y"); break;
      case 'l': printf("  -l                     if a directory was previously processed, skip descending the subtree"); break;
      case 'U': printf("  -U <library_name>      plugin library for modifying db entries"); break;
      case 't': printf("  -t <filter_type>       one or more types to keep ('f', 'd', 'l')"); break;
      default: printf("print_help(): unrecognized option '%c'", (char)ch);
      }
      printf("\n");
   }
   printf("\n");
}

// DEBUGGING
void show_input(struct input* in, int retval) {
   printf("in.printed_version          = %d\n",            in->printed_version);
   printf("in.printdir                 = %d\n",            in->printdir);
   printf("in.buildindex               = %d\n",            in->buildindex);
   printf("in.maxthreads               = %zu\n",           in->maxthreads);
   printf("in.delim                    = '%c'\n",          in->delim);
   printf("in.process_sql              = %d\n",            (int) in->process_sql);
   printf("in.types.prefix             = %d\n",            in->types.prefix);
   printf("in.process_xattrs           = %d\n",            in->process_xattrs);
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
   printf("in.skip_count               = '%zu'\n",         in->skip_count);
   printf("in.target_memory_footprint  = %" PRIu64 "\n",   in->target_memory_footprint);
   printf("in.subdir_limit             = %zu\n",           in->subdir_limit);
   printf("in.compress                 = %d\n",            in->compress);
   printf("in.check_extdb_valid        = %d\n",            in->check_extdb_valid);
   size_t i = 0;
   sll_loop(&in->external_attach, node) {
       eus_t *eus = (eus_t *) sll_node_data(node);
       printf("in.external_attach[%zu] = ('%s', '%s', '%s', '%s')\n",
              i++, eus->basename.data, eus->table.data, eus->template_table.data, eus->view.data);
   }
   printf("\n");
   printf("in.swap_prefix              = '%s'\n",          in->swap_prefix.data);
   printf("in.source_prefix            = '%s'\n",          in->sql_format.source_prefix.data);
   printf("in.subtree_list             = '%s'\n",          in->subtree_list.data);
   printf("in.check_already_processed  = %d\n",            in->check_already_processed);
   printf("in.filter_types             = %d\n",            in->filter_types);

   printf("retval                      = %d\n",            retval);
   printf("\n");
}

// process command-line options
//
// <positional_args_help_str> is a string like "input_dir to_dir"
//    which is placed after "[options]" in the output produced for the '-h'
//    option.  In other words, you end up with a picture of the
//    command-line, for the help text.
//
// Do not call this function multiple times without calling
//    input_fini between calls.
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

   input_init(in);

   int show                    = 0;
   int retval                  = 0;
   int bad_skipfile            = 0;
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

      case 'v':
          printf(STR(GUFI_VERSION) "\n");
          in->printed_version = 1;
          break;

      case 'x':               // enable xattr processing
         in->process_xattrs = 1;
         break;

      case 'P':               // print dirs?
         in->printdir = 1;
         break;

      case 'b':               // build index?
         in->buildindex = 1;
         break;

      case 'a':
         {
             int a = -1;
             INSTALL_INT(&a, optarg, 0, 2, "-a", &retval);
             in->process_sql = a;
         }
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

      case 'o':
         in->output = OUTFILE;
         INSTALL_STR(&in->outname, optarg);
         break;

      case 'O':
         in->output = OUTDB;
         INSTALL_STR(&in->outname, optarg);
         break;

      case 'u':
         in->types.prefix = 1;
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
          {
              refstr_t skipfile = {
                  .data = NULL,
                  .len = 0,
              };
              INSTALL_STR(&skipfile, optarg);
              const ssize_t added = setup_directory_skip(in->skip, skipfile.data);
              if (added < 0) {
                  retval = -1;
                  bad_skipfile = 1;
                  /* cannot return here - expected behavior is to parse remaining options first */
              }
              else {
                  in->skip_count += added;
              }
          }
          break;

      case 'M':
          INSTALL_UINT64(&in->target_memory_footprint, optarg, (uint64_t) 0, (uint64_t) -1, "-M", &retval);
          break;

      case 'C':
          INSTALL_SIZE(&in->subdir_limit, optarg, (size_t) 0, (size_t) -1, "-C", &retval);
          break;

      case 'e':
          in->compress = 1;
          break;

      case 'q':
          in->check_extdb_valid = 1;
          break;

      case 'Q':
          {
              eus_t *user = calloc(1, sizeof(*user));

              INSTALL_STR(&user->basename, optarg);

              optarg = argv[optind];
              INSTALL_STR(&user->table, optarg);

              optarg = argv[++optind];
              INSTALL_STR(&user->template_table, optarg);

              optarg = argv[++optind];
              INSTALL_STR(&user->view, optarg);

              optarg = argv[++optind];

              sll_push(&in->external_attach, user);
          }
          break;

      case 's':
          INSTALL_STR(&in->swap_prefix, optarg);
          break;

      case 'p':
          INSTALL_STR(&in->sql_format.source_prefix, optarg);
          break;

      case 'D':
          INSTALL_STR(&in->subtree_list, optarg);
          break;

      case 'l':
          in->check_already_processed = 1;
          break;

      case 'U':
          if (load_plugin_library(in, optarg)) {
              retval = -1;
          }
          break;

      case 't':
          while (*optarg) {
              switch (*optarg) {
                  case 'f':
                      in->filter_types |= FILTER_TYPE_FILE;
                      break;
                  case 'd':
                      in->filter_types |= FILTER_TYPE_DIR;
                      break;
                  case 'l':
                      in->filter_types |= FILTER_TYPE_LINK;
                      break;
                  default:
                      fprintf(stderr, "%c is not a valid option\n", *optarg);
                      retval = -1;
                      break;
              }
              ++optarg; 
          }
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

   if (in->subtree_list.len && (in->min_level == 0)) {
       fprintf(stderr, "-D must be used with -y level > 0\n");
       retval = -1;
   }

   if (in->printed_version) {
       return -1;
   }

   if (in->filter_types == 0) {
      in->filter_types = FILTER_TYPE_FILE | FILTER_TYPE_DIR | FILTER_TYPE_LINK;
   }

   // caller requires given number of positional args, after the options.
   // <optind> is the number of argv[] values that were recognized as options.
   // NOTE: caller may have custom options ovf their own, so returning a
   //       value > n_positional is okay (?)
   if (retval
       || ((argc - optind) < n_positional)) {
      if (!in->helped && !bad_skipfile) {
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

/*
 * Returns size of a dynamically sized struct work_packed.
 *
 * Size of base struct plus size of stored name plus NUL terminator.
 */
size_t struct_work_size(struct work *w) {
    return sizeof(*w) + w->name_len + 1;
}

/*
 * Allocates a new struct work on the heap with enough room to
 * fit the given `basename` with an optional `prefix`.
 *
 * Initializes the following fields:
 *   - name
 *   - name_len
 */
struct work *new_work_with_name(const char *prefix, const size_t prefix_len,
                                const char *basename, const size_t basename_len) {
    /* +1 for path separator */
    const size_t name_total = prefix_len + 1 + basename_len;

    struct work *w = calloc(1, sizeof(*w) + name_total + 1);
    w->name = (char *) &w[1];
    if (prefix_len == 0) {
        w->name_len = SNFORMAT_S(w->name, name_total + 1, 1,
                                 basename, basename_len);
    } else {
        w->name_len = SNFORMAT_S(w->name, name_total + 1, 3,
                                 prefix, prefix_len,
                                 "/", (size_t) 1,
                                 basename, basename_len);
    }

    w->basename_len = basename_len;

    return w;
}
