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
#include "debug.h"
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
    sqlite3_initialize(); /* explicitly initialize here, in a fuction that is run serially once in main */
    if (in) {
        memset(in, 0, sizeof(*in));
        in->maxthreads              = 1;                      // don't default to zero threads
        in->delim                   = fielddelim;
        in->process_sql             = RUN_ON_ROW;
        in->suspecttime             = time(NULL);
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
    sqlite3_shutdown();
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
                const struct option* options,
                const char* positional_args_help_str) {
    /* Not checking arguments */

    printf("usage: %s [options] %s\n", prog_name, positional_args_help_str);
    printf("options:\n");

    while (options && options->name != NULL) {
        switch (options->val) {
            case FLAG_HELP_SHORT:                    printf("  -h, --help                        help"); break;
            case FLAG_DEBUG_SHORT:                   printf("  -H, --debug                       show assigned input values (debugging)"); break;
            case FLAG_VERSION_SHORT:                 printf("  -v, --version                     version"); break;
            case FLAG_PRINTDIR_SHORT:                printf("  -P                                print directories as they are encountered"); break;
            case FLAG_PROCESS_SQL_SHORT:             printf("  -a <0|1|2>                        0 - if returned row, run next SQL, else stop (continue descent) (default)\n"
                                                            "                                    1 - skip T, run S and E whether or not a row was returned (old -a)\n"
                                                            "                                    2 - run T, S, and E whether or not a row was returned"); break;
            case FLAG_THREADS_SHORT:                 printf("  -n, --threads <threads>           number of threads"); break;
            case FLAG_DELIM_SHORT:                   printf("  -d, --delim <delim>               delimiter (one char)  [use 'x' for 0x%02X]", (uint8_t)fielddelim); break;
            case FLAG_OUTPUT_FILE_SHORT:             printf("  -o, --output-file <out_fname>     output file (one-per-thread, with thread-id suffix)"); break;
            case FLAG_OUTPUT_DB_SHORT:               printf("  -O, --output-db <out_DB>          output DB"); break;
            case FLAG_SQL_INIT_SHORT:                printf("  -I <SQL_init>                     SQL init"); break;
            case FLAG_SQL_TSUM_SHORT:                printf("  -T <SQL_tsum>                     SQL for tree-summary table"); break;
            case FLAG_SQL_SUM_SHORT:                 printf("  -S <SQL_sum>                      SQL for summary table"); break;
            case FLAG_SQL_ENT_SHORT:                 printf("  -E <SQL_ent>                      SQL for entries table"); break;
            case FLAG_SQL_INTERM_SHORT:              printf("  -J <SQL_interm>                   SQL for intermediate results"); break;
            case FLAG_SQL_CREATE_AGG_SHORT:          printf("  -K <create aggregate>             SQL to create the final aggregation table"); break;
            case FLAG_SQL_AGG_SHORT:                 printf("  -G <SQL_aggregate>                SQL for aggregated results"); break;
            case FLAG_SQL_FIN_SHORT:                 printf("  -F <SQL_fin>                      SQL cleanup"); break;
            case FLAG_READ_WRITE_SHORT:              printf("  -w, --read-write                  open the database files in read-write mode instead of read only mode"); break;
            case FLAG_CHECK_EXTDB_VALID_SHORT:       printf("  -q                                check that external databases are valid before tracking during indexing"); break;
            case FLAG_EXTERNAL_ATTACH_SHORT:         printf("  -Q <basename>\n"
                                                            "     <table>\n"
                                                            "     <template>.<table>\n"
                                                            "     <view>                         External database file basename, per-attach table name, template + table name, and the resultant view"); break;
            case FLAG_PATH_SHORT:                    printf("  -p, --path <path>                 Source path prefix for %%s in SQL"); break;
            case FLAG_FILTER_TYPE_SHORT:             printf("  -t, --filter-type <filter_type>   one or more types to keep ('f', 'd', 'l')"); break;

            /* no typable short flags */

            case FLAG_MIN_LEVEL_SHORT:               printf("      --min-level <min level>       minimum level to go down"); break;
            case FLAG_MAX_LEVEL_SHORT:               printf("      --max-level <max level>       maximum level to go down"); break;
            case FLAG_PRINT_TLV_SHORT:               printf("      --print-tlv                   prefix row with 1 int column count and each column with 1 octet type and 1 size_t length"); break;
            case FLAG_KEEP_MATIME_SHORT:             printf("      --keep-matime                 Keep mtime and atime same on the database files"); break;
            case FLAG_SKIP_FILE_SHORT:               printf("      --skip-file <filename>        file containing directory names to skip"); break;
            case FLAG_DRY_RUN_SHORT:                 printf("      --dry-run                     Dry run"); break;
            case FLAG_PLUGIN_SHORT:                  printf("      --plugin <library_name>       plugin library for modifying db entries"); break;
            case FLAG_SUBTREE_LIST_SHORT:            printf("      --subtree-list <filename>     File containing paths at single level to index (not including starting path). Must also use --min-level"); break;
            case FLAG_FORMAT_SHORT:                  printf("      --format <FORMAT>             use the specified FORMAT instead of the default; output a newline after each use of FORMAT"); break;
            case FLAG_TERSE_SHORT:                   printf("      --terse                       print the information in terse form"); break; /* output from stat --help */
            case FLAG_ROLLUP_LIMIT_SHORT:            printf("      --limit <count>               Highest number of files/links in a directory allowed to be rolled up"); break;
            case FLAG_DONT_REPROCESS_SHORT:          printf("      --dont-reprocess              if a directory was previously processed, skip descending the subtree"); break;

            /* memory usage flags */
            case FLAG_OUTPUT_BUFFER_SIZE_SHORT:      printf("      --output-buffer-size <bytes>  size of each thread's output buffer in bytes"); break;
            case FLAG_TARGET_MEMORY_SHORT:           printf("      --target-memory <bytes>       target memory utilization (soft limit)"); break;
            case FLAG_SUBDIR_LIMIT_SHORT:            printf("      --subdir-limit <count>        Number of subdirectories allowed to be enqueued for parallel processing. Any remainders will be processed serially"); break;
            case FLAG_COMPRESS_SHORT:                printf("      --compress                    compress work items"); break;
            case FLAG_SWAP_PREFIX_SHORT:             printf("      --swap-prefix <path>          File name prefix for swap files"); break;

            /* xattr flags */
            case FLAG_XATTRS_SHORT:                  options++; continue;
            case FLAG_INDEX_XATTRS_SHORT:            printf("  -x, --index-xattrs                index xattrs"); break;
            case FLAG_QUERY_XATTRS_SHORT:            printf("  -x, --query-xattrs                query xattrs"); break;
            case FLAG_SET_XATTRS_SHORT:              printf("  -x, --set-xattrs                  set xattrs"); break;

            /* gufi_incremental_update flags */
            case FLAG_SUSPECT_FILE_SHORT:            printf("      --suspect-file <path>         suspect input file"); break;
            case FLAG_SUSPECT_METHOD_SHORT:          printf("      --suspect-method <0|1|2|3>    suspect method (0 no suspects, 1 suspect file_dfl, 2 suspect stat d and file_fl, 3 suspect stat_dfl"); break;
            case FLAG_SUSPECT_TIME_SHORT:            printf("      --suspect-time <s>            time in seconds since epoch for suspect comparision"); break;
            case FLAG_SUSPECT_DIR_SHORT:             printf("      --suspect-dir                 default to all directories suspect"); break;
            case FLAG_SUSPECT_FILE_LINK_SHORT:       printf("      --suspect-fl                  default to all files/links suspect"); break;

            default:                                 printf("print_help(): unrecognized option '%c'", (char)options->val); break;
        }
        options++;
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
    printf("in.nobody.uid               = %" STAT_uid "\n", in->nobody.uid);
    printf("in.nobody.gid               = %" STAT_gid "\n", in->nobody.gid);
    printf("in.sql.init                 = '%s'\n",          in->sql.init.data);
    printf("in.sql.tsum                 = '%s'\n",          in->sql.tsum.data);
    printf("in.sql.sum                  = '%s'\n",          in->sql.sum.data);
    printf("in.sql.ent                  = '%s'\n",          in->sql.ent.data);
    printf("in.sql.intermediate         = '%s'\n",          in->sql.intermediate.data);
    printf("in.sql.init_agg             = '%s'\n",          in->sql.init_agg.data);
    printf("in.sql.agg                  = '%s'\n",          in->sql.agg.data);
    printf("in.sql.fin                  = '%s'\n",          in->sql.fin.data);
    printf("in.open_flags               = %d\n",            in->open_flags);
    printf("in.check_extdb_valid        = %d\n",            in->check_extdb_valid);
    size_t i = 0;
    sll_loop(&in->external_attach, node) {
        eus_t *eus = (eus_t *) sll_node_data(node);
        printf("in.external_attach[%zu] = ('%s', '%s', '%s', '%s')\n",
               i++, eus->basename.data, eus->table.data, eus->template_table.data, eus->view.data);
    }
    printf("\n");
    printf("in.source_prefix            = '%s'\n",          in->source_prefix.data);
    printf("in.filter_types             = %d\n",            in->filter_types);

    /* no typable short flags */

    printf("in.min_level                = %zu\n",           in->min_level);
    printf("in.max_level                = %zu\n",           in->max_level);
    printf("in.types.prefix             = %d\n",            in->types.print_tlv);
    printf("in.keep_matime              = %d\n",            in->keep_matime);
    printf("in.skip_count               = %zu\n",           in->skip_count);
    printf("in.dry_run                  = %d\n",            in->dry_run);
    printf("in.subtree_list             = '%s'\n",          in->subtree_list.data);
    printf("in.format_set               = %d\n",            in->format_set);
    printf("in.format                   = '%s'\n",          in->format.data);
    printf("in.terse                    = %d\n",            in->terse);
    printf("in.rollup_entries_limit     = %zu\n",           in->rollup_entries_limit);
    printf("in.dont_reprocess           = %d\n",            in->dont_reprocess);

    /* memory usage flags */

    printf("in.output_buffer_size       = %zu\n",           in->output_buffer_size);
    printf("in.target_memory            = %" PRIu64 "\n",   in->target_memory);
    printf("in.subdir_limit             = %zu\n",           in->subdir_limit);
    printf("in.compress                 = %d\n",            in->compress);
    printf("in.swap_prefix              = '%s'\n",          in->swap_prefix.data);

    /* xattr flags */

    printf("in.process_xattrs           = %d\n",            in->process_xattrs);

    /* gufi_incremental_update */
    printf("in.insuspect                = '%s'\n",          in->insuspect.data);
    printf("in.suspectfile              = '%d'\n",          in->suspectfile);
    printf("in.suspectmethod            = '%d'\n",          in->suspectmethod);
    printf("in.suspecttime              = '%d'\n",          in->suspecttime);
    printf("in.suspectd                 = '%d'\n",          in->suspectd);
    printf("in.suspectfl                = '%d'\n",          in->suspectfl);

    printf("retval                      = %d\n",            retval);
    printf("\n");
}

char *build_getopt_str(const struct option *options) {
    size_t len = 1; /* null terminator */

    for (const struct option *opt = options; opt->name != NULL; opt++) {
        if (opt->val >= '0' && opt->val <= 'z') {
            len++;
            if (opt->has_arg == required_argument) {
                len++;
            } else if (opt->has_arg == optional_argument) {
                len += 2;
            }
        }
    }

    char *getopt_str = calloc(len, sizeof(char));

    char *ptr = getopt_str;
    for (const struct option *opt = options; opt->name != NULL; opt++) {
        if (opt->val >= '0' && opt->val <= 'z') {
            *ptr++ = (char) opt->val;
            if (opt->has_arg == required_argument) {
                *ptr++ = ':';
            } else if (opt->has_arg == optional_argument) {
                *ptr++ = ':';
                *ptr++ = ':';
            }
        }
    }
    *ptr = '\0';
    return getopt_str;
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
int parse_cmd_line(int                  argc,
                   char*                argv[],
                   const struct option* options,
                   int                  n_positional,
                   const char*          positional_args_help_str,
                   struct input*        in) {
    input_init(in);
    char *getopt_str = build_getopt_str(options);

    int show                    = 0;
    int retval                  = 0;
    int bad_skipfile            = 0;
    int ch;
    setenv("POSIXLY_CORRECT", "1", 1); /* don't check errors? */
    optind = 0;                        /* man 3 getopt_long */
    while ( (ch = getopt_long(argc, argv, getopt_str, options, NULL)) != -1) {
        switch (ch) {

            case FLAG_HELP_SHORT:               // help
                print_help(argv[0], options, positional_args_help_str);
                in->helped = 1;
                retval = -1;
                break;

            case FLAG_DEBUG_SHORT:               // show parsed inputs
                show = 1;
                retval = -1;
                break;

            case FLAG_VERSION_SHORT:
                printf(STR(GUFI_VERSION) "\n");
                in->printed_version = 1;
                break;

            case FLAG_PRINTDIR_SHORT:               // print dirs?
                in->printdir = 1;
                break;

            case FLAG_PROCESS_SQL_SHORT:
                {
                    int a = -1;
                    INSTALL_INT(&a, optarg, 0, 2, "-a", &retval);
                    in->process_sql = a;
                }
                break;

            case FLAG_THREADS_SHORT:
                INSTALL_SIZE(&in->maxthreads, optarg, 1, (size_t) -1, "-n", &retval);
                break;

            case FLAG_DELIM_SHORT:
                if (optarg[0] == 'x') {
                    in->delim = fielddelim;
                }
                else {
                    in->delim = optarg[0];
                }
                break;

            case FLAG_OUTPUT_FILE_SHORT:
                in->output = OUTFILE;
                INSTALL_STR(&in->outname, optarg);
                break;

            case FLAG_OUTPUT_DB_SHORT:
                in->output = OUTDB;
                INSTALL_STR(&in->outname, optarg);
                break;

            case FLAG_SQL_INIT_SHORT:               // SQL initializations
                INSTALL_STR(&in->sql.init, optarg);
                break;

            case FLAG_SQL_TSUM_SHORT:               // SQL for tree-summary
                INSTALL_STR(&in->sql.tsum, optarg);
                break;

            case FLAG_SQL_SUM_SHORT:               // SQL for summary
                INSTALL_STR(&in->sql.sum, optarg);
                break;

            case FLAG_SQL_ENT_SHORT:               // SQL for entries
                INSTALL_STR(&in->sql.ent, optarg);
                break;

            case FLAG_SQL_INTERM_SHORT:
                INSTALL_STR(&in->sql.intermediate, optarg);
                break;

            case FLAG_SQL_CREATE_AGG_SHORT:
                INSTALL_STR(&in->sql.init_agg, optarg);
                break;

            case FLAG_SQL_AGG_SHORT:
                INSTALL_STR(&in->sql.agg, optarg);
                break;

            case FLAG_SQL_FIN_SHORT:               // SQL clean-up
                INSTALL_STR(&in->sql.fin, optarg);
                break;

            case FLAG_READ_WRITE_SHORT:
                in->open_flags = SQLITE_OPEN_READWRITE;
                break;

            case FLAG_CHECK_EXTDB_VALID_SHORT:
                in->check_extdb_valid = 1;
                break;

            case FLAG_EXTERNAL_ATTACH_SHORT:
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

            case FLAG_PATH_SHORT:
                INSTALL_STR(&in->source_prefix, optarg);
                break;

            case FLAG_FILTER_TYPE_SHORT:
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

            /* no typable short flags */

            case FLAG_MIN_LEVEL_SHORT:
                INSTALL_SIZE(&in->min_level, optarg, (size_t) 0, (size_t) -1, "--min-level", &retval);
                break;

            case FLAG_MAX_LEVEL_SHORT:
                INSTALL_SIZE(&in->max_level, optarg, (size_t) 0, (size_t) -1, "--max-level", &retval);
                break;

            case FLAG_PRINT_TLV_SHORT:
                in->types.print_tlv = 1;
                break;

            case FLAG_KEEP_MATIME_SHORT:
                in->keep_matime = 1;
                break;

            case FLAG_SKIP_FILE_SHORT:
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

            case FLAG_DRY_RUN_SHORT:
                in->dry_run = 1;
                break;

            case FLAG_PLUGIN_SHORT:
                if (load_plugin_library(in, optarg)) {
                    retval = -1;
                }
                break;

            case FLAG_SUBTREE_LIST_SHORT:
                INSTALL_STR(&in->subtree_list, optarg);
                break;

            case FLAG_FORMAT_SHORT:
                INSTALL_STR(&in->format, optarg);
                in->format_set = 1;
                break;

            case FLAG_TERSE_SHORT:
                in->terse = 1;
                break;

            case FLAG_ROLLUP_LIMIT_SHORT:
                INSTALL_SIZE(&in->rollup_entries_limit, optarg, (size_t) 0, (size_t) -1,
                             "--" FLAG_ROLLUP_LIMIT_LONG, &retval);
                break;

            case FLAG_DONT_REPROCESS_SHORT:
                in->dont_reprocess = 1;
                break;

            /* memory usage flags */

            case FLAG_OUTPUT_BUFFER_SIZE_SHORT:
                INSTALL_SIZE(&in->output_buffer_size, optarg, (size_t) 0, (size_t) -1,
                             "--" FLAG_OUTPUT_BUFFER_SIZE_LONG, &retval);
                break;

            case FLAG_TARGET_MEMORY_SHORT:
                INSTALL_UINT64(&in->target_memory, optarg, (uint64_t) 0, (uint64_t) -1,
                               "--" FLAG_TARGET_MEMORY_LONG, &retval);
                break;

            case FLAG_SUBDIR_LIMIT_SHORT:
                INSTALL_SIZE(&in->subdir_limit, optarg, (size_t) 0, (size_t) -1,
                             "--" FLAG_SUBDIR_LIMIT_LONG, &retval);
                break;

            case FLAG_COMPRESS_SHORT:
                in->compress = 1;
                break;

            case FLAG_SWAP_PREFIX_SHORT:
                INSTALL_STR(&in->swap_prefix, optarg);
                break;

            /* xattr flags */

            case FLAG_XATTRS_SHORT:
            case FLAG_INDEX_XATTRS_SHORT:
            case FLAG_QUERY_XATTRS_SHORT:
            case FLAG_SET_XATTRS_SHORT:
                in->process_xattrs = 1; /* all xattr flags point to the same variable */
                break;

            /* gufi_incremental_update */

            case FLAG_SUSPECT_FILE_SHORT:
                INSTALL_STR(&in->insuspect, optarg);
                in->suspectfile = 1;
                break;

            case FLAG_SUSPECT_METHOD_SHORT:
                INSTALL_INT(&in->suspectmethod, optarg, 0, 3,
                            "--" FLAG_SUSPECT_METHOD_LONG, &retval);
                break;

            case FLAG_SUSPECT_TIME_SHORT:
                INSTALL_INT(&in->suspecttime, optarg, 1, 2147483646,
                            "--" FLAG_SUSPECT_TIME_LONG, &retval);
                break;

            case FLAG_SUSPECT_DIR_SHORT:
                in->suspectd = 1;
                break;

            case FLAG_SUSPECT_FILE_LINK_SHORT:
                in->suspectfl = 1;
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
    free(getopt_str);

    // if there were no other errors,
    // make sure min_level <= max_level
    if (retval == 0) {
        retval = -(in->min_level > in->max_level);
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
            print_help(argv[0], options, positional_args_help_str);
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
