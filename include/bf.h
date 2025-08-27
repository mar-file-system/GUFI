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



#ifndef BF_H
#define BF_H

#include <inttypes.h>
#include <sys/stat.h>

#include "SinglyLinkedList.h"
#include "compress.h"
#include "plugin.h"
#include "str.h"
#include "trie.h"
#include "xattrs.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MAXPATH 4096
#define MAXSQL 8192
#define MAXRECS 100000
#define MAXSTRIDE 1000000000   /* maximum records per stripe */
#define DBNAME "db.db"
#define DBNAME_LEN (sizeof(DBNAME) - 1)
#define GETLINE_DEFAULT_SIZE 750 /* magic number */

struct sum {
  long long int totfiles;
  long long int totlinks;
  long long int minuid;
  long long int maxuid;
  long long int mingid;
  long long int maxgid;
  long long int minsize;
  long long int maxsize;
  long long int totzero;
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
  long long int totblocks; /* files only - does not include directory/self */
  long long int totxattr;
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
  long long int totextdbs; /* only tracked in treesummary, not summary */
};

typedef enum AFlag {
    RUN_ON_ROW = 0,   /* if returned row, run next SQL, else stop (continue descent) (default) */
    RUN_SE     = 1,   /* skip T, run S and E whether or not a row was returned (old -a) */
    RUN_TSE    = 2,   /* run T, S, and E whether or not a row was returned */
} AFlag_t;

typedef enum OutputMethod {
    STDOUT,    /* default */
    OUTFILE,   /* -o */
    OUTDB,     /* -O */
} OutputMethod_t;

struct input {
   refstr_t  name;
   refstr_t  nameto;
   int process_xattrs;
   struct {
       uid_t uid;
       gid_t gid;
   } nobody;

   struct {
       /* set up per-thread intermidate tables */
       refstr_t init;

       refstr_t tsum;
       refstr_t sum;
       refstr_t ent;

       /* if not aggregating, output results */
       /* if aggregating, insert into aggregate table */
       refstr_t intermediate;

       /* set up final aggregation table */
       refstr_t init_agg;

       /* query table containing aggregated results */
       refstr_t agg;

       refstr_t fin;
   } sql;

   /*
    * if the SQL has any formatting in it,
    * store (at least) the positions of the
    * first character being replaced
    *
    * each item should be malloc-ed
    */
   struct {
       refstr_t source_prefix; /* for %s since the source directory is not stored in the index */
       sll_t tsum;
       sll_t sum;
       sll_t ent;
   } sql_format;

   /*
    * if outputting to STDOUT or OUTFILE, get list of
    * types of final output to prefix columns with
    *
    * set up by gufi_query but cleaned up by input_fini
    */
   struct {
       int prefix;

       /* set if not aggregating */
       int *tsum;
       int *sum;
       int *ent;

       /* set if aggregating */
       int *agg;
   } types;

   int  printdir;
   int  helped;               /* support parsing of per-app sub-options */
   int  printed_version;
   char delim;
   int  buildindex;
   size_t maxthreads;
   AFlag_t process_sql;       /* what to do if an SQL statement returns/doesn't return 1 row */
   int  insertdir;                // added for bfwreaddirplus2db
   int  insertfl;                 // added for bfwreaddirplus2db
   int  suspectd;                 // added for bfwreaddirplus2db for how to default suspect directories 0 - not supsect 1 - suspect
   int  suspectfl;                // added for bfwreaddirplus2db for how to default suspect file/link 0 - not suspect 1 - suspect
   refstr_t insuspect;            // added for bfwreaddirplus2db input path for suspects file
   int  suspectfile;              // added for bfwreaddirplus2db flag for if we are processing suspects file
   int  suspectmethod;            // added for bfwreaddirplus2db flag for if we are processing suspects what method do we use
   int  stride;                   // added for bfwreaddirplus2db stride size control striping inodes to output dbs default 0(nostriping)
   int  suspecttime;              // added for bfwreaddirplus2db time for suspect comparison in seconds since epoch
   size_t min_level;              // minimum level of recursion to reach before running queries
   size_t max_level;              // maximum level of recursion to run queries on
   int dry_run;

   OutputMethod_t output;
   refstr_t outname;

   int keep_matime;

   size_t output_buffer_size;
   int open_flags;

   /* used by gufi_query (cumulative times) and gufi_stat (regular output) */
   int terse;

   /* only used by gufi_stat */
   int format_set;
   refstr_t format;

   /* only used by rollup */
   size_t max_in_dir;

   /* trie containing directory basenames to skip during tree traversal */
   trie_t *skip;
   size_t skip_count;

   /* attempt to drain QPTPool work items until this memory footprint is reached */
   uint64_t target_memory_footprint;

   /*
    * If a directory has more than this many subdirectories,
    * subdirectories discovered past this number will be processed
    * in-situ rather than in a separate thread.
    *
    * This is a size_t rather than a fixed width integer because if a
    * directory has more than a few thousand subdirectories, it is
    * probably too big.
    */
   size_t subdir_limit;

   /* compress work items (if compression library was found) */
   int compress;

   /* check if a listed external db is valid when indexing (-q) */
   int check_extdb_valid;

   /* used when querying (-Q) */
   sll_t external_attach;     /* list of eus_t */

   /* prefix of swap files */
   refstr_t swap_prefix;

   /* directory paths to process at -y level > 0 */
   refstr_t subtree_list;

   /*
    * if a directory has already been
    * processed, do not descend further
    *
    * used by BottomUp programs
    */
   int check_already_processed;

    /*
    * Holds pointers to plugin functions for running custom user code to manipulate the databases.
    * These will be NULL if no plugin was specified.
    */
   const struct plugin_operations *plugin_ops;

   /* A handle to a plugin shared library, or NULL if no plugin is being used. */
   void *plugin_handle;
};

struct input *input_init(struct input *in);
void input_fini(struct input *in);

void print_help(const char *prog_name,
                const char *getopt_str,
                const char *positional_args_help_str);

/* DEBUGGING */
void show_input(struct input*in, int retval);

int parse_cmd_line(int         argc,
                   char       *argv[],
                   const char *getopt_str,
                   int         n_positional,
                   const char *positional_args_help_str,
                   struct input *in);

/*
 * Call parse_cmd_line, call sub_help(), and possibly exit.
 * Requires argc, argv, and sub_help() to be defined.
 * Defines idx variable that points to first unprocessed argv.
 * This should only be used at the start of main.
 * Caller still needs to process positional args after this.
 */
#define process_args_and_maybe_exit(flags, n_positional, positional, in)          \
    int idx = parse_cmd_line(argc, argv, flags, n_positional, positional, (in));  \
                                                                                  \
    if ((in)->printed_version) {                                                  \
        input_fini((in));                                                         \
        return EXIT_SUCCESS;                                                      \
    }                                                                             \
                                                                                  \
    if ((in)->helped) {                                                           \
        sub_help();                                                               \
    }                                                                             \
                                                                                  \
    if (idx < 0) {                                                                \
        input_fini((in));                                                         \
        return EXIT_FAILURE;                                                      \
    }

int INSTALL_STR(refstr_t *VAR, const char *SOURCE);

/* parse a cmd-line string-argument */
#define INSTALL_NUMBER_PROTOTYPE(name, type, fmt)            \
    void INSTALL_##name(type *dst, const char *argv,         \
                        const type min, const type max,      \
                        const char *arg_name, int *retval)

INSTALL_NUMBER_PROTOTYPE(INT, int, "%d");
INSTALL_NUMBER_PROTOTYPE(SIZE, size_t, "%zu");
INSTALL_NUMBER_PROTOTYPE(UINT64, uint64_t, "%" PRIu64);

typedef enum {
   DO_FREE   = 0x01,
   CLOSE_DIR = 0x02,
   CLOSE_DB  = 0x04,
} CleanUpTasks;

typedef enum {
    STAT_NOT_CALLED = 0,
    NOT_STATX_CALLED,    /* stat(2), lstat(2), fstatat(2), etc. */
    STATX_CALLED,
} StatCalled;

/*
 * Minimum data needs to be passed around between threads.
 *
 * struct work generally should not be allocated directly - the helper
 *    new_work_with_name() should be used to ensure correct size is allocated.
 *
 * Similarly, the size of struct work should not be calculated with
 * sizeof, but rather using the helper struct_work_size() to account for the
 * storage for name.
 */
struct work {
   compressed_t  compressed;
   refstr_t      orig_root;              /* argv[i] */
   refstr_t      root_parent;            /* dirname(realpath(argv[i])) */
   size_t        root_basename_len;      /* strlen(basename(argv[i])) */
   size_t        level;
   char          *name;                  /* points to memory located after struct work */
   size_t        name_len;               /* == strlen(name) - meaning excludes NUL! */
   size_t        basename_len;           /* can usually get through readdir */
   struct stat   statuso;
   time_t        crtime;
   StatCalled    stat_called;
   long long int pinode;
   size_t        recursion_level;

   /* probably shouldn't be here */
   char *        fullpath;
   size_t        fullpath_len;

   /* name is actually here, but not using flexible arrays */
};

size_t struct_work_size(struct work *w);
struct work *new_work_with_name(const char *prefix, const size_t prefix_len,
                                const char *basename, const size_t basename_len);

/* extra data used by entries that does not depend on data from other directories */
struct entry_data {
   int           parent_fd;    /* holds an FD that can be used for fstatat(2), etc. */
   char          type;
   char          linkname[MAXPATH];
   long long int offset;
   struct xattrs xattrs;
   int           ossint1;
   int           ossint2;
   int           ossint3;
   int           ossint4;
   char          osstext1[MAXXATTR];
   char          osstext2[MAXXATTR];
   char          pinodec[128];
   int           suspect;  // added for bfwreaddirplus2db for suspect
};

extern const char fielddelim;

#ifdef __cplusplus
}
#endif

#endif
