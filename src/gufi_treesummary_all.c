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



#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "BottomUp.h"
#include "bf.h"
#include "dbutils.h"
#include "utils.h"

struct treesummary {
    struct BottomUp data;
    int modified; /* whether or not this directory was modified; informs parent if it needs to be modified */
};

static int treesummary_found(void *args, int count, char **data, char **columns) {
    (void) count;
    (void) data;
    (void) columns;

    int *keep_going = (int *) args;
    *keep_going = 0;
    return 0;
}

/* only called if --dont-reprocess is set */
static int treesummary_descend(void *args, int *keep_going) {
    struct BottomUp *dir = (struct BottomUp *) args;

    char dbname[MAXPATH];
    SNFORMAT_S(dbname, sizeof(dbname), 2,
               dir->name, dir->name_len,
               "/" DBNAME, DBNAME_LEN + 1);

    sqlite3 *db = opendb(dbname, SQLITE_OPEN_READONLY, 1, 0, NULL, NULL);

    int rc = !db;
    if (db) {
        char *err = NULL;
        /* check if treesummary table exists - if it does, don't descend */
        if (sqlite3_exec(db, TREESUMMARY_EXISTS, treesummary_found, keep_going, &err) != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr, "Error: Could not check for existence of treesummary table at \"%s\": %s\n",
                                      dir->name, err);
            rc = 1;
        }
    }

    closedb(db);

    return rc;
}

static int treesummary_ascend(void *args) {
    struct treesummary *ts = (struct treesummary *) args;
    struct BottomUp *dir = &ts->data;
    struct input *in = (struct input *) dir->extra_args;

    /* this directory has not been modified yet */
    ts->modified = 0;

    /* check if any subdirs were modified */
    int subdir_modified = 0;
    sll_loop(&dir->subdirs, node) {
        struct treesummary *subdir = (struct treesummary *) sll_node_data(node);
        subdir_modified |= subdir->modified;
    }

    char dbname[MAXPATH];
    SNFORMAT_S(dbname, sizeof(dbname), 2,
               dir->name, dir->name_len,
               "/" DBNAME, DBNAME_LEN + 1);

    /* check if this treesummary table needs to be updated */
    if (!subdir_modified && in->suspecttime) {
        struct stat st;

        #if HAVE_STATX
        struct statx stx;
        if (statx(AT_FDCWD, dbname,
                  AT_SYMLINK_NOFOLLOW | AT_STATX_DONT_SYNC,
                  STATX_MTIME | STATX_CTIME, &stx) != 0) {
            const int err = errno;
            fprintf(stderr, "Error: Could not statx \"%s\": %s (%d)\n",
                    dbname, strerror(err), err);
            return 1;
        }

        st.st_mtime = stx.stx_mtime.tv_sec + !!stx.stx_mtime.tv_nsec; /* round up */
        st.st_ctime = stx.stx_ctime.tv_sec + !!stx.stx_ctime.tv_nsec; /* round up */
        #else
        if (lstat(dbname, &st) != 0) {
            const int err = errno;
            fprintf(stderr, "Error: Could not lstat \"%s\": %s (%d)\n",
                    dbname, strerror(err), err);
            return 1;
        }
        #endif

        /* suspect time is more recent than mtime/ctime -> nothing to update */
        if ((st.st_mtime < in->suspecttime) &&
            (st.st_ctime < in->suspecttime)) {
            return 0;
        }
    }

    sqlite3 *db = opendb(dbname, SQLITE_OPEN_READWRITE, 1, 0, create_treesummary_tables, NULL);

    int rc = !db;
    if (db) {
        /* the treesummary table was not found, so create it */
        rc = bottomup_collect_treesummary(db, dir->name, &dir->subdirs, ROLLUPSCORE_CHECK);
        ts->modified = 1;
    }

    closedb(db);

    return rc;
}

static void sub_help(void) {
    printf("GUFI_tree                path to GUFI tree\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    /*
     * process input args - all programs share the common 'struct input',
     * but allow different fields to be filled at the command-line.
     * Callers provide the options-string for get_opt(), which will
     * control which options are parsed for each program.
     */
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_THREADS,

        /* processing/tree walk flags */
        FLAG_MIN_LEVEL, FLAG_MAX_LEVEL, FLAG_PATH_LIST, FLAG_DONT_REPROCESS,

         /*
          * if --suspect-time is not passed in, the suspecttime is set to 0,
          * skipping the lstat/statx calls
          */
        FLAG_SUSPECT_TIME,

        FLAG_END
    };

    struct input in;
    process_args_and_maybe_exit(options, 1, "GUFI_tree", &in);

    /* default to create/update treesummary tables for all directories */
    if (!in.suspecttime_set) {
        in.suspecttime = 0;
    }

    BU_descend_f desc = in.dont_reprocess?treesummary_descend:NULL;

    const int rc = parallel_bottomup(argv + idx, argc - idx,
                                     in.min_level, in.max_level,
                                     &in.path_list,
                                     in.maxthreads,
                                     sizeof(struct treesummary),
                                     desc, treesummary_ascend,
                                     0,
                                     0,
                                     &in);

    input_fini(&in);

    return rc?EXIT_FAILURE:EXIT_SUCCESS;
}
