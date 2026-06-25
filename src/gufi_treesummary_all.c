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
#include "config.h"
#include "debug.h"
#include "dbutils.h"
#include "rollup.h"
#include "utils.h"

struct treesummary {
    struct BottomUp data;
    int canrollup;  /* used by parent */
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

    const size_t dbname_len = dir->name_len + 1 + DBNAME_LEN;
    char *dbname = malloc(dbname_len + 1);
    SNFORMAT_S(dbname, dbname_len + 1, 2,
               dir->name, dir->name_len,
               "/" DBNAME, DBNAME_LEN + 1);

    int rc = 0; /* keep going down even if the database file doesn't exist */

    sqlite3 *db = opendb(dbname, SQLITE_OPEN_READONLY, 1, 0, NULL, NULL);
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
    free(dbname);

    return rc;
}

static int get_uidgid_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    struct stat *st = (struct stat *) args;

    /* mark that this callback was called */
    st->st_size++; /* doing ++ instead of = 1 just in case this is somehow called multiple times */

    return !((sscanf(data[0], "%" STAT_uid, &st->st_uid) == 1) &&
             (sscanf(data[0], "%" STAT_gid, &st->st_gid) == 1));
}

static int treesummary_ascend(void *args) {
    struct treesummary *ts = (struct treesummary *) args;
    struct BottomUp *dir = &ts->data;

    const size_t dbname_len = dir->name_len + 1 + DBNAME_LEN;
    char *dbname = malloc(dbname_len + 1);
    SNFORMAT_S(dbname, dbname_len + 1, 2,
               dir->name, dir->name_len,
               "/" DBNAME, DBNAME_LEN + 1);

    sqlite3 *db = opendb(dbname, SQLITE_OPEN_READWRITE, 1, 0, NULL, NULL);
    if (!db) {
        free(dbname);
        return 1;
    }

    int rc = 0;

    char *err = NULL;

    /* pull uid and gid for bttomup_collect_treesummary */
    struct stat st = {0};
    if (sqlite3_exec(db, "SELECT uid, gid FROM " SUMMARY " WHERE isroot == 1;",
                     get_uidgid_callback, &st, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr,
                                  "Error: Could not get data from " SUMMARY " table at \"%s\": %s\n",
                                  dir->name, err);
        rc = 1;
        goto cleanup;
    }

    if (sqlite3_exec(db,
                     /* create treesummary table if it doesn't already exist */
                     TREESUMMARY_SCHEMA(TREESUMMARY, "")
                     /* delete old treesummary data for just this directory (in case the index is rolled up) */
                     "DELETE FROM " TREESUMMARY " "
                     "WHERE inode == (SELECT " SUMMARY ".inode "
                     "                FROM " SUMMARY " JOIN " TREESUMMARY " ON " SUMMARY ".inode == " TREESUMMARY ".inode "
                     "                WHERE " SUMMARY ".isroot == 1);",
                     NULL, NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr,
                                  "Error: Could not set up " TREESUMMARY " table at \"%s\": %s\n",
                                  dir->name, err);
        rc = 1;
        goto cleanup;
    }

    /* the callback would not have run if the database file is empty (such as at the top) */
    if (st.st_size != 1) {
        rc = (st.st_size > 1); /* if count is 0, don't error - just finish this thread without writing the treesummary data */
        goto cleanup;
    }

    rc = bottomup_collect_treesummary(db, dir->name, &dir->subdirs, ISROLLEDUP_CHECK,
                                      st.st_uid, st.st_gid, &ts->canrollup);

    if (rc == 0) {
        /* update summary canrollup */
        char update_canrollup[] = "UPDATE " SUMMARY " SET canrollup = 0 WHERE isroot == 1;";
        update_canrollup[sizeof(update_canrollup) - sizeof("0 WHERE isroot == 1;")] = '0' + ts->canrollup;

        if (sqlite3_exec(db, update_canrollup, NULL, NULL, &err) != SQLITE_OK) {
            sqlite_print_err_and_free(err, stderr,
                                      "Error: Could not get update " SUMMARY ".canrollup at \"%s\": %s\n",
                                      dir->name, err);
            rc = 1;
        }
    }

  cleanup:
    closedb(db);
    free(dbname);

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

        FLAG_END
    };

    struct input in;
    process_args_and_maybe_exit(options, 1, "GUFI_tree", &in);

    BU_descend_f desc = in.dont_reprocess?treesummary_descend:NULL;

    struct start_end after_init;
    clock_gettime(CLOCK_MONOTONIC, &after_init.start);

    const int rc = parallel_bottomup(in.pos.argv, in.pos.argc,
                                     in.min_level, in.max_level,
                                     &in.path_list,
                                     in.maxthreads,
                                     sizeof(struct treesummary),
                                     desc, treesummary_ascend,
                                     0,
                                     0,
                                     NULL);

    clock_gettime(CLOCK_MONOTONIC, &after_init.end);
    const long double processtime = sec(nsec(&after_init));
    fprintf(stderr, "Time Spent Computing Tree Summary tables: %.2Lfs\n", processtime);

    input_fini(&in);

    return rc?EXIT_FAILURE:EXIT_SUCCESS;
}
