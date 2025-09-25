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



#include <stdlib.h>
#include <stdio.h>

#include "BottomUp.h"
#include "bf.h"
#include "dbutils.h"
#include "utils.h"

static int treesummary_found(void *args, int count, char **data, char **columns) {
    (void) count;
    (void) data;
    (void) columns;

    int *keep_going = (int *) args;
    *keep_going = 0;
    return 0;
}

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
        if (sqlite3_exec(db, TREESUMMARY_EXISTS, treesummary_found, &keep_going, &err) != SQLITE_OK) {
            fprintf(stderr, "Error: Could not check for existence of treesummary table at \"%s\": %s\n",
                    dir->name, err);
            sqlite3_free(err);
            rc = 1;
        }
    }

    closedb(db);

    return rc;
}

static int treesummary_ascend(void *args) {
    struct BottomUp *dir = (struct BottomUp *) args;

    char dbname[MAXPATH];
    SNFORMAT_S(dbname, sizeof(dbname), 2,
               dir->name, dir->name_len,
               "/" DBNAME, DBNAME_LEN + 1);

    sqlite3 *db = opendb(dbname, SQLITE_OPEN_READWRITE, 1, 0, create_treesummary_tables, NULL);

    int rc = !db;
    if (db) {
        /* the treesummary table was not found, so create it */
        rc = bottomup_collect_treesummary(db, dir->name, &dir->subdirs, ROLLUPSCORE_CHECK);
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
    struct input in;
    process_args_and_maybe_exit("hHvn:y:z:D:l", 1, "GUFI_tree", &in);

    BU_descend_f desc = in.check_already_processed?treesummary_descend:NULL;

    const int rc = parallel_bottomup(argv + idx, argc - idx,
                                     in.min_level, in.max_level,
                                     &in.subtree_list,
                                     in.maxthreads,
                                     sizeof(struct BottomUp),
                                     desc, treesummary_ascend,
                                     0,
                                     0,
                                     &in);

    input_fini(&in);

    return rc?EXIT_FAILURE:EXIT_SUCCESS;
}
