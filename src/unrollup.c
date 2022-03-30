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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "bf.h"
#include "dbutils.h"
#include "debug.h"
#include "QueuePerThreadPool.h"
#include "utils.h"

extern int errno;

struct Unrollup {
    char name[MAXPATH];
    int rolledup; /* set by parent, can be modified by self */
};

int get_rolled_up(void * args, int count, char ** data, char ** columns) {
    if ((count == 1) && (data[0][1] == '\0')) {
        int * rolledup = (int *) args;
        *rolledup = (data[0][0] == '1');
        return 0;
    }
    return 1;
}

int processdir(struct QPTPool * ctx, const size_t id, void * data, void * args) {
    struct Unrollup * work = (struct Unrollup *) data;
    char * err = NULL;
    int rc = 0;

    DIR * dir = opendir(work->name);
    if (!dir) {
        fprintf(stderr, "Error: Could not open directory \"%s\": %s", work->name, strerror(errno));
        rc = 1;
        goto free_work;
    }

    int rolledup = 0;

    char dbname[MAXPATH];
    SNPRINTF(dbname, MAXPATH, "%s/" DBNAME, work->name);
    sqlite3 * db = opendb(dbname, SQLITE_OPEN_READWRITE, 0, 0
                , NULL, NULL
                #if defined(DEBUG) && defined(PER_THREAD_STATS)
                , NULL, NULL
                , NULL, NULL
                #endif
        );

    if (!db) {
        rc = 1;
    }

    /* get roll up status set by parent */
    rolledup = work->rolledup;

    /* even if the parent was not rolled up, this directory might be */
    /* directories known to be rolled up can skip this check */
    if (db && !work->rolledup) {
        /* get whether or not the current directory was rolled up */
        if (sqlite3_exec(db, "SELECT (rollupscore <> 0) FROM summary WHERE isroot == 1", get_rolled_up, &rolledup, &err) != SQLITE_OK) {
            fprintf(stderr, "Error: Failed to get rollup score from \"%s\": %s\n", work->name, err);
            rc = 1;
        }
        sqlite3_free(err);
        err = NULL;
    }

    /* always descend */
    /* if not rolled up, children might be */
    /* if rolled up, all child also need processing */
    /**/
    /* descend first to keep working while cleaning up db */
    struct dirent * entry = NULL;
    while ((entry = readdir(dir))) {
        if ((strncmp(entry->d_name, ".",  2) == 0) ||
            (strncmp(entry->d_name, "..", 3) == 0)) {
            continue;
        }

        char name[MAXPATH];
        size_t len = SNPRINTF(name, MAXPATH, "%s/%s", work->name, entry->d_name);

        struct stat st;
        if (lstat(name, &st) == 0) {
            if (S_ISDIR(st.st_mode)) {
                struct Unrollup * subdir = malloc(sizeof(struct Unrollup));
                memcpy(subdir->name, name, len + 1);

                /* set child rolledup status so that if this dir */
                /* was rolled up, child can skip roll up check */
                subdir->rolledup = rolledup;

                QPTPool_enqueue(ctx, id, processdir, subdir);
            }
        }
    }

    /* now that work has been pushed onto the queue, clean up this db */
    if (db && rolledup) {
        if (sqlite3_exec(db,
                         "BEGIN TRANSACTION;"
                         "DROP TABLE pentries;"
                         "CREATE VIEW pentries AS SELECT entries.*, summary.inode AS pinode FROM entries, summary WHERE rectype = 0;"
                         "DELETE FROM summary WHERE isroot <> 1;"
                         "UPDATE summary SET rollupscore = 0 WHERE isroot == 1;"
                         "END TRANSACTION;",
                         NULL,
                         NULL,
                         &err) != SQLITE_OK) {
            fprintf(stderr, "Could not remove roll up data from \"%s\": %s\n", work->name, err);
            rc = 1;
        }
        sqlite3_free(err);
        err = NULL;
    }

    closedb(db);
    closedir(dir);

  free_work:
    free(work);

    return rc;
}

void sub_help() {
   printf("GUFI_index        GUFI index to unroll up\n");
   printf("\n");
}

int main(int argc, char * argv[]) {
    int idx = parse_cmd_line(argc, argv, "hHn:", 1, "GUFI_index ...", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    epoch = since_epoch(NULL);

    timestamp_init(timestamp_buffers, in.maxthreads + 1, 1024 * 1024, NULL);
    #endif

    struct QPTPool * pool = QPTPool_init(in.maxthreads
                                         #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                         , timestamp_buffers
                                         #endif
        );

    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        return -1;
    }

    if (QPTPool_start(pool, NULL) != (size_t) in.maxthreads) {
        fprintf(stderr, "Failed to start threads\n");
        return -1;
    }

    /* enqueue all input paths */
    for(int i = idx; i < argc; i++) {
        /* remove trailing slashes */
        size_t len = strlen(argv[i]);
        while (len && (argv[i][len] == '/')) {
            len--;
        }

        /* root is special case */
        if (len == 0) {
            len = 1;
        }

        struct Unrollup * mywork = malloc(sizeof(struct Unrollup));

        /* copy argv[i] into the work item */
        SNFORMAT_S(mywork->name, MAXPATH, 1, argv[i], len);

        /* assume index root was not rolled up */
        mywork->rolledup = 0;

        struct stat st;
        lstat(mywork->name, &st);
        if (!S_ISDIR(st.st_mode) ) {
            fprintf(stderr,"input-dir '%s' is not a directory\n", mywork->name);
            free(mywork);
            continue;
        }

        /* push the path onto the queue */
        QPTPool_enqueue(pool, i % in.maxthreads, processdir, mywork);
    }

    QPTPool_wait(pool);

    /* #ifdef DEBUG */
    /* const size_t thread_count = QPTPool_threads_completed(pool); */
    /* #endif */

    QPTPool_destroy(pool);

    #ifdef DEBUG
    #ifdef PER_THREAD_STATS
    timestamp_destroy(timestamp_buffers);
    #endif

    #endif

    return 0;
}
