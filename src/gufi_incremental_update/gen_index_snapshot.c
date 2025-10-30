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
#include <dirent.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "config.h"
#include "dbutils.h"
#include "debug.h"
#include "descend.h"

#include "gufi_incremental_update/aggregate.h"
#include "gufi_incremental_update/incremental_update.h"

#define INDEX_SNAPSHOT_EXT "index"

size_t gen_index_snapshot_name(struct PoolArgs *pa, char *name, const size_t name_size) {
    return SNFORMAT_S(name, name_size, 3,
                      pa->in.outname.data, pa->in.outname.len,
                      ".", (size_t) 1,
                      INDEX_SNAPSHOT_EXT, sizeof(INDEX_SNAPSHOT_EXT) - 1);
}

static const char UPDATE_WORK_SQL[] = "SELECT inode, pinode FROM " SUMMARY " WHERE isroot == 1;";

static int update_work(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    struct work *work = (struct work *) args;
    return !((sscanf(data[0], "%" STAT_ino, &work->statuso.st_ino) == 1) &&
             (sscanf(data[1], "%lld",       &work->pinode)         == 1));
}

/* place directory data into per-thread database (merged later) */
static int processdir(QPTPool_ctx_t *ctx, void *data) {
    /* Not checking arguments */

    int rc = 0;

    const size_t id = QPTPool_get_id(ctx);
    struct PoolArgs *pa = (struct PoolArgs *) QPTPool_get_args_internal(ctx);
    struct work *work = NULL;
    DIR *dir = NULL;

    decompress_work(&work, data);

    dir = opendir(work->name);
    if (!dir) {
        const int err = errno;
        fprintf(stderr, "Error: Could not open directory \"%s\": %s (%d)\n",
                work->name, strerror(err), err);
        rc = 1;
        goto cleanup;
    }

    if (lstat_wrapper(work) != 0) {
        rc = 1;
        goto close_dir;
    }

    /* push more work first */
    descend(ctx, &pa->in, work, dir, 0,
            processdir, NULL, NULL, NULL);

    struct entry_data ed;
    memset(&ed, 0, sizeof(ed));
    ed.type = 'd';

    sqlite3 *db = pa->index.agg.dbs[id]; /* partial snapshot db */

    /* attach db.db from index to get information from index, not tree walk */
    char index_dbname[MAXPATH];
    SNFORMAT_S(index_dbname, sizeof(index_dbname), 3,
               work->name, work->name_len,
               "/", 1,
               DBNAME, DBNAME_LEN);

    if (!attachdb(index_dbname, db, "tree", SQLITE_OPEN_READONLY, 1)) {
        goto close_dir;
    }

    char *err = NULL;
    if (sqlite3_exec(db, UPDATE_WORK_SQL, update_work, work, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Could not pull data from index at \"%s\": %s\n",
                                  work->name, err);
        rc = 1;
        goto detach_db;
    }

    /* insert this directory into the index snapshot db */
    sqlite3_stmt *res = insertdbprep(db, SNAPSHOT_INSERT);
    insert_snapshot_row(work, &ed, res, pa->index.parent_len);
    sqlite3_finalize(res);

  detach_db:
    detachdb(index_dbname, db, "tree", 1);

  close_dir:
    closedir(dir);

  cleanup:
    free(work);

    return rc;
}

int gen_index_snapshot(struct PoolArgs *pa, struct work *work) {
    char index_snapshot_name[MAXPATH];
    gen_index_snapshot_name(pa, index_snapshot_name, sizeof(index_snapshot_name));

    /* set up per-thread databases to write to */
    if (aggregate_init(&pa->index.agg, pa->in.maxthreads, index_snapshot_name, 0) != 0) {
        return 1;
    }

    fprintf(stdout, "Pulling directory data from index \"%s\" with %zu threads\n",
            pa->index.path.data, pa->in.maxthreads);
    fflush(stdout);

    /* walk the old index and get snapshot of directories */
    QPTPool_enqueue(pa->ctx, processdir, work);

    return 0;
}
