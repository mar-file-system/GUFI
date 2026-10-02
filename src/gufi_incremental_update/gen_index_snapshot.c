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
#include "str.h"

#include "gufi_incremental_update/aggregate.h"
#include "gufi_incremental_update/incremental_update.h"

#define INDEX_SNAPSHOT_EXT "index"

static const char UPDATE_WORK_SQL[] = "SELECT inode, pinode FROM " SUMMARY " WHERE isroot == 1;";

static int update_work(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    struct work *work = (struct work *) args;
    return !((sscanf(data[0], "%" STAT_ino, &work->statuso.st_ino) == 1) &&
             (sscanf(data[1], "%" STAT_ino, &work->pinode)         == 1));
}

/* place directory data into per-thread database (merged later) */
static int processdir(QPTPool_ctx_t *ctx, void *data) {
    /* Not checking arguments */

    int rc = 0;

    const size_t id = QPTPool_get_id(ctx);
    struct PoolArgs *pa = (struct PoolArgs *) QPTPool_get_args_internal(ctx);
    struct GenSnapshot *index = (struct GenSnapshot *) data;
    struct work *work = index->work; /* no compression */

    DIR *dir = opendir(work->name);
    if (!dir) {
        const int err = errno;
        if (err != ENOENT) { /* new directory in source tree but not yet in index */
            fprintf(stderr, "Error: Could not open directory \"%s\": %s (%d)\n",
                    work->name, strerror(err), err);
        }
        rc = 1;
        goto cleanup;
    }

    if (lstat_wrapper(work->name, &work->statuso, &work->crtime,
                      &work->stat_called, 1, NULL) != 0) {
        rc = 1;
        goto close_dir;
    }

    struct descend_counters ctrs = {0};
    descend(ctx, &pa->in, work, dir, 0,
            try_skip_lstat, wrap_work, index,
            processdir, NULL, NULL,
            &ctrs);

    struct entry_data ed;
    memset(&ed, 0, sizeof(ed));
    ed.type = 'd';

    sqlite3 *db = index->agg.dbs[id]; /* partial snapshot db */

    /* attach db.db from index to get information from index, not tree walk */
    const size_t index_dbname_len = work->name_len + 1 + DBNAME_LEN;
    char *index_dbname = malloc(index_dbname_len + 1);
    SNFORMAT_S(index_dbname, index_dbname_len + 1, 3,
               work->name, work->name_len,
               "/", 1,
               DBNAME, DBNAME_LEN);

    /* TODO: keep track of uri so only basename needs converting */
    const size_t attach_name_size = index_dbname_len * 3 + 1;
    char *attach_name = malloc(attach_name_size);
    size_t converted_len = index_dbname_len; /* unused */
    const size_t attach_name_len = sqlite_uri_path(attach_name, attach_name_size,
                                                   index_dbname, &converted_len);
    attach_name[attach_name_len] = '\0';
    free(index_dbname);

    if (!attachdb(attach_name, db, "tree", SQLITE_OPEN_READONLY, 1, NULL)) {
        goto free_attach_name;
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
    insert_snapshot_row(work, &ed, res, index->parent_len);
    sqlite3_finalize(res);

  detach_db:
    detachdb(attach_name, db, "tree", 1, NULL);

  free_attach_name:
    free(attach_name);

  close_dir:
    closedir(dir);

  cleanup:
    pthread_mutex_lock(index->mutex);
    --(*index->counter);
    pthread_cond_broadcast(index->cond);
    pthread_mutex_unlock(index->mutex);

    if (index->free_work) {
        index->free_work(index->work);
    }
    free(index);

    return rc;
}

int gen_index_snapshot(struct PoolArgs *pa, const ino_t inode, struct GenSnapshot *index) {
    str_alloc_existing(&index->snapshot, pa->artifacts.len + 1 + UINT64_DIGITS + 1 + sizeof(INDEX_SNAPSHOT_EXT) - 1);
    SNPRINTF(index->snapshot.data, index->snapshot.len + 1,
             "%s/%" STAT_ino "." INDEX_SNAPSHOT_EXT,
             pa->artifacts.data, inode);

    /* set up per-thread databases to write to */
    if (aggregate_init(&index->agg, pa->in.maxthreads, index->snapshot.data, 0) != 0) {
        str_free_existing(&index->snapshot);
        return 1;
    }

    fprintf(stdout, "Pulling directory data from index \"%s\" with %zu threads\n",
            index->work->name, pa->in.maxthreads);

    /* clone the original struct so that it can be freed without affecting the original */
    struct GenSnapshot *copy = malloc(sizeof(*copy));
    *copy = *index;
    copy->free_work = NULL;
    copy->snapshot.free = NULL;

    /* walk the old index and get snapshot of directories */
    QPTPool_enqueue(pa->ctx, processdir, copy);

    return 0;
}
