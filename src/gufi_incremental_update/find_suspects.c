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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "debug.h"
#include "template_db.h"
#include "str.h"
#include "utils.h"

#include "gufi_incremental_update/aggregate.h"
#include "gufi_incremental_update/incremental_update.h"

#define TREE_SNAPSHOT_EXT "tree"

/* search the list of suspect inodes */
static int find_inode(struct SuspectInodes *suspectinodes, const ino_t inode) {
    if (inode < suspectinodes->min) { return 0; }
    if (inode > suspectinodes->max) { return 0; }

    char inode_str[32];
    const size_t len = SNPRINTF(inode_str, sizeof(inode_str), "%" STAT_ino, inode);
    return trie_search(suspectinodes->inodes, inode_str, len, NULL);
}

struct NonDirArgs {
    struct PoolArgs *pa;
    struct entry_data *ed; /* current directory's data */
};

static int compare_suspect_time(struct work *work, const time_t suspect_time) {
    if (lstat_wrapper(work->name, &work->statuso, &work->crtime,
                      &work->stat_called, 1, 1) != 0) {
        return 1; /* something broke - try to reindex */
    }

    return ((work->statuso.st_ctime >= suspect_time) ||
            (work->statuso.st_mtime >= suspect_time));
}

int is_suspect(const int suspectmethod,
               struct SuspectInodes *suspectinodes,
               const int suspectstat,
               const time_t suspecttime,
               struct work *work) {
    switch (suspectmethod) {
        /* case 0: /\* no suspects *\/ */
        /*     break; */
        case 1: /* suspect directories/files/links (from suspect file) */
            {
                /* check if this inode shows up in the suspect file */
                const int found = find_inode(suspectinodes, work->statuso.st_ino);
                if (found && suspectstat) {
                    return compare_suspect_time(work, suspecttime);
                }
                return found;
            }
        case 3: /* compare timestamps with given suspect time */
            return compare_suspect_time(work, suspecttime);
        /* no default */
    }

    return 0;
}

/* check if any non-directory were changed */
static int process_nondir(struct work *nondir, struct entry_data *ed, void *nondir_args) {
    struct NonDirArgs *nda = (struct NonDirArgs *) nondir_args;
    struct PoolArgs *pa = nda->pa;

    if (ed->suspect == 0) {
        /* mark the parent directory as suspect */
        /* OR with existing suspect value instead of overwriting */
        nda->ed->suspect |= is_suspect(pa->in.suspect.method,
                                       &pa->suspects.fl,
                                       pa->in.suspect.stat,
                                       pa->in.suspect.time,
                                       nondir);
    }

    return 0;
}

/*
 * walk the source filesystem tree to get a list of all directories
 * if a directory is suspected to have been modified, reindex that one directory
 */
static int processdir(QPTPool_ctx_t *ctx, void *data) {
    /* Not checking arguments */

    int rc = 0;

    struct PoolArgs *pa = (struct PoolArgs *) QPTPool_get_args_internal(ctx);
    struct work *work = NULL;
    DIR *dir = NULL;

    decompress_work(&work, data);

    dir = opendir_wrapper(work->name, 1);
    if (!dir) {
        rc = 1;
        goto cleanup;
    }

    struct entry_data ed;
    memset(&ed, 0, sizeof(ed));
    ed.type = 'd';

    /* set whether or not this directory is suspect before processing children */
    ed.suspect = is_suspect(pa->in.suspect.method,
                            &pa->suspects.dir,
                            pa->in.suspect.stat,
                            pa->in.suspect.time,
                            work);

    struct NonDirArgs nda = {
        .pa  = pa,
        .ed  = &ed,
    };

    /* only process files/links if checking for file/link suspects or comparing timestamps */
    process_nondir_f func = (pa->in.suspect.method > 1)?process_nondir:NULL;
    descend(ctx, &pa->in, work, dir, 0,
            processdir, func, &nda, NULL);

    const size_t id = QPTPool_get_id(ctx);

    if (pa->same == 0) {
        /*
         * if this directory is a suspect, insert it
         *
         * if this directory is not a suspect, insert it anyways so that
         * it is not treated as having been deleted
         */
        sqlite3 *db = pa->tree.agg.dbs[id]; /* partial snapshot db */
        sqlite3_stmt *res = insertdbprep(db, SNAPSHOT_INSERT);
        insert_snapshot_row(work, &ed, res, pa->tree.parent_len);
        sqlite3_finalize(res);
    }

    /* reindex the directory if it needs to be reindexed */
    if (ed.suspect == 1) {
        reindex_dir(pa, work, &ed, dir, id);
    }

    closedir(dir);

  cleanup:
    free(work);

    return rc;
}

int find_suspects(struct PoolArgs *pa, struct work *work) {
    /* parking lot already set up */

    if (pa->same == 0) {
        str_alloc_existing(&pa->tree.snapshot, pa->in.outname.len + 1 + sizeof(TREE_SNAPSHOT_EXT) - 1);
        SNFORMAT_S(pa->tree.snapshot.data, pa->tree.snapshot.len + 1, 3,
                   pa->in.outname.data, pa->in.outname.len,
                   ".", (size_t) 1,
                   TREE_SNAPSHOT_EXT, sizeof(TREE_SNAPSHOT_EXT) - 1);
    }

    /* set up per-thread databases to write to */
    if (aggregate_init(&pa->tree.agg, pa->in.maxthreads, pa->tree.snapshot.data, pa->in.maxthreads) != 0) {
        free(work);
        str_free_existing(&pa->tree.snapshot);
        return 1;
    }

    /* set up template db.db for copying instead of running SQL to create each table */
    init_template_db(&pa->db);
    if (create_dbdb_template(&pa->db, NULL) != 0) {
        fprintf(stderr, "Could not create template file\n");
        aggregate_fin(&pa->tree.agg, pa->in.maxthreads);
        free(work);
        return 1;
    }

    fprintf(stdout, "Scanning current state of \"%s\" with %zu threads\n",
            pa->tree.path.data, pa->in.maxthreads);
    fflush(stdout);

    /*
     * do tree walk
     * get per-thread treewalk records
     * put per-directory update dbs into parking lot
     */
    QPTPool_enqueue(pa->ctx, processdir, work);

    return 0;
}
