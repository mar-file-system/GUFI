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
#include "utils.h"

#include "gufi_incremental_update/aggregate.h"
#include "gufi_incremental_update/incremental_update.h"

#define TREE_SNAPSHOT_EXT "tree"

size_t gen_tree_snapshot_name(struct PoolArgs *pa, char *name, const size_t name_size) {
    return SNFORMAT_S(name, name_size, 3,
                      pa->in.outname.data, pa->in.outname.len,
                      ".", (size_t) 1,
                      TREE_SNAPSHOT_EXT, sizeof(TREE_SNAPSHOT_EXT) - 1);
}

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
    sqlite3_stmt *res;     /* used for inserting into snapshot db */
};

static int compare_suspect_time(struct work *work, const time_t suspect_time) {
    if (lstat_wrapper(work) != 0) {
        return 1; /* something broke - try to reindex */
    }

    return ((work->statuso.st_ctime >= suspect_time) ||
            (work->statuso.st_mtime >= suspect_time));
}

/* check if any non-directory were changed */
static int process_nondir(struct work *nondir, struct entry_data *ed, void *nondir_args) {
    struct NonDirArgs *nda = (struct NonDirArgs *) nondir_args;

    if (ed->suspect == 0) {
        /* mark the parent directory as suspect */
        switch (nda->pa->in.suspectmethod) {
            /* case 0: /\* no suspects *\/ */
            /*     /\* do nothing - this was already done by processdir *\/ */
            /*     break; */
            case 1: /* suspect directories/files/links (from suspect file) */
                {
                    /* check if this file/link inode shows up in the suspect file */
                    const int found = find_inode(&nda->pa->suspects.fl, nondir->statuso.st_ino);
                    if (found && nda->pa->in.suspectstat) {
                        nda->ed->suspect |= compare_suspect_time(nondir, nda->pa->in.suspecttime);
                    }
                    else {
                        nda->ed->suspect |= found;
                    }
                }
                break;
            case 3: /* compare file/link timestamps with given suspect time */
                /* don't overwrite ed.suspect if the ctime/mtime are not newer than the suspect time */
                nda->ed->suspect |= compare_suspect_time(nondir, nda->pa->in.suspecttime);
                break;
            /* no default */
        }
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

    struct entry_data ed;
    memset(&ed, 0, sizeof(ed));
    ed.type = 'd';
    ed.suspect = 0;

    sqlite3 *db = pa->tree.agg.dbs[id]; /* partial snapshot db */
    sqlite3_stmt *res = insertdbprep(db, SNAPSHOT_INSERT);

    if (ed.suspect == 0) {
        switch (pa->in.suspectmethod) {
            /* case 0: /\* no suspects *\/ */
            /*     /\* do nothing - just insert record *\/ */
            /*     break; */
            case 1: /* suspect directories/files/links (from suspect file) */
                {
                    const int found = find_inode(&pa->suspects.dir, work->statuso.st_ino);
                    if (found && pa->in.suspectstat) {
                        ed.suspect |= compare_suspect_time(work, pa->in.suspecttime);
                    }
                    else {
                        ed.suspect |= found;
                    }
                }
                break;
            case 3: /* compare directory timestamps with suspect time */
                /* don't overwrite ed.suspect if the ctime/mtime are not newer than the suspect time */
                ed.suspect |= compare_suspect_time(work, pa->in.suspecttime);
                break;
            /* no default */
        }
    }

    struct NonDirArgs nda = {
        .pa  = pa,
        .ed  = &ed,
        .res = res,
    };

    /* only process files/links if checking for file/link suspects or comparing timestamps */
    process_nondir_f func = (pa->in.suspectmethod > 1)?process_nondir:NULL;
    descend(ctx, &pa->in, work, dir, 0,
            processdir, func, &nda, NULL);

    /*
     * if this directory is a suspect, insert it
     *
     * if this directory is not a suspect, insert it anyways so that
     * it is not treated as having been deleted
     */
    insert_snapshot_row(work, &ed, res, pa->tree.parent_len);

    sqlite3_finalize(res);

    /* reindex the directory if it needs to be reindexed */
    if (ed.suspect == 1) {
        reindex_dir(pa, work, &ed, dir);
    }

    closedir(dir);

  cleanup:
    free(work);

    return rc;
}

int find_suspects(struct PoolArgs *pa, struct work *work) {
    /* parking lot already set up */

    char tree_snapshot_name[MAXPATH];
    gen_tree_snapshot_name(pa, tree_snapshot_name, sizeof(tree_snapshot_name));

    /* set up per-thread databases to write to */
    if (aggregate_init(&pa->tree.agg, pa->in.maxthreads, tree_snapshot_name, pa->in.maxthreads) != 0) {
        free(work);
        return 1;
    }

    /* set up template db.db for copying instead of running SQL to create each table */
    init_template_db(&pa->db);
    if (create_dbdb_template(&pa->db) != 0) {
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
