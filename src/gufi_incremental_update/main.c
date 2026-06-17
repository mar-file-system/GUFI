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
#include <string.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "debug.h"
#include "descend.h"
#include "plugin.h"
#include "str.h"
#include "template_db.h"
#include "utils.h"

#include "gufi_incremental_update/PoolArgs.h"
#include "gufi_incremental_update/aggregate.h"
#include "gufi_incremental_update/incremental_update.h"

static int validate_path(const char *type, const str_t *path,
                         const size_t parent_len,
                         struct work **work) {
    *work = NULL;

    /* get input path metadata */
    struct stat st;
    if (lstat(path->data, &st) != 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not stat %s \"%s\": %s (%d)\n",
                type, path->data, strerror(err), err);
        return 1;
    }

    /* check that the source tree path is a directory */
    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Error: %s path is not a directory \"%s\"\n", type, path->data);
        return 1;
    }

    /* create work for the source tree */
    struct work *new_work = new_work_with_name(NULL, 0, path->data, path->len);
    new_work->orig_root = *path;
    new_work->root_parent.data = path->data;
    new_work->root_parent.len = parent_len;
    new_work->statuso = st;
    new_work->stat_called = NOT_STATX_CALLED;

    *work = new_work;

    return 0;
}

static int validate_source(struct PoolArgs *pa, struct work **index, struct work **tree) {
    if (validate_path("tree", &pa->tree.path, pa->tree.parent_len, tree) != 0) {
        return 1;
    }

    if (!strcmp(pa->tree.path.data, pa->index.path.data)) {
        fprintf(stderr,"You are putting the index dbs in input directory\n");
        pa->same = 1;
    }
    else {
        if (validate_path("index", &pa->index.path, pa->index.parent_len, index) != 0) {
            free(*tree);
            *tree = NULL;
            return 1;
        }
    }

    return 0;
}

static int compare_and_update(struct PoolArgs *pa, struct work *index, struct work *tree) {
    fprintf(stdout, "--------------------\n");
    fprintf(stdout, "Processing top of changed subtree: %s\n", tree->name);

    int rc = 0;

    /* if the index is in the tree, there is no need to get a snapshot of the index */
    if (pa->same == 0) {
        /*
         * get a snapshot of the existing index
         * (write to <snapshotdb>.index)
         */
        rc = gen_index_snapshot(pa, index);
    }

    /*
     * get a snapshot of the current tree
     * (write to <snapshotdb>.tree)
     * generate update dbs
     * (write to <parking lot>/<dir inode>)
     */
    if (rc == 0) {
        rc = find_suspects(pa, tree);
    }
    else {
        free(tree);
    }

    QPTPool_wait(pa->ctx);

    /* aggregate index results into one file */
    if (pa->same == 0) {
        aggregate_intermediate(&pa->index.agg, pa->in.maxthreads, 0);
        aggregate_fin(&pa->index.agg, pa->in.maxthreads);
    }

    /* aggregate tree results into one file */
    aggregate_intermediate(&pa->tree.agg, pa->in.maxthreads, pa->in.maxthreads);
    aggregate_fin(&pa->tree.agg, pa->in.maxthreads);
    close_template_db(&pa->db);

    if (rc == 0) {
        /* if the index is in the tree, databases have already been created/updated */
        if (pa->same == 0) {
            /* do the incremental update */
            incremental_update(pa);
        }
    }

    /* clean up artifacts */
    delete_artifact(pa->diff.data);
    delete_artifact(pa->tree.snapshot.data);
    delete_artifact(pa->index.snapshot.data);

    fprintf(stdout, "--------------------\n");

    return rc;
}

static int find_top(QPTPool_ctx_t *ctx, void *data) {
    struct PoolArgs *pa = NULL;
    QPTPool_get_args(ctx, (void **) &pa);

    struct work *tree = (struct work *) data; /* source tree */

    DIR *dir = opendir_wrapper(tree->name, NULL);
    if (!dir) {
        goto free_work;
    }

    /* not deep enough - descend */
    if (tree->level < pa->in.min_level) {
        descend(ctx,
                &pa->in, tree,
                dir, 1,
                find_top, NULL, NULL,
                NULL);
        goto close_dir;
    }

    int suspect = 0;

    /* make sure timestamps are available before calling is_suspect() */
    time_t crtime = 0; /* unused */
    if (lstat_wrapper(tree->name, &tree->statuso, &crtime,
                      &tree->stat_called, 1, NULL) != 0) {
        suspect = 1; /* assume something changed? */
    }

    /*
     * FIXME: if only doing mtime/ctime check (no suspect file/inodes), will miss
     * when files are modified because directory mtime/ctime are not updated
     */
    suspect |= is_suspect(pa->in.suspect.method,
                          &pa->suspects.dir,
                          pa->in.suspect.stat,
                          pa->in.suspect.time,
                          tree);

    /* found top - do incremental update on subtree */
    if (suspect) {
        const size_t id = QPTPool_get_id(ctx);
        sll_push_back(&pa->tops[id], tree);
        tree = NULL; /* freed later */
    }
    else {
        /* unchanged, so descend */
        descend(ctx,
                &pa->in, tree,
                dir, 1,
                find_top, NULL, NULL,
                NULL);
    }

  close_dir:
    closedir(dir);
  free_work:
    free(tree);

    return 0;
}

static void sub_help(void) {
    printf("GUFI_tree         GUFI tree\n");
    printf("dir               source tree\n");
    printf("parking_lot       directory prefix to place update db.dbs and moved directories\n");
    printf("\n");
    printf("GUFI_tree and tree may be the same path\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_THREADS,

        /* processing flags */
        FLAG_SUSPECT_STAT, FLAG_SUSPECT_FILE, FLAG_SUSPECT_METHOD,
        FLAG_SUSPECT_TIME, FLAG_SNAPSHOT_PREFIX,

        FLAG_INDEX_XATTRS, FLAG_PLUGIN,

        /* memory usage flags */
        #ifdef HAVE_ZLIB
        FLAG_COMPRESS,
        #endif

        FLAG_END
    };

    struct PoolArgs pa = {0};
    process_args_and_maybe_exit(options, 3, "GUFI_tree dir parking_lot", &pa.in);

    /* fail early */
    if (plugins_check_type(&pa.in.plugins, PLUGIN_INCREMENTAL) != pa.in.plugins.count) {
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    if (plugins_global_init(&pa.in.plugins, &pa.in) != pa.in.plugins.count) {
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    /* parse positional args, following the options */
    INSTALL_STR(&pa.index.path,  pa.in.pos.argv[pa.in.pos.argc - 3]);
    INSTALL_STR(&pa.tree.path,   pa.in.pos.argv[pa.in.pos.argc - 2]);
    INSTALL_STR(&pa.parking_lot, pa.in.pos.argv[pa.in.pos.argc - 1]);

    int rc = 0;

    if (PoolArgs_init(&pa) != 0) {
        rc = 1;
        goto cleanup;
    }

    /* make sure the parking lot exists */
    const int created_parking_lot = setup_parking_lot(pa.parking_lot.data);
    if (created_parking_lot < 0) {
        rc = 1;
        goto cleanup;
    }

    struct work *index = NULL;
    struct work *tree = NULL;
    if ((rc = validate_source(&pa, &index, &tree)) == 0) {
        /* get tops of all subtrees that changed */
        QPTPool_enqueue(pa.ctx, find_top, tree);
        QPTPool_wait(pa.ctx);

        /*
         * run (parallel) incremental update on subtrees one at a time
         * so that there are not pa.in.maxthreads in-memory dbs per
         * subtree being processed at once
         *
         */
        for(size_t i = 0; i < pa.in.maxthreads; i++) {
            sll_loop(&pa.tops[i], node) {
                struct work *subtree = (struct work *) sll_node_data(node);

                /* jump into index */
                struct work *subindex = NULL;
                if (pa.same == 0) {
                    subindex = new_work_with_name(index->name, index->name_len - index->root_parent.len,
                                                  subtree->name + pa.tree.parent_len,
                                                  subtree->name_len - pa.tree.parent_len);
                }

                rc |= compare_and_update(&pa, subindex, subtree);
            }
        }

        free(index);
        /* tree would have been freed in find_top or compare_and_update */
    }

    cleanup_parking_lot(pa.parking_lot.data, created_parking_lot);

  cleanup:
    PoolArgs_fini(&pa);

    return (rc == 0)?EXIT_SUCCESS:EXIT_FAILURE;
}
