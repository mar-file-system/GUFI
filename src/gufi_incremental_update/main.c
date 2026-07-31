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
                         const size_t parent_len, struct work **work) {
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

static int validate_source(str_t *argv_index, struct GenSnapshot *index,
                           str_t *argv_tree,  struct GenSnapshot *tree,
                           int *same) {
    {
        char *real_index = realpath(argv_index->data, NULL);
        if (!real_index) {
            const int err = errno;
            fprintf(stderr, "Error: Could not get realpath of \"%s\": %s (%d)\n",
                    argv_index->data, strerror(err), err);
            return 1;
        }

        char *real_tree = realpath(argv_tree->data, NULL);
        if (!real_tree) {
            const int err = errno;
            fprintf(stderr, "Error: Could not get realpath of \"%s\": %s (%d)\n",
                    argv_tree->data, strerror(err), err);
            free(real_index);
            return 1;
        }

        *same = !strcmp(real_tree, real_index); /* both strings are NULL terminated, so not getting lengths */

        /* not keeping real paths */
        free(real_index);
        free(real_tree);
    }

    if (*same) {
        fprintf(stderr, "You are putting the index dbs in input directory\n");
    }
    else {
        index->parent_len = dirname_len(argv_index->data, argv_index->len);
        if (validate_path("index", argv_index, index->parent_len, &index->work) != 0) {
            return 1;
        }
    }

    tree->parent_len = dirname_len(argv_tree->data, argv_tree->len);
    if (validate_path("tree", argv_tree, tree->parent_len, &tree->work) != 0) {
        free(index->work);
        index->work = NULL;
        return 1;
    }

    return 0;
}

static int compare_and_update(struct PoolArgs *pa,
                              const ino_t inode,
                              struct GenSnapshot *index,
                              struct GenSnapshot *tree) {
    fprintf(stdout, "--------------------\n");
    fprintf(stdout, "Processing top of changed subtree: %s\n", tree->work->name);

    int rc = 0;

    pthread_mutex_t mutex;
    pthread_mutex_init(&mutex, NULL);

    pthread_cond_t cond;
    pthread_cond_init(&cond, NULL);

    size_t counter = 0;

    /* if the index is in the tree, there is no need to get a snapshot of the index */
    if (pa->same == 0) {
        ++counter;

        index->mutex = &mutex;
        index->cond = &cond;
        index->counter = &counter;

        /* use the tree's inode for the snapshot name (this gets overwritten by a second lstat_wrapper call) */
        index->work->statuso.st_ino = inode;

        /*
         * get a snapshot of the existing index
         * (write to <artifacts dir>/<inode>.index)
         */
        rc = gen_index_snapshot(pa, inode, index);
    }

    tree->mutex = &mutex;
    tree->cond = &cond;
    tree->counter = &counter;

    /*
     * get a snapshot of the current tree
     * (write to <artifacts dir>/<inode>.tree)
     * generate update dbs
     * (write to <parking lot>/<dir inode>)
     */
    if (rc == 0) {
        pthread_mutex_lock(&mutex);
        ++counter;
        pthread_mutex_unlock(&mutex);
        rc = find_suspects(pa, inode, tree);
    }
    else {
        free(tree->work);
        tree->work = NULL;
    }

    if (rc != 0) {
        pthread_mutex_lock(&mutex);
        --counter;
        pthread_mutex_unlock(&mutex);
    }

    pthread_mutex_lock(&mutex);
    while (counter) {
        pthread_cond_wait(&cond, &mutex);
    }
    pthread_mutex_unlock(&mutex);

    pthread_cond_destroy(&cond);
    pthread_mutex_destroy(&mutex);

    /* aggregate index results into one file */
    if (pa->same == 0) {
        aggregate_intermediate(&index->agg, pa->in.maxthreads, 0);
        aggregate_fin(&index->agg, pa->in.maxthreads);
    }

    /* aggregate tree results into one file */
    aggregate_intermediate(&tree->agg, pa->in.maxthreads, pa->in.maxthreads);
    aggregate_fin(&tree->agg, pa->in.maxthreads);

    if (rc == 0) {
        /* if the index is in the tree, databases have already been created/updated */
        if (pa->same == 0) {
            /* do the incremental update */
            incremental_update(pa, inode, index, tree);
        }
    }

    /* clean up artifacts */
    if (!pa->in.artifacts.keep) {
        delete_artifact(tree->snapshot.data);
    }
    str_free_existing(&tree->snapshot);
    free(tree->work);
    free(tree);

    if (pa->same == 0) {
        if (!pa->in.artifacts.keep) {
            delete_artifact(index->snapshot.data);
        }
        str_free_existing(&index->snapshot);
        free(index->work);
        free(index);
    }

    fprintf(stdout, "--------------------\n");

    pthread_mutex_lock(&pa->mutex);
    --pa->active;
    pthread_mutex_unlock(&pa->mutex);

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
                NULL, NULL,
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
                NULL, NULL,
                find_top, NULL, NULL,
                NULL);
    }

  close_dir:
    closedir(dir);
  free_work:
    free(tree);

    return 0;
}

static int handle_artifacts_dir(struct PoolArgs *pa, const int created_parking_lot) {
    if (pa->in.artifacts.keep) {
        /* artifacts directory must already exist */
        char *real_artifacts = realpath(pa->in.artifacts.dir.data, NULL);
        if (!real_artifacts) {
            const int err = errno;
            fprintf(stderr, "Error: Could not get realpath of \"%s\": %s (%d)\n",
                    pa->in.artifacts.dir.data, strerror(err), err);
            return 1;
        }

        /*
         * if the parking lot was created by main(), do not allow for
         * the artifacts to be placed there since the parking lot will
         * be deleted at the end
         *
         * if the parking lot already existed, let the artifacts be
         * placed there since the parking lot will not be deleted at
         * the end
         */
        if (created_parking_lot) {
            /* this should never fail because the parking lot already exists */
            char *real_parking_lot = realpath(pa->parking_lot.data, NULL);

            const int same = !strcmp(real_artifacts, real_parking_lot);  /* both strings are NULL terminated, so not getting lengths */
            free(real_parking_lot);

            if (same) {
                fprintf(stderr, "Error: Refusing to save artifacts to the parking lot directory \"%s\" because it will be deleted at the end\n",
                        pa->in.artifacts.dir.data);
                free(real_artifacts);
                return 1;
            }
        }

        free(real_artifacts);

        if (access(pa->in.artifacts.dir.data, W_OK | X_OK) != 0) {
            const int err = errno;
            fprintf(stderr, "Error: Cannot place artifacts into \"%s\": %s (%d)\n",
                    pa->in.artifacts.dir.data, strerror(err), err);
            return 1;
        }

        pa->artifacts = pa->in.artifacts.dir;
    }
    else {
        pa->artifacts = pa->parking_lot;
    }

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
        FLAG_SUSPECT_TIME, FLAG_MAX_SUBTREES, FLAG_KEEP_ARTIFACTS,

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
    if (plugins_check_type(&pa.in.plugins, PLUGIN_INDEX) != pa.in.plugins.count) {
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    if (plugins_global_init(&pa.in.plugins, &pa.in) != pa.in.plugins.count) {
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    /* parse positional args, following the options */
    str_t argv_index;
    str_t argv_tree;
    INSTALL_STR(&argv_index,     pa.in.pos.argv[pa.in.pos.argc - 3]);
    INSTALL_STR(&argv_tree,      pa.in.pos.argv[pa.in.pos.argc - 2]);
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

    /* have to do this after creating the parking lot so that realpath can get the path */
    if (handle_artifacts_dir(&pa, created_parking_lot) != 0) {
        rc = 1;
        goto cleanup_pl;
    }

    struct GenSnapshot index = {0}; /* used for entire lifetime of run, if set */

    /* tree.work
     *     set by validate_source
     *     sent into find_top
     *         stored as top of subtree if suspected to have changed
     *         freed if not suspected to have changed, and children spawned
     *     either way, tree.work is not valid after find_top
     *     if subtrees were found, they will be passed to compare_and_update/find_suspects and freed
     */
    struct GenSnapshot tree = {0};
    if ((rc = validate_source(&argv_index, &index, &argv_tree, &tree, &pa.same)) == 0) {
        /* get tops of all subtrees that changed */
        QPTPool_enqueue(pa.ctx, find_top, tree.work);
        QPTPool_wait(pa.ctx);

        /* tree.work is no longer valid */
        tree.work = NULL;

        /* comparison is >, so subtract 1 to get correct wait condition */
        --pa.in.max_subtrees;

        int has_slash = 0;
        if (pa.same == 0) {
            if (index.work->root_parent.len) {
                has_slash = (index.work->name[index.work->root_parent.len - 1] == '/');
            }
        }

        /*
         * run (parallel) incremental update on subtrees one at a time
         * so that there are not pa.in.maxthreads in-memory dbs per
         * subtree being processed at once
         */
        for(size_t i = 0; i < pa.in.maxthreads; i++) {
            sll_loop(&pa.tops[i], node) {
                pthread_mutex_lock(&pa.mutex);

                /*
                 * pa.in.max_subtrees is needed because there is no
                 * way to predict how many aggregation dbs are needed
                 * for a particular subtree
                 */
                while (pa.active > pa.in.max_subtrees) {
                    pthread_cond_wait(&pa.cond, &pa.mutex);
                }

                ++pa.active;
                pthread_mutex_unlock(&pa.mutex);

                struct GenSnapshot *subtree = malloc(sizeof(*subtree));
                *subtree = tree;
                subtree->work = (struct work *) sll_node_data(node);

                /* jump into index */
                struct GenSnapshot *subindex = NULL;
                if (pa.same == 0) {
                    subindex = malloc(sizeof(*subindex));
                    *subindex = index;
                    subindex->work = new_work_with_name(index.work->name, index.work->root_parent.len - has_slash,
                                                        subtree->work->name + tree.parent_len,
                                                        subtree->work->name_len - tree.parent_len);
                }

                rc |= compare_and_update(&pa, subtree->work->statuso.st_ino, subindex, subtree);
            }
        }

        free(index.work);
        /* tree.work would have been freed in find_top or compare_and_update */
    }

  cleanup_pl:
    cleanup_parking_lot(pa.parking_lot.data, created_parking_lot);

  cleanup:
    PoolArgs_fini(&pa);

    return (rc == 0)?EXIT_SUCCESS:EXIT_FAILURE;
}
