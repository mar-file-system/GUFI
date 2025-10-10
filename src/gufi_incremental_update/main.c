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
#include "template_db.h"
#include "utils.h"

#include "gufi_incremental_update/PoolArgs.h"
#include "gufi_incremental_update/aggregate.h"
#include "gufi_incremental_update/incremental_update.h"

static int validate_source(struct PoolArgs *pa, struct work **tree, struct work **index) {
    /* get input path metadata */
    struct stat st;
    if (lstat(pa->orig.tree.data, &st) != 0) {
        fprintf(stderr, "Could not stat source directory \"%s\"\n", pa->orig.tree.data);
        return 1;
    }

    /* check that the source tree path is a directory */
    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Source tree path is not a directory \"%s\"\n", pa->orig.tree.data);
        return 1;
    }

    if (lstat(pa->orig.index.data, &st) != 0) {
        fprintf(stderr, "Could not stat index root \"%s\"\n", pa->orig.index.data);
        return 1;
    }

    /* check that the index path is a directory */
    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Index path is not a directory \"%s\"\n", pa->orig.index.data);
        return 1;
    }

    /* create work for the source tree */
    struct work *new_tree = new_work_with_name(NULL, 0, pa->tree.data, pa->tree.len);
    new_tree->orig_root = pa->orig.tree;
    new_tree->root_parent.data = pa->tree.data;
    new_tree->root_parent.len = pa->parent_len.tree;
    *tree = new_tree;

    if (!strcmp(pa->tree.data, pa->index.data)) {
        fprintf(stderr,"You are putting the index dbs in input directory\n");
        pa->same = 1;
    }
    else {
        /* source tree and index are different paths, so need to create work for index */
        struct work *new_index = new_work_with_name(NULL, 0, pa->index.data, pa->index.len);
        new_index->orig_root = pa->orig.index;
        new_index->root_parent.data = pa->index.data;
        new_index->root_parent.len = pa->parent_len.index;
        *index = new_index;
    }

    return 0;
}

static void sub_help(void) {
    printf("GUFI_tree         GUFI tree\n");
    printf("tree              source tree\n");
    printf("snapshotdb        database file containing records of all directories\n");
    printf("parking_lot       directory to place update db.dbs and moved directories\n");
    printf("\n");
    printf("GUFI_tree and tree may be the same path\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_THREADS, FLAG_DELIM,
        FLAG_SUSPECT_DIR, FLAG_SUSPECT_FILE_LINK,
        FLAG_SUSPECT_FILE, FLAG_SUSPECT_METHOD, FLAG_SUSPECT_TIME, FLAG_XATTRS,
        #ifdef HAVE_ZLIB
        FLAG_COMPRESS,
        #endif
        FLAG_END
    };

    struct PoolArgs pa = {0};
    process_args_and_maybe_exit(options, 4, "GUFI_tree tree snapshotdb parking_lot", &pa.in);

    /* parse positional args, following the options */
    /* not using in.name and in.nameto */
    INSTALL_STR(&pa.orig.index,  argv[argc - 4]);
    INSTALL_STR(&pa.orig.tree,   argv[argc - 3]);
    INSTALL_STR(&pa.in.outname,  argv[argc - 2]);
    INSTALL_STR(&pa.parking_lot, argv[argc - 1]);

    int rc = EXIT_SUCCESS;

    if (PoolArgs_init(&pa) != 0) {
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    /* make sure the parking lot exists */
    const int created_parking_lot = setup_parking_lot(pa.parking_lot.data);
    if (created_parking_lot < 0) {
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    struct work *tree = NULL;
    struct work *index = NULL;
    if (validate_source(&pa, &tree, &index) != 0) {
        rc = EXIT_FAILURE;
        goto remove_parking_lot;
    }

    /* if the index is in the tree, there is no need to get a snapshot of the index */
    if (pa.same == 0) {
        /*
         * get a snapshot of the existing index
         * (write to <snapshotdb>.index; not deleted afterwards for debugging)
         */
        rc = (gen_index_snapshot(&pa, index) == 0)?EXIT_SUCCESS:EXIT_FAILURE;
    }

    /*
     * get a snapshot of the current tree
     * (write to <snapshotdb>.tree; not deleted afterwards for debugging)
     * generate update dbs
     * (write to <parking lot>/<dir inode>)
     */
    if (rc == EXIT_SUCCESS) {
        rc = (find_suspects(&pa, tree) == 0)?EXIT_SUCCESS:EXIT_FAILURE;
    }

    QPTPool_wait(pa.pool);

    /* aggregate index results into one file */
    if (pa.same == 0) {
        aggregate_intermediate(&pa.agg_index, pa.in.maxthreads, 0);
        aggregate_fin(&pa.agg_index, pa.in.maxthreads);
    }

    /* aggregate tree results into one file */
    aggregate_intermediate(&pa.agg_tree, pa.in.maxthreads, pa.in.maxthreads);
    aggregate_fin(&pa.agg_tree, pa.in.maxthreads);
    close_template_db(&pa.db);

    if (rc == EXIT_SUCCESS) {
        /* if the index is in the tree, databases have already been created/updated */
        if (pa.same == 0) {
            /* do the incremental update */
            incremental_update(&pa);
        }
    }

  remove_parking_lot:
    cleanup_parking_lot(pa.parking_lot.data, created_parking_lot);

  cleanup:
    PoolArgs_fini(&pa);

    return rc;
}
