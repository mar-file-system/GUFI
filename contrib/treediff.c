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



#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdlib.h>
#include <string.h>

#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "bf.h"
#include "print.h"
#include "trie.h"
#include "utils.h"

/*
 * M. Sowjanya, V. Kiran, "Quick and efficient algorithm to compare trees," Journal of University of Shanghai for Science and Technology, vol. 23, issue 7, pp. 437-441, July 2021. Available: https://jusst.org/wp-content/uploads/2021/07/Quick-and-Efficient-Algorithm-to-Compare-Trees.pdf
 */

struct PoolArgs {
    struct input in;
    struct OutputBuffers obufs;
};

/*
 * treated as POD and simply copy instead of
 * doing 2 dynamic allocations per instance
 */
typedef struct str {
    char *data;
    size_t len;
} str_t;

static inline void str_copy_construct(str_t *dst, const char *str, const size_t len) {
    dst->len = len;
    dst->data = malloc(len + 1);
    SNFORMAT_S(dst->data, dst->len + 1, 1, str, len);
}

static inline void str_destruct(str_t *str) {
    free(str->data);
}

static int str_cmp(const void *l, const void *r) {
    str_t **lhs = (str_t **) l;
    str_t **rhs = (str_t **) r;

    const size_t len = ((*lhs)->len < (*rhs)->len)?(*lhs)->len:(*rhs)->len;
    return strncmp((*lhs)->data, (*rhs)->data, len + 1);
}

static int get_entries(DIR *dir, trie_t *skip_db,
                       sll_t *subdirs) {
    struct dirent *entry = NULL;
    while ((entry = readdir(dir))) {
        const size_t len = strlen(entry->d_name);

        /* skip . and .. and *.db */
        const int skip = (trie_search(skip_db, entry->d_name, len, NULL) ||
                          ((len >= 3) && (strncmp(entry->d_name + len - 3, ".db", 3) == 0)));
        if (skip) {
            continue;
        }

        /* only pull directories */
        if (entry->d_type != DT_DIR) {
            continue;
        }

        str_t *subdir = malloc(sizeof(*subdir));
        str_copy_construct(subdir, entry->d_name, len);
        sll_push(subdirs, subdir);
    }

    return sll_get_size(subdirs);
}

/*
 * Compare Paths
 * Currently, the only directory names are compared.
 *
 * Possible expansions:
 *     Types
 *     Sizes
 *     UID/GID
 *     Permissions
 *     Timestamps
 *     db.db contents
 *         Rollups
 *     Handle/Detect directory moves
 */
static int compare_paths(str_t *lhs, str_t *rhs) {
    return str_cmp(&lhs, &rhs);
}

struct ComparePaths {
    size_t level;
    str_t lhs;
    str_t rhs;
};

#define ERROR_AND_JUMP(rc, err, label, fmt, ...)    \
    do {                                            \
        const int err = errno;                      \
        (void) err;                                 \
        if (strlen(fmt)) {                          \
            fprintf(stderr, fmt, __VA_ARGS__);      \
        }                                           \
        (rc) = 1;                                   \
        goto label;                                 \
    } while (0)

static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    struct ComparePaths *cp = (struct ComparePaths *) data;
    struct PoolArgs *pa = (struct PoolArgs *) args;

    int rc = 0;

    if (cp->level >= pa->in.max_level) {
        goto cleanup;
    }

    /* ********************************************** */
    struct stat lst;
    struct stat rst;

    if (lstat(cp->lhs.data, &lst) != 0) {
        ERROR_AND_JUMP(rc, err, cleanup,
                       "Error: Could not stat left directory \"%s\": %s (%d)\n",
                       cp->lhs.data, strerror(err), err);
    }

    if (lstat(cp->rhs.data, &rst) != 0) {
        ERROR_AND_JUMP(rc, err, cleanup,
                       "Error: Could not stat right directory \"%s\": %s (%d)\n",
                       cp->rhs.data, strerror(err), err);
    }
    /* ********************************************** */

    /* ********************************************** */
    DIR *ldir = NULL;
    DIR *rdir = NULL;

    if (!(ldir = opendir(cp->lhs.data))) {
        ERROR_AND_JUMP(rc, err, closedir,
                       "Error: Could not open left directory \"%s\": %s (%d)\n",
                       cp->lhs.data, strerror(err), err);
    }

    if (!(rdir = opendir(cp->rhs.data))) {
        ERROR_AND_JUMP(rc, err, closedir,
                       "Error: Could not open right directory \"%s\": %s (%d)\n",
                       cp->rhs.data, strerror(err), err);
    }
    /* ********************************************** */

    /* hard link count without . and .. */
    sll_t lsubdirs;
    sll_init(&lsubdirs);
    sll_t rsubdirs;
    sll_init(&rsubdirs);

    /* ********************************************** */
    /* get list of subdirs in each directory */
    const size_t lcount = get_entries(ldir, pa->in.skip, &lsubdirs);
    const size_t rcount = get_entries(rdir, pa->in.skip, &rsubdirs);
    /* ********************************************** */

    /* ********************************************** */
    /* convert lists to arrays of references */
    size_t i = 0;

    str_t **lsubdir = calloc(lcount, sizeof(*lsubdir));
    sll_loop(&lsubdirs, node) {
        lsubdir[i++] = sll_node_data(node);
    }

    i = 0;
    str_t **rsubdir = calloc(rcount, sizeof(*rsubdir));
    sll_loop(&rsubdirs, node) {
        rsubdir[i++] = sll_node_data(node);
    }
    /* ********************************************** */

    /* ********************************************** */
    /* sort the paths to be able to find matches in linear time */
    qsort(lsubdir, lcount, sizeof(*lsubdir), str_cmp);
    qsort(rsubdir, rcount, sizeof(*rsubdir), str_cmp);
    /* ********************************************** */

    /* ********************************************** */
    const size_t next_level = cp->level + 1;

    struct PrintArgs print;
    print.output_buffer = &pa->obufs.buffers[id];
    print.delim = '/';
    print.mutex = pa->obufs.mutex;
    print.outfile = stdout;
    print.rows = 0;

    char *buf[] = {NULL, NULL};     /* passed to print_parallel */

    /* mergesort */
    size_t lidx = 0;
    size_t ridx = 0;
    while ((lidx < lcount) && (ridx < rcount)) {
        const int diff = compare_paths(lsubdir[lidx], rsubdir[ridx]);

        /*
         * if diff != 0, print the path and stop
         *
         * subdirectories are not traversed in order to not have to
         * process/print potentially massive subtrees when just
         * printing the highest path already implies that everything
         * underneath is different
         */

        if (diff < 0) {
            buf[0] = cp->lhs.data;
            buf[1] = lsubdir[lidx]->data;
            print_parallel(&print, 2, buf, NULL);
            str_destruct(lsubdir[lidx]);
            lidx++;
        }
        else if (diff == 0) {
            struct ComparePaths *match = malloc(sizeof(*match));
            match->level = next_level;

            match->lhs.len = cp->lhs.len + 1 + lsubdir[lidx]->len;
            match->lhs.data = malloc(match->lhs.len + 1);
            SNFORMAT_S(match->lhs.data, match->lhs.len + 1, 3,
                       cp->lhs.data, cp->lhs.len,
                       "/", (size_t) 1,
                       lsubdir[lidx]->data, lsubdir[lidx]->len);

            match->rhs.len = cp->rhs.len + 1 + rsubdir[ridx]->len;
            match->rhs.data = malloc(match->rhs.len + 1);
            SNFORMAT_S(match->rhs.data, match->rhs.len + 1, 3,
                       cp->rhs.data, cp->rhs.len,
                       "/", (size_t) 1,
                       rsubdir[ridx]->data, rsubdir[ridx]->len);

            QPTPool_enqueue(ctx, id, processdir, match);

            str_destruct(rsubdir[ridx]);
            str_destruct(lsubdir[lidx]);

            lidx++;
            ridx++;
        }
        else if (diff > 0) {
            buf[0] = cp->rhs.data;
            buf[1] = rsubdir[ridx]->data;
            print_parallel(&print, 2, buf, NULL);
            str_destruct(rsubdir[ridx]);
            ridx++;
        }
    }

    for(size_t l = lidx; l < lcount; l++) {
        buf[0] = cp->lhs.data;
        buf[1] = lsubdir[l]->data;
        print_parallel(&print, 2, buf, NULL);
        str_destruct(lsubdir[l]);
    }

    for(size_t r = ridx; r < rcount; r++) {
        buf[0] = cp->rhs.data;
        buf[1] = rsubdir[r]->data;
        print_parallel(&print, 2, buf, NULL);
        str_destruct(rsubdir[r]);
    }
    /* ********************************************** */

    free(rsubdir);
    free(lsubdir);

    sll_destroy(&rsubdirs, free);
    sll_destroy(&lsubdirs, free);

  closedir:
    closedir(rdir);
    closedir(ldir);

  cleanup:
    str_destruct(&cp->rhs);
    str_destruct(&cp->lhs);
    free(cp);

    return rc;
}

static void sub_help(void) {
    printf("lhs               starting path of tree used on left hand side of this comparision\n");
    printf("rhs               starting path of tree used on right hand side of this comparision\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    struct PoolArgs pa;
    int idx = parse_cmd_line(argc, argv, "hHn:z:k:", 2, "lhs rhs", &pa.in);
    if (pa.in.helped)
        sub_help();
    if (idx < 0)
        return 1;
    else {
        INSTALL_STR(&pa.in.name,   argv[argc - 2]);
        INSTALL_STR(&pa.in.nameto, argv[argc - 1]);
    }

    int rc = 0;

    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    if (!OutputBuffers_init(&pa.obufs, pa.in.maxthreads, pa.in.output_buffer_size, &mutex)) {
        rc = 1;
        goto done;
    }

    const uint64_t queue_depth = pa.in.target_memory_footprint / sizeof(struct ComparePaths) / pa.in.maxthreads;
    QPTPool_t *pool = QPTPool_init_with_props(pa.in.maxthreads, &pa, NULL, NULL, queue_depth, 1, 2
                                              #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                              , NULL
                                              #endif
        );
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        rc = 1;
        goto cleanup_qptp;
    }

    struct ComparePaths *cp = malloc(sizeof(*cp));
    cp->level = 0;
    str_copy_construct(&cp->lhs, pa.in.name.data,   pa.in.name.len);
    str_copy_construct(&cp->rhs, pa.in.nameto.data, pa.in.nameto.len);

    QPTPool_enqueue(pool, 0, processdir, cp);

  cleanup_qptp:
    QPTPool_wait(pool);
    QPTPool_destroy(pool);

    OutputBuffers_flush_to_single(&pa.obufs, stdout);
    OutputBuffers_destroy(&pa.obufs);

  done:
    return rc;
}
