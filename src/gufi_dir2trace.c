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
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "bf.h"
#include "debug.h"
#include "external_attach.h"
#include "outfiles.h"
#include "path_list.h"
#include "str.h"
#include "trace.h"
#include "trie.h"
#include "utils.h"
#include "xattrs.h"

struct PoolArgs {
    struct input in;
    str_t trace_prefix;

    FILE **outfiles;
    pthread_mutex_t *mutex; /* for writing to stdout */

    size_t *total_files;
};

struct NondirArgs {
    struct input *in;

    /* trace file */
    FILE *fp;

    /* stdout */
    struct {
        char *buf;
        size_t size;
        size_t offset;
    } pipe;
};

static int process_external(struct input *in, void *args,
                            const long long int pinode,
                            const char *filename) {
    (void) pinode;
    struct NondirArgs *nda = (struct NondirArgs *) args;
    if (nda->fp == stdout) {
        write_with_resize(&nda->pipe.buf, &nda->pipe.size, &nda->pipe.offset,
                          "%" PATH_PREFIX_FORMAT "%s%ce%c\n",
                          strlen(filename), filename, in->delim, in->delim);
    }
    else {
        externaltofile(nda->fp, in->delim, filename);
    }
    return 0;
}

static int process_nondir(struct work *entry, struct entry_data *ed, void *args) {
    struct NondirArgs *nda = (struct NondirArgs *) args;

    if (fstatat_wrapper(entry, ed, AT_SYMLINK_NOFOLLOW, 1, NULL) != 0) {
        return 1;
    }

    if (nda->fp == stdout) {
        worktobuffer(&nda->pipe.buf, &nda->pipe.size, &nda->pipe.offset,
                     nda->in->delim, entry->root_parent.len, entry, ed);
    }
    else {
        worktofile(nda->fp, nda->in->delim, entry->root_parent.len, entry, ed);
    }

    if (strncmp(entry->name + entry->name_len - entry->basename_len,
                EXTERNAL_DB_USER_FILE, EXTERNAL_DB_USER_FILE_LEN + 1) == 0) {
        external_read_file(nda->in, entry, process_external, nda);
    }

    return 0;
}

/* Write out stat info for each intermediate path from a --path-list entry. */
static int process_intermediate_dir(QPTPool_ctx_t *ctx, void *data) {
    struct work *work = (struct work *) data;

    struct PoolArgs *pa = (struct PoolArgs *) QPTPool_get_args_internal(ctx);
    struct input *in = &pa->in;
    FILE *fp = pa->outfiles[QPTPool_get_id(ctx)];
    struct entry_data ed = {0};
    int rc = 0;

    if (!S_ISDIR(work->statuso.st_mode)) {
        fprintf(stderr, "Error: Intermediate path is not a directory \"%s\"\n", work->name);
        free(work);
        return 1;
    }

    ed.type = 'd';

    if (fp == stdout) {
        pthread_mutex_lock(pa->mutex);
    }

    worktofile(fp, in->delim, work->root_parent.len, work, &ed);

    if (fp == stdout) {
        pthread_mutex_unlock(pa->mutex);
    }

    free(work);
    return rc;
}

/* process the work under one directory (no recursion) */
/* deletes work */
static int processdir(QPTPool_ctx_t *ctx, void *data) {
    /* Not checking arguments */

    const size_t id = QPTPool_get_id(ctx);
    struct PoolArgs *pa = (struct PoolArgs *) QPTPool_get_args_internal(ctx);
    struct input *in = &pa->in;
    struct work *work = NULL;
    struct entry_data ed;
    int rc = 0;
    struct NondirArgs nda = {
        .in = in,
        .fp = pa->outfiles[id],
        .pipe = {
            .buf = NULL,
            .size = 0,
            .offset = 0,
        },
    };
    struct descend_counters ctrs = {0};

    decompress_work(&work, data);

    DIR *dir = opendir_wrapper(work->name, NULL);
    if (!dir) {
        rc = 0;
        goto cleanup;
    }

    memset(&ed, 0, sizeof(ed));
    if (lstat_wrapper(work->name, &work->statuso, &work->crtime,
                      &work->stat_called, 1, NULL) != 0) {
        rc = 0;
        goto close_dir;
    }

    const int process_dir = ((pa->in.min_level <= work->level) &&
                             (work->level <= pa->in.max_level));

    if (!process_dir) {
        goto descend_tree;
    }

    /* get source directory xattrs */
    if (in->process_xattrs) {
        xattrs_setup(&ed.xattrs);
        xattrs_get(work->name, &ed.xattrs);
    }

    ed.type = 'd';

    /* write start of stanza */
    if (nda.fp == stdout) {
        worktobuffer(&nda.pipe.buf, &nda.pipe.size, &nda.pipe.offset,
                     in->delim, work->root_parent.len, work, &ed);
    }
    else {
        worktofile(pa->outfiles[id], in->delim, work->root_parent.len, work, &ed);
    }

    if (in->process_xattrs) {
        xattrs_cleanup(&ed.xattrs);
    }

  descend_tree:
    descend(ctx, in, work, dir, 1,
            try_skip_lstat, NULL, NULL,
            processdir, process_dir?process_nondir:NULL, &nda,
            &ctrs);

    if (process_dir) {
        if (nda.fp == stdout) {
            pthread_mutex_lock(pa->mutex);
            fwrite(nda.pipe.buf, sizeof(char), nda.pipe.offset, stdout);
            fflush(stdout);
            pthread_mutex_unlock(pa->mutex);
            free(nda.pipe.buf);
        }
    }

  close_dir:
    closedir(dir);

  cleanup:
    free(work);

    pa->total_files[id] += ctrs.nondirs_processed;

    return rc;
}

static int validate_source(const char *path, struct work **work, const int use_exact_path) {
    /* get input path metadata */
    struct stat st;
    if (lstat(path, &st) != 0) {
        const int err = errno;
        fprintf(stderr, "Could not stat source directory \"%s\": %s (%d)\n", path, strerror(err), err);
        return 1;
    }

    /* check that the input path is a directory */
    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Source path is not a directory \"%s\"\n", path);
        return 1;
    }

    struct work *new_work = new_work_with_name(NULL, 0, path, strlen(path));

    new_work->root_parent.data = (char *) path;
    new_work->root_parent.len = use_exact_path?0:dirname_len(path, new_work->name_len);
    new_work->level = 0;
    new_work->basename_len = new_work->name_len - new_work->root_parent.len;
    new_work->root_basename_len = new_work->basename_len;

    *work = new_work;

    return 0;
}

struct process_path_entry_args {
    struct input *in;
    struct work *root;
    QPTPool_ctx_t *ctx;
    QPTPool_f func;
    trie_t *traced;
    sll_t paths;
};

/*
 * Process one path-list entry: trace intermediate directories (dedup via trie),
 * then enqueue the listed path itself for the normal tree walk.
 */
static int process_path_entry(const char *path, size_t len, void *arg) {
    struct process_path_entry_args *cb = (struct process_path_entry_args *) arg;

    /* trace each intermediate directory between root and this path-list entry */
    ino_t pinode = 0;
    for (char *p = strchr((char *) path + 1, '/'); p; p = strchr(p + 1, '/')) {
        *p = '\0';

        struct stat st;
        /* lstat, not stat - don't follow symlinks in path components */
        if (lstat(path, &st) != 0) {
            const int err = errno;
            *p = '/';
            fprintf(stderr, "Error: Cannot stat intermediate path \"%s\": %s (%d)\n",
                    path, strerror(err), err);
            return 1;
        }

        if (!S_ISDIR(st.st_mode)) {
            *p = '/';
            fprintf(stderr, "Error: Intermediate path is not a directory \"%s\"\n", path);
            return 1;
        }

        /* skip directories already traced from a previous path-list entry */
        if (trie_search(cb->traced, path, strlen(path), NULL) == 1) {
            *p = '/';
            continue;
        }
        trie_insert(cb->traced, path, strlen(path), NULL, NULL);

        struct work *work = new_work_with_name(NULL, 0, path, strlen(path));

        work->root_parent.data = NULL;
        work->root_parent.len = dirname_len(path, strlen(path));
        work->basename_len = work->name_len - work->root_parent.len;
        work->statuso = st;
        work->pinode = pinode;
        pinode = st.st_ino;

        sll_push_back(&cb->paths, work);

        *p = '/';
    }

    /* enqueue the listed path itself for the normal tree walk.
     * path already includes the root prefix from foreach_path_list_entry. */
    struct work *subtree_root = new_work_with_name(NULL, 0, path, len);

    if (lstat_wrapper(subtree_root->name, &subtree_root->statuso, &subtree_root->crtime,
                      &subtree_root->stat_called, 1, cb->in->no_print_errno) != 0) {
        free(subtree_root);
        return 0;
    }

    if (!S_ISDIR(subtree_root->statuso.st_mode)) {
        free(subtree_root);
        return 0;
    }

    subtree_root->orig_root = cb->root->orig_root;
    subtree_root->root_parent = cb->root->root_parent;
    subtree_root->basename_len = subtree_root->name_len - (trailing_match_index(subtree_root->name + 1, subtree_root->name_len - 1, "/", 1) + 1);
    subtree_root->level = cb->in->min_level;

    QPTPool_enqueue(cb->ctx, cb->func, subtree_root);

    return 0;
}

static int trace_intermediate_paths(struct input *in, struct work *root,
                                    QPTPool_ctx_t *ctx, QPTPool_f func) {
    trie_t *traced = trie_alloc();

    struct process_path_entry_args cb = {
        .in = in,
        .root = root,
        .ctx = ctx,
        .func = func,
        .traced = traced,
    };
    sll_init(&cb.paths);

    ssize_t rc = foreach_path_list_entry(in, root, process_path_entry, &cb);
    if (rc < 0) {
        trie_free(traced);
        sll_destroy(&cb.paths, free);
        return -1;
    }

    sll_loop(&cb.paths, node) {
        struct work *work = sll_node_data(node);
        QPTPool_enqueue(ctx, process_intermediate_dir, work);
    }
    /* work items are owned by QPTPool now; process_intermediate_dir frees them */
    sll_destroy(&cb.paths, NULL);
    trie_free(traced);

    return 0;
}

static void sub_help(void) {
    printf("dir...               walk one or more trees to produce trace file\n");
    printf("trace                prefix of trace files (<trace>.<tid>)\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_THREADS,

        /* tree walk flags */
        FLAG_MIN_LEVEL, FLAG_MAX_LEVEL, FLAG_PATH_LIST,
        FLAG_INDEX_XATTRS, FLAG_SKIP_FILE,

        /* miscellaneous flags */
        FLAG_EXTERNAL_ATTACH_VALIDATE, FLAG_USE_EXACT_PATH,

        /* output flags */
        FLAG_DELIM,

        /* memory usage flags */
        FLAG_TARGET_MEMORY, FLAG_SWAP_PREFIX, FLAG_SUBDIR_LIMIT,
        #ifdef HAVE_ZLIB
        FLAG_COMPRESS,
        #endif

        FLAG_END
    };

    struct PoolArgs pa;
    process_args_and_maybe_exit(options, 1, "dir... trace", &pa.in);

    /* parse positional args, following the options */
    INSTALL_STR(&pa.trace_prefix, pa.in.pos.argv[pa.in.pos.argc - 1]);

    int rc = EXIT_SUCCESS;

    pa.in.pos.argc--; /* trace prefix is no longer needed */
    const size_t root_count = pa.in.pos.argc;

    if (bad_partial_walk(&pa.in, root_count)){
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    pa.outfiles = outfiles_init(&pa.trace_prefix, pa.in.maxthreads);
    if (!pa.outfiles) {
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    const uint64_t queue_limit = get_queue_limit(pa.in.target_memory, pa.in.maxthreads);
    QPTPool_ctx_t *ctx = QPTPool_init_with_props(pa.in.maxthreads, &pa, NULL, NULL, queue_limit, pa.in.swap_prefix.data, 1, 2, 1);
    if (QPTPool_start(ctx) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(ctx);
        rc = EXIT_FAILURE;
        goto free_outfiles;
    }

    fprintf(stderr, "Creating GUFI Traces %s with %zu threads\n", pa.trace_prefix.data, pa.in.maxthreads);

    pthread_mutex_t stdout_mutex = PTHREAD_MUTEX_INITIALIZER;
    pa.mutex = &stdout_mutex;

    pa.total_files = calloc(pa.in.maxthreads, sizeof(size_t));

    struct start_end rt;
    clock_gettime(CLOCK_REALTIME, &rt.start);

    struct start_end after_init;
    clock_gettime(CLOCK_MONOTONIC, &after_init.start);

    if (doing_partial_walk(&pa.in, root_count)) {
        if (root_count == 0) {
            trace_intermediate_paths(&pa.in, NULL, ctx, processdir);
        }
        else if (root_count == 1) {
            struct work *root = NULL;
            if (validate_source(pa.in.pos.argv[0], &root, pa.in.use_exact_path) == 0) {
                trace_intermediate_paths(&pa.in, root, ctx, processdir);
                free(root);
            }
            else {
                rc = EXIT_FAILURE;
            }
        }
    }
    else {
        for (int i = 0; i < pa.in.pos.argc; i++) {
            /* get first work item by validating source path */
            struct work *root = NULL;
            if (validate_source(pa.in.pos.argv[i], &root, pa.in.use_exact_path) != 0) {
                continue;
            }

            struct work *copy = compress_struct(pa.in.compress, root, struct_work_size(root));
            QPTPool_enqueue(ctx, processdir, copy);
        }
    }
    QPTPool_stop(ctx);

    clock_gettime(CLOCK_MONOTONIC, &after_init.end);
    clock_gettime(CLOCK_REALTIME, &rt.end);
    const long double processtime = sec(nsec(&after_init));

    /* don't count as part of processtime */

    const uint64_t thread_count = QPTPool_threads_completed(ctx);

    if (QPTPool_stopped_on_error(ctx) == 1) {
        rc = EXIT_FAILURE;
    }

    QPTPool_destroy(ctx);

    size_t total_files = 0;
    for (size_t i = 0; i < pa.in.maxthreads; i++) {
        total_files += pa.total_files[i];
    }

    free(pa.total_files);

    if (rc == EXIT_SUCCESS) {
        fprintf(stderr, "Total Dirs:          %" PRIu64 "\n", thread_count);
        fprintf(stderr, "Total Files:         %zu\n",         total_files);
        fprintf(stderr, "Start Time:          %.6Lf\n",       sec(since_epoch(&rt.start)));
        fprintf(stderr, "End Time:            %.6Lf\n",       sec(since_epoch(&rt.end)));
        fprintf(stderr, "Time Spent Indexing: %.2Lfs\n",      processtime);
        fprintf(stderr, "Dirs/Sec:            %.2Lf\n",       thread_count / processtime);
        fprintf(stderr, "Files/Sec:           %.2Lf\n",       total_files / processtime);
    }

  free_outfiles:
    outfiles_fin(pa.outfiles, pa.in.maxthreads);

  cleanup:
    input_fini(&pa.in);

    dump_memory_usage(stderr);

    return rc;
}
