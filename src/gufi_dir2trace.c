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
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "dbutils.h"
#include "debug.h"
#include "external.h"
#include "outfiles.h"
#include "str.h"
#include "trace.h"
#include "trie.h"
#include "utils.h"
#include "xattrs.h"

struct PoolArgs {
    struct input in;
    refstr_t trace_prefix;

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
    externaltofile((FILE *) args, in->delim, filename);
    return 0;
}

static int process_nondir(struct work *entry, struct entry_data *ed, void *args) {
    struct NondirArgs *nda = (struct NondirArgs *) args;

    if (fstatat_wrapper(entry, ed, 1) != 0) {
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
        external_read_file(nda->in, entry, process_external, nda->fp);
    }

    return 0;
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

    DIR *dir = opendir(work->name);
    if (!dir) {
        rc = 1;
        goto cleanup;
    }

    memset(&ed, 0, sizeof(ed));
    if (lstat_wrapper(work, 1) != 0) {
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
            processdir, process_dir?process_nondir:NULL, &nda, &ctrs);

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

static int validate_source(const char *path, struct work **work) {
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

    new_work->root_parent.data = path;
    new_work->root_parent.len = dirname_len(path, new_work->name_len);
    new_work->level = 0;
    new_work->basename_len = new_work->name_len - new_work->root_parent.len;
    new_work->root_basename_len = new_work->basename_len;

    *work = new_work;

    return 0;
}

static void sub_help(void) {
    printf("tree...              walk one or more trees to produce trace file\n");
    printf("output_prefix        prefix of output files (<prefix>.<tid>)\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_THREADS,

        /* tree walk flags */
        FLAG_MIN_LEVEL, FLAG_MAX_LEVEL, FLAG_PATH_LIST,
        FLAG_INDEX_XATTRS, FLAG_SKIP_FILE,

        /* miscellaneous flags */
        FLAG_CHECK_EXTDB_VALID,

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
    process_args_and_maybe_exit(options, 1, "input_dir... output_prefix", &pa.in);

    /* parse positional args, following the options */
    INSTALL_STR(&pa.trace_prefix, argv[argc - 1]);

    int rc = EXIT_SUCCESS;

    argc--; /* trace prefix is no longer needed */
    const size_t root_count = argc - idx;

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
    QPTPool_ctx_t *ctx = QPTPool_init_with_props(pa.in.maxthreads, &pa, NULL, NULL, queue_limit, pa.in.swap_prefix.data, 1, 2);
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
            process_path_list(&pa.in, NULL, ctx, processdir);
        }
        else if (root_count == 1) {
            struct work *root = NULL;
            if (validate_source(argv[idx], &root) == 0) {
                process_path_list(&pa.in, root, ctx, processdir);
            }
            else {
                rc = EXIT_FAILURE;
            }
        }
    }
    else {
        for(int i = idx; i < argc; i++) {
            /* get first work item by validating source path */
            struct work *root = NULL;
            if (validate_source(argv[i], &root) != 0) {
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

    QPTPool_destroy(ctx);

    size_t total_files = 0;
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
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

    return EXIT_SUCCESS;
}
