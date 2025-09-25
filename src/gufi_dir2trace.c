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
#include "trace.h"
#include "trie.h"
#include "utils.h"
#include "xattrs.h"

struct PoolArgs {
    struct input in;
    FILE **outfiles;

    size_t *total_files;
};

struct NondirArgs {
    struct input *in;
    FILE *fp;
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

    if (fstatat_wrapper(entry, ed) != 0) {
        return 1;
    }

    worktofile(nda->fp, nda->in->delim, entry->root_parent.len, entry, ed);

    if (strncmp(entry->name + entry->name_len - entry->basename_len,
                EXTERNAL_DB_USER_FILE, EXTERNAL_DB_USER_FILE_LEN + 1) == 0) {
        external_read_file(nda->in, entry, process_external, nda->fp);
    }

    return 0;
}

/* process the work under one directory (no recursion) */
/* deletes work */
static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    /* Not checking arguments */

    struct PoolArgs *pa = (struct PoolArgs *) args;
    struct input *in = &pa->in;
    struct work *work = NULL;
    struct entry_data ed;
    int rc = 0;
    struct descend_counters ctrs = {0};

    decompress_work(&work, data);

    DIR *dir = opendir(work->name);
    if (!dir) {
        rc = 1;
        goto cleanup;
    }

    memset(&ed, 0, sizeof(ed));
    if (lstat_wrapper(work) != 0) {
        goto cleanup;
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
    worktofile(pa->outfiles[id], in->delim, work->root_parent.len, work, &ed);

    if (in->process_xattrs) {
        xattrs_cleanup(&ed.xattrs);
    }

    struct NondirArgs nda = {
        .in = in,
        .fp = pa->outfiles[id],
    };

  descend_tree:
    descend(ctx, id, pa, in, work, dir, 0,
            processdir, process_dir?process_nondir:NULL, &nda, &ctrs);

  cleanup:
    closedir(dir);

    free(work);

    pa->total_files[id] += ctrs.nondirs_processed;

    return rc;
}

static int validate_source(const char *path, struct work **work) {
    /* get input path metadata */
    struct stat st;
    if (lstat(path, &st) != 0) {
        fprintf(stderr, "Could not stat source directory \"%s\"\n", path);
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

    *work = new_work;

    return 0;
}

static void sub_help(void) {
    printf("input_dir...         walk one or more trees to produce trace file\n");
    printf("output_prefix        prefix of output files (<prefix>.<tid>)\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    struct PoolArgs pa;
    process_args_and_maybe_exit("hHvn:xy:z:d:k:M:s:C:" COMPRESS_OPT "qD:", 2, "input_dir... output_prefix", &pa.in);

    /* parse positional args, following the options */
    INSTALL_STR(&pa.in.nameto, argv[argc - 1]);

    const size_t root_count = argc - idx - 1;

    if ((pa.in.min_level && pa.in.subtree_list.len) &&
        (root_count > 1)) {
        fprintf(stderr, "Error: When -D is passed in, only one source directory may be specified\n");
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    pa.outfiles = outfiles_init(pa.in.nameto.data, pa.in.maxthreads);
    if (!pa.outfiles) {
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    const uint64_t queue_limit = get_queue_limit(pa.in.target_memory_footprint, pa.in.maxthreads);
    QPTPool_t *pool = QPTPool_init_with_props(pa.in.maxthreads, &pa, NULL, NULL, queue_limit, pa.in.swap_prefix.data, 1, 2);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        outfiles_fin(pa.outfiles, pa.in.maxthreads);
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    fprintf(stdout, "Creating GUFI Traces %s with %zu threads\n", pa.in.nameto.data, pa.in.maxthreads);
    fflush(stdout);

    pa.total_files = calloc(pa.in.maxthreads, sizeof(size_t));

    struct start_end after_init;
    clock_gettime(CLOCK_MONOTONIC, &after_init.start);

    char **roots = calloc(root_count, sizeof(char *));
    for(size_t i = 0; idx < (argc - 1);) {
        /* force all input paths to be canonical */
        roots[i] = realpath(argv[idx++], NULL);
        if (!roots[i]) {
            const int err = errno;
            fprintf(stderr, "Could not resolve path \"%s\": %s (%d)\n",
                    argv[idx - 1], strerror(err), err);
            continue;
        }

        /* get first work item by validating source path */
        struct work *root;
        if (validate_source(roots[i], &root) != 0) {
            continue;
        }

        /*
         * manually get basename of provided path since
         * there is no source for the basenames
         */
        root->basename_len = root->name_len - root->root_parent.len;
        root->root_basename_len = root->basename_len;

        if (doing_partial_walk(&pa.in, root_count)) {
            process_subtree_list(&pa.in, root, pool, processdir);
        }
        else {
            struct work *copy = compress_struct(pa.in.compress, root, struct_work_size(root));
            QPTPool_enqueue(pool, 0, processdir, copy);
        }

        i++;
    }
    QPTPool_stop(pool);

    clock_gettime(CLOCK_MONOTONIC, &after_init.end);
    const long double processtime = sec(nsec(&after_init));

    /* don't count as part of processtime */

    const uint64_t thread_count = QPTPool_threads_completed(pool);

    QPTPool_destroy(pool);

    for(size_t i = 0; i < root_count; i++) {
        free(roots[i]);
    }
    free(roots);
    outfiles_fin(pa.outfiles, pa.in.maxthreads);

    size_t total_files = 0;
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        total_files += pa.total_files[i];
    }

    free(pa.total_files);
    input_fini(&pa.in);

    fprintf(stdout, "Total Dirs:          %" PRIu64 "\n", thread_count);
    fprintf(stdout, "Total Files:         %zu\n",         total_files);
    fprintf(stdout, "Time Spent Indexing: %.2Lfs\n",      processtime);
    fprintf(stdout, "Dirs/Sec:            %.2Lf\n",       thread_count / processtime);
    fprintf(stdout, "Files/Sec:           %.2Lf\n",       total_files / processtime);

    dump_memory_usage();

    return EXIT_SUCCESS;
}
