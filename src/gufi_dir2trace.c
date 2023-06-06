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
#include "trace.h"
#include "trie.h"
#include "utils.h"
#include "xattrs.h"

struct PoolArgs {
    struct input in;
    trie_t *skip;
    FILE **outfiles;

    uint64_t *total_files;
};

struct NondirArgs {
    struct input *in;
    FILE *fp;
};

static int process_nondir(struct work *entry, struct entry_data *ed, void *args) {
    struct NondirArgs *nda = (struct NondirArgs *) args;
    if (lstat(entry->name, &ed->statuso) == 0) {
        if (ed->type == 'l') {
            readlink(entry->name, ed->linkname, MAXPATH);
        }
        worktofile(nda->fp, nda->in->delim, entry->root_parent.len, entry, ed);
    }
    return 0;
}

/* process the work under one directory (no recursion) */
/* deletes work */
static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    /* Not checking arguments */

    struct PoolArgs *pa = (struct PoolArgs *) args;
    struct input *in = &pa->in;
    struct work work_src;
    struct work *work = (struct work *) data;
    struct entry_data ed;
    uint64_t nondirs_processed = 0;
    int rc = 0;

    if (work->compressed.yes) {
        work = &work_src;
        decompress_struct((void **) &work, data, sizeof(work_src));
    }

    DIR *dir = opendir(work->name);
    if (!dir) {
        rc = 1;
        goto cleanup;
    }

    memset(&ed, 0, sizeof(ed));
    if (lstat(work->name, &ed.statuso) != 0) {
        fprintf(stderr, "Could not stat directory \"%s\"\n", work->name);
        rc = 1;
        goto cleanup;
    }

    /* get source directory xattrs */
    if (in->external_enabled) {
        xattrs_setup(&ed.xattrs);
        xattrs_get(work->name, &ed.xattrs);
    }

    ed.type = 'd';

    /* write start of stanza */
    worktofile(pa->outfiles[id], in->delim, work->root_parent.len, work, &ed);

    if (in->external_enabled) {
        xattrs_cleanup(&ed.xattrs);
    }

    struct NondirArgs nda = {
        .in = in,
        .fp = pa->outfiles[id],
    };
    descend(ctx, id, pa,
            in, work, ed.statuso.st_ino,
            dir, pa->skip, 0, 0,
            processdir, process_nondir, &nda,
            NULL, NULL, NULL, &nondirs_processed);

  cleanup:
    closedir(dir);

    free_struct(work, data, work->recursion_level);

    pa->total_files[id] += nondirs_processed;

    return rc;
}

/* close all output files */
static int outfiles_fin(FILE **files, const size_t end) {
    /* Not checking arguments */
    for(size_t i = 0; i < end; i++) {
        fflush(files[i]);
        fclose(files[i]);
    }
    free(files);
    return 0;
}

/* allocate the array of FILE * and open files */
static FILE **outfiles_init(const char *prefix, const size_t count) {
    /* Not checking arguments */
    FILE **files = calloc(count, sizeof(FILE *));

    for(size_t i = 0; i < count; i++) {
        char outname[MAXPATH];
        SNPRINTF(outname, MAXPATH, "%s.%zu", prefix, i);

        /* check if the destination path already exists (not an error) */
        struct stat st;
        if (lstat(outname, &st) == 0) {
            fprintf(stderr, "\"%s\" Already exists!\n", outname);

            /* if the deestination path is not a regular file (error) */
            if (!S_ISREG(st.st_mode)) {
                outfiles_fin(files, i);
                fprintf(stderr, "Destination path is not a file \"%s\"\n", outname);
                return NULL;
            }
        }

        if (!(files[i] = fopen(outname, "w"))) {
            outfiles_fin(files, i);
            fprintf(stderr, "Could not open output file %s\n", outname);
            return NULL;
        }
    }

    return files;
}

static int validate_source(const char *path, struct work *work) {
    memset(work, 0, sizeof(*work));

    /* get input path metadata */
    struct stat st;
    if (lstat(path, &st) < 0) {
        fprintf(stderr, "Could not stat source directory \"%s\"\n", path);
        return 1;
    }

    /* check that the input path is a directory */
    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Source path is not a directory \"%s\"\n", path);
        return 1;
    }

    work->name_len = SNFORMAT_S(work->name, MAXPATH, 1, path, strlen(path));
    work->root_parent.data = path;
    work->root_parent.len = dirname_len(path, work->name_len);

    return 0;
}

static void sub_help() {
    printf("input_dir...         walk one or more trees to produce trace file\n");
    printf("output_prefix        prefix of output files (<prefix>.<tid>)\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    struct PoolArgs pa;
    int idx = parse_cmd_line(argc, argv, "hHn:xd:k:M:C:" COMPRESS_OPT, 2, "input_dir... output_prefix", &pa.in);
    if (pa.in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        /* parse positional args, following the options */
        INSTALL_STR(&pa.in.nameto, argv[argc - 1]);

        if (setup_directory_skip(pa.in.skip.data, &pa.skip) != 0) {
            return -1;
        }
    }

    pa.outfiles = outfiles_init(pa.in.nameto.data, pa.in.maxthreads);
    if (!pa.outfiles) {
        trie_free(pa.skip);
        return -1;
    }

    const uint64_t queue_depth = pa.in.target_memory_footprint / sizeof(struct work) / pa.in.maxthreads;
    QPTPool_t *pool = QPTPool_init_with_props(pa.in.maxthreads, &pa, NULL, NULL, queue_depth, 1, 2
                                              #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                              , NULL
                                              #endif
        );
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        outfiles_fin(pa.outfiles, pa.in.maxthreads);
        trie_free(pa.skip);
        return -1;
    }

    fprintf(stdout, "Creating GUFI Traces %s with %zu threads\n", pa.in.nameto.data, pa.in.maxthreads);

    pa.total_files = calloc(pa.in.maxthreads, sizeof(uint64_t));

    struct start_end after_init;
    clock_gettime(CLOCK_MONOTONIC, &after_init.start);

    const size_t root_count = argc - idx - 1;
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
        struct work root;
        if (validate_source(roots[i], &root) != 0) {
            continue;
        }

        struct work *copy = compress_struct(pa.in.compress, &root, sizeof(root));
        QPTPool_enqueue(pool, 0, processdir, copy);

        i++;
    }
    QPTPool_wait(pool);

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
    trie_free(pa.skip);

    uint64_t total_files = 0;
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        total_files += pa.total_files[i];
    }

    free(pa.total_files);

    fprintf(stdout, "Total Dirs:          %" PRIu64 "\n", thread_count);
    fprintf(stdout, "Total Files:         %" PRIu64 "\n", total_files);
    fprintf(stdout, "Time Spent Indexing: %.2Lfs\n",      processtime);
    fprintf(stdout, "Dirs/Sec:            %.2Lf\n",       thread_count / processtime);
    fprintf(stdout, "Files/Sec:           %.2Lf\n",       total_files / processtime);

    return 0;
}
