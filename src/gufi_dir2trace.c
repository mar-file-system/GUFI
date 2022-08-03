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
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "dbutils.h"
#include "debug.h"
#include "outfiles.h"
#include "trace.h"
#include "trie.h"
#include "utils.h"
#include "xattrs.h"

extern int errno;

#if BENCHMARK
#include <time.h>

pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;
size_t total_dirs = 0;
size_t total_files = 0;
#endif

struct PoolArgs {
    trie_t *skip;
    FILE **outfiles;
};

static int process_nondir(struct work *entry, void *args) {
    worktofile((FILE *) args, in.delim, entry->root_len, entry);
    return 0;
}

/* process the work under one directory (no recursion) */
/* deletes work */
static int processdir(struct QPTPool *ctx, const size_t id, void *data, void *args) {
    #if BENCHMARK
    pthread_mutex_lock(&global_mutex);
    total_dirs++;
    pthread_mutex_unlock(&global_mutex);
    #endif

    /* /\* Can probably skip this *\/ */
    /* if (!data) { */
    /*     return 1; */
    /* } */

    /* /\* Can probably skip this *\/ */
    /* if (!ctx || (id >= ctx->size)) { */
    /*     free(data); */
    /*     return 1; */
    /* } */

    struct PoolArgs *pa = (struct PoolArgs *) args;
    struct work *work = (struct work *) data;

    DIR *dir = opendir(work->name);
    if (!dir) {
        free(data);
        return 1;
    }

    /* get source directory xattrs */
    if (in.xattrs.enabled) {
        xattrs_setup(&work->xattrs);
        xattrs_get(work->name, &work->xattrs);
    }

    /* write start of stanza */
    worktofile(pa->outfiles[id], in.delim, work->root_len, work);

    if (in.xattrs.enabled) {
        xattrs_cleanup(&work->xattrs);
    }

    size_t *nondirs_processed = NULL;
    #if BENCHMARK
    size_t nondirs_processed_benchmark = 0;
    nondirs_processed = &nondirs_processed_benchmark;
    #endif
    descend(ctx, id, work, dir, pa->skip,
            processdir, process_nondir, pa->outfiles[id],
            NULL, NULL, nondirs_processed);

    closedir(dir);
    free(data);

    #if BENCHMARK
    pthread_mutex_lock(&global_mutex);
    total_files += nondirs_processed_benchmark;
    pthread_mutex_unlock(&global_mutex);
    #endif

    return 0;
}

static int check_prefix(const char *nameto, const size_t nameto_len, const size_t thread_count) {
    /* check the output files, if a prefix was provided */
    if (!nameto_len || !nameto_len) {
        fprintf(stderr, "No output file name specified\n");
        return -1;
    }

    /* check if the destination path already exists (not an error) */
    for(int i = 0; i < thread_count; i++) {
        char outname[MAXPATH];
        SNPRINTF(outname, MAXPATH, "%s.%d", nameto, i);

        struct stat dst_st;
        if (lstat(outname, &dst_st) == 0) {
            fprintf(stderr, "\"%s\" Already exists!\n", outname);

            /* if the destination path is not a directory (error) */
            if (S_ISDIR(dst_st.st_mode)) {
                fprintf(stderr, "Destination path is a directory \"%s\"\n", outname);
                return -1;
            }
        }
    }

    return 0;
}

static struct work *validate_source(char *path) {
    struct work *root = (struct work *) calloc(1, sizeof(struct work));
    if (!root) {
        fprintf(stderr, "Could not allocate root struct\n");
        return NULL;
    }

    /* get input path metadata */
    if (lstat(path, &root->statuso) < 0) {
        fprintf(stderr, "Could not stat source directory \"%s\"\n", path);
        free(root);
        return NULL;
    }

    /* check that the input path is a directory */
    if (S_ISDIR(root->statuso.st_mode)) {
        root->type[0] = 'd';
    }
    else {
        fprintf(stderr, "Source path is not a directory \"%s\"\n", path);
        free(root);
        return NULL;
    }

    /* check if the source directory can be accessed */
    if (access(path, R_OK | X_OK) != 0) {
        fprintf(stderr, "couldn't access input dir '%s': %s\n",
                root->name, strerror(errno));
        free(root);
        return NULL;
    }

    root->name_len = SNFORMAT_S(root->name, MAXPATH, 1, path, strlen(path));
    root->root = path;
    root->root_len = dirname_len(path, root->name_len);

    return root;
}

static void sub_help() {
    printf("input_dir...         walk one or more trees to produce trace file\n");
    printf("output_prefix        prefix of output files (<prefix>.<tid>)\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    int idx = parse_cmd_line(argc, argv, "hHn:xd:k:", 2, "input_dir... output_prefix", &in);
    struct PoolArgs args;
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        /* parse positional args, following the options */
        int retval = 0;
        INSTALL_STR(in.nameto, argv[argc - 1], MAXPATH, "output_prefix");

        if (retval)
            return retval;

        if (setup_directory_skip(in.skip, &args.skip) != 0) {
            return -1;
        }

        in.nameto_len = strlen(in.nameto);
    }

    if (check_prefix(in.nameto, in.nameto_len, in.maxthreads) != 0) {
        trie_free(args.skip);
        return -1;
    }

    args.outfiles = outfiles_init(1, in.nameto, in.maxthreads);
    if (!args.outfiles) {
        trie_free(args.skip);
        return -1;
    }

    #if BENCHMARK
    struct start_end benchmark;
    clock_gettime(CLOCK_MONOTONIC, &benchmark.start);
    #endif

    struct QPTPool *pool = QPTPool_init(in.maxthreads
                                        #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                        , NULL
                                        #endif
        );
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        outfiles_fin(args.outfiles, in.maxthreads);
        trie_free(args.skip);
        return -1;
    }

    if (QPTPool_start(pool, &args) != (size_t) in.maxthreads) {
        fprintf(stderr, "Failed to start threads\n");
        outfiles_fin(args.outfiles, in.maxthreads);
        trie_free(args.skip);
        return -1;
    }

    const size_t root_count = argc - idx - 1;
    char **roots = calloc(root_count, sizeof(char *));
    for(size_t i = 0; i < root_count; i++) {
        /* force all input paths to be canonical */
        roots[i] = realpath(argv[idx], NULL);

        /* get first work item by validating source path */
        struct work *root = validate_source(roots[i]);
        if (!root) {
            continue;
        }

        QPTPool_enqueue(pool, 0, processdir, root);
        idx++;
    }
    QPTPool_wait(pool);
    QPTPool_destroy(pool);

    for(size_t i = 0; i < root_count; i++) {
        free(roots[i]);
    }
    free(roots);
    outfiles_fin(args.outfiles, in.maxthreads);
    trie_free(args.skip);

    #if BENCHMARK
    clock_gettime(CLOCK_MONOTONIC, &benchmark.end);
    const long double processtime = sec(nsec(&benchmark));

    fprintf(stderr, "Total Dirs:            %zu\n",    total_dirs);
    fprintf(stderr, "Total Files:           %zu\n",    total_files);
    fprintf(stderr, "Time Spent Indexing:   %.2Lfs\n", processtime);
    fprintf(stderr, "Dirs/Sec:              %.2Lf\n",  total_dirs / processtime);
    fprintf(stderr, "Files/Sec:             %.2Lf\n",  total_files / processtime);
    #endif

    return 0;
}
