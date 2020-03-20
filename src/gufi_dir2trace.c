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
#include <sys/xattr.h>
#include <unistd.h>

#include "SinglyLinkedList.h"
#include "QueuePerThreadPool.h"
#include "bf.h"
#include "dbutils.h"
#include "outfiles.h"
#include "template_db.h"
#include "trace.h"
#include "utils.h"

extern int errno;

#if BENCHMARK
#include <time.h>

pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;
size_t total_dirs = 0;
size_t total_files = 0;
#endif

/* process the work under one directory (no recursion) */
/* deletes work */
int processdir(struct QPTPool * ctx, const size_t id, void * data, void * args) {
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

    struct work * work = (struct work *) data;

    DIR * dir = opendir(work->name);
    if (!dir) {
        closedir(dir);
        free(data);
        return 1;
    }

    /* get source directory info */
    struct stat dir_st;
    if (lstat(work->name, &dir_st) < 0)  {
        closedir(dir);
        free(data);
        return 1;
    }

    /* get a copy of the full source path */
    char work_name[MAXPATH];
    size_t work_name_len = strlen(work->name);
    SNFORMAT_S(work_name, MAXPATH, 1, work->name, work_name_len);

    /* remove this directory's path prefix for writing to the trace file */
    SNFORMAT_S(work->name, MAXPATH, 1, work_name + in.name_len, work_name_len - in.name_len);

    /* a stanza starts with the directory */
    struct sll stanza_list;
    sll_init(&stanza_list);
    sll_push(&stanza_list, work);

    size_t stanza_buffer_size = STANZA_SIZE;

    struct dirent * entry = NULL;
    size_t rows = 0;
    while ((entry = readdir(dir))) {
        /* skip . and .. */
        if (entry->d_name[0] == '.') {
            size_t len = strlen(entry->d_name);
            if ((len == 1) ||
                ((len == 2) && (entry->d_name[1] == '.'))) {
                continue;
            }
        }

        /* get entry path */
        struct work * e = calloc(1, sizeof(struct work));
        memset(e->name, 0, MAXPATH);
        SNPRINTF(e->name, MAXPATH, "%s/%s", work_name, entry->d_name);

        /* get the entry's metadata */
        if (lstat(e->name, &e->statuso) < 0) {
            continue;
        }

        e->xattrs=0;
        if (in.doxattrs > 0) {
            e->xattrs = pullxattrs(e->name, e->xattr);
        }

        /* push subdirectories onto the queue */
        if (S_ISDIR(e->statuso.st_mode)) {
            e->type[0] = 'd';
            e->pinode = work->statuso.st_ino;
            QPTPool_enqueue(ctx, id, processdir, e);
            continue;
        }

        rows++;

        /* non directories */
        if (S_ISLNK(e->statuso.st_mode)) {
            e->type[0] = 'l';
            readlink(e->name, e->linkname, MAXPATH);
        }
        else if (S_ISREG(e->statuso.st_mode)) {
            e->type[0] = 'f';
        }

        #if BENCHMARK
        pthread_mutex_lock(&global_mutex);
        total_files++;
        pthread_mutex_unlock(&global_mutex);
        #endif

        if (work_name_len <= in.name_len) {
            SNPRINTF(e->name, MAXPATH, "%s", entry->d_name);
        }
        else {
            /* offset by in.name_len to remove prefix */
            SNPRINTF(e->name, MAXPATH, "%s/%s", work_name + in.name_len, entry->d_name);
        }

        sll_push(&stanza_list, e);
        stanza_buffer_size += STANZA_SIZE;
    }

    closedir(dir);

    /* write out the stanza */
    void * stanza_buf = calloc(1, stanza_buffer_size);
    if (stanza_buf)  {
        /* convert the entries into text */
        size_t offset = 0;
        for(struct node * node = sll_head_node(&stanza_list); node; node = sll_next_node(node)) {
            offset += worktobuf(stanza_buf + offset, stanza_buffer_size - offset, in.delim, sll_node_data(node));
        }

        /* write the text to the file in one pass */
        pthread_mutex_t * print_mutex = (pthread_mutex_t *) args;
        if (print_mutex) {
            pthread_mutex_lock(print_mutex);
        }

        fwrite(stanza_buf, sizeof(char), offset, gts.outfd[id]);

        if (print_mutex) {
            pthread_mutex_unlock(print_mutex);
        }
    }
    else {
        fprintf(stderr, "Could not allocate buffer for %s\n", work_name);
    }

    free(stanza_buf);
    sll_destroy(&stanza_list, 1);

    return 0;
}

struct work * validate_inputs() {
    struct work * root = (struct work *) calloc(1, sizeof(struct work));
    if (!root) {
        fprintf(stderr, "Could not allocate root struct\n");
        return NULL;
    }

    SNPRINTF(root->name, MAXPATH, "%s", in.name);

    /* get input path metadata */
    if (lstat(root->name, &root->statuso) < 0) {
        fprintf(stderr, "Could not stat source directory \"%s\"\n", root->name);
        free(root);
        return NULL;
    }

    /* check that the input path is a directory */
    if (S_ISDIR(root->statuso.st_mode)) {
        root->type[0] = 'd';
    }
    else {
        fprintf(stderr, "Source path is not a directory \"%s\" %d\n", root->name, S_ISDIR(root->statuso.st_mode));
        free(root);
        return NULL;
    }

    /* check if the source directory can be accessed */
    if (access(root->name, R_OK | X_OK) != 0) {
        fprintf(stderr, "couldn't access input dir '%s': %s\n",
                root->name, strerror(errno));
        free(root);
        return NULL;
    }

    /* check the output files, if a prefix was provided */
    if (in.outfile) {
        if (!strlen(in.outfilen)) {
            fprintf(stderr, "No output file name specified\n");
            free(root);
            return NULL;
        }

        /* check if the destination path already exists (not an error) */
        for(int i = 0; i < in.maxthreads; i++) {
            char outname[MAXPATH];
            SNPRINTF(outname, MAXPATH, "%s.%d", in.outfilen, i);

            struct stat dst_st;
            if (lstat(outname, &dst_st) == 0) {
                fprintf(stderr, "\"%s\" Already exists!\n", outname);

                /* if the destination path is not a directory (error) */
                if (S_ISDIR(dst_st.st_mode)) {
                    fprintf(stderr, "Destination path is a directory \"%s\"\n", in.outfilen);
                    free(root);
                    return NULL;
                }
            }
        }
    }

    if (in.doxattrs > 0) {
        root->xattrs = pullxattrs(in.name, root->xattr);
    }

    return root;
}

void sub_help() {
    printf("input_dir            walk this tree to produce trace file\n");
    printf("output_prefix        prefix of the per-thread output files\n");
    printf("\n");
}

int main(int argc, char * argv[]) {
    int idx = parse_cmd_line(argc, argv, "hHn:xd:o:", 1, "input_dir output_prefix", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        /* parse positional args, following the options */
        int retval = 0;
        INSTALL_STR(in.name,     argv[idx++], MAXPATH, "input_dir");

        if (retval)
            return retval;
    }

    /* get first work item by validating inputs */
    struct work * root = validate_inputs();
    if (!root) {
        return -1;
    }

    pthread_mutex_t initialized_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t * print_mutex = NULL;
    /* no output file, so writing to stdout, and need to lock */
    if (!in.outfile) {
        print_mutex = &initialized_mutex;
    }

    if (!outfiles_init(gts.outfd, in.outfile, in.outfilen, in.maxthreads)) {
        outfiles_fin(gts.outfd, in.maxthreads);
        return -1;
    }

    #if BENCHMARK
    struct start_end benchmark;
    clock_gettime(CLOCK_MONOTONIC, &benchmark.start);
    #endif

    struct QPTPool * pool = QPTPool_init(in.maxthreads);
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        outfiles_fin(gts.outfd, in.maxthreads);
        return -1;
    }

    if (QPTPool_start(pool, print_mutex) != (size_t) in.maxthreads) {
        fprintf(stderr, "Failed to start threads\n");
        outfiles_fin(gts.outfd, in.maxthreads);
        return -1;
    }

    QPTPool_enqueue(pool, 0, processdir, root);
    QPTPool_wait(pool);
    QPTPool_destroy(pool);

    outfiles_fin(gts.outfd, in.maxthreads);

    #if BENCHMARK
    clock_gettime(CLOCK_MONOTONIC, &benchmark.end);
    const long double processtime = elapsed(&benchmark);

    fprintf(stderr, "Total Dirs:            %zu\n",    total_dirs);
    fprintf(stderr, "Total Files:           %zu\n",    total_files);
    fprintf(stderr, "Time Spent Indexing:   %.2Lfs\n", processtime);
    fprintf(stderr, "Dirs/Sec:              %.2Lf\n",  total_dirs / processtime);
    fprintf(stderr, "Files/Sec:             %.2Lf\n",  total_files / processtime);
    #endif

    return 0;
}
