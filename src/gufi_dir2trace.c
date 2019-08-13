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

-----
NOTE:
-----

GUFI uses the C-Thread-Pool library.  The original version, written by
Johan Hanssen Seferidis, is found at
https://github.com/Pithikos/C-Thread-Pool/blob/master/LICENSE, and is
released under the MIT License.  LANS, LLC added functionality to the
original work.  The original work, plus LANS, LLC added functionality is
found at https://github.com/jti-lanl/C-Thread-Pool, also under the MIT
License.  The MIT License can be found at
https://opensource.org/licenses/MIT.


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



#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <unistd.h>

#include "bf.h"
#include "utils.h"
#include "dbutils.h"
#include "template_db.h"
#include "trace.h"
#include "QueuePerThreadPool.h"

#if BENCHMARK
#include <time.h>

pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;
size_t total_dirs = 0;
size_t total_files = 0;
#endif

// per thread output file handles
FILE ** output;

// close all output files
int output_fin(const int end) {
    for(int i = 0; i < end; i++) {
        fclose(output[i]);
    }
    free(output);
    return 0;
}

// allocate the array of FILE * and open file
int output_init(char * prefix, const int count) {
    if (!(output = (FILE **) malloc(in.maxthreads * sizeof(FILE *)))) {
        fprintf(stderr, "Could not allocate space for per thread file array\n");
        return -1;
    }

    for(int i = 0; i < count; i++) {
        char buf[MAXPATH];
        SNPRINTF(buf, MAXPATH, "%s.%d", prefix, i);
        if (!(output[i] = fopen(buf, "w"))) {
            output_fin(i);
            fprintf(stderr, "Could not open thread file %s\n", buf);
            return -1;
        }
    }
    return 0;
}

// process the work under one directory (no recursion)
// deletes work
int processdir(struct QPTPool * ctx, void * data, const size_t id, size_t * next_queue, void * args) {
    #if BENCHMARK
    pthread_mutex_lock(&global_mutex);
    total_dirs++;
    pthread_mutex_unlock(&global_mutex);
    #endif

    if (!ctx || !data) {
        return 0;
    }

    struct work * work = (struct work *) data;

    DIR * dir = opendir(work->name);
    if (!dir) {
        closedir(dir);
        return 0;
    }

    // get source directory info
    struct stat dir_st;
    if (lstat(work->name, &dir_st) < 0)  {
        closedir(dir);
        return 0;
    }

    worktofile(output[id], in.delim, work);

    struct dirent * entry = NULL;
    size_t rows = 0;
    while ((entry = readdir(dir))) {
        // skip . and ..
        if (entry->d_name[0] == '.') {
            size_t len = strlen(entry->d_name);
            if ((len == 1) ||
                ((len == 2) && (entry->d_name[1] == '.'))) {
                continue;
            }
        }

        // get entry path
        struct work * e = (struct work *) calloc(1, sizeof(struct work));
        SNPRINTF(e->name, MAXPATH, "%s/%s", work->name, entry->d_name);

        // get the entry's metadata
        if (lstat(e->name, &e->statuso) < 0) {
            continue;
        }

        e->xattrs=0;
        if (in.doxattrs > 0) {
            memset(e->xattr, 0, sizeof(e->xattr));
            e->xattrs = pullxattrs(e->name, e->xattr);
        }

        // push subdirectories onto the queue
        if (S_ISDIR(e->statuso.st_mode)) {
            e->type[0] = 'd';
            e->pinode = work->statuso.st_ino;
            QPTPool_enqueue_internal(ctx, e, next_queue);
            continue;
        }

        rows++;

        // non directories
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

        worktofile(output[id], in.delim, e);
    }

    closedir(dir);

    return 1;
}

// This app allows users to do any of the following: (a) just walk the
// input tree, (b) like a, but also creating corresponding GUFI-tree
// directories, (c) like b, but also creating an index.
int validate_inputs(struct work * root) {
    SNPRINTF(root->name, MAXPATH, "%s", in.name);

    // get input path metadata
    if (lstat(root->name, &root->statuso) < 0) {
        fprintf(stderr, "Could not stat source directory \"%s\"\n", in.name);
        return -1;
    }

    // check that the input path is a directory
    if (S_ISDIR(root->statuso.st_mode)) {
        root->type[0] = 'd';
    }
    else {
        fprintf(stderr, "Source path is not a directory \"%s\"\n", in.name);
        return -1;
    }

    // check if the source directory can be accessed
    static mode_t PERMS = R_OK | X_OK;
    if ((root->statuso.st_mode & PERMS) != PERMS) {
        fprintf(stderr, "couldn't access input dir '%s': %s\n",
                in.name, strerror(errno));
        return 1;
    }

   if (!strlen(in.outfilen)) {
       fprintf(stderr, "No output file name specified\n");
       return -1;
   }

   // check if the destination path already exists (not an error)
   for(int i = 0; i < in.maxthreads; i++) {
       char outname[MAXPATH];
       SNPRINTF(outname, MAXPATH, "%s.%d", in.outfilen, i);

       struct stat dst_st;
       if (lstat(in.outfilen, &dst_st) == 0) {
           fprintf(stderr, "\"%s\" Already exists!\n", in.nameto);

           // if the destination path is not a directory (error)
           if (S_ISDIR(dst_st.st_mode)) {
               fprintf(stderr, "Destination path is a directory \"%s\"\n", in.outfilen);
               return -1;
           }
       }
   }

   if (in.doxattrs > 0) {
       root->xattrs = pullxattrs(in.name, root->xattr);
   }

   return 0;
}

void sub_help() {
    printf("input_dir         walk this tree to produce trace file\n");
    printf("out_file          output prefix\n");
    printf("\n");
}

int main(int argc, char * argv[]) {
    int idx = parse_cmd_line(argc, argv, "hHn:d:", 1, "input_dir out_file", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        // parse positional args, following the options
        int retval = 0;
        INSTALL_STR(in.name,     argv[idx++], MAXPATH, "input_dir");
        INSTALL_STR(in.outfilen, argv[idx++], MAXPATH, "out_file");

        if (retval)
            return retval;
    }

    struct work * root = (struct work *) calloc(1, sizeof(struct work));
    if (!root) {
        fprintf(stderr, "Could not allocate root struct\n");
        return -1;
    }

    if (validate_inputs(root))
        return -1;

    if (output_init(in.outfilen, in.maxthreads) != 0) {
        return -1;
    }

    #if BENCHMARK
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    #endif

    struct QPTPool * pool = QPTPool_init(in.maxthreads);
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        return -1;
    }

    QPTPool_enqueue_external(pool, root);
    if (QPTPool_start(pool, processdir, NULL) != (size_t) in.maxthreads) {
        fprintf(stderr, "Failed to start all threads\n");
        return -1;
    }

    QPTPool_wait(pool);
    QPTPool_destroy(pool);

    #if BENCHMARK
    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);
    const long double processtime = elapsed(&start, &end);

    fprintf(stderr, "Total Dirs:            %zu\n",    total_dirs);
    fprintf(stderr, "Total Files:           %zu\n",    total_files);
    fprintf(stderr, "Time Spent Indexing:   %.2Lfs\n", processtime);
    fprintf(stderr, "Dirs/Sec:              %.2Lf\n",  total_dirs / processtime);
    fprintf(stderr, "Files/Sec:             %.2Lf\n",  total_files / processtime);
    #endif

    output_fin(in.maxthreads);

    return 0;
}
