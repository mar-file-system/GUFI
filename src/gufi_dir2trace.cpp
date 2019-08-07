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



#include <atomic>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <mutex>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

extern "C" {

#include "bf.h"
#include "utils.h"
#include "dbutils.h"
#include "template_db.h"

}

#include "gufi_dir2x.hpp"

FILE ** output;

int output_fin(const int end) {
    for(int i = 0; i < end; i++) {
        fclose(output[i]);
    }
    free(output);
    return 0;
}

int output_init(const int end) {
    if (!(output = (FILE **) malloc(in.maxthreads * sizeof(FILE *)))) {
        fprintf(stderr, "Could not allocate space for per thread file array\n");
        return -1;
    }

    for(int i = 0; i < end; i++) {
        char buf[MAXPATH];
        SNPRINTF(buf, MAXPATH, "%s.%d", in.outfilen, i);
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
bool processdir(struct work & work, const int id, State & state, std::atomic_size_t & queued, std::size_t & next_queue
                #if BENCHMARK
                , std::atomic_size_t & total_dirs, std::atomic_size_t & total_files
                #endif
    ) {

    #if BENCHMARK
    total_dirs++;
    #endif

    DIR * dir = opendir(work.name);
    if (!dir) {
        processdir_cleanup(dir, queued, state);
        return false;
    }

    // get source directory info
    struct stat dir_st;
    if (lstat(work.name, &dir_st) < 0)  {
        processdir_cleanup(dir, queued, state);
        return false;
    }

    struct dirent * entry = nullptr;
    std::size_t rows = 0;
    while ((entry = readdir(dir))) {
        // skip . and ..
        if (entry->d_name[0] == '.') {
            std::size_t len = strlen(entry->d_name);
            if ((len == 1) ||
                ((len == 2) && (entry->d_name[1] == '.'))) {
                continue;
            }
        }

        // get entry path
        struct work e;
        SNPRINTF(e.name, MAXPATH, "%s/%s", work.name, entry->d_name);

        // lstat entry
        if (lstat(e.name, &e.statuso) < 0) {
            continue;
        }

        // push subdirectories onto the queue
        if (S_ISDIR(e.statuso.st_mode)) {
            e.pinode = work.statuso.st_ino;

            // put the previous work on the queue
            {
                std::lock_guard <std::mutex> lock(state[next_queue].first.mutex);
                state[next_queue].first.queue.emplace_back(std::move(e));
                queued++;
                state[next_queue].first.cv.notify_all();
            }

            // round robin
            next_queue++;
            next_queue %= in.maxthreads;

            continue;
        }

        rows++;

        // non directories
        if (S_ISLNK(e.statuso.st_mode)) {
            e.type[0] = 'l';
            readlink(e.name, e.linkname, MAXPATH);
        }
        else if (S_ISREG(e.statuso.st_mode)) {
            e.type[0] = 'f';
        }

        #if BENCHMARK
        total_files++;
        #endif

        fprintf(output[id], "%s%c", e.name, in.delim[0]);
        fprintf(output[id], "%c%c", e.type[0], in.delim[0]);
        fprintf(output[id], "%" STAT_ino "%c", e.statuso.st_ino, in.delim[0]);
        fprintf(output[id], "%d%c", e.statuso.st_mode, in.delim[0]);
        fprintf(output[id], "%" STAT_nlink"%c", e.statuso.st_nlink, in.delim[0]);
        fprintf(output[id], "%d%c", e.statuso.st_uid, in.delim[0]);
        fprintf(output[id], "%d%c", e.statuso.st_gid, in.delim[0]);
        fprintf(output[id], "%" STAT_size "%c", e.statuso.st_size, in.delim[0]);
        fprintf(output[id], "%" STAT_bsize "%c", e.statuso.st_blksize, in.delim[0]);
        fprintf(output[id], "%" STAT_blocks "%c", e.statuso.st_blocks, in.delim[0]);
        fprintf(output[id], "%lld%c", e.statuso.st_atime, in.delim[0]);
        fprintf(output[id], "%lld%c", e.statuso.st_mtime, in.delim[0]);
        fprintf(output[id], "%lld%c", e.statuso.st_ctime, in.delim[0]);
        fprintf(output[id], "%s%c", e.linkname, in.delim[0]);
        fprintf(output[id], "%s%c", e.xattr, in.delim[0]);
        fprintf(output[id], "%d%c", e.crtime, in.delim[0]);
        fprintf(output[id], "%lld%c", e.pinode, in.delim[0]);
        fprintf(output[id], "\n");
    }

    processdir_cleanup(dir, queued, state);

    return true;
}

// This app allows users to do any of the following: (a) just walk the
// input tree, (b) like a, but also creating corresponding GUFI-tree
// directories, (c) like b, but also creating an index.
int validate_inputs() {
    if (!in.outfile || !strlen(in.outfilen)) {
        fprintf(stderr, "No output file name provided (-o)\n");
        return -1;
    }

    struct stat src_st;
    if (lstat(in.name, &src_st) < 0) {
        fprintf(stderr, "Could not stat source directory \"%s\"\n", in.name);
        return -1;
    }

    if (!S_ISDIR(src_st.st_mode)) {
        fprintf(stderr, "Source path is not a directory \"%s\"\n", in.name);
        return -1;
    }

    return 0;
}

void sub_help() {
    printf("input_dir         walk this tree to produce trace file\n");
    // printf("GUFI_dir          build GUFI index here (if -b)\n");
    printf("\n");
}

int main(int argc, char * argv[]) {
    int idx = parse_cmd_line(argc, argv, "hHn:d:o:", 1, "input_dir", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        // parse positional args, following the options
        int retval = 0;
        INSTALL_STR(in.name,   argv[idx++], MAXPATH, "input_dir");

        if (retval)
            return retval;
    }
    if (validate_inputs())
        return -1;


    if (output_init(in.maxthreads) != 0) {
        return -1;
    }

    State state(in.maxthreads);
    std::atomic_size_t queued(0); // so long as one directory is queued, there is potential for more subdirectories, so don't stop the thread

    #if BENCHMARK
    std::atomic_size_t total_dirs(0);
    std::atomic_size_t total_files(0);

    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    #endif

    const bool spawned_threads = processinit(queued, state
                                             #if BENCHMARK
                                             , total_dirs, total_files
                                             #endif
                                            );
    processfin(state, spawned_threads);

    #if BENCHMARK
    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);
    const long double processtime = elapsed(&start, &end);

    fprintf(stderr, "Total Dirs:            %zu\n",  total_dirs.load());
    fprintf(stderr, "Total Files:           %zu\n",  total_files.load());
    fprintf(stderr, "Time Spent Indexing:   %Lfs\n", processtime);
    fprintf(stderr, "Dirs/Sec:              %Lf\n",  total_dirs.load() / processtime);
    fprintf(stderr, "Files/Sec:             %Lf\n",  total_files.load() / processtime);
    #endif

    output_fin(in.maxthreads);

    return 0;
}
