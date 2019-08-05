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



#if BENCHMARK
#include <atomic>
#endif

#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

extern "C" {

#include "bf.h"
#include "utils.h"
#include "dbutils.h"
#include "template_db.h"

}

extern int errno;

#include "gufi_dir2x.hpp"

// process the work under one directory (no recursion)
// deletes work
bool processdir(struct work & work, State & state, std::atomic_size_t & queued, std::size_t & next_queue
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

        // print to stdout, to be redirected by caller
        // this should probably be changed to write to a per thread database/file to avoid locking
        static std::mutex print_mutex;
        std::lock_guard <std::mutex> lock(print_mutex);

        fprintf(stdout, "%s%c", e.name, in.delim[0]);
        fprintf(stdout, "%c%c", e.type[0], in.delim[0]);
        fprintf(stdout, "%" STAT_ino "%c", e.statuso.st_ino, in.delim[0]);
        fprintf(stdout, "%d%c", e.statuso.st_mode, in.delim[0]);
        fprintf(stdout, "%" STAT_nlink"%c", e.statuso.st_nlink, in.delim[0]);
        fprintf(stdout, "%d%c", e.statuso.st_uid, in.delim[0]);
        fprintf(stdout, "%d%c", e.statuso.st_gid, in.delim[0]);
        fprintf(stdout, "%" STAT_size "%c", e.statuso.st_size, in.delim[0]);
        fprintf(stdout, "%" STAT_bsize "%c", e.statuso.st_blksize, in.delim[0]);
        fprintf(stdout, "%" STAT_blocks "%c", e.statuso.st_blocks, in.delim[0]);
        fprintf(stdout, "%d%c", e.statuso.st_atime, in.delim[0]);
        fprintf(stdout, "%d%c", e.statuso.st_mtime, in.delim[0]);
        fprintf(stdout, "%d%c", e.statuso.st_ctime, in.delim[0]);
        fprintf(stdout, "%s%c", e.linkname, in.delim[0]);
        fprintf(stdout, "%s%c", e.xattr, in.delim[0]);
        fprintf(stdout, "%d%c", e.crtime, in.delim[0]);
        fprintf(stdout, "%lld%c", e.pinode, in.delim[0]);
        fprintf(stdout, "\n");
    }

    processdir_cleanup(dir, queued, state);

    return true;
}

// This app allows users to do any of the following: (a) just walk the
// input tree, (b) like a, but also creating corresponding GUFI-tree
// directories, (c) like b, but also creating an index.
int validate_inputs() {
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
    int idx = parse_cmd_line(argc, argv, "hHpn:d:", 1, "input_dir", &in);
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

    struct stat st;
    if (lstat(in.name, &st) < 0) {
        fprintf(stderr, "Could not stat directory \"%s\"\n", in.name);
        return -1;
    }

    State state(in.maxthreads);
    std::atomic_size_t queued(0); // so long as one directory is queued, there is potential for more subdirectories, so don't stop the thread
    std::vector <std::size_t> processed(in.maxthreads);

    #if BENCHMARK
    std::atomic_size_t total_dirs(0);
    std::atomic_size_t total_files(0);

    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
    #endif

    const bool spawned_threads = processinit(queued, state
                                            #if BENCHMARK
                                            , total_dirs, total_files
                                            #endif
                                            );
    processfin(state, spawned_threads);

    #if BENCHMARK
    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();

    const long double processtime = std::chrono::duration_cast <std::chrono::nanoseconds> (end - start).count() / 1e9L;
    fprintf(stderr, "Total Dirs:            %zu\n",  total_dirs.load());
    fprintf(stderr, "Total Files:           %zu\n",  total_files.load());
    fprintf(stderr, "Time Spent Indexing:   %Lfs\n", processtime);
    fprintf(stderr, "Dirs/Sec:              %Lf\n",  total_dirs.load() / processtime);
    fprintf(stderr, "Files/Sec:             %Lf\n",  total_files.load() / processtime);
    #endif

    return 0;
}
