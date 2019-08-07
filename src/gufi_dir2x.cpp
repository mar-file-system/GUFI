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
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <list>
#include <mutex>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <vector>
#include <unistd.h>

extern "C" {

#include "bf.h"
#include "utils.h"
#include "dbutils.h"
#include "template_db.h"

}

extern int errno;

#include "gufi_dir2x.hpp"

int templatefd = -1;    // this is really a constant that is set at runtime
off_t templatesize = 0; // this is really a constant that is set at runtime

typedef std::pair <ThreadWork <struct work>, std::thread> WorkPair;
typedef std::vector <WorkPair> State;

sqlite3 * opendb(const char *name)
{
    sqlite3 * db = NULL;

    // no need to create because the file should already exist
    if (sqlite3_open_v2(name, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, "unix-none") != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db));
        return NULL;
    }

    // try to turn sychronization off
    if (sqlite3_exec(db, "PRAGMA synchronous = OFF", NULL, NULL, NULL) != SQLITE_OK) {
        std::cerr << "Could not turn off synchronization" << std::endl;
    }

    // try to turn journaling off
    if (sqlite3_exec(db, "PRAGMA journal_mode = OFF", NULL, NULL, NULL) != SQLITE_OK) {
        std::cerr << "Could not turn off journaling" << std::endl;
    }

    // try to store temp_store in memory
    if (sqlite3_exec(db, "PRAGMA temp_store = 2", NULL, NULL, NULL) != SQLITE_OK) {
        std::cerr << "Could not set temporary storage to in-memory" << std::endl;
    }

    // try to increase the page size
    if (sqlite3_exec(db, "PRAGMA page_size = 16777216", NULL, NULL, NULL) != SQLITE_OK) {
        std::cerr << "Could not set page size" << std::endl;
    }

    // try to increase the cache size
    if (sqlite3_exec(db, "PRAGMA cache_size = 16777216", NULL, NULL, NULL) != SQLITE_OK) {
        std::cerr << "Could not set cache size" << std::endl;
    }

    // try to get an exclusive lock
    if (sqlite3_exec(db, "PRAGMA locking_mode = EXCLUSIVE", NULL, NULL, NULL) != SQLITE_OK) {
        std::cerr << "Could not set locking mode" << std::endl;
    }

    return db;
}

void processdir_cleanup(DIR * dir, std::atomic_size_t & queued, State & state) {
    closedir(dir);

    queued--;

    for(int i = 0; i < in.maxthreads; i++) {
        state[i].first.cv.notify_all();
    }
}

// consumer of work
std::size_t worker_function(std::atomic_size_t & queued, State & state, ThreadWork <struct work> & tw
                            #if BENCHMARK
                            , std::atomic_size_t & total_dirs, std::atomic_size_t & total_files
                            #endif
    ) {
    std::size_t next_queue = tw.id;

    std::size_t processed = 0;
    while (true) {
        std::list <struct work> dirs;
        {
            std::unique_lock <std::mutex> lock(tw.mutex);

            // wait for work
            while (queued && !tw.queue.size()) {
                tw.cv.wait(lock);
            }

            if (!queued && !tw.queue.size()) {
                break;
            }

            // // take single work item
            // dirs.push_back(tw.queue.front());
            // tw.queue.pop_front();

            // take all work
            dirs = std::move(tw.queue);
            tw.queue.clear();
        }

        // process all work
        for(struct work & dir : dirs) {
            processed += processdir(tw.id, dir, state, queued, next_queue
                                    #if BENCHMARK
                                    , total_dirs, total_files
                                    #endif
                                   );
        }
    }

    return processed;
}

bool processinit(std::atomic_size_t & queued, State & state
                 #if BENCHMARK
                 , std::atomic_size_t & total_dirs, std::atomic_size_t & total_files
                 #endif
    ) {
    // check if the privided path is accessible
    if (access(in.name, R_OK | X_OK)) {
        fprintf(stderr, "couldn't access input dir '%s': %s\n",
                in.name, strerror(errno));
        return false;
    }

    // get the path's information
    struct work root;
    if (lstat(in.name, &root.statuso) < 0) {
        fprintf(stderr,"could not lstat '%s'\n", in.name);
        return false;
    }

    if (!S_ISDIR(root.statuso.st_mode) ) {
        fprintf(stderr,"input-dir '%s' is not a directory\n", in.name);
        return false;
    }

    SNPRINTF(root.name, MAXPATH, "%s", in.name);
    memset(root.xattr, 0, sizeof(root.xattr));
    memset(root.linkname, 0, sizeof(root.linkname));
    if (in.doxattrs > 0) {
        root.xattrs = 0;
        root.xattrs = pullxattrs(root.name, root.xattr);
    }
    root.pinode = 0;

    // create the destination path
    dupdir(in.nameto, &root.statuso);

    // so long as there is a job in the queue, there is potential for more subdirectories
    queued = 1;

    // start up threads
    for(int i = 0; i < in.maxthreads; i++) {
        state[i].first.id = i;
        state[i].second = std::thread(worker_function, std::ref(queued), std::ref(state), std::ref(state[i].first)
                                      #if BENCHMARK
                                      , std::ref(total_dirs), std::ref(total_files)
                                      #endif
                                      );
    }

    // push work onto first thread's queue
    state[0].first.queue.emplace_back(std::move(root));
    state[0].first.cv.notify_all();

    return true;
}

void processfin(State & state, const bool spawned_threads) {
    if (spawned_threads) {
        for(WorkPair & wp : state) {
            wp.second.join();
        }
    }
}
