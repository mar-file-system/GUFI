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



#include <atomic>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <iostream>
#include <libgen.h>
#include <list>
#include <mutex>
#include <string>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include <sqlite3.h>

#define MAXPATH 4096
#define MAXXATTR 1024
#define MAXSQL 4096

// Data stored during first pass of input file
struct first {
    first()
        : first_delim(std::string::npos),
          line(),
          offset(-1),
          entries(0)
    {}

    std::string::size_type first_delim;
    std::string line;
    off_t offset;
    std::size_t entries;
};

typedef struct first * Row;

Row new_row() {
    return new struct first();
}

void delete_row(Row & row) {
    delete row;
}

void parsefirst (const char delim, Row & work) {
    work->first_delim = work->line.find(delim);
}

// basically thread args
struct ThreadWork {
    ThreadWork()
        : id(0),
          queue(),
          mutex(),
          cv()
    {}

    int id;
    std::list <Row> queue;
    std::mutex mutex;
    std::condition_variable cv;
};

typedef std::pair <ThreadWork, std::thread> WorkPair;
typedef std::vector <WorkPair> State;

void scout_function(bool & correct, std::atomic_bool & scouting, const char * filename, State & consumers) {
    // figure out whether or not this function is correct
    std::ifstream file(filename, std::ios::binary);
    if (!(scouting = (bool) file)) {
        std::cerr << "Could not open file " << filename << std::endl;
        return;
    }

    Row work = new_row();
    if (!std::getline(file, work->line)) {
        delete_row(work);
        return;
    }

    parsefirst('\x1e', work);
    work->offset = file.tellg();

    int tid = 0;
    std::size_t file_count = 0;
    std::size_t dir_count = 1; // always start with a directory
    std::size_t empty = 0;

    std::string line;
    while (correct && std::getline(file, line)) {
        Row next = new_row();
        next->line = std::move(line);

        // parse
        parsefirst('\x1e', next);

        // push directories onto queues
        if (next->line[next->first_delim + 1] == 'd') {
            dir_count++;

            empty += !work -> entries;
            next->offset = file.tellg();

            // put the previous work on the queue
            {
                std::lock_guard <std::mutex> lock(consumers[tid].first.mutex);
                if (work -> entries) {
                    consumers[tid].first.queue.emplace_back(std::move(work));
                }
                consumers[tid].first.cv.notify_all();
            }

            // round robin
            tid++;
            tid %= consumers.size();

            work = next;
        }
        else {
            work->entries++;
            file_count++;
            delete_row(next);
        }
    }

    // insert the last work item
    if (correct) {
        std::lock_guard <std::mutex> lock(consumers[tid].first.mutex);
        if (work -> entries) {
            consumers[tid].first.queue.emplace_back(std::move(work));
        }

        consumers[tid].first.cv.notify_all();
    }

    scouting = false;

    // release all condition variables
    for(std::size_t i = 0; i < consumers.size(); i++) {
        consumers[i].first.cv.notify_all();
    }

    std::cerr << "Done scouting" << std::endl
              << "Files: " << file_count << std::endl
              << "Dirs:  " << dir_count << " (" << empty << " empty)" << std::endl
              << "Total: " << file_count + dir_count << std::endl;

    return;
}

struct work {
   char          name[MAXPATH];
   char          type[2];
   char          linkname[MAXPATH];
   struct stat   statuso;
   char          xattr[MAXXATTR];
   int           crtime;
};

void parsetowork (char * delim, char * line, struct work * pinwork) {
    char *p;
    char *q;

    //printf("in parsetowork delim %s line %s\n",delim,line);
    line[strlen(line)-1]= '\0';
    p=line;    q=strstr(p,delim); memset(q, 0, 1); snprintf(pinwork->name,MAXPATH,"%s",p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); snprintf(pinwork->type,2,"%s",p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); pinwork->statuso.st_ino=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); pinwork->statuso.st_mode=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); pinwork->statuso.st_nlink=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); pinwork->statuso.st_uid=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); pinwork->statuso.st_gid=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); pinwork->statuso.st_size=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); pinwork->statuso.st_blksize=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); pinwork->statuso.st_blocks=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); pinwork->statuso.st_atime=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); pinwork->statuso.st_mtime=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); pinwork->statuso.st_ctime=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); snprintf(pinwork->linkname,MAXPATH,"%s",p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); snprintf(pinwork->xattr,MAXXATTR,"%s",p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); pinwork->crtime=atol(p);
}

int count_callback(void * arg, int, char **, char **) {
    int * count = (int *) arg;
    (void) (*count)++;
    return 0;
}

bool check_stanza(Row & row, std::istream & stream, const std::string & tree, const char delim = '\x1e', const char dir = 'd') {
    static char d[2] = {delim, '\0'};

    std::string line = row -> line;

    const std::string::size_type first_delim = line.find(delim);
    if (first_delim == std::string::npos) {
        std::cerr << "Missing delimiter: " << line << std::endl;
        return false;
    }

    // assume that a directory was found
    const std::string parent = tree + "/" + line.substr(0, first_delim - (line[first_delim - 1] == '/'));

    // check the parent directory's properties
    {
        // make sure the path exists
        struct stat st;
        if (stat(parent.c_str(), &st) != 0) {
            std::cerr << "Parent doesn't exist: " << parent << std::endl;
            return false;
        }

        // make sure the path is a directory
        if (!S_ISDIR(st.st_mode)) {
            std::cerr << "Parent is not a directory: " << parent << std::endl;
            return false;
        }

        // parse the line to check other values
        char buf[MAXPATH] = {};
        memcpy(buf, line.c_str(), line.size());
        struct work work;
        parsetowork(d, buf, &work);

        // make sure the permissions are correct
        if (st.st_mode != work.statuso.st_mode) {
            std::cerr << "Parent permission is incorrect:" << parent << std::endl;
            return false;
        }

        // make sure the uid is correct
        if (st.st_uid != work.statuso.st_uid) {
            std::cerr << "Parent uid incorrect: " << parent << std::endl;
            return false;
        }

        // make sure the gid is correct
        if (st.st_gid != work.statuso.st_gid) {
            std::cerr << "Parent gid incorrect: " << parent << std::endl;
            return false;
        }
    }

    const std::string db_name = parent + "/db.db";

    // if the database path is too long, skip checking the entries
    if (db_name.size() > 490) {
        std::cerr << "Warning: Name too long. Skipping " << db_name << std::endl;
        return true;
    }

    // move the trace file to the starting non-directory
    stream.seekg(row -> offset);

    // followed by a series of non-directories
    while (true) {
        const std::istream::streampos pos = stream.tellg();

        if (!std::getline(stream, line)) {
            break;
        }

        const std::string::size_type first_delim = line.find(delim);
        if (first_delim == std::string::npos) {
            std::cerr << "Missing delimiter: " << line << std::endl;
            return false;
        }

        // stop when a directory is encountered (new stanza)
        if (line[first_delim + 1] == dir) {
            // go back to the beginning of this line
            stream.seekg(pos);
            break;
        }

        // assume the current line is a direct child of the parent

        // parse the line
        char buf[MAXPATH] = {};
        memcpy(buf, line.c_str(), line.size());
        struct work work;
        parsetowork(d, buf, &work);

        // since there should be a database file, open it
        sqlite3 * db = nullptr;
        if (sqlite3_open_v2(db_name.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) {
            std::cerr << "Could not open database file: " << db_name << std::endl;
            return false;
        }

        // extract the basename from the entry name
        char * bufbase = basename(work.name);

        // query the database for the current entry
        char sql[1024];
        snprintf(sql, 1024, "SELECT * FROM entries WHERE (name == '%s') AND (uid == %d) AND (gid == %d);", bufbase, work.statuso.st_uid, work.statuso.st_gid);
        int results = 0;
        char * err = nullptr;
        if (sqlite3_exec(db, sql, count_callback, &results, &err) != SQLITE_OK) {
            std::cerr << "SQLite error: " << err << ": " << sql << std::endl;
            sqlite3_free(err);
            sqlite3_close(db);
            return false;
        }

        if (results != 1) {
            std::cerr << "Expected 1 row. Got " << results << ": " << parent << "/db.db " << bufbase << std::endl;
            sqlite3_close(db);
            return false;
        }

        sqlite3_close(db);
    }

    return true;
}

void worker(std::mutex & correct_mutex, bool & correct,
            std::atomic_bool & scouting,
            char * trace_name,
            const std::string & GUFI_tree,
            ThreadWork & tw) {
    std::ifstream trace(trace_name, std::ios::binary);
    if (!trace) {
        std::cerr << "Could not open " << trace_name << ". Stopping Thread" << std::endl;
        correct = false;
        return;
    }

    while (correct || scouting || tw.queue.size()) {
        std::list <Row> dirs;
        {
            std::unique_lock <std::mutex> lock(tw.mutex);

            // wait for work
            while (correct && scouting && !tw.queue.size()) {
                tw.cv.wait(lock);
            }

            if (!correct || (!scouting && !tw.queue.size())) {
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
        for(Row & dir : dirs) {
            // std::cout << tw.id << " " << dirs.size() << " " << dir->line << std::endl;
            const bool rc = check_stanza(dir, trace, GUFI_tree);
            std::lock_guard <std::mutex> lock(correct_mutex);
            correct &= rc;
        }
    }
}

int main(int argc, char * argv[]) {
    if (argc < 3) {
        std::cerr << "Syntax: " << argv[0] << " trace GUFI_tree [threads]" << std::endl;
        return 0;
    }

    std::size_t thread_count = std::thread::hardware_concurrency();
    if (argc > 3) {
        std::stringstream s;
        s << argv[3];

        if (!(s >> thread_count)) {
            std::cerr << "Error: Bad thread count: " << argv[3] << std::endl;
            return 1;
        }
    }

    const std::string GUFI_tree = argv[2];

    State state(thread_count);
    std::mutex correct_mutex;
    bool correct = true;
    std::atomic_bool scouting(true);

    // start threads
    std::thread scout(scout_function, std::ref(correct), std::ref(scouting), argv[1], std::ref(state));
    for(std::size_t i = 0; i < thread_count; i++) {
        state[i].first.id = i;
        state[i].second = std::thread(worker, std::ref(correct_mutex), std::ref(correct), std::ref(scouting), argv[1], std::ref(GUFI_tree), std::ref(state[i].first));
    }

    // wait for all threads to finish
    scout.join();
    for(WorkPair & wp : state) {
        wp.second.join();
    }

    std::cout << argv[1] << " " << (correct?"Pass":"Fail")  << std::endl;

    return 0;
}
