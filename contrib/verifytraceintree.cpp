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
#include <cstring>
#include <fstream>
#include <iostream>
#include <libgen.h>
#include <string>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>

#include <sqlite3.h>

#include "QueuePerThreadPool.h"
#include "trace.h"

// Data stored during first pass of input file
struct StanzaStart {
    StanzaStart()
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

struct CallbackArgs {
    CallbackArgs()
        : count(0),
          st()
        {}
    std::size_t count;
    struct stat st;
};

static int callback(void *arg, int, char **data, char **) {
    static const int mode_col = 0;
    static const int uid_col  = 1;
    static const int gid_col  = 2;

    CallbackArgs *ca = (CallbackArgs *) arg;
    ca->count++;

    // assume no errors
    sscanf(data[mode_col], "%d", &ca->st.st_mode);
    sscanf(data[uid_col],  "%d", &ca->st.st_uid);
    sscanf(data[gid_col],  "%d", &ca->st.st_gid);

    return 0;
}

struct CheckStanzaArgs {
    CheckStanzaArgs(const std::size_t threads,
                    std::atomic_bool & correct,
                    const char delim,
                    const std::string & tree)
        : threads(threads),
          correct(correct),
          delim(delim),
          tree(tree),
          traces()
    {}

    ~CheckStanzaArgs() {
        for(std::istream *trace : traces) {
            delete trace;
        }
    }

    const std::size_t threads;
    std::atomic_bool & correct;
    const char delim;
    const std::string & tree;
    std::vector <std::istream *> traces;
};

static int check_stanza(QPTPool_t *, const size_t id, void *data, void *args) {
    struct CheckStanzaArgs *csa = static_cast <struct CheckStanzaArgs *> (args);
    std::istream &trace = *(csa->traces[id]);

    struct StanzaStart *sa = static_cast <struct StanzaStart *> (data);

    // stop early
    if (!csa->correct) {
        delete sa;
        return 0;
    }

    // assume that the current directory was found
    std::string parent = csa->tree + "/" + sa->line.substr(0, sa->first_delim);

    // remove trailing /
    std::size_t parent_len = parent.size();
    while (parent_len && (parent[parent_len - 1] == '/')) {
        parent_len--;
    }
    parent.resize(parent_len);

    // check the parent directory's properties
    {
        // make sure the path exists
        struct stat st;
        if (stat(parent.c_str(), &st) != 0) {
            std::cerr << "Source path listed in trace doesn't exist: " << parent << std::endl;
            csa->correct = false;
            delete sa;
            return 1;
        }

        // make sure the path is a directory
        if (!S_ISDIR(st.st_mode)) {
            std::cerr << "Source path listed in trace is not a directory: " << parent << std::endl;
            csa->correct = false;
            delete sa;
            return 1;
        }

        // parse the line to check other values
        char buf[MAXPATH] = {};
        memcpy(buf, sa->line.c_str(), sa->line.size());
        struct work work;
        struct entry_data ed;
        linetowork(buf, sa->line.size(), csa->delim, &work, &ed);
        xattrs_cleanup(&ed.xattrs);

        // make sure the permissions are correct
        if (st.st_mode != ed.statuso.st_mode) {
            std::cerr << "Permission mismatch on directory: " << parent << std::endl;
            csa->correct = false;
            delete sa;
            return 1;
        }

        // make sure the uid is correct
        if (st.st_uid != ed.statuso.st_uid) {
            std::cerr << "UID mismatch on directory: " << parent << std::endl;
            csa->correct = false;
            delete sa;
            return 1;
        }

        // make sure the gid is correct
        if (st.st_gid != ed.statuso.st_gid) {
            std::cerr << "GID mismatch on directory: " << parent << std::endl;
            csa->correct = false;
            delete sa;
            return 1;
        }
    }

    const std::string db_name = parent + "/db.db";

    // if the database path is too long, skip checking the entries
    if (db_name.size() > 490) {
        std::cerr << "Warning: Name too long. Skipping " << db_name << std::endl;
        return 0;;
    }

    // open the database file
    sqlite3 *db = nullptr;
    if (sqlite3_open_v2(db_name.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) {
        std::cerr << "Could not open database file: " << db_name << std::endl;
        csa->correct = false;
        delete sa;
        return 1;
    }

    // move the trace file to the starting non-directory
    trace.seekg(sa->offset);

    // followed by a series of non-directories
    std::size_t entries = 0;
    int rc = 0;
    while (true) {
        // store pos here so it can be jumped back to if this was a directory
        const std::streampos pos = trace.tellg();

        std::string line;
        if (!std::getline(trace, line)) {
            break;
        }

        if (!line.size()) {
            continue;
        }

        // can't get npos here
        const std::string::size_type first_delim = line.find(csa->delim);

        // stop when a directory is encountered (new stanza)
        if (line[first_delim + 1] == 'd') {
            // go back to the beginning of this line
            trace.seekg(pos);
            break;
        }

        // assume the current line is a direct child of the parent

        // parse the line
        char buf[MAXPATH] = {};
        memcpy(buf, line.c_str(), line.size());
        struct work work;
        struct entry_data ed;
        linetowork(buf, line.size(), csa->delim, &work, &ed);
        xattrs_cleanup(&ed.xattrs);

        // extract the basename from the entry name
        char *bufbase = basename(work.name);

        // query the database for the current entry
        char sql[MAXSQL];
        snprintf(sql, MAXSQL, "SELECT mode, uid, gid FROM entries WHERE name == '%s';", bufbase);
        CallbackArgs ca;
        char *err = nullptr;
        if (sqlite3_exec(db, sql, callback, &ca, &err) != SQLITE_OK) {
            std::cerr << "SQLite error: " << err << ": " << sql << std::endl;
            sqlite3_free(err);
            continue;
        }

        if (ca.count != 1) {
            std::cerr << "Did not find entry with name \"" << bufbase << "\""
                      << " in " << db_name << std::endl;
            continue;
        }

        if (ca.st.st_mode != ed.statuso.st_mode) {
            std::cerr << "Permission mismatch on entry \"" << bufbase << "\""
                      << " in " << db_name << std::endl;
            continue;
        }

        if (ca.st.st_uid != ed.statuso.st_uid) {
            std::cerr << "UID mismatch on entry \"" << bufbase << "\""
                      << " in " << db_name << std::endl;
            continue;
        }

        if (ca.st.st_gid != ed.statuso.st_gid) {
            std::cerr << "GID mismatch on entry \"" << bufbase << "\""
                      << " in " << db_name << std::endl;
            continue;
        }

        entries++;
    }

    sqlite3_close(db);

    // previously counted entries should be the
    // same as the number encountered here
    if (sa->entries != entries) {
        std::cerr << "Did not get " << sa->entries << " matching entries." << std::endl;
        rc = 1;
    }

    delete sa;

    if (rc != 0) {
        csa->correct = false;
    }

    return rc;
}

// find first occurance of the delimiter
static std::string::size_type parsefirst(const char delim, struct StanzaStart *work) {
    return (work->first_delim = work->line.find(delim));
}

static int scout_function(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    struct CheckStanzaArgs *csa = static_cast <struct CheckStanzaArgs *> (args);
    csa->correct = false;

    char *filename = static_cast <char *> (data);

    std::ifstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Could not open file " << filename << std::endl;
        return 1;
    }

    // open multiple copies of the trace file
    // in preparation for check_stanza threads
    for(std::size_t i = 0; i < csa->threads; i++) {
        csa->traces.push_back(new std::ifstream(filename, std::ios::binary));
        if (!*(csa->traces[i])) {
            std::cerr << "Failed to open copies of " << filename << std::endl;
            return 1;
        }
    }

    struct StanzaStart *curr = new struct StanzaStart();
    while (!curr->line.size()) {
        if (!std::getline(file, curr->line)) {
            delete curr;
            return 1;
        }
    }

    if (parsefirst(csa->delim, curr) == std::string::npos) {
        std::cerr << "Could not find delimiter of first line" << std::endl;
        delete curr;
        return 1;
    }

    if (curr->line[curr->first_delim + 1] != 'd') {
        std::cerr << "First line of trace is not a directory" << std::endl;
        delete curr;
        return 1;
    }

    csa->correct = true;
    curr->offset = file.tellg();

    std::size_t file_count = 0;
    std::size_t dir_count = 1; // always start with a directory
    std::size_t empty = 0;

    std::string line;
    while (csa->correct && std::getline(file, line)) {
        if (!line.size()) {
            continue;
        }

        struct StanzaStart *next = new struct StanzaStart();
        next->line = std::move(line);

        if (parsefirst(csa->delim, next) == std::string::npos) {
            std::cerr << "Could not find delimiter of next line" << std::endl;
            delete curr;
            delete next;
            csa->correct = false;
            break;
        }

        // push directories onto queues
        if (next->line[next->first_delim + 1] == 'd') {
            dir_count++;

            empty += !curr->entries;
            next->offset = file.tellg();

            QPTPool_enqueue(ctx, id, check_stanza, curr);

            curr = next;
        }
        else {
            curr->entries++;
            file_count++;
            delete next;
        }
    }

    // insert the last work item
    if (csa->correct) {
        QPTPool_enqueue(ctx, id, check_stanza, curr);
    }

    std::cerr << "Dirs:  " << dir_count << " (" << empty << " empty)" << std::endl
              << "Files: " << file_count << std::endl
              << "Total: " << file_count + dir_count << std::endl;

    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        std::cerr << "Syntax: " << argv[0] << " trace delim GUFI_tree [threads]" << std::endl;
        return 0;
    }

    char *trace = argv[1];
    const char delim = argv[2][0];
    const std::string GUFI_tree = argv[3];

    std::size_t threads = 1;
    if (argc > 4) {
        if (!(std::stringstream(argv[4]) >> threads)) {
            std::cerr << "Error: Bad thread count: " << argv[4] << std::endl;
            return 1;
        }
    }

    std::atomic_bool correct(false);
    struct CheckStanzaArgs csa(threads, correct, delim, GUFI_tree);

    QPTPool_t *ctx = QPTPool_init(threads, &csa, nullptr, nullptr, 0
                                  #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                  , nullptr
                                  #endif
        );

    // start scouting function to push work onto queues
    QPTPool_enqueue(ctx, 0, scout_function, trace);

    QPTPool_wait(ctx);
    QPTPool_destroy(ctx);

    std::cout << argv[1] << " " << (correct?"Pass":"Fail")  << std::endl;

    return 0;
}
