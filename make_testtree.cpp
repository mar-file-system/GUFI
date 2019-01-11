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
#include <chrono>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <pthread.h>
#include <random>
#include <sstream>
#include <thread>
#include <vector>
#include <unistd.h>

extern "C" {

#include <sqlite3.h>
#include "C-Thread-Pool/thpool.h"

#include "bf.h"
#include "dbutils.h"

}

// File size min, max, and weight to choose from
struct Bucket {
    Bucket(const std::size_t min, const std::size_t max, const double weight)
        : min(min),
          max(max),
          weight(weight)
    {}

    const std::size_t min;
    const std::size_t max;
    const double weight;
};

// setting set by the user
struct Settings {
    Settings()
        : users(200),
          files(750000000),
          uid(0),
          gid(0),
          path_separator('/'),
          leading_zeros(0),
          db_name("db.db"),
          files_with_dirs(true),
          seed(std::chrono::high_resolution_clock::now().time_since_epoch().count()),
          depth(),
          subdirs(),
          time(),
          threads(std::max(std::thread::hardware_concurrency() / 2, (decltype(std::thread::hardware_concurrency())) 1)),
          blocksize(512),
          top(),
          file_count(),
          file_size(),
          chown(false),
          chgrp(false),
          dryrun(false)
    {
        depth.min = 1;
        depth.max = 5;

        subdirs.min = 1;
        subdirs.max = 5;

        time.min = 0;
        time.max = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    }

    bool valid(std::ostream &stream = std::cerr) const {
        bool ret = true;
        if (!users) {
            stream << "Need at least 1 user" << std::endl;
            ret = false;
        }

        if (!files) {
            stream << "Need at least 1 file" << std::endl;
            ret = false;
        }

        if (uid >= ((uid_t) -1) - users) {
            stream << "Starting uid too low" << std::endl;
            ret = false;
        }

        if (gid >= ((gid_t) -1) - users) {
            stream << "Starting gid too low" << std::endl;
            ret = false;
        }

        if (!depth.min) {
            stream << "Minimum depth should be at least 1" << std::endl;
            ret = false;
        }

        if (depth.min > depth.max) {
            stream << "Minimum depth should be less than or equal to maximum depth" << std::endl;
            ret = false;
        }

        if (time.min > time.max) {
            stream << "Minimum time should be less than or equal to maximum time" << std::endl;
            ret = false;
        }

        if (!threads) {
            stream << "There should be at least 1 thread in the thread pool" << std::endl;
            ret = false;
        }

        if (!blocksize) {
            stream << "File blocksize should be greater than 0" << std::endl;
            ret = false;
        }

        if (!path_separator) {
            stream << "Path separator character should not be '\\0'" << std::endl;
            ret = false;
        }

        if (!db_name.size()) {
            stream << "Bad database file name: \"" << db_name << "\"" << std::endl;
            ret = false;
        }

        if (!top.size()) {
            stream << "Bad top level path: \"" << top << "\"" << std::endl;
            ret = false;
        }

        if ((chown || chgrp) && (geteuid() != 0)) {
            stream << "Need to be root to change owner/group" << std::endl;
            ret = false;
        }

        if (!file_count.size()) {
            stream << "Need at least 1 file count bucket" << std::endl;
            ret = false;
        }

        if (!file_size.size()) {
            stream << "Need at least 1 file size bucket" << std::endl;
            ret = false;
        }

        return ret;
    }

    std::ostream &print(std::ostream &stream) const {
        stream << "Tree properties:"
               << "\n    Path:            " << top
               << "\n    Files:           " << files
               << "\n        Blocksize:   " << blocksize
               << "\n        Timestamps:  [" << time.min << ", " << time.max << "]"
               << "\n    Users:           " << users
               << "\n    Depth:           [" << depth.min << ", " << depth.max << "]"
               << "\n    Subdirectories:  [" << subdirs.min << ", " << subdirs.max << "]"
               << "\n    Set UID if able: " << std::boolalpha << chown
               << "\n    Set GID if able: " << std::boolalpha << chgrp;

        stream << "\n    File Count Buckets:";
        for(Bucket const & count: file_count) {
            stream << "\n        " << count.min << ", " << count.max << ", " << count.weight;
        }

        stream << "\n    File Size Buckets:";
        for(Bucket const & size: file_size) {
            stream << "\n        " << size.min << ", " << size.max << ", " << size.weight;
        }

        stream << "\n"
               << "\nOutput properties:"
               << "\n    Path Separator:         " << path_separator
               << "\n    Leading Zeros:          " << leading_zeros
               << "\n    Database name:          " << db_name
               << "\n    Files with directories: " << std::boolalpha << files_with_dirs
               << "\n"
               << "\nRNG Seed: " << seed
               << "\nThreads:  " << threads
               << "\nDry Run:  " << std::boolalpha << dryrun;

        return stream;
    }

    template <typename T = std::size_t>
    struct Range {
        T min;
        T max;
    };

    std::size_t users;                   // count
    std::size_t files;                   // count
    uid_t uid;                           // starting uid
    gid_t gid;                           // starting gid

    char        path_separator;          // '/'
    std::size_t leading_zeros;           // for filling file/directory names
    std::string db_name;                 // db.db
    bool        files_with_dirs;         // whether or not to generate files next to directories

    // RNG settings
    std::size_t seed;                    // used to seed a seed rng
    Range <>    depth;
    Range <>    subdirs;
    Range <>    time;

    std::size_t threads;                 // count

    std::size_t blocksize;               // > 0

    std::string top;                     // top depth directory
    std::vector <Bucket> file_count;     // set of file distribution buckets
    std::vector <Bucket> file_size;      // set of file size buckets

    bool chown;
    bool chgrp;

    bool dryrun;
};

std::ostream &operator<<(std::ostream &stream, const Settings &settings) {
    return settings.print(stream);
}

// per thread configuration
struct ThreadArgs {
    ThreadArgs()
    {}

    std::string path;                    // path of current directory being worked on
    std::size_t files;                   // how many files should be created under this directory (including subdirectories)
    std::size_t target_depth;            // final depth of directories
    std::size_t curr_depth;              // the current depth
    uid_t  uid;                          // uid to use
    gid_t  gid;                          // gid to use
    std::size_t seed;                    // seed for seeding the RNGs inside this thread

    static Settings *settings;
    static threadpool *pool;
    static std::atomic_int directories;
};

Settings   *ThreadArgs::settings        = nullptr;
threadpool *ThreadArgs::pool            = nullptr;
std::atomic_int ThreadArgs::directories = {};

// Distribute total into slots (which should
// have been resized to the desired number of slots
// before being passed into this function).
//
// https://stackoverflow.com/a/22381248
template <typename Generator, typename BucketDistribution>
void distribute_total_randomly(const std::size_t total,
                               std::vector <std::size_t> &slots,
                               const bool index_0,
                               const std::size_t seed,
                               const std::vector <Bucket> &counts) {
    if (!slots.size()) {
        throw std::runtime_error("Input vector requires at least 1 slot");
    }

    // use this as the generator for this function
    Generator gen(seed);

    // file count weights
    std::vector <std::size_t> weights;

    // RNGs for each count bucket
    std::vector <BucketDistribution> count_rngs;

    for(Bucket const & count : counts) {
        weights.push_back(count.weight);
        count_rngs.emplace_back(BucketDistribution(count.min, count.max));
    }

    // use this to select the bucket index
    std::discrete_distribution <std::size_t> bucket_rng(weights.begin(), weights.end());

    // assign each user a random number of files, ignoring the total
    std::size_t sum = 0;
    for(std::size_t &count : slots) {
        count = count_rngs[bucket_rng(gen)](gen);
        sum += count;
    }

    // if files are not supposed to be in the first slot, set it to 0
    if (!index_0) {
        sum -= slots[0];
        slots[0] = 0;
    }

    // scale the number of files so that the sum is close to the target number of files
    const double factor = std::max(((double) total - index_0 - slots.size()), 0.0) / sum;
    std::size_t missing = total;
    for(std::size_t &count : slots) {
        count = ((double) count) * factor;// + 1;
        missing -= count;
    }

    // place missing counts in random locations
    std::uniform_int_distribution <std::size_t> fill_rng(!index_0, slots.size() - 1);
    for(std::size_t i = 0; i < missing; i++) {
        slots[fill_rng(gen)]++;
    }
}

bool create_tables(sqlite3 *db) {
    return ((sqlite3_exec(db, esql,       nullptr, nullptr, nullptr) == SQLITE_OK) &&
            (sqlite3_exec(db, ssql,       nullptr, nullptr, nullptr) == SQLITE_OK) &&
            (sqlite3_exec(db, vssqldir,   nullptr, nullptr, nullptr) == SQLITE_OK) &&
            (sqlite3_exec(db, vssqluser,  nullptr, nullptr, nullptr) == SQLITE_OK) &&
            (sqlite3_exec(db, vssqlgroup, nullptr, nullptr, nullptr) == SQLITE_OK) &&
            (sqlite3_exec(db, vesql,      nullptr, nullptr, nullptr) == SQLITE_OK));
}

sqlite3 *open_and_create_tables(const std::string &name){
    sqlite3 *db = nullptr;
    if (sqlite3_open_v2(name.c_str(), &db, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, nullptr) != SQLITE_OK) {
        return nullptr;
    }

    // try to turn sychronization off
    if (sqlite3_exec(db, "PRAGMA synchronous = OFF", nullptr, nullptr, nullptr) != SQLITE_OK) {
        std::cerr << "Could not turn synchronziation off " << sqlite3_errmsg(db) << std::endl;
    }

    // try to turn journaling off
    if (sqlite3_exec(db, "PRAGMA journal_mode = OFF", nullptr, nullptr, nullptr) != SQLITE_OK) {
        std::cerr << "Could not turn journal_mode off " << sqlite3_errmsg(db) << std::endl;
    }

    // // try increasing the page size
    // if (sqlite3_exec(db, "PRAGMA page_size = 16777216", nullptr, nullptr, nullptr) != SQLITE_OK) {
    //     std::cerr << "Could not change page size" << sqlite3_errmsg(db) << std::endl;
    // }

    // create tables
    if (!create_tables(db)) {
        std::cerr << "Could not create tables: " << sqlite3_errmsg(db) << std::endl;
        sqlite3_close(db);
        return nullptr;
    }

    return db;
}

// This function creates somewhat random file records for the current directory
template <typename Generator>
void generatecurr(ThreadArgs *arg, const std::size_t files) {
    const std::string fullname = arg->path + arg->settings->path_separator + arg->settings->db_name;

    sqlite3 *on_disk = open_and_create_tables(fullname.c_str());
    if (!on_disk) {
        std::cerr << "Could not open " << fullname << ": " << sqlite3_errmsg(on_disk) << std::endl;
        return;
    }

    // rng used to generate seeds for other rngs
    Generator gen(arg->seed);

    // generic rng
    std::uniform_int_distribution <std::size_t> rng(0, ((std::size_t) -1) >> 1);

    // rng for timestamps
    std::uniform_int_distribution <std::size_t> time_rng(arg->settings->time.min, arg->settings->time.max);

    // rng for selecting size bucket
    std::vector <double> weights;
    for(Bucket const &file_size : arg->settings->file_size) {
        weights.push_back(file_size.weight);
    }
    std::discrete_distribution <std::size_t> bucket_rng(weights.begin(), weights.end());

    // rng for selecting size within a bucket
    std::vector <std::uniform_int_distribution <std::size_t> > size_rngs(arg->settings->file_size.size());
    for(std::size_t i = 0; i < arg->settings->file_size.size(); i++) {
        size_rngs[i] = std::uniform_int_distribution <std::size_t> (arg->settings->file_size[i].min, arg->settings->file_size[i].max);
    }

    struct sum summary;
    zeroit(&summary);

    // prepare to insert entries
    sqlite3_stmt *res = nullptr;
    static const std::size_t esqli_len = strlen(esqli);
    if (sqlite3_prepare_v2(on_disk, esqli, esqli_len, &res, nullptr) != SQLITE_OK) {
        std::cerr << "Could not prepare statement for " << fullname << " (" << sqlite3_errmsg(on_disk) << "). Skipping" << std::endl;
        sqlite3_close(on_disk);
        return;
    }

    // https://stackoverflow.com/q/1711631
    sqlite3_exec(on_disk, "BEGIN TRANSACTION", nullptr, nullptr, nullptr);

    // create file entries and insert them into the database
    for(std::size_t i = 0; i < files; i++) {
        std::stringstream s;
        s << arg->path << arg->settings->path_separator << "file" << std::setw(arg->settings->leading_zeros) << std::setfill('0') << i;

        const std::size_t bucket = bucket_rng(gen);

        struct work work;

        snprintf(work.name, MAXPATH, "%s", s.str().c_str());
        snprintf(work.type, 2, "f");
        snprintf(work.linkname, MAXPATH, "");
        snprintf(work.xattr, MAXPATH, "xattr %zu", i);
        snprintf(work.osstext1, MAXXATTR, "osstext1 %zu", i);
        snprintf(work.osstext2, MAXXATTR, "osstext2 %zu", i);

        work.statuso.st_ino = rng(gen);
        work.statuso.st_mode = 0777;
        work.statuso.st_nlink = 1;
        work.statuso.st_uid = arg->uid;
        work.statuso.st_gid = arg->gid;
        work.statuso.st_size = size_rngs[bucket](gen);
        work.statuso.st_blksize = arg->settings->blocksize;
        work.statuso.st_blocks = work.statuso.st_size / work.statuso.st_blksize;
        work.statuso.st_ctime = time_rng(gen);
        work.statuso.st_atime = std::max(work.statuso.st_ctime, (time_t) time_rng(gen));
        work.statuso.st_mtime = std::max(work.statuso.st_ctime, (time_t) time_rng(gen));

        work.pinode  = rng(gen);
        work.xattrs  = rng(gen);
        work.crtime  = rng(gen);
        work.ossint1 = rng(gen);
        work.ossint2 = rng(gen);
        work.ossint3 = rng(gen);
        work.ossint4 = rng(gen);

        sumit(&summary, &work);
        insertdbgo(&work, on_disk, res);
    }

    sqlite3_exec(on_disk, "END TRANSACTION", nullptr, nullptr, nullptr);

    insertdbfin(on_disk, res);

    // summarize this directory
    std::size_t bucket = bucket_rng(gen);

    struct work work;

    snprintf(work.name, MAXPATH, "%s", arg->path.c_str());
    snprintf(work.type, 2, "d");
    snprintf(work.linkname, MAXPATH, "");
    snprintf(work.xattr, MAXPATH, "xattr");
    snprintf(work.osstext1, MAXXATTR, "osstext1");
    snprintf(work.osstext2, MAXXATTR, "osstext2");

    work.statuso.st_ino = rng(gen);
    work.statuso.st_mode = 0775;
    work.statuso.st_nlink = 1;
    work.statuso.st_uid = arg->uid;
    work.statuso.st_gid = arg->gid;
    work.statuso.st_size = size_rngs[bucket](gen);
    work.statuso.st_blksize = rng(gen);
    work.statuso.st_blocks = rng(gen);
    work.statuso.st_ctime = time_rng(gen);
    work.statuso.st_atime = work.statuso.st_ctime + time_rng(gen);
    work.statuso.st_mtime = work.statuso.st_ctime + time_rng(gen);

    work.crtime  = rng(gen);
    work.ossint1 = rng(gen);
    work.ossint2 = rng(gen);
    work.ossint3 = rng(gen);
    work.ossint4 = rng(gen);

    insertsumdb(on_disk, &work, &summary);

    sqlite3_close(on_disk);

    // don't check for error
    chown(fullname.c_str(), arg->settings->chown?arg->uid:-1, arg->settings->chgrp?arg->gid:-1);
}

template <typename DirGenerator, typename DBGenerator = DirGenerator>
void generatedir(void *args) {
    ThreadArgs *arg = (ThreadArgs *) args;
    if (!arg || !arg->files) {
        return;
    }

    // rng used to generate seeds for other rngs
    DirGenerator gen(arg->seed);

    // subdirectory count generator
    std::uniform_int_distribution <std::size_t> subdir_rng(arg->settings->subdirs.min, arg->settings->subdirs.max);

    // distribute the files across a random number of subdirectories in the current directory
    std::vector <std::size_t> file_distribution(subdir_rng(gen) + 1);
    if (arg->curr_depth < arg->target_depth) {
        distribute_total_randomly <std::mt19937, std::uniform_int_distribution <std::size_t> > (arg->files, file_distribution, arg->settings->files_with_dirs, gen(), arg->settings->file_count);
    }
    // this is the final directory, so all files are here
    else {
        file_distribution.resize(1);
        file_distribution[0] = arg->files;
    }

    // change the seed for generatecurr
    // have to do this even during a dry run
    arg->seed = gen();

    if (!arg->settings->dryrun) {
        // this function is responsible for creating the directory it is working on
        if (mkdir(arg->path.c_str(), 0777) != 0) {
            const int err = errno;
            std::cerr << "mkdir error: " << arg->path << ": " << strerror(err) << std::endl;
            if (err != EEXIST) {
                return;
            }
        }

        // generate the files in the current directory
        generatecurr <DBGenerator> (arg, file_distribution[0]);

        // don't check for error
        chown(arg->path.c_str(), arg->settings->chown?arg->uid:-1, arg->settings->chgrp?arg->gid:-1);
    }

    arg->directories++;

    for(std::size_t i = 1; i < file_distribution.size(); i++) {
        std::stringstream s;
        s << arg->path << "/dir" << std::setw(arg->settings->leading_zeros) << std::setfill('0') << i - 1;

        // push into work queue
        ThreadArgs *sub_args = new ThreadArgs();
        sub_args->path = s.str();
        sub_args->files = file_distribution[i];
        sub_args->target_depth = arg->target_depth;
        sub_args->curr_depth = arg->curr_depth + 1;
        sub_args->uid = arg->uid;
        sub_args->gid = arg->uid;
        sub_args->seed = gen();

        thpool_add_work(*arg->pool, generatedir <DirGenerator, DBGenerator>, sub_args);
    }

    delete arg;
}

std::ostream &print_help(std::ostream &stream, char *argv0) {
    static const Settings s;
    return stream << "Syntax: " << argv0 << " [options] directory --file-count [--file-count ...] --file-size [--file-size ...]\n"
                  << "\n"
                  << "    Options:\n"
                  << "        --users n               Number of users                                        (Default: "  << s.users << ")\n"
                  << "        --files n               Number of files in the tree                            (Default: "  << s.files << ")\n"
                  << "        --uid uid               Set the starting uid                                   (Default: "  << s.uid << ")\n"
                  << "        --gid gid               Set the starting gid                                   (Default: "  << s.gid << ")\n"
                  << "        --path-separator c      Character that separates path sections                 (Default: '" << s.path_separator << "') \n"
                  << "        --leading-zeros n       Number of leading zeros in names                       (Default: "  << s.leading_zeros <<")\n"
                  << "        --db-name name          Name of each database file                             (Default: "  << s.db_name << ")\n"
                  << "        --files-with-dirs b     Whether or not files exist in intermediate directories (Default: "  << std::boolalpha << s.files_with_dirs << ")\n"
                  << "        --threads n             Set the number of threads to use                       (Default: "  << s.threads << ")\n"
                  << "        --blocksize n           Block size for filesystem I/O                          (Default: "  << s.blocksize << ")\n"
                  << "        --chown uid             Set the owner of each directory (requires sudo)        (Default: "  << s.chown << ")\n"
                  << "        --chgrp gid             Set the group of each directory (requires sudo)        (Default: "  << s.chgrp << ")\n"
                  << "        --file-count            Comma separated triple of min,max,weight. At least one should be provided.\n"
                  << "        --file-size             Comma separated triple of min,max,weight. At least one should be provided.\n"
                  << "        --dry-run               Runs without creating the GUFI tree                    (Default: " << s.dryrun << ")\n"
                  << "\n"
                  << "    RNG configuration:\n"
                  << "        --seed n                Value to seed the seed generator                       (Default: Seconds since epoch)\n"
                  << "        --min-depth n           Minimum layers of directories (at least 1)             (Default: "  << s.depth.min << ")\n"
                  << "        --max-depth n           Maximum layers of directories                          (Default: "  << s.depth.max << ")\n"
                  << "        --min-time n            Minimum timestamp                                      (Default: "  << s.time.min << ")\n"
                  << "        --max-time n            Maximum timestamp                                      (Default: "  << s.time.max << ")\n"
                  << "        --min-subdirs n         Minimum subdirectories                                 (Default: "  << s.subdirs.min << ")\n"
                  << "        --max-subdirs n         Maximum subdirectories                                 (Default: "  << s.subdirs.max << ")\n"
                  << "\n"
                  << "    directory                   The directory to put the generated tree into\n";
}

template <typename T>
bool read_value(int argc, char *argv[], int &i, T &setting, const std::string &name) {
    if (i >= argc) {
        return false;
    }

    if (!(std::stringstream(argv[i]) >> setting)) {
        std::cerr << "Bad " << name << ": " <<  argv[i] << std::endl;
        return false;
    }

    i++;

    return true;
}

#define PARSE_VALUE(opt, var, err)                           \
    else if (args == "--" opt) {                             \
        if (!read_value(argc, argv, i, settings.var, err)) { \
            return false;                                    \
        }                                                    \
    }

#define PARSE_FLAG(opt, var)                                 \
    else if (args == "--" opt) {                             \
        settings.var = true;                                 \
    }

bool parse_args(int argc, char *argv[], Settings &settings, bool &help) {
    bool positional = false;
    int i = 1;
    while (i < argc) {
        const std::string args(argv[i++]);
        if ((args == "--help") || (args == "-h")) {
            return help = true;
        }
        PARSE_VALUE("users",           users,             "user count")
        PARSE_VALUE("files",           files,             "file count")
        PARSE_VALUE("uid",             uid,               "uid")
        PARSE_VALUE("gid",             gid,               "gid")
        PARSE_VALUE("path-separator",  path_separator,    "path separator")
        PARSE_VALUE("leading-zeros",   leading_zeros,     "leading zero count")
        PARSE_VALUE("db-name",         db_name,           "db name")
        else if (args == "--files-with-dirs") {
            if (!(std::stringstream(argv[i]) >> std::boolalpha >> settings.files_with_dirs)) {
                std::cerr << "Bad boolean: " << argv[i] << std::endl;
            }

            i++;
        }
        PARSE_FLAG("chown", chown)
        PARSE_FLAG("chgrp", chgrp)
        else if (args == "--file-count") {
            std::size_t min;
            char comma1;
            std::size_t max;
            char comma2;
            double weight;

            if (!(std::stringstream(argv[i++]) >> min >> comma1 >> max >> comma2 >> weight) ||
                (comma1 != ',') || (comma2 != ',') ||
                (min > max) || !max) {
                std::cerr << "Bad file count bucket: " << args << std::endl;
                return false;
            }

            settings.file_count.emplace_back(Bucket(min, max, weight));
        }
        else if (args == "--file-size") {
            std::size_t min;
            char comma1;
            std::size_t max;
            char comma2;
            double weight;

            if (!(std::stringstream(argv[i++]) >> min >> comma1 >> max >> comma2 >> weight) ||
                (comma1 != ',') || (comma2 != ',') ||
                (min > max) || !max) {
                std::cerr << "Bad file size bucket: " << args << std::endl;
                return false;
            }

            settings.file_size.emplace_back(Bucket(min, max, weight));
        }
        PARSE_VALUE("seed",            seed,              "seed")
        PARSE_VALUE("min-depth",       depth.min,         "min depth")
        PARSE_VALUE("max-depth",       depth.max,         "max depth")
        PARSE_VALUE("min-subdirs",     subdirs.min,       "min subdirectories")
        PARSE_VALUE("max-subdirs",     subdirs.max,       "max subdirectories")
        PARSE_VALUE("min-time",        time.min,          "min atime")
        PARSE_VALUE("max-time",        time.max,          "max atime")
        PARSE_VALUE("threads",         threads,           "thread count")
        PARSE_VALUE("blocksize",       blocksize,         "blocksize")
        PARSE_FLAG("dry-run", dryrun)
        else if ((args[0] == '-') && (args[1] == '-')) {
            std::cerr << "Unknown option: " << args << std::endl;
            return false;
        }
        else {
            // first positional argument is the directory
            if (!settings.top.size()) {
                settings.top = std::move(args);
            }
        }
    }

    return (i == argc);
}

bool create_path(std::string path, const char sep) {
    // append a separator if necessary
    if (path.back() != sep) {
        path += sep;
    }

    // assume top level exists
    bool exists = true;
    for(std::size_t i = 0; i < path.size(); i++) {
        if (path[i] == sep) {
            const std::string subpath = path.substr(0, i + 1);

            // check if the path is a directory
            struct stat st;
            if (exists && stat(subpath.data(), &st) == 0) {
                if (!S_ISDIR(st.st_mode)) {
                    std::cerr << subpath << " is not a directory. Skipping." << std::endl;
                    return false;
                }
                else {
                    exists = true;
                }
            }
            else if (!exists || (errno == ENOENT)) {
                // if it doesn't, create the directory
                exists = false;
                if (mkdir(subpath.data(), 0777) != 0) {
                    std::cerr << "Could not create " << path << ". Skipping." << std::endl;
                    return false;
                }
            }
        }
    }

    return true;
}

// create the top level separately
bool create_top(const Settings &settings) {
    // create the top level directory
    if (!create_path(settings.top, settings.path_separator)) {
        std::cerr << "Could not create directory " << settings.top << std::endl;
        return false;
    }

    // create the top level database file
    const std::string fullname = settings.top + settings.path_separator + settings.db_name;
    sqlite3 *top = open_and_create_tables(fullname);
    if (!top) {
        std::cerr << "Could not open " << fullname << std::endl;
        return false;
    }

    // do not add files to this layer
    sqlite3_close(top);

    return true;
}

int main(int argc, char *argv[]) {
    Settings settings;

    // init
    {
        bool help = false;
        if (!parse_args(argc, argv, settings, help)) {
            print_help(std::cout, argv[0]);
            return 1;
        }

        if (help) {
            print_help(std::cout, argv[0]);
            return 0;
        }

        if (!settings.valid(std::cout)) {
            print_help(std::cout, argv[0]);
            return 1;
        }

        std::cout << settings << std::endl;
    }

    if (!settings.dryrun) {
        // create the top level separately
        if (!create_top(settings)) {
            return 1;
        }
    }

    // start up thread pool
    threadpool pool = thpool_init(settings.threads);
    if (thpool_null(pool)) {
        std::cerr << "Failed to create thread pool" << std::endl;
        return 1;
    }

    // save "globals"
    ThreadArgs::settings = &settings;
    ThreadArgs::pool = &pool;
    ThreadArgs::directories = 1; // top level directory

    // use this rng to generate seeds for other rngs
    std::mt19937 gen(settings.seed);

    // generate file distribution by user
    std::vector <std::size_t> file_counts(settings.users);
    distribute_total_randomly <std::mt19937, std::uniform_int_distribution <std::size_t> > (settings.files, file_counts, true, gen(), settings.file_count);

    // rng for depth selection
    std::uniform_int_distribution <std::size_t> depth_rng(settings.depth.min, settings.depth.max);

    // push each subdirectory generation into the thread pool
    for(std::size_t offset = 0; offset < settings.users; offset++) {
        std::stringstream s;
        s << settings.top << "/user" << std::setw(settings.leading_zeros) << std::setfill('0') << settings.uid + offset;

        // push into work queue
        ThreadArgs *args = new ThreadArgs();
        args->path = s.str();
        args->files = file_counts[offset];
        args->target_depth = depth_rng(gen);
        args->curr_depth = 1;
        args->uid = settings.uid + offset;
        args->gid = settings.gid + offset;
        args->seed = gen();

        thpool_add_work(pool, generatedir <std::knuth_b, std::mt19937>, args);
    }

    thpool_wait(pool);
    thpool_destroy(pool);

    if (settings.dryrun) {
        std::cout << "Would have generated ";
    }
    else {
        std::cout << "Generated ";
    }

    std::cout << ThreadArgs::directories << " directories" << std::endl;

    return 0;
}
