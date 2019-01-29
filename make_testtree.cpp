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

#include <condition_variable>
#include <chrono>
#include <cmath>
#include <cstring>
#include <ctime>
#include <functional>
#include <iomanip>
#include <iostream>
#include <list>
#include <mutex>
#include <pthread.h>
#include <random>
#include <sstream>
#include <sys/stat.h>
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

    bool in(const std::size_t val) const {
        return ((min <= val) && (val <= max));
    }

    const std::size_t min;
    const std::size_t max;
    const double weight;
};

std::ostream &operator<<(std::ostream &stream, const Bucket &bucket) {
    return stream << "(" << bucket.min << ", " << bucket.max << ", " << bucket.weight << ")";
}

// setting set by the user
struct Settings {
    Settings()
        : users(1000),
          files(750000000),
          uid(0),
          gid(0),
          path_separator('/'),
          leading_zeros(0),
          db_name("db.db"),
          files_with_dirs(true),
          seed(std::chrono::high_resolution_clock::now().time_since_epoch().count()),
          subdirs(),
          time(),
          threads(std::max(std::thread::hardware_concurrency() / 2, (decltype(std::thread::hardware_concurrency())) 1)),
          blocksize(512),
          top(),
          file_count(),
          file_size(),
          chown(false),
          chgrp(false),
          dryrun(false),
          stat_rate(1000),
          progress_rate(1000)
    {
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
               << "\n    Subdirectories:  [" << subdirs.min << ", " << subdirs.max << "]"
               << "\n    Set UID if able: " << std::boolalpha << chown
               << "\n    Set GID if able: " << std::boolalpha << chgrp;

        stream << "\n    File Count Buckets:";
        for(Bucket const & count: file_count) {
            stream << "\n        " << count;
        }

        stream << "\n    File Size Buckets:";
        for(Bucket const & size: file_size) {
            stream << "\n        " << size;
        }

        stream << "\n"
               << "\nOutput properties:"
               << "\n    Path Separator:         " << path_separator
               << "\n    Leading Zeros:          " << leading_zeros
               << "\n    Database name:          " << db_name
               << "\n    Files with directories: " << std::boolalpha << files_with_dirs
               << "\n"
               // << "\nRNG Seed: " << seed
               << "\nThreads:              " << threads
               << "\nDry Run:              " << std::boolalpha << dryrun
               << "\nStat Collection Rate: " << stat_rate << " ms"
               << "\nProgress Print Rate:  " << progress_rate << " ms";

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
    Range <>    subdirs;
    Range <>    time;

    std::size_t threads;                 // count

    std::size_t blocksize;               // > 0

    std::string top;                     // top level directory
    std::vector <Bucket> file_count;     // set of file distribution buckets
    std::vector <Bucket> file_size;      // set of file size buckets

    bool chown;
    bool chgrp;

    bool dryrun;

    unsigned int stat_rate;              // interval between collecting statistics, in milliseconds
    unsigned int progress_rate;          // interval to print progress, in milliseconds
};

std::ostream &operator<<(std::ostream &stream, const Settings &settings) {
    return settings.print(stream);
}

// per thread configuration
struct ThreadArgs {
    ThreadArgs() {}

    std::string path;                    // path of current directory being worked on
    std::size_t depth;                   // how deep this directory is
    uid_t uid;                           // uid to use
    gid_t gid;                           // gid to use
    std::size_t seed;                    // seed for seeding the RNGs inside this thread

    static Settings *settings;
    static threadpool *pool;

    static std::mutex mutex;
    static std::size_t files;            // runtime sum of file counts (so summing stats is not necessary)
    static off_t size;                   // runtime sum of file sizes (so summing stats is not necessary)
    struct Stats {
        Stats(const std::string &name, const std::size_t files, const std::size_t depth, const bool leaf, const std::list <off_t> &sizes)
            : name(name),
              files(files),
              depth(depth),
              leaf(leaf),
              sizes(sizes)
        {}

        const std::string name;
        const std::size_t files;
        const std::size_t depth;
        const bool leaf;
        const std::list <off_t> sizes;
    };
    static std::list <Stats> stats;

    // stats that are collected and wiped on a timer
    struct Timed {
        void clear() {
            files = 0;
            directories = 0;
            mkdir = 0;
        }

        std::mutex mutex;
        std::size_t files;
        std::size_t directories;
        double mkdir;
    };

    static Timed timed_stats;
};

std::ostream &operator<<(std::ostream &stream, const ThreadArgs::Timed &stats) {
    return stream << "Created " << stats.directories << " directories, averaging " << stats.mkdir / stats.directories << " seconds per directory, and " << stats.files << " files";
}

Settings *ThreadArgs::settings = nullptr;
threadpool *ThreadArgs::pool = nullptr;
std::mutex ThreadArgs::mutex = {};
std::size_t ThreadArgs::files = 0;
off_t ThreadArgs::size = 0;
std::list <ThreadArgs::Stats> ThreadArgs::stats = {};
ThreadArgs::Timed ThreadArgs::timed_stats = {};

long double elapsed(const struct timespec &start, const struct timespec &end) {
    return ((long double) (end.tv_sec - start.tv_sec) +
            ((long double) (end.tv_nsec - start.tv_nsec) / 1000000000.0));
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
void generatecurr(ThreadArgs *arg, const std::size_t files, std::list <off_t> &sizes) {
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
    std::vector <std::normal_distribution <double> > size_rngs(arg->settings->file_size.size());
    for(std::size_t i = 0; i < arg->settings->file_size.size(); i++) {
        const double mean = (arg->settings->file_size[i].max - arg->settings->file_size[i].min) / 2.0;
        const double stddev = (mean - arg->settings->file_size[i].min) / 3.0;
        size_rngs[i] = std::normal_distribution <double> (mean, stddev);
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
        work.linkname[0] = '\0';
        snprintf(work.xattr, MAXPATH, "xattr %zu", i);
        snprintf(work.osstext1, MAXXATTR, "osstext1 %zu", i);
        snprintf(work.osstext2, MAXXATTR, "osstext2 %zu", i);

        work.statuso.st_ino = rng(gen);
        work.statuso.st_mode = 0777;
        work.statuso.st_nlink = 1;
        work.statuso.st_uid = arg->uid;
        work.statuso.st_gid = arg->gid;
        work.statuso.st_size = static_cast <off_t> (size_rngs[bucket](gen));
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

        {
            std::lock_guard <std::mutex> lock(ThreadArgs::mutex);
            arg->size += work.statuso.st_size;
            sizes.push_back(work.statuso.st_size);
        }

        // collect timed stats
        {
            std::lock_guard <std::mutex> lock(ThreadArgs::timed_stats.mutex);
            ThreadArgs::timed_stats.files++;
        }
    }

    sqlite3_exec(on_disk, "END TRANSACTION", nullptr, nullptr, nullptr);

    insertdbfin(on_disk, res);

    // summarize this directory
    std::size_t bucket = bucket_rng(gen);

    struct work work;

    snprintf(work.name, MAXPATH, "%s", arg->path.c_str());
    snprintf(work.type, 2, "d");
    work.linkname[0] = '\0';
    snprintf(work.xattr, MAXPATH, "xattr");
    snprintf(work.osstext1, MAXXATTR, "osstext1");
    snprintf(work.osstext2, MAXXATTR, "osstext2");

    work.statuso.st_ino = rng(gen);
    work.statuso.st_mode = 0775;
    work.statuso.st_nlink = 1;
    work.statuso.st_uid = arg->uid;
    work.statuso.st_gid = arg->gid;
    work.statuso.st_size = static_cast <std::size_t> (size_rngs[bucket](gen));
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

// select a bucket with given weights, and then select a value
// from within the selected bucket with the given distribution
//
// DistributionInBucket should generate reals, not integers
// The result is cast to a std::size_t
template <typename Generator, typename BucketDistribution>
std::size_t random_from_bucket(const std::size_t seed,
                               const std::vector <Bucket> &buckets) {
    if (!buckets.size()) {
        throw std::runtime_error("Input vector requires at least 1 bucket");
    }

    // use this as the generator for this function
    Generator gen(seed);

    std::vector <double> intervals(buckets.size() + 1);
    std::vector <double> weights(buckets.size());

    for(std::size_t i = 0; i < buckets.size(); i++) {
        intervals[1] = i;
        weights[i] = buckets[i].weight;
    }
    intervals.back() = buckets.size();

    std::piecewise_constant_distribution <double> bucket_rng(intervals.begin(), intervals.end(), weights.begin());

    // select bucket
    const std::size_t index = std::floor(bucket_rng(gen));
    const Bucket &bucket = buckets[index];
    const double mean = (bucket.max + bucket.min) / 2.0;
    const double stddev = (mean - bucket.min) / 3.0;

    // select a value within the bucket
    BucketDistribution dist(mean, stddev);
    return static_cast <std::size_t> (dist(gen));
}

template <typename DirGenerator, typename DBGenerator, typename BucketDistribution>
void generatedir(void *args) {
    ThreadArgs *arg = (ThreadArgs *) args;
    if (!arg) {
        return;
    }

    // make sure there are still files to generate
    {
        std::lock_guard <std::mutex> lock(arg->mutex);
        if (arg->files >= arg->settings->files) {
            delete arg;
            return;
        }
    }

    // rng used to generate seeds for other rngs
    DirGenerator gen(arg->seed);

    // generate the number of files that will be in this directory
    const std::size_t files = random_from_bucket <DBGenerator, BucketDistribution> (gen(), arg->settings->file_count);

    // change the seed for generatecurr
    arg->seed = gen();

    std::list <off_t> sizes;

    if (!arg->settings->dryrun) {
        struct timespec start;
        clock_gettime(CLOCK_MONOTONIC, &start);

        // this function is responsible for creating the directory it is working on
        const int ret = mkdir(arg->path.c_str(), 0777);

        struct timespec end;
        clock_gettime(CLOCK_MONOTONIC, &end);

        // collect timed stats
        {
            std::lock_guard <std::mutex> lock(ThreadArgs::timed_stats.mutex);
            ThreadArgs::timed_stats.directories++;
            ThreadArgs::timed_stats.mkdir += elapsed(start, end);
        }

        if (ret != 0) {
            const int err = errno;
            std::cerr << "mkdir error: " << arg->path << ": " << strerror(err) << std::endl;
            if (err != EEXIST) {
                delete arg;
                return;
            }
        }

        // generate the files in the current directory
        generatecurr <DBGenerator> (arg, files, sizes);

        // don't check for error
        chown(arg->path.c_str(), arg->settings->chown?arg->uid:-1, arg->settings->chgrp?arg->gid:-1);

        // only allow the owner access to their home directory
        if (arg->depth == 1) {
            // don't check for error
            chmod(arg->path.c_str(), S_IRWXU);
        }
    }

    // generate number of subdirectories
    std::uniform_int_distribution <std::size_t> subdir_rng(arg->settings->subdirs.min, arg->settings->subdirs.max);
    const std::size_t subdir_count = subdir_rng(gen);

    // collect stats
    {
        std::lock_guard <std::mutex> lock(arg->mutex);
        arg->files += files;
        arg->stats.emplace_back(ThreadArgs::Stats(arg->path, files, arg->depth, !subdir_count || (arg->files >= arg->settings->files), sizes));

        if (arg->files >= arg->settings->files) {
            delete arg;
            return;
        }
    }

    for(std::size_t i = 0; i < subdir_count; i++) {
        std::stringstream s;
        s << arg->path << "/dir" << std::setw(arg->settings->leading_zeros) << std::setfill('0') << i;

        // push into work queue
        ThreadArgs *sub_args = new ThreadArgs();
        sub_args->path = s.str();
        sub_args->depth = arg->depth + 1;
        sub_args->uid = arg->uid;
        sub_args->gid = arg->uid;
        sub_args->seed = gen();

        thpool_add_work(*arg->pool, generatedir <DirGenerator, DBGenerator, BucketDistribution>, sub_args);
    }
}

std::ostream &print_help(std::ostream &stream, const char *argv0) {
    static const Settings s;
    return stream << "Syntax: " << argv0 << " [options] directory --file-count [--file-count ...] --file-size [--file-size ...]"
                  << "\n"
                  << "\n    -h | --help"
                  << "\n"
                  << "\n    Options:"
                  << "\n        --users n               Number of users                                        (Default: "  << s.users << ")"
                  << "\n        --files n               Number of files in the tree                            (Default: "  << s.files << ")"
                  << "\n        --uid uid               Set the starting uid                                   (Default: "  << s.uid << ")"
                  << "\n        --gid gid               Set the starting gid                                   (Default: "  << s.gid << ")"
                  << "\n        --path-separator c      Character that separates path sections                 (Default: '" << s.path_separator << "') "
                  << "\n        --leading-zeros n       Number of leading zeros in names                       (Default: "  << s.leading_zeros <<")"
                  << "\n        --db-name name          Name of each database file                             (Default: "  << s.db_name << ")"
                  << "\n        --files-with-dirs       Whether or not files exist in intermediate directories (Default: "  << std::boolalpha << s.files_with_dirs << ")"
                  << "\n        --blocksize n           Block size for filesystem I/O                          (Default: "  << s.blocksize << ")"
                  << "\n        --chown                 Set the owner of each directory (requires sudo)        (Default: "  << s.chown << ")"
                  << "\n        --chgrp                 Set the group of each directory (requires sudo)        (Default: "  << s.chgrp << ")"
                  << "\n        --file-count            Comma separated triple of min,max,weight. At least one should be provided."
                  << "\n        --file-size             Comma separated triple of min,max,weight. At least one should be provided."
                  << "\n"
                  << "\n    RNG configuration:"
                  // << "\n        --seed n                Value to seed the seed generator                       (Default: Seconds since epoch)"
                  << "\n        --min-time n            Minimum timestamp                                      (Default: "  << s.time.min << ")"
                  << "\n        --max-time n            Maximum timestamp                                      (Default: "  << s.time.max << ")"
                  << "\n        --min-subdirs n         Minimum subdirectories                                 (Default: "  << s.subdirs.min << ")"
                  << "\n        --max-subdirs n         Maximum subdirectories                                 (Default: "  << s.subdirs.max << ")"
                  << "\n"
                  << "\n    Runtime Configuration:"
                  << "\n        --threads n             Set the number of threads to use                       (Default: "  << s.threads << ")"
                  << "\n        --stat-rate ms          How often to print timed statistics, in milliseconds   (Default: "  << s.stat_rate << ", 0 cancels)"
                  << "\n        --progress-rate ms      How often to print progress, in milliseconds           (Default: "  << s.progress_rate << ", 0 cancels)"
                  << "\n        --dry-run               Runs without creating the GUFI tree                    (Default: "  << s.dryrun << ")"
                  << "\n    directory                   The directory to put the generated tree into"
                  << "\n";
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
        settings.var ^= true;                                \
    }

#define PARSE_BUCKET(opt, var, err)                          \
    else if (args == "--" opt) {                             \
        std::size_t min;                                     \
        char comma1;                                         \
        std::size_t max;                                     \
        char comma2;                                         \
        double weight;                                       \
        if (!(std::stringstream(argv[i]) >> min >> comma1    \
                                         >> max >> comma2    \
                                         >> weight) ||       \
            (comma1 != ',') || (comma2 != ',') ||            \
            (min > max) || !max) {                           \
            std::cerr << "Bad " << err << " bucket: "        \
                      << args << std::endl;                  \
            return false;                                    \
        }                                                    \
        settings.var.emplace_back(Bucket(min, max, weight)); \
        i++;                                                 \
    }

bool parse_args(int argc, char *argv[], Settings &settings, bool &help) {
    int i = 1;
    while (i < argc) {
        const std::string args(argv[i++]);
        if ((args == "--help") || (args == "-h")) {
            return help = true;
        }
        PARSE_VALUE ("users",           users,             "user count")
        PARSE_VALUE ("files",           files,             "file count")
        PARSE_VALUE ("uid",             uid,               "uid")
        PARSE_VALUE ("gid",             gid,               "gid")
        PARSE_VALUE ("path-separator",  path_separator,    "path separator")
        PARSE_VALUE ("leading-zeros",   leading_zeros,     "leading zero count")
        PARSE_VALUE ("db-name",         db_name,           "db name")
        PARSE_FLAG  ("files-with-dirs", files_with_dirs)
        PARSE_FLAG  ("chown",           chown)
        PARSE_FLAG  ("chgrp",           chgrp)
        PARSE_BUCKET("file-count",      file_count,        "file count")
        PARSE_BUCKET("file-size",       file_size,         "file size")
        // PARSE_VALUE ("seed",            seed,              "seed")
        PARSE_VALUE ("min-subdirs",     subdirs.min,       "min subdirectories")
        PARSE_VALUE ("max-subdirs",     subdirs.max,       "max subdirectories")
        PARSE_VALUE ("min-time",        time.min,          "min atime")
        PARSE_VALUE ("max-time",        time.max,          "max atime")
        PARSE_VALUE ("blocksize",       blocksize,         "blocksize")
        PARSE_VALUE ("threads",         threads,           "thread count")
        PARSE_VALUE ("stat-rate",       stat_rate,         "timed statistics collection rate")
        PARSE_VALUE ("progress-rate",   progress_rate,     "progress print rate")
        PARSE_FLAG  ("dry-run", dryrun)
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

bool create_path(std::string path, const char sep = '/') {
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

    // set top level database owner and group
    // don't check for errors
    chown(fullname.c_str(), settings.chown?(settings.uid + settings.users):-1, settings.chgrp?(settings.gid + settings.users):-1);

    // set top level directory owner, group, and permissions
    // don't check for errors
    chown(settings.top.c_str(), settings.chown?(settings.uid + settings.users):-1, settings.chgrp?(settings.gid + settings.users):-1);
    chmod(settings.top.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);

    return true;
}

// Convenience class for running functions in a timed loop on a thread
class LoopedThread {
    public:
        template <typename F, typename... Args>
        LoopedThread(const unsigned int &ms, std::mutex &mutex, F &&func, Args &&...args)
            : rate(ms),
              mutex(mutex),
              cv(),
              thread([this, &func, &args...]() {
                      auto run = std::bind(std::forward <F> (func), std::forward <Args>(args)...);
                      std::unique_lock <std::mutex> lock(this->mutex);
                      while (this->rate.count()) {
                          this->cv.wait_for(lock, this->rate);
                          run();
                      }
                  }
                  )
        {}

        ~LoopedThread() {
            stop();
            thread.join();
        }

        void stop() {
            {
                std::lock_guard <std::mutex> lock(mutex);
                rate = std::chrono::milliseconds::zero();
            }

            cv.notify_all();
        }

    private:
        std::chrono::milliseconds rate;
        std::mutex &mutex;
        std::condition_variable cv;

        std::thread thread;
};

int main(int argc, char *argv[]) {
    Settings settings;

    // parse arguments
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

        // make sure the path doesn't already exist
        struct stat st;
        if (stat(settings.top.c_str(), &st) == 0) { // if the top level path exists
            std::cerr << "Error: \"" << settings.top << "\" already exists" << std::endl;
            return 1;
        }
        else if (errno != ENOENT) {                 // if the error is not "path doesn't exist"
            std::cerr << "Error: " << strerror(errno) << " (" << errno << ")" << std::endl;
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

    // initialize static values
    {
        std::lock_guard <std::mutex> lock(ThreadArgs::mutex);
        ThreadArgs::settings = &settings;
        ThreadArgs::pool = &pool;
        ThreadArgs::stats.clear();
    }

    // use this rng to generate seeds for other rngs
    std::mt19937 gen(settings.seed);

    // mutex for preventing prints from overlapping
    std::mutex print_mutex;

    // set up timed stats thread
    LoopedThread timed_stats(settings.stat_rate, ThreadArgs::timed_stats.mutex, [&settings, &print_mutex](){
            std::lock_guard <std::mutex> print_lock(print_mutex);
            std::cout << ThreadArgs::timed_stats << " since the last stat collection " << settings.stat_rate << " ms ago" << std::endl;
            ThreadArgs::timed_stats.clear();
        });

    // set up timed stats thread
    LoopedThread progress(settings.progress_rate, ThreadArgs::mutex, [&print_mutex](){
            std::lock_guard <std::mutex> print_lock(print_mutex);
            std::cout << "Progress: " << ThreadArgs::files << " files in " << ThreadArgs::stats.size() << " directories" << std::endl;
        });

    struct timespec start = {};
    clock_gettime(CLOCK_MONOTONIC, &start);

    // push each subdirectory generation into the thread pool
    for(std::size_t offset = 0; offset < settings.users; offset++) {
        std::stringstream s;
        s << settings.top << "/user" << std::setw(settings.leading_zeros) << std::setfill('0') << settings.uid + offset;

        // push into work queue
        ThreadArgs *args = new ThreadArgs();
        args->path = s.str();
        args->depth = 1;
        args->uid = settings.uid + offset;
        args->gid = settings.gid + offset;
        args->seed = gen();

        thpool_add_work(pool, generatedir <std::knuth_b, std::mt19937, std::normal_distribution <double> >, args);
    }

    thpool_wait(pool);

    struct timespec end = {};
    clock_gettime(CLOCK_MONOTONIC, &end);

    // stop print threads
    timed_stats.stop();
    progress.stop();

    thpool_destroy(pool);

    // print stats
    std::cout << std::endl
              << "Elapsed time: " << elapsed(start, end) << " seconds" << std::endl;

    if (settings.dryrun) {
        std::cout << "Would have generated" << std::endl;
    }
    else {
        std::cout << "Generated" << std::endl;;
    }

    std::size_t leaf_nodes = 0;
    std::vector <std::size_t> file_count_histogram(settings.file_count.size());

    // list all directories
    std::cout << "    " << ThreadArgs::stats.size() << " + 1 directories" << std::endl;
    for(ThreadArgs::Stats const & stat : ThreadArgs::stats) {
        // std::cout << "        " << stat.name << " " << stat.files << " " << stat.depth << " " << stat.leaf << std::endl;

        // count leaf nodes
        leaf_nodes += stat.leaf;

        // get file count distribution
        for(std::size_t i = 0; i < settings.file_count.size(); i++) {
            if (settings.file_count[i].in(stat.files)) {
                file_count_histogram[i]++;
                break;
            }
        }
    }

    std::cout << "    " << leaf_nodes << " Leaf Nodes" << std::endl
              << "    Files: " << ThreadArgs::files << std::endl
              << "    Size: " << ThreadArgs::size << std::endl
              << "    File count distribution" << std::endl;
    for(std::size_t i = 0; i < file_count_histogram.size(); i++) {
        std::cout << "        " << settings.file_count[i] << ": " << file_count_histogram[i] << std::endl;
    }

    return 0;
}
