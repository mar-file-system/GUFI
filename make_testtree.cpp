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
          path_separator('/'),
          leading_zeros(5),
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
          file_size()
    {
        depth.min = 1;
        depth.max = 5;

        subdirs.min = 1;
        subdirs.max = 5;

        time.min = 0;
        time.max = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    }

    operator bool() const {
        return users &&
            (files >= users) &&
            depth.min && (depth.min <= depth.max) &&
            (time.min <= time.max) &&
            threads &&
            blocksize &&
            path_separator &&
            db_name.size() &&
            top.size() &&
            file_count.size() &&
            file_size.size();
    }

    std::ostream &print(std::ostream &stream) const {
        stream << "Tree properties:"
               << "\n    Path:           " << top
               << "\n    Files:          " << files
               << "\n        Blocksize:  " << blocksize
               << "\n        Timestamps: [" << time.min << ", " << time.max << "]"
               << "\n    Users:          " << users
               << "\n    Depth:          [" << depth.min << ", " << depth.max << "]"
               << "\n    Subdirectories: [" << subdirs.min << ", " << subdirs.max << "]";

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
               << "\nThreads:  " << threads;

        return stream;
    }

    template <typename T = std::size_t>
    struct Range {
        T min;
        T max;
    };

    std::size_t users;                   // count
    std::size_t files;                   // count

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
    std::vector <Bucket> file_count;      // set of file distribution buckets
    std::vector <Bucket> file_size;      // set of file size buckets
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
    std::size_t uid;                     // uid/gid to use
    std::size_t index;                   // unique id within the current depth
    std::size_t seed;                    // seed for seeding the RNGs inside this thread
    std::size_t inode;                   // inode offset

    static Settings *settings;
    static threadpool *pool;
};

Settings   *ThreadArgs::settings = nullptr;

threadpool *ThreadArgs::pool     = nullptr;

// Distribute total into file_distribution (which should
// have been resized to the desired number of slots
// before being passed into this function).
//
// file_distribution[0] is the current directory.
//
// https://stackoverflow.com/a/22381248
template <typename Distribution>
void distribute_total_randomly(const std::size_t total,
                               std::vector <std::size_t> &file_distribution,
                               const bool files_in_currdir,
                               Distribution &seed_rng) {
    if (!file_distribution.size()) {
        throw std::runtime_error("Input vector requires at least 1 slot");
    }

    std::mt19937 count_gen(seed_rng());
    std::uniform_int_distribution <std::size_t> count_rng(1, total);
    std::size_t sum = 0;

    // assign each user a random number of files
    for(std::size_t &count : file_distribution) {
        count = count_rng(count_gen);
        sum += count;
    }

    // if files are not supposed to be in the current directory, set it to 0
    if (!files_in_currdir) {
        sum -= file_distribution[0];
        file_distribution[0] = 0;
    }

    // scale the number of files down so that the sum is close to the target number of files
    const double factor = ((double) total - files_in_currdir - file_distribution.size()) / sum;
    std::size_t missing = total;
    for(std::size_t &count : file_distribution) {
        count = ((double) count) * factor + 1;
        missing -= count;
    }

    // place missing counts in random locations
    std::mt19937 fill_gen(seed_rng());
    std::uniform_int_distribution <std::size_t> fill_rng(!files_in_currdir, file_distribution.size() - 1);

    for(std::size_t i = 0; i < missing; i++) {
        file_distribution[fill_rng(fill_gen)]++;
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
void generatecurr(ThreadArgs *arg, const std::size_t files) {
    const std::string fullname = arg->path + arg->settings->path_separator + arg->settings->db_name;

    sqlite3 *on_disk = open_and_create_tables(fullname.c_str());
    if (!on_disk) {
        std::cerr << "Could not open " << fullname << ": " << sqlite3_errmsg(on_disk) << std::endl;
        return;
    }

    // rng used to generate seeds for other rngs
    std::mt19937 seed_rng(arg->seed);

    // generic rng
    std::mt19937 gen(seed_rng());
    std::uniform_int_distribution <std::size_t> rng(0, ((std::size_t) -1) >> 1);

    // rng for timestamps
    std::mt19937 time_gen(seed_rng());
    std::uniform_int_distribution <std::size_t> time_rng(arg->settings->time.min, arg->settings->time.max);

    // rng for selecting size bucket
    std::vector <double> weights;
    for(Bucket const &file_size : arg->settings->file_size) {
        weights.push_back(file_size.weight);
    }
    std::mt19937 bucket_gen(seed_rng());
    std::discrete_distribution <std::size_t> bucket_rng(weights.begin(), weights.end());

    // rng for selecting size within a bucket
    std::vector <std::mt19937> size_gens(arg->settings->file_size.size());
    std::vector <std::uniform_int_distribution <std::size_t> > size_rngs(arg->settings->file_size.size());
    for(std::size_t i = 0; i < arg->settings->file_size.size(); i++) {
        size_gens[i] = std::mt19937(seed_rng());
        size_rngs[i] = std::uniform_int_distribution <std::size_t>(arg->settings->file_size[i].min, arg->settings->file_size[i].max);
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

        const std::size_t bucket = bucket_rng(bucket_gen);

        struct work work;

        snprintf(work.name, MAXPATH, "%s", s.str().c_str());
        snprintf(work.type, 2, "f");
        snprintf(work.linkname, MAXPATH, "");
        snprintf(work.xattr, MAXPATH, "xattr %zu", i);
        snprintf(work.osstext1, MAXXATTR, "osstext1 %zu", i);
        snprintf(work.osstext2, MAXXATTR, "osstext2 %zu", i);

        work.statuso.st_ino = arg->inode++;
        work.statuso.st_mode = 0777;
        work.statuso.st_nlink = 1;
        work.statuso.st_uid = arg->uid;
        work.statuso.st_gid = arg->uid;
        work.statuso.st_size = size_rngs[bucket](size_gens[bucket]);
        work.statuso.st_blksize = arg->settings->blocksize;
        work.statuso.st_blocks = work.statuso.st_size / work.statuso.st_blksize;
        work.statuso.st_ctime = time_rng(time_gen);
        work.statuso.st_atime = std::max(work.statuso.st_ctime, (time_t) time_rng(time_gen));
        work.statuso.st_mtime = std::max(work.statuso.st_ctime, (time_t) time_rng(time_gen));

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
    std::size_t bucket = bucket_rng(bucket_gen);

    struct work work;

    snprintf(work.name, MAXPATH, "%s", arg->path.c_str());
    snprintf(work.type, 2, "d");
    snprintf(work.linkname, MAXPATH, "");
    snprintf(work.xattr, MAXPATH, "xattr");
    snprintf(work.osstext1, MAXXATTR, "osstext1");
    snprintf(work.osstext2, MAXXATTR, "osstext2");

    work.statuso.st_ino = arg->inode;
    work.statuso.st_mode = 0775;
    work.statuso.st_nlink = 1;
    work.statuso.st_uid = arg->uid;
    work.statuso.st_gid = arg->uid;
    work.statuso.st_size = size_rngs[bucket](size_gens[bucket]);
    work.statuso.st_blksize = rng(gen);
    work.statuso.st_blocks = rng(gen);
    work.statuso.st_ctime = time_rng(time_gen);
    work.statuso.st_atime = work.statuso.st_ctime + time_rng(time_gen);
    work.statuso.st_mtime = work.statuso.st_ctime + time_rng(time_gen);

    work.crtime  = rng(gen);
    work.ossint1 = rng(gen);
    work.ossint2 = rng(gen);
    work.ossint3 = rng(gen);
    work.ossint4 = rng(gen);

    insertsumdb(on_disk, &work, &summary);

    sqlite3_close(on_disk);
}

void generatedir(void *args) {
    ThreadArgs *arg = (ThreadArgs *) args;
    if (!arg->files) {
        return;
    }

    // this function is responsible for creating the directory it is working on
    if (mkdir(arg->path.c_str(), 0777) != 0) {
        const int err = errno;
        std::cerr << "mkdir error: " << arg->path << ": " << strerror(err) << std::endl;
        if (err != EEXIST) {
            return;
        }
    }

    // rng used to generate seeds for other rngs
    std::mt19937 seed_rng(arg->seed);

    // subdirectory count generator
    std::mt19937 subdir_gen(seed_rng());
    std::uniform_int_distribution <std::size_t> subdir_rng(arg->settings->subdirs.min, arg->settings->subdirs.max);

    // distribute the files across the current directory
    std::vector <std::size_t> file_distribution(subdir_rng(subdir_gen) + 1);
    if (arg->curr_depth < arg->target_depth) {
        distribute_total_randomly(arg->files, file_distribution, arg->settings->files_with_dirs, seed_rng);
    }
    // this is the final directory, so all files are here
    else {
        file_distribution.resize(1);
        file_distribution[0] = arg->files;
    }

    // generate the files in the current directory
    generatecurr(arg, file_distribution[0]);

    std::size_t inode = arg->inode + 1 + file_distribution[0];

    for(std::size_t i = 1; i < file_distribution.size(); i++) {
        std::stringstream s;
        s << arg->path << "/dir" << i - 1;

        // push into work queue
        ThreadArgs *sub_args = new ThreadArgs();
        sub_args->path = s.str();
        sub_args->files = file_distribution[i];
        sub_args->target_depth = arg->target_depth;
        sub_args->curr_depth = arg->curr_depth + 1;
        sub_args->uid = arg->uid;
        sub_args->index = arg->index;
        sub_args->seed = seed_rng();
        sub_args->inode = inode;

        // increment inode by directory + files (not perfect because subdirectory count is not known)
        inode += 1 + file_distribution[i];

        thpool_add_work(*arg->pool, generatedir, sub_args);
    }

    delete arg;
}

std::ostream &print_help(std::ostream &stream, char *argv0) {
    static const Settings s;
    return stream << "Syntax: " << argv0 << " [options] directory --filesize [--filesize ...]\n"
                  << "\n"
                  << "    Options:\n"
                  << "        --users n               Number of users                                        (Default: "  << s.users << ")\n"
                  << "        --files n               Number of files in the tree                            (Default: "  << s.files << ")\n"
                  << "        --path-separator c      Character that separates path sections                 (Default: '" << s.path_separator << "') \n"
                  << "        --leading-zeros n       Number of leading zeros in names                       (Default: "  << s.leading_zeros <<")\n"
                  << "        --db-name name          Name of each database file                             (Default: "  << s.db_name << ")\n"
                  << "        --files-with-dirs b     Whether or not files exist in intermediate directories (Default: "  << std::boolalpha << s.files_with_dirs << ")\n"
                  << "        --threads n             Set the number of threads to use                       (Default: "  << s.threads << ")\n"
                  << "        --blocksize n           Block size for filesystem I/O                          (Default: "  << s.blocksize << ")\n"
                  << "        --file-count            Comma separated triple of min,max,weight. At least one should be provided.\n"
                  << "        --file-size             Comma separated triple of min,max,weight. At least one should be provided.\n"
                  << "\n"
                  << "    RNG configuration:\n"
                  << "        --seed n                Value to seed the seed generator                       (Default: Seconds since epoch)\n"
                  << "        --min-depth n           Minimum layers of directories                          (Default: "  << s.depth.min << ")\n"
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

#define PARSE(opt, var, err)                                 \
    else if (args == "--" opt) {                             \
        if (!read_value(argc, argv, i, settings.var, err)) { \
            return false;                                    \
        }                                                    \
    }

bool parse_args(int argc, char *argv[], Settings &settings, bool &help) {
    bool positional = false;
    int i = 1;
    while (i < argc) {
        const std::string args(argv[i++]);
        if ((args == "--help") || (args == "-h")) {
            return help = true;
        }
        PARSE("users",           users,             "user count")
        PARSE("files",           files,             "file count")
        PARSE("path-separator",  path_separator,    "path separator")
        PARSE("leading-zeros",   leading_zeros,     "leading zero count")
        PARSE("db-name",         db_name,           "db name")
        else if (args == "--files-with-dirs") {
            if (!(std::stringstream(argv[i]) >> std::boolalpha >> settings.files_with_dirs)) {
                std::cerr << "Bad boolean: " << argv[i] << std::endl;
            }

            i++;
        }
        else if (args == "--file-count") {
            std::size_t min;
            char comma1;
            std::size_t max;
            char comma2;
            double weight;

            if (!(std::stringstream(argv[i++]) >> min >> comma1 >> max >> comma2 >> weight) ||
                (comma1 != ',') || (comma2 != ',') ||
                (min > max) || !max) {
                std::cerr << "Bad file count bucket: " << argv[i] << std::endl;
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
                std::cerr << "Bad file size bucket: " << argv[i] << std::endl;
                return false;
            }

            settings.file_size.emplace_back(Bucket(min, max, weight));
        }
        PARSE("seed",            seed,              "seed")
        PARSE("min-depth",       depth.min,         "min depth")
        PARSE("max-depth",       depth.max,         "max depth")
        PARSE("min-subdirs",     subdirs.min,       "min subdirectories")
        PARSE("max-subdirs",     subdirs.max,       "max subdirectories")
        PARSE("min-time",        time.min,          "min atime")
        PARSE("max-time",        time.max,          "max atime")
        PARSE("threads",         threads,           "thread count")
        PARSE("blocksize",       blocksize,         "blocksize")
        else {
            // first positional argument is the directory
            if (!settings.top.size()) {
                settings.top = std::move(args);
            }
        }
    }

    if (!settings.file_count.size()) {
        std::cerr << "At least one file count range is needed" << std::endl;
        return false;
    }

    if (!settings.file_size.size()) {
        std::cerr << "At least one file size range is needed" << std::endl;
        return false;
    }

    return (i == argc) && settings;
}

// Recursive mkdir
// If there is an error, previously created directories are not deleted
bool create_path(const std::string &path, bool &exists) {
    // check if the path is a directory
    struct stat path_stat;
    if (exists && stat(path.c_str(), &path_stat) == 0) {
        if (!S_ISDIR(path_stat.st_mode)) {
            std::cerr << path << " is not a directory. Skipping." << std::endl;
            return false;
        }
        else {
            exists = true;
        }
    }
    else if (!exists || (errno == ENOENT)) {
        // if it doesn't, create the directory
        exists = false;
        if (mkdir(path.c_str(), 0777) != 0) {
            std::cerr << "Could not create " << path << ". Skipping." << std::endl;
            return false;
        }
    }

    return true;
}

// create the top level separately
bool create_top(const Settings &settings, std::string &fullname) {
    // create the top level directory
    bool exists = true;
    if (!create_path(settings.top, exists)) {
        std::cerr << "Could not create directory " << settings.top << std::endl;
        return false;
    }

    // create the top level database file
    fullname = settings.top + settings.path_separator + settings.db_name;
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
    bool help = false;
    if (!parse_args(argc, argv, settings, help)) {
        print_help(std::cout, argv[0]);
        return 1;
    }

    if (help) {
        print_help(std::cout, argv[0]);
        return 0;
    }

    std::cout << settings << std::endl;

    // create the top level separately
    std::string fullname;
    if (!create_top(settings, fullname)) {
        return 1;
    }

    // start up thread pool
    threadpool pool = thpool_init(settings.threads);
    if (thpool_null(pool)) {
        std::cerr << "Failed to create thread pool" << std::endl;
        return 1;
    }

    // save globals
    ThreadArgs::settings = &settings;
    ThreadArgs::pool = &pool;

    // use this rng to generate seeds for other rngs
    std::mt19937 seed_rng(settings.seed);

    // generate file distribution by user
    std::vector <std::size_t> file_counts(settings.users);
    distribute_total_randomly(settings.files, file_counts, true, seed_rng);

    // rng for depth selection
    std::mt19937 depth_gen(seed_rng());
    std::uniform_int_distribution <std::size_t> depth_rng(settings.depth.min, settings.depth.max);

    // inode current subdirectory starts at
    std::size_t inode = 0;

    // push each subdirectory generation into the thread pool
    for(std::size_t uid = 0; uid < settings.users; uid++) {
        std::stringstream s;
        s << settings.top << "/user" << uid;

        // push into work queue
        ThreadArgs *args = new ThreadArgs();
        args->path = s.str();
        args->files = file_counts[uid];
        args->target_depth = depth_rng(depth_gen);
        args->curr_depth = 1;
        args->uid = uid;
        args->index = uid;
        args->seed = seed_rng();
        args->inode = inode;

        // next directory starts with an new set of inodes
        inode += args->files + args->target_depth;

        thpool_add_work(pool, generatedir, args);
    }

    thpool_wait(pool);
    thpool_destroy(pool);

    return 0;
}
