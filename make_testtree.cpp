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

const std::size_t DEFAULT_USERS = 200;
const std::size_t DEFAULT_NUM_FILES = 750000000;

const char        DEFAULT_PATH_SEPARATOR = '/';
const std::size_t DEFAULT_MIN_DEPTH = 1;
const std::size_t DEFAULT_MAX_DEPTH = 1;
const std::size_t DEFAULT_LEADING_ZEROS = 5;
const std::string DEFAULT_DB_NAME = "db.db";
const bool        DEFAULT_FILES_WITH_DIRS = false;

const std::size_t DEFAULT_THREAD_COUNT = std::max(std::thread::hardware_concurrency() / 2, (decltype(std::thread::hardware_concurrency())) 1);

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
        : users(DEFAULT_USERS),
          files(DEFAULT_NUM_FILES),
          path_separator(DEFAULT_PATH_SEPARATOR),
          min_depth(DEFAULT_MIN_DEPTH),
          max_depth(DEFAULT_MAX_DEPTH),
          leading_zeros(DEFAULT_LEADING_ZEROS),
          db_name(DEFAULT_DB_NAME),
          seed(std::chrono::high_resolution_clock::now().time_since_epoch().count()),
          thread_count(DEFAULT_THREAD_COUNT),
          top(),
          buckets()
    {}

    operator bool() const {
        return users &&
            (files >= users) &&
            min_depth &&
            (min_depth <= max_depth) &&
            thread_count &&
            path_separator &&
            db_name.size() &&
            top.size() &&
            buckets.size();
    }

    std::size_t users;                   // count
    std::size_t files;                   // count

    char        path_separator;          // '/'
    std::size_t min_depth;               // > 0
    std::size_t max_depth;               // >= min_depth
    std::size_t leading_zeros;           // for filling file/directory names
    std::string db_name;                 // db.db
    bool        files_with_dirs;         // whether or not to generate files next to directories

    std::size_t seed;                    // used to seed a seed rng
    std::size_t thread_count;            // count

    std::string top;                     // top depth directory
    std::vector <Bucket> buckets;        // set of file size buckets
};

struct ThreadArgs {
    std::size_t target_depth;            // final depth of directories
    std::size_t uid;                     // uid/gid to use
    std::size_t index;                   // unique id within the current depth
    std::size_t seed;                    // seed for seeding the RNGs inside this thread
    std::size_t inode;                   // inode offset
    std::size_t files;                   // total number of files under this directory
    std::string directory;               // directory to put database into
    static Settings *settings;
    static threadpool *pool;
};

Settings   *ThreadArgs::settings  = nullptr;
threadpool *ThreadArgs::pool      = nullptr;

// Generate random counts such that they add up to total
// file_counts should have been resized to the desired
// number of slots before being passed into this function
//
// https://stackoverflow.com/a/22381248
template <typename Engine, typename Distribution>
void distribute_total_randomly(std::vector <std::size_t> &counts, const std::size_t total_files,
                                Engine &seed_gen, Distribution &seed_rng) {
    std::default_random_engine count_gen(seed_rng(seed_gen));
    std::uniform_int_distribution <std::size_t> count_rng(1, total_files);
    std::size_t sum = 0;

    // assign each user a random number of files
    for(std::size_t &count : counts) {
        count = count_rng(count_gen);
        sum += count;
    }

    const double factor = (double) (total_files - counts.size()) / sum;

    // scale the number of files down so that the sum is close to the target number of files
    std::size_t missing = total_files;
    for(std::size_t &count : counts) {
        count = ((double) count) * factor + 1;
        missing -= count;
    }

    // place missing counts in random locations
    std::default_random_engine user_gen(seed_rng(seed_gen));
    std::uniform_int_distribution <std::size_t> user_rng(0, counts.size() - 1);
    for(std::size_t i = 0; i < missing; i++) {
        counts[user_rng(user_gen)]++;
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

// Creates the path and creates a databse file in the directory
// If there is an error, previously created directories and databases are not deleted
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

// This function creates all directories, database files, and fills
// in the database files with random file metadata for a single user
//
// https://stackoverflow.com/q/1711631
void thread(void *args) {
    ThreadArgs *arg = (ThreadArgs *) args;

    // use this rng to generate seeds for other rngs
    std::default_random_engine seed_gen(arg->seed);
    std::uniform_int_distribution <std::size_t> seed_rng(0, -1);

    // generic rng
    std::default_random_engine gen(seed_rng(seed_gen));
    std::uniform_int_distribution <std::size_t> rng(0, -1);

    // rng for timestamps
    std::default_random_engine time_gen(seed_rng(seed_gen));
    std::uniform_int_distribution <std::size_t> time_rng(0, std::chrono::high_resolution_clock::now().time_since_epoch().count());

    // rng for selecting size bucket
    std::vector <double> weights;
    for(Bucket const &bucket : arg->settings->buckets) {
        weights.push_back(bucket.weight);
    }
    std::default_random_engine bucket_gen(seed_rng(seed_gen));
    std::discrete_distribution <std::size_t> bucket_rng(weights.begin(), weights.end());

    // rng for selecting size within a bucket
    std::vector <std::default_random_engine> size_gens(arg->settings->buckets.size());
    std::vector <std::uniform_int_distribution <std::size_t> > size_rngs(arg->settings->buckets.size());
    for(std::size_t i = 0; i < arg->settings->buckets.size(); i++) {
        size_gens[i] = std::default_random_engine(seed_rng(seed_gen));
        size_rngs[i] = std::uniform_int_distribution <std::size_t>(arg->settings->buckets[i].min, arg->settings->buckets[i].max);
    }

    // generate file distribution for each level of this path
    // this vector is used as 1 indexed instead of 0
    std::vector <std::size_t> file_counts(arg->target_depth);

    // if records should be created throughout the directory, distribute the file count
    // otherwise, default to the bottom-most directory containing all of the files
    file_counts.back() = arg->files;
    if (arg->settings->files_with_dirs) {
        distribute_total_randomly(file_counts, arg->files, seed_gen, seed_rng);
    }

    // full directory is accumulated here
    std::stringstream s;
    s << arg->directory;

    const std::size_t last_level = arg->target_depth - 1;

    // generate entire path, including database file at each level
    bool exists = true; // assume the current directory does exist
    for(std::size_t depth = 0; depth < arg->target_depth; depth++) {
        // generate the current directory path
        s << arg->settings->path_separator << "dir" << std::setw(arg->settings->leading_zeros) << std::setfill('0') << arg->index;

        const std::string directory = s.str();
        if (!create_path(directory, exists)) {
            std::cerr << "Could not create directory " << directory << std::endl;
            break;
        }

        // open the database file (but don't do anything with it until the very end)
        const std::string db_name = directory + arg->settings->path_separator + arg->settings->db_name;
        sqlite3 *on_disk = open_and_create_tables(db_name.c_str());
        if (!on_disk) {
            std::cerr << "Could not open " << db_name << ": " << sqlite3_errmsg(on_disk) << std::endl;
            break;
        }

        struct sum summary;
        zeroit(&summary);

        // only generate files if specified or if at last level
        if (arg->settings->files_with_dirs || (depth == last_level)) {
            // prepare to insert entries
            sqlite3_stmt *res = nullptr;
            static const std::size_t esqli_len = strlen(esqli);
            if (sqlite3_prepare_v2(on_disk, esqli, esqli_len, &res, nullptr) != SQLITE_OK) {
                std::cerr << "Could not prepare statement for " << arg->directory << " (" << sqlite3_errmsg(on_disk) << "). Skipping" << std::endl;
                sqlite3_close(on_disk);
                break;
            }

            sqlite3_exec(on_disk, "BEGIN TRANSACTION", nullptr, nullptr, nullptr);

            // create file entries and insert them into the database
            for(std::size_t i = 0; i < file_counts[depth]; i++) {
                std::stringstream s;
                s << arg->directory << arg->settings->path_separator << "file" << std::setw(arg->settings->leading_zeros) << std::setfill('0') << i;

                std::size_t bucket = bucket_rng(bucket_gen);

                struct work work;

                snprintf(work.name, MAXPATH, s.str().c_str());
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
                work.statuso.st_blksize = rng(gen);
                work.statuso.st_blocks = rng(gen);
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
        }

        // summarize this directory
        std::size_t bucket = bucket_rng(bucket_gen);

        struct work work;

        snprintf(work.name, MAXPATH, arg->directory.c_str());
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

    delete arg;
}

std::ostream &print_help(std::ostream &stream, char *argv0) {
    return stream << "Syntax: " << argv0 << " [options] directory bucket_0 [bucket_1 ... bucket_n]\n"
                  << "\n"
                  << "    Options:\n"
                  << "        --users n               Number of users                                        (Default: " << DEFAULT_USERS << ")\n"
                  << "        --files n               Number of files in the tree                            (Default: " << DEFAULT_NUM_FILES << ")\n"
                  << "        --path_separator c      Character that separates path sections                 (Default: " << DEFAULT_PATH_SEPARATOR << ") \n"
                  << "        --min-depth n           Minimum layers of directories                          (Default: " << DEFAULT_MIN_DEPTH << ")\n"
                  << "        --max-depth n           Maximum layers of directories                          (Default: " << DEFAULT_MAX_DEPTH << ")\n"
                  << "        --leading-zeros n       Number of leading zeros in names                       (Default: " << DEFAULT_LEADING_ZEROS <<")\n"
                  << "        --db-name name          Name of each database file                             (Default: " << DEFAULT_DB_NAME << ")\n"
                  << "        --files-with-dirs b     Whether or not files exist in intermediate directories (Default: " << DEFAULT_FILES_WITH_DIRS << ")\n"
                  << "        --threads n             Set the number of threads to use                       (Default: " << DEFAULT_THREAD_COUNT << ")\n"
                  << "        --seed n                Value to seed the seed generator                       (Default: Seconds since epoch)\n"
                  << "\n"
                  << "    directory                   The directory to put the generated tree into\n"
                  << "    bucket_i                    A comma separated triple of min,max,weight file size bucket\n";
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

bool parse_args(int argc, char *argv[], Settings &settings, bool &help) {
    int positional = 0;
    int i = 1;
    while (i < argc) {
        const std::string args(argv[i++]);
        if ((args == "--help") || (args == "-h")) {
            return help = true;
        }
        else if (args == "--users") {
            if (!read_value(argc, argv, i, settings.users, "user count")) {
                return false;
            }
        }
        else if (args == "--files") {
            if (!read_value(argc, argv, i, settings.files, "file count")) {
                return false;
            }
        }
        else if (args == "--path_separator") {
            if (!read_value(argc, argv, i, settings.path_separator, "path separator")) {
                return false;
            }
        }
        else if (args == "--min-depth") {
            if (!read_value(argc, argv, i, settings.min_depth, "min depth")) {
                return false;
            }
        }
        else if (args == "--max-depth") {
            if (!read_value(argc, argv, i, settings.max_depth, "max depth")) {
                return false;
            }
        }
        else if (args == "--leading-zeros") {
            if (!read_value(argc, argv, i, settings.leading_zeros, "leading zero count")) {
                return false;
            }
        }
        else if (args == "--db-name") {
            if (!read_value(argc, argv, i, settings.db_name, "db name")) {
                return false;
            }
        }
        else if (args == "--files-with-dirs") {
            if (!(std::stringstream(argv[i]) >> std::boolalpha >> settings.files_with_dirs)) {
                std::cerr << "Bad boolean: " << argv[i] << std::endl;
                return false;
            }

            i++;
        }
        else if (args == "--seed") {
            if (!read_value(argc, argv, i, settings.seed, "seed count")) {
                return false;
            }
        }
        else if (args == "--threads") {
            if (!read_value(argc, argv, i, settings.thread_count, "thread count")) {
                return false;
            }
        }
        else {
            // first positional argument is the directory
            if (!settings.top.size()) {
                settings.top = std::move(args);
            }
            // all others are buckets
            else {
                std::size_t min;
                char comma1;
                std::size_t max;
                char comma2;
                double weight;

                if (!(std::stringstream(args) >> min >> comma1 >> max >> comma2 >> weight) ||
                    (comma1 != ',') || (comma2 != ',') ||
                    (min > max) || !max) {
                    std::cerr << "Bad bucket: " << argv[i] << std::endl;
                    return false;
                }

                settings.buckets.emplace_back(Bucket(min, max, weight));
            }

            positional++;
        }
    }

    if (positional < 2) {
        std::cerr << "Missing positional arguments" << std::endl;
        return false;
    }

    return (i == argc) && settings;
}

int main(int argc, char *argv[]) {
    Settings settings;
    bool help = false;
    if (!parse_args(argc, argv, settings, help)) {
        return 1;
    }

    if (help) {
        print_help(std::cout, argv[0]);
        return 0;
    }

    // create the top level directory
    bool exists = true;
    if (!create_path(settings.top, exists)) {
        std::cerr << "Could not create directory " << settings.top << std::endl;
        return 1;
    }

    // create the top level database file
    const std::string full_name = settings.top + settings.path_separator + settings.db_name;
    sqlite3 *top = open_and_create_tables(full_name);
    if (!top) {
        std::cerr << "Could not open " << full_name << std::endl;
        return 1;
    }

    // do not add files to this layer
    sqlite3_close(top);

    // start up thread pool
    threadpool pool = thpool_init(settings.thread_count);
    if (thpool_null(pool)) {
        std::cerr << "Failed to create thread pool" << std::endl;
        return 1;
    }

    ThreadArgs::settings = &settings;
    ThreadArgs::pool = &pool;

    // use this rng to generate seeds for other rngs
    std::default_random_engine seed_gen(std::chrono::high_resolution_clock::now().time_since_epoch().count());
    std::uniform_int_distribution <std::size_t> seed_rng(0, -1);

    // generate file distribution by user
    std::vector <std::size_t> file_counts(settings.users);
    distribute_total_randomly(file_counts, settings.files, seed_gen, seed_rng);

    // rng for depth selection
    std::default_random_engine depth_gen(seed_rng(seed_gen));
    std::uniform_int_distribution <std::size_t> depth_rng(settings.min_depth, settings.max_depth);

    // inode current subdirectory starts at
    std::size_t inode = 0;

    // push each subdirectory generation into the thread pool
    for(std::size_t uid = 0; uid < settings.users; uid++) {
        ThreadArgs *args = new ThreadArgs;
        args->target_depth = depth_rng(depth_gen);
        args->uid = uid;
        args->index = uid;
        args->seed = seed_rng(seed_gen);
        args->inode = inode;
        args->files = file_counts[uid];
        args->directory = settings.top;

        // next directory starts with an new set of inodes
        inode += args->files + args->target_depth;

        thpool_add_work(pool, thread, args);
    }

    thpool_wait(pool);
    thpool_destroy(pool);

    return 0;
}
