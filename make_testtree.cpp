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
const std::size_t DEFAULT_LEADING_ZEROS = 0;
const std::string DEFAULT_DB_NAME = "db.db";

const std::size_t DEFAULT_THREAD_COUNT = 10;

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
            max_depth &&
            (max_depth >= min_depth) &&
            thread_count &&
            path_separator &&
            db_name.size() &&
            top.size() &&
            buckets.size();
    }

    std::size_t users;                   // count
    std::size_t files;                   // count

    char        path_separator;          // '/'
    std::size_t min_depth;
    std::size_t max_depth;
    std::size_t leading_zeros;           // for filling file/directory names
    std::string db_name;

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
    std::size_t file_start;              // starting file number
    std::size_t file_end;                // ending file number
    std::string directory;               // directory to put database into
    Settings *settings;
    threadpool *pool;
};

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
bool create_database_file(const std::string &path, const char separator, const std::string &db_name, bool &exists) {
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

    // open database file
    const std::string full_name = path + separator + db_name;
    sqlite3 *db = open_and_create_tables(full_name);
    if (!db) {
        std::cerr << "Could not open " << full_name << std::endl;
        return false;
    }

    sqlite3_close(db);
    return true;
}

// Thread function for handling database file generation
// This function starts out by building the directory it was assigned, along with all subdirectories
// The database file is generated for each directory. Once the target depth has been reached,
// random file metadata is generated and pushed into the database file of the bottom most directory.
void thread(void *args) {
    ThreadArgs *arg = (ThreadArgs *) args;

    // full directory is accumulated here
    std::stringstream s;
    s << arg->directory;

    // generate entire path, including database file at each level
    bool exists = true; // assume the current directory does exist
    for(std::size_t depth = 0; depth < arg->target_depth; depth++) {
        // generate the current directory path
        s << arg->settings->path_separator << "dir" << std::setw(arg->settings->leading_zeros) << std::setfill('0') << arg->index;

        if (!create_database_file(s.str(), arg->settings->path_separator, arg->settings->db_name, exists)) {
            delete arg;
            return;
        }
    }

    // depth has been reached, fill up the database file

    // open the database file (but don't do anything with it until the very end)
    const std::string db_name = s.str() + arg->settings->path_separator + arg->settings->db_name;
    sqlite3 *on_disk = nullptr;
    if (sqlite3_open(db_name.c_str(), &on_disk) != SQLITE_OK) {
        std::cerr << "Could not open " << db_name << ": " << sqlite3_errmsg(on_disk) << std::endl;
        delete arg;
        return;
    }

    // the active database is in memory until insertion is done
    sqlite3 *in_memory = open_and_create_tables(":memory:");
    if (!in_memory) {
        std::cerr << "Could not open in memory database for " << db_name << std::endl;
        sqlite3_close(on_disk);
        delete arg;
        return;
    }

    // prepare to insert entries
    static const std::size_t esqli_len = strlen(esqli);
    sqlite3_stmt *res = nullptr;
    const char *tail = nullptr;
    if (sqlite3_prepare_v2(in_memory, esqli, esqli_len, &res, &tail) != SQLITE_OK) {
        std::cerr << "Could not prepare statement for " << arg->directory << " (" << sqlite3_errmsg(in_memory) << "). Skipping" << std::endl;
        sqlite3_close(on_disk);
        sqlite3_close(in_memory);
        delete arg;
        return;
    }

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

    struct sum summary;
    zeroit(&summary);

    // create file entries and insert them into the database
    for(std::size_t i = arg->file_start; i < arg->file_end; i++) {
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
        insertdbgo(&work, in_memory, res);
    }

    insertdbfin(in_memory, res);

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

    insertsumdb(in_memory, &work, &summary);

    // write the in memory database out to disk
    sqlite3_backup *backup = sqlite3_backup_init(on_disk, "main", in_memory, "main");
    if (backup) {
        (void) sqlite3_backup_step(backup, -1);
        (void) sqlite3_backup_finish(backup);
    }
    else {
        std::cerr << "Could not initiate moving of in memory database into " << db_name << std::endl;
    }

    sqlite3_close(on_disk);
    sqlite3_close(in_memory);
    delete arg;
}

std::ostream &print_help(std::ostream &stream, char *argv0) {
    return stream << "Syntax: " << argv0 << " [options] directory bucket_0 [bucket_1 ... bucket_n]\n"
                  << "\n"
                  << "    Options:\n"
                  << "        --users n               Number of users                        (Default: " << DEFAULT_USERS << ")\n"
                  << "        --files n               Number of files in the tree            (Default: " << DEFAULT_NUM_FILES << ")\n"
                  << "        --path_separator c      Character that separates path sections (Default: " << DEFAULT_PATH_SEPARATOR << ") \n"
                  << "        --min-depth n           Minimum layers of directories          (Default: " << DEFAULT_MIN_DEPTH << ")\n"
                  << "        --max-depth n           Maximum layers of directories          (Default: " << DEFAULT_MAX_DEPTH << ")\n"
                  << "        --leading_zeros n       Number of leading zeros in names       (Default: " << DEFAULT_LEADING_ZEROS <<")\n"
                  << "        --db_name name          Name of each database file             (Default: " << DEFAULT_DB_NAME << ")\n"
                  << "        --threads n             Set the number of threads to use       (Default: " << DEFAULT_THREAD_COUNT << ")\n"
                  << "        --seed n                Value to seed the seed generator       (Default: Seconds since epoch)\n"
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
        else if (args == "--leading_zeros") {
            if (!read_value(argc, argv, i, settings.leading_zeros, "leading zero count")) {
                return false;
            }
        }
        else if (args == "--db_name") {
            if (!read_value(argc, argv, i, settings.db_name, "db name")) {
                return false;
            }
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

// Generate files counts for each user
// file_counts should have been resized to the desired
// number of slots before being passed into this function
//
// The number of files adds up to total_files
// https://stackoverflow.com/a/22381248
void generate_file_distribution(std::vector <std::size_t> &file_counts, const std::size_t total_files,
                                std::default_random_engine &seed_gen, std::uniform_int_distribution <std::size_t > &seed_rng) {
    std::default_random_engine file_count_gen(seed_rng(seed_gen));
    std::uniform_int_distribution <std::size_t> file_count_rng(0, total_files);
    std::size_t sum = 0;

    // assign each user a random number of files
    for(std::size_t &count : file_counts) {
        count = file_count_rng(file_count_gen);
        sum += count;
    }

    const double factor = (double) (total_files - file_counts.size()) / sum;

    // scale the number of files down so that the sum is close to the target number of files
    std::size_t missing = total_files;
    for(std::size_t &count : file_counts) {
        count = ((double) count) * factor + 1;
        missing -= count;
    }

    // place missing counts in random locations
    std::default_random_engine user_gen(seed_rng(seed_gen));
    std::uniform_int_distribution <std::size_t> user_rng(0, file_counts.size());
    for(std::size_t i = 0; i < missing; i++) {
        file_counts[user_rng(user_gen)]++;
    }
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

    // create the top level directory and the database file
    bool exists = true;
    if (!create_database_file(settings.top, settings.path_separator, settings.db_name, exists)) {
        std::cerr << "Could not create database file in " << settings.top << std::endl;
        return 1;
    }

    // use this rng to generate seeds for other rngs
    std::default_random_engine seed_gen(std::chrono::high_resolution_clock::now().time_since_epoch().count());
    std::uniform_int_distribution <std::size_t> seed_rng(0, -1);

    // generate file distribution by user
    std::vector <std::size_t> file_counts(settings.users);
    generate_file_distribution(file_counts, settings.files, seed_gen, seed_rng);

    // rng for depth selection
    std::default_random_engine depth_gen(seed_rng(seed_gen));
    std::uniform_int_distribution <std::size_t> depth_rng(settings.min_depth, settings.max_depth);

    // start up thread pool
    threadpool pool = thpool_init(settings.thread_count);
    if (thpool_null(pool)) {
        std::cerr << "Failed to create thread pool" << std::endl;
        return 1;
    }

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
        args->file_start = 0;
        args->file_end = file_counts[uid];
        args->directory = settings.top;
        args->settings = &settings;
        args->pool = &pool;

        // next directory starts with an new set of inodes
        inode += args->file_end + args->target_depth;

        thpool_add_work(pool, thread, args);
    }

    thpool_wait(pool);
    thpool_destroy(pool);

    return 0;
}
