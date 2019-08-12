#include <atomic>
#include <condition_variable>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <libgen.h>
#include <list>
#include <mutex>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <vector>
#include <unistd.h>

// #ifdef DEBUG
// #undef DEBUG
// #endif

#ifndef PRINT_STAGE
// #define PRINT_STAGE
#endif

#ifdef DEBUG
#include <chrono>
#endif

extern "C" {

#include "bf.h"
#include "utils.h"
#include "dbutils.h"
#include "template_db.h"
#include "trace.h"

}

#include <ThreadWork.hpp>

#define MAXLINE MAXPATH+MAXPATH+MAXPATH

int templatefd = -1;    // this is really a constant that is set at runtime
off_t templatesize = 0; // this is really a constant that is set at runtime

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

typedef std::pair <ThreadWork <Row>, std::thread> WorkPair;
typedef std::vector <WorkPair> State;

int duping(0);
int copying(0);
int opening(0);
int settings(0);
int waiting(0);
int prepping(0);
int bt(0);
int parsing(0);
int inserting(0);
int et(0);
int unprep(0);
int closing(0);
std::mutex mutex;

static inline void incr(int & var) {
    #ifdef PRINT_STAGE
    {
        std::lock_guard <std::mutex> lock(mutex);
        var++;
        std::cout << "duping: " << duping << " copying: " << copying << " opening: " << opening << " settings: " << settings << " waiting: " << waiting << " prepping: " << prepping << " begin_transaction: " << bt << " parsing: " << parsing << " inserting: " << inserting << " end_transaction: " << et << " unprep: " << unprep << " closing: " << closing << std::endl;
    }
    #endif
}

static inline void decr(int & var) {
    #ifdef PRINT_STAGE
    {
        std::lock_guard <std::mutex> lock(mutex);
        var--;
        std::cout << "duping: " << duping << " copying: " << copying << " opening: " << opening << " settings: " << settings << " waiting: " << waiting << " prepping: " << prepping << " begin_transaction: " << bt << " parsing: " << parsing << " inserting: " << inserting << " end_transaction: " << et << " unprep: " << unprep << " closing: " << closing << std::endl;
    }
    #endif
}

void parsefirst(const char delim, Row & work) {
    work->first_delim = work->line.find(delim);
}

void scout_cleanup(std::atomic_bool & scouting, State & consumers) {
    scouting = false;

    // release all condition variables
    for(int i = 0; i < in.maxthreads; i++) {
        consumers[i].first.cv.notify_all();
    }
}

// Read ahead to figure out where files under directories start
void scout_function(std::atomic_bool & scouting, const char * filename, State & consumers) {
    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();

    // figure out whether or not this function is running
    std::ifstream file(filename, std::ios::binary);
    if (!(scouting = (bool) file)) {
        std::cerr << "Could not open file " << filename << std::endl;
        scout_cleanup(scouting, consumers);
        return;
    }

    // keep current directory while finding next directory
    // in order to find out whether or not the current
    // directory has files in it
    Row work = new_row();
    if (!std::getline(file, work->line)) {
        delete_row(work);
        scout_cleanup(scouting, consumers);
        return;
    }

    parsefirst(in.delim[0], work);
    work->offset = file.tellg();

    int tid = 0;
    std::size_t file_count = 0;
    std::size_t dir_count = 1; // always start with a directory
    std::size_t empty = 0;

    std::string line;
    while (std::getline(file, line)) {
        Row next = new_row();
        next->line = std::move(line);

        // parse
        parsefirst(in.delim[0], next);

        // push directories onto queues
        if (next->line[next->first_delim + 1] == 'd') {
            dir_count++;

            empty += !work -> entries;
            next->offset = file.tellg();

            // put the previous work on the queue
            {
                std::lock_guard <std::mutex> lock(consumers[tid].first.mutex);
                consumers[tid].first.queue.emplace_back(std::move(work));
                consumers[tid].first.cv.notify_all();
            }

            // round robin
            tid++;
            tid %= in.maxthreads;

            work = next;
        }
        else {
            work->entries++;
            file_count++;
            delete_row(next);
        }
    }

    // insert the last work item
    {
        empty += !work -> entries;
        std::lock_guard <std::mutex> lock(consumers[tid].first.mutex);
        consumers[tid].first.queue.emplace_back(std::move(work));
        consumers[tid].first.cv.notify_all();
    }

    scout_cleanup(scouting, consumers);

    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();

    std::lock_guard <std::mutex> lock(mutex);
    std::cerr << "Scout finished in " << std::chrono::duration_cast <std::chrono::nanoseconds> (end - start).count() / 1e9 << " seconds"  << std::endl
              << "Files: " << file_count << std::endl
              << "Dirs:  " << dir_count << " (" << empty << " empty)" << std::endl
              << "Total: " << file_count + dir_count << std::endl;

    return;
}

sqlite3 * opendb(const char *name)
{
    incr(opening);
    sqlite3 * db = NULL;
    // no need to create because the file should already exist
    if (sqlite3_open_v2(name, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, "unix-none") != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db));
        decr(opening);
        return NULL;
    }
    decr(opening);

    incr(settings);
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
    decr(settings);

    return db;
}

// process the work under one directory (no recursion)
// also deletes w
bool processdir(Row & w, std::ifstream & trace) {
    // might want to skip this check
    if (!w) {
        return false;
    }

    // parse the directory data
    incr(parsing);
    struct work dir;
    char line[MAXLINE] = {0};
    memcpy(line, w->line.c_str(), w->line.size());
    linetowork(line, in.delim, &dir);
    decr(parsing);

    // create the directory
    incr(duping);
    char topath[MAXPATH];
    SNPRINTF(topath,MAXPATH,"%s/%s",in.nameto,dir.name);
    if (dupdir(topath, &dir.statuso)) {
      const int err = errno;
      fprintf(stderr, "Dupdir failure: %d %s\n", err, strerror(err));
      delete_row(w);
      return false;
    }
    decr(duping);

    // create the database name
    char dbname[MAXPATH];
    SNPRINTF(dbname, MAXPATH, "%s/" DBNAME, topath);

    // // don't bother doing anything if there is nothing to insert
    // // (the database file will not exist for empty directories)
    // if (!w->entries) {
    //     delete_row(w);
    //     return true;
    // }

    // copy the template file
    if (copy_template(templatefd, dbname, templatesize, dir.statuso.st_uid, dir.statuso.st_gid)) {
        delete_row(w);
        return false;
    }

    // process the work
    sqlite3 * db = opendb(dbname);
    if (db) {
        struct sum summary;
        zeroit(&summary);

        incr(prepping);
        sqlite3_stmt * res = insertdbprep(db, NULL);
        decr(prepping);

        incr(bt);
        startdb(db);
        decr(bt);

        // move the trace file to the offet
        trace.seekg(w->offset);

        std::size_t row_count = 0;
        while (true) {
            char line[MAXLINE];
            if (!trace.getline(line, MAXLINE)) {
                break;
            }

            incr(parsing);
            struct work row;
            linetowork(line, in.delim, &row);
            decr(parsing);

            // stop on directories, since files are listed first
            if (row.type[0] == 'd') {
                break;
            }

            // update summary table
            sumit(&summary,&row);

            // dont't record pinode
            row.pinode = 0;

            // add row to bulk insert
            incr(inserting);
            insertdbgo(&row,db,res);
            decr(inserting);

            row_count++;
            if (row_count > 100000) {
                stopdb(db);
                startdb(db);
                row_count=0;
            }
        }

        incr(et);
        stopdb(db);
        decr(et);

        incr(unprep);
        insertdbfin(db, res);
        decr(unprep);

        insertsumdb(db, &dir, &summary);

        incr(closing);
        closedb(db); // don't set to nullptr
        decr(closing);
    }

    delete_row(w);

    return db;
}

// consumer of work
std::size_t worker_function(std::atomic_bool & scouting, ThreadWork <Row> & tw) {
    std::ifstream trace(in.name, std::ios::binary);
    if (!trace) {
        std::cerr << "Could not open " << in.name << ". Thread not running." << std::endl;
        // possibly move queued work into anther thread's queue?
        return 0;
    }

    std::size_t processed = 0;
    while (scouting || tw.queue.size()) {
        std::list <Row> dirs;
        {
            // #ifdef DEBUG
            // std::chrono::high_resolution_clock::time_point wait_start = std::chrono::high_resolution_clock::now();
            // #endif

            incr(waiting);

            std::unique_lock <std::mutex> lock(tw.mutex);

            // wait for work
            while (scouting && !tw.queue.size()) {
                tw.cv.wait(lock);
            }

            decr(waiting);

            // #ifdef DEBUG
            // std::chrono::high_resolution_clock::time_point wait_end = std::chrono::high_resolution_clock::now();
            // std::cerr << "Thread " << tw.id << " waited for " << std::chrono::duration_cast <std::chrono::nanoseconds> (wait_end - wait_start).count() / 1e9 << " seconds. Queued: " << tw.queue.size() << std::endl;
            // #endif

            if (!scouting && !tw.queue.size()) {
                break;
            }

            // // take single work item
            // dirs.push_back(tw.queue.front());
            // tw.queue.pop_front();

            // take all work
            dirs = std::move(tw.queue);
            tw.queue.clear();
        }

        // #ifdef DEBUG
        // std::chrono::high_resolution_clock::time_point process_start = std::chrono::high_resolution_clock::now();
        // #endif

        // process all work
        for(Row & dir : dirs) {
            processed += processdir(dir, trace);
        }

        // #ifdef DEBUG
        // std::chrono::high_resolution_clock::time_point process_end = std::chrono::high_resolution_clock::now();
        // std::cerr << "Thread " << tw.id << " processed for " << std::chrono::duration_cast <std::chrono::nanoseconds> (process_end - process_start).count() / 1e9 << " seconds." << std::endl;
        // #endif
    }

    return processed;
}

bool processinit(std::thread & scout, std::atomic_bool & scouting, const char * filename, State & state) {
    scouting = true;

    for(int i = 0; i < in.maxthreads; i++) {
        state[i].first.id = i;
        state[i].second = std::thread(worker_function, std::ref(scouting), std::ref(state[i].first));
    }

    scout = std::thread(scout_function, std::ref(scouting), filename, std::ref(state));

    return true;
}

void processfin(std::thread & scout, State & state, const bool spawned_threads) {
    scout.join();

    if (spawned_threads) {
        for(int i = 0; i < in.maxthreads; i++) {
            state[i].second.join();
        }
    }
}

// This app allows users to do any of the following: (a) just walk the
// input tree, (b) like a, but also creating corresponding GUFI-tree
// directories, (c) like b, but also creating an index.
int validate_inputs() {
   char expathin[MAXPATH];
   char expathout[MAXPATH];
   char expathtst[MAXPATH];

   SNPRINTF(expathtst,MAXPATH,"%s/%s",in.nameto,in.name);
   realpath(expathtst,expathout);
   //printf("expathtst: %s expathout %s\n",expathtst,expathout);
   realpath(in.name,expathin);
   //printf("in.name: %s expathin %s\n",in.name,expathin);
   if (!strcmp(expathin,expathout)) {
     fprintf(stderr,"You are putting the index dbs in input directory\n");
     in.buildinindir = 1;
   }

   // not errors, but you might want to know ...
   if (! in.nameto[0])
      fprintf(stderr, "WARNING: No GUFI-tree specified (-t).  No GUFI-tree will be built.\n");
   //   else if (! in.buildindex)
   //      fprintf(stderr, "WARNING: Index-building not requested (-b).  No GUFI index will be built.\n");

   return 0;
}

void sub_help() {
   printf("input_dir         walk this tree to produce GUFI-tree\n");
   printf("output_dir        build GUFI index here\n");
   printf("\n");
}

int main(int argc, char * argv[]) {
    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();

    int idx = parse_cmd_line(argc, argv, "hHpn:d:", 1, "input_dir", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        // parse positional args, following the options
        int retval = 0;
        INSTALL_STR(in.name,   argv[idx++], MAXPATH, "input_dir");
        INSTALL_STR(in.nameto, argv[idx++], MAXPATH, "output_dir");

        if (retval)
            return retval;
    }
    if (validate_inputs())
        return -1;

    State state(in.maxthreads);
    std::atomic_bool scouting;
    std::thread scout;

    if ((templatesize = create_template(&templatefd)) == (off_t) -1) {
        std::cerr << "Could not create template file" << std::endl;
        return -1;
    }

    const bool spawned_threads = processinit(scout, scouting, in.name, state);
    processfin(scout, state, spawned_threads);

    close(templatefd);

    // set top level permissions
    chmod(in.nameto, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    std::cerr << "main finished in " << std::chrono::duration_cast <std::chrono::nanoseconds> (end - start).count() / 1e9 << " seconds"  << std::endl;

    return 0;
}
