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

#include "ThreadWork.hpp"

#define MAXLINE MAXPATH+MAXPATH+MAXPATH

static int templatefd = -1;    // this is really a constant that is set at runtime
static off_t templatesize = 0; // this is really a constant that is set at runtime

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

// processdir cleanup
void cleanup(DIR * dir, std::atomic_size_t & queued, State & state) {
    closedir(dir);

    queued--;

    for(int i = 0; i < in.maxthreads; i++) {
        state[i].first.cv.notify_all();
    }
}

// process the work under one directory (no recursion)
// deletes work
bool processdir(struct work & work, State & state, std::atomic_size_t & queued, std::size_t & next_queue) {
    DIR * dir = opendir(work.name);
    if (!dir) {
        cleanup(dir, queued, state);
        return false;
    }

    // get source directory info
    struct stat dir_st;
    if (lstat(work.name, &dir_st) < 0)  {
        cleanup(dir, queued, state);
        return false;
    }

    // create the directory
    char topath[MAXPATH];
    SNPRINTF(topath, MAXPATH, "%s/%s", in.nameto, work.name);
    int rc = mkdir(topath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH); // don't need recursion because parent is guaranteed to exist
    if (rc < 0) {
        const int err = errno;
        if (err != EEXIST) {
            fprintf(stderr, "mkdir %s failure: %d %s\n", topath, err, strerror(err));
            cleanup(dir, queued, state);
            return false;
        }
    }

    // create the database name
    char dbname[MAXPATH];
    SNPRINTF(dbname, MAXPATH, "%s/" DBNAME, topath);

    // copy the template file
    if (copy_template(templatefd, dbname, templatesize, dir_st.st_uid, dir_st.st_gid)) {
        cleanup(dir, queued, state);
        return false;
    }

    sqlite3 * db = opendb(dbname);
    if (!db) {
        cleanup(dir, queued, state);
        return false;
    }

    // prepare to insert into the database
    struct sum summary;
    zeroit(&summary);

    sqlite3_stmt * res = insertdbprep(db, NULL);

    startdb(db);

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

        // update summary table
        sumit(&summary, &e);

        // add entry into bulk insert
        insertdbgo(&e, db, res);
    }

    stopdb(db);
    insertdbfin(db, res);
    insertsumdb(db, &work, &summary);
    closedb(db);
    db = NULL;

    // ignore errors
    chmod(work.name, work.statuso.st_mode);
    chown(work.name, work.statuso.st_uid, work.statuso.st_gid);

    cleanup(dir, queued, state);

    return true;
}

// consumer of work
std::size_t worker_function(std::atomic_size_t & queued, State & state, ThreadWork <struct work> & tw) {
    std::size_t next_queue = tw.id;

    std::size_t processed = 0;
    while (queued || tw.queue.size()) {
        std::list <struct work> dirs;
        {
            std::unique_lock <std::mutex> lock(tw.mutex);

            // wait for work
            while (queued && !tw.queue.size()) {
                tw.cv.wait(lock);
            }

            if (!tw.queue.size()) {
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
            processed += processdir(dir, state, queued, next_queue);
        }
    }

    return processed;
}

bool processinit(std::atomic_size_t & queued, State & state) {
    // check if the privided path is accessible
    if (access(in.name, R_OK | X_OK)) {
        fprintf(stderr, "couldn't access input dir '%s': %s\n",
                in.name, strerror(errno));
        return 1;
    }

    // get the path's information
    struct work root;
    if (lstat(in.name, &root.statuso) < 0) {
        fprintf(stderr,"could not lstat '%s'\n", in.name);
        return 1;
    }

    if (!S_ISDIR(root.statuso.st_mode) ) {
        fprintf(stderr,"input-dir '%s' is not a directory\n", in.name);
        return 1;
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
        state[i].second = std::thread(worker_function, std::ref(queued), std::ref(state), std::ref(state[i].first));
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
   if (!in.nameto[0]) {
      fprintf(stderr, "No GUFI-tree specified (-t).\n");
      return -1;
   }

   struct stat src_st;
   if (lstat(in.name, &src_st) < 0) {
      fprintf(stderr, "Could not stat source directory \"%s\"\n", in.name);
      return -1;
   }

   if (!S_ISDIR(src_st.st_mode)) {
     fprintf(stderr, "Source path is not a directory \"%s\"\n", in.name);
     return -1;
   }

   // check if the destination path already exists (not an error)
   struct stat dst_st;
   if (lstat(in.nameto, &dst_st) == 0) {
      fprintf(stderr, "\"%s\"Already exists!\n", in.nameto);

      // if the destination path is not a directory, error
      if (!S_ISDIR(dst_st.st_mode)) {
          fprintf(stderr, "Destination path is not a directory \"%s\"\n", in.nameto);
          return -1;
      }
   }

   return 0;
}

void sub_help() {
   printf("input_dir         walk this tree to produce GUFI-tree\n");
   // printf("GUFI_dir          build GUFI index here (if -b)\n");
   printf("\n");
}

int main(int argc, char * argv[]) {
    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();

    int idx = parse_cmd_line(argc, argv, "hHpn:d:t:", 1, "input_dir", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        // parse positional args, following the options
        int retval = 0;
        INSTALL_STR(in.name,   argv[idx++], MAXPATH, "input_dir");
        // INSTALL_STR(in.nameto, argv[idx++], MAXPATH, "to_dir");

        if (retval)
            return retval;
    }
    if (validate_inputs())
        return -1;

    char root[MAXPATH];
    SNPRINTF(root, MAXPATH, "%s/%s", in.nameto, in.name);

    struct stat st;
    if (lstat(in.name, &st) < 0) {
        fprintf(stderr, "Could not stat directory \"%s\"\n", in.name);
        return -1;
    }

    // create the source root under the destination directory using
    // the source directory's permissions and owners
    // this allows for the threads to not have to recursively create directories
    if (dupdir(root, &st)) {
        std::cerr << "Could not create " << in.name << " under " << in.nameto << std::endl;
        return -1;
    }

    if ((templatesize = create_template(&templatefd)) == (off_t) -1) {
        std::cerr << "Could not create template file" << std::endl;
        return -1;
    }

    State state(in.maxthreads);
    std::atomic_size_t queued(0); // so long as one directory is queued, there is potential for more subdirectories, so don't stop the thread
    std::vector <std::size_t> processed(in.maxthreads);

    const bool spawned_threads = processinit(queued, state);
    processfin(state, spawned_threads);

    close(templatefd);

    // set top level permissions
    chmod(in.nameto, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    std::cerr << "main finished in " << std::chrono::duration_cast <std::chrono::nanoseconds> (end - start).count() / 1e9 << " seconds"  << std::endl;

    return 0;
}
