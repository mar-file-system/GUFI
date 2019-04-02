#include <atomic>
#include <condition_variable>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <vector>

extern "C" {

#include "bf.h"
#include "structq.h"
#include "utils.h"
#include "dbutils.h"

}

#define MAXLINE MAXPATH+MAXPATH+MAXPATH

static const char templatename[] = "tmp.db";
static off_t templatesize = 0; // this is really a constant that is set at runtime

// basically thread args
struct ThreadWork {
    ThreadWork()
        : id(0),
          queue(),
          mutex(),
          cv()
    {}

    int id;
    // shared_ptr because valgrind doesn't seem to like std::move-ing pointers
    std::list <std::shared_ptr <struct work> > queue;
    std::mutex mutex;
    std::condition_variable cv;
};

typedef std::pair <ThreadWork, std::thread> WorkPair;
typedef std::vector <WorkPair> State;

void parsetowork (char * delim, char * line, struct work * pinwork ) {
    char *p;
    char *q;

    //printf("in parsetowork delim %s line %s\n",delim,line);
    line[strlen(line)-1]= '\0';
    p=line;    q=strstr(p,delim); memset(q, 0, 1); SNPRINTF(pinwork->name,MAXPATH,"%s",p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); SNPRINTF(pinwork->type,2,"%s",p);
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
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); SNPRINTF(pinwork->linkname,MAXPATH,"%s",p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); SNPRINTF(pinwork->xattr,MAXXATTR,"%s",p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); pinwork->crtime=atol(p);
}

// Read ahead to figure out where files under directories start
void scout_function(std::atomic_bool & scouting, const char * filename, State & consumers) {
    // figure out whether or not this function is running
    std::ifstream file(filename, std::ios::binary);
    if (!(scouting = (bool) file)) {
        std::cerr << "Could not open file " << filename << std::endl;
        return;
    }

    int tid = 0;
    char line[MAXLINE];
    char delim[2] = {'\x1e', '\x00'};

    while (file.getline(line, MAXLINE)) {
        // parse
        std::shared_ptr <struct work> work = std::make_shared <struct work> ();
        parsetowork(delim, line, work.get());

        if (work->type[0] == 'd') {
            work->pinode=0;
            work->offset=file.tellg();

            // put on queues
            std::lock_guard <std::mutex> lock(consumers[tid].first.mutex);
            consumers[tid].first.queue.push_back(work);
            consumers[tid].first.cv.notify_all();

            // round robin
            tid++;
            tid %= in.maxthreads;
        }
    }

    scouting = false;

    // release all condition variables
    for(int i = 0; i < in.maxthreads; i++) {
        consumers[i].first.cv.notify_all();
    }

    return;
}

// create the initial database file to copy from
static off_t create_template(const char * templatename) {
     sqlite3 * templatedb = NULL;
     if (sqlite3_open_v2(templatename, &templatedb, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
         fprintf(stderr, "Cannot open database: %s %s rc %d\n", templatename, sqlite3_errmsg(templatedb), sqlite3_errcode(templatedb));
         return -1;
     }

     create_tables(templatename, 4, templatedb);
     closedb(templatedb);

     int templatefd = -1;
     if ((templatefd = open(templatename, O_RDONLY)) == -1) {
         fprintf(stderr, "Could not open template file\n");
         return -1;
     }

     templatesize = lseek(templatefd, 0, SEEK_END);
     close(templatefd);
     if (templatesize == (off_t) -1) {
         fprintf(stderr, "failed to lseek\n");
         return -1;
     }

     return templatesize;
}

// copy the template file instead of creating a new database and new tables for each work item
static int copy_template(const char * src, const char * dst, std::size_t size) {
    // ignore errors here
    const int src_db = open(src, O_RDONLY);
    const int dst_db = open(dst, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRWXG | S_IWGRP | S_IROTH | S_IROTH);
    const ssize_t sf = sendfile(dst_db, src_db, NULL, size);
    lseek(dst_db, 0, SEEK_SET);
    close(src_db);
    close(dst_db);

    if (sf == -1) {
        fprintf(stderr, "Could not copy template file\n");
        return -1;
    }

    return 0;
}

// process the work under one directory (no recursion)
static bool processdir(const std::shared_ptr <struct work> & w, std::ifstream & trace) {
    // create the directory
    dupdir(w.get());

    // create the database name
    char dbname[MAXPATH];
    SNPRINTF(dbname, MAXPATH, "%s/%s/" DBNAME, in.nameto, w->name);

    // copy the template file
    if (copy_template(templatename, dbname, templatesize)) {
        return false;
    }

    // process the work
    sqlite3 * db = nullptr;
    if ((db = opendb(dbname, 4, 0))) {
        // struct sum summary;
        // zeroit(&summary);

        sqlite3_stmt * res = insertdbprep(db, NULL);
        startdb(db);

        // move the trace file to the offet
        trace.seekg(w->offset);

        std::size_t rows = 0;
        while (true) {
            char line[MAXLINE];
            if (!trace.getline(line, MAXLINE)) {
                break;
            }

            struct work row;
            parsetowork(in.delim, line, &row);
            if (row.type[0] != 'd') {
                break;
            }

            // // update summary table
            // sumit(&summary,&row);

            // add row to bulk insert
            insertdbgo(&row,db,res);

            rows++;
            if (rows > 100000) {
                stopdb(db);
                startdb(db);
                rows=0;
            }
        }

        stopdb(db);
        insertdbfin(db, res);
        // insertsumdb(db, w.get(), &summary);

        closedb(db); // don't set to nullptr

        // ignore errors
        chown(dbname, w->statuso.st_uid, w->statuso.st_gid);
        chmod(dbname, w->statuso.st_mode | S_IRUSR);
    }

    return db;
}

// consumer of work
static std::size_t worker_function(std::atomic_bool & scouting, ThreadWork & tw) {
    std::ifstream trace(in.name, std::ios::binary);
    if (!trace) {
        std::cerr << "Could not open " << in.name << ". Thread not running." << std::endl;
        // possibly move queued work into anther thread's queue?
        return 0;
    }

    std::size_t processed = 0;
    while (scouting || tw.queue.size()) {
        std::list <std::shared_ptr <struct work> > dirs;
        {
            // wait for work
            std::unique_lock <std::mutex> lock(tw.mutex);
            while (scouting && !tw.queue.size()) {
                tw.cv.wait(lock);
            }

            if (!scouting && !tw.queue.size()) {
                break;
            }

            // take all work
            dirs = std::move(tw.queue);
            tw.queue.clear();
        }

        // process all work
        for(std::shared_ptr <struct work> const & dir : dirs) {
            processed += processdir(dir, trace);
        }
    }

    return processed;
}

static bool processinit(std::thread & scout, std::atomic_bool & scouting, const char * filename, State & state) {
    scout = std::thread(scout_function, std::ref(scouting), filename, std::ref(state));

    // should probably check if scouting == true here

    for(int i = 0; i < in.maxthreads; i++) {
        state[i].first.id = i;
        state[i].second = std::thread(worker_function, std::ref(scouting), std::ref(state[i].first));
    }

    return true;
}

static void processfin(std::thread & scout, State & state, const bool spawned_threads) {
    scout.join();

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

   if (in.buildindex && !in.nameto[0]) {
      fprintf(stderr, "Building an index '-b' requires a destination dir '-t'.\n");
      return -1;
   }
   if (in.nameto[0] && ! in.buildindex) {
      fprintf(stderr, "Destination dir '-t' found.  Assuming implicit '-b'.\n");
      in.buildindex = 1;        // you're welcome
   }

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
   // printf("GUFI_dir          build GUFI index here (if -b)\n");
   printf("\n");
}

int main(int argc, char * argv[]) {
    int idx = parse_cmd_line(argc, argv, "hHpn:d:xPbo:t:Du", 1, "input_dir", &in);
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

    State state(in.maxthreads);
    std::atomic_bool scouting;
    std::thread scout;

    if (create_template(templatename) == (off_t) -1) {
        std::cerr << "Could not create template file" << std::endl;
        return -1;
    }

    const bool spawned_threads = processinit(scout, scouting, in.name, state);
    processfin(scout, state, spawned_threads);
    remove(templatename);

    return 0;
}
