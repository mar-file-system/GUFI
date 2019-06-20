#include <atomic>
#include <condition_variable>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <libgen.h>
#include <list>
#include <mutex>
#include <sys/sendfile.h>
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

}

#define MAXLINE MAXPATH+MAXPATH+MAXPATH

static int templatefd = -1;    // this is really a constant that is set at runtime
static off_t templatesize = 0; // this is really a constant that is set at runtime

struct dbwork {								// c++ structure to hold a struct work
    dbwork()
        : w()
    { bzero(&w,sizeof(w)); }

    struct work w;
};

typedef struct dbwork * Row;

Row new_row() {
    return new struct dbwork();
}

void delete_row(Row & row) {
    delete row;
}

struct ThreadWork {							// data needed by all threads
    ThreadWork()
        : id(0),
	  indbd(NULL),
          queue(),
          mutex(),
          cv()
    {}

    int id;
    sqlite3 * indbd;							// holds the SQLite database connection to the input db.
    std::list <Row> queue;						// work queue
    std::mutex mutex;							// mutex for work queue
    std::condition_variable cv;
};

typedef std::pair <ThreadWork, std::thread> WorkPair;
typedef std::vector <WorkPair> State;

size_t dircnt(0);
size_t filecnt(0);
size_t linkcnt(0);
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
std::mutex mutex;							// global mutex for various counters

/**
* Increments a global counter
*
* @param var	the counter to increment
*/
static inline void incr(int & var) {
    #ifdef PRINT_STAGE
    {
        std::lock_guard <std::mutex> lock(mutex);
        var++;
        std::cout << "duping: " << duping << " copying: " << copying << " opening: " << opening << " settings: " << settings << " waiting: " << waiting << " prepping: " << prepping << " begin_transaction: " << bt << " parsing: " << parsing << " inserting: " << inserting << " end_transaction: " << et << " unprep: " << unprep << " closing: " << closing << std::endl;
    }
    #endif
}

/**
* Decrements a global counter
*
* @param var	the counter to decrement
*/
static inline void decr(int & var) {
    #ifdef PRINT_STAGE
    {
        std::lock_guard <std::mutex> lock(mutex);
        var--;
        std::cout << "duping: " << duping << " copying: " << copying << " opening: " << opening << " settings: " << settings << " waiting: " << waiting << " prepping: " << prepping << " begin_transaction: " << bt << " parsing: " << parsing << " inserting: " << inserting << " end_transaction: " << et << " unprep: " << unprep << " closing: " << closing << std::endl;
    }
    #endif
}

/**
* Increments a big global counter
*
* @param var	the counter to increment
* @param amt	the amount to increment by
*/
static inline void incr(size_t & var, size_t & amt) {
    std::lock_guard <std::mutex> lock(mutex);
    var+=amt;

    return;
}

/**
* Populates a work structure from an SQLite database
* row.
*
* @rec		the database cursor row
* @dbrow	a pointer to a Row object,
* 		which contains the work structure
* 		to populate
*/
void parsetowork (sqlite3_stmt * rec, struct dbwork * dbrow) {
    struct work * pinwork = &dbrow->w;					// the work structure to fill
    const char *tc;						// a temporary character pointer, used in initializing

    SNPRINTF(pinwork->type,2,"%s",sqlite3_column_text(rec,0));		// type of record/object
    if (!strncmp(pinwork->type,"r",1) ||!strncmp(pinwork->type,"j",1)) 
      sprintf(pinwork->type,"d");

    pinwork->statuso.st_nlink=sqlite3_column_int64(rec,1);
    pinwork->pinode=sqlite3_column_int64(rec,2);			// parent inode/pobject_id
    pinwork->statuso.st_ino=sqlite3_column_int64(rec,3);		// inode/object_id
    pinwork->statuso.st_uid=sqlite3_column_int64(rec,4);
    pinwork->statuso.st_gid=sqlite3_column_int64(rec,5);
    pinwork->statuso.st_mode=sqlite3_column_int64(rec,6);
    pinwork->statuso.st_ctime=sqlite3_column_int64(rec,7);
    pinwork->statuso.st_atime=sqlite3_column_int64(rec,8);
    pinwork->statuso.st_mtime=sqlite3_column_int64(rec,9);
    SNPRINTF(pinwork->name,MAXPATH,"%s",sqlite3_column_text(rec,10));	// full path - TO FIX: Need to check for paths > MAXPATH? - cds 4/2019

    if (strIsBlank(tc=(const char *)sqlite3_column_text(rec,11))) 	// the linkname, if any
      bzero(pinwork->linkname,1);
    else								// don't seem to have to worry about freeing tc
      SNPRINTF(pinwork->linkname,MAXPATH,"%s",tc);
    
    if (strIsBlank(tc=(const char *)sqlite3_column_text(rec,12))) {	// xattr column - if any
      bzero(pinwork->xattr,1);
      pinwork->xattrs=0;
    } else {
      SNPRINTF(pinwork->xattr,MAXXATTR,"%s",tc);
      pinwork->xattrs=1;
    }

    pinwork->statuso.st_size=sqlite3_column_int64(rec, 13);		// file size
    pinwork->ossint1=sqlite3_column_int64(rec, 14);			// COS id
    pinwork->ossint2=sqlite3_column_int64(rec, 15);			// # of copies
    pinwork->ossint3=sqlite3_column_int64(rec, 16);			// file family
    pinwork->ossint4=sqlite3_column_int64(rec, 17);			// on disk?
    pinwork->suspect=sqlite3_column_int(rec, 18);			// the suspect/change flag
    pinwork->statuso.st_blksize=(pinwork->ossint4)?4096:1048576;	// block size varies, based on if the file is on disk or tape
    pinwork->statuso.st_blocks=(pinwork->statuso.st_size/pinwork->statuso.st_blksize) + 1;

//    pinwork->crtime=atol(p);						// TO FIX: What is this? - cds 4/2019
    return;
}

// copy the template file instead of creating a new database and new tables for each work item
// the ownership and permissions are set too
static int copy_template(const int src_fd, const char * dst, off_t size, uid_t uid, gid_t gid) {
    incr(copying);

    // ignore errors here
    const int src_db = dup(src_fd);
    const int dst_db = open(dst, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    off_t offset = 0;
    const ssize_t sf = sendfile(dst_db, src_db, &offset, size);
    fchown(dst_db, uid, gid);
    close(src_db);
    close(dst_db);

    if (sf == -1) {
        fprintf(stderr, "Could not copy template file to %s\n", dst);
        return -1;
    }

    decr(copying);
    return 0;
}

// Read ahead to figure out where files under directories start
void scout_function(std::atomic_bool & scouting, const char * filename, State & consumers, ThreadWork & tw) {
    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
    int tid = 0;								// index for the consumers vector
    char startsql[MAXSQL];							// buffer to hold the SQL query
    sqlite3_stmt    *res;							// holds the current record from the result set
    const char      *tail;
    std::size_t dir_count=0;							// counter for # of directories in input database

    	// get all of the directories
    sprintf(startsql,"select type,nlink,pinode,inode,uid,gid,mode,ctime,atime,mtime,name,linkname,xattr,size,mcos,copies,family,disk,ischanged from nsbf where type in ('r','d','j');");
    if(sqlite3_prepare_v2(tw.indbd, startsql, MAXSQL, &res, &tail) != SQLITE_OK) {
      fprintf(stderr, "scout_function(): SQL error on preparing startup query: %s err %s\n",startsql,sqlite3_errmsg(tw.indbd));
      scouting = false;
      return;
    }

	// process result set - send all work to worker threads
    while (sqlite3_step(res) == SQLITE_ROW) {
      Row next = new_row();

      incr(parsing);
      parsetowork(res,next);							// populate work structure from current record
      decr(parsing);

        // push directories onto queues
      if (next->w.type[0] == 'd') {						// verify we have a directory object!
        dir_count++;

        std::lock_guard <std::mutex> lock(consumers[tid].first.mutex);		// lock worker's queue mutex
        consumers[tid].first.queue.emplace_back(std::move(next));		// put on worker's queue
        consumers[tid].first.cv.notify_all();					// wake worker

        tid++; tid %= in.maxthreads;						// use round robin ...
     }
     else
        fprintf(stderr, "scout_function(): Encountered non-directory type '%c' (%s)\n", next->w.type[0], next->w.name);
    }
    sqlite3_finalize(res);							// clean up cursor

    scouting = false;								// we are done scouting!

	// release all condition variables
    for(int i = 0; i < in.maxthreads; i++) {
        consumers[i].first.cv.notify_all();
    }

    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();

    std::lock_guard <std::mutex> lock(mutex);
    std::cerr << "Scout finished in " << std::chrono::duration_cast <std::chrono::nanoseconds> (end - start).count() / 1e9 << " seconds"  << std::endl
              << "Dirs:  " << dir_count << std::endl;

    return;
}

static sqlite3 * opendb(const char *name)
{
    char dbn[MAXPATH];
    snprintf(dbn, MAXSQL, "%s", name);

    incr(opening);
    sqlite3 * db = NULL;
    if (sqlite3_open_v2(dbn, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s %s rc %d\n", dbn, sqlite3_errmsg(db), sqlite3_errcode(db));
    }
    decr(opening);

    incr(settings);
    // try to turn sychronization off
    if (sqlite3_exec(db, "PRAGMA synchronous = OFF", NULL, NULL, NULL) != SQLITE_OK)
      fprintf(stderr, "Could not turn off synchronization for %s %s rc %d\n", dbn, sqlite3_errmsg(db), sqlite3_errcode(db));

    // try to turn journaling off
    if (sqlite3_exec(db, "PRAGMA journal_mode = OFF", NULL, NULL, NULL) != SQLITE_OK) 
      fprintf(stderr, "Could not turn off journaling for %s %s rc %d\n", dbn, sqlite3_errmsg(db), sqlite3_errcode(db));

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
// also deletes r
static bool processdir(Row & r, ThreadWork & tw) {
    if (!r) return false;						// might want to skip this check

//    pid_t mytid = gettid();						// the Thread ID - get it so we can use for error messages/printing
    pid_t mytid = pthread_self();					// the Thread ID - get it so we can use for error messages/printing
    struct work dir=r->w;						// get the work structure passed from the thread
    char dbname[MAXPATH];						// string to hold the database name for this directory - and other temporary names
    char myinsql[MAXSQL];						// SQL buffer for querying the input SQLite3 database
    sqlite3 *db;							// a pointer a SQLLite database connection for the current directory
    sqlite3_stmt    *resi;
    const char      *taili;
    int    error = 0;							// holds any error codes - typically system errno
    size_t file_count = 0, link_count = 0;				// local counters
    struct stat st;							// a temporary stat structure to test database existance

    if (in.buildindex > 0) {						// build the GUFI index tree
      char topath[MAXPATH];
	// create the directory
      SNPRINTF(topath,MAXPATH,"%s/%s",in.nameto,dir.name);
      incr(duping);
      if((error=dupdir(topath,&dir.statuso)) && error != EEXIST) {	// make the directory specified in dir in the GUFI tree. dupdir() should take a work structure!
	 char eStr[512];						// note: In multi-threaded algorithm, it is OK for the directory to have already been created

	 strerror_r(error,eStr,512);					// look up errno
         fprintf(stderr,"Thread[0x%x] processdir(): Failed to make %s%s. errno=%d (%s)\n",
			mytid,in.nameto,dir.name,error,eStr);
         delete_row(r);
	 return false;							// nothing else to do at this point ...
      }
      decr(duping);

      SNPRINTF(dbname, MAXPATH, "%s%s", in.nameto, dir.name);
      if(stat(dbname,&st)) {
        error = errno;
        fprintf(stderr,"Thread[0x%x] processdir(): Error doing verify stat of %s (errno %d)\n",mytid,dbname,error);
        return false;
      }
      if(st.st_uid != dir.statuso.st_uid || st.st_gid != dir.statuso.st_gid)
        chown(dbname,dir.statuso.st_uid,dir.statuso.st_gid);		// make sure owner/group correct for directory. May have been created via mkpath()!

      if(!dir.suspect) {						// nothing in the directory has changed -> no need to do further processing
        delete_row(r);
        return true;
      }

	// create the database - only if NOT suspect!
      SNPRINTF(dbname, MAXPATH, "%s%s/%s", in.nameto, dir.name, DBNAME);
      if(!stat(dbname,&st)) {						// note: However the database file should NOT exist!
        fprintf(stderr,"Thread[0x%x] processdir(): Database (dir inode %ld) %s exists!\n",mytid,dir.statuso.st_ino,dbname);
        return false;
      }
      else if(errno != ENOENT) {
        error = errno;
        fprintf(stderr,"Thread[0x%x] processdir(): Error stating %s (errno %d)\n",mytid,dbname,error);
        return false;
      }
      if (copy_template(templatefd, dbname, templatesize, dir.statuso.st_uid, dir.statuso.st_gid)) {
        delete_row(r);
        return false;							// problems creating the empty database
      }
    }

	// do query against the input database
    sprintf(myinsql,"select type,nlink,pinode,inode,uid,gid,mode,ctime,atime,mtime,name,linkname,xattr,size,mcos,copies,family,disk,ischanged from nsbf where pinode=%ld;",dir.statuso.st_ino);
    if (sqlite3_prepare_v2(tw.indbd, myinsql, MAXSQL, &resi, &taili) != SQLITE_OK) {
      fprintf(stderr, "Thread[0x%x] SQL error on preparing loop query: %s err %s\n",mytid,myinsql,sqlite3_errmsg(tw.indbd));
        delete_row(r);
        return false;							// problems running SQL against the input database
     }

    // process the results of the query
    if ((db = opendb(dbname))) {						// if we are able to open the current database ...
      struct sum summary;							// summary structure
      std::size_t rows = 0;							// row counter
      incr(prepping);
      sqlite3_stmt * res = insertdbprep(db, NULL);
      decr(prepping);

      incr(bt);
      startdb(db);								// start entries insert transaction
      decr(bt);
      zeroit(&summary);								// prep the summary structure

      while (sqlite3_step(resi) == SQLITE_ROW) {
        Row row = new_row();							// row buffer

        incr(parsing);
        parsetowork(resi, row);
        decr(parsing);

        if (row->w.type[0] != 'd') {						// ignore directories at this point
          if(row->w.type[0] == 'f') file_count++;
          if(row->w.type[0] == 'l') link_count++;

          if (in.printing > 0) {						// if printing -> print out row
            printits(&(row->w),mytid);
          }
          if (in.buildindex > 0) {
            sumit(&summary,&(row->w));						// update summary table
            incr(inserting);
            insertdbgo(&(row->w),db,res);					// add row to bulk insert
            decr(inserting);

            rows++;
            if (rows > 100000) {						// insert 100,000 files/links at a time
                stopdb(db);
                startdb(db);
                rows=0;
            }
          } //end building index
        } // end not a directory
      } // end of row loop
      sqlite3_finalize(resi);							// clean up cursor

      incr(et);
      stopdb(db);								// finish entries insert
      decr(et);
      incr(unprep);
      insertdbfin(db, res);
      decr(unprep);

      insertsumdb(db, &dir, &summary);						// this i believe has to be after we close off the entries transaction
      incr(closing);
      closedb(db);								// don't set to nullptr
      decr(closing);
    } // end open database

    if(file_count) incr(filecnt,file_count);					// update counters as needed
    if(link_count) incr(linkcnt,link_count);
    delete_row(r);
    return true;
}

// consumer of work
static std::size_t worker_function(std::atomic_bool & scouting, ThreadWork & tw) {
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
            processed += (size_t)processdir(dir, tw);
        }

        // #ifdef DEBUG
        // std::chrono::high_resolution_clock::time_point process_end = std::chrono::high_resolution_clock::now();
        // std::cerr << "Thread " << tw.id << " processed for " << std::chrono::duration_cast <std::chrono::nanoseconds> (process_end - process_start).count() / 1e9 << " seconds." << std::endl;
        // #endif
    }

    if(processed) incr(dircnt,processed);
    return processed;
}

/**
* Initializes the threads used in the treewalk.
*
* @param scout	the scouting (or work-assigment) thread
* @param scouting	a flag to indicate that scouting is happening
* @param srcname	the name of the source directory for the GUFI tree -
* 			specifically the directory where the SQLite3
* 			database is located (named <srcname>/db.db)
* @param state		a vector containing the processing threads
*
* @returns TRUE if all threads are initialized. FALSE otherwise.
*/
static bool processinit(WorkPair & scout, std::atomic_bool & scouting, const char * srcname, State & state) {
    scouting = true;

    for(int i = 0; i < in.maxthreads; i++) {
        state[i].first.id = i;
//	state[i].first.indbd = opendb(srcname,5,0);			// open the connection to the input database for each thread
	state[i].first.indbd = opendb(srcname);				// open the connection to the input database for each thread
        state[i].second = std::thread(worker_function, std::ref(scouting), std::ref(state[i].first));
    }

    scout.first.id = in.maxthreads +1;
//    scout.first.indbd = opendb(srcname,5,0);				// open the connection to the input database for scout
    scout.first.indbd = opendb(srcname);				// open the connection to the input database for scout
    scout.second = std::thread(scout_function, std::ref(scouting), srcname, std::ref(state), std::ref(scout.first));

    return true;
}

static void processfin(WorkPair & scout, State & state, const bool spawned_threads) {
    scout.second.join();
    closedb(scout.first.indbd);						// close the connection to the input database for the scout thread

    if (spawned_threads) {
        for(WorkPair & wp : state) {
            wp.second.join();
	    closedb(wp.first.indbd);					// close the connection to the input database
        }
    }
    return;
}

// create the initial database file to copy from
static off_t create_template(int & fd) {
    static const char name[] = "tmp.db";

    sqlite3 * db = nullptr;
    if (sqlite3_open_v2(name, &db, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
        fprintf(stderr, "Cannot create template database: %s %s rc %d\n", name, sqlite3_errmsg(db), sqlite3_errcode(db));
        return -1;
    }

    create_tables(name, 4, db);
    closedb(db);

    if ((fd = open(name, O_RDONLY)) == -1) {
        fprintf(stderr, "Could not open template file\n");
        return -1;
    }

    // no need for the file to remain on the filesystem
    remove(name);

    return lseek(fd, 0, SEEK_END);
}

// This app allows users to do any of the following: (a) just walk the db.db hpss ns bf input
// (b) like a, but also creating corresponding GUFI-tree
// directories
int validate_inputs() {
   if (in.buildindex && !in.nameto[0]) {
      fprintf(stderr, "Building a GUFI index '-b' requires a GUFI index tree '-t'.\n");
      return -1;
   }
   if (in.nameto[0] && ! in.buildindex) {
      fprintf(stderr, "GUFI index tree '-t' found.  Assuming implicit '-b'.\n");
      in.buildindex = 1;                                                                        // you're welcome
   }

   if (! in.nameto[0])                                                                          // not errors, but you might want to know ...
      fprintf(stderr, "WARNING: No GUFI index tree specified (-t). No GUFI-tree will be built.\n");
   return 0;
}

/**
* Routine to print additional help with arguments
*/
void sub_help() {
   printf("input_sqlite_db       input database that contains a data extraction from hpss's db2 nsobject and bitfile tables\n");
   printf("\n");
}

/**
* The MAIN routine for BFHI - Breath-first HPSS Input.
*
* @param argc   number of arguments on commandline
* @param argv   the array of strings holding the value of
*               the arguments
*
* @return 0 on success, with no error. Non-zero
*       otherwise.
*/
int main(int argc, char * argv[]) {
    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();

    int idx = parse_cmd_line(argc, argv, "hHpn:d:xPbo:t:Du", 1, "input_sqlite_db", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {											// parse positional args, following the options
        int retval = 0;

        INSTALL_STR(in.name,   argv[idx++], MAXPATH, "input_sqlite3_db");
        if (retval) return retval;
    }
    if (validate_inputs())
        return -1;
    in.doxattrs=1;										// make sure xattrs are gathered when creating a GUFI index
    in.outdb=0;											// using an outdb makes no sense.

    State state(in.maxthreads);
    std::atomic_bool scouting;
    WorkPair scout;										// the scouting thread - assigns work to the State threads

    if ((templatesize = create_template(templatefd)) == (off_t) -1) {
        std::cerr << "Could not create template file" << std::endl;
        return -1;
    }

    const bool spawned_threads = processinit(scout, scouting, in.name, state);
    processfin(scout, state, spawned_threads);

    close(templatefd);

    // set top level permissions
    chmod(in.nameto, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    std::lock_guard <std::mutex> lock(mutex);
    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    std::cerr << "main finished in " << std::chrono::duration_cast <std::chrono::nanoseconds> (end - start).count() / 1e9 << " seconds"  << std::endl
              << "Processed - Dirs:  " << dircnt << "  Files:  " << filecnt << "  Links:  " << linkcnt << std::endl;

    return 0;
}
