#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "dbutils.h"
#include "opendb.h"
#include "template_db.h"
#include "trace.h"
#include "utils.h"

#define MAXLINE MAXPATH+MAXPATH+MAXPATH

int templatefd = -1;    // this is really a constant that is set at runtime
off_t templatesize = 0; // this is really a constant that is set at runtime

#ifdef DEBUG
int duping = 0;
int copying = 0;
int opening = 0;
int settings = 0;
int waiting = 0;
int prepping = 0;
int bt = 0;
int parsing = 0;
int inserting = 0;
int et = 0;
int unprep = 0;
int closing = 0;
pthread_mutex_t counter_mutex = PTHREAD_MUTEX_INITIALIZER;

#ifdef PRINT_STAGE
static inline void print_counters() {
    fprintf(stdout, "duping:    %d ", duping);
    fprintf(stdout, "copying:   %d ", copying);
    fprintf(stdout, "opening:   %d ", opening);
    fprintf(stdout, "settings:  %d ", settings);
    fprintf(stdout, "waiting:   %d ", waiting);
    fprintf(stdout, "waiting:   %d ", waiting);
    fprintf(stdout, "prepping:  %d ", prepping);
    fprintf(stdout, "bt:        %d ", bt);
    fprintf(stdout, "parsing:   %d ", parsing);
    fprintf(stdout, "inserting: %d ", inserting);
    fprintf(stdout, "et:        %d ", et);
    fprintf(stdout, "unprep:    %d ", unprep);
    fprintf(stdout, "closing:   %d ", closing);
    fprintf(stdout, "\n");
}
#endif

static inline void incr(int * var) {
    #ifdef PRINT_STAGE
    {
        pthread_mutex_lock(&counter_mutex);
        (*var)++;
        print_counters();
        pthread_mutex_unlock(&counter_mutex);
    }
    #endif
}

static inline void decr(int * var) {
    #ifdef PRINT_STAGE
    {
        pthread_mutex_lock(&counter_mutex);
        (*var)--;
        print_counters();
        pthread_mutex_unlock(&counter_mutex);
    }
    #endif
}
#endif

// Data stored during first pass of input file
struct row {
    size_t first_delim;
    char * line;
    size_t len;
    long offset;
    size_t entries;
};

struct row * row_init() {
    struct row * row = calloc(1, sizeof(struct row));
    if (row) {
        row->first_delim = -1;
        row->offset = -1;
    }
    return row;
}

void row_destroy(struct row * row) {
    if (row) {
        free(row->line);
        free(row);
    }
}

void parsefirst(const char delim, struct row * work) {
    work->first_delim = 0;
    while ((work->first_delim < work->len) && (work->line[work->first_delim] != delim)) {
        work->first_delim++;
    }

    if (work->first_delim == work->len) {
        work->first_delim = -1;
    }
}

struct scout_args {
    struct QPTPool * ctx;
    const char * filename;
    pthread_mutex_t mutex;
    pthread_cond_t cv;
    int processed;
};

void scout_args_init(struct scout_args * sa, struct QPTPool * ctx, char * filename) {
    if (sa) {
        sa->ctx = ctx;
        sa->filename = filename;
        pthread_mutex_init(&sa->mutex, NULL);
        pthread_cond_init(&sa->cv, NULL);
        sa->processed = 0;
    }
}

void scout_args_destroy(struct scout_args * sa) {
    if (sa) {
        pthread_mutex_destroy(&sa->mutex);
        pthread_cond_destroy(&sa->cv);
    }
}

// Read ahead to figure out where files under directories start
void * scout_function(void * args) {
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);

    struct scout_args * sa = (struct scout_args *) args;
    struct QPTPool * ctx = sa->ctx;
    FILE * trace = fopen(sa->filename, "rb");

    // figure out whether or not this function is running
    if (!trace) {
        fprintf(stderr, "Could not open file %s\n", sa->filename);
        return NULL;;
    }

    // keep current directory while finding next directory
    // in order to find out whether or not the current
    // directory has files in it
    struct row * work = row_init();
    if (getline(&work->line, &work->len, trace) == -1) {
        row_destroy(work);
        fclose(trace);
        return NULL;
    }

    parsefirst(in.delim[0], work);
    work->offset = ftell(trace);

    // int tid = 0;
    size_t file_count = 0;
    size_t dir_count = 1; // always start with a directory
    size_t empty = 0;

    char * line = NULL;
    size_t n = 0;
    while (getline(&line, &n, trace) != -1) {
        struct row * next = row_init();
        next->line = line;
        next->len = n;

        // parse
        parsefirst(in.delim[0], next);

        // push directories onto queues
        if (next->line[next->first_delim + 1] == 'd') {
            dir_count++;

            empty += !work->entries;
            next->offset = ftell(trace);

            // put the previous work on the queue
            QPTPool_enqueue_external(ctx, work);

            if (!sa->processed) {
                pthread_mutex_lock(&sa->mutex);
                sa->processed = 1;
                pthread_cond_broadcast(&sa->cv);
                pthread_mutex_unlock(&sa->mutex);
            }

            work = next;
        }
        else {
            work->entries++;
            file_count++;
            row_destroy(next);
        }

        line = NULL;
        n = 0;
    }
    free(line);

    // insert the last work item
    QPTPool_enqueue_external(ctx, work);

    fclose(trace);

    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);

    // in case nothing was read, force the main thread to continue
    pthread_mutex_lock(&sa->mutex);
    sa->processed = 1;
    pthread_cond_broadcast(&sa->cv);
    pthread_mutex_unlock(&sa->mutex);

    #ifdef DEBUG
    pthread_mutex_lock(&counter_mutex);
    #endif
    fprintf(stderr, "Scout finished in %.2Lf seconds\n", elapsed(&start, &end));
    fprintf(stderr, "Files: %zu\n", file_count);
    fprintf(stderr, "Dirs:  %zu\n", dir_count);
    fprintf(stderr, "Total: %zu\n", file_count + dir_count);
    #ifdef DEBUG
    pthread_mutex_unlock(&counter_mutex);
    #endif

    return NULL;
}

// process the work under one directory (no recursion)
// also deletes data
int processdir(struct QPTPool * ctx, void * data, const size_t id, size_t * next_queue, void * args) {
    // might want to skip this check
    if (!data) {
        return 1;
    }

    if (!ctx || (id >= ctx->size) || !next_queue) {
        free(data);
        return 1;
    }

    struct row * w = (struct row *) data;
    FILE * trace = ((FILE **) args)[id];

    struct work dir;
    memset(&dir, 0, sizeof(struct work));

    // parse the directory data
    #ifdef DEBUG
    incr(&parsing);
    #endif
    linetowork(w->line, in.delim, &dir);
    #ifdef DEBUG
    decr(&parsing);
    #endif

    // create the directory
    #ifdef DEBUG
    incr(&duping);
    #endif
    char topath[MAXPATH];
    SNPRINTF(topath,MAXPATH,"%s/%s",in.nameto,dir.name);
    if (dupdir(topath, &dir.statuso)) {
      const int err = errno;
      fprintf(stderr, "Dupdir failure: %d %s\n", err, strerror(err));
      row_destroy(w);
      return 1;
    }
    #ifdef DEBUG
    decr(&duping);
    #endif

    // create the database name
    char dbname[MAXPATH];
    SNPRINTF(dbname, MAXPATH, "%s/" DBNAME, topath);

    // // don't bother doing anything if there is nothing to insert
    // // (the database file will not exist for empty directories)
    // if (!w->entries) {
    //     row_destroy(w);
    //     return true;
    // }

    // copy the template file
    if (copy_template(templatefd, dbname, templatesize, dir.statuso.st_uid, dir.statuso.st_gid)) {
        row_destroy(w);
        return 1;
    }

    // process the work
    sqlite3 * db = opendb2(dbname, 0, 0, 1);
    if (db) {
        struct sum summary;
        zeroit(&summary);

        #ifdef DEBUG
        incr(&prepping);
        #endif
        sqlite3_stmt * res = insertdbprep(db, NULL);
        #ifdef DEBUG
        decr(&prepping);
        #endif

        #ifdef DEBUG
        incr(&bt);
        #endif
        startdb(db);
        #ifdef DEBUG
        decr(&bt);
        #endif

        // move the trace file to the offet
        fseek(trace, w->offset, SEEK_SET);

        size_t row_count = 0;
        while (1) {
            char * line = NULL;
            size_t n = 0;
            if (getline(&line, &n, trace) == -1) {
                free(line);
                break;
            }

            struct work row;
            memset(&row, 0, sizeof(struct row));
            #ifdef DEBUG
            incr(&parsing);
            #endif
            linetowork(line, in.delim, &row);
            #ifdef DEBUG
            decr(&parsing);
            #endif
            free(line);

            // stop on directories, since files are listed first
            if (row.type[0] == 'd') {
                break;
            }

            // update summary table
            sumit(&summary,&row);

            // dont't record pinode
            row.pinode = 0;

            // add row to bulk insert
            #ifdef DEBUG
            incr(&inserting);
            #endif
            insertdbgo(&row,db,res);
            #ifdef DEBUG
            decr(&inserting);
            #endif

            row_count++;
            if (row_count > 100000) {
                stopdb(db);
                startdb(db);
                row_count=0;
            }
        }

        #ifdef DEBUG
        incr(&et);
        #endif
        stopdb(db);
        #ifdef DEBUG
        decr(&et);
        #endif

        #ifdef DEBUG
        incr(&unprep);
        #endif
        insertdbfin(db, res);
        #ifdef DEBUG
        decr(&unprep);
        #endif
        insertsumdb(db, &dir, &summary);

        #ifdef DEBUG
        incr(&closing);
        #endif
        closedb(db); // don't set to nullptr
        #ifdef DEBUG
        decr(&closing);
        #endif
    }

    row_destroy(w);

    return !db;
}

void sub_help() {
   printf("input_file        parse this trace file to produce GUFI-tree\n");
   printf("output_dir        build GUFI index here\n");
   printf("\n");
}

void close_per_thread_traces(FILE ** traces, const int count) {
    if (traces) {
        for(int i = 0; i < count; i++) {
            fclose(traces[i]);
        }

        free(traces);
    }
}

FILE ** open_per_thread_traces(char * filename, const int count) {
    FILE ** traces = (FILE **) calloc(count, sizeof(FILE *));
    if (traces) {
        for(int i = 0; i < count; i++) {
            if (!(traces[i] = fopen(filename, "rb"))) {
                close_per_thread_traces(traces, i);
                return NULL;
            }
        }
    }
    return traces;
}

int main(int argc, char * argv[]) {
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);

    int idx = parse_cmd_line(argc, argv, "hHn:d:", 1, "input_dir", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        // parse positional args, following the options
        int retval = 0;
        INSTALL_STR(in.name,   argv[idx++], MAXPATH, "input_file");
        INSTALL_STR(in.nameto, argv[idx++], MAXPATH, "output_dir");

        if (retval)
            return retval;
    }

    if ((templatesize = create_template(&templatefd)) == (off_t) -1) {
        fprintf(stderr, "Could not create template file\n");
        return -1;
    }

    // open trace files for threads to jump around in
    // all have to be passed in at once because there's no way to send one to each thread
    // the trace files have to be opened outside of the thread in order to not repeatedly open the files
    FILE ** traces = open_per_thread_traces(in.name, in.maxthreads);
    if (!traces) {
        fprintf(stderr, "Failed to open trace file for each thread\n");
        return -1;
    }

    struct QPTPool * pool = QPTPool_init(in.maxthreads);
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        close_per_thread_traces(traces, in.maxthreads);
        return -1;
    }

    // the scout thread pushes more work into the queue instead of processdir
    pthread_t scout;
    struct scout_args sargs;
    scout_args_init(&sargs, pool, in.name);
    if (pthread_create(&scout, NULL, scout_function, &sargs) != 0) {
        fprintf(stderr, "Failed to start scout thread\n");
        close_per_thread_traces(traces, in.maxthreads);
        scout_args_destroy(&sargs);
        close(templatefd);
        return -1;
    }

    // wait for the scouting function to push at least 1 item onto the queue before starting the threads
    pthread_mutex_lock(&sargs.mutex);
    while (!sargs.processed) {
        pthread_cond_wait(&sargs.cv, &sargs.mutex);
    }
    pthread_mutex_unlock(&sargs.mutex);

    // scouting function errored
    if (sargs.processed == -1) {
        fprintf(stderr, "Scouting error\n");
        QPTPool_destroy(pool);
        close_per_thread_traces(traces, in.maxthreads);
        scout_args_destroy(&sargs);
        close(templatefd);
        return -1;
    }

    // start up threads and start processing
    if (QPTPool_start(pool, processdir, traces) != (size_t) in.maxthreads) {
        fprintf(stderr, "Failed to start all threads\n");
        pthread_join(scout, NULL);
        QPTPool_wait(pool);
        QPTPool_destroy(pool);
        close_per_thread_traces(traces, in.maxthreads);
        scout_args_destroy(&sargs);
        close(templatefd);
        return -1;
    }

    QPTPool_wait(pool);
    QPTPool_destroy(pool);

    pthread_join(scout, NULL);

    // set top level permissions
    chmod(in.nameto, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    close_per_thread_traces(traces, in.maxthreads);
    scout_args_destroy(&sargs);
    close(templatefd);

    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);
    fprintf(stderr, "main finished in %.2Lf seconds\n", elapsed(&start, &end));

    return 0;
}
