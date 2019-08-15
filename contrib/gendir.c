/*
This code generates directories in a well defined manner. Starting
at the root directory, the provided fixed number subdirectories
and files are generated at every directory. At the bottom level, the
subdirectories are empty.

Each directory has the name d.<number>, where number is in [0, # of directories - 1].
Each file has the name f.<number>, where number is in [0, # of files - 1].

The number of directories there would be if each directory spawned 1 subdirectory is

             depth - 1
                ---
           s =  \   # of directories ** i = ((# of directories ** depth) - 1) / (# of directories - 1)
                /
                ---
               i = 0

The number of directories generated is

           # of directories * s + 1

The number of files generated is

           # of files * s
*/

#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "utils.h"

extern int errno;

#define DIR_PERMS (S_IRWXU | S_IRWXG | S_IRWXO)

// constants to pass to threads
struct settings {
    size_t dirs;
    size_t files;
    size_t max_level;
};

// work for each thread
struct dir {
    char name[MAXPATH];
    size_t current_level;
};

int generate_level(struct QPTPool * ctx, void * data, const size_t id, void * args) {
    struct settings * settings = (struct settings *) args;
    struct dir * dir = (struct dir *) data;

    if (dir->current_level >= settings->max_level) {
        free(dir);
        return 0;
    }

    // create the subdirectories in this directory
    for(size_t i = 0; i < settings->dirs; i++) {
        struct dir * subdir = calloc(1, sizeof(struct dir));
        if (!subdir) {
            continue;
        }

        SNPRINTF(subdir->name, MAXPATH, "%s/d.%zu", dir->name, i);

        // no need to recursively make upper directories because they have already been created
        if (mkdir(subdir->name, DIR_PERMS) < 0) {
            if (errno != EEXIST) {
                fprintf(stderr, "mkdir failed for %s: %d %s\n", subdir->name, errno, strerror(errno));
                free(subdir);
                continue;
            }
        }

        subdir->current_level = dir->current_level + 1;

        // recurse down by placing subdirectories onto queue
        QPTPool_enqueue(ctx, id, subdir);
    }

    // create the files in this directory
    char name[MAXPATH];
    for(size_t i = 0; i < settings->files; i++) {
        SNPRINTF(name, MAXPATH, "%s/f.%zu", dir->name, i);

        // overwrite any existing data
        const int fd = open(name, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
        if (fd < 0) {
            fprintf(stderr, "open failed for %s: %d %s\n", name, errno, strerror(errno));
        }
        close(fd);
    }

    free(dir);
    return 0;
}

int main(int argc, char * argv[]) {
    if (argc < 5) {
        fprintf(stderr, "Syntax: %s output_dir directories files depth [threads=1]\n", argv[0]);
        return 1;
    }

    struct settings settings;

    if (sscanf(argv[2], "%zu", &settings.dirs) != 1) {
        fprintf(stderr, "Bad directory count: %s\n", argv[2]);
        return 1;
    }

    if (sscanf(argv[3], "%zu", &settings.files) != 1) {
        fprintf(stderr, "Bad file_count count: %s\n", argv[3]);
        return 1;
    }

    if (sscanf(argv[4], "%zu", &settings.max_level) != 1) {
        fprintf(stderr, "Bad depth: %s\n", argv[4]);
        return 1;
    }

    size_t threads = 1;
    if (argc > 5) {
        if (sscanf(argv[5], "%zu", &threads) != 1) {
            fprintf(stderr, "Bad thread count: %s\n", argv[5]);
            return 1;
        }
    }

    printf("Generating into %s with %zu threads: %zu levels, %zu directories per level, %zu files per directory\n", argv[1], threads, settings.max_level, settings.dirs, settings.files);

    size_t sum = 1;
    for(size_t i = 0; i < settings.max_level; i++) {
        sum *= settings.dirs;
    }
    sum = (sum - 1) / (settings.dirs - 1);

    const size_t total_dirs = settings.dirs * sum;
    const size_t total_files = settings.files * sum;

    printf("Total Dirs:            %zu\n", total_dirs);
    printf("Total Files:           %zu\n", total_files);

    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);

    // generate the top level
    // each thread only generates the contents of its directory, not the directory itself
    struct dir * root = calloc(1, sizeof(struct dir));
    if (!root) {
        fprintf(stderr, "Could not allocate root work: %s\n", argv[2]);
        return 1;
    }
    memcpy(root->name, argv[1], strlen(argv[1]));

    struct stat top_st;
    memset(&top_st, 0, sizeof(struct stat));
    top_st.st_mode = DIR_PERMS;
    top_st.st_uid = geteuid();
    top_st.st_uid = getegid();
    if (dupdir(root->name, &top_st) != 0) {
        fprintf(stderr, "mkdir failed for %s: %d %s\n", root->name, errno, strerror(errno));
        free(root);
        return 1;
    }

    // start up threads and push root into the queue for processing
    struct QPTPool * pool = QPTPool_init(threads);
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        free(root);
        return -1;
    }

    QPTPool_enqueue(pool, 0, root);
    if (QPTPool_start(pool, generate_level, &settings) != threads) {
        fprintf(stderr, "Failed to start all threads\n");
        return -1;
    }

    QPTPool_wait(pool);
    QPTPool_destroy(pool);

    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);

    const long double gen_time = elapsed(&start, &end);

    printf("Time Spent Generating: %.2Lfs\n", gen_time);
    printf("Dirs/Sec:              %.2Lf\n",  total_dirs / gen_time);
    printf("Files/Sec:             %.2Lf\n",  total_files / gen_time);

    return 0;
}
