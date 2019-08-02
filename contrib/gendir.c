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
           s =  \   # directories ** i = ((# of directories ** depth) - 1) / (# of directories - 1)
                /
                ---
               i = 0

The number of directories generated is

           # of directories * s

The number of files generated is

           # of files * s
*/
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

extern int errno;

long double elapsed(const struct timespec *start, const struct timespec *end) {
    const long double s = ((long double) start->tv_sec) + ((long double) start->tv_nsec) / 1000000000ULL;
    const long double e = ((long double) end->tv_sec)   + ((long double) end->tv_nsec)   / 1000000000ULL;
    return e - s;
}

int generate_level(const char * dir, const size_t subdir_count, const size_t file_count, const size_t current_level, const size_t max_level,
                   long double * mkdir_time, long double * mkfile_time) {
    if (current_level == max_level) {
        return 0;
    }

    char name[4096];
    for(size_t i = 0; i < file_count; i++) {
        int rc = snprintf(name, 4096, "%s/f.%zu", dir, i);
        if ((rc < 0) || (4096 <= rc)) {
            return 1;
        }

        struct timespec start;
        clock_gettime(CLOCK_MONOTONIC, &start);
        const int fd = open(name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
        struct timespec end;
        clock_gettime(CLOCK_MONOTONIC, &end);
        *mkfile_time += elapsed(&start, &end);
        if (fd < 0) {
            fprintf(stderr, "open failed for %s: %d %s\n", name, errno, strerror(errno));
            return 1;
        }
        close(fd);
    }

    for(size_t i = 0; i < subdir_count; i++) {
        int rc = snprintf(name, 4096, "%s/d.%zu", dir, i);
        if ((rc < 0) || (4096 <= rc)) {
            return 1;
        }

        struct timespec start;
        clock_gettime(CLOCK_MONOTONIC, &start);
        rc = mkdir(name, S_IRWXU | S_IRWXG | S_IRWXO);
        struct timespec end;
        clock_gettime(CLOCK_MONOTONIC, &end);
        *mkdir_time += elapsed(&start, &end);

        if (rc < 0) {
            fprintf(stderr, "mkdir failed for %s: %d %s\n", name, errno, strerror(errno));
            return 1;
        }
    }

    for(size_t i = 0; i < subdir_count; i++) {
        int rc = snprintf(name, 4096, "%s/d.%zu", dir, i);
        if ((rc < 0) || (4096 <= rc)) {
            return 1;
        }

        if (generate_level(name, subdir_count, file_count, current_level + 1, max_level, mkdir_time, mkfile_time) != 0) {
            fprintf(stderr, "descent failed for %s\n", name);
            return 1;
        }
    }

    return 0;
}

int main(int argc, char * argv[]) {
    if (argc < 5) {
        fprintf(stderr, "Syntax: %s output_dir directories files depth\n", argv[0]);
        return 1;
    }

    const char * output = argv[1];

    size_t dirs = 0;
    if ((sscanf(argv[2], "%zu", &dirs) != 1) || (dirs < 1)) {
        fprintf(stderr, "Bad directory count: %s\n", argv[2]);
        return 1;
    }

    size_t files = 0;
    if (sscanf(argv[3], "%zu", &files) != 1) {
        fprintf(stderr, "Bad file_count count: %s\n", argv[3]);
        return 1;
    }

    size_t depth = 0;
    if ((sscanf(argv[4], "%zu", &depth) != 1) || (depth < 1)) {
        fprintf(stderr, "Bad depth: %s\n", argv[4]);
        return 1;
    }

    printf("Generating into %s: %zu directories per level, %zu files per level, and %zu levels\n", output, dirs, files, depth);

    if (mkdir(output, S_IRWXU | S_IRWXG | S_IRWXO) < 0) {
        fprintf(stderr, "mkdir failed for %s: %d %s\n", output, errno, strerror(errno));
        return 1;
    }

    size_t sum = 1;
    for(size_t i = 0; i < depth; i++) {
        sum *= dirs;
    }
    sum = (sum - 1) / (dirs - 1);

    const size_t total_dirs = dirs * sum;
    const size_t total_files = files * sum;

    printf("Total Dirs:            %zu\n", total_dirs);
    printf("Total Files:           %zu\n", total_files);

    long double mkdir_time = 0;
    long double mkfile_time = 0;
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    const int rc = generate_level(output, dirs, files, 0, depth, &mkdir_time, &mkfile_time);
    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);

    if (rc != 0) {
        fprintf(stderr, "Error generating tree\n");
        return 1;
    }

    printf("Dirs/Sec:              %Lf\n",  total_dirs / mkdir_time);
    printf("Files/Sec:             %Lf\n",  total_files / mkfile_time);
    printf("Time Spent Generating: %Lfs\n", elapsed(&start, &end));

    return 0;
}
