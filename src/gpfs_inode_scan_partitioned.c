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



/*
 * GPFS Inode scan, partitioned
 *
 * This program scans a contiguous chunk of inodes in a GPFS filesystem at the specified path.
 * The range is delimited by [args.min, args.max] (inclusive). If min and max are not specified,
 * the range is taken to be [0, ULLONG_MAX].
 *
 * Multiple threads are launched according to args.n_threads.
 * Each thread is given a contiguous chunk from the overall range scanned by this program.
 *
 * Inodes whose ctime or mtime is newer than args.newer_than are printed to stdout.
 *
 * Usage example:
 *
 *     $ gpfs_inode_scan_partitioned /gpfs/ --newer_than 1771531335 --min 1000000 --max 2000000 -n4 | wc -l
 *     thread 1 chunk: [1250000, 1499999]
 *     thread 0 chunk: [1000000, 1249999]
 *     thread 3 chunk: [1750000, 2000000]
 *     thread 2 chunk: [1500000, 1749999]
 *     thread 2 processed 14336 nodes, of which 13312 (92.86%) were new.
 *     thread 0 processed 21504 nodes, of which 19456 (90.48%) were new.
 *     thread 1 processed 25955 nodes, of which 21504 (82.85%) were new.
 *     thread 3 processed 33943 nodes, of which 27631 (81.40%) were new.
 *     Total inodes processed: 95738
 *     Total new inodes: 81903 (85.55% of total)
 *     81903
 */

#include <getopt.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include <gpfs.h>

void *do_work(void *data);
void fetch_max_inum(void);

/*
 * Command line arguments, global for convenience.
 */
static struct {
    gpfs_fssnap_handle_t *fs_handle;
    size_t n_threads;
    uint64_t min;
    uint64_t max;
    gpfs_ino64_t highest_allocated_inum;
    long long newer_than;
} args = {
    .fs_handle = NULL,
    .n_threads = 1,
    .min = 0,
    .max = ULLONG_MAX,
    .highest_allocated_inum = 0,
    .newer_than = 0,
};

/*
 * Data returned from an individual worker thread.
 */
struct thread_ret {
    uint64_t total_seen;
    uint64_t newer_seen;
    bool error;
};

int main(int argc, char *argv[])
{
    int opt;
    bool newer_initialized = false;

    struct option opts[] = {
        {"min", required_argument, 0, 1},
        {"max", required_argument, 0, 2},
        {"newer_than", required_argument, 0, 3},
        {"threads", required_argument, 0, 'n'},
        { 0 }
    };
    while ((opt = getopt_long(argc, argv, "n:", opts, NULL)) != -1) {
        switch (opt) {
        case 1:
            args.min = strtoull(optarg, NULL, 10);
            break;
        case 2:
            args.max = strtoull(optarg, NULL, 10);
            break;
        case 3:
            args.newer_than = strtoll(optarg, NULL, 10);
            newer_initialized = true;
            break;
        case 'n':
            args.n_threads = atoi(optarg);
            break;
        }
    }

    if (optind >= argc || !newer_initialized) {
        fprintf(stderr, "Usage: %s PATH --newer_than TIMESTAMP [--min MIN] [--max MAX] [--threads N]\n", argv[0]);
        return 1;
    }

    args.fs_handle = gpfs_get_fssnaphandle_by_path(argv[optind]);
    if (!args.fs_handle) {
        fprintf(stderr, "gpfs_get_fssnaphandle_by_path() failed: %s\n", strerror(errno));
        return 1;
    }

    fetch_max_inum();

    pthread_t *threads = calloc(args.n_threads, sizeof *threads);
    for (uintptr_t i = 0; i < args.n_threads; i++) {
        pthread_create(&threads[i], NULL, do_work, (void *) i);
    }

    bool error = false;
    uint64_t total_seen = 0;
    uint64_t newer_seen = 0;
    for (int i = 0; i < args.n_threads; i++) {
        struct thread_ret *ret;
        int rc = pthread_join(threads[i], (void **) &ret);
        if (rc) {
            fprintf(stderr, "pthread_join() failed: %s\n", strerror(rc));
            continue;
        }
        if (!ret) {
            error = true;
            continue;
        }
        if (ret->error)
            error = true;

        total_seen += ret->total_seen;
        newer_seen += ret->newer_seen;

        free(ret);
    }

    free(threads);

    float percent = total_seen ?  (float) newer_seen / (float) total_seen * (float) 100 : 0;
    fprintf(stderr, "Total inodes processed: %llu\n", total_seen);
    fprintf(stderr, "Total new inodes: %llu (%.2f%% of total)\n", newer_seen, percent);

    gpfs_free_fssnaphandle(args.fs_handle);

    return error;
}

/*
 * Get the maximum allocated inode number from the GPFS,
 * and stash it in the global args struct.
 */
void fetch_max_inum(void)
{
    gpfs_iscan_t *scan = gpfs_open_inodescan64(args.fs_handle, NULL, &args.highest_allocated_inum);
    if (!scan) {
        fprintf(stderr, "gpfs_open_inodescan64() failed: %s\n", strerror(errno));
        exit(1);
    }

    gpfs_close_inodescan(scan);
    fprintf(stderr, "Max inode number: %llu\n", args.highest_allocated_inum);

    uint64_t max = args.max > args.highest_allocated_inum ? args.max : args.highest_allocated_inum;

    if (args.min >= max) {
        fprintf(stderr, "Warning: minimum %llu is >= than maximum %llu. Nothing to do, exiting.\n", args.min, max);
        exit(1);
    }
}

/*
 * Determine a thread's individual range of nodes to process based on the
 * thread's id and the overall range specified in the arguments.
 */
void get_thread_chunk(uint64_t *min, uint64_t *max, size_t id)
{
    uint64_t range;
    uint64_t chunk_size;
    uint64_t process_max;

    // If user specified a non-default maximum for this process, then
    // the chunk size is based on that. Otherwise, if the default max is used,
    // then use the highest inode number returned from GPFS as the upper
    // limit, in order to avoid absurdly huge chunks.
    if (args.max == ULLONG_MAX) {
        process_max = args.highest_allocated_inum;
    } else {
        process_max = args.max;
    }

    range = process_max - args.min;

    chunk_size = range / args.n_threads;

    *min = chunk_size * id + args.min;

    if (id == args.n_threads - 1) {
        // If this is the highest thread, it must go to the end:
        *max = args.max;
    } else {
        // Otherwise, go to the last slot before the next thread's chunk:
        *max = chunk_size * (id + 1) - 1 + args.min;
    }
}

/*
 * Individual thread worker function.
 *
 *   - Open an inode scan
 *   - Determine thread's own range subset to scan
 *   - loop through the inodes starting at the bottom of this thread's range
 *     - for each inode, if the timestamp is newer, then print out its number.
 *
 * Returns:
 *   - total number of nodes processed
 *   - how many nodes were new
 *   - whether an error occurred
 */
void *do_work(void *data) {
    uintptr_t id = (uintptr_t) data;
    uint64_t local_min, local_max;

    struct thread_ret *ret = calloc(1, sizeof *ret);
    if (!ret)
        return NULL;

    gpfs_ino64_t max_inum;
    gpfs_iscan_t *scan = gpfs_open_inodescan64(args.fs_handle, NULL, &max_inum);
    if (!scan) {
        fprintf(stderr, "gpfs_open_inodescan64() failed: %s\n", strerror(errno));
        ret->error = true;
        return ret;
    }

    get_thread_chunk(&local_min, &local_max, id);
    fprintf(stderr, "thread %llu chunk: [%llu, %llu]\n", id, local_min, local_max);

    gpfs_seek_inode64(scan, local_min);

    const gpfs_iattr64_t *attr = NULL;
    int rc;
    while ((rc = gpfs_next_inode64(scan, 0, &attr)) == 0) {
        if (attr == NULL)
            break;

        uint64_t inum = attr->ia_inode;
        if (inum > local_max)
            break;

        ret->total_seen += 1;

        if (attr->ia_mtime.tv_sec < args.newer_than ||
            attr->ia_ctime.tv_sec < args.newer_than) {
            continue;
        }

        // Object was new enough; print it. (Unless it's not a reg file/dir/link.)
        char type;
        if (S_ISDIR(attr->ia_mode)) {
            type = 'd';
        } else if (S_ISREG(attr->ia_mode)) {
            type = 'f';
        } else if (S_ISLNK(attr->ia_mode)) {
            type = 'l';
        } else {
            continue;
        };
        printf("%c %llu\n", type, inum);
        ret->newer_seen += 1;
    }

    if (rc) {
        fprintf(stderr, "gpfs_next_inode64() failed: %d: %s\n", rc, strerror(errno));
        ret->error = true;
    }

    float percent = ret->total_seen ? (float) ret->newer_seen / (float) ret->total_seen * (float) 100 : 0;
    fprintf(stderr, "thread %llu processed %llu nodes, of which %llu (%.2f%%) were new.\n",
            id, ret->total_seen, ret->newer_seen, percent);

    gpfs_close_inodescan(scan);

    return ret;
}
