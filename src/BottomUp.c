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
iterate through all directories
wait until all subdirectories have been
processed before processing the current one
*/

#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "BottomUp.h"
#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "dbutils.h"
#include "debug.h"
#include "utils.h"


extern int errno;

struct UserArgs {
    size_t user_struct_size;
    AscendFunc_t func;
    int track_non_dirs;

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    struct OutputBuffers *timestamp_buffers;
    #else
    void *timestamp_buffers;
    #endif
};

int ascend_to_top(struct QPTPool *ctx, const size_t id, void *data, void *args) {
    timestamp_create_buffer(4096);
    timestamp_start(ascend);

    struct UserArgs *ua = (struct UserArgs *) args;

    /* reached root */
    if (!data) {
        timestamp_end(ua->timestamp_buffers, id, "ascend_to_top", ascend);
        return 0;
    }

    struct BottomUp *bu = (struct BottomUp *) data;

    timestamp_start(lock_refs);
    pthread_mutex_lock(&bu->refs.mutex);
    timestamp_end(ua->timestamp_buffers, id, "lock_refs", lock_refs);

    timestamp_start(get_remaining_refs);
    size_t remaining = 0;
    if (bu->refs.remaining) {
        remaining = --bu->refs.remaining;
    }
    timestamp_end(ua->timestamp_buffers, id, "get_remaining_refs", get_remaining_refs);

    timestamp_start(unlock_refs);
    pthread_mutex_unlock(&bu->refs.mutex);
    timestamp_end(ua->timestamp_buffers, id, "unlock_refs", unlock_refs);

    if (remaining) {
        timestamp_end(ua->timestamp_buffers, id, "ascend_to_top", ascend);
        return 0;
    }

    /* no subdirectories still need processing, so can attempt to roll up */

    /* keep track of which thread was used to go back up */
    bu->tid.up = id;

    /* call user function */
    timestamp_start(run_user_func);
    ua->func(bu
             #ifdef DEBUG
                 #ifdef PER_THREAD_STATS
                     , ua->timestamp_buffers
                 #endif
             #endif
        );
    timestamp_end(ua->timestamp_buffers, id, "run_user_function", run_user_func);

    /* clean up first, just in case parent runs before  */
    /* the current `struct BottomUp` finishes cleaning up */

    /* clean up 'struct BottomUp's here, when they are */
    /* children instead of when they are the parent  */
    timestamp_start(cleanup);
    sll_destroy(&bu->subdirs, free);
    sll_destroy(&bu->subnondirs, free);

    /* mutex is not needed any more */
    pthread_mutex_destroy(&bu->refs.mutex);
    timestamp_end(ua->timestamp_buffers, id, "cleanup", cleanup);

    /* always push parent to decrement their reference counters */
    timestamp_start(enqueue_ascend);
    QPTPool_enqueue(ctx, id, ascend_to_top, bu->parent);
    timestamp_end(ua->timestamp_buffers, id, "enqueue_ascend", enqueue_ascend);

    timestamp_end(ua->timestamp_buffers, id, "ascend_to_top", ascend);
    return 0;
}

static struct BottomUp *track(const char *name, const size_t name_len,
                              const size_t user_struct_size, struct sll *sll,
                              const size_t level) {
    struct BottomUp *copy = malloc(user_struct_size);

    memcpy(copy->name, name, name_len + 1); /* NULL terminate */
    copy->name_len = name_len;

    copy->level = level;

    /* store the subdirectories without enqueuing them */
    sll_push(sll, copy);

    return copy;
}

int descend_to_bottom(struct QPTPool *ctx, const size_t id, void *data, void *args) {
    timestamp_create_buffer(4096);
    timestamp_start(descend);

    struct UserArgs *ua = (struct UserArgs *) args;
    struct BottomUp *bu = (struct BottomUp *) data;

    /* keep track of which thread was used to walk downwards */
    bu->tid.down = id;

    timestamp_start(open_dir);
    DIR *dir = opendir(bu->name);
    timestamp_end(ua->timestamp_buffers, id, "opendir", open_dir);

    if (!dir) {
        fprintf(stderr, "Error: Could not open directory \"%s\": %s\n", bu->name, strerror(errno));
        free(data);
        timestamp_end(ua->timestamp_buffers, id, "descend_to_bottom", descend);
        return 0;
    }

    timestamp_start(init);
    pthread_mutex_init(&bu->refs.mutex, NULL);
    bu->subdir_count = 0;
    bu->subnondir_count = 0;
    sll_init(&bu->subdirs);
    sll_init(&bu->subnondirs);
    timestamp_end(ua->timestamp_buffers, id, "init", init);

    timestamp_start(read_dir_loop);
    const size_t next_level = bu->level + 1;
    while (1) {
        timestamp_start(read_dir);
        struct dirent *entry = readdir(dir);;
        timestamp_end(ua->timestamp_buffers, id, "readdir", read_dir);

        if (!entry) {
            break;
        }

        const size_t name_len = strlen(entry->d_name);
        if (name_len < 3) {
            if ((strncmp(entry->d_name, ".",  1) == 0) ||
                (strncmp(entry->d_name, "..", 2) == 0)) {
                continue;
            }
        }

        struct BottomUp new_work;
        new_work.name_len = SNFORMAT_S(new_work.name, MAXPATH, 3,
                                       bu->name, bu->name_len,
                                       "/", (size_t) 1,
                                       entry->d_name, name_len);

        timestamp_start(lstat_entry);
        struct stat st;
        const int rc = lstat(new_work.name, &st);
        timestamp_end(ua->timestamp_buffers, id, "lstat", lstat_entry);

        if (rc != 0) {
            fprintf(stderr, "Error: Could not stat \"%s\": %s\n", new_work.name, strerror(errno));
            continue;
        }

        timestamp_start(track_entry);
        if (S_ISDIR(st.st_mode)) {
            track(new_work.name, new_work.name_len,
                  ua->user_struct_size, &bu->subdirs,
                  next_level);

            /* count how many subdirectories this directory has */
            bu->subdir_count++;
        }
        else {
            if (ua->track_non_dirs) {
                track(new_work.name, new_work.name_len,
                      ua->user_struct_size, &bu->subnondirs,
                      next_level);
            }
            bu->subnondir_count++;
        }
        timestamp_end(ua->timestamp_buffers, id, "track", track_entry);
    }
    timestamp_end(ua->timestamp_buffers, id, "readdir_loop", read_dir_loop);

    timestamp_start(close_dir);
    closedir(dir);
    timestamp_end(ua->timestamp_buffers, id, "closedir", close_dir);

    /* if there are subdirectories, this directory cannot go back up just yet */
    bu->refs.remaining = bu->subdir_count;
    if (bu->subdir_count) {
        timestamp_start(enqueue_subdirs);
        /* have to lock to prevent subdirs from getting popped */
        /* off before all of them have been enqueued */
        pthread_mutex_lock(&bu->refs.mutex);
        sll_loop(&bu->subdirs, node)  {
            struct BottomUp *child = (struct BottomUp *) sll_node_data(node);
            child->parent = bu;
            child->extra_args = bu->extra_args;

            /* keep going down */
            timestamp_start(enqueue_subdir);
            QPTPool_enqueue(ctx, id, descend_to_bottom, child);
            timestamp_end(ua->timestamp_buffers, id, "enqueue_subdir", enqueue_subdir);
        }
        pthread_mutex_unlock(&bu->refs.mutex);
        timestamp_end(ua->timestamp_buffers, id, "enqueue_subdirs", enqueue_subdirs);
    }
    else {
        /* start working upwards */
        timestamp_start(enqueue_bottom);
        QPTPool_enqueue(ctx, id, ascend_to_top, bu);
        timestamp_end(ua->timestamp_buffers, id, "enqueue_bottom", enqueue_bottom);
    }

    timestamp_end(ua->timestamp_buffers, id, "descend_to_bottom", descend);
    return 0;
}

int parallel_bottomup(char **root_names, size_t root_count,
                      const size_t thread_count,
                      const size_t user_struct_size, AscendFunc_t func,
                      const int track_non_dirs,
                      void *extra_args
                      #if defined(DEBUG) && defined(PER_THREAD_STATS)
                      , struct OutputBuffers *timestamp_buffers
                      #endif
    ) {
    struct UserArgs ua;

    timestamp_create_buffer(4096);

    if (user_struct_size < sizeof(struct BottomUp)) {
        fprintf(stderr, "Error: Provided user struct size is smaller than a struct BottomUp\n");
        return -1;
    }

    if (!func) {
        fprintf(stderr, "Error: No function provided\n");
        return -1;
    }

    ua.user_struct_size = user_struct_size;
    ua.func = func;
    ua.track_non_dirs = track_non_dirs;

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    ua.timestamp_buffers = timestamp_buffers;
    #endif

    struct QPTPool *pool = QPTPool_init(thread_count
                                        #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                        , timestamp_buffers
                                        #endif
    );

    if (!pool) {
        fprintf(stderr, "Error: Failed to initialize thread pool\n");
        return -1;
    }

    if (QPTPool_start(pool, &ua) != (size_t) thread_count) {
        fprintf(stderr, "Error: Failed to start threads\n");
        QPTPool_destroy(pool);
        return -1;
    }

    /* enqueue all root directories */
    timestamp_start(enqueue_roots);
    struct BottomUp *roots = malloc(root_count * user_struct_size);
    for(size_t i = 0; i < root_count; i++) {
        struct BottomUp *root = &roots[i];
        root->name_len = SNPRINTF(root->name, MAXPATH, "%s", root_names[i]);

        struct stat st;
        if (lstat(root->name, &st) != 0) {
            fprintf(stderr, "Could not stat %s\n", root->name);
            continue;
        }

        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "%s is not a directory\n", root->name);
            continue;
        }

        root->parent = NULL;
        root->extra_args = extra_args;
        root->level = 0;

        timestamp_start(enqueue_root);
        QPTPool_enqueue(pool, i % in.maxthreads, descend_to_bottom, root);
        timestamp_end(ua.timestamp_buffers, thread_count, "enqueue_root", enqueue_root);
    }
    timestamp_end(ua.timestamp_buffers, thread_count, "enqueue_roots", enqueue_roots);

    timestamp_start(qptpool_wait);
    QPTPool_wait(pool);
    timestamp_end(ua.timestamp_buffers, thread_count, "wait_for_threads", qptpool_wait);

    /* clean up root directories since they don't get freed during processing */
    free(roots);

    #ifdef DEBUG
    const size_t threads_ran = QPTPool_threads_completed(pool);
    fprintf(stderr, "Ran %zu threads\n", threads_ran);
    #endif

    QPTPool_destroy(pool);

    return 0;
}
