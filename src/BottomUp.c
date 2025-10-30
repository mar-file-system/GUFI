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
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "BottomUp.h"
#include "dbutils.h"
#include "debug.h"
#include "utils.h"

/* define so that descend and ascend always have valid functions to call */
static int noop_descend(void *user_struct, int *keep_going) {
    (void) user_struct;
    (void) keep_going;
    return 0;
}

static int noop_ascend(void *user_struct) {
    (void) user_struct;
    return 0;
}

/* free a struct BottomUp */
void bottomup_destroy(void *p) {
    struct BottomUp *b = (struct BottomUp *) p;

    free(b->name);
    free(b->alt_name);
    free(b);
}

struct UserArgs {
    long long int epoch;
    size_t user_struct_size;
    size_t min_level;
    size_t max_level;
    BU_descend_f descend;
    BU_ascend_f ascend;
    int track_non_dirs;
    int generate_alt_name;
    trie_t *skip;
};

/*
 * new_pathname() -
 *   Allocates and initializes a new pathname from the given dirname and basename.
 *
 *     - dirname_len and basename_len should be WITHOUT the null terminator.
 *
 *     - If basename is NULL, then this only uses the dirname and doesn't append a path component
 *       for the basename.
 *
 *   Fills in the path and path length in the given struct BottomUp.
 */
static void new_pathname(struct BottomUp *work, const char *dirname, size_t dirname_len,
                         const char *basename, size_t basename_len) {
    size_t new_len = dirname_len; // does NOT count null terminator

    if (basename) {
        new_len += basename_len + 1; // +1 for path separator
    }

    char *path = calloc(new_len + 1, sizeof(char));  // Additional +1 for null terminator

    if (basename) {
        SNFORMAT_S(path, new_len + 1, 3, dirname, dirname_len, "/", (size_t) 1, basename, basename_len);
    } else {
        strncpy(path, dirname, new_len + 1);
    }

    work->name = path;
    work->name_len = new_len;
}

/*
 * new_alt_pathname() -
 *   Allocates and initializes a new alternate pathname, that is, a name that escapes
 *   characters that cannot appear in a sqlite uri.
 *
 *   Parameters [dir|base]name[_len] are the same as in new_pathname().
 *
 *   Fills in alt_name and alt_name_len in the given struct BottomUp.
 *
 *   Returns 0 if conversion succeeded and -1 if conversion failed, because the output
 *   buffer wasn't large enough to hold the new name.
 */
static int new_alt_pathname(struct BottomUp *work, const char *dirname, size_t dirname_len,
                             const char *basename, size_t basename_len) {
    // Alternate name may be longer than the provided dirname and basename together
    char buf[MAXPATH] = { 0 };
    size_t bufsize = sizeof(buf);
    size_t new_len = 0;

    if (basename) {
        // Assume dirname is already sanitized, only need to sanitize basename:
        new_len = SNFORMAT_S(buf, bufsize, 2, dirname, dirname_len, "/", (size_t) 1);

        size_t converted_len = basename_len;
        new_len += sqlite_uri_path(buf + new_len, bufsize - new_len, basename, &converted_len);

        if (converted_len < basename_len) {
            // uh-oh ... didn't convert entire string
            return -1;
        }
    } else {
        // Need to sanitize dirname:
        size_t converted_len = dirname_len;
        new_len = sqlite_uri_path(buf, bufsize, dirname, &converted_len);

        if (converted_len < dirname_len) {
            // uh-oh ... didn't convert entire string
            return -1;
        }
    }

    char *path = calloc(new_len + 1, sizeof(char));
    memcpy(path, buf, new_len);

    work->alt_name = path;
    work->alt_name_len = new_len;

    return 0;
}

static int ascend_to_top(QPTPool_ctx_t *ctx, void *data) {
    struct UserArgs *ua = (struct UserArgs *) QPTPool_get_args_internal(ctx);
    struct BottomUp *bu = (struct BottomUp *) data;

    pthread_mutex_lock(&bu->refs.mutex);

    size_t remaining = 0;
    if (bu->refs.remaining) {
        remaining = --bu->refs.remaining;
    }

    pthread_mutex_unlock(&bu->refs.mutex);

    if (remaining) {
        return 0;
    }

    /* no subdirectories still need processing, so can process current directory */

    /* keep track of which thread was used to go back up */
    bu->tid.up = QPTPool_get_id(ctx);

    /* call user ascend function */
    int asc_rc = 0;
    if ((ua->min_level <= bu->level) && (bu->level <= ua->max_level)) {
        asc_rc = ua->ascend(bu);
    }

    /* clean up 'struct BottomUp's here, when they are */
    /* children instead of when they are the parent */
    sll_destroy(&bu->subdirs, bottomup_destroy);
    sll_destroy(&bu->subnondirs, bottomup_destroy);

    /* mutex is not needed any more */
    pthread_mutex_destroy(&bu->refs.mutex);

    if (bu->parent) {
        /* push parent to decrement their reference counters */
        QPTPool_enqueue(ctx, ascend_to_top, bu->parent);
    }
    else {
        /* reached root */
        bottomup_destroy(bu);
    }

    return asc_rc;
}

/*
 * track() - add the given struct BottomUp src to the given sll_t for later processing.
 *
 * Takes ownership of the name and alt_name from src.
 */
static struct BottomUp *track(struct BottomUp *src,
                              const size_t user_struct_size, sll_t *sll,
                              const size_t level, const long long int epoch,
                              const int generate_alt_name) {
    struct BottomUp *copy = calloc(user_struct_size, 1);

    copy->name = src->name;
    copy->name_len = src->name_len;

    if (generate_alt_name) {
        copy->alt_name = src->alt_name;
        copy->alt_name_len = src->alt_name_len;
    }

    copy->level = level;
    copy->epoch = epoch;

    /* store the subdirectories without enqueuing them */
    sll_push(sll, copy);

    return copy;
}

static int descend_to_bottom(QPTPool_ctx_t *ctx, void *data) {
    struct UserArgs *ua = (struct UserArgs *) QPTPool_get_args_internal(ctx);
    struct BottomUp *bu = (struct BottomUp *) data;

    /* keep track of which thread was used to walk downwards */
    bu->tid.down = QPTPool_get_id(ctx);

    DIR *dir = opendir(bu->name);

    if (!dir) {
        const int err = errno;
        fprintf(stderr, "Error: Could not open directory \"%s\": %s (%d)\n", bu->name, strerror(err), err);
        sll_destroy(&bu->subdirs, bottomup_destroy);
        sll_destroy(&bu->subnondirs, bottomup_destroy);
        if (bu->parent) {
            QPTPool_enqueue(ctx, ascend_to_top, bu->parent); /* reduce parent counter */
        }
        else {
            bottomup_destroy(bu);
        }
        return 1;
    }

    int dir_fd = gufi_dirfd(dir);

    pthread_mutex_init(&bu->refs.mutex, NULL);
    bu->subdir_count = 0;
    bu->subnondir_count = 0;
    sll_init(&bu->subdirs);
    sll_init(&bu->subnondirs);

    const size_t next_level = bu->level + 1;
    while (1) {
        struct dirent *entry = readdir(dir);

        if (!entry) {
            break;
        }

        size_t name_len = strlen(entry->d_name);
        if (trie_search(ua->skip, entry->d_name, name_len, NULL) == 1) {
            continue;
        }

        int is_dir = 0;
        if (entry->d_type == DT_UNKNOWN) {
            #if HAVE_STATX
            struct statx stx;
            if (statx(dir_fd, entry->d_name,
                      AT_SYMLINK_NOFOLLOW | AT_STATX_DONT_SYNC,
                      STATX_ALL, &stx) != 0) {
                const int err = errno;
                fprintf(stderr, "Error: Could not statx \"%s\": %s (%d)\n",
                        entry->d_name, strerror(err), err);
                return 1;
            }

            if (S_ISDIR(stx.stx_mode)) {
                is_dir = 1;
            }
            #else
            struct stat st;
            const int rc = fstatat(dir_fd, entry->d_name, &st, AT_SYMLINK_NOFOLLOW);

            if (rc != 0) {
                fprintf(stderr, "Error: Could not stat \"%s/%s\": %s\n",
                        bu->name, entry->d_name, strerror(errno));
                continue;
            }

            if (S_ISDIR(st.st_mode)) {
                is_dir = 1;
            }
            #endif
        } else if (entry->d_type == DT_DIR) {
            is_dir = 1;
        }

        if (is_dir) {
            bu->subdir_count++;
        } else {
            bu->subnondir_count++;
            // For files, only keep going if asked to track them:
            if (!ua->track_non_dirs)
                continue;
        }

        struct BottomUp new_work = { 0 };

        new_pathname(&new_work, bu->name, bu->name_len, entry->d_name, name_len);

        if (ua->generate_alt_name) {
            /* append converted entry name to converted directory */
            new_alt_pathname(&new_work, bu->alt_name, bu->alt_name_len, entry->d_name, name_len);
        }

        if (is_dir) {
            track(&new_work,
                  ua->user_struct_size, &bu->subdirs,
                  next_level, bu->epoch, ua->generate_alt_name);
        }
        else {
            track(&new_work,
                  ua->user_struct_size, &bu->subnondirs,
                  next_level, bu->epoch, ua->generate_alt_name);
        }
    }

    closedir(dir);

    /* call user descend function before further descent to ensure */
    /* that the descent function runs before the ascent function */

    /* this will probably be a bottleneck since subdirectories won't */
    /* be queued/processed while the descend function runs */
    int desc_rc = 0;
    int keep_descending = 1;
    if ((ua->min_level <= bu->level) && (bu->level <= ua->max_level)) {
        desc_rc = ua->descend(bu, &keep_descending);
    }

    /* if the descent function succeeded */
    if (desc_rc == 0) {
        /* if there are subdirectories, this directory cannot go back up just yet */
        if (keep_descending && (next_level <= ua->max_level) && bu->subdir_count) {
            /* decrement each time child triggers parent for processing */
            bu->refs.remaining = bu->subdir_count;

            /* have to lock to prevent subdirs from getting popped */
            /* off before all of them have been enqueued */
            pthread_mutex_lock(&bu->refs.mutex);
            sll_loop(&bu->subdirs, node)  {
                struct BottomUp *child = (struct BottomUp *) sll_node_data(node);
                child->parent = bu;
                child->extra_args = bu->extra_args;

                /* keep going down */
                QPTPool_enqueue(ctx, descend_to_bottom, child);
            }
            pthread_mutex_unlock(&bu->refs.mutex);
        }
        else {
            bu->refs.remaining = 0;

            /* start working upwards */
            QPTPool_enqueue(ctx, ascend_to_top, bu);
        }
    }
    else {
        sll_destroy(&bu->subdirs, bottomup_destroy);
        sll_destroy(&bu->subnondirs, bottomup_destroy);
        bu->subdir_count = 0;
        bu->subnondir_count = 0;
        bottomup_destroy(bu);
    }

    return desc_rc;
}

QPTPool_ctx_t *parallel_bottomup_init(const size_t thread_count,
                                      const size_t user_struct_size,
                                      const size_t min_level, const size_t max_level,
                                      BU_descend_f descend, BU_ascend_f ascend,
                                      const int track_non_dirs,
                                      const int generate_alt_name) {
    if (user_struct_size < sizeof(struct BottomUp)) {
        fprintf(stderr, "Error: Provided user struct size is smaller than a struct BottomUp\n");
        return NULL;
    }

    struct UserArgs *ua = calloc(1, sizeof(*ua));
    ua->epoch = (long long int) sec(since_epoch(NULL));
    ua->user_struct_size = user_struct_size;
    ua->min_level = min_level;
    ua->max_level = max_level;
    ua->descend = descend?descend:noop_descend;
    ua->ascend = ascend?ascend:noop_ascend;
    ua->track_non_dirs = track_non_dirs;
    ua->generate_alt_name = generate_alt_name;

    /* only skip . and .. */
    ua->skip = trie_alloc();
    trie_insert(ua->skip, ".",  1, NULL, NULL);
    trie_insert(ua->skip, "..", 2, NULL, NULL);

    QPTPool_ctx_t *ctx = QPTPool_init_with_props(thread_count, ua, NULL, NULL, 0, "", 1, 2);
    if (QPTPool_start(ctx) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(ctx);
        trie_free(ua->skip);
        free(ua);
        return NULL;
    }

    return ctx;
}

int parallel_bottomup_enqueue(QPTPool_ctx_t *ctx,
                              const char *path, const size_t len,
                              void *extra_args) {
    if (!ctx) {
        return -1;
    }

    size_t thread_count = 0;
    QPTPool_get_nthreads(ctx, &thread_count);

    struct UserArgs *ua = NULL;
    QPTPool_get_args(ctx, (void **) &ua);

    struct BottomUp *root = (struct BottomUp *) calloc(ua->user_struct_size, 1);
    new_pathname(root, path, len, NULL, 0);

    if (ua->generate_alt_name) {
        if (new_alt_pathname(root, root->name, root->name_len, NULL, 0)) {
            fprintf(stderr, "%s could not fit into ALT_NAME buffer\n", root->name);
            bottomup_destroy(root);
            return -1;
        }
    }

    root->parent = NULL;
    root->extra_args = extra_args;
    root->level = 0;

    QPTPool_enqueue(ctx, descend_to_bottom, root);
    return 0;
}

static int parallel_bottomup_enqueue_subdirs(QPTPool_ctx_t *ctx,
                                             const char *path, const size_t len,
                                             const size_t min_level, const refstr_t *path_list,
                                             void *extra_args) {
    struct UserArgs *ua = NULL;
    QPTPool_get_args(ctx, (void **) &ua);

    FILE *file = fopen(path_list->data, "r");
    if (!file) {
        const int err = errno;
        fprintf(stderr, "could not open directory list file \"%s\": %s (%d)\n",
                path_list->data, strerror(err), err);
        return -1;
    }

    char *line = NULL;
    size_t n = 0;
    ssize_t got = 0;
    while ((got = getline(&line, &n, file)) != -1) {
        /* remove trailing CRLF */
        const size_t line_len = trailing_non_match_index(line, got, "\r\n", 2);

        struct BottomUp *root = (struct BottomUp *) calloc(ua->user_struct_size, 1);
        new_pathname(root, path, len, line, line_len);

        if (ua->generate_alt_name) {
            if (new_alt_pathname(root, root->name, root->name_len, NULL, 0)) {
                fprintf(stderr, "%s could not fit into ALT_NAME buffer\n", root->name);
                bottomup_destroy(root);
                return -1;
            }
        }

        root->parent = NULL;
        root->extra_args = extra_args;
        root->level = min_level;

        QPTPool_enqueue(ctx, descend_to_bottom, root);
    }

    free(line);
    fclose(file);

    return 0;
}

int parallel_bottomup_fini(QPTPool_ctx_t *ctx) {
    if (!ctx) {
        return -1;
    }

    size_t thread_count = 0;
    QPTPool_get_nthreads(ctx, &thread_count);

    struct UserArgs *ua = NULL;
    QPTPool_get_args(ctx, (void **) &ua);

    QPTPool_stop(ctx);

    const size_t threads_started = QPTPool_threads_started(ctx);
    const size_t threads_completed = QPTPool_threads_completed(ctx);

    trie_free(ua->skip);
    free(ua);

    #ifdef DEBUG
    fprintf(stderr, "Started %zu threads. Successfully completed %zu threads.\n",
            threads_started, threads_completed);
    #endif

    QPTPool_destroy(ctx);

    return -(threads_started != threads_completed);
}

int parallel_bottomup(char **root_names, const size_t root_count,
                      const size_t min_level, const size_t max_level,
                      const refstr_t *path_list,
                      const size_t thread_count,
                      const size_t user_struct_size,
                      BU_descend_f descend, BU_ascend_f ascend,
                      const int track_non_dirs,
                      const int generate_alt_name,
                      void *extra_args) {
    if (min_level && path_list->data && path_list->len) {
        if (root_count > 1) {
            fprintf(stderr, "Error: Only one root may be provided when a --min-level and -D are both provided\n");
            return -1;
        }
    }

    QPTPool_ctx_t *ctx = parallel_bottomup_init(thread_count, user_struct_size,
                                                min_level, max_level,
                                                descend, ascend,
                                                track_non_dirs, generate_alt_name);
    if (!ctx) {
        return -1;
    }

    struct UserArgs *ua = NULL;
    QPTPool_get_args(ctx, (void **) &ua);

    /* enqueue all root directories */
    size_t good_roots = 0;
    if (min_level && path_list->data && path_list->len) {
        good_roots += !parallel_bottomup_enqueue_subdirs(ctx, root_names[0], strlen(root_names[0]),
                                                         min_level, path_list, extra_args);
    }
    else {
        for(size_t i = 0; i < root_count; i++) {
            good_roots += !parallel_bottomup_enqueue(ctx, root_names[i], strlen(root_names[i]), extra_args);
        }
    }

    return -(parallel_bottomup_fini(ctx) || (root_count != good_roots));
}
