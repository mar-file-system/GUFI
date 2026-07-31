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



#include <errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "bf.h"
#include "debug.h"
#include "dbutils.h"
#include "external_attach.h"
#include "index.h"
#include "path_list.h"
#include "plugin.h"
#include "template_db.h"
#include "str.h"
#include "utils.h"

struct PoolArgs {
    struct input in;
    str_t index_parent; /* actual index is placed at <index parent>/$(basename <src>) */

    struct template_db db;
    struct template_db xattr;

    uint64_t *total_dirs;
    uint64_t *total_nondirs;
};

static int processdir(QPTPool_ctx_t *ctx, void *data) {
    /* Not checking arguments */

    struct PoolArgs *pa = (struct PoolArgs *) QPTPool_get_args_internal(ctx);
    struct work *work = NULL;
    decompress_work(&work, data);
    struct entry_data ed = {0};
    ed.type = 'd';

    plugin_dir_action process_dir = PLUGIN_NO_PROCESS_DIR;
    struct descend_counters ctrs = {0};

    int rc = 0;

    DIR *dir = opendir_wrapper(work->name, NULL);
    if (!dir) {
        rc = 1;
        goto done;
    }

    str_t topath = {0};

    /*
     * allocate space for "/db.db" in topath so that an extra buffer
     * is not needed to switch between directory and db paths
     */
    topath.len = SNFORMAT_S_ALLOC(&topath.data, 4,
                                  pa->index_parent.data, pa->index_parent.len,
                                  "/", (size_t) 1,
                                  work->name + work->root_parent.len, work->name_len - work->root_parent.len, /* remove prefix */
                                  "/" DBNAME, (size_t) 1 + DBNAME_LEN);

    topath.free = free;

    rc = index_dir(&topath, 1, ctx, &pa->in, &pa->db, &pa->xattr,
                   &process_dir, work, &ed, dir, processdir, &ctrs);

    if (rc != 0) {
        rc = (rc < 0);
        goto free_topath;
    }

    /* remove db.db */
    topath.len -= 1 + DBNAME_LEN;
    topath.data[topath.len] = '\0';

    /* set permissions on index directory */
    if (chmod(topath.data, work->statuso.st_mode) != 0) {
        const int err = errno;
        fprintf(stderr, "Warning: Unable to set permission for \"%s\": %s (%d)\n",
                topath.data, strerror(err), err);
    }

    /* set owners on index directory */
    if (chown(topath.data, work->statuso.st_uid, work->statuso.st_gid) != 0) {
        const int err = errno;
        fprintf(stderr, "Warning: Unable to set owners for \"%s\": %s (%d)\n",
                topath.data, strerror(err), err);
    }

  free_topath:
    str_free_existing(&topath);

  done:
    if (process_dir == PLUGIN_PROCESS_DIR) {
        const size_t id = QPTPool_get_id(ctx);
        pa->total_dirs[id]++;
        pa->total_nondirs[id] += ctrs.nondirs_processed;
    }

    closedir(dir);
    free(work);

    return rc;
}

/* set up parent for a single subtree root in the index and enqueue subtree root as normal work */
static int process_subtree_root(QPTPool_ctx_t *ctx, void *data) {
    struct work *subtree_root = (struct work *) data;
    struct PoolArgs *pa = (struct PoolArgs *) QPTPool_get_args_internal(ctx);

    /* offset by root_parent.len to remove prefix */
    char *topath = NULL;
    SNFORMAT_S_ALLOC(&topath, 3,
                     pa->index_parent.data, pa->index_parent.len,
                     "/", (size_t) 1,
                     subtree_root->name + subtree_root->root_parent.len, subtree_root->name_len - subtree_root->root_parent.len);

    /*
     * create directories up to parent with correct permissions and owners
     * so that processdir can maintain assumption that the parent directory
     * already exists
     *
     * empty db.db files are not created
     */
    for (char *p = strchr(topath + 1, '/'); p; p = strchr(p + 1, '/')) {
        *p = '\0';

        struct stat st;
        if (stat(topath, &st) != 0) { /* stat(2) not lstat(2) */
            const int err = errno;
            if (err != ENOENT) {
                fprintf(stderr, "Error: Cannot stat subtree root parent \"%s\": %s (%d)\n",
                        topath, strerror(err), err);
                goto error;
            }
        }

        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "Error: Subtree root parent is not a directory \"%s\"\n", topath);
            goto error;
        }

        if (mkdir(topath, st.st_mode) == -1) {
            const int err = errno;
            if (err != EEXIST) {
                fprintf(stderr, "Error: Could not make subtree root parent \"%s\": %s (%d)\n",
                        topath, strerror(err), err);
                *p = '/';
                goto error;
            }
        }
        else {
            chmod(topath, st.st_mode);
            chown(topath, st.st_uid, st.st_gid);
        }

        *p = '/';
    }

    free(topath);

    struct work *copy = compress_struct(pa->in.compress, subtree_root, struct_work_size(subtree_root));
    QPTPool_enqueue(ctx, processdir, copy);

    return 0;

  error:
    free(topath);
    free(subtree_root);
    return 1;
}

/*
 * create the target directory
 *
 * note that the provided directories go into
 * individual directories underneath this one
 */
static int setup_dst(char *index_parent) {
    /* check if the destination path already exists (not an error) */
    struct stat dst_st;
    if (lstat(index_parent, &dst_st) == 0) {
        fprintf(stderr, "\"%s\" Already exists!\n", index_parent);

        /* if the destination path is not a directory (error) */
        if (!S_ISDIR(dst_st.st_mode)) {
            fprintf(stderr, "Destination path is not a directory \"%s\"\n", index_parent);
            return -1;
        }

        return 0;
    }

    if (dupdir(index_parent, S_IRWXU | S_IRWXG | S_IRWXO, geteuid(), getegid())) {
        fprintf(stderr, "Could not create %s\n", index_parent);
        return -1;
    }

    return 0;
}

static int validate_source(str_t *index_parent, const char *path, struct work **work) {
    /* get input path metadata */
    struct stat st;
    if (lstat(path, &st) != 0) {
        const int err = errno;
        fprintf(stderr, "Could not stat source directory \"%s\": %s (%d)\n", path, strerror(err), err);
        return 1;
    }

    /* check that the input path is a directory */
    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Source path is not a directory \"%s\"\n", path);
        return 1;
    }

    struct work *new_work = new_work_with_name(NULL, 0, path, strlen(path));

    new_work->root_parent.data = (char *) path;
    new_work->root_parent.len = dirname_len(path, new_work->name_len);
    new_work->level = 0;
    new_work->basename_len = new_work->name_len - new_work->root_parent.len;
    new_work->root_basename_len = new_work->basename_len;
    new_work->orig_root.data = (char *) path;
    new_work->orig_root.len = strlen(new_work->orig_root.data);

    const size_t expathtst_len = index_parent->len + 1 + new_work->name_len - new_work->root_parent.len;
    char *expathtst = malloc(expathtst_len + 1);
    SNPRINTF(expathtst, expathtst_len + 1, "%s/%s", index_parent->data, new_work->root_parent.data + new_work->root_parent.len);

    char *expathout = realpath(expathtst, NULL);
    if (expathout)  {
        char *expathin = realpath(new_work->root_parent.data, NULL);

        if (!strcmp(expathin, expathout)) {
            fprintf(stderr,"You are putting the index dbs in input directory\n");
        }

        free(expathin);
        free(expathout);
    }

    free(expathtst);

    *work = new_work;

    return 0;
}

static void sub_help(void) {
   printf("dir...            walk one or more trees to produce GUFI tree\n");
   printf("GUFI_tree_parent  build GUFI tree under here\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_THREADS,

        /* tree walk flags */
        FLAG_MIN_LEVEL, FLAG_MAX_LEVEL, FLAG_PATH_LIST,
        FLAG_INDEX_XATTRS, FLAG_SKIP_FILE,

        /* miscellaneous flags */
        FLAG_EXTERNAL_ATTACH_VALIDATE, FLAG_PLUGIN,

        /* memory usage flags */
        FLAG_TARGET_MEMORY, FLAG_SWAP_PREFIX, FLAG_SUBDIR_LIMIT,
        #ifdef HAVE_ZLIB
        FLAG_COMPRESS,
        #endif

        FLAG_END
    };

    struct PoolArgs pa;
    process_args_and_maybe_exit(options, 1, "dir... GUFI_tree_parent", &pa.in);

    if (plugins_check_type(&pa.in.plugins, PLUGIN_INDEX) != pa.in.plugins.count) {
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    if (plugins_global_init(&pa.in.plugins, &pa.in) != pa.in.plugins.count) {
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    /* parse positional args, following the options */
    /* does not have to be canonicalized */
    INSTALL_STR(&pa.index_parent, pa.in.pos.argv[pa.in.pos.argc - 1]);

    /* index parent is no longer needed */
    const size_t root_count = --pa.in.pos.argc;

    int rc = EXIT_SUCCESS;

    if (bad_partial_walk(&pa.in, root_count)){
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    if (setup_dst(pa.index_parent.data) != 0) {
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    init_template_db(&pa.db);
    if (create_dbdb_template(&pa.db, &pa.index_parent) != 0) {
        fprintf(stderr, "Could not create template file\n");
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    /*
     * create empty db.db in index parent (this file is placed in
     * "${dst}/db.db"; index is placed in "${dst}/$(basename ${src}))"
     * so that when querying "${dst}", no error is printed
     */
    if (create_empty_dbdb(&pa.db, &pa.index_parent, geteuid(), getegid()) != 0) {
        rc = EXIT_FAILURE;
        goto free_db;
    }

    init_template_db(&pa.xattr);
    if (create_xattrs_template(&pa.xattr, &pa.index_parent) != 0) {
        fprintf(stderr, "Could not create xattr template file\n");
        rc = EXIT_FAILURE;
        goto free_db;
    }

    const uint64_t queue_limit = get_queue_limit(pa.in.target_memory, pa.in.maxthreads);
    QPTPool_ctx_t *ctx = QPTPool_init_with_props(pa.in.maxthreads, &pa, NULL, NULL, queue_limit, pa.in.swap_prefix.data, 1, 2, 1);
    if (QPTPool_start(ctx) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        rc = EXIT_FAILURE;
        goto free_xattr;
    }

    fprintf(stderr, "Creating GUFI tree %s with %zu threads\n", pa.index_parent.data, pa.in.maxthreads);

    pa.total_dirs    = calloc(pa.in.maxthreads, sizeof(uint64_t));
    pa.total_nondirs = calloc(pa.in.maxthreads, sizeof(uint64_t));

    struct start_end rt;
    clock_gettime(CLOCK_REALTIME, &rt.start);

    struct start_end after_init;
    clock_gettime(CLOCK_MONOTONIC, &after_init.start);

    if (doing_partial_walk(&pa.in, root_count)) {
        if (root_count == 0) {
            process_path_list(&pa.in, NULL, ctx, process_subtree_root);
        }
        else if (root_count == 1) {
            struct work *root = NULL;
            if (validate_source(&pa.index_parent, pa.in.pos.argv[0], &root) == 0) {
                process_path_list(&pa.in, root, ctx, process_subtree_root);
            }
            else {
                rc = EXIT_FAILURE;
            }
        }
    }
    else {
        if (root_count) {
            for(int i = 0; i < pa.in.pos.argc; i++) {
                /* get first work item by validating source path */
                struct work *root = NULL;
                if (validate_source(&pa.index_parent, pa.in.pos.argv[i], &root) != 0) {
                    continue;
                }

                struct work *copy = compress_struct(pa.in.compress, root, struct_work_size(root));
                QPTPool_enqueue(ctx, processdir, copy);
            }
        }
        else {
            fprintf(stderr, "Error: At least one root is needed\n");
            rc = EXIT_FAILURE;
        }
    }
    QPTPool_stop(ctx);

    clock_gettime(CLOCK_MONOTONIC, &after_init.end);
    clock_gettime(CLOCK_REALTIME, &rt.end);
    const long double processtime = sec(nsec(&after_init));

    /* don't count as part of processtime */

    if (QPTPool_stopped_on_error(ctx) == 1) {
        rc = EXIT_FAILURE;
    }

    QPTPool_destroy(ctx);

    uint64_t total_dirs = 0;
    uint64_t total_nondirs = 0;
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        total_dirs    += pa.total_dirs[i];
        total_nondirs += pa.total_nondirs[i];
    }

    free(pa.total_dirs);
    free(pa.total_nondirs);

    if (rc == EXIT_SUCCESS) {
        fprintf(stderr, "Total Dirs:          %" PRIu64 "\n", total_dirs);
        fprintf(stderr, "Total Non-Dirs:      %" PRIu64 "\n", total_nondirs);
        fprintf(stderr, "Start Time:          %.6Lf\n",       sec(since_epoch(&rt.start)));
        fprintf(stderr, "End Time:            %.6Lf\n",       sec(since_epoch(&rt.end)));
        fprintf(stderr, "Time Spent Indexing: %.2Lfs\n",      processtime);
        fprintf(stderr, "Dirs/Sec:            %.2Lf\n",       total_dirs / processtime);
        fprintf(stderr, "Non-Dirs/Sec:        %.2Lf\n",       total_nondirs / processtime);
    }

  free_xattr:
    close_template_db(&pa.xattr);

  free_db:
    close_template_db(&pa.db);

  cleanup:
    plugins_global_exit(&pa.in.plugins, &pa.in);

    input_fini(&pa.in);

    dump_memory_usage(stderr);

    return rc;
}
