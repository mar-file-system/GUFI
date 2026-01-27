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
#include "external.h"
#include "path_list.h"
#include "plugin.h"
#include "template_db.h"
#include "str.h"
#include "utils.h"

struct PoolArgs {
    struct input in;
    refstr_t index_parent; /* actual index is placed at <index parent>/$(basename <src>) */

    struct template_db db;
    struct template_db xattr;

    uint64_t *total_dirs;
    uint64_t *total_nondirs;
};

/*
 * values passed to process_nondir
 *
 * serves double duty as state within processdir
 */
struct NonDirArgs {
    struct input *in;
    refstr_t *index_parent;

    /* thread args */
    struct template_db *temp_db;
    struct template_db *temp_xattr;
    struct work *work;
    struct entry_data ed;

    /* index path */
    str_t topath;

    /* summary of the current directory */
    struct sum summary;

    /* db.db */
    sqlite3 *db;

    /* prepared statements */
    sqlite3_stmt *entries_res;
    sqlite3_stmt *xattrs_res;
    sqlite3_stmt *xattr_files_res;

    /* list of xattr dbs */
    sll_t xattr_db_list;

    /*
     * an optional opaque pointer to user data for a plugin. if the plugin's ctx_init() function
     * creates and returns a pointer, then the later plugin functions can use it to store state.
     */
    void *plugin_user_data;
};

static int process_external(struct input *in, void *args,
                            const long long int pinode,
                            const char *filename) {
    (void) in;
    return external_insert((sqlite3 *) args, EXTERNAL_TYPE_USER_DB_NAME, pinode, filename);
}

static int process_nondir(struct work *entry, struct entry_data *ed, void *args) {
    struct NonDirArgs *nda = (struct NonDirArgs *) args;
    struct input *in = nda->in;
    int rc = 0;

    if (fstatat_wrapper(entry, ed, 1) != 0) {
        rc = 1;
        goto out;
    }

    if (in->process_xattrs) {
        insertdbgo_xattrs(in, &nda->work->statuso, entry, ed,
                          &nda->xattr_db_list, nda->temp_xattr,
                          nda->topath.data, nda->topath.len,
                          nda->xattrs_res, nda->xattr_files_res);
    }

    /* read external files before modifying the entry's path */
    if (strncmp(entry->name + entry->name_len - entry->basename_len,
                EXTERNAL_DB_USER_FILE, EXTERNAL_DB_USER_FILE_LEN + 1) == 0) {
        external_read_file(in, entry, process_external, nda->db);
    }

    /* update summary table */
    sumit(&nda->summary, entry, ed);

    /* add entry + xattr names into bulk insert */
    insertdbgo(entry, ed, nda->entries_res);

    if (in->plugin_ops->process_file) {
        PCS_t pcs = {
            .db = nda->db,
            .work = entry,
            .ed = ed,
        };
        in->plugin_ops->process_file(&pcs, nda->plugin_user_data);
    }

out:
    return rc;
}

static int processdir(QPTPool_ctx_t *ctx, void *data) {
    /* Not checking arguments */

    int rc = 0;

    struct PoolArgs *pa = (struct PoolArgs *) QPTPool_get_args_internal(ctx);

    struct NonDirArgs nda;
    nda.in         = &pa->in;
    nda.temp_db    = &pa->db;
    nda.temp_xattr = &pa->xattr;
    nda.work       = NULL;
    memset(&nda.ed, 0, sizeof(nda.ed));
    nda.ed.type    = 'd';
    nda.plugin_user_data = NULL;

    PCS_t pcs; /* references passed into plugin */
    memset(&pcs, 0, sizeof(pcs));

    DIR *dir = NULL;

    decompress_work(&nda.work, data);

    const int process_dir = ((pa->in.min_level <= nda.work->level) &&
                             (nda.work->level <= pa->in.max_level));

    dir = opendir(nda.work->name);
    if (!dir) {
        const int err = errno;
        fprintf(stderr, "Error: Could not open directory \"%s\": %s (%d)\n", nda.work->name, strerror(err), err);
        rc = 1;
        goto cleanup;
    }

    if (lstat_wrapper(nda.work, 1) != 0) {
        rc = 1;
        goto close_dir;
    }

    /* offset by work->root_len to remove prefix */
    nda.topath.len = pa->index_parent.len + 1 + nda.work->name_len - nda.work->root_parent.len;

    /*
     * allocate space for "/db.db" in nda.topath
     *
     * extra buffer is not needed and save on memcpy-ing
     */
    const size_t topath_size = nda.topath.len + 1 + DBNAME_LEN + 1;

    nda.topath.data = malloc(topath_size);
    SNFORMAT_S(nda.topath.data, topath_size, 4,
               pa->index_parent.data, pa->index_parent.len,
               "/", (size_t) 1,
               nda.work->name + nda.work->root_parent.len, nda.work->name_len - nda.work->root_parent.len,
               "\0" DBNAME, (size_t) 1 + DBNAME_LEN);

    /* don't need recursion because parent is guaranteed to exist */
    if (mkdir(nda.topath.data, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) < 0) {
        const int err = errno;
        if (err != EEXIST) {
            fprintf(stderr, "mkdir %s failure: %d %s\n", nda.topath.data, err, strerror(err));
            rc = 1;
            goto close_dir;
        }
    }

    /*
     * set up for processing, but keep to minimum to quickly hit
     * descend (and enqueue more work, keeping queues fed)
     */
    if (process_dir) {
        /* restore "/db.db" */
        nda.topath.data[nda.topath.len] = '/';

        nda.db = template_to_db(nda.temp_db, nda.topath.data, nda.work->statuso.st_uid, nda.work->statuso.st_gid);

        /* remove "/db.db" */
        nda.topath.data[nda.topath.len] = '\0';

        if (!nda.db) {
            rc = 1;
            goto close_dir;
        }

        pcs.db = nda.db;
        pcs.work = nda.work;
        pcs.ed = &nda.ed;
        pcs.data = &nda.topath;

        /* prepare to insert into the database */
        zeroit(&nda.summary);

        /* prepared statements within db.db */
        nda.entries_res = insertdbprep(nda.db, ENTRIES_INSERT);
        nda.xattrs_res = NULL;
        nda.xattr_files_res = NULL;

        if (nda.in->process_xattrs) {
            nda.xattrs_res = insertdbprep(nda.db, XATTRS_PWD_INSERT);
            nda.xattr_files_res = insertdbprep(nda.db, EXTERNAL_DBS_PWD_INSERT);

            /* external per-user and per-group dbs */
            sll_init(&nda.xattr_db_list);
        }

        startdb(nda.db);

        /* run light-weight plugin setup */
        if (pa->in.plugin_ops->ctx_init) {
            nda.plugin_user_data = pa->in.plugin_ops->ctx_init(&pcs);
        }
   }

    struct descend_counters ctrs;
    descend(ctx, nda.in, nda.work, dir, 1,
            processdir, process_dir?process_nondir:NULL, &nda, &ctrs);

    /*
     * now that subdirectories have been enqueued,
     * do slower processing on this directory
     */
    if (process_dir) {
        stopdb(nda.db);

        /* entries and xattrs have been inserted */

        if (nda.in->process_xattrs) {
            /* write out per-user and per-group xattrs */
            sll_destroy(&nda.xattr_db_list, destroy_xattr_db);

            /* keep track of per-user and per-group xattr dbs */
            insertdbfin(nda.xattr_files_res);

            /* pull this directory's xattrs because they were not pulled by the parent */
            xattrs_setup(&nda.ed.xattrs);
            xattrs_get(nda.work->name, &nda.ed.xattrs);

            /* directory xattrs go into the same table as entries xattrs */
            insertdbgo_xattrs_avail(nda.work, &nda.ed, nda.xattrs_res);
            insertdbfin(nda.xattrs_res);
        }
        insertdbfin(nda.entries_res);

        /* insert this directory's summary data */
        /* the xattrs go into the xattrs_avail table in db.db */
        insertsumdb(nda.db, nda.work->name + nda.work->name_len - nda.work->basename_len,
                    nda.work, &nda.ed, &nda.summary);

        /* run plugin before destroying data */
        if (pa->in.plugin_ops->process_dir) {
            pa->in.plugin_ops->process_dir(&pcs, nda.plugin_user_data);
        }

        if (nda.in->process_xattrs) {
            xattrs_cleanup(&nda.ed.xattrs);
        }

        if (pa->in.plugin_ops->ctx_exit) {
            pa->in.plugin_ops->ctx_exit(&pcs, nda.plugin_user_data);
        }

        closedb(nda.db);
        nda.db = NULL;
    }

    /* ignore errors */
    chmod(nda.topath.data, nda.work->statuso.st_mode);
    chown(nda.topath.data, nda.work->statuso.st_uid, nda.work->statuso.st_gid);

  close_dir:
    closedir(dir);

  cleanup:
    if (process_dir) {
        const size_t id = QPTPool_get_id(ctx);
        pa->total_dirs[id]++;
        pa->total_nondirs[id] += ctrs.nondirs_processed;
    }

    free(nda.topath.data);
    free(nda.work);

    return rc;
}

/* set up parent for a single subtree root in the index and enqueue subtree root as normal work */
static int process_subtree_root(QPTPool_ctx_t *ctx, void *data) {
    struct work *subtree_root = (struct work *) data;
    struct PoolArgs *pa = (struct PoolArgs *) QPTPool_get_args_internal(ctx);

    /* offset by root_parent.len to remove prefix */
    const size_t topath_len = pa->index_parent.len + 1 + subtree_root->name_len - subtree_root->root_parent.len;

    char *topath = malloc(topath_len + 1);
    SNFORMAT_S(topath, topath_len + 1, 3,
               pa->index_parent.data, pa->index_parent.len,
               "/", (size_t) 1,
               subtree_root->name + subtree_root->root_parent.len, subtree_root->name_len - subtree_root->root_parent.len);

    /*
     * create directories up to parent with corrrect permissions and owners
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
                free(topath);
                free(subtree_root);
                return 1;
            }
        }

        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "Error: Subtree root parent is not a directory \"%s\"\n", topath);
            free(topath);
            free(subtree_root);
            return 1;
        }

        if (mkdir(topath, st.st_mode) == -1) {
            const int err = errno;
            if (err != EEXIST) {
                fprintf(stderr, "Error: Could not make subtree root parent \"%s\": %s (%d)\n",
                        topath, strerror(err), err);
                *p = '/';
                free(topath);
                free(subtree_root);
                return 1;
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
}

/*
 * create the target directory
 *
 * note that the provided directories go into
 * individual directories underneath this one
 */
static int setup_dst(const char *index_parent) {
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

static int validate_source(refstr_t *index_parent, const char *path, struct work **work) {
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

    new_work->root_parent.data = path;
    new_work->root_parent.len = dirname_len(path, new_work->name_len);
    new_work->level = 0;
    new_work->basename_len = new_work->name_len - new_work->root_parent.len;
    new_work->root_basename_len = new_work->basename_len;

    char expathin[MAXPATH];
    char expathout[MAXPATH];
    char expathtst[MAXPATH];

    SNPRINTF(expathtst, MAXPATH,"%s/%s", index_parent->data, new_work->root_parent.data + new_work->root_parent.len);
    realpath(expathtst, expathout);
    realpath(new_work->root_parent.data, expathin);

    if (!strcmp(expathin, expathout)) {
        fprintf(stderr,"You are putting the index dbs in input directory\n");
    }

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
        FLAG_CHECK_EXTDB_VALID, FLAG_PLUGIN,

        /* memory usage flags */
        FLAG_TARGET_MEMORY, FLAG_SWAP_PREFIX, FLAG_SUBDIR_LIMIT,
        #ifdef HAVE_ZLIB
        FLAG_COMPRESS,
        #endif

        FLAG_END
    };

    struct PoolArgs pa;
    process_args_and_maybe_exit(options, 1, "dir... GUFI_tree_parent", &pa.in);

    /* parse positional args, following the options */
    /* does not have to be canonicalized */
    INSTALL_STR(&pa.index_parent, argv[argc - 1]);

    int rc = EXIT_SUCCESS;

    if (check_plugin(pa.in.plugin_ops, PLUGIN_INDEX) != 1) {
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    if (pa.in.plugin_ops->global_init) {
        pa.in.plugin_ops->global_init(&pa.in);
    }

    argc--; /* index parent is no longer needed */
    const size_t root_count = argc - idx;

    if (bad_partial_walk(&pa.in, root_count)){
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    if (setup_dst(pa.index_parent.data) != 0) {
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    init_template_db(&pa.db);
    if (create_dbdb_template(&pa.db) != 0) {
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
    if (create_xattrs_template(&pa.xattr) != 0) {
        fprintf(stderr, "Could not create xattr template file\n");
        rc = EXIT_FAILURE;
        goto free_db;
    }

    const uint64_t queue_limit = get_queue_limit(pa.in.target_memory, pa.in.maxthreads);
    QPTPool_ctx_t *ctx = QPTPool_init_with_props(pa.in.maxthreads, &pa, NULL, NULL, queue_limit, pa.in.swap_prefix.data, 1, 2);
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
            if (validate_source(&pa.index_parent, argv[idx], &root) == 0) {
                process_path_list(&pa.in, root, ctx, process_subtree_root);
            }
            else {
                rc = EXIT_FAILURE;
            }
        }
    }
    else {
        if (root_count) {
            for(int i = idx; i < argc; i++) {
                /* get first work item by validating source path */
                struct work *root = NULL;
                if (validate_source(&pa.index_parent, argv[i], &root) != 0) {
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
    if (pa.in.plugin_ops->global_exit) {
        pa.in.plugin_ops->global_exit(&pa.in);
    }

    input_fini(&pa.in);

    dump_memory_usage(stderr);

    return rc;
}
