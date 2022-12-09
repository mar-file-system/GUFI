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
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "bf.h"
#include "dbutils.h"
#include "debug.h"
#include "template_db.h"
#include "trie.h"
#include "utils.h"

extern int errno;

struct PoolArgs {
    struct input in;

    trie_t *skip;
    struct template_db db;
    struct template_db xattr;

    #if BENCHMARK
    /* locked by print_mutex in debug.h */
    size_t total_dirs;
    size_t total_files;
    #endif
};

struct NonDirArgs {
    struct input *in;

    /* thread args */
    struct template_db *temp_db;
    struct template_db *temp_xattr;
    struct work *work;

    /* index path */
    char topath[MAXPATH];
    size_t topath_len;

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
};

static int process_nondir(struct work *entry, void *args) {
    struct NonDirArgs *nda = (struct NonDirArgs *) args;
    struct input *in = nda->in;

    if (in->external_enabled) {
        insertdbgo_xattrs(in, &nda->work->statuso, entry,
                          &nda->xattr_db_list, nda->temp_xattr,
                          nda->topath, nda->topath_len,
                          nda->xattrs_res, nda->xattr_files_res);
    }

    /* get entry relative path (use extra buffer to prevent memcpy overlap) */
    char relpath[MAXPATH];
    const size_t relpath_len = SNFORMAT_S(relpath, MAXPATH, 1,
                                          entry->name + entry->root_len + 1, entry->name_len - entry->root_len - 1);

    /* overwrite full path with relative path */
    /* e.name_len = */ SNFORMAT_S(entry->name, MAXPATH, 1, relpath, relpath_len);

    /* update summary table */
    sumit(&nda->summary, entry);

    /* add entry + xattr names into bulk insert */
    insertdbgo(entry, nda->db, nda->entries_res);

    return 0;
}

static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    /* skip argument checking */
    /* if (!data) { */
    /*     return 1; */
    /* } */

    /* if (!ctx || (id >= ctx->size)) { */
    /*     free(data); */
    /*     return 1; */
    /* } */
    int rc = 0;

    struct PoolArgs *pa = (struct PoolArgs *) args;
    struct input *in = &pa->in;

    #if BENCHMARK
    pthread_mutex_lock(&print_mutex);
    ta->total_dirs++;
    pthread_mutex_unlock(&print_mutex);
    #endif

    struct NonDirArgs nda;
    nda.in         = &pa->in;
    nda.temp_db    = &pa->db;
    nda.temp_xattr = &pa->xattr;
    nda.work       = (struct work *) data;

    DIR *dir = opendir(nda.work->name);
    if (!dir) {
        fprintf(stderr, "Could not open directory \"%s\"\n", nda.work->name);
        rc = 1;
        goto cleanup;
    }

    /* offset by work->root_len to remove prefix */
    nda.topath_len = SNFORMAT_S(nda.topath, MAXPATH, 3,
                                in->nameto, in->nameto_len,
                                "/", (size_t) 1,
                                nda.work->name + nda.work->root_len, nda.work->name_len - nda.work->root_len);

    /* don't need recursion because parent is guaranteed to exist */
    if (mkdir(nda.topath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) < 0) {
        const int err = errno;
        if (err != EEXIST) {
            fprintf(stderr, "mkdir %s failure: %d %s\n", nda.topath, err, strerror(err));
            rc = 1;
            goto cleanup;
        }
    }

    /* create the database name */
    char dbname[MAXPATH];
    SNFORMAT_S(dbname, MAXPATH, 3,
               nda.topath, nda.topath_len,
               "/", (size_t) 1,
               DBNAME, DBNAME_LEN);

    /* copy the template file */
    if (copy_template(nda.temp_db, dbname, nda.work->statuso.st_uid, nda.work->statuso.st_gid)) {
        rc = 1;
        goto cleanup;
    }

    nda.db = opendb(dbname, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 1, 0
                    , NULL, NULL
                    #if defined(DEBUG) && defined(PER_THREAD_STATS)
                    , NULL, NULL
                    , NULL, NULL
                    #endif
        );
    if (!nda.db) {
        rc = 1;
        goto cleanup;
    }

    /* prepare to insert into the database */
    zeroit(&nda.summary);

    /* prepared statements within db.db */
    nda.entries_res = insertdbprep(nda.db, ENTRIES_INSERT);
    nda.xattrs_res = NULL;
    nda.xattr_files_res = NULL;

    if (in->external_enabled) {
        nda.xattrs_res = insertdbprep(nda.db, XATTRS_PWD_INSERT);
        nda.xattr_files_res = insertdbprep(nda.db, EXTERNAL_DBS_PWD_INSERT);

        /* external per-user and per-group dbs */
        sll_init(&nda.xattr_db_list);
    }

    startdb(nda.db);
    size_t nondirs_processed = 0;
    descend(ctx, id, in, nda.work, dir, pa->skip, 0, 1,
            processdir, process_nondir, &nda,
            NULL, NULL, &nondirs_processed);
    stopdb(nda.db);

    /* entries and xattrs have been inserted */

    if (in->external_enabled) {
        /* write out per-user and per-group xattrs */
        sll_destroy(&nda.xattr_db_list, destroy_xattr_db);

        /* keep track of per-user and per-group xattr dbs */
        insertdbfin(nda.xattr_files_res);

        /* pull this directory's xattrs because they were not pulled by the parent */
        xattrs_setup(&nda.work->xattrs);
        xattrs_get(nda.work->name, &nda.work->xattrs);

        /* directory xattrs go into the same table as entries xattrs */
        insertdbgo_xattrs_avail(nda.work, nda.xattrs_res);
        insertdbfin(nda.xattrs_res);
    }
    insertdbfin(nda.entries_res);

    /* insert this directory's summary data */
    /* the xattrs go into the xattrs_avail table in db.db */
    insertsumdb(nda.db, nda.work->name + nda.work->name_len - nda.work->basename_len, nda.work, &nda.summary);
    if (in->external_enabled) {
        xattrs_cleanup(&nda.work->xattrs);
    }

    closedb(nda.db);
    nda.db = NULL;

    /* ignore errors */
    chmod(nda.topath, nda.work->statuso.st_mode);
    chown(nda.topath, nda.work->statuso.st_uid, nda.work->statuso.st_gid);

    closedir(dir);

  cleanup:

    free(nda.work);

    #if BENCHMARK
    pthread_mutex_lock(&print_mutex);
    pa->total_files += nondirs_processed;
    pthread_mutex_unlock(&print_mutex);
    #endif

    return rc;
}

/*
 * create the target directory
 *
 * note that the provided directories go into
 * individual directories underneath this one
 */
static int setup_dst(const char *nameto) {
    /* check if the destination path already exists (not an error) */
    struct stat dst_st;
    if (lstat(nameto, &dst_st) == 0) {
        fprintf(stderr, "\"%s\" Already exists!\n", nameto);

        /* if the destination path is not a directory (error) */
        if (!S_ISDIR(dst_st.st_mode)) {
            fprintf(stderr, "Destination path is not a directory \"%s\"\n", nameto);
            return -1;
        }

        return 0;
    }

    struct stat st;
    st.st_mode = S_IRWXU | S_IRWXG | S_IRWXO;
    st.st_uid = geteuid();
    st.st_gid = getegid();

    if (dupdir(nameto, &st)) {
        fprintf(stderr, "Could not create %s\n", nameto);
        return -1;
    }

    return 0;
}

struct work *validate_source(struct input *in, char *path) {
    struct work *root = (struct work *) calloc(1, sizeof(struct work));
    if (!root) {
        fprintf(stderr, "Could not allocate root struct\n");
        return NULL;
    }

    /* get input path metadata */
    if (lstat(path, &root->statuso) < 0) {
        fprintf(stderr, "Could not stat source directory \"%s\"\n", path);
        free(root);
        return NULL;
    }

    /* check that the input path is a directory */
    if (!S_ISDIR(root->statuso.st_mode)) {
        fprintf(stderr, "Source path is not a directory \"%s\"\n", path);
        free(root);
        return NULL;
    }

    root->name_len = SNFORMAT_S(root->name, MAXPATH, 1, path, strlen(path));
    root->root = path;
    root->root_len = dirname_len(path, root->name_len);
    root->type[0] = 'd';

    char expathin[MAXPATH];
    char expathout[MAXPATH];
    char expathtst[MAXPATH];

    SNPRINTF(expathtst, MAXPATH,"%s/%s", in->nameto, root->root + root->root_len);
    realpath(expathtst, expathout);
    realpath(root->root, expathin);

    if (!strcmp(expathin, expathout)) {
        fprintf(stderr,"You are putting the index dbs in input directory\n");
    }

    return root;
}

void sub_help() {
   printf("input_dir...      walk one or more trees to produce GUFI index\n");
   printf("output_dir        build GUFI index here\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    struct PoolArgs pa;
    int idx = parse_cmd_line(argc, argv, "hHn:xz:k:", 2, "input_dir... output_dir", &pa.in);
    if (pa.in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        /* parse positional args, following the options */
        int retval = 0;

        /* does not have to be canonicalized */
        INSTALL_STR(pa.in.nameto, argv[argc - 1], MAXPATH, "output_dir");

        if (retval)
            return retval;

        if (setup_directory_skip(pa.in.skip, &pa.skip) != 0) {
            return -1;
        }

        pa.in.nameto_len = strlen(pa.in.nameto);
    }

    #if BENCHMARK
    fprintf(stderr, "Creating GUFI Index %s with %d threads\n", in.nameto, in.maxthreads);
    #endif

    if (setup_dst(pa.in.nameto) != 0) {
        trie_free(pa.skip);
        return -1;
    }

    init_template_db(&pa.db);
    if (create_dbdb_template(&pa.db) != 0) {
        fprintf(stderr, "Could not create template file\n");
        trie_free(pa.skip);
        return -1;
    }

    init_template_db(&pa.xattr);
    if (create_xattrs_template(&pa.xattr) != 0) {
        fprintf(stderr, "Could not create xattr template file\n");
        close_template_db(&pa.db);
        trie_free(pa.skip);
        return -1;
    }

    #if BENCHMARK
    struct start_end benchmark;
    clock_gettime(CLOCK_MONOTONIC, &benchmark.start);

    pa.total_dirs = 0;
    pa.total_files = 0;
    #endif

    QPTPool_t *pool = QPTPool_init(pa.in.maxthreads, &pa, NULL, NULL
                                   #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                   , NULL
                                   #endif
        );
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        close_template_db(&pa.xattr);
        close_template_db(&pa.db);
        trie_free(pa.skip);
        return -1;
    }

    const size_t root_count = argc - idx - 1;
    char **roots = calloc(root_count, sizeof(char *));
    for(size_t i = 0; idx < (argc - 1);) {
        /* force all input paths to be canonical */
        roots[i] = realpath(argv[idx++], NULL);
        if (!roots[i]) {
            const int err = errno;
            fprintf(stderr, "Could not resolve path \"%s\": %s (%d)\n",
                    argv[idx - 1], strerror(err), err);
            continue;
        }

        /* get first work item by validating source path */
        struct work *root = validate_source(&pa.in, roots[i]);
        if (!root) {
            continue;
        }

        /*
         * manually get basename of provided path since
         * there is no source for the basenames
         */
        root->basename_len = 0;
        while ((root->basename_len < root->name_len) &&
               (root->name[root->name_len - root->basename_len - 1] != '/')) {
            root->basename_len++;
        }

        QPTPool_enqueue(pool, 0, processdir, root);
        i++;
    }

    QPTPool_wait(pool);
    QPTPool_destroy(pool);

    for(size_t i = 0; i < root_count; i++) {
        free(roots[i]);
    }
    free(roots);
    close_template_db(&pa.xattr);
    close_template_db(&pa.db);
    trie_free(pa.skip);

    #if BENCHMARK
    clock_gettime(CLOCK_MONOTONIC, &benchmark.end);
    const long double processtime = sec(nsec(&benchmark));

    fprintf(stderr, "Total Dirs:            %zu\n",    pa.total_dirs);
    fprintf(stderr, "Total Files:           %zu\n",    pa.total_files);
    fprintf(stderr, "Time Spent Indexing:   %.2Lfs\n", processtime);
    fprintf(stderr, "Dirs/Sec:              %.2Lf\n",  pa.total_dirs / processtime);
    fprintf(stderr, "Files/Sec:             %.2Lf\n",  pa.total_files / processtime);
    #endif

    return 0;
}
