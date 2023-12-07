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
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "bf.h"
#include "dbutils.h"
#include "template_db.h"
#include "trie.h"
#include "utils.h"

struct PoolArgs {
    struct input in;

    trie_t *skip;
    struct template_db db;
    struct template_db xattr;

    uint64_t *total_files;
};

struct NonDirArgs {
    struct input *in;

    /* thread args */
    struct template_db *temp_db;
    struct template_db *temp_xattr;
    struct work *work;
    struct entry_data ed;

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

static int process_nondir(struct work *entry, struct entry_data *ed, void *args) {
    struct NonDirArgs *nda = (struct NonDirArgs *) args;
    struct input *in = nda->in;

    if (lstat(entry->name, &ed->statuso) != 0) {
        return 1;
    }

    if (ed->type == 'l') {
        readlink(entry->name, ed->linkname, MAXPATH);
        /* error? */
    }

    if (in->external_enabled) {
        insertdbgo_xattrs(in, &nda->ed.statuso, ed,
                          &nda->xattr_db_list, nda->temp_xattr,
                          nda->topath, nda->topath_len,
                          nda->xattrs_res, nda->xattr_files_res);
    }

    /* get entry relative path (use extra buffer to prevent memcpy overlap) */
    char relpath[MAXPATH];
    const size_t relpath_len = SNFORMAT_S(relpath, MAXPATH, 1,
                                          entry->name + entry->root_parent.len, entry->name_len - entry->root_parent.len);
    /* overwrite full path with relative path */
    entry->name_len = SNFORMAT_S(entry->name, MAXPATH, 1, relpath, relpath_len);

    /* update summary table */
    sumit(&nda->summary, ed);

    /* add entry + xattr names into bulk insert */
    insertdbgo(entry, ed, nda->entries_res);

    return 0;
}

static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    /* Not checking arguments */

    int rc = 0;

    struct PoolArgs *pa = (struct PoolArgs *) args;
    struct input *in = &pa->in;

    struct work work_src;
    size_t nondirs_processed = 0;

    struct NonDirArgs nda;
    nda.in         = &pa->in;
    nda.temp_db    = &pa->db;
    nda.temp_xattr = &pa->xattr;
    nda.work       = (struct work *) data;
    memset(&nda.ed, 0, sizeof(nda.ed));
    nda.ed.type    = 'd';

    DIR *dir = NULL;

    if (nda.work->compressed.yes) {
        nda.work = &work_src;
        decompress_struct((void **) &nda.work, data, sizeof(work_src));
    }

    if (lstat(nda.work->name, &nda.ed.statuso) != 0) {
        fprintf(stderr, "Could not stat directory \"%s\"\n", nda.work->name);
        rc = 1;
        goto cleanup;
    }

    dir = opendir(nda.work->name);
    if (!dir) {
        fprintf(stderr, "Could not open directory \"%s\"\n", nda.work->name);
        rc = 1;
        goto cleanup;
    }

    /* offset by work->root_len to remove prefix */
    nda.topath_len = SNFORMAT_S(nda.topath, MAXPATH, 3,
                                in->nameto.data, in->nameto.len,
                                "/", (size_t) 1,
                                nda.work->name + nda.work->root_parent.len, nda.work->name_len - nda.work->root_parent.len);

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

    nda.db = template_to_db(nda.temp_db, dbname, nda.ed.statuso.st_uid, nda.ed.statuso.st_gid);
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
    descend(ctx, id, pa, in, nda.work, nda.ed.statuso.st_ino, dir, pa->skip, 0, 0,
            processdir, process_nondir, &nda, NULL, NULL, NULL, &nondirs_processed);
    stopdb(nda.db);

    /* entries and xattrs have been inserted */

    if (in->external_enabled) {
        /* write out per-user and per-group xattrs */
        sll_destroy(&nda.xattr_db_list, destroy_xattr_db);

        /* keep track of per-user and per-group xattr dbs */
        insertdbfin(nda.xattr_files_res);

        /* pull this directory's xattrs because they were not pulled by the parent */
        xattrs_setup(&nda.ed.xattrs);
        xattrs_get(nda.work->name, &nda.ed.xattrs);

        /* directory xattrs go into the same table as entries xattrs */
        insertdbgo_xattrs_avail(&nda.ed, nda.xattrs_res);
        insertdbfin(nda.xattrs_res);
    }
    insertdbfin(nda.entries_res);

    /* insert this directory's summary data */
    /* the xattrs go into the xattrs_avail table in db.db */
    insertsumdb(nda.db, nda.work->name + nda.work->name_len - nda.work->basename_len,
                nda.work, &nda.ed, &nda.summary);
    if (in->external_enabled) {
        xattrs_cleanup(&nda.ed.xattrs);
    }

    closedb(nda.db);
    nda.db = NULL;

    /* ignore errors */
    chmod(nda.topath, nda.ed.statuso.st_mode);
    chown(nda.topath, nda.ed.statuso.st_uid, nda.ed.statuso.st_gid);

  cleanup:
    closedir(dir);

    free_struct(nda.work, data, nda.work->recursion_level);

    pa->total_files[id] += nondirs_processed;

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

static int validate_source(struct input *in, const char *path, struct work *work) {
    memset(work, 0, sizeof(*work));

    /* get input path metadata */
    struct stat st;
    if (lstat(path, &st) < 0) {
        fprintf(stderr, "Could not stat source directory \"%s\"\n", path);
        return 1;
    }

    /* check that the input path is a directory */
    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Source path is not a directory \"%s\"\n", path);
        return 1;
    }

    work->name_len = SNFORMAT_S(work->name, MAXPATH, 1, path, strlen(path));
    work->root_parent.data = path;
    work->root_parent.len = dirname_len(path, work->name_len);

    char expathin[MAXPATH];
    char expathout[MAXPATH];
    char expathtst[MAXPATH];

    SNPRINTF(expathtst, MAXPATH,"%s/%s", in->nameto.data, work->root_parent.data + work->root_parent.len);
    realpath(expathtst, expathout);
    realpath(work->root_parent.data, expathin);

    if (!strcmp(expathin, expathout)) {
        fprintf(stderr,"You are putting the index dbs in input directory\n");
    }

    return 0;
}

static void sub_help(void) {
   printf("input_dir...      walk one or more trees to produce GUFI index\n");
   printf("output_dir        build GUFI index here\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    struct PoolArgs pa;
    int idx = parse_cmd_line(argc, argv, "hHn:xz:k:M:C:" COMPRESS_OPT, 2, "input_dir... output_dir", &pa.in);
    if (pa.in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        /* parse positional args, following the options */
        /* does not have to be canonicalized */
        INSTALL_STR(&pa.in.nameto, argv[argc - 1]);

        if (setup_directory_skip(pa.in.skip.data, &pa.skip) != 0) {
            return -1;
        }
    }

    int rc = 0;

    if (setup_dst(pa.in.nameto.data) != 0) {
        rc = -1;
        goto free_skip;
    }

    init_template_db(&pa.db);
    if (create_dbdb_template(&pa.db) != 0) {
        fprintf(stderr, "Could not create template file\n");
        rc = -1;
        goto free_skip;
    }

    /*
     * create empty db.db in index parent (this file is placed in
     * "${dst}/db.db"; index is placed in "${dst}/$(basename ${src}))"
     * so that when querying "${dst}", no error is printed
     */
    if (create_empty_dbdb(&pa.db, &pa.in.nameto, geteuid(), getegid()) != 0) {
        rc = -1;
        goto free_skip;
    }

    init_template_db(&pa.xattr);
    if (create_xattrs_template(&pa.xattr) != 0) {
        fprintf(stderr, "Could not create xattr template file\n");
        rc = -1;
        goto free_db;
    }

    const uint64_t queue_depth = pa.in.target_memory_footprint / sizeof(struct work) / pa.in.maxthreads;
    QPTPool_t *pool = QPTPool_init_with_props(pa.in.maxthreads, &pa, NULL, NULL, queue_depth, 1, 2
                                              #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                              , NULL
                                              #endif
        );
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        rc = -1;
        goto free_xattr;
    }

    fprintf(stdout, "Creating GUFI Index %s with %zu threads\n", pa.in.nameto.data, pa.in.maxthreads);

    pa.total_files = calloc(pa.in.maxthreads, sizeof(uint64_t));

    struct start_end after_init;
    clock_gettime(CLOCK_MONOTONIC, &after_init.start);

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
        struct work root;
        if (validate_source(&pa.in, roots[i], &root) != 0) {
            continue;
        }

        /*
         * manually get basename of provided path since
         * there is no source for the basenames
         */
        root.basename_len = root.name_len - root.root_parent.len;

        struct work *copy = compress_struct(pa.in.compress, &root, sizeof(root));
        QPTPool_enqueue(pool, 0, processdir, copy);
        i++;
    }

    QPTPool_wait(pool);

    clock_gettime(CLOCK_MONOTONIC, &after_init.end);
    const long double processtime = sec(nsec(&after_init));

    /* don't count as part of processtime */

    const uint64_t thread_count = QPTPool_threads_completed(pool);

    QPTPool_destroy(pool);

    for(size_t i = 0; i < root_count; i++) {
        free(roots[i]);
    }
    free(roots);

    uint64_t total_files = 0;
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        total_files += pa.total_files[i];
    }

    free(pa.total_files);

    fprintf(stdout, "Total Dirs:          %" PRIu64 "\n", thread_count);
    fprintf(stdout, "Total Files:         %" PRIu64 "\n", total_files);
    fprintf(stdout, "Time Spent Indexing: %.2Lfs\n",      processtime);
    fprintf(stdout, "Dirs/Sec:            %.2Lf\n",       thread_count / processtime);
    fprintf(stdout, "Files/Sec:           %.2Lf\n",       total_files / processtime);

  free_xattr:
    close_template_db(&pa.xattr);
  free_db:
    close_template_db(&pa.db);
  free_skip:
    trie_free(pa.skip);

    return rc;
}
