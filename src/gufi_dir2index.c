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

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    uint64_t *total_files;
    #endif
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
        insertdbgo_xattrs(in, &nda->ed.statuso, entry, ed,
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
    sumit(&nda->summary, ed);

    /* add entry + xattr names into bulk insert */
    insertdbgo(entry, ed, nda->db, nda->entries_res);

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
    nda.work       = &work_src;
    memset(&nda.ed, 0, sizeof(nda.ed));
    nda.ed.type    = 'd';

    decompress_struct(in->compress, data, (void **) &nda.work, &work_src, sizeof(work_src));

    DIR *dir = opendir(nda.work->name);
    if (!dir) {
        fprintf(stderr, "Could not open directory \"%s\"\n", nda.work->name);
        rc = 1;
        goto cleanup;
    }

    if (lstat(nda.work->name, &nda.ed.statuso) != 0) {
        fprintf(stderr, "Could not stat directory \"%s\"\n", nda.work->name);
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
    if (copy_template(nda.temp_db, dbname, nda.ed.statuso.st_uid, nda.ed.statuso.st_gid)) {
        rc = 1;
        goto cleanup;
    }

    /*
     * don't need to convert dbname because sqlite3_open_v2
     * does not interpret strings as SQL statements
     */
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

    free_struct(in->compress, nda.work, &work_src, nda.work->recursion_level);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    pa->total_files[id] += nondirs_processed;
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

int validate_source(struct input *in, char *path, struct work *work) {
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
    work->root = path;
    work->root_len = dirname_len(path, work->name_len);

    char expathin[MAXPATH];
    char expathout[MAXPATH];
    char expathtst[MAXPATH];

    SNPRINTF(expathtst, MAXPATH,"%s/%s", in->nameto, work->root + work->root_len);
    realpath(expathtst, expathout);
    realpath(work->root, expathin);

    if (!strcmp(expathin, expathout)) {
        fprintf(stderr,"You are putting the index dbs in input directory\n");
    }

    return 0;
}

void sub_help() {
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

    const uint64_t queue_depth = pa.in.target_memory_footprint / sizeof(struct work) / pa.in.maxthreads;
    QPTPool_t *pool = QPTPool_init(pa.in.maxthreads, &pa, NULL, NULL, queue_depth
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

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    fprintf(stderr, "Creating GUFI Index %s with %d threads\n", pa.in.nameto, pa.in.maxthreads);

    pa.total_files = calloc(pa.in.maxthreads, sizeof(uint64_t));

    struct start_end after_init;
    clock_gettime(CLOCK_MONOTONIC, &after_init.start);
    #endif


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
        root.basename_len = 0;
        while ((root.basename_len < root.name_len) &&
               (root.name[root.name_len - root.basename_len - 1] != '/')) {
            root.basename_len++;
        }

        struct work *copy = compress_struct(pa.in.compress, &root, sizeof(root));
        QPTPool_enqueue(pool, 0, processdir, copy);
        i++;
    }

    QPTPool_wait(pool);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    const uint64_t thread_count = QPTPool_threads_completed(pool);
    #endif

    QPTPool_destroy(pool);

    for(size_t i = 0; i < root_count; i++) {
        free(roots[i]);
    }
    free(roots);
    close_template_db(&pa.xattr);
    close_template_db(&pa.db);
    trie_free(pa.skip);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    clock_gettime(CLOCK_MONOTONIC, &after_init.end);
    const long double processtime = sec(nsec(&after_init));

    /* don't count as part of processtime */
    uint64_t total_files = 0;
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        total_files += pa.total_files[i];
    }

    free(pa.total_files);

    fprintf(stderr, "Total Dirs:            %zu\n",    thread_count);
    fprintf(stderr, "Total Files:           %zu\n",    total_files);
    fprintf(stderr, "Time Spent Indexing:   %.2Lfs\n", processtime);
    fprintf(stderr, "Dirs/Sec:              %.2Lf\n",  thread_count / processtime);
    fprintf(stderr, "Files/Sec:             %.2Lf\n",  total_files / processtime);
    #endif

    return 0;
}
