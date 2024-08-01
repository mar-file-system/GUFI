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
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "dbutils.h"
#include "external.h"
#include "utils.h"
#include "xattrs.h"

struct PoolArgs {
    struct input in;
    size_t src_dirname_len;
};

static const char SELECT_SUMMARY[] =
    "SELECT inode, mode, uid, gid, atime, mtime, xattr_names, LENGTH(xattr_names) FROM " SUMMARY " WHERE isroot == 1;";

/*
 * entries not pentries because not using rolled up data
 *
 * size is not selected because created file is not filled
 */
static const char SELECT_ENTRIES[] =
    "SELECT name, type, inode, mode, uid, gid, atime, mtime, linkname, xattr_names, LENGTH(xattr_names) FROM " ENTRIES ";";

struct DirCallbackArgs {
    struct input *in;
    const char *nameto;
    size_t nameto_len;
    sqlite3 *db;
};

/* get single xattr pair */
static int get_xattr(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    struct xattrs *xattrs = (struct xattrs *) args;
    struct xattr  *xattr  = &xattrs->pairs[xattrs->count];

    const char *name  = data[0];
    const char *value = data[1];

    xattr->name_len  = SNFORMAT_S(xattr->name,  sizeof(xattr->name),  1, name,  strlen(name));
    xattr->value_len = SNFORMAT_S(xattr->value, sizeof(xattr->value), 1, value, strlen(value));
    xattrs->name_len += xattr->name_len;
    xattrs->len += xattr->name_len + xattr->value_len;
    xattrs->count++;

    return 0;
}

/* get all xattrs pairs for this entry */
static void get_xattrs(struct DirCallbackArgs *dcba,
                       const char *name,
                       const char *inode,
                       const char *view,
                       const char *xattr_names, const size_t xattr_names_len,
                       struct xattrs *xattrs) {
    /* xattr_names never returns NULL */
    if (!xattr_names_len) {
        return;
    }

    for(size_t i = 0; i < xattr_names_len; i++) {
        if (xattr_names[i] == XATTRDELIM) {
            xattrs->count++;
        }
    }

    xattrs_alloc(xattrs);

    xattrs->count = 0; /* use this as the index when filling in */

    char xattrs_sql[MAXSQL];
    SNPRINTF(xattrs_sql, sizeof(xattrs_sql),
             "SELECT xattr_name, xattr_value FROM %s WHERE inode == %s;", view, inode);

    char *err = NULL;
    if (sqlite3_exec(dcba->db, xattrs_sql, get_xattr, xattrs, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Could get xattrs of %s: %s\n", name, err);
    }

    /* skip double checking xattrs->count matches first count */
}

static int process_summary(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    struct DirCallbackArgs *dcba = (struct DirCallbackArgs *) args;

    struct stat st;
    int64_t t;
    size_t xattr_names_len = 0;
    const char *inode = data[0];
    /* sscanf(data[0], "%" STAT_inode, &st.st_ino); */
    sscanf(data[1], "%" STAT_mode,  &st.st_mode);
    sscanf(data[2], "%" STAT_uid,   &st.st_uid);
    sscanf(data[3], "%" STAT_gid,   &st.st_gid);
    sscanf(data[4], "%" PRId64,     &t); st.st_atime = t;
    sscanf(data[5], "%" PRId64,     &t); st.st_mtime = t;
    char *xattr_names = data[6];
    sscanf(data[7], "%zu",         &xattr_names_len);

    struct xattrs xattrs;
    xattrs_setup(&xattrs);

    if (dcba->in->process_xattrs) {
        get_xattrs(dcba, dcba->nameto, inode, XSUMMARY,
                   xattr_names, xattr_names_len, &xattrs);
    }

    set_metadata(dcba->nameto, &st, &xattrs);

    xattrs_cleanup(&xattrs);

    return 0;
}

static int process_entries(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    struct DirCallbackArgs *dcba = (struct DirCallbackArgs *) args;

    const char *name = data[0];
    const char type = data[1][0];

    char entry[MAXPATH];
    SNFORMAT_S(entry, sizeof(entry), 3,
               dcba->nameto, dcba->nameto_len,
               "/", (size_t) 1,
               name, strlen(name));

    switch (type) {
        case 'f':
            ;
            struct stat st;
            int64_t t;
            size_t xattr_names_len = 0;
            const char *inode = data[2];
            /* sscanf(data[2], "%" STAT_ino,  &st.st_ino); */
            sscanf(data[3], "%" STAT_mode, &st.st_mode);
            sscanf(data[4], "%" STAT_uid,  &st.st_uid);
            sscanf(data[5], "%" STAT_gid,  &st.st_gid);
            sscanf(data[6], "%" PRId64,    &t); st.st_atime = t;
            sscanf(data[7], "%" PRId64,    &t); st.st_mtime = t;
            /* const char *linkname = data[8]; */
            char *xattr_names = data[9];
            sscanf(data[10], "%zu",         &xattr_names_len);

            int fd = open(entry, O_WRONLY | O_TRUNC | O_CREAT, st.st_mode);
            if (fd < 0) {
                const int err = errno;
                fprintf(stderr, "Error opening file %s: %d\n", entry, err);
            }
            close(fd);

            struct xattrs xattrs;
            xattrs_setup(&xattrs);

            if (dcba->in->process_xattrs) {
                get_xattrs(dcba, entry, inode, XENTRIES,
                           xattr_names, xattr_names_len, &xattrs);
            }

            set_metadata(entry, &st, &xattrs);

            xattrs_cleanup(&xattrs);

            break;
        case 'l':
            ;
            const char *linkname = data[8];
            if (symlink(linkname, entry) < 0) {
                const int err = errno;
                if (err != EEXIST) {
                    fprintf(stderr, "Error creating symlink %s: %s (%d)\n",
                            entry, strerror(err), err);
                }
            }
            break;
        /* case 'd': */
        /* default: */
        /*     fprintf(stderr, "Bad entry type: %c\n", type); */
        /*     return 1; */
    }

    return 0;
}

static int processdir(struct QPTPool * ctx, const size_t id, void * data, void * args) {
    struct work * work = (struct work *) data;
    struct PoolArgs *pa = (struct PoolArgs *) args;

    sqlite3 *db = NULL;
    int rc = 0;

    DIR *dir = opendir(work->name);
    if (!dir) {
        fprintf(stderr, "Could not open directory \"%s\"\n", work->name);
        rc = 1;
        goto cleanup;
    }

    // get source directory info
    struct stat dir_st;
    if (lstat(work->name, &dir_st) < 0)  {
        rc = 1;
        goto cleanup;
    }

    // create the destination directory using the source directory
    char topath[MAXPATH];
    const size_t topath_len = SNFORMAT_S(topath, MAXPATH, 3,
                                         pa->in.nameto.data, pa->in.nameto.len,
                                         "/", (size_t) 1,
                                         work->name + pa->src_dirname_len, work->name_len - pa->src_dirname_len);

    rc = mkdir(topath, dir_st.st_mode); /* don't need recursion because parent is guaranteed to exist */
    if (rc < 0) {
        const int err = errno;
        if (err != EEXIST) {
            fprintf(stderr, "mkdir %s failure: %d %s\n", topath, err, strerror(err));
            rc = 1;
            goto cleanup;
        }
    }

    descend(ctx, id, args, &pa->in, work, dir_st.st_ino,
            dir, pa->in.skip, 1,
            processdir, NULL, NULL,
            NULL);

    /* open the index db.db */
    char dbname[MAXPATH];
    SNFORMAT_S(dbname, MAXPATH, 2,
               work->name, work->name_len,
               "/" DBNAME, (size_t) (DBNAME_LEN + 1));

    db = opendb(dbname, SQLITE_OPEN_READONLY, 0, 0, NULL, NULL);
    if (!db) {
        rc = 1;
        goto cleanup;
    }

    size_t ext_xattrs = 0;

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    size_t queries = 0;
    #endif

    if (pa->in.process_xattrs) {
        #if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
        struct start_end xattrprep_call; /* not used after setup_xattrs_views */
        #endif
        setup_xattrs_views(&pa->in, db,
                           work, &ext_xattrs
                           #if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
                           , &xattrprep_call
                           #endif
                           #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                           , &queries
                           #endif
            );
    }

    struct DirCallbackArgs dcba = {
        .in = &pa->in,
        .nameto = topath,
        .nameto_len = topath_len,
        .db = db,
    };

    char *err = NULL;
    if (sqlite3_exec(db, SELECT_ENTRIES, process_entries, &dcba, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Could create entries at %s: %s\n", topath, err);
        rc = 1;
    }

    if (sqlite3_exec(db, SELECT_SUMMARY, process_summary, &dcba, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Could set directory metadata at %s: %s\n", topath, err);
        rc = 1;
    }

    if (pa->in.process_xattrs) {
        external_concatenate_cleanup(db, "DROP VIEW " XATTRS ";",
                                     &EXTERNAL_TYPE_XATTR,
                                     NULL,
                                     external_decrement_attachname,
                                     &ext_xattrs
                                     #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                                     , &queries
                                     #endif
            );
    }

    /* ignore errors */
    chmod(topath, dir_st.st_mode & 0777);
    chown(topath, dir_st.st_uid, dir_st.st_gid);

  cleanup:

    closedb(db);
    db = NULL;

    closedir(dir);

    free(work);

    return rc;
}

struct work *validate_inputs(struct PoolArgs *pa) {
    char expathin[MAXPATH];
    char expathout[MAXPATH];
    char expathtst[MAXPATH];

    SNPRINTF(expathtst, MAXPATH, "%s/%s", pa->in.nameto.data, pa->in.name.data);
    realpath(expathtst, expathout);
    realpath(pa->in.name.data, expathin);

    if (!strcmp(expathin, expathout)) {
        fprintf(stderr,"You are putting the tree in the index directory\n");
    }

    // get input path metadata
    struct stat src_st;
    if (lstat(pa->in.name.data, &src_st) < 0) {
        fprintf(stderr, "Could not stat source directory \"%s\"\n", pa->in.name.data);
        return NULL;
    }

    // check that the source path is a directory
    if (!S_ISDIR(src_st.st_mode)) {
        fprintf(stderr, "Source path is not a directory \"%s\"\n", pa->in.name.data);
        return NULL;
    }

    if (!pa->in.nameto.len) {
        fprintf(stderr, "No output path specified\n");
        return NULL;
    }

    // check if the destination path already exists (not an error)
    struct stat dst_st;
    if (lstat(pa->in.nameto.data, &dst_st) == 0) {
        fprintf(stderr, "\"%s\" Already exists!\n", pa->in.nameto.data);

        // if the destination path is not a directory (error)
        if (!S_ISDIR(dst_st.st_mode)) {
            fprintf(stderr, "Destination path is not a directory \"%s\"\n", pa->in.nameto.data);
            return NULL;
        }
    }

    // create the source root under the destination directory using
    // the source directory's permissions and owners
    // this allows for the threads to not have to recursively create directories
    char dst_path[MAXPATH];
    SNFORMAT_S(dst_path, MAXPATH, 3,
               pa->in.nameto.data, pa->in.nameto.len,
               "/", (size_t) 1,
               pa->in.name.data + pa->src_dirname_len, pa->in.name.len - pa->src_dirname_len);
    if (dupdir(dst_path, &src_st)) {
        fprintf(stderr, "Could not create %s under %s\n", pa->in.name.data, pa->in.nameto.data);
        return NULL;
    }

    struct work *root = (struct work *) calloc(1, sizeof(struct work));
    if (!root) {
        fprintf(stderr, "Could not allocate root struct\n");
        return NULL;
    }

    root->name_len = SNFORMAT_S(root->name, MAXPATH, 1,
                                pa->in.name.data, pa->in.name.len);

    return root;
}

void sub_help(void) {
   printf("input_dir         walk this GUFI index to produce a tree\n");
   printf("output_dir        reconstruct the tree under here\n");
   printf("\n");
}

int main(int argc, char * argv[]) {
    struct PoolArgs pa;
    int idx = parse_cmd_line(argc, argv, "hHn:x", 2, "input_dir output_dir", &pa.in);
    if (pa.in.helped)
        sub_help();
    if (idx < 0) {
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }
    else {
        INSTALL_STR(&pa.in.name,   argv[idx++]);
        INSTALL_STR(&pa.in.nameto, argv[idx++]);

        pa.in.name.len   = trailing_non_match_index(pa.in.name.data,   pa.in.name.len   - 1, "/", 1) + 1;
        pa.in.nameto.len = trailing_non_match_index(pa.in.nameto.data, pa.in.nameto.len - 1, "/", 1) + 1;

        pa.src_dirname_len = dirname_len(pa.in.name.data,
                                         pa.in.name.len - (pa.in.name.data[pa.in.name.len - 1] == '/'));
    }

    // get first work item by validating inputs
    struct work *root = validate_inputs(&pa);
    if (!root) {
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    int rc = EXIT_SUCCESS;

    struct QPTPool *pool = QPTPool_init(pa.in.maxthreads, &pa);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        rc = EXIT_FAILURE;
        goto cleanup;
    }

    QPTPool_enqueue(pool, 0, processdir, root);
    QPTPool_stop(pool);
    QPTPool_destroy(pool);

  cleanup:
    input_fini(&pa.in);
    return rc;
}
