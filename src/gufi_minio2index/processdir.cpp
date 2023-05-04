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



#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "bf.h"
#include "dbutils.h"
#include "template_db.h"
#include "trie.h"
#include "utils.h"

#include "gufi_minio2index/descend.h"
#include "gufi_minio2index/policies.h"
#include "gufi_minio2index/process.h"
#include "gufi_minio2index/structs.h"

/* process directories (should only be path segments, but minio allows for subbuckets) */
int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    /* Not checking arguments */

    int rc = 0;

    struct PoolArgs *pa = (struct PoolArgs *) args;
    gmw_t *gmw = (gmw_t *) data;

    gmw_t gmw_src;
    size_t subbuckets = 0;
    size_t objects = 0;             /* any objects found by readdir (0 or 1) */
    size_t objects_processed = 0;   /* objects successfully processed */

    char topath[MAXPATH];
    size_t topath_len;

    struct NonDirArgs nda;
    nda.client     = pa->client;
    nda.gmw        = &gmw_src;
    memset(&nda.ed, 0, sizeof(nda.ed));
    nda.ed.type    = 'd';
    nda.db         = NULL;
    nda.xattrs     = NULL;

    DIR *dir = NULL;
    struct dirent *entry = NULL;

    decompress_struct((void **) &nda.gmw, data, sizeof(gmw_src));

    if (gmw->work.level >= pa->in.max_level) {
        rc = 1;
        goto cleanup;
    }

    dir = opendir(nda.gmw->work.name);
    if (!dir) {
        fprintf(stderr, "Could not open directory \"%s\"\n", nda.gmw->work.name);
        rc = 1;
        goto cleanup;
    }

    if (lstat(nda.gmw->work.name, &nda.ed.statuso) != 0) {
        fprintf(stderr, "Could not stat directory \"%s\"\n", nda.gmw->work.name);
        rc = 1;
        goto cleanup;
    }

    /* offset by work->root_len to remove prefix */
    topath_len = SNFORMAT_S(topath, MAXPATH, 3,
                                pa->in.nameto.data, pa->in.nameto.len,
                                "/", (size_t) 1,
                                nda.gmw->work.name + nda.gmw->work.root_parent.len,
                                nda.gmw->work.name_len - nda.gmw->work.root_parent.len);

    /* don't need recursion because parent is guaranteed to exist */
    if (mkdir(topath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) < 0) {
        const int err = errno;
        if (err != EEXIST) {
            fprintf(stderr, "mkdir %s failure: %d %s\n", topath, err, strerror(err));
            rc = 1;
            goto cleanup;
        }
    }

    /* symlink to bucket policies before writing any data */
    char bucket_policies_symlink[MAXPATH];
    SNFORMAT_S(bucket_policies_symlink, MAXPATH, 3,
               topath, topath_len,
               "/", (size_t) 1,
               BUCKET_POLICIES_DB, sizeof(BUCKET_POLICIES_DB) - 1);
    if (symlink(gmw->cache->bucket_policies.c_str(), bucket_policies_symlink) != 0) {
        /* error? */
    }

    /* symlink bucket's xattrs */
    char bucket_xattrs[MAXPATH];
    SNFORMAT_S(bucket_xattrs, MAXPATH, 3,
               topath, topath_len,
               "/", (size_t) 1,
               XATTR_SYMLINK_NAME, XATTR_SYMLINK_NAME_LEN);
    if (symlink(nda.gmw->cache->bucket_xattrs.c_str(), bucket_xattrs) != 0) {
        /* error? */
    }

    /* create the database name */
    char dbname[MAXPATH];
    SNFORMAT_S(dbname, MAXPATH, 3,
               topath, topath_len,
               "/", (size_t) 1,
               DBNAME, DBNAME_LEN);
    nda.db = template_to_db(&pa->db, dbname, nda.ed.statuso.st_uid, nda.ed.statuso.st_gid);
    if (!nda.db) {
        rc = 1;
        goto cleanup;
    }

    /* if there is at least one object in this directory, this directory needs xattr dbs */
    while ((entry = readdir(dir))) {
        char xlmeta[MAXPATH];
        SNFORMAT_S(xlmeta, sizeof(xlmeta), 5,
                   nda.gmw->work.name, nda.gmw->work.name_len,
                   "/", (size_t) 1,
                   entry->d_name, strlen(entry->d_name),
                   "/", (size_t) 1,
                   XLMETA, XLMETA_LEN);

        struct stat st;
        if (lstat(xlmeta, &st) == 0) {
            objects++;
            break;
        }
    }
    rewinddir(dir);

    /* link bucket xattrs in */
    nda.xattr_files_res = insertdbprep(nda.db, EXTERNAL_DBS_PWD_INSERT);
    sqlite3_bind_text(nda.xattr_files_res, 1,
                      nda.gmw->cache->bucket_xattrs.c_str(),
                      nda.gmw->cache->bucket_xattrs.size(),
                      SQLITE_STATIC);
    sqlite3_bind_text(nda.xattr_files_res, 2, XATTR_ATTACH_BUCKET_NAME, XATTR_ATTACH_BUCKET_NAME_LEN, SQLITE_STATIC);
    sqlite3_bind_int64(nda.xattr_files_res, 3, nda.ed.statuso.st_mode);
    sqlite3_bind_int64(nda.xattr_files_res, 4, nda.ed.statuso.st_uid);
    sqlite3_bind_int64(nda.xattr_files_res, 5, nda.ed.statuso.st_gid);

    sqlite3_step(nda.xattr_files_res);
    sqlite3_reset(nda.xattr_files_res);

    if (objects) {
        /* set up xattrs database */
        char xattr_dbname[MAXPATH];
        size_t xattr_dbname_len = SNFORMAT_S(xattr_dbname, MAXPATH, 3,
                                             topath, topath_len,
                                             "/", (size_t) 1,
                                             XATTR_DBNAME, XATTR_DBNAME_LEN);
        nda.xattrs = template_to_db(&pa->xattr, xattr_dbname, nda.ed.statuso.st_uid, nda.ed.statuso.st_gid);
        if (!nda.xattrs) {
            rc = 1;
            insertdbfin(nda.xattr_files_res);
            goto close_db;
        }

        xattrs_setup(&nda.ed.xattrs);
        nda.xattrs_res = insertdbprep(nda.xattrs, XATTRS_PWD_INSERT);

        /* link this file in */
        sqlite3_bind_text(nda.xattr_files_res, 1, xattr_dbname, xattr_dbname_len, SQLITE_STATIC);
        sqlite3_bind_text(nda.xattr_files_res, 2, XATTR_ATTACH_NAME, XATTR_ATTACH_NAME_LEN, SQLITE_STATIC);
        sqlite3_bind_int64(nda.xattr_files_res, 3, nda.ed.statuso.st_mode);
        sqlite3_bind_int64(nda.xattr_files_res, 4, nda.ed.statuso.st_uid);
        sqlite3_bind_int64(nda.xattr_files_res, 5, nda.ed.statuso.st_gid);

        sqlite3_step(nda.xattr_files_res);
        sqlite3_reset(nda.xattr_files_res);

    }

    insertdbfin(nda.xattr_files_res);

    /* prepare to insert into the database */
    zeroit(&nda.summary);

    /* prepared statements within db.db */
    nda.entries_res = insertdbprep(nda.db, ENTRIES_INSERT);

    startdb(nda.db);
    descend2(ctx, id, pa, &pa->in, nda.gmw, nda.ed.statuso.st_ino, dir, pa->skip, 0,
             processdir, processobject, &nda, &subbuckets, NULL, NULL, &objects_processed);
    stopdb(nda.db);

    /* entries and their xattrs have been inserted */

    insertdbfin(nda.entries_res);

    /* insert this directory's summary data */
    /* the xattrs go into the xattrs_avail table in db.db */
    insertsumdb(nda.db, nda.gmw->work.name + nda.gmw->work.name_len - nda.gmw->work.basename_len,
                &nda.gmw->work, &nda.ed, &nda.summary);

    if (objects) {
        insertdbfin(nda.xattrs_res);
        xattrs_cleanup(&nda.ed.xattrs);
        closedb(nda.xattrs);
    }

  close_db:
    closedb(nda.db);
    nda.db = NULL;

    /* ignore errors */
    chmod(topath, nda.ed.statuso.st_mode);
    chown(topath, nda.ed.statuso.st_uid, nda.ed.statuso.st_gid);

  cleanup:
    closedir(dir);

    free_struct(nda.gmw, data, nda.gmw->work.recursion_level);

    pa->total[id].objects += objects_processed;
    pa->total[id].subbuckets += subbuckets;

    return rc;
}
