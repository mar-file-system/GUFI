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
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "bf.h"
#include "dbutils.h"
#include "utils.h"

#include "gufi_minio2index/descend.h"
#include "gufi_minio2index/policies.h"
#include "gufi_minio2index/process.h"
#include "gufi_minio2index/structs.h"

static int processpolicies(struct PoolArgs *pa, struct NonDirArgs *nda,
                           const char *topath, const size_t topath_len) {
    PoolArgs::Cache *cache = nda->gmw->cache;

    /* create bucket policies db path in a cached string */
    cache->bucket_policies.resize(topath_len + 1 + sizeof(BUCKET_POLICIES_DB));
    cache->bucket_policies = topath;
    cache->bucket_policies += "/";
    cache->bucket_policies += BUCKET_POLICIES_DB;

    /* create the bucket policies db file */
    sqlite3 *bucket_policies_db = template_to_db(&pa->policies, cache->bucket_policies.c_str(),
                                                 nda->ed.statuso.st_uid,
                                                 nda->ed.statuso.st_gid);
    if (!bucket_policies_db) {
        return 1;
    }

    /* fill in bucket policies database */
    minio::s3::GetBucketPolicyArgs request;
    request.bucket = nda->gmw->cache->bucket_name;
    minio::s3::GetBucketPolicyResponse response = pa->client->GetBucketPolicy(request);

    if (response) {
        insert_policy(bucket_policies_db, request.bucket, response.policy);
    }
    else {
        std::cerr << "    " << response.Error().String() << std::endl;
    }

    closedb(bucket_policies_db);

    return 0;
}

int processbucket(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    /* Not checking arguments */

    int rc = 0;

    struct PoolArgs *pa = (struct PoolArgs *) args;
    gmw_t *gmw = (gmw_t *) data;

    gmw_t gmw_src;
    size_t subbuckets = 0;
    size_t objects_processed = 0;

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

    decompress_struct((void **) &nda.gmw, data, sizeof(gmw_src));

    #if defined(DEBUG) && defined(PRINT_MINIO_TYPES)
    fprintf(stderr, "%-20s %s\n", "bucket", gmw->work.name);
    #endif

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
        goto close_dir;
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
            goto close_dir;
        }
    }

    /* set up bucket policies before indexing bucket */
    if (processpolicies(pa, &nda, topath, topath_len) != 0) {
        rc = 1;
        goto close_dir;
    }

    /* set up db.db  */
    char dbname[MAXPATH];
    SNFORMAT_S(dbname, MAXPATH, 3,
               topath, topath_len,
               "/", (size_t) 1,
               DBNAME, DBNAME_LEN);
    nda.db = template_to_db(&pa->db, dbname, nda.ed.statuso.st_uid, nda.ed.statuso.st_gid);
    if (!nda.db) {
        rc = 1;
        goto close_dir;
    }

    /* set up xattrs database */
    nda.gmw->cache->bucket_xattrs.resize(topath_len + 1 + XATTR_DBNAME_LEN + 1);
    nda.gmw->cache->bucket_xattrs = topath;
    nda.gmw->cache->bucket_xattrs += "/";
    nda.gmw->cache->bucket_xattrs += XATTR_DBNAME;
    nda.xattrs = template_to_db(&pa->xattr, nda.gmw->cache->bucket_xattrs.c_str(),
                                nda.ed.statuso.st_uid, nda.ed.statuso.st_gid);
    if (!nda.xattrs) {
        rc = 1;
        goto close_db;
    }

    /* prepare to insert into the database */
    zeroit(&nda.summary);

    /* pull this directory's xattrs because they were not pulled by the parent */
    xattrs_setup(&nda.ed.xattrs);

    /* prepared to insert data */
    nda.entries_res = insertdbprep(nda.db, ENTRIES_INSERT);
    nda.xattrs_res = insertdbprep(nda.xattrs, XATTRS_PWD_INSERT);

    nda.xattr_files_res = insertdbprep(nda.db, EXTERNAL_DBS_PWD_INSERT);
    sqlite3_bind_text(nda.xattr_files_res, 1,
                      nda.gmw->cache->bucket_xattrs.c_str(),
                      nda.gmw->cache->bucket_xattrs.size(),
                      SQLITE_STATIC);
    sqlite3_bind_text(nda.xattr_files_res, 2, XATTR_ATTACH_NAME, XATTR_ATTACH_NAME_LEN, SQLITE_STATIC);
    sqlite3_bind_int64(nda.xattr_files_res, 3, nda.ed.statuso.st_mode);
    sqlite3_bind_int64(nda.xattr_files_res, 4, nda.ed.statuso.st_uid);
    sqlite3_bind_int64(nda.xattr_files_res, 5, nda.ed.statuso.st_gid);

    sqlite3_step(nda.xattr_files_res);
    sqlite3_reset(nda.xattr_files_res);
    insertdbfin(nda.xattr_files_res);

    {
        /* https://github.com/minio/minio-cpp/blob/main/examples/GetBucketTags.cc */
        minio::s3::GetBucketTagsArgs request;
        request.bucket = nda.gmw->cache->bucket_name;
        minio::s3::GetBucketTagsResponse response = pa->client->GetBucketTags(request);

        if (response) {
            const std::map <std::string, std::string> &tags = response.tags;

            if (tags.size()) {
                nda.ed.xattrs.count = tags.size();

                xattrs_alloc(&nda.ed.xattrs);

                size_t i = 0;
                #if defined(DEBUG) && defined(PRINT_MINIO_TYPES)
                std::cerr << "    Tags: " << std::endl;
                #endif
                for(std::pair<const std::string, std::string> const &tag: tags) {
                    #if defined(DEBUG) && defined(PRINT_MINIO_TYPES)
                    std::cerr << "      " << tag.first << ": " << tag.second << std::endl;
                    #endif

                    struct xattr *xattr = &nda.ed.xattrs.pairs[i++];
                    xattr->name_len = SNFORMAT_S(xattr->name, sizeof(xattr->name), 1,
                                                 tag.first.data(), tag.first.size());
                    xattr->value_len = SNFORMAT_S(xattr->value, sizeof(xattr->value), 1,
                                                  tag.second.data(), tag.second.size());

                    nda.ed.xattrs.name_len += xattr->name_len;
                    nda.ed.xattrs.len += xattr->name_len + xattr->value_len;
                }

                /* directory xattrs go into the same table as entries xattrs */
                insertdbgo_xattrs_avail(&nda.ed, nda.xattrs_res);
            }
        }
    }

    /* delay descent until after xattrs are processed so that they print in the correct order */
    startdb(nda.db);
    descend2(ctx, id, pa, &pa->in, nda.gmw, nda.ed.statuso.st_ino, dir, pa->skip, 0,
             processdir, processobject, &nda, &subbuckets, NULL, NULL, &objects_processed);
    stopdb(nda.db);

    /* insert this directory's summary data */
    /* the xattrs go into the xattrs_avail table in db.db */
    insertsumdb(nda.db, nda.gmw->work.name + nda.gmw->work.name_len - nda.gmw->work.basename_len,
                &nda.gmw->work, &nda.ed, &nda.summary);

    /* have to cleanup here because insertsumdb needs xattr names */
    insertdbfin(nda.xattrs_res);
    xattrs_cleanup(&nda.ed.xattrs);
    closedb(nda.xattrs);

    insertdbfin(nda.entries_res);

  close_db:
    closedb(nda.db);
    nda.db = NULL;

    /* ignore errors */
    chmod(topath, nda.ed.statuso.st_mode);
    chown(topath, nda.ed.statuso.st_uid, nda.ed.statuso.st_gid);

  close_dir:
    closedir(dir);

  cleanup:
    free_struct(nda.gmw, data, nda.gmw->work.recursion_level);

    pa->total[id].objects += objects_processed;
    pa->total[id].subbuckets += subbuckets;
    pa->total[id].buckets++;

    return rc;
}
