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

#include "bf.h"
#include "utils.h"

#include "gufi_minio2index/process.h"
#include "gufi_minio2index/structs.h"

int processroot(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    /* Not checking arguments */

    struct PoolArgs *pa = (struct PoolArgs *) args;
    char *root_path = (char *) data;
    const size_t root_len = strlen(root_path);
    const size_t parent_len = dirname_len(root_path, root_len);

    /* <nameto>/<server basename> */
    char topath[MAXPATH];
    SNFORMAT_S(topath, sizeof(topath), 3,
               pa->in.nameto.data, pa->in.nameto.len,
               "/", (size_t) 1,
               root_path + parent_len, root_len - parent_len);
    if (mkdir(topath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) < 0) {
        const int err = errno;
        if (err != EEXIST) {
            fprintf(stderr, "mkdir %s failure: %d %s\n", topath, err, strerror(err));
            return 1;
        }
    }

    /* enqueue buckets for processing */
    for(minio::s3::Bucket const &bucket : pa->client->ListBuckets().buckets) {
        /* save this bucket name to prevent repeated copying */
        pa->all_buckets[id].emplace_back(PoolArgs::Cache{});
        PoolArgs::Cache *bucket_cache = &*(pa->all_buckets[id].rbegin());
        bucket_cache->bucket_name = bucket.name;

        gmw_t *gmw = (gmw_t *) calloc(1, sizeof(gmw_t));
        gmw->work.name_len = SNFORMAT_S(gmw->work.name, sizeof(gmw->work.name), 3,
                                        root_path, root_len,
                                        "/", (size_t) 1,
                                        bucket.name.data(), bucket.name.size());
        gmw->work.basename_len = bucket.name.size();
        gmw->work.level = 0;
        gmw->work.root_parent.data = root_path;
        gmw->work.root_parent.len = parent_len;
        gmw->root_len = root_len;
        gmw->cache = bucket_cache;

        QPTPool_enqueue(ctx, id, processbucket, gmw);
    }

    return 0;
}
