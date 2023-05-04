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



#include <map>
#include <string>

#include "miniocpp/client.h"

#include "bf.h"
#include "dbutils.h"
#include "utils.h"

#include "gufi_minio2index/structs.h"
#include "gufi_minio2index/process.h"

int processobject(gmw_t *entry, struct entry_data *ed, void *args) {
    struct NonDirArgs *nda = (struct NonDirArgs *) args;

    /* https://github.com/minio/minio-cpp/blob/main/examples/StatObject.cc */
    minio::s3::StatObjectArgs st;
    st.bucket = entry->cache->bucket_name;
    st.object = entry->work.name + entry->root_len + 1 + entry->cache->bucket_name.size() + 1;
    minio::s3::StatObjectResponse st_resp = nda->client->StatObject(st);
    if (st_resp) {
        // ed->statuso.st_ino = resp.etag;
        ed->statuso.st_size = st_resp.size;
        ed->statuso.st_mtime = st_resp.last_modified;
    }
    else {
        return 1;
    }

    /* https://github.com/minio/minio-cpp/blob/main/examples/GetObjectTags.cc */
    minio::s3::GetObjectTagsArgs request;
    request.bucket = entry->cache->bucket_name;
    request.object = entry->work.name + entry->root_len + 1 + entry->cache->bucket_name.size() + 1;
    minio::s3::GetObjectTagsResponse response = nda->client->GetObjectTags(request);

    if (response) {
        const std::map <std::string, std::string> &tags = response.tags;
        struct xattrs *xattrs = &ed->xattrs;
        xattrs_setup(xattrs);
        xattrs->count = tags.size();

        if (tags.size()) {
            xattrs_alloc(xattrs);

            size_t i = 0;
            #if defined(DEBUG) && defined(PRINT_MINIO_TYPES)
            std::cerr << "    Tags: " << std::endl;
            #endif
            for(std::pair<const std::string, std::string> const &tag: tags) {
                #if defined(DEBUG) && defined(PRINT_MINIO_TYPES)
                std::cerr << "      " << tag.first << ": " << tag.second << std::endl;
                #endif

                struct xattr *xattr = &xattrs->pairs[i++];
                xattr->name_len = SNFORMAT_S(xattr->name, sizeof(xattr->name), 1,
                                             tag.first.data(), tag.first.size());
                xattr->value_len = SNFORMAT_S(xattr->value, sizeof(xattr->value), 1,
                                              tag.second.data(), tag.second.size());

                xattrs->name_len += xattr->name_len;
                xattrs->len += xattr->name_len + xattr->value_len;
            }

            insertdbgo_xattrs_avail(ed, nda->xattrs_res);
        }
    }

    /* get entry relative path (use extra buffer to prevent memcpy overlap) */
    char relpath[MAXPATH];
    const size_t relpath_len = SNFORMAT_S(relpath, MAXPATH, 1,
                                          entry->work.name + entry->work.root_parent.len + 1 + entry->cache->bucket_name.size() + 1,
                                          entry->work.name_len - entry->work.root_parent.len - 1 - entry->cache->bucket_name.size() - 1);

    /* overwrite full path with relative path */
    /* e.name_len = */ SNFORMAT_S(entry->work.name, MAXPATH, 1, relpath, relpath_len);

    /* update summary table */
    sumit(&nda->summary, ed);

    /* add entry + xattr names into bulk insert */
    insertdbgo(&entry->work, ed, nda->db, nda->entries_res);

    /* free xattrs after insertdbgo to be able to insert names into entries */
    xattrs_cleanup(&ed->xattrs);

    return 0;
}
