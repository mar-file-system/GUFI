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
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "config.h"
#include "dbutils.h"
#include "index.h"
#include "plugin.h"
#include "template_db.h"

#include "gufi_incremental_update/incremental_update.h"

/* reindex the source directory */
int reindex_dir(QPTPool_ctx_t *ctx,
                struct work *work, struct entry_data *ed,
                DIR *dir) {
    struct PoolArgs *pa = (struct PoolArgs *) QPTPool_get_args_internal(ctx);

    str_t topath = {0};

    /*
     * if building in-tree, use existing db.db
     * else, create the file in the parking lot with the directory's inode as the name
     */
    if (pa->same == 1) {
        topath.len = SNFORMAT_S_ALLOC(&topath.data, 3,
                                      work->name, work->name_len,
                                      "/", (size_t) 1,
                                      DBNAME, DBNAME_LEN);
    }
    else {
        topath.data = malloc(pa->parking_lot.len + 1 + UINT64_DIGITS + 1);
        topath.len = SNPRINTF(topath.data, MAXPATH, "%s/%" STAT_ino, pa->parking_lot.data, work->statuso.st_ino);
    }

    topath.free = free;

    /* either way, truncate any existing db file */
    if (truncate(topath.data, 0) != 0) {
        const int err = errno;
        if (err != ENOENT) {
            fprintf(stderr, "Warning: Failed to truncate db file \"%s\": %s (%d)\n",
                    topath.data, strerror(err), err);
        }
    }

    rewinddir(dir);

    plugin_dir_action process_dir = PLUGIN_NO_PROCESS_DIR;
    int rc = index_dir(&topath, 0, ctx, &pa->in, &pa->db, &pa->xattr,
                       &process_dir, work, ed, dir, NULL, NULL);
    if (rc != 0) {
        rc = (rc < 0);
        goto cleanup;
    }

    /* set permissions on db file */
    if (chmod(topath.data, (work->statuso.st_mode & ~(S_IXUSR | S_IXGRP | S_IXOTH)) | S_IRUSR) != 0) {
        const int err = errno;
        fprintf(stderr, "Warning: Unable to set permission for \"%s\": %s (%d)\n",
                topath.data, strerror(err), err);
    }

    /* set owners on db file */
    if (chown(topath.data, work->statuso.st_uid, work->statuso.st_gid) != 0) {
        const int err = errno;
        fprintf(stderr, "Warning: Unable to set owners for \"%s\": %s (%d)\n",
                topath.data, strerror(err), err);
    }

    fprintf(stdout, "    Created update db \"%s\" for \"%s\"\n", topath.data, work->name);

  cleanup:
    str_free_existing(&topath);

    return rc;
}
