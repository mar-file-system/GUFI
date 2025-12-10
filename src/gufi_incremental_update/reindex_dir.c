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



#include <string.h>
#include <unistd.h>

#include "dbutils.h"
#include "template_db.h"

#include "gufi_incremental_update/incremental_update.h"

/* reindex the source directory */
int reindex_dir(struct PoolArgs *pa,
                struct work *work, struct entry_data *ed,
                DIR *dir) {
    if (lstat_wrapper(work, 1) != 0) {
        return 1;
    }

    /* need to fill this in for the directory as we dont need to do this unless we are making a new gufi db */
    if (pa->in.process_xattrs) {
        xattrs_setup(&ed->xattrs);
        xattrs_get(work->name, &ed->xattrs);
    }

    /*
     * if building in-tree, wipe the db.db and recreate it
     * else, create the file in the parking lot with the directory's inode as the name
     */
    char dbpath[MAXPATH];
    if (pa->same == 1) {
        SNPRINTF(dbpath, MAXPATH, "%s/%s", work->name, DBNAME);
        truncate(dbpath, 0);
    } else {
        SNPRINTF(dbpath, MAXPATH, "%s/%" STAT_ino, pa->parking_lot.data, work->statuso.st_ino);
    }

    sqlite3 *db = template_to_db(&pa->db, dbpath, -1, -1);
    if (!db) {
        return -1;
    }

    addqueryfuncs(db);

    sqlite3_stmt *res = insertdbprep(db, ENTRIES_INSERT);
    startdb(db);
    struct sum summary;
    zeroit(&summary);

    /* rewind the directory */
    rewinddir(dir);

    /*
     * loop over dirents, if link push it on the queue, if file or
     * link print it, fill up qwork structure for each
     */
    struct dirent *entry = NULL;
    while ((entry = readdir(dir))) {
        const size_t len = strlen(entry->d_name);

        const int skip = (
            /* skip ., .., and anything else in skip_names */
            trie_search(pa->in.skip, entry->d_name, len, NULL) ||
            /* skip db.db */
            ((len >= DBNAME_LEN) && (strncmp(entry->d_name + len - DBNAME_LEN, DBNAME, DBNAME_LEN) == 0))
        );

        if (skip) {
            continue;
        }

        struct work *child = new_work_with_name(work->name, work->name_len, entry->d_name, len);
        child->basename_len = len;

        if (!try_skip_lstat(entry, child, 1)) {
            free(child);
            continue;
        }

        /* only processing non-directories here */
        if (S_ISDIR(child->statuso.st_mode)) {
            free(child);
            continue;
        }
        else if (S_ISLNK(child->statuso.st_mode)) {}
        else if (S_ISREG(child->statuso.st_mode)) {}
        else {
            free(child);
            continue;
        }

        /* need actual values of files/links */
        if (lstat_wrapper(child, 1) != 0) {
            free(child);
            continue;
        }

        child->basename_len = len;
        child->pinode = work->statuso.st_ino;

        struct entry_data child_ed;
        memset(&child_ed, 0, sizeof(child_ed));
        child_ed.parent_fd = -1;

        if (S_ISLNK(child->statuso.st_mode)) {
            child_ed.type = 'l';
            readlink(child->name, child_ed.linkname, MAXPATH);
        }
        else if (S_ISREG(child->statuso.st_mode)) {
            child_ed.type = 'f';
        }

        if (pa->in.process_xattrs) {
            xattrs_setup(&child_ed.xattrs);
            xattrs_get(child->name, &child_ed.xattrs);
        }

        sumit(&summary, child, &child_ed);
        insertdbgo(child, &child_ed, res);

        if (pa->in.process_xattrs) {
            xattrs_cleanup(&child_ed.xattrs);
        }

        free(child);
    }

    stopdb(db);
    insertdbfin(res);

    insertsumdb(db, work->name, work, ed, &summary);
    if (pa->in.process_xattrs) {
        xattrs_cleanup(&ed->xattrs);
    }
    closedb(db);

    chown(dbpath, work->statuso.st_uid, work->statuso.st_gid);
    chmod(dbpath, (work->statuso.st_mode & ~(S_IXUSR | S_IXGRP | S_IXOTH)) | S_IRUSR);

    fprintf(stdout, "    Created update db \"%s\" for \"%s\"\n", dbpath, work->name);

    return 0;
}
