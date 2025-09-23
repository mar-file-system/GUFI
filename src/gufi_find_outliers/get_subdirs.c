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
#include <fcntl.h>
#include <math.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "bf.h"
#include "dbutils.h"

#include "gufi_find_outliers/get_subdirs.h"
#include "gufi_find_outliers/processdir.h"

static int is_subdir(const char *path, struct dirent *entry) {
    switch (entry->d_type) {
        case DT_DIR:
            return 1;
        /* case DT_LNK: */
        case DT_REG:
        /* case DT_FIFO: */
        /* case DT_SOCK: */
        /* case DT_CHR: */
        /* case DT_BLK: */
            return 0;
        case DT_UNKNOWN:
        default:
            ;
            /* some filesystems don't support d_type - fall back to calling stat */
            #if HAVE_STATX
            struct statx stx;
            if (statx(AT_FDCWD, path,
                      AT_SYMLINK_NOFOLLOW | AT_STATX_DONT_SYNC,
                      STATX_ALL, &stx) != 0) {
                const int err = errno;
                fprintf(stderr, "Error: Could not statx \"%s\": %s (%d)\n",
                        path, strerror(err), err);
                return -1;
            }

            return S_ISDIR(stx.stx_mode);
            #else
            struct stat st;
            if (stat(path, &st) != 0) { /* follow links in index */
                const int err = errno;
                fprintf(stderr, "Error: Could not stat \"%s\": %s (%d)\n",
                        path, strerror(err), err);
                return -1;
            }

            return S_ISDIR(st.st_mode);
            #endif
    }

    return 0; /* shoud never get here */
}

/* go down only one level */
void get_subdirs(OutlierWork_t *ow, sll_t *subdirs, size_t *opendbs,
                 QPTPool_t *pool, const size_t id) {
    DIR *dir = opendir(ow->path.data);
    if (!dir) {
        const int err = errno;
        fprintf(stderr, "Error: Could not open directory \"%s\": %s (%d)\n",
                ow->path.data, strerror(err), err);
        return;
    }

    sll_init(subdirs);

    struct dirent *entry = NULL;
    while ((entry = readdir(dir))) {
        const size_t len = strlen(entry->d_name);

        const int skip = (
            /* skip . and .. ; not using skip_names */
            ((len == 1) && (strncmp(entry->d_name, ".",  1) == 0)) ||
            ((len == 2) && (strncmp(entry->d_name, "..", 2) == 0)) ||
            /* skip *.db */
            ((len >= 3) && (strncmp(entry->d_name + len - 3, ".db", 3) == 0))
            );

        if (skip) {
            continue;
        }

        struct DirData *dd = DirData_create(&ow->path, entry->d_name, len);
        if (is_subdir(dd->path.data, entry) != 1) {
            DirData_free(dd);
            continue;
        }

        /* create the db.db path */
        const size_t dbname_len = dd->path.len + 1 + DBNAME_LEN;
        char *dbname = malloc(dbname_len + 1);
        SNFORMAT_S(dbname, dbname_len + 1, 3,
                   dd->path.data, dd->path.len,
                   "/", (size_t) 1,
                   DBNAME, DBNAME_LEN);

        (*opendbs)++;

        sqlite3 *db = opendb(dbname, SQLITE_OPEN_READONLY, 0, 0, NULL, NULL);

        /* no db.db == no information - just go down */
        if (!db) {
            Stats_t ts = {
                .value   = NAN,
                .mean    = NAN,
                .stdev   = NAN,
            };

            OutlierWork_t *new_ow = OutlierWork_create(&dd->path, ow->level + 1,
                                                       ow->col,
                                                       ow->handler, ow->query,
                                                       0,
                                                       &ts, &ts);
            QPTPool_enqueue(pool, id, processdir, new_ow);

            free(dbname);
            DirData_free(dd);
            continue;
        }

        /* get data from db */
        char *err = NULL;
        if (sqlite3_exec(db, ow->query->data, ow->handler->sqlite_callback, dd, &err) == SQLITE_OK) {
            /* save this subdir */
            sll_push(subdirs, dd);
        }
        else {
            sqlite_print_err_and_free(err, stderr, "Error: Could not get column from \"%s\": \"%s\": %s\n",
                                      dbname, ow->query->data, err);
            DirData_free(dd);
        }

        free(dbname);

        closedb(db);
    }

    closedir(dir);
}
