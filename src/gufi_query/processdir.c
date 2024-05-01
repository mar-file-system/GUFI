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



#include <dirent.h>
#include <errno.h>
#include <utime.h>

#if defined(DEBUG) && defined(PER_THREAD_STATS)
#include "OutputBuffers.h"
#endif
#include "bf.h"
#include "compress.h"
#include "dbutils.h"
#include "external.h"
#include "gufi_query/PoolArgs.h"
#include "gufi_query/gqw.h"
#include "gufi_query/process_queries.h"
#include "gufi_query/processdir.h"
#include "gufi_query/timers.h"
#include "utils.h"

int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    /* Not checking arguments */

    DIR *dir = NULL;
    sqlite3 *db = NULL;

    PoolArgs_t *pa = (PoolArgs_t *) args;
    struct input *in = pa->in;
    ThreadArgs_t *ta = &(pa->ta[id]);

    gqw_t stack;
    gqw_t *gqw = &stack;

    decompress_struct((void **) &gqw, data, sizeof(stack));

    char dbpath[MAXPATH];  /* filesystem path of db.db; only generated if keep_matime is set */
    char dbname[MAXPATH];  /* path of db.db modified so that sqlite3 can open it */
    const size_t dbname_len = SNFORMAT_S(dbname, MAXPATH, 2,
                                         gqw->sqlite3_name, gqw->sqlite3_name_len,
                                         "/" DBNAME, DBNAME_LEN + 1);

    #if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
    timestamps_t ts;
    timestamps_init(&ts, &pa->start_time);
    #endif

    /* keep opendir near opendb to help speed up sqlite3_open_v2 */
    thread_timestamp_start(ts.tts, opendir_call);
    dir = opendir(gqw->work.name);
    thread_timestamp_end(opendir_call);

    /* if the directory can't be opened, don't bother with anything else */
    if (!dir) {
        goto out_free;
    }

    thread_timestamp_start(ts.tts, lstat_db_call);
    struct utimbuf dbtime;
    if (in->keep_matime) {
        const size_t dbpath_len = SNFORMAT_S(dbpath, sizeof(dbpath), 2,
                                             gqw->work.name, gqw->work.name_len,
                                             "/" DBNAME, DBNAME_LEN + 1);
        struct stat db_st;
        if (lstat(dbpath, &db_st) != 0) {
            const int err = errno;

            char buf[MAXPATH];
            present_user_path(dbpath, dbpath_len,
                              &gqw->work.root_parent, gqw->work.root_basename_len, &gqw->work.orig_root,
                              buf, sizeof(buf));

            fprintf(stderr, "Could not stat database file \"%s\": %s (%d)\n",
                    buf, strerror(err), err);
            goto out_free;
        }
        dbtime.actime  = db_st.st_atime;
        dbtime.modtime = db_st.st_mtime;
    }
    thread_timestamp_end(lstat_db_call);

    #if OPENDB
    thread_timestamp_start(ts.tts, attachdb_call);
    db = attachdb(dbname, ta->outdb, ATTACH_NAME, in->open_flags, 1);
    thread_timestamp_end(attachdb_call);
    increment_query_count(ta);
    #endif

    /* this is needed to add some query functions like path() uidtouser() gidtogroup() */
    #ifdef ADDQUERYFUNCS
    thread_timestamp_start(ts.tts, addqueryfuncs_call);
    if (db) {
        if (addqueryfuncs_with_context(db, &gqw->work) != 0) {
            fprintf(stderr, "Could not add functions to sqlite\n");
        }
    }
    thread_timestamp_end(addqueryfuncs_call);
    #endif

    size_t subdirs_walked_count = 0;

    process_queries(pa, ctx, id, dir, gqw, db, dbname, dbname_len, 1, &subdirs_walked_count
                    #if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
                    , &ts
                    #endif
        );

    #ifdef OPENDB
    thread_timestamp_start(ts.tts, detachdb_call);
    if (db) {
        detachdb_cached(dbname, db, pa->detach, 1);
        increment_query_count(ta);
    }
    thread_timestamp_end(detachdb_call);
    #endif

    /* restore mtime and atime */
    thread_timestamp_start(ts.tts, utime_call);
    if (db) {
        if (in->keep_matime) {
            if (utime(dbpath, &dbtime) != 0) {
                const int err = errno;
                fprintf(stderr, "Warning: Failed to run utime on database file \"%s\": %s (%d)\n",
                        dbpath, strerror(err), err);
            }
        }
    }
    thread_timestamp_end(utime_call);

    thread_timestamp_start(ts.tts, closedir_call);
    closedir(dir);
    thread_timestamp_end(closedir_call);

  out_free:
    ;

    thread_timestamp_start(ts.tts, free_work);
    free(gqw->work.fullpath);
    free_struct(gqw, data, 0);
    thread_timestamp_end(free_work);

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    struct OutputBuffers *debug_buffers = NULL;
    QPTPool_get_debug_buffers(ctx, &debug_buffers);
    timestamps_print(debug_buffers, id, &ts, dir, db);
    #endif
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamps_sum(&pa->tt, &ts);
    #endif

    #if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
    timestamps_destroy(&ts);
    #endif

    return 0;
}
