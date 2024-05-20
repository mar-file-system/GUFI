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



#include "external.h"
#include "gufi_query/timers.h"
#include "gufi_query/xattrs.h"
#include "xattrs.h"

static size_t xattr_modify_filename(char **dst, const size_t dst_size,
                                    const char *src, const size_t src_len,
                                    void *args) {
    struct work *work = (struct work *) args;

    if (src[0] == '/') {
        *dst = (char *) src;
        return src_len;
    }

    return SNFORMAT_S(*dst, dst_size, 3,
                      work->name, work->name_len,
                      "/", (size_t) 1,
                      src, src_len);
}

static int create_view(const char *name, sqlite3 *db, const char *query, size_t *query_counter) {
    char *err = NULL;
    const int rc = sqlite3_exec(db, query, NULL, NULL, &err);

    (*query_counter)++;

    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: Create %s view failed: %s\n", name, err);
        sqlite3_free(err);
        return 0;
    }

    return 1;
}

static int xattr_create_views(sqlite3 *db
                              #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                              , size_t *query_count
                              #endif
    ) {
    #if !(defined(DEBUG) && defined(CUMULATIVE_TIMES))
    static size_t query_count_stack = 0;
    static size_t *query_count = &query_count_stack;
    #endif

    /* create LEFT JOIN views (all rows, with and without xattrs) */
    /* these should run once, and be no-ops afterwards since the backing data of the views get swapped out */

    return !(
        /* entries, pentries, summary */
        create_view(XENTRIES, db, "CREATE TEMP VIEW IF NOT EXISTS " XENTRIES " AS SELECT " ENTRIES ".*, " XATTRS ".name as xattr_name, " XATTRS ".value as xattr_value FROM " ENTRIES " LEFT JOIN " XATTRS " ON " ENTRIES ".inode == " XATTRS ".inode;", query_count) &&

        create_view(XPENTRIES, db, "CREATE TEMP VIEW IF NOT EXISTS " XPENTRIES " AS SELECT " PENTRIES ".*, " XATTRS ".name as xattr_name, " XATTRS ".value as xattr_value FROM " PENTRIES " LEFT JOIN " XATTRS " ON " PENTRIES ".inode == " XATTRS ".inode;", query_count) &&

        create_view(XSUMMARY, db, "CREATE TEMP VIEW IF NOT EXISTS " XSUMMARY " AS SELECT " SUMMARY ".*, " XATTRS ".name as xattr_name, " XATTRS ".value as xattr_value FROM " SUMMARY " LEFT JOIN " XATTRS " ON " SUMMARY ".inode == " XATTRS ".inode;", query_count) &&

        /* vrpentries and vrsummary */
        /* vrentries is not available because rolled up entries tables are not correct */
        create_view(VRXPENTRIES, db, "CREATE TEMP VIEW IF NOT EXISTS " VRXPENTRIES " AS SELECT " VRPENTRIES ".*, " XATTRS ".name as xattr_name, " XATTRS ".value as xattr_value FROM " VRPENTRIES " LEFT JOIN " XATTRS " ON " VRPENTRIES ".inode == " XATTRS ".inode;", query_count) &&

        create_view(VRXSUMMARY, db, "CREATE TEMP VIEW IF NOT EXISTS " VRXSUMMARY " AS SELECT " VRSUMMARY ".*, " XATTRS ".name as xattr_name, " XATTRS ".value as xattr_value FROM " VRSUMMARY " LEFT JOIN " XATTRS " ON " VRSUMMARY ".inode == " XATTRS ".inode;", query_count)
        );
}

void setup_xattrs_views(struct input *in, gqw_t *gqw, sqlite3 *db,
                        size_t *extdb_count
                        #if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
                        , timestamps_t *ts
                        #endif
                        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                        , size_t *queries
                        #endif
    ) {
    static const refstr_t XATTRS_REF = {
        .data = XATTRS,
        .len  = sizeof(XATTRS) - 1,
    };

    #define XATTRS_COLS " SELECT inode, name, value FROM "
    static const refstr_t XATTRS_COLS_REF = {
        .data = XATTRS_COLS,
        .len  = sizeof(XATTRS_COLS) - 1,
    };

    static const refstr_t XATTRS_AVAIL_REF = {
        .data = XATTRS_AVAIL,
        .len  = sizeof(XATTRS_AVAIL) - 1,
    };

    static const refstr_t XATTRS_TEMPLATE_REF = {
        .data = XATTRS_TEMPLATE,
        .len  = sizeof(XATTRS_TEMPLATE) - 1,
    };

    thread_timestamp_start(ts->tts, xattrprep_call);

    /* always set up xattrs view */
    external_concatenate(db,
                         in->process_xattrs?&EXTERNAL_TYPE_XATTR:NULL,
                         NULL,
                         &XATTRS_REF,
                         &XATTRS_COLS_REF,
                         &XATTRS_AVAIL_REF,
                         in->process_xattrs?&XATTRS_AVAIL_REF:&XATTRS_TEMPLATE_REF,
                         xattr_modify_filename, &gqw->work,
                         external_increment_attachname, extdb_count
                         #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                         , queries
                         #endif
    );

    /* set up xattr variant views */
    xattr_create_views(db
                       #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                       , queries
                       #endif
        );

    thread_timestamp_end(xattrprep_call);
}
