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



#include <sqlite3.h>
#include <string.h>

#include "dbutils.h"
#include "debug.h"
#include "external.h"
#include "gufi_query/debug.h"
#include "gufi_query/process_queries.h"
#include "gufi_query/processdir.h"
#include "gufi_query/query.h"
#include "gufi_query/timers.h"
#include "print.h"
#include "utils.h"

/* Push the subdirectories in the current directory onto the queue */
static size_t descend2(QPTPool_t *ctx,
                       const size_t id,
                       gqw_t *gqw,
                       DIR *dir,
                       trie_t *skip_names,
                       QPTPoolFunc_t func,
                       const size_t max_level,
                       const int comp
                       #if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
                       , sll_t *dts
                       #endif
    ) {
    descend_timestamp_start(dts, within_descend);

    /* Not checking arguments */

    descend_timestamp_start(dts, level_cmp);
    size_t pushed = 0;
    const size_t next_level = gqw->work.level + 1;
    const int level_check = (next_level < max_level);
    descend_timestamp_end(level_cmp);

    descend_timestamp_start(dts, level_branch);
    if (level_check) {
        descend_timestamp_end(level_branch);

        /* Send subdirs to queue */
        /* loop over dirents */
        /* skip db.db and any filename listed in the trie struct */
        /* fill qwork struct for each dirent */
        descend_timestamp_start(dts, while_branch);
        while (1) {
            descend_timestamp_start(dts, readdir_call);
            struct dirent *entry = readdir(dir);
            descend_timestamp_end(readdir_call);

            descend_timestamp_start(dts, readdir_branch);
            if (!entry) {
                descend_timestamp_end(readdir_branch);
                break;
            }
            else {
                descend_timestamp_end(readdir_branch);
            }

            descend_timestamp_start(dts, strncmp_call);
            size_t len = strlen(entry->d_name);
            const int skip = (trie_search(skip_names, entry->d_name, len, NULL) ||
                             (strncmp(entry->d_name + len - 3, ".db", 3) == 0));
            descend_timestamp_end(strncmp_call);

            descend_timestamp_start(dts, strncmp_branch);
            if (skip) {
                descend_timestamp_end(strncmp_branch);
                continue;
            }
            else {
                descend_timestamp_end(strncmp_branch);
            }

            gqw_t child;
            memset(&child, 0, sizeof(child));

            child.work.basename_len = len;

            descend_timestamp_start(dts, snprintf_call);
            /* append entry name to directory */
            child.work.name_len = SNFORMAT_S(child.work.name, MAXPATH, 3,
                                             gqw->work.name, gqw->work.name_len,
                                             "/", (size_t) 1,
                                             entry->d_name, len);

            descend_timestamp_end(snprintf_call);

            descend_timestamp_start(dts, isdir_cmp);
            int isdir = (entry->d_type == DT_DIR);
            if (!isdir) {
                /* allow for paths immediately under the input paths to be symlinks */
                if (next_level < 2) {
                    struct stat st;
                    if (stat(child.work.name, &st) == 0) {
                        isdir = S_ISDIR(st.st_mode);
                    }
                    /* errors are ignored */
                }
            }
            descend_timestamp_end(isdir_cmp);

            descend_timestamp_start(dts, isdir_branch);
            if (isdir) {
                descend_timestamp_end(isdir_branch);

                descend_timestamp_start(dts, set);

                child.work.level = next_level;
                child.work.orig_root = gqw->work.orig_root;
                child.work.root_parent = gqw->work.root_parent;
                child.work.root_basename_len = gqw->work.root_basename_len;

                /* append converted entry name to converted directory */
                child.sqlite3_name_len = SNFORMAT_S(child.sqlite3_name, MAXPATH, 2,
                                                    gqw->sqlite3_name, gqw->sqlite3_name_len,
                                                    "/", (size_t) 1);
                const size_t converted_len = sqlite_uri_path(child.sqlite3_name + child.sqlite3_name_len,
                                                            MAXPATH - child.sqlite3_name_len,
                                                            entry->d_name, &len);
                child.sqlite3_name_len += converted_len;

                /* this is how the parent gets passed on */
                descend_timestamp_end(set);

                /* make a clone here so that the data can be pushed into the queue */
                /* this is more efficient than malloc+free for every single entry */
                descend_timestamp_start(dts, make_clone);
                gqw_t *clone = compress_struct(comp, &child, sizeof(child));
                descend_timestamp_end(make_clone);

                /* push the subdirectory into the queue for processing */
                descend_timestamp_start(dts, pushdir);
                QPTPool_enqueue(ctx, id, func, clone);
                descend_timestamp_end(pushdir);

                pushed++;
            }
            else {
                descend_timestamp_end(isdir_branch);
            }
        }
        descend_timestamp_end(while_branch);
    }
    else {
        descend_timestamp_end(level_branch);
    }
    descend_timestamp_end(within_descend);

    return pushed;
}

/*
 * SELECT subdirs(srollsubdirs, sroll) FROM vrsummary;
 *
 * custom sqlite function for getting number of subdirectories under current directory
 */
static void subdirs(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc; (void) argv;

    const int rollupscore = sqlite3_value_int(argv[1]);
    if (rollupscore == 0) {
        size_t *subdirs_walked_count = (size_t *) sqlite3_user_data(context);
        sqlite3_result_int64(context, *subdirs_walked_count);
    }
    else {
        const int64_t rollup_subdirs = sqlite3_value_int64(argv[0]);
        sqlite3_result_int64(context, rollup_subdirs);
    }
}

/*
 * process -S and -E
 *
 * AND mode (default):
 *     if -S returns at least 1 row, run -E
 * OR  mode:
 *     whether or not -S returns anything, run -E
 */
int process_queries(PoolArgs_t *pa,
                    QPTPool_t *ctx, const int id,
                    DIR *dir,
                    gqw_t *gqw, sqlite3 *db,
                    const char *dbname, const size_t dbname_len,
                    const int descend, size_t *subdirs_walked_count
                    #if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
                    , timestamps_t *ts
                    #endif
    ) {
    struct input *in = pa->in;

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    ThreadArgs_t *ta = &(pa->ta[id]);
    #endif

    if (descend) {
        /* get the rollup score
         * ignore errors - if the db wasn't opened, or if
         * summary is missing the columns, keep descending
         */
        thread_timestamp_start(get_rollupscore_call, &ts->tts[tts_get_rollupscore_call]);
        int rollupscore = 0;
        if (db) {
            get_rollupscore(db, &rollupscore);
            increment_query_count(ta);
        }
        thread_timestamp_end(get_rollupscore_call);

        /* push subdirectories into the queue */
        if (rollupscore == 0) {
            thread_timestamp_start(descend_call, &ts->tts[tts_descend_call]);
            *subdirs_walked_count =
                descend2(ctx, id, gqw, dir, in->skip, processdir, in->max_level, in->compress
                         #if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
                         , ts->dts
                         #endif
                    );
            thread_timestamp_end(descend_call);
        }
    }

    if (!db) {
        return 1;
    }

    int recs = 1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
                  /* if it fails then this will be set to 1 and will go on */

    /* only query this level if the min_level has been reached */
    if (gqw->work.level >= in->min_level) {
        if (sqlite3_create_function(db, "subdirs", 2, SQLITE_UTF8,
                                    subdirs_walked_count, &subdirs,
                                    NULL, NULL) != SQLITE_OK) {
            sqlite_print_err_and_free(NULL, stderr,
                                      "Warning: Could not create subdirs_walked function: %s (%d)\n",
                                      sqlite3_errmsg(db), sqlite3_errcode(db));
        }

        char shortname[MAXPATH];
        char endname[MAXPATH];

        /* run query on summary, print it if printing is needed, if returns none */
        /* and we are doing AND, skip querying the entries db */
        shortpath(gqw->work.name, shortname, endname);

        if (in->sql.sum.len) {
            recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
            /* put in the path relative to the user's input */
            thread_timestamp_start(sqlsum, &ts->tts[tts_sqlsum]);
            querydb(&gqw->work, dbname, dbname_len, db, in->sql.sum.data, pa, id, print_parallel, &recs);
            thread_timestamp_end(sqlsum);
            increment_query_count(ta);
        } else {
            recs = 1;
        }
        if (in->andor == OR) {
            recs = 1;
        }
        /* if we have recs (or are running an OR) query the entries table */
        if (recs > 0) {
            if (in->sql.ent.len) {
                thread_timestamp_start(sqlent, &ts->tts[tts_sqlent]);
                querydb(&gqw->work, dbname, dbname_len, db, in->sql.ent.data, pa, id, print_parallel, &recs); /* recs is not used */
                thread_timestamp_end(sqlent);
                increment_query_count(ta);
            }
        }
    }

    return recs;
}
