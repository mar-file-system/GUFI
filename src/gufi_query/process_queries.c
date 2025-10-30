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
#include "external.h"
#include "print.h"
#include "str.h"
#include "utils.h"

#include "gufi_query/process_queries.h"
#include "gufi_query/processdir.h"
#include "gufi_query/query.h"
#include "gufi_query/query_replacement.h"

#ifdef QPTPOOL_SWAP
static int gqw_serialize_and_free(const int fd, QPTPool_f func, void *work, size_t *size) {
    gqw_t *gqw = (gqw_t *) work;
    const size_t len = gqw->work.compressed.yes?gqw->work.compressed.len:sizeof(*gqw);

    return QPTPool_generic_serialize_and_free(fd, func, work, len, size);
}
#endif

/* Push the subdirectories in the current directory onto the queue */
static size_t descend2(QPTPool_ctx_t *ctx,
                       struct input *in,
                       gqw_t *gqw,
                       DIR *dir,
                       trie_t *skip_names,
                       QPTPool_f func) {
    /* Not checking arguments */

    size_t pushed = 0;
    const size_t next_level = gqw->work.level + 1;

    if (next_level <= in->max_level) {
        struct dirent *entry = NULL;
        while ((entry = readdir(dir))) {
            const size_t len = strlen(entry->d_name);

            /* if this entry's name is found, skip this entry */
            const int skip = (trie_search(skip_names, entry->d_name, len, NULL) ||
                             (strncmp(entry->d_name + len - 3, ".db", 3) == 0));

            if (skip) {
                continue;
            }

            gqw_t *child = new_gqw_with_name(gqw->work.name, gqw->work.name_len,
                                             entry->d_name, len,
                                             entry, next_level,
                                             gqw->sqlite3_name, gqw->sqlite3_name_len);
            if (!child) {
                continue;
            }

            if (S_ISDIR(child->work.statuso.st_mode)) {
                child->work.basename_len = len;
                child->work.fullpath = NULL;
                child->work.fullpath_len = 0;

                child->work.level = next_level;
                child->work.orig_root = gqw->work.orig_root;
                child->work.root_parent = gqw->work.root_parent;
                child->work.root_basename_len = gqw->work.root_basename_len;

                gqw_t *clone = compress_struct(in->compress, child, gqw_size(child));

                if (in->dir_match.on != DIR_MATCH_NONE) {
                    /* just in case lstat(2)/statx(2) was skipped */
                    if (lstat_wrapper(&clone->work) != 0) {
                        free(child);
                        continue;
                    }

                    /* check if uid/gid match */
                    if (!dir_match(in, &clone->work.statuso)) {
                        free(child);
                        continue;
                    }
                }

                /* push the subdirectory into the queue for processing */
                #ifdef QPTPOOL_SWAP
                QPTPool_enqueue_swappable(ctx, func, clone,
                                          gqw_serialize_and_free,
                                          QPTPool_generic_alloc_and_deserialize);
                #else
                QPTPool_enqueue(ctx, func, clone);
                #endif

                pushed++;
            }
            else {
                free(child);
            }
        }
    }

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
int process_queries(PoolArgs_t *pa, QPTPool_ctx_t *ctx,
                    DIR *dir, gqw_t *gqw, sqlite3 *db, trie_t *user_strs,
                    const char *dbname, const size_t dbname_len,
                    const int descend, size_t *subdirs_walked_count) {
    struct input *in = pa->in;
    const int id = QPTPool_get_id(ctx);

    if (descend) {
        /* get the rollup score
         * ignore errors - if the db wasn't opened, or if
         * summary is missing the columns, keep descending
         */
        int rollupscore = 0;
        if (db) {
            get_rollupscore(db, &rollupscore);
        }

        /* push subdirectories into the queue */
        if (rollupscore == 0) {
            *subdirs_walked_count =
                descend2(ctx, in, gqw, dir, in->skip, processdir);
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

            /* replace {} */
            char *sum = NULL;
            if (replace_sql(&in->sql.sum, &in->sql_format.sum,
                            user_strs,
                            &sum) != 0) {
                fprintf(stderr, "Error: Failed to do string replacements for -S\n");
                return 0;
            }

            /* put in the path relative to the user's input */
            querydb(&gqw->work, dbname, dbname_len, db,
                    sum, in->types.sum,
                    pa, id, print_parallel, &recs);

            free_sql(sum, in->sql.sum.data);

            /* keep going even if S returned nothing */
            if (in->process_sql == RUN_TSE) {
                recs = 1;
            }
        } else {
            recs = 1;
        }

        if (in->process_sql == RUN_SE) {
            recs = 1;
        }

        /* if we have recs (or are running an OR) query the entries table */
        if (recs > 0) {
            if (in->sql.ent.len) {
                /* replace {} */
                char *ent = NULL;
                if (replace_sql(&in->sql.ent, &in->sql_format.ent,
                                user_strs,
                                &ent) != 0) {
                    fprintf(stderr, "Error: Failed to do string replacements for -E\n");
                    return 0;
                }

                /* replace original SQL string if there is formatting */
                querydb(&gqw->work, dbname, dbname_len, db,
                        ent, in->types.ent,
                        pa, id, print_parallel, &recs); /* recs is not used */

                free_sql(ent, in->sql.ent.data);
            }
        }
    }

    return recs;
}
