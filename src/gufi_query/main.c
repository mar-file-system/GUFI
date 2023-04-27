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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <utime.h>

#include "OutputBuffers.h"
#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "bf.h"
#include "compress.h"
#include "dbutils.h"
#if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
#include "debug.h"
#endif
#include "external.h"
#include "print.h"
#include "trie.h"
#include "utils.h"

#include "gufi_query/aggregate.h"
#include "gufi_query/query.h"
#include "gufi_query/timers.h"

#if defined(DEBUG) && defined(CUMULATIVE_TIMES)
#define print_stats(normal_fmt, terse_fmt, ...)             \
    if (in.terse) {                                         \
        fprintf(stderr, terse_fmt " ", ##__VA_ARGS__);      \
    }                                                       \
    else {                                                  \
        fprintf(stderr, normal_fmt "\n", ##__VA_ARGS__);    \
    }

#define query_count_arg , &ta->queries
#define increment_query_count(ta) ta->queries++
#else
#define query_count_arg
#define increment_query_count(ta)
#endif

/* name doesn't matter, so long as it is not used by callers */
#define ATTACH_NAME "tree"

/* additional data gufi_query needs */
typedef struct gufi_query_work {
    #if HAVE_ZLIB
    compressed_t comp;
    #endif

    struct work work;

    /*
     * some characters need to be converted for sqlite3,
     * but opendir must use the unconverted version
     */
    char          sqlite3_name[MAXPATH];
    size_t        sqlite3_name_len;
} gqw_t;

/* prepend the current directory to the database filenamee */
size_t xattr_modify_filename(char *dst, const size_t dst_size,
                             const char *src, const size_t src_len,
                             struct work *work) {
    return SNFORMAT_S(dst, dst_size, 3,
                      work->name, work->name_len,
                      "/", (size_t) 1,
                      src, src_len + 1); /* NULL terminate */
}

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
            const int skip = (trie_search(skip_names, entry->d_name, len) ||
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
            child.work.basename_len = len;
            child.work.fullpath = NULL;
            child.work.fullpath_len = 0;

            descend_timestamp_start(dts, snprintf_call);
            /* append entry name to directory */
            child.work.name_len = SNFORMAT_S(child.work.name, MAXPATH, 3,
                                             gqw->work.name, gqw->work.name_len,
                                             "/", (size_t) 1,
                                             entry->d_name, len);

            descend_timestamp_end(snprintf_call);

            descend_timestamp_start(dts, isdir_cmp);
            const int isdir = (entry->d_type == DT_DIR);
            descend_timestamp_end(isdir_cmp);

            descend_timestamp_start(dts, isdir_branch);
            if (isdir) {
                descend_timestamp_end(isdir_branch);

                descend_timestamp_start(dts, set);

                child.work.level = next_level;
                child.work.root = gqw->work.root;
                child.work.root_len = gqw->work.root_len;

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

int count_rows(void *args, int count, char **data, char **columns) {
    (void) count;
    (void) data;
    (void) columns;

    PrintArgs_t *print = (PrintArgs_t *) args;
    print->rows++;
    return 0;
}

int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    int recs;
    char shortname[MAXPATH];
    char endname[MAXPATH];
    DIR *dir = NULL;

    /* Not checking arguments */

    PoolArgs_t *pa = (PoolArgs_t *) args;
    struct input *in = pa->in;
    ThreadArgs_t *ta = &(pa->ta[id]);

    gqw_t stack;
    gqw_t *gqw = &stack;
    decompress_struct(in->compress, data, (void **) &gqw, &stack, sizeof(stack));

    char dbname[MAXPATH];
    SNFORMAT_S(dbname, MAXPATH, 2, gqw->sqlite3_name, gqw->sqlite3_name_len, "/" DBNAME, DBNAME_LEN + 1);

    #if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
    timestamps_t ts;
    timestamps_init(&ts, &pa->start_time);
    #endif

    /* lstat first to not affect atime and mtime */
    thread_timestamp_start(ts.tts, lstat_call);
    struct stat st;
    if (lstat(gqw->work.name, &st) != 0) {
        fprintf(stderr, "Could not stat directory \"%s\"\n", gqw->work.name);
        goto out_free;
    }
    thread_timestamp_end(lstat_call);

    /* keep opendir near opendb to help speed up sqlite3_open_v2 */
    thread_timestamp_start(ts.tts, opendir_call);
    dir = opendir(gqw->work.name);
    thread_timestamp_end(opendir_call);

    /* if the directory can't be opened, don't bother with anything else */
    if (!dir) {
        goto out_free;
    }

    #if OPENDB
    thread_timestamp_start(ts.tts, attachdb_call);
    sqlite3 *db = attachdb(dbname, ta->outdb, ATTACH_NAME, in->open_flags, 1);
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

    /* set up XATTRS so that it can be used by -T, -S, and -E */
    if (db && in->external_enabled) {
        static const char XATTR_COLS[] = " SELECT inode, name, value FROM ";

        thread_timestamp_start(ts.tts, xattrprep_call);
        external_loop(&gqw->work, db,
                      XATTRS, sizeof(XATTRS) - 1,
                      XATTR_COLS, sizeof(XATTR_COLS) - 1,
                      XATTRS_AVAIL, sizeof(XATTRS_AVAIL) - 1,
                      xattr_modify_filename query_count_arg);
        xattr_create_views(db query_count_arg);
        thread_timestamp_end(xattrprep_call);
    }

    recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
            /* if it fails then this will be set to 1 and will go on */

    /* if AND operation, and sqltsum is there, run a query to see if there is a match. */
    /* if this is OR, as well as no-sql-to-run, skip this query */
    if (in->sql.tsum_len > 1) {
        if (in->andor == AND) {
            /* make sure the treesummary table exists */
            thread_timestamp_start(ts.tts, sqltsumcheck);
            querydb(dbname, db, "SELECT name FROM " ATTACH_NAME ".sqlite_master "
                                "WHERE (type == 'table') AND (name == 'treesummary');",
                    pa, id, count_rows, &recs);
            thread_timestamp_end(sqltsumcheck);
            increment_query_count(ta);
            if (recs < 1) {
                recs = -1;
            }
            else {
                /* run in->sql.tsum */
                thread_timestamp_start(ts.tts, sqltsum);
                querydb(dbname, db, in->sql.tsum, pa, id, print_parallel, &recs);
                thread_timestamp_end(sqltsum);
                increment_query_count(ta);
            }
        }
        /* this is an OR or we got a record back. go on to summary/entries */
        /* queries, if not done with this dir and all dirs below it */
        /* this means that no tree table exists so assume we have to go on */
        if (recs < 0) {
            recs=1;
        }
    }
    /* so we have to go on and query summary and entries possibly */
    if (recs > 0) {
        /* get the rollup score
         * ignore errors - if the db wasn't opened, or if
         * summary is missing the columns, keep descending
         */
        thread_timestamp_start(ts.tts, get_rollupscore_call);
        int rollupscore = 0;
        if (db) {
            get_rollupscore(db, &rollupscore);
            increment_query_count(ta);
        }
        thread_timestamp_end(get_rollupscore_call);

        /* push subdirectories into the queue */
        if (rollupscore == 0) {
            thread_timestamp_start(ts.tts, descend_call);
            #if defined(DEBUG) && defined(SUBDIRECTORY_COUNTS)
            const size_t pushed =
            #endif
            descend2(ctx, id, gqw, dir, pa->skip, processdir, in->max_level, in->compress
                     #if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
                     , ts.dts
                     #endif
                    );
            thread_timestamp_end(descend_call);
            #if defined(DEBUG) && defined(SUBDIRECTORY_COUNTS)
            pthread_mutex_lock(&print_mutex);
            fprintf(stderr, "%s %zu\n", gqw->work.name, pushed);
            pthread_mutex_unlock(&print_mutex);
            #endif
        }

        if (db) {
            /* only query this level if the min_level has been reached */
            if (gqw->work.level >= in->min_level) {
                /* run query on summary, print it if printing is needed, if returns none */
                /* and we are doing AND, skip querying the entries db */
                shortpath(gqw->work.name,shortname,endname);

                if (in->sql.sum_len > 1) {
                    recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
                    /* put in the path relative to the user's input */
                    thread_timestamp_start(ts.tts, sqlsum);
                    querydb(dbname, db, in->sql.sum, pa, id, print_parallel, &recs);
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
                    if (in->sql.ent_len > 1) {
                        thread_timestamp_start(ts.tts, sqlent);
                        querydb(dbname, db, in->sql.ent, pa, id, print_parallel, &recs); /* recs is not used */
                        thread_timestamp_end(sqlent);
                        increment_query_count(ta);
                    }
                }
            }
        }
    }

    /* detach xattr dbs */
    thread_timestamp_start(ts.tts, xattrdone_call);
    if (db && in->external_enabled) {
        external_done(db, "DROP VIEW " XATTRS ";" query_count_arg);
    }
    thread_timestamp_end(xattrdone_call);

    #ifdef OPENDB
    thread_timestamp_start(ts.tts, detachdb_call);
    if (db) {
        detachdb(dbname, db, ATTACH_NAME, 1);
        increment_query_count(ta);
    }
    thread_timestamp_end(detachdb_call);
    #endif

    /* restore mtime and atime */
    thread_timestamp_start(ts.tts, utime_call);
    if (db) {
        if (in->keep_matime) {
            struct utimbuf dbtime = {};
            dbtime.actime  = st.st_atime;
            dbtime.modtime = st.st_mtime;
            utime(dbname, &dbtime);
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
    free_struct(in->compress, gqw, &stack, 0);
    thread_timestamp_end(free_work);

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    timestamps_print(ctx->buffers, id, &ts, dir, db);
    #endif
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamps_sum(&pa->tt, &ts);
    #endif

    #if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
    timestamps_destroy(&ts);
    #endif

    return 0;
}

int validate_inputs(struct input *in) {
    /*
     * - Leaves are final outputs
     * - OUTFILE/OUTDB + aggregation will create per thread and final aggregation files
     *
     *                         init                       | if writing to outdb or aggregating
     *                          |                         | -I CREATE [TEMP] TABLE <name>
     *                          |
     *                          |                         | if aggregating, create an aggregate table
     *                          |                         | -K CREATE [TEMP] TABLE <aggregate name>
     *                          |
     *   -----------------------------------------------  | start walk
     *                          |
     *                       thread
     *                          |
     *          -------------------------------
     *          |               |             |
     *          |               |       stdout/outfile.*  | -T/-S/-E SELECT FROM <index table>
     *          |               |
     *   intermediate db      outdb.*                     | -T/-S/-E INSERT into <name> SELECT FROM <index table>
     *          |
     *          |
     *   -----------------------------------------------  | after walking index
     *          |
     *          |                                         | move intermediate results into aggregate table
     *    aggregate db - outdb                            | -J INSERT INTO <aggregate name>
     *    |          |
     * outfile     stdout                                 | Get final results
     *                                                    | -G SELECT * FROM <aggregate name>
     */
     if ((in->output == OUTDB) || in->sql.init_agg_len) {
        /* -I (required) */
        if (!in->sql.init_len) {
            fprintf(stderr, "Error: Missing -I\n");
            return -1;
        }
    }

    /* not aggregating */
    if (!in->sql.init_agg_len) {
        if (in->sql.intermediate_len) {
            fprintf(stderr, "Warning: Got -J even though not aggregating. Ignoring\n");
        }

        if (in->sql.agg_len) {
            fprintf(stderr, "Warning: Got -G even though not aggregating. Ignoring\n");
        }
    }
    /* aggregating */
    else {
        /* need -J to write to aggregate db */
        if (!in->sql.intermediate_len) {
            fprintf(stderr, "Error: Missing -J\n");
            return -1;
        }

        if ((in->output == STDOUT) || (in->output == OUTFILE)) {
            /* need -G to write out results */
            if (!in->sql.agg_len) {
                fprintf(stderr, "Error: Missing -G\n");
                return -1;
            }
        }
        /* -G can be called when aggregating, but is not necessary */
    }

    return 0;
}

void sub_help() {
   printf("GUFI_index        find GUFI index here\n");
   printf("\n");
}

int main(int argc, char *argv[])
{
    /* process input args - all programs share the common 'struct input', */
    /* but allow different fields to be filled at the command-line. */
    /* Callers provide the options-string for get_opt(), which will */
    /* control which options are parsed for each program. */
    struct input in;
    int idx = parse_cmd_line(argc, argv, "hHT:S:E:an:jo:d:O:I:F:y:z:J:K:G:m:B:wxk:M:" COMPRESS_OPT, 1, "GUFI_index ...", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;

    if (validate_inputs(&in) != 0) {
        return -1;
    }

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamp_create_start(setup_globals);
    #endif

    /* mutex writing to stdout/stderr */
    pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;
    PoolArgs_t pa;
    if (PoolArgs_init(&pa, &in, &global_mutex) != 0) {
        return -1;
    }

    #ifdef DEBUG
    #ifdef PER_THREAD_STATS
    timestamp_print_init(timestamp_buffers, in.maxthreads, 1073741824ULL, &global_mutex);
    #endif

    #ifdef CUMULATIVE_TIMES
    timestamp_set_end(setup_globals);
    const uint64_t setup_globals_time = timestamp_elapsed(setup_globals);
    #endif
    #endif

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamp_create_start(setup_aggregate);
    #endif

    Aggregate_t aggregate;
    memset(&aggregate, 0, sizeof(aggregate));
    if (in.sql.init_agg_len) {
        if (!aggregate_init(&aggregate, &in)) {
            PoolArgs_fin(&pa, in.maxthreads);
            return -1;
        }
    }

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamp_set_end(setup_aggregate);
    const uint64_t setup_aggregate_time = timestamp_elapsed(setup_aggregate);
    #endif

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    uint64_t total_time = 0;

    timestamp_create_start(work);
    #endif

    /* provide a function to print if PRINT is set */
    const uint64_t queue_depth = in.target_memory_footprint / sizeof(struct work) / in.maxthreads;
    QPTPool_t *pool = QPTPool_init(in.maxthreads, &pa, NULL, NULL, queue_depth, 1, 2
                                   #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                   , timestamp_buffers
                                   #endif
        );

    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        aggregate_fin(&aggregate, &in);
        PoolArgs_fin(&pa, in.maxthreads);
        return -1;
    }

    /* enqueue all input paths */
    for(int i = idx; i < argc; i++) {
        /* remove trailing slashes */
        size_t len = trailing_match_index(argv[i], strlen(argv[i]), "/", 1);

        /* root is special case */
        if (len == 0) {
            argv[i][0] = '/';
            len = 1;
        }

        struct stat st;

        if (lstat(argv[i], &st) != 0) {
            fprintf(stderr, "Could not stat directory \"%s\"\n", argv[i]);
            continue;
        }

        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr,"input-dir '%s' is not a directory\n", argv[i]);
            continue;
        }

        /* copy argv[i] into the work item */
        gqw_t *root = calloc(1, sizeof(gqw_t));
        root->work.name_len = SNFORMAT_S(root->work.name, MAXPATH, 1, argv[i], len);
        root->sqlite3_name_len = sqlite_uri_path(root->sqlite3_name, MAXPATH, argv[i], &len);

        /* parent of input path */
        root->work.root = argv[i];
        root->work.root_len = trailing_non_match_index(root->work.root, root->work.name_len, "/", 1);
        root->work.root[root->work.root_len] = '\0';
        root->work.basename_len = trailing_match_index(root->work.root, root->work.root_len, "/", 1);

        /* push the path onto the queue (no compression) */
        QPTPool_enqueue(pool, i % in.maxthreads, processdir, root);
    }

    QPTPool_wait(pool);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    const size_t thread_count = QPTPool_threads_completed(pool);
    #endif

    QPTPool_destroy(pool);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamp_set_end(work);

    const uint64_t work_time = timestamp_elapsed(work);
    total_time += work_time;
    #endif

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    uint64_t aggregate_time = 0;
    uint64_t output_time = 0;
    size_t rows = 0;
    #endif

    int rc = 0;
    if (in.sql.init_agg_len) {
        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        timestamp_create_start(aggregation);
        #endif

        /* aggregate the intermediate results */
        aggregate_intermediate(&aggregate, &pa, &in);

        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        timestamp_set_end(aggregation);
        aggregate_time = timestamp_elapsed(aggregation);
        total_time += aggregate_time;
        #endif

        /* final query on aggregate results */
        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        timestamp_create_start(output);

        rows +=
        #endif

        aggregate_process(&aggregate, &in);

        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        timestamp_set_end(output);
        output_time = timestamp_elapsed(output);
        total_time += output_time;
        #endif

        aggregate_fin(&aggregate, &in);
    }

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamp_create_start(cleanup_globals);
    #endif

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    /* aggregate per thread query counts */
    size_t query_count = 0;
    for(int i = 0; i < in.maxthreads; i++) {
        query_count += pa.ta[i].queries;
    }
    #endif

    /* clear out buffered data */
    for(int i = 0; i < in.maxthreads; i++) {
        ThreadArgs_t *ta = &(pa.ta[i]);
        OutputBuffer_flush(&ta->output_buffer, ta->outfile);

        #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
        rows += ta->output_buffer.count;
        #endif
    }

    /*
     * PoolArgs_fin does not deallocate the total_times
     * struct, but reading from pa would look weird
     * after PoolArgs_fin was called.
     */
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    total_time_t *tt = &pa.tt;
    #endif

    /* clean up globals */
    PoolArgs_fin(&pa, in.maxthreads);

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    timestamp_set_end(cleanup_globals);
    const uint64_t cleanup_globals_time = timestamp_elapsed(cleanup_globals);
    total_time += cleanup_globals_time;

    const long double total_time_sec = sec(total_time);
    #endif

    #ifdef DEBUG
    timestamp_print_destroy(timestamp_buffers);
    #endif

    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    const uint64_t thread_time = total_time_sum(tt);

    print_stats("set up globals:                             %.2Lfs", "%Lf", sec(setup_globals_time));
    print_stats("set up intermediate databases:              %.2Lfs", "%Lf", sec(setup_aggregate_time));
    print_stats("thread pool:                                %.2Lfs", "%Lf", sec(work_time));
    print_stats("    lstat:                                  %.2Lfs", "%Lf", sec(tt->lstat));
    print_stats("    open directories:                       %.2Lfs", "%Lf", sec(tt->opendir));
    print_stats("    attach index:                           %.2Lfs", "%Lf", sec(tt->attachdb));
    print_stats("    xattrprep:                              %.2Lfs", "%Lf", sec(tt->xattrprep));
    print_stats("    addqueryfuncs:                          %.2Lfs", "%Lf", sec(tt->addqueryfuncs));
    print_stats("    get_rollupscore:                        %.2Lfs", "%Lf", sec(tt->get_rollupscore));
    print_stats("    descend:                                %.2Lfs", "%Lf", sec(tt->descend));
    print_stats("        check args:                         %.2Lfs", "%Lf", sec(tt->check_args));
    print_stats("        check level:                        %.2Lfs", "%Lf", sec(tt->level));
    print_stats("        check level <= max_level branch:    %.2Lfs", "%Lf", sec(tt->level_branch));
    print_stats("        while true:                         %.2Lfs", "%Lf", sec(tt->while_branch));
    print_stats("            readdir:                        %.2Lfs", "%Lf", sec(tt->readdir));
    print_stats("            readdir != null branch:         %.2Lfs", "%Lf", sec(tt->readdir_branch));
    print_stats("            strncmp:                        %.2Lfs", "%Lf", sec(tt->strncmp));
    print_stats("            strncmp != . or ..:             %.2Lfs", "%Lf", sec(tt->strncmp_branch));
    print_stats("            snprintf:                       %.2Lfs", "%Lf", sec(tt->snprintf));
    print_stats("            isdir:                          %.2Lfs", "%Lf", sec(tt->isdir));
    print_stats("            isdir branch:                   %.2Lfs", "%Lf", sec(tt->isdir_branch));
    print_stats("            access:                         %.2Lfs", "%Lf", sec(tt->access));
    print_stats("            set:                            %.2Lfs", "%Lf", sec(tt->set));
    print_stats("            clone:                          %.2Lfs", "%Lf", sec(tt->clone));
    print_stats("            pushdir:                        %.2Lfs", "%Lf", sec(tt->pushdir));
    print_stats("    check if treesummary table exists:      %.2Lfs", "%Lf", sec(tt->sqltsumcheck));
    print_stats("    sqltsum:                                %.2Lfs", "%Lf", sec(tt->sqltsum));
    print_stats("    sqlsum:                                 %.2Lfs", "%Lf", sec(tt->sqlsum));
    print_stats("    sqlent:                                 %.2Lfs", "%Lf", sec(tt->sqlent));
    print_stats("    xattrdone:                              %.2Lfs", "%Lf", sec(tt->xattrdone));
    print_stats("    detach index:                           %.2Lfs", "%Lf", sec(tt->detachdb));
    print_stats("    close directories:                      %.2Lfs", "%Lf", sec(tt->closedir));
    print_stats("    restore timestamps:                     %.2Lfs", "%Lf", sec(tt->utime));
    print_stats("    free work:                              %.2Lfs", "%Lf", sec(tt->free_work));
    print_stats("    output timestamps:                      %.2Lfs", "%Lf", sec(tt->output_timestamps));
    print_stats("aggregate into final databases:             %.2Lfs", "%Lf", sec(aggregate_time));
    print_stats("print aggregated results:                   %.2Lfs", "%Lf", sec(output_time));
    print_stats("clean up globals:                           %.2Lfs", "%Lf", sec(cleanup_globals_time));
    if (!in.terse) {
        fprintf(stderr, "\n");
    }
    print_stats("Threads run:                                %zu",    "%zu", thread_count);
    print_stats("Queries performed:                          %zu",    "%zu", query_count);
    print_stats("Rows printed to stdout or outfiles:         %zu",    "%zu", rows);
    print_stats("Total Thread Time (not including main):     %.2Lfs", "%Lf", sec(thread_time));
    print_stats("Real time (main):                           %.2Lfs", "%Lf", total_time_sec);
    if (in.terse) {
        fprintf(stderr, "\n");
    }
    #endif
    return rc;
}
