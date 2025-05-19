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
#include <stdlib.h>
#include <string.h>
#include <utime.h>

#include "bf.h"
#include "compress.h"
#include "dbutils.h"
#include "external.h"
#include "gufi_query/PoolArgs.h"
#include "gufi_query/external.h"
#include "gufi_query/gqw.h"
#include "gufi_query/process_queries.h"
#include "gufi_query/processdir.h"
#include "gufi_query/query.h"
#include "gufi_query/query_replacement.h"
#include "print.h"
#include "utils.h"

static inline int save_matime(gqw_t *gqw,
                              char *dbpath, const size_t dbpath_size,
                              struct utimbuf *dbtime) {
    const size_t dbpath_len = SNFORMAT_S(dbpath, dbpath_size, 2,
                                         gqw->work.name, gqw->work.name_len,
                                         "/" DBNAME, DBNAME_LEN + 1);
    struct stat st;
    if (lstat(dbpath, &st) != 0) {
        const int err = errno;

        char buf[MAXPATH];
        present_user_path(dbpath, dbpath_len,
                          &gqw->work.root_parent, gqw->work.root_basename_len, &gqw->work.orig_root,
                          buf, sizeof(buf));

        fprintf(stderr, "Could not stat database file \"%s\": %s (%d)\n",
                buf, strerror(err), err);

        return 1;
    }

    dbtime->actime  = st.st_atime;
    dbtime->modtime = st.st_mtime;

    return 0;
}

static inline int restore_matime(const char *dbpath, struct utimbuf *dbtime) {
    if (utime(dbpath, dbtime) != 0) {
        const int err = errno;
        fprintf(stderr, "Warning: Failed to run utime on database file \"%s\": %s (%d)\n",
                dbpath, strerror(err), err);
    }

    return 0;
}

static int count_rows(void *args, int count, char **data, char **columns) {
    (void) count;
    (void) data;
    (void) columns;

    PrintArgs_t *print = (PrintArgs_t *) args;
    print->rows++;
    return 0;
}

/* prepend the current directory to the database filenamee */
static int collect_dir_inodes(void *args, int count, char **data, char **columns) {
    (void) count;
    (void) columns;

    sll_t *inodes = (sll_t *) args;

    size_t len = strlen(data[0]);
    char *inode = malloc(len + 1);
    memcpy(inode, data[0], len);
    inode[len] = '\0';

    sll_push(inodes, inode);

    return 0;
}


int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    /* Not checking arguments */

    DIR *dir = NULL;
    sqlite3 *db = NULL;
    struct utimbuf dbtime;

    PoolArgs_t *pa = (PoolArgs_t *) args;
    struct input *in = pa->in;
    ThreadArgs_t *ta = &(pa->ta[id]);

    gqw_t *gqw = NULL;
    decompress_gqw(&gqw, data);

    char dbpath[MAXPATH];  /* filesystem path of db.db; only generated if keep_matime is set */
    char dbname[MAXPATH];  /* path of db.db modified so that sqlite3 can open it */
    const size_t dbname_len = SNFORMAT_S(dbname, MAXPATH, 2,
                                         gqw->sqlite3_name, gqw->sqlite3_name_len,
                                         "/" DBNAME, DBNAME_LEN + 1);

    /* keep opendir near opendb to help speed up sqlite3_open_v2 */
    dir = opendir(gqw->work.name);

    /* if the directory can't be opened, don't bother with anything else */
    if (!dir) {
        const int err = errno;
        fprintf(stderr, "Error: Skipping directory \"%s\": %s (%d)\n",
                gqw->work.name, strerror(err), err);
        goto out_free;
    }

    int rc = 0;
    if (in->keep_matime) {
        rc = save_matime(gqw, dbpath, sizeof(dbpath), &dbtime);
    }

    if (rc != 0) {
        goto close_dir;
    }

    if (gqw->work.level >= in->min_level) {
        db = attachdb(dbname, ta->outdb, ATTACH_NAME, in->open_flags, 1);
    }

    /* get number of subdirs walked on first call to process_queries */
    size_t subdirs_walked_count = 0;

    if (db && (gqw->work.level >= in->min_level)) {
        /* add some query functions like path() uidtouser() gidtogroup() */
        if (addqueryfuncs_with_context(db, &gqw->work) != 0) {
            fprintf(stderr, "Warning: Could not add functions to sqlite\n");
        }

        /* ********************************************** */
        /* reset for each directory */
        /* only valid for current directory */

        /* {n} -> directory name */
        const refstr_t n = {
            .data = gqw->work.name + gqw->work.name_len - gqw->work.basename_len,
            .len = gqw->work.basename_len,
        };

        /* {i} -> directory path */
        const refstr_t i = {
            .data = gqw->work.name,
            .len = gqw->work.name_len,
        };

        trie_insert(ta->user_strs, "n", 1, (void *) &n, NULL);
        trie_insert(ta->user_strs, "i", 1, (void *) &i, NULL);
        /* ********************************************** */

        int recs = 1;

        /*
         * can iterate over treesummary, but not creating external
         * database view for treesummary, so run once here to not
         * get duplicate results when querying treesummary
         */
        if (in->sql.tsum.len) {
            /* if AND operation, and sqltsum is there, run a query to see if there is a match. */
            /* if this is OR, as well as no-sql-to-run, skip this query */
            if (in->andor == AND) {
                /* make sure the treesummary table exists */
                static const char TSUM_CHECK_QUERY[] =
                    "SELECT name FROM " ATTACH_NAME ".sqlite_master "
                    "WHERE (type == 'table') AND (name == '" TREESUMMARY "');";
                static const int TSUM_CHECK_TYPES[] = { SQLITE_TEXT };

                querydb(&gqw->work, dbname, dbname_len, db,
                        TSUM_CHECK_QUERY, in->types.prefix?TSUM_CHECK_TYPES:NULL,
                        pa, id, count_rows, &recs);
                if (recs < 1) {
                    recs = -1;
                }
                else {
                    /* replace {} */
                    char *tsum = NULL;
                    if (replace_sql(&in->sql.tsum, &in->sql_format.tsum,
                                    ta->user_strs,
                                    &tsum) != 0) {
                        fprintf(stderr, "Error: Failed to do string replacements for -T\n");
                        goto detach;
                    }

                    /* run in->sql.tsum */
                    querydb(&gqw->work, dbname, dbname_len, db,
                            in->sql.tsum.data, in->types.tsum,
                            pa, id, print_parallel, &recs);

                    free_sql(tsum, in->sql.tsum.data);
                }
            }
            /* this is an OR or we got a record back. go on to summary/entries */
            /* queries, if not done with this dir and all dirs below it */
            /* this means that no tree table exists so assume we have to go on */
            if (recs < 0) {
                recs=1;
            }
        }

        if (recs > 0) {
            size_t extdb_count = 0; /* shared between xattrs and external databases */

            /*
             * if xattr processing is enabled, then the xattrs view was
             * not created in PoolArgs_init, so have to create it here
             */
            if (in->process_xattrs) {
                setup_xattrs_views(in, db,
                                   &gqw->work,
                                   &extdb_count);
            }

            const size_t xattr_db_count = extdb_count;

            /* set up external user databases for use with -S and -E */
            if (sll_get_size(&in->external_attach)) {
                char *err = NULL;

                /* get list of directory inodes in the current directory */
                sll_t dir_inodes;
                sll_init(&dir_inodes);
                if (sqlite3_exec(db, "SELECT inode FROM " ATTACH_NAME "." SUMMARY ";",
                                 collect_dir_inodes, &dir_inodes, &err) != SQLITE_OK) {
                    sqlite_print_err_and_free(err, stderr, "Error: Could not pull directory inodes: %s\n", err);
                    err = NULL;
                    /* ignore errors (still have to descend) */
                }

                /* the only time there are no rows in the summary table is at the index root's parent */
                if (sll_get_size(&dir_inodes) == 0) {
                    /* attach external dbs and create views specified by input args */
                    attach_extdbs(in, db, "", 0, &extdb_count);

                    /* create view for attaching external dbs to */
                    create_extdb_views_noiter(db);

                    /* run queries */
                    process_queries(pa, ctx, id,
                                    dir, gqw, db, ta->user_strs,
                                    dbname, dbname_len,
                                    1, &subdirs_walked_count);

                    /* drop views for attaching GUFI tables to */
                    drop_extdb_views(db);

                    /* detach each external db */
                    detach_extdbs(in, db, NULL, 0, &extdb_count);
                }
                else {
                    /* don't want to shadow descend function */
                    int desc = 1;

                    /*
                     * for each directory in the summary table, create views
                     *
                     * each view consists of the empty template table and one
                     * external database associated with the current directory
                     *
                     * there should always be at least one directory (the current one)
                     * except at the index root's parent
                     */
                    sll_loop(&dir_inodes, dir_inode_node) {
                        const char *dir_inode = (const char *) sll_node_data(dir_inode_node);
                        const size_t dir_inode_len = strlen(dir_inode);

                        /* attach external dbs and create views specified by input args */
                        attach_extdbs(in, db, dir_inode, dir_inode_len, &extdb_count);

                        /* create views for attaching GUFI tables to */
                        create_extdb_views_iter(db, dir_inode);

                        /* run queries */
                        process_queries(pa, ctx, id,
                                        dir, gqw, db, ta->user_strs,
                                        dbname, dbname_len,
                                        desc, &subdirs_walked_count);

                        /* drop views for attaching GUFI tables to */
                        drop_extdb_views(db);

                        /* detach each external db */
                        detach_extdbs(in, db, dir_inode, dir_inode_len, &extdb_count);

                        /* only descend once */
                        desc = 0;
                    }
                }

                sll_destroy(&dir_inodes, free);
            }
            else {
                /* external databases views were created in PoolArgs_init */

                /* run queries */
                process_queries(pa, ctx, id,
                                dir, gqw, db, ta->user_strs,
                                dbname, dbname_len,
                                1, &subdirs_walked_count);
            }

            if (xattr_db_count != extdb_count) {
                fprintf(stderr, "Error: xattr db count does not match extdb count after cleanup: %zu != %zu\n",
                        xattr_db_count, extdb_count);
            }

            /* drop xattrs view */
            if (in->process_xattrs) {
                external_concatenate_cleanup(db, "DROP VIEW " XATTRS ";",
                                             &EXTERNAL_TYPE_XATTR,
                                             NULL,
                                             external_decrement_attachname,
                                             &extdb_count);
            }
        }
    }
    else {
        /* if the database was not opened or not deep enough, still have to descend */
        process_queries(pa, ctx, id,
                        dir, gqw, db, ta->user_strs,
                        dbname, dbname_len,
                        1, &subdirs_walked_count);
    }

  detach:
    if (db) {
        detachdb_cached(dbname, db, pa->detach, 1);
    }

    /* restore mtime and atime */
    if (db) {
        if (in->keep_matime) {
            restore_matime(dbpath, &dbtime);
        }
    }

  close_dir:
    closedir(dir);

  out_free:
    free(gqw->work.fullpath);
    free(gqw);

    return 0;
}
