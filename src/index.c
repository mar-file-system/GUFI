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
#include <string.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "dbutils.h"
#include "descend.h"
#include "external_attach.h"
#include "index.h"
#include "plugin.h"
#include "xattrs.h"

/*
 * structure containing information used to
 * index non-directories (files and links)
 */
struct IndexNonDirArgs {
    struct input *in;

    /* thread args */
    size_t id;
    struct template_db *temp_xattr;
    struct work *work;
    struct entry_data *ed;

    /* index path */
    str_t *topath;

    /* summary of the current directory */
    struct sum summary;

    /* db.db */
    sqlite3 *db;

    /* prepared statements */
    sqlite3_stmt *entries_res;
    sqlite3_stmt *xattrs_res;
    sqlite3_stmt *xattr_files_res;

    /* list of xattr dbs */
    sll_t xattr_db_list;
};

static int index_external(struct input *in, void *args,
                            const long long int pinode,
                            const char *filename) {
    (void) in;
    return external_insert((sqlite3 *) args, EXTERNAL_TYPE_USER_DB_NAME, pinode, filename);
}

static int index_nondir(struct work *entry, struct entry_data *ed, void *args) {
    struct IndexNonDirArgs *inda = (struct IndexNonDirArgs *) args;
    struct input *in = inda->in;
    int rc = 0;

    /* references passed into plugins; built up front so a plugin may
       provide stat before the entry is inserted (GUFI#196) */
    PCS_t pcs = {
        .db = inda->db,
        .work = entry,
        .ed = ed,
    };

    /* Let a plugin supply the entry's stat metadata from its own data
       source (e.g. a pseudo-file read) instead of statx. If no plugin
       provides it, fall back to statx exactly as before. */
    if (!plugins_stat_file(&in->plugins, &pcs, inda->id)) {
        if (fstatat_wrapper(entry, ed, 1, NULL) != 0) {
            rc = 1;
            goto out;
        }
    }

    if (plugins_pre_process_file(&in->plugins, &pcs, inda->id) == PLUGIN_PROCESS_FILE) {
        /* read external files before modifying the entry's path */
        if (strncmp(entry->name + entry->name_len - entry->basename_len,
                    EXTERNAL_DB_USER_FILE, EXTERNAL_DB_USER_FILE_LEN + 1) == 0) {
            external_read_file(in, entry, index_external, inda->db);
        }

        if (in->process_xattrs) {
            insertdbgo_xattrs(in, &inda->work->statuso, entry, ed,
                              &inda->xattr_db_list, inda->temp_xattr,
                              inda->topath->data, inda->topath->len,
                              inda->xattrs_res, inda->xattr_files_res);
        }

        /* update summary table */
        sumit(&inda->summary, entry, ed);

        /* add entry + xattr names into bulk insert */
        insertdbgo(entry, ed, inda->entries_res);

        plugins_post_process_file(&in->plugins, &pcs, inda->id);
    }

out:
    return rc;
}

/*
 * return -1 on actual error  (caller returns not ok)
 *         0 ok
 *         1 acceptable error (caller returns ok)
 */
int index_dir(str_t *topath, const int dir2index,
              QPTPool_ctx_t *ctx, struct input *in,
              struct template_db *temp_db, struct template_db *temp_xattr,
              plugin_dir_action *process_dir,
              struct work *work, struct entry_data *ed,
              DIR *dir, QPTPool_f processdir,
              struct descend_counters *ctrs) {
    /* Not checking arguments */

    int rc = 0;

    struct IndexNonDirArgs inda = {
        .in              = in,
        .id              = QPTPool_get_id(ctx),
        .temp_xattr      = temp_xattr,
        .work            = work,
        .ed              = ed,
        .topath          = topath,
        .summary         = {0},
        .db              = NULL,
        .entries_res     = NULL,
        .xattrs_res      = NULL,
        .xattr_files_res = NULL,
        .xattr_db_list   = {0},
    };

    PCS_t pcs = {0};

    // if we're in the min-max range use the result of the plugins "dir_action" to determine process_dir
    if ((in->min_level <= work->level) &&
        (work->level <= in->max_level)) {
        pcs.work = work;
        *process_dir = plugins_dir_action(&in->plugins, &pcs);
    }

    if (lstat_wrapper(work->name, &work->statuso, &work->crtime,
                      &work->stat_called, 1, NULL) != 0) {
        rc = 1;
        goto done;
    }

    if (dir2index) { /* implementation leak from gufi_dir2index */
        if (*process_dir != PLUGIN_NO_PROCESS_NO_DESCEND_DIR) {
            /* remove db.db for now */
            topath->len -= 1 + DBNAME_LEN;
            topath->data[topath->len] = '\0';

            /* don't need recursion because parent is guaranteed to exist */
            rc = mkdir(topath->data, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

            /* restore "/db.db" */
            topath->data[topath->len] = '/';
            topath->len += 1 + DBNAME_LEN;

            if (rc != 0) {
                const int err = errno;
                if (err != EEXIST) {
                    fprintf(stderr, "mkdir %s failure: %d %s\n", topath->data, err, strerror(err));
                    rc = -(err == ENOSPC);
                    goto done;
                }

                rc = 0;
            }
        }
    }

    /*
     * set up for processing, but keep to minimum to quickly hit
     * descend (and enqueue more work, keeping queues fed)
     */
    if (*process_dir == PLUGIN_PROCESS_DIR) {
        int copy_err = 0;
        inda.db = template_to_db(temp_db, topath->data,
                                 work->statuso.st_uid, work->statuso.st_gid,
                                 &copy_err);

        if (!inda.db) {
            rc = -(copy_err == ENOSPC);
            goto done;
        }

        pcs.db = inda.db;
        pcs.work = work;
        pcs.ed = ed;
        pcs.summary = &inda.summary;
        pcs.data = NULL;

        /* prepare to insert into the database */
        zeroit(&inda.summary);

        /* prepared statements within db.db */
        inda.entries_res = insertdbprep(inda.db, ENTRIES_INSERT);
        inda.xattrs_res = NULL;
        inda.xattr_files_res = NULL;

        if (in->process_xattrs) {
            inda.xattrs_res = insertdbprep(inda.db, XATTRS_PWD_INSERT);
            inda.xattr_files_res = insertdbprep(inda.db, EXTERNAL_DBS_PWD_INSERT);

            /* external per-user and per-group dbs */
            sll_init(&inda.xattr_db_list);
        }

        startdb(inda.db);

        /* run light-weight plugin setup */
        plugins_ctx_init(&in->plugins, &pcs, inda.id);
    }

    if (*process_dir != PLUGIN_NO_PROCESS_NO_DESCEND_DIR){
        if (dir2index) {
            /* remove db.db for now */
            topath->len -= 1 + DBNAME_LEN;
            topath->data[topath->len] = '\0';
        }

        descend(ctx, in, work, dir, 1,
                try_skip_lstat, NULL, NULL,
                processdir, (*process_dir == PLUGIN_PROCESS_DIR)?index_nondir:NULL, &inda,
                ctrs);

        if (dir2index) {
            /* restore "/db.db" (not strictly necessary) */
            topath->data[topath->len] = '/';
            topath->len += 1 + DBNAME_LEN;
        }
    }

    /*
     * now that subdirectories have been enqueued,
     * do slower processing on this directory
     */
    if (*process_dir == PLUGIN_PROCESS_DIR) {
        /* entries and xattrs have been added to the transaction */

        if (in->process_xattrs) {
            /* write out per-user and per-group xattrs */
            sll_destroy(&inda.xattr_db_list, destroy_xattr_db);

            /* keep track of per-user and per-group xattr dbs */
            insertdbfin(inda.xattr_files_res);
        }
        insertdbfin(inda.entries_res);

        if (in->process_xattrs) {
            /* pull this directory's xattrs because they were not pulled by the parent */
            xattrs_setup(&inda.ed->xattrs);
            xattrs_get(work->name, &inda.ed->xattrs);
        }

        /* allow plugins to modify this directory's metadata before summary/xattr insertion */
        plugins_pre_process_dir(&in->plugins, &pcs, inda.id);

        if (in->process_xattrs) {
            /* directory xattrs go into the same table as entries xattrs */
            insertdbgo_xattrs_avail(work, inda.ed, inda.xattrs_res);
            insertdbfin(inda.xattrs_res);
        }

        /* insert this directory's summary data */
        /* the xattrs go into the xattrs_avail table in db.db */
        insertsumdb(inda.db, work->name + work->name_len - work->basename_len,
                    work, ed, &inda.summary);

        /* run plugin post_processing before destroying data */
        plugins_post_process_dir(&in->plugins, &pcs, inda.id);

        /* end the transaction */
        stopdb(inda.db);

        if (in->process_xattrs) {
            xattrs_cleanup(&ed->xattrs);
        }

        plugins_ctx_exit(&in->plugins, &pcs, inda.id);

        closedb(inda.db);
        inda.db = NULL;
    }

  done:
    return rc;
}
