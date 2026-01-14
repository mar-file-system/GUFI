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



#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "SinglyLinkedList.h"
#include "dsi.h"
#include "plugin.h"
#include "utils.h"           /* SNPRINTF, SNFORMAT_S */

/*
 * dsi_collection_name(xattr_name)
 *
 * if the xattr_name is a DSI xattr, return the collection name
 * if not, return NULL
 */
static void dsi_collection_name(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    const char *name = (char *) sqlite3_value_text(argv[0]);

    if (!name || !is_dsi_xattr(name, strlen(name))) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, name + DSI_NAME_PREFIX_LEN, -1, SQLITE_TRANSIENT);
}

/*
 * dsi_uuid(xattr_name, xattr_value)
 *
 * if the xattr_name is a DSI xattr, extract the UUID from xattr_value
 * if not, return NULL
 */
static void dsi_uuid(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    const char *name = (char *) sqlite3_value_text(argv[0]);

    if (!name || !is_dsi_xattr(name, strlen(name))) {
        sqlite3_result_null(context);
        return;
    }

    const char *value = (char *) sqlite3_value_text(argv[1]);
    sqlite3_result_text(context, value, DSI_VALUE_UUID_LEN, SQLITE_TRANSIENT);
}

/*
 * dsi_dbpath(xattr_name, xattr_value)
 *
 * if the xattr_name is a DSI xattr, extract the db path from xattr_value
 * if not, return NULL
 */
static void dsi_dbpath(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    const char *name = (char *) sqlite3_value_text(argv[0]);

    if (!name || !is_dsi_xattr(name, strlen(name))) {
        sqlite3_result_null(context);
        return;
    }

    const char *value = (char *) sqlite3_value_text(argv[1]);
    sqlite3_result_text(context, value + DSI_VALUE_PREFIX_LEN, -1, SQLITE_TRANSIENT);
}

/* used for both plugin_user_data and sqlite3_exec callback */
struct DSI_Querying_State {
    sqlite3 *db;
    sll_t attached; /* list of collection name strings */
};

static void dsi_querying_global_init(void *global) {
    struct input *in = (struct input *) global;
    in->process_xattrs = 1; /* automatically enable xattr processing if not already */

    sqlite3_initialize();
}

/* attach collection dbs once using the directory's xattrs instead of once per file/link */
const char ATTACH_COLLECTIONDB_SQL[] = "SELECT rpath(sname, sroll), xattr_name FROM vrxsummary "
                                       "WHERE xattr_name IS NOT NULL;";
static int attach_collectiondb_cb(void *args, int count, char **data, char **columns) {
    struct DSI_Querying_State *dsiqs = (struct DSI_Querying_State *) args;

    (void) count;
    (void) columns;

    const char *pwd        = data[0];
    const char *xattr_name = data[1];

    /* xattr_name == NULL has already been checked */
    const size_t xattr_name_len = strlen(xattr_name);

    /* not a DSI collection db name */
    if (!is_dsi_xattr(xattr_name, xattr_name_len)) {
        return 0;
    }

    const size_t collection_name_len = xattr_name_len - DSI_NAME_PREFIX_LEN;
    const char *collection_name = xattr_name + DSI_NAME_PREFIX_LEN;

    /* check if attaching will work - correct check requires SQL - just do simple check here */
    if (sll_get_size(&dsiqs->attached) == 253) { /* max 254 - 1 (db.db) */
        fprintf(stderr, "Warning: Not attaching %s: Too many attaches\n", collection_name);
        return 0;
    }

    /* get the copied collection db file path */
    const size_t pwd_len = strlen(pwd);
    const size_t collection_dbname_len = pwd_len + 1 + collection_name_len + 3;
    char *collection_dbname = malloc(collection_dbname_len + 1);
    SNFORMAT_S(collection_dbname, collection_dbname_len + 1, 4,
               pwd, pwd_len,
               "/", (size_t) 1,
               collection_name, collection_name_len,
               ".db", (size_t) 3);

    /* copy the collection name */
    char *attach_name = strndup(collection_name, collection_name_len);

    /* attach collection db as the collection name */
    char attach_sql[MAXSQL];
    SNPRINTF(attach_sql, sizeof(attach_sql), "ATTACH 'file:%s?mode=ro' AS '%s';",
             collection_dbname, attach_name);

    if (sqlite3_exec(dsiqs->db, attach_sql, NULL, NULL, NULL) != SQLITE_OK) {
        free(attach_name);
        free(collection_dbname);
        /*
         * Not an error - could not attach for some reason (probably
         * permissions, or does not exist) - so be it
         */
        return 0;
    }

    /* keep attach name for detaching */
    sll_push_back(&dsiqs->attached, attach_name);

    free(collection_dbname);
    return 0;
}

static void dsi_querying_ctx_exit(void *ptr, void *plugin_user_data);

static void *dsi_querying_ctx_init(void *ptr) {
    sqlite3 *db = (sqlite3 *) ptr;

    if ((sqlite3_create_function(db,   "dsi_collection_name", 1, SQLITE_UTF8,
                                 NULL, &dsi_collection_name,  NULL, NULL) != SQLITE_OK) ||
        (sqlite3_create_function(db,   "dsi_uuid",            2, SQLITE_UTF8,
                                 NULL, &dsi_uuid,             NULL, NULL) != SQLITE_OK) ||
        (sqlite3_create_function(db,   "dsi_dbpath",          2, SQLITE_UTF8,
                                 NULL, &dsi_dbpath,           NULL, NULL) != SQLITE_OK)) {
        fprintf(stderr, "Error: Could not create DSI functions\n");
        return NULL;
    }

    struct DSI_Querying_State *dsiqs = malloc(sizeof(*dsiqs));
    dsiqs->db = db;
    sll_init(&dsiqs->attached);

    /* attach all collection dbs known to this file */
    char *err = NULL;
    if (sqlite3_exec(db, ATTACH_COLLECTIONDB_SQL, attach_collectiondb_cb, dsiqs, &err) != SQLITE_OK) {
        fprintf(stderr, "Error: Could not get xattrs for attaching collection dbs: %s\n", err);
        sqlite3_free(err);
        dsi_querying_ctx_exit(ptr, dsiqs);
        return NULL;
    }

    return dsiqs;
}

static void dsi_querying_ctx_exit(void *ptr, void *plugin_user_data) {
    (void) ptr;

    struct DSI_Querying_State *dsiqs = (struct DSI_Querying_State *) plugin_user_data;
    if (!dsiqs) {
        return;
    }

    /* detach all collection dbs */
    sll_loop(&dsiqs->attached, node) {
        const char *attach_name = sll_node_data(node);

        char detach_sql[MAXSQL];
        SNPRINTF(detach_sql, sizeof(detach_sql), "DETACH '%s';", attach_name);

        char *err = NULL;
        if (sqlite3_exec(dsiqs->db, detach_sql, NULL, NULL, &err) != SQLITE_OK) {
            fprintf(stderr, "Error: Could not detach collection db %s: %s\n", attach_name, err);
            sqlite3_free(err);
            /* keep going */
        }
    }

    sll_destroy(&dsiqs->attached, free);
    free(dsiqs);
}

static void dsi_querying_global_exit(void *global) {
    (void) global;
    sqlite3_shutdown();
}

struct plugin_operations GUFI_PLUGIN_SYMBOL = {
    .type = PLUGIN_QUERY,
    .global_init = dsi_querying_global_init,
    .ctx_init = dsi_querying_ctx_init,
    .process_dir = NULL,
    .process_file = NULL,
    .ctx_exit = dsi_querying_ctx_exit,
    .global_exit = dsi_querying_global_exit,
};
