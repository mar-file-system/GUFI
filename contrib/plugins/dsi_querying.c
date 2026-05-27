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

#include "bf.h"
#include "plugin.h"

static const char   DSI_NAME_PREFIX[]    = "user.COL.file.";
static const size_t DSI_NAME_PREFIX_LEN  = sizeof(DSI_NAME_PREFIX) - 1;
static const size_t DSI_VALUE_UUID_LEN   = 36;                     /* UUID (32 + 4 separators) */
static const size_t DSI_VALUE_PREFIX_LEN = DSI_VALUE_UUID_LEN + 1; /* UUID + period */

static int is_dsi_xattr(const char *name, const size_t name_len) {
    if (name_len <= DSI_NAME_PREFIX_LEN) {
        return 0;
    }

    return (strncmp(name, DSI_NAME_PREFIX, DSI_NAME_PREFIX_LEN) == 0);
}

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

static int dsi_querying_global_init(void *global) {
    struct input *in = (struct input *) global;
    in->process_xattrs = 1; /* automatically enable xattr processing if not already */

    sqlite3_initialize();
    return 0;
}

static void *dsi_querying_ctx_init(void *ptr) {
    sqlite3 *db = (sqlite3 *) ptr;

    if ((sqlite3_create_function(db,   "dsi_collection_name", 1, SQLITE_UTF8,
                                 NULL, &dsi_collection_name,  NULL, NULL) != SQLITE_OK) ||
        (sqlite3_create_function(db,   "dsi_uuid",            2, SQLITE_UTF8,
                                 NULL, &dsi_uuid,             NULL, NULL) != SQLITE_OK) ||
        (sqlite3_create_function(db,   "dsi_dbpath",          2, SQLITE_UTF8,
                                 NULL, &dsi_dbpath,           NULL, NULL) != SQLITE_OK)) {
        fprintf(stderr, "Error: Could not create DSI functions\n");
    }

    return NULL;
}

static void dsi_querying_global_exit(void *global) {
    (void) global;
    sqlite3_shutdown();
}

struct plugin_operations gufi_plugin_operations = {
    .type = PLUGIN_QUERY,
    .global_init = dsi_querying_global_init,
    .dir_action = NULL,
    .ctx_init = dsi_querying_ctx_init,
    .process_dir = NULL,
    .process_file = NULL,
    .ctx_exit = NULL,
    .global_exit = dsi_querying_global_exit,
};
