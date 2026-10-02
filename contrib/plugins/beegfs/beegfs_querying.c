/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ThinkParQ GmbH
 */

#include <sqlite3.h>

#include "bf.h"
#include "beegfs_shared.h"

static int global_init(struct input *in) {
    (void) in;

    if (sqlite3_initialize() != SQLITE_OK) {
        return 1;
    }

    return 0;
}

static void *ctx_init(void *ptr) {
    sqlite3 *db = (sqlite3 *) ptr;
    if (!db) {
        return NULL;
    }

    (void) beegfs_create_query_views(db);

    return NULL;
}

static void global_exit(struct input *in) {
    (void) in;
    sqlite3_shutdown();
}

struct plugin_operations beegfs_query_ops = {
    .type = PLUGIN_QUERY,
    .global_init = global_init,
    .ctx_init = ctx_init,
    .ctx_exit = NULL,
    .global_exit = global_exit,
};
