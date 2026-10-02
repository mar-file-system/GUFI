/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ThinkParQ GmbH
 */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "bf.h"
#include "beegfs_shared.h"

struct beegfs_index_ctx {
    sqlite3_stmt *entries_stmt;
    sqlite3_stmt *targets_stmt;
    sqlite3_stmt *rst_stmt;
    int dir_fd;
};

static struct beegfs_index_ctx *beegfs_index_ctx_create(sqlite3 *db) {
    struct beegfs_index_ctx *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        return NULL;
    }

    ctx->dir_fd = -1;

    if (beegfs_plugin_create_tables(db) != 0) {
        free(ctx);
        return NULL;
    }

    if (beegfs_plugin_prepare_index_statements(db, &ctx->entries_stmt,
                                               &ctx->targets_stmt, &ctx->rst_stmt) != 0) {
        free(ctx);
        return NULL;
    }

    return ctx;
}

static void beegfs_index_ctx_destroy(struct beegfs_index_ctx **ctxp) {
    if (!ctxp || !*ctxp) {
        return;
    }
    struct beegfs_index_ctx *ctx = *ctxp;

    if (ctx->dir_fd >= 0) {
        close(ctx->dir_fd);
    }

    beegfs_plugin_finalize_index_statements(ctx->entries_stmt, ctx->targets_stmt,
                                            ctx->rst_stmt);
    free(ctx);
    *ctxp = NULL;
}

static int global_init(struct input *in) {
    (void) in;
    return sqlite3_initialize() == SQLITE_OK ? 0 : 1;
}

static void *ctx_init(void *ptr) {
    PCS_t *pcs = (PCS_t *) ptr;
    if (!pcs || !pcs->db || !pcs->work) {
        return NULL;
    }

    struct beegfs_index_ctx *ctx = beegfs_index_ctx_create(pcs->db);
    if (ctx) {
        ctx->dir_fd = open(pcs->work->name, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (ctx->dir_fd < 0) {
            fprintf(stderr, "beegfs plugin: cannot open directory %s: %s\n",
                    pcs->work->name, strerror(errno));
            beegfs_index_ctx_destroy(&ctx);
        }
    }

    return ctx;
}

static int beegfs_index_entry(struct beegfs_index_ctx *ctx,
                              const struct beegfs_entry_metadata *metadata) {
    int64_t entry_rowid = 0;
    int rc = beegfs_plugin_insert_metadata(ctx->entries_stmt, metadata, &entry_rowid);
    if (rc != 0) {
        return rc;
    }

    if (metadata->num_targets > 0) {
        rc = beegfs_plugin_insert_targets(metadata, ctx->targets_stmt, entry_rowid);
        if (rc != 0) {
            return rc;
        }
    }

    if (metadata->num_rst_ids > 0) {
        rc = beegfs_plugin_insert_rst_ids(metadata, ctx->rst_stmt, entry_rowid);
    }

    return rc;
}

static void process_entry(void *ptr, void *user_data) {
    PCS_t *pcs = (PCS_t *) ptr;
    struct beegfs_index_ctx *ctx = (struct beegfs_index_ctx *) user_data;

    if (!pcs || !ctx) {
        return;
    }

    if (!pcs->ed || (pcs->ed->type != 'f')) {
        return;
    }

    struct beegfs_entry_metadata metadata;

    if (beegfs_collect_metadata(ctx->dir_fd, pcs, &metadata) < 0) {
        return;
    }

    sqlite3 *db = sqlite3_db_handle(ctx->entries_stmt);
    if (sqlite3_exec(db, "SAVEPOINT beegfs_entry", NULL, NULL, NULL) != SQLITE_OK) {
        return;
    }

    int rc = beegfs_index_entry(ctx, &metadata);

    if (sqlite3_exec(db, rc == 0 ? "RELEASE beegfs_entry"
                                 : "ROLLBACK TO beegfs_entry; RELEASE beegfs_entry",
                     NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "beegfs plugin: failed to finalize entry savepoint: %s\n",
                sqlite3_errmsg(db));
    }
}

static void ctx_exit(void *ptr, void *user_data) {
    (void) ptr;
    struct beegfs_index_ctx *ctx = (struct beegfs_index_ctx *) user_data;
    beegfs_index_ctx_destroy(&ctx);
}

static void global_exit(struct input *in) {
    (void) in;
    sqlite3_shutdown();
}

struct plugin_operations beegfs_index_ops = {
    .type         = PLUGIN_INDEX,
    .global_init  = global_init,
    .ctx_init     = ctx_init,
    /* after the entry's row is inserted, as process_file ran before */
    .post_process_file = process_entry,
    .ctx_exit     = ctx_exit,
    .global_exit  = global_exit,
};
