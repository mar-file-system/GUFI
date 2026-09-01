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
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "config.h"
#include "dbutils.h"
#include "popen_argv.h"
#include "str.h"

#include "gufi_query/spread_tree.h"

#define SQLITE3 "sqlite3"

#define EXTDB "extdb" /* extdbprefix.db table name */

/* namespaces for copying from spread tree to extdb */
#define SPREAD_ATTACH_SHARD_NAME "shard" /* source */
#define SPREAD_ATTACH_EXTDB_NAME "extdb" /* destination */

typedef struct SpreadTreeSQLCache {
    str_t *schema;                       /* cache schema SQL here */
    str_t *shard_to_extdb;               /* cache shard_to_extdb SQ: here */
} SpreadTreeSQLCache_t;

static void spreadtreesqlcache_free(void *ptr) {
    SpreadTreeSQLCache_t *cache = (SpreadTreeSQLCache_t *) ptr;
    str_free(cache->shard_to_extdb);
    str_free(cache->schema);
    free(cache);
}

typedef struct StrCounter {
    char **strs;
    size_t count;
} StrCounter_t;

static int copy_str_and_counter_callback(void *args, int count, char **data, char **columns) {
    (void) columns;

    /*
     * strs should have space for at least count columns
     *
     * make sure to initialize strs[i] to NULL or good values
     */
    StrCounter_t *sc = (StrCounter_t *) args;

    for(int i = 0; i < count; i++) {
        if (!data[i]) {
            sc->strs[i] = NULL;
            continue;
        }

        const size_t len = strlen(data[i]);

        if (!sc->strs[i]) {
            sc->strs[i] = malloc(len + 1);
        }

        SNFORMAT_S(sc->strs[i], len + 1, 1,
                   data[i], len);
    }

    sc->count++;

    return 0;
}

static int pull_from_db(sqlite3 *db, const char *columns, const size_t col_count,
                        const char *table, const char *match_column, const char *match_value,
                        char **results, char **err) {
    for(size_t i = 0; i < col_count; i++) {
        results[i] = NULL;
    }

    StrCounter_t sc = {
        .strs = results,
        .count = 0,
    };

    char *sql = sqlite3_mprintf("SELECT %s FROM %s WHERE %s == %Q;",
                                columns, table, match_column, match_value);
    const int rc = sqlite3_exec(db, sql, copy_str_and_counter_callback, &sc, err);
    sqlite3_free(sql);

    if (rc != SQLITE_OK) {
        for(size_t i = 0; i < col_count; i++) {
            results[i] = NULL;
        }
        return 1;
    }

    if (sc.count != 1) {
        *err = sqlite3_mprintf("Did not get exactly one set of results");
        for(size_t i = 0; i < col_count; i++) {
            free(results[i]);
            results[i] = NULL;
        }
        return 1;
    }

    return 0;
}

static int run_cmd(const char **cmd, char **res, char **err, const int expect_output,
                   const str_t **input, const size_t input_count) {
    *res = NULL;

    popen_argv_t *p = popen_argv(cmd, !!input);
    if (p == NULL) {
        *err = sqlite3_mprintf("Failed to run %s", cmd[0]);
        return 1;
    }

    if (input_count) {
        const int in = popen_argv_in(p);
        for(size_t i = 0; i < input_count; i++) {
            if (write_size(in, input[i]->data, input[i]->len) != (ssize_t) input[i]->len) {
                *err = sqlite3_mprintf("Could not write to %s", cmd[0]);
                popen_argv_close(p);
                return 1;
            }
        }
        close(in);
    }

    const int out = popen_argv_out(p);

    if (expect_output) {
        size_t n = 0;
        const ssize_t rc = getline_fd_stream(res, &n, out, GETLINE_DEFAULT_SIZE);
        if (rc < 1) {
            if (err) {
                *err = sqlite3_mprintf("Did not get result from %s", cmd[0]);
            }
            free(*res);
            *res = NULL;
            popen_argv_close(p);
            return 1;
        }
    }

    if (popen_argv_close(p) != 0) {
        if (err) {
            *err = sqlite3_mprintf("%s errored", cmd[0]);
        }
        free(*res);
        *res = NULL;
        return 1;
    }

    return 0;
}

/*
 * create the spread tree path for a single record
 *
 * SELECT create_spread(extdbprefix, prefix, fsid, rpath(sname, sroll, name), inode, mtime)
 * FROM vrpentries
 * WHERE ...
 *
 * extdbprefix.db, prefix, and fsid come from an external source
 * create_spread returns the path of the generated shard
 */
static void create_spread(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    /* these values are forwarded without being checked */
    const char *extdbprefix_db = (char *) sqlite3_value_text(argv[0]);
    const char *prefix         = (char *) sqlite3_value_text(argv[1]);
    const char *fsid           = (char *) sqlite3_value_text(argv[2]);
    const char *path           = (char *) sqlite3_value_text(argv[3]);
    const char *inode          = (char *) sqlite3_value_text(argv[4]);
    const char *mtime          = (char *) sqlite3_value_text(argv[5]);

    /* run the shard generator program */
    const char *gen_shard_cmd[] = {
        "gufi_spread_copy_to_shard.sh",
        extdbprefix_db,
        prefix, fsid, path,
        inode, mtime,
        NULL /* must NULL terminate */
    };

    char *shard_path = NULL;
    char *err = NULL;
    if (run_cmd(gen_shard_cmd, &shard_path, &err, 1, NULL, 0) != 0) {
        sqlite3_result_error(context, err, -1);
        sqlite3_free(err);
        return;
    }

    /* result of this function is the path of the shard */
    sqlite3_result_text(context, shard_path, -1, free);
}

static str_t *read_file(sqlite3_context *context, const char *path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        const int err = errno;
        char *errmsg = sqlite3_mprintf("Could not open file \"%s\": %s (%d)",
                                       path, strerror(err), err);
        sqlite3_result_error(context, errmsg, -1);
        sqlite3_free(errmsg);
        return NULL;
    }

    const off_t len = lseek(fd, 0, SEEK_END);
    if (len < 0) {
        const int err = errno;
        char *errmsg = sqlite3_mprintf("Could not get size of file \"%s\": %s (%d)",
                                       path, strerror(err), err);
        sqlite3_result_error(context, errmsg, -1);
        sqlite3_free(errmsg);
        close(fd);
        return NULL;
    }

    if (lseek(fd, 0, SEEK_SET) != 0) {
        const int err = errno;
        char *errmsg = sqlite3_mprintf("Could not reset position of file \"%s\": %s (%d)",
                                       path, strerror(err), err);
        sqlite3_result_error(context, errmsg, -1);
        sqlite3_free(errmsg);
        close(fd);
        return NULL;
    }

    char *data = malloc(len + 1);
    const ssize_t rs = read_size(fd, data, len);
    close(fd);

    if (rs != len) {
        char *errmsg = sqlite3_mprintf("Could not read sql schema file \"%s\"",
                                       path);
        sqlite3_result_error(context, errmsg, -1);
        sqlite3_free(errmsg);
        free(data);
        return NULL;
    }

    data[len] = '\0';

    str_t *ret = malloc(sizeof(*ret));
    ret->data = data;
    ret->len = len;
    ret->free = free;

    return ret;
}

/* find existing sql cache or create new cache for this prefix */
static SpreadTreeSQLCache_t *get_sql(sqlite3_context *context, trie_t *caches,
                                     const char *prefix, const size_t prefix_len,
                                     const char *schema_file, const char *copy_to_extdb) {
    SpreadTreeSQLCache_t *cache = NULL;

    /* not found - create */
    if (trie_search(caches, prefix, prefix_len, (void **) &cache) == 0) {
        /*
         * SQL used to create an empty external database
         *
         * do not use SELECT statements that print to stdout
         * (e.g. SELECT load_extension('...'))
         */
        str_t *schema = read_file(context, schema_file);
        if (!schema) {
            return NULL;
        }

        /*
         * SQL to copy from a shard to an external database
         *
         * SQL must select from the shard namespace and insert into the extdb namespace
         */
        str_t *shard_to_extdb = read_file(context, copy_to_extdb);
        if (!shard_to_extdb) {
            str_free(schema);
            return NULL;
        }

        cache = malloc(sizeof(*cache));
        cache->schema = schema;
        cache->shard_to_extdb = shard_to_extdb;
        trie_insert(caches, prefix, prefix_len, cache, spreadtreesqlcache_free);
    }

    return cache;
}

#define read_int(type, name, str, format, err_name)                     \
    type name = 0;                                                      \
    if (!str) {                                                         \
        char *errmsg = sqlite3_mprintf("Missing %s", err_name);         \
        sqlite3_result_error(context, errmsg, -1);                      \
        sqlite3_free(errmsg);                                           \
        return 1;                                                       \
    }                                                                   \
                                                                        \
    if (sscanf(str, "%" format, &name) != 1) {                          \
        char *errmsg = sqlite3_mprintf("Bad %s: %s", err_name, str);    \
        sqlite3_result_error(context, errmsg, -1);                      \
        sqlite3_free(errmsg);                                           \
        return 1;                                                       \
    }

static int chmod_chown(const char *path, const mode_t mode,
                       const uid_t uid, const gid_t gid,
                       char **errmsg) {
    if (chmod(path, mode) != 0) {
        if (errmsg) {
            const int err = errno;
            *errmsg = sqlite3_mprintf("Failed to chmod \"%s\": %s (%d)",
                                      path, strerror(err), err);
        }
        return 1;
    }

    /* requires root, so only printing warning, not error */
    if (chown(path, uid, gid) != 0) {
        const int err = errno;
        fprintf(stderr, "Failed to chown \"%s\" to %" STAT_uid " %" STAT_gid ": %s (%d)\n",
                path, uid, gid, strerror(err), err);
        return 1;
    }

    return 0;
}

static int set_up_extdb(sqlite3_context *context, const char *prefix,
                        struct PoolArgs *pa, gqw_t *gqw,
                        const char *mode_str,  const char *uid_str,  const char *gid_str,
                        const char *dmode_str, const char *duid_str, const char *dgid_str,
                        SpreadTreeSQLCache_t *sql_cache,
                        const char *shard_path) {
    /* convert input arguments to integers */
    read_int(mode_t, mode,  mode_str , STAT_mode, "mode");
    read_int(uid_t,  uid,   uid_str,   STAT_uid,  "uid");
    read_int(gid_t,  gid,   gid_str,   STAT_gid,  "gid");
    read_int(mode_t, dmode, dmode_str, STAT_mode, "directory mode"); /* unused */
    read_int(uid_t,  duid,  duid_str,  STAT_uid,  "directory uid");
    read_int(gid_t,  dgid,  dgid_str,  STAT_gid,  "directory gid");

    /* figure out what permission set the data is in */
    const char READ_PERMS[] = "ugo";
    typedef enum {
        PERM_UR    = 0,
        PERM_GR    = 1,
        PERM_OR    = 2,
        PERM_UNSET = 4,
    } ReadPerm_t;

    ReadPerm_t rp = PERM_UNSET;
    if ((uid == duid) &&
        (mode & S_IRUSR) == S_IRUSR) {
        rp = PERM_UR;
    }
    else if ((gid == dgid) &&
             ((mode & S_IRGRP) == S_IRGRP)) {
        rp = PERM_GR;
    }
    else if ((mode & S_IROTH) == S_IROTH) {
        rp = PERM_OR;
    }

    /* generate the destination path */
    char *extdb_path = NULL;
    char *extdb_uri = NULL;

    /* <index path>/<prefix>_<permissions>.db */
    if (rp != PERM_UNSET) {
        extdb_path = sqlite3_mprintf("%s/%s_%cr.db",
                                     gqw->work.name,
                                     prefix,
                                     READ_PERMS[rp]);
        extdb_uri  = sqlite3_mprintf("%s/%s_%cr.db",
                                     gqw->sqlite3_name,
                                     prefix,
                                     READ_PERMS[rp]);
    }
    /* <index path>/<prefix>_<uid>_<gid>.db */
    else {
        extdb_path = sqlite3_mprintf("%s/%s_%" STAT_uid "_%" STAT_gid ".db",
                                     gqw->work.name,
                                     prefix,
                                     uid, gid);
        extdb_uri  = sqlite3_mprintf("%s/%s_%" STAT_uid "_%" STAT_gid ".db",
                                     gqw->sqlite3_name,
                                     prefix,
                                     uid, gid);
    }

    int rc = 0;

    /* run the schema file with the SQLite 3 cli */
    /* TODO: check existence of file first, and only do once */
    {
        const char *cmd[] = { SQLITE3, extdb_path, NULL };
        char *res = NULL; /* unused; will be allocated, but will be empty/ignored */
        char *err = NULL;
        const str_t *inputs[] = { sql_cache->schema };
        rc = run_cmd(cmd, &res, &err, 0, inputs, 1);
        free(res);

        if (rc != 0) {
            char *errmsg = sqlite3_mprintf("Could not run schema file to set up \"%s\": %s",
                                           extdb_path, err);
            sqlite3_result_error(context, errmsg, -1);
            sqlite3_free(errmsg);
            sqlite3_free(err);
            goto done;
        }
    }

    /* set file permissions and ownership (TODO: check existence of file first, and only do once) */
    switch (rp) {
        case PERM_UR:
            chmod_chown(extdb_path, S_IRUSR | S_IWUSR, duid, pa->in->nobody.gid, NULL);
            break;
        case PERM_GR:
            chmod_chown(extdb_path, S_IRGRP | S_IWGRP, pa->in->nobody.uid, dgid, NULL);
            break;
        case PERM_OR:
            chmod_chown(extdb_path, S_IROTH | S_IWOTH, pa->in->nobody.uid, pa->in->nobody.gid, NULL);
            break;
        case PERM_UNSET:
        default:
            chmod_chown(extdb_path, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP, duid, dgid, NULL);
            break;
    }

    /* copy data from the shard to the external database */
    {
        char *sql = sqlite3_mprintf("ATTACH 'file:%q?mode=ro' AS %s;\n"
                                    "ATTACH 'file:%q?mode=rw' AS %s;\n"
                                    "%s"
                                    /* skipping DETACH */
                                    ,
                                    shard_path, SPREAD_ATTACH_SHARD_NAME,
                                    extdb_uri,  SPREAD_ATTACH_EXTDB_NAME,
                                    sql_cache->shard_to_extdb->data);

        const char *cmd[] = { SQLITE3, NULL }; /* in-memory db */
        char *res = NULL;
        char *err = NULL;
        str_t sql_str = {
            .data = sql,
            .len = strlen(sql),
            .free = sqlite3_free,
        };

        const str_t *inputs[] = { &sql_str };
        rc = run_cmd(cmd, &res, &err, 0, inputs, 1);
        str_free_existing(&sql_str);

        if (rc == 0) {
            sqlite3_result_text(context, extdb_path, -1, SQLITE_TRANSIENT);
        }
        else {
            sqlite3_result_error(context, err, -1);
            sqlite3_free(err);
            /* fallthrough */
        }
    }

  done:
    sqlite3_free(extdb_uri);
    sqlite3_free(extdb_path);
    return rc;
}

/*
 * moving external data into GUFI, so cannot let users define most of functionality
 *
 * First, ATTACH extdbprefix.db in -I
 *
 * SELECT spread_to_external(prefix, fsid, rpath(sname, sroll, name),
 *                           inode, mode, uid, gid, mtime,
 *                           dmode, duid, dgid)
 * FROM vrpentries
 * WHERE ...
 *
 * prefix, and fsid come from an external source
 */
static void spread_to_external(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    /* extdbprefix.db should have been attached once at the top using -I */
    sqlite3 *db = sqlite3_context_db_handle(context);

    asfctx_t *ctx = (asfctx_t *) sqlite3_user_data(context);
    struct PoolArgs *pa = ctx->pa;

    const char *prefix = (char *) sqlite3_value_text(argv[0]);
    const char *fsid   = (char *) sqlite3_value_text(argv[1]);
    const char *path   = (char *) sqlite3_value_text(argv[2]);
    const char *inode  = (char *) sqlite3_value_text(argv[3]);
    const char *mode   = (char *) sqlite3_value_text(argv[4]);
    const char *uid    = (char *) sqlite3_value_text(argv[5]);
    const char *gid    = (char *) sqlite3_value_text(argv[6]);
    const char *mtime  = (char *) sqlite3_value_text(argv[7]);
    const char *dmode  = (char *) sqlite3_value_text(argv[8]);
    const char *duid   = (char *) sqlite3_value_text(argv[9]);
    const char *dgid   = (char *) sqlite3_value_text(argv[10]);

    char *err = NULL;

    /* get data from extdbprefix.db */
    char *extdbprefix[] = { NULL, NULL, NULL, NULL, };
    if (pull_from_db(db, "schema_file, gen_shard_path, spread_top, copy_to_extdb",
                     2, EXTDB, "prefix", prefix, extdbprefix, &err) != 0) {
        char *errmsg = sqlite3_mprintf("Could not data from extdb: %s", err);
        sqlite3_result_error(context, errmsg, -1);
        sqlite3_free(errmsg);
        sqlite3_free(err);
        return;
    }

    /* get the program that generates the shard name */
    char *schema_file    = extdbprefix[0];
    char *gen_shard_path = extdbprefix[1];
    char *spread_tree    = extdbprefix[2];
    char *copy_to_extdb  = extdbprefix[3];

    /* get SQL from files */
    SpreadTreeSQLCache_t *sql_cache = get_sql(context, pa->ta[ctx->id].sql_caches,
                                              prefix, strlen(prefix),
                                              schema_file, copy_to_extdb);
    if (!sql_cache) {
        goto free_extdbprefix;
    }

    /* get the shard name */
    const char *gen_shard_path_cmd[] = {
        gen_shard_path,
        spread_tree, prefix, fsid,
        path, inode, mtime,
        NULL /* must NULL terminate */
    };
    char *shard_path = NULL;
    if (run_cmd(gen_shard_path_cmd, &shard_path, &err, 1, NULL, 0) != 0) {
        sqlite3_result_error(context, err, -1);
        sqlite3_free(err);
        goto free_extdbprefix;
    }

    /* create global spread tree state */
    set_up_extdb(context, prefix, pa, ctx->gqw,
                 mode,  uid,  gid, dmode, duid, dgid,
                 sql_cache, shard_path);
    /* fallthrough */

    free(shard_path);

  free_extdbprefix:
    free(copy_to_extdb);
    free(spread_tree);
    free(schema_file);
    free(gen_shard_path);
}

int addspreadfuncs(sqlite3 *db, asfctx_t *ctx) {
    return !(
        (sqlite3_create_function(db,   "create_spread",      6,    SQLITE_UTF8,
                                 NULL, &create_spread,       NULL, NULL) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "spread_to_external", 11,   SQLITE_UTF8,
                                 ctx,  &spread_to_external,  NULL, NULL) == SQLITE_OK)
        );
}
