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
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "config.h"
#include "dbutils.h"
#include "popen_argv.h"
#include "str.h"
#include "template_db.h"

#include "gufi_query/spread_tree.h"

#define SQLITE3 "sqlite3"

#define EXTDB "extdb" /* extdbprefix.db table name */

/* namespaces for copying from spread tree to extdb */
#define SPREAD_ATTACH_SHARD_NAME "shard" /* source */
#define SPREAD_ATTACH_EXTDB_NAME "extdb" /* destination */

typedef struct SpreadTreeExtdbCache {
    struct template_db template;   /* cache extdb for copying with copyfd(), which calls sendfile(2) on linux */
    str_t *shard_to_extdb;         /* cache shard_to_extdb SQL */
} SpreadTreeExtdbCache_t;

static void spreadtreesqlcache_free(void *ptr) {
    SpreadTreeExtdbCache_t *cache = (SpreadTreeExtdbCache_t *) ptr;
    str_free(cache->shard_to_extdb);
    close(cache->template.fd);
    free(cache);
}

typedef struct StrCounter {
    char **strs;
    size_t count;
} StrCounter_t;

static int copy_str_and_counter_callback(void *args, int count, char **data, char **columns) {
    StrCounter_t *sc = (StrCounter_t *) args;
    copy_columns_callback(sc->strs, count, data, columns);
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
 * SELECT create_spread(extdbprefix, prefix, fsid, rpath(sname, sroll, name), inode, mtime, flock_dir)
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
    const char *flock_dir      = (char *) sqlite3_value_text(argv[6]);

    /* run the shard generator program */
    const char *copy_to_shard_cmd[] = {
        "gufi_spread_copy_to_shard.sh",
        extdbprefix_db,
        prefix, fsid, path,
        inode, mtime,
        flock_dir,
        NULL /* must NULL terminate */
    };

    char *shard_path = NULL;
    char *err = NULL;
    if (run_cmd(copy_to_shard_cmd, &shard_path, &err, 1, NULL, 0) != 0) {
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
static SpreadTreeExtdbCache_t *cache_extdb(sqlite3_context *context, trie_t *caches,
                                           const char *prefix, const size_t prefix_len,
                                           const char *extdb_schema_path,
                                           const char *shard_to_extdb_path) {
    SpreadTreeExtdbCache_t *cache = NULL;
    if (trie_search(caches, prefix, prefix_len, (void **) &cache) == 1) {
        return cache;
    }

    /* not found - create cache */

    /*
     * SQL used to create an empty external database
     *
     * do not use SELECT statements that print to stdout
     * (e.g. SELECT load_extension('...'))
     */
    str_t *schema = read_file(context, extdb_schema_path);
    if (!schema) {
        return NULL;
    }

    char extdb_path[] = "template.XXXXXX";
    int extdb_fd = mkstemp(extdb_path);

    /* create the external database template file with shell */
    {
        const char *cmd[] = { SQLITE3, extdb_path, NULL };
        char *res = NULL; /* unused; will be allocated, but will be empty/ignored */
        char *err = NULL;
        const str_t *inputs[] = { schema };
        const int rc = run_cmd(cmd, &res, &err, 0, inputs, 1);
        free(res);

        if (rc != 0) {
            char *errmsg = sqlite3_mprintf("Could not run schema file to set up \"%s\": %s",
                                           extdb_path, err);
            sqlite3_result_error(context, errmsg, -1);
            sqlite3_free(errmsg);
            sqlite3_free(err);
            str_free(schema);
            close(extdb_fd);
            remove(extdb_path);
            return NULL;
        }
    }

    str_free(schema);

    /* fsync? */

    /* no need for the path any more */
    if (remove(extdb_path) != 0) {
        const int err = errno;
        fprintf(stderr, "Warning: Could not remove \"%s\": %s (%d)\n",
                extdb_path, strerror(err), err);
    }

    /* get the size of the database file */
    const off_t extdb_size = lseek(extdb_fd, 0, SEEK_END);
    if (extdb_size == (off_t) -1) {
        const int err = errno;
        char *errmsg = sqlite3_mprintf("Could not lseek \"%s\": %s",
                                       extdb_path, strerror(err), err);
        sqlite3_result_error(context, errmsg, -1);
        sqlite3_free(errmsg);
        close(extdb_fd);
        return NULL;
    }

    /*
     * SQL to copy from a shard to an external database
     *
     * SQL must select from the shard namespace and insert into the extdb namespace
     */
    str_t *shard_to_extdb = read_file(context, shard_to_extdb_path);
    if (!shard_to_extdb) {
        close(extdb_fd);
        return NULL;
    }

    cache = malloc(sizeof(*cache));
    cache->template.fd = extdb_fd;
    cache->template.size = extdb_size;
    cache->shard_to_extdb = shard_to_extdb;
    trie_insert(caches, prefix, prefix_len, cache, spreadtreesqlcache_free);

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
        fprintf(stderr, "Warning: Failed to chown \"%s\" to uid=%" STAT_uid " gid=%" STAT_gid ": %s (%d)\n",
                path, uid, gid, strerror(err), err);
    }

    return 0;
}

static int set_up_extdb(sqlite3_context *context, const char *prefix,
                        struct PoolArgs *pa, gqw_t *gqw, trie_t *set_up,
                        /* const char *inode_str, */ const char *mode_str,  const char *uid_str,  const char *gid_str,
                        const char *pinode_str, /* const char *dmode_str, */ const char *duid_str, const char *dgid_str,
                        SpreadTreeExtdbCache_t *extdb_cache,
                        const char *shard_path) {
    int rc = 1;

    /* convert input arguments to integers */
    read_int(mode_t, mode,  mode_str , STAT_mode, "mode");
    read_int(uid_t,  uid,   uid_str,   STAT_uid,  "uid");
    read_int(gid_t,  gid,   gid_str,   STAT_gid,  "gid");
    /* read_int(mode_t, dmode, dmode_str, STAT_mode, "directory mode"); */
    read_int(uid_t,  duid,  duid_str,  STAT_uid,  "directory uid");
    read_int(gid_t,  dgid,  dgid_str,  STAT_gid,  "directory gid");

    /* figure out what permission set the data is in */
    const char READ_PERMS[] = "ugo";
    typedef enum {
        PERM_UR       = 0,
        PERM_GR       = 1,
        PERM_OR       = 2,
        PERM_UNCOMMON = 3,
    } ReadPerm_t;

    ReadPerm_t rp = PERM_UNCOMMON;
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

    char *extdb_basename = NULL;

    if (rp != PERM_UNCOMMON) {
        /* <prefix>_<permissions> */
        extdb_basename = sqlite3_mprintf("%s_%cr",
                                         prefix,
                                         READ_PERMS[rp]);
    }
    else {
        /* <prefix>_<uid>_<gid> */
        extdb_basename = sqlite3_mprintf("%s_%s_%s",
                                         prefix,
                                         uid_str, gid_str);
    }

    /* generate the destination path */
    char *extdb_path = sqlite3_mprintf("%s/%s.db",
                                       gqw->work.name,
                                       extdb_basename);

    /* key for this directory + permission permutation to check */
    char *key = sqlite3_mprintf("%s/%s",
                                pinode_str, extdb_basename);

    const size_t key_len = strlen(key);

    /* only set up external database once per directory + permission permutation */
    if (trie_search(set_up, key, key_len, NULL) == 0) {
        /* make sure this external database path exists (old file or create new) */
        struct stat st;
        if (stat(extdb_path, &st) == 0) { /* stat(2), not lstat(2) */
            if (!S_ISREG(st.st_mode)) {
                char *errmsg = sqlite3_mprintf("\"%s\" is not a file",
                                               extdb_path);
                sqlite3_result_error(context, errmsg, -1);
                sqlite3_free(errmsg);
                goto done;
            }

            /* check permissions/owners? */

            /* not verifying the file is a valid db */
        }
        else {
            const int err = errno;
            if (err != ENOENT) {
                char *errmsg = sqlite3_mprintf("Could stat \"%s\" failed: %s (%d)",
                                               extdb_path, strerror(err), err);
                sqlite3_result_error(context, errmsg, -1);
                sqlite3_free(errmsg);
                goto done;
            }

            /* if the file does not exist, create it */

            const int extdb_fd = open(extdb_path, O_CREAT | O_WRONLY);
            if (extdb_fd < 0) {
                const int err = errno;
                char *errmsg = sqlite3_mprintf("Could not create external database \"%s\": %s (%d)",
                                               extdb_path, strerror(err), err);
                sqlite3_result_error(context, errmsg, -1);
                sqlite3_free(errmsg);
                goto done;
            }

            /* copy the prefix cache to this specific external database */
            const ssize_t written = copyfd(extdb_cache->template.fd, 0,
                                           extdb_fd, 0,
                                           extdb_cache->template.size);
            close(extdb_fd);

            if (written != extdb_cache->template.size) {
                const int err = errno;
                char *errmsg = sqlite3_mprintf("Could not initialize external database \"%s\": %s (%d)",
                                               extdb_path, strerror(err), err);
                sqlite3_result_error(context, errmsg, -1);
                sqlite3_free(errmsg);
                remove(extdb_path); /* ignore errors? */
                goto done;
            }

            /* set file permissions and ownership */
            char *errmsg = NULL;
            switch (rp) {
                case PERM_UR:
                    rc = chmod_chown(extdb_path, S_IRUSR | S_IWUSR, duid, pa->in->nobody.gid, &errmsg);
                    break;
                case PERM_GR:
                    rc = chmod_chown(extdb_path, S_IRGRP | S_IWGRP, pa->in->nobody.uid, dgid, &errmsg);
                    break;
                case PERM_OR:
                    rc = chmod_chown(extdb_path, S_IROTH | S_IWOTH, pa->in->nobody.uid, pa->in->nobody.gid, &errmsg);
                    break;
                case PERM_UNCOMMON:
                default:
                    rc = chmod_chown(extdb_path, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP, duid, dgid, &errmsg);
                    break;
            }

            if (rc != 0) {
                sqlite3_result_error(context, errmsg, -1);
                sqlite3_free(errmsg);
                remove(extdb_path); /* ignore errors? */
                goto done;
            }
        }

        /* record that this external database has been set up */
        trie_insert(set_up, key, key_len, NULL, NULL);
    }

    /* copy data from the shard to the external database */
    {
        char *extdb_uri = sqlite3_mprintf("%s/%s.db",
                                          gqw->sqlite3_name,
                                          extdb_basename);

        char *sql = sqlite3_mprintf("ATTACH 'file:%q?mode=ro' AS %s;\n"
                                    "ATTACH 'file:%q?mode=rw' AS %s;\n"
                                    "%s"
                                    /* skipping DETACH */
                                    ,
                                    shard_path, SPREAD_ATTACH_SHARD_NAME,
                                    extdb_uri,  SPREAD_ATTACH_EXTDB_NAME,
                                    extdb_cache->shard_to_extdb->data);

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
        sqlite3_free(extdb_uri);

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
    sqlite3_free(key);
    sqlite3_free(extdb_path);
    sqlite3_free(extdb_basename);
    return rc;
}

/*
 * moving external data into GUFI, so cannot let users define most of functionality
 *
 * First, ATTACH extdbprefix.db in -I
 *
 * SELECT spread_to_external(prefix, fsid, rpath(sname, sroll, name),
 *                           inode, mode, uid, gid, mtime,
 *                           pinode, dmode, duid, dgid)
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
    const char *pinode = (char *) sqlite3_value_text(argv[8]);
    /* const char *dmode  = (char *) sqlite3_value_text(argv[9]); */
    const char *duid   = (char *) sqlite3_value_text(argv[10]);
    const char *dgid   = (char *) sqlite3_value_text(argv[11]);

    char *err = NULL;

    /* get data from extdbprefix.db */
    char *extdbprefix[] = { NULL, NULL, NULL, NULL, };
    if (pull_from_db(db, "spread_top, gen_shard_path, extdb_schema, shard_to_extdb",
                     2, EXTDB, "prefix", prefix, extdbprefix, &err) != 0) {
        char *errmsg = sqlite3_mprintf("Could not data from extdb: %s", err);
        sqlite3_result_error(context, errmsg, -1);
        sqlite3_free(errmsg);
        sqlite3_free(err);
        return;
    }

    /* get the program that generates the shard name */
    char *spread_tree    = extdbprefix[0];
    char *gen_shard_path = extdbprefix[1];
    char *extdb_schema   = extdbprefix[2];
    char *shard_to_extdb = extdbprefix[3];

    /* get SQL from files */
    SpreadTreeExtdbCache_t *extdb_cache = cache_extdb(context, pa->ta[ctx->id].extdb_caches,
                                                      prefix, strlen(prefix),
                                                      extdb_schema, shard_to_extdb);
    if (!extdb_cache) {
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
    set_up_extdb(context, prefix, pa, ctx->gqw, ctx->set_up,
                 /* inode, */ mode, uid, gid,
                 pinode, /* dmode, */ duid, dgid,
                 extdb_cache, shard_path);
    /* fallthrough */

    free(shard_path);

  free_extdbprefix:
    free(shard_to_extdb);
    free(extdb_schema);
    free(gen_shard_path);
    free(spread_tree);
}

int addspreadfuncs(sqlite3 *db, asfctx_t *ctx) {
    return !(
        (sqlite3_create_function(db,   "create_spread",      7,    SQLITE_UTF8,
                                 NULL, &create_spread,       NULL, NULL) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "spread_to_external", 12,   SQLITE_UTF8,
                                 ctx,  &spread_to_external,  NULL, NULL) == SQLITE_OK)
        );
}
