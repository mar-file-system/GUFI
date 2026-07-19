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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "incremental_non_path.h"

#define FILEID_LUSTRE  0x97

static int file_handle_size(struct file_handle *fh) {
    /*
     * Hack for Lustre specifically: the filehandle is twice as large as a FID because it
     * can optionally encode the parent FID next to the FID of this object. This functionality
     * is not used for GUFI; in this use case, the second half of the FID is always all 0s.
     *
     * (Storing the parent FID is useful when the target object is a file, not a directory,
     *  but this database only stores directory FIDs.)
     *
     * In order to save space, and make comparisons with FIDs easier when later querying the
     * database, only store the first half of the filehandle.
     */
    if (fh->handle_type == FILEID_LUSTRE && fh->handle_bytes == 32) {
        return 16;
    }

    return sizeof *fh + fh->handle_bytes;
}

static void *file_handle_data(struct file_handle *fh) {
    if (fh->handle_type == FILEID_LUSTRE && fh->handle_bytes == 32) {
        return fh->f_handle;
    }

    return fh;
}

static struct file_handle *alloc_fh(void) {
    struct file_handle *fh = calloc(1, sizeof *fh + MAX_HANDLE_SZ);
    if (!fh)
        return NULL;

    fh->handle_bytes = MAX_HANDLE_SZ;

    return fh;
}

static sqlite3 *open_filehandle_db(const char *parent_dir) {
    sqlite3 *db = NULL;

    char path[4096];
    snprintf(path, 4096, "%s/filehandles.db", parent_dir);

    if (sqlite3_open(path, &db) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open() failed: %s\n", db ? sqlite3_errmsg(db) : "out of memory");
        if (db) {
            sqlite3_close(db);
        }
        return NULL;
    }

    return db;
}

struct fh_incremental_state open_incremental_state(const char *path) {
    struct fh_incremental_state new;

    new.fh_mount_fd = open(path, O_RDONLY|O_DIRECTORY);
    if (new.fh_mount_fd < 0) {
        fprintf(stderr, "Could not open GUFI root '%s': %s\n", path, strerror(errno));
        exit(1);
    }

    new.fh_db = open_filehandle_db(path);
    if (!new.fh_db) {
        fprintf(stderr, "Could not open filehandle database.\n");
        exit(1);
    }

    return new;
}

/*
 * Closes the incremental updates database. Exits the program with an error if
 * any sqlite3 object leaks were detected.
 *
 * Also closes the GUFI tree file descriptor.
 */
void close_incremental_state(struct fh_incremental_state s) {
    if (close(s.fh_mount_fd)) {
        fprintf(stderr, "Warning: closing GUFI tree file descriptor failed: %s\n", strerror(errno));
    }

    if (sqlite3_close(s.fh_db) != SQLITE_OK) {
        fprintf(stderr, "Closing the filehandle database failed. Check for leaked sqlite3 objects.\n");
        exit(1);
    }
}

/*
 * Create the filehandle database in the given parent_dir. The database will be named
 * "filehandles.db".
 *
 * The database has one table which just stores key/value pairs.
 *   - The key is a filehandle in the source filesystem (the FS being indexed)
 *   - The value is a filehandle for the equivalent directory in the index itself
 */
sqlite3 *create_filehandle_db(const char *parent_dir) {
    sqlite3 *db = NULL;

    char path[4096];
    snprintf(path, 4096, "%s/filehandles.db", parent_dir);

    printf("creating FH db at %s\n", path);

    if (sqlite3_open(path, &db) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open() failed: %s\n", db ? sqlite3_errmsg(db) : "out of memory");
        if (db) {
            sqlite3_close(db);
        }
        return NULL;
    }

    const char *sql =
        "CREATE TABLE IF NOT EXISTS filehandles ("
        "  src_fh BLOB NOT NULL PRIMARY KEY,"
        "  dst_fh BLOB NOT NULL"
        ") WITHOUT ROWID;";

    char *errmsg = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &errmsg) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec() failed: %s\n", errmsg ? errmsg : sqlite3_errmsg(db));
        sqlite3_free(errmsg);
        return NULL;
    }

    return db;
}

/*
 * Inserts the source and target filehandles into the database.
 *
 * The target filehandle is stored in its entirety, but for the source filehandle only
 * the handle bytes are stored, not the metadata. This is convenient for two reasons:
 *
 *   1) Since only the f_handle field is needed to adequately distinguish a file,
 *      this saves a few bytes and makes the database file smaller.
 *
 *   2) When indexing Lustre, files are identified by Lustre FIDs which are
 *      equal to the f_handle of a Lustre file handle. By only storing f_handle, a later
 *      comparison with a Lustre FID will work as expected without needing to trim
 *      the file handle metadata away.
 *
 * In order for it to be safe to strip the metadata out of the source filehandle,
 * the handle_bytes and handle_type field must be identical for all files in the
 * source filesystem.
 * That assumption holds for the filesystems we are interested in indexing.
 */
static int fh_insert(sqlite3 *db, void *src_fh, size_t src_len, struct file_handle *dst_fh) {
    int ret = 0;

    const char *sql = "INSERT INTO filehandles(src_fh, dst_fh) VALUES(?, ?);";

    sqlite3_stmt *stmt = NULL;

    // printf("source fid size: %d, dest fid size: %d\n", src_len, dst_fh->handle_bytes);

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "fh_insert(): sqlite3_prepare_v2() failed: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    if (sqlite3_bind_blob(stmt, 1, src_fh, src_len, SQLITE_STATIC) != SQLITE_OK) {
        fprintf(stderr, "fh_insert(): bind source FH failed: %s\n", sqlite3_errmsg(db));
        ret = 1;
        goto out;
    }

    if (sqlite3_bind_blob(stmt, 2, (void *) file_handle_data(dst_fh),
                          file_handle_size(dst_fh), SQLITE_STATIC) != SQLITE_OK) {
        fprintf(stderr, "fh_insert(): bind dest FH failed: %s\n", sqlite3_errmsg(db));
        ret = 1;
        goto out;
    }

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        fprintf(stderr, "fh_insert(): statement failed: %s\n", sqlite3_errmsg(db));
        ret = 1;
    }

out:
    sqlite3_finalize(stmt);

    return ret;
}

static int fh_delete(sqlite3 *db, void *src_fh, size_t src_len) {
    int ret = 0;

    const char *sql = "DELETE FROM filehandles WHERE src_fh = ?;";

    sqlite3_stmt *stmt = NULL;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "fh_delete(): sqlite3_prepare_v2() failed: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    if (sqlite3_bind_blob(stmt, 1, src_fh, src_len, SQLITE_STATIC) != SQLITE_OK) {
        fprintf(stderr, "fh_delete(): bind source FH failed: %s\n", sqlite3_errmsg(db));
        ret = 1;
        goto out;
    }

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        fprintf(stderr, "fh_delete(): statement failed: %s\n", sqlite3_errmsg(db));
        ret = 1;
    }

out:
    sqlite3_finalize(stmt);

    return ret;
}

/*
 * Returns either an allocated file handle or NULL if an error occurred.
 */
struct file_handle *fh_lookup(sqlite3 *db, void *src_fh, size_t src_fh_len) {
    struct file_handle *dst_fh = NULL;

    const char *sql = "SELECT dst_fh FROM filehandles WHERE src_fh = ?";
    sqlite3_stmt *stmt = NULL;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_prepare_v2() failed: %s\n", sqlite3_errmsg(db));
        return NULL;
    }

    if (sqlite3_bind_blob(stmt, 1, src_fh, src_fh_len, SQLITE_STATIC) != SQLITE_OK) {
        fprintf(stderr, "bind src FH failed: %s\n", sqlite3_errmsg(db));
        goto out;
    }

    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        const void *data = sqlite3_column_blob(stmt, 0);
        int n = sqlite3_column_bytes(stmt, 0);

        if (n < 0 || n > sizeof(struct file_handle) + MAX_HANDLE_SZ) {
            fprintf(stderr, "invalid size of dest filehandle: %d\n", n);
            goto out;
        }

        dst_fh = malloc(n);
        if (!dst_fh) {
            fprintf(stderr, "out of memory");
            goto out;
        }

        memcpy(dst_fh, data, n);

        if (dst_fh->handle_bytes > MAX_HANDLE_SZ) {
            fprintf(stderr, "invalid file handle returned: size too large: %u\n", dst_fh->handle_bytes);
            free(dst_fh);
            dst_fh = NULL;
            goto out;
        }
    } else {
        fprintf(stderr, "expected to get a row.\n");
        goto out;
    }

out:
    sqlite3_finalize(stmt);

    return dst_fh;
}

static struct file_handle *get_fh(int dirfd, const char *path) {
    int mount_id;

    struct file_handle *fh = alloc_fh();
    if (!fh)
        return NULL;

    int rc = name_to_handle_at(dirfd, path, fh, &mount_id, 0);
    if (rc) {
        fprintf(stderr, "name_to_handle_at() failed: %s\n", strerror(errno));
        free(fh);
        return NULL;
    }

    return fh;
}

int fh_db_insert_by_path(sqlite3 *db, char *src, char *dst) {
    int ret = 0;
    struct file_handle *src_fh = get_fh(AT_FDCWD, src);
    struct file_handle *dst_fh = get_fh(AT_FDCWD, dst);

    if (!src_fh || !dst_fh) {
        ret = 1;
        goto out;
    }

    if (fh_insert(db, file_handle_data(src_fh), file_handle_size(src_fh), dst_fh)) {
        ret = 1;
    }

out:
    free(src_fh);
    free(dst_fh);

    return ret;
}

/*
 * Perform a rename in the index given the following information about a rename
 * that occurred in the source FS:
 *   - filehandles of old parent and new parent directory
 *   - old name and new name
 *
 *  gufi_fd must be a file descriptor to a file in the GUFI index tree, most likely
 *  the index root.
 */
int do_rename(struct fh_incremental_state *s, void *old_parent, size_t old_parent_len,
              void *new_parent, size_t new_parent_len,
              const char *oldpath, const char *newpath) {
    int ret = 0;

    struct file_handle *index_old = fh_lookup(s->fh_db, old_parent, old_parent_len);
    if (!index_old) {
        fprintf(stderr, "Could not look up old parent's handle in filehandle database.\n");
        ret = 1;
        goto out_free;
    }
    struct file_handle *index_new = fh_lookup(s->fh_db, new_parent, new_parent_len);
    if (!index_new) {
        fprintf(stderr, "Could not look up new parent's handle in filehandle database.\n");
        ret = 1;
        goto out_free;
    }

    int old_fd = open_by_handle_at(s->fh_mount_fd, index_old, O_PATH|O_DIRECTORY);
    if (old_fd < 0) {
        fprintf(stderr, "Could not open old parent file in index tree: %s\n", strerror(errno));
        ret = 1;
        goto out_free;
    }

    int new_fd = open_by_handle_at(s->fh_mount_fd, index_new, O_PATH|O_DIRECTORY);
    if (new_fd < 0) {
        fprintf(stderr, "Could not open new parent file in index tree: %s\n", strerror(errno));
        ret = 1;
        goto out_close;
    }

    if (renameat(old_fd, oldpath, new_fd, newpath)) {
        fprintf(stderr, "Could not perform rename in index tree: %s\n", strerror(errno));
        ret = 1;
    }

    close(new_fd);
out_close:
    close(old_fd);

out_free:
    free(index_old);
    free(index_new);

    return ret;
}

int do_mkdir(struct fh_incremental_state *s, void *parent, size_t parent_len,
             void *child, size_t child_len, const char *name) {
    int ret = 0;

    struct file_handle *index_parent = fh_lookup(s->fh_db, parent, parent_len);
    if (!index_parent) {
        fprintf(stderr, "Could not look up parent directory's handle in filehandle database.\n");
        return 1;
    }

    int fd = open_by_handle_at(s->fh_mount_fd, index_parent, O_PATH|O_DIRECTORY);
    if (fd < 0) {
        fprintf(stderr, "Could not open parent directory in index tree: %s\n", strerror(errno));
        ret = 1;
        goto out_free;
    }

    // TODO: need to fix up mode, ownership of new directory...
    if (mkdirat(fd, name, 0644)) {
        fprintf(stderr, "Could not perform mkdir in index tree: %s\n", strerror(errno));
        ret = 1;
        goto out;
    }

    // Update file handle index
    struct file_handle *new_fh = get_fh(fd, name);
    if (!new_fh) {
        ret = 1;
        goto out;
    }

    int rc = fh_insert(s->fh_db, child, child_len, new_fh);
    if (rc) {
        ret = 1;
    }

    free(new_fh);

out:
    close(fd);

out_free:
    free(index_parent);

    return ret;
}

int do_rmdir(struct fh_incremental_state *s, void *parent, size_t parent_len,
             void *child, size_t child_len, const char *name) {
    int ret = 0;

    struct file_handle *index_parent = fh_lookup(s->fh_db, parent, parent_len);
    if (!index_parent) {
        fprintf(stderr, "Could not look up parent directory's handle in filehandle database.\n");
        return 1;
    }

    int fd = open_by_handle_at(s->fh_mount_fd, index_parent, O_PATH|O_DIRECTORY);
    if (fd < 0) {
        fprintf(stderr, "Could not open parent directory in index tree: %s\n", strerror(errno));
        ret = 1;
        goto out_free;
    }

    if (unlinkat(fd, name, AT_REMOVEDIR)) {
        fprintf(stderr, "Could not perform rmdir in index tree: %s\n", strerror(errno));
        ret = 1;
        goto out;
    }

    ret = fh_delete(s->fh_db, child, child_len);

out:
    close(fd);

out_free:
    free(index_parent);

    return ret;
}
