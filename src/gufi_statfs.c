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



#ifndef _GNU_SOURCE
#define _GNU_SOURCE /* statvfs(3) f_flag flags */
#endif

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#if defined(__APPLE__)
#include <sys/param.h>
#include <sys/mount.h>
#elif defined(__linux__)
#include <sys/vfs.h>
#endif
#include <sys/statvfs.h>
#include <sys/types.h>

#include "dbutils.h"

#define FSINFO "fsinfo"

static const char FSINFO_CREATE[] =
    "CREATE TABLE " FSINFO "(system TEXT, version TEXT, path TEXT, type INT64, bsize INT64, frsize IN64, blocks INT64, files INT64, fsid INT64, flags INT64, namemax INT64, notes TEXT);";

static const char FSINFO_INSERT[] =
    "INSERT INTO " FSINFO " VALUES (@system, @version, @path, @type, @bsize, @frsize, @blocks, @files, @fsid, @flags, @namemax, @notes);";

static int statfs_and_insert(sqlite3 *db,
                             const char *system, const char *version,
                             const char *path, const char *notes) {
    /*
     * Use information from statvfs(3) instead of information from
     * statfs(2)/statfs64(2) in order to not handle different
     * implementations
     */

    /* get filesystem type */
    struct statfs fs;
    if (statfs(path, &fs)) {
        const int err = errno;
        fprintf(stderr, "Error: Could not statfs '%s': %s (%d)\n", path, strerror(err), err);
        return 1;
    }

    /* get everything else */
    struct statvfs vfs;
    if (statvfs(path, &vfs)) {
        const int err = errno;
        fprintf(stderr, "Error: Could not statvfs '%s': %s (%d)\n", path, strerror(err), err);
        return 1;
    }

    /* insert into database */
    sqlite3_stmt *res = insertdbprep(db, FSINFO_INSERT);
    if (!res) {
        return 1;
    }

    char *sql_system = sqlite3_mprintf("%q", system);
    char *sql_version = NULL;
    if (version && strlen(version)) {
        sql_version = sqlite3_mprintf("%q", version);
    }
    char *sql_path = sqlite3_mprintf("%q", path);
    char *sql_notes = NULL;
    if (notes && strlen(notes)) {
        sql_notes = sqlite3_mprintf("%q", notes);
    }

    sqlite3_bind_text(res, 1, sql_system, -1, SQLITE_STATIC);
    if (sql_version) {
        sqlite3_bind_text(res, 2, sql_version, -1, SQLITE_STATIC);
    }
    sqlite3_bind_text (res, 3,  sql_path, -1, SQLITE_STATIC);
    sqlite3_bind_int64(res, 4,  fs.f_type);
    sqlite3_bind_int64(res, 5,  vfs.f_bsize);
    sqlite3_bind_int64(res, 6,  vfs.f_frsize);
    sqlite3_bind_int64(res, 7,  vfs.f_blocks);
    sqlite3_bind_int64(res, 8,  vfs.f_files);
    sqlite3_bind_int64(res, 9,  vfs.f_fsid);
    sqlite3_bind_int64(res, 10, vfs.f_flag);
    sqlite3_bind_int64(res, 11, vfs.f_namemax);
    if (sql_notes) {
        sqlite3_bind_text(res, 12, sql_notes, -1, SQLITE_STATIC);
    }

    sqlite3_step(res);
    sqlite3_reset(res);

    /* cleanup */
    if (sql_notes) {
        sqlite3_free(sql_notes);
    }
    sqlite3_free(sql_path);
    if (sql_version) {
        sqlite3_free(sql_version);
    }
    sqlite3_free(sql_system);

    insertdbfin(res);

    /* should probably move to debug/verbose mode */
    printf("System:              %s\n",    system);
    printf("Version:             %s\n",    version);
    printf("Path:                %s\n",    path);
    printf("Type:                0x%lx\n", fs.f_type);
    printf("Block Size:          %lu\n",   vfs.f_bsize);
    printf("Fragment Size:       %lu\n",   vfs.f_frsize);
    printf("Block Count:         %lu\n",   vfs.f_blocks);
    printf("Inodes:              %lu\n",   vfs.f_files);
    printf("ID:                  %lu\n",   vfs.f_fsid);
    printf("Flags:               %lu\n",   vfs.f_flag);
    printf("Max filename length: %lu\n",   vfs.f_namemax);
    printf("Extra Notes:         %s\n",    notes);

    return 0;
}

static void help(const char *argv0) {
    printf("usage: %s fsdb system version src [extra notes]\n", argv0);
    printf("\n");
    printf("fsdb           database to place filesystem data into\n");
    printf("system         name of source system\n");
    printf("version        version string (counter, timestamp, etc.)\n");
    printf("path           source path to statfs\n");
    printf("extra notes    extra (human readable) text to place into database entry\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    if (argc < 5) {
        help(argv[0]);
        return 1;
    }

    const char *fsdbname = argv[1];
    const char *system = argv[2];
    const char *version = argv[3];
    const char *path = argv[4];
    const char *notes = argv[5];

    sqlite3 *db = NULL;

    /* try to open existing database file */
    if (sqlite3_open_v2(fsdbname, &db, SQLITE_OPEN_READWRITE, GUFI_SQLITE_VFS) != SQLITE_OK) {
        closedb(db);
        db = NULL;

        /* try to create a new database file */
        if (sqlite3_open_v2(fsdbname, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, GUFI_SQLITE_VFS) != SQLITE_OK) {
            fprintf(stderr, "Error: Could not create database file '%s': %s (%d)\n",
                    fsdbname, sqlite3_errmsg(db), sqlite3_errcode(db));
            closedb(db);
            db = NULL;
            return 1;
        }

        /* new database file - need to create table */
        char *err = NULL;
        if (sqlite3_exec(db, FSINFO_CREATE, NULL, NULL, &err) != SQLITE_OK) {
            fprintf(stderr, "Error: Could not create " FSINFO " table: %s\n", err);
            sqlite3_free(err);
            return 1;
        }
    }

    char *fullpath = realpath(path, NULL);
    statfs_and_insert(db, system, version, fullpath, notes);
    free(fullpath);

    closedb(db);

    return 0;
}
