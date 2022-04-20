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
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "dbutils.h"
#include "template_db.h"

#ifdef __APPLE__

#include <copyfile.h>

static ssize_t gufi_copyfd(int src_fd, int dst_fd, size_t size) {
    (void) size;
    lseek(src_fd, 0, SEEK_SET);
    return fcopyfile(src_fd, dst_fd, 0, COPYFILE_DATA);
}

#else

#include <sys/sendfile.h>

static ssize_t gufi_copyfd(int src_fd, int dst_fd, size_t size) {
    off_t offset = 0;
    return sendfile(dst_fd, src_fd, &offset, size);
}
#endif

extern int errno;

static int create_tables(const char *name, sqlite3 *db, void *args) {
    if ((create_table_wrapper(name, db, "esql",        esql,        NULL, NULL) != SQLITE_OK) ||
        (create_table_wrapper(name, db, "ssql",        ssql,        NULL, NULL) != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vssqldir",    vssqldir,    NULL, NULL) != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vssqluser",   vssqluser,   NULL, NULL) != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vssqlgroup",  vssqlgroup,  NULL, NULL) != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vesql",       vesql,       NULL, NULL) != SQLITE_OK)) {
        return -1;
    }

    return 0;
}

// create the initial database file to copy from
off_t create_template(int *fd) {
    static const char name[] = "tmp.db";

    sqlite3 *db = opendb(name, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 0, 0
                          , create_tables, NULL
                          #if defined(DEBUG) && defined(PER_THREAD_STATS)
                          , NULL, NULL
                          , NULL, NULL
                          #endif
                          );

    sqlite3_close(db);

    if ((*fd = open(name, O_RDONLY)) == -1) {
        fprintf(stderr, "Could not open template file\n");
        return -1;
    }

    // no need for the file to remain on the filesystem
    remove(name);

    return lseek(*fd, 0, SEEK_END);
}

// copy the template file instead of creating a new database and new tables for each work item
// the ownership and permissions are set too
int copy_template(const int src_fd, const char *dst, off_t size, uid_t uid, gid_t gid) {
    // ignore errors here
    const int src_db = dup(src_fd);
    const int dst_db = open(dst, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    const ssize_t sf = gufi_copyfd(src_db, dst_db, size);
    fchown(dst_db, uid, gid);
    close(src_db);
    close(dst_db);

    if (sf == -1) {
        fprintf(stderr, "Could not copy template file to %s: %s (%d)\n", dst, strerror(errno), errno);
        return -1;
    }

    return 0;
}
