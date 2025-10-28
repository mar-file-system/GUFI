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



#include "gufi_incremental_update/incremental_update.h"

const char SNAPSHOT_CREATE[] =
    DROP_TABLE(SNAPSHOT)
    SNAPSHOT_SCHEMA(SNAPSHOT);

const char SNAPSHOT_INSERT[] =
    "INSERT INTO " SNAPSHOT " VALUES (@path, @type, @inode, @pinode, @depth, @suspect);";

int create_snapshot_table(const char *name, sqlite3 *db, void *args) {
    (void) args;

    return (create_table_wrapper(name, db, SNAPSHOT, SNAPSHOT_CREATE) != SQLITE_OK);
}

int insert_snapshot_row(struct work *work, struct entry_data *ed,
                        sqlite3_stmt *res, const size_t offset_name) {
    char *zname = sqlite3_mprintf("%q", work->name + offset_name); /* remove parents of starting paths */
    char *ztype = sqlite3_mprintf("%c", ed->type);
    sqlite3_bind_text (res, 1, zname, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text (res, 2, ztype, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(res, 3, work->statuso.st_ino);
    sqlite3_bind_int64(res, 4, work->pinode);
    sqlite3_bind_int64(res, 5, work->level);
    sqlite3_bind_int64(res, 6, ed->suspect);

    int error = sqlite3_step(res);
    if (error != SQLITE_DONE) {
        fprintf(stderr,  "SQL insertdbgor step: %s error %d err %s\n",
                work->name, error, sqlite3_errstr(error));
    }

    error = sqlite3_reset(res);
    if (error != SQLITE_OK) {
        fprintf(stderr,  "SQL insertdbgor reset: %s error %d err %s\n",
                work->name, error, sqlite3_errstr(error));
    }

    sqlite3_free(ztype);
    sqlite3_free(zname);
    sqlite3_reset(res);
    sqlite3_clear_bindings(res);

    return !(error == SQLITE_OK);
}
