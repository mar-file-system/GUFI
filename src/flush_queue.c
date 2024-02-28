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



#include <stdlib.h>

#include "bf.h"
#include "dbutils.h"
#include "flush_queue.h"

const size_t UNFLUSHED_LIMIT = 100; /* magic number */

unflushed_db_t *unflushed_db_init(const char *topath, const size_t topath_len,
                                  uid_t uid, gid_t gid, sqlite3 *db) {
    unflushed_db_t *udb = malloc(sizeof(*udb));
    udb->path = malloc(topath_len + 1);
    udb->path_len = SNFORMAT_S(udb->path, topath_len + 1, 1,
                               topath, topath_len);
    udb->uid = uid;
    udb->gid = gid;
    udb->db.buf = sqlite3_serialize(db, "main", &udb->db.size, 0);
    return udb;
}

sll_t *flush_queue_init(const size_t threads) {
    sll_t *udbs = malloc(threads * sizeof(*udbs));
    for(size_t i = 0; i < threads; i++) {
        sll_init(&udbs[i]);
    }
    return udbs;
}

/* sll destructor function */
void flush_db(void *ptr) {
    if (!ptr) {
        return;
    }

    unflushed_db_t *udb = (unflushed_db_t *) ptr;
    char dbname[MAXPATH];
    SNFORMAT_S(dbname, sizeof(dbname), 3,
               udb->path, udb->path_len,
               "/", (size_t) 1,
               DBNAME, DBNAME_LEN);

    write_db_file(udb->db.buf, udb->db.size, dbname, udb->uid, udb->gid);
    sqlite3_free(udb->db.buf);
    free(udb->path);
    free(udb);
}

int enqueue_and_flush(sll_t *udbs, const size_t id, unflushed_db_t *udb) {
    if (udb) {
        sll_push(&udbs[id], udb);
    }

    if (sll_get_size(&udbs[id]) >= UNFLUSHED_LIMIT) {
        sll_destroy(&udbs[id], flush_db);
        /* no need to init */
    }

    return 0;
}

void flush_queue_destroy(sll_t *udbs, const size_t threads, void (*cleanup)(void *)) {
    for(size_t i = 0; i < threads; i++) {
        sll_destroy(&udbs[i], cleanup);
    }

    free(udbs);
}
