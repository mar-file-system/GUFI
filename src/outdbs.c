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



#include "bf.h"
#include "dbutils.h"
#include "outdbs.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* the array of db handles should already be allocated */
sqlite3 **outdbs_init(const int opendbs, char *prefix, const size_t count, const char *sqlinit, const size_t sqlinit_len) {
    sqlite3 **dbs = calloc(count, sizeof(sqlite3 *));
    /* if (!dbs) { */
    /*     return NULL; */
    /* } */

    if (!opendbs) {
        return dbs;
    }

    /* output results to database files */
    for(size_t i = 0; i < count; i++) {
        char buf[MAXPATH];
        SNPRINTF(buf, MAXPATH, "%s.%d", prefix, i);
        if (!(dbs[i] = opendb(buf, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 1, 1
                              , NULL, NULL
                              #if defined(DEBUG) && defined(PER_THREAD_STATS)
                              , NULL, NULL
                              , NULL, NULL
                              #endif
                              ))) {
            outdbs_fin(dbs, i, NULL, 0);
            fprintf(stderr, "Could not open output database file %s\n", buf);
            return NULL;
        }
    }

    if (sqlinit && sqlinit_len) {
        for(size_t i = 0; i < count; i++) {
            char *err = NULL;
            if (sqlite3_exec(dbs[i], sqlinit, NULL, NULL, &err) != SQLITE_OK) {
                outdbs_fin(dbs, i, NULL, 0);
                fprintf(stderr, "Initial SQL Error: %s\n", err);
                sqlite3_free(err);
                return NULL;
            }
            sqlite3_free(err);
        }
    }

    return dbs;
}

/* close all output dbs */
int outdbs_fin(sqlite3 **dbs, const size_t end, const char *sqlfin, const size_t sqlfin_len) {
    /* if (!dbs) { */
    /*    return 0; */
    /* } */

    if (sqlfin && sqlfin_len) {
        for(size_t i = 0; i < end; i++) {
            char *err = NULL;
            if (sqlite3_exec(dbs[i], sqlfin, NULL, NULL, &err) != SQLITE_OK) {
                fprintf(stderr, "Final SQL Error: %s\n", err);
                /* ignore errors, since the database is closing anyways */
            }
            sqlite3_free(err);
        }
    }

    for(size_t i = 0; i < end; i++) {
        closedb(dbs[i]);
    }

    return 0;
}
