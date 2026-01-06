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



#include "dbutils.h"
#include "print.h"

#include "gufi_incremental_update/aggregate.h"
#include "gufi_incremental_update/incremental_update.h"

/* must have shared cache */
static const char INTERMEDIATE_ATTACH_FORMAT[] = "file:memory%zu?mode=memory&cache=shared" GUFI_SQLITE_VFS_URI;
#define INTERMEDIATE_ATTACH_NAME "intermediate"

int aggregate_init(Aggregate_t *aggregate, const size_t threads, const char *name, const size_t offset) {
    /* Not checking arguments */

    /* create per-thread in-memory dbs */
    aggregate->dbs = calloc(threads, sizeof(aggregate->dbs[0]));

    char dbname[MAXPATH];
    for(size_t i = 0; i < threads; i++) {
        SNPRINTF(dbname, MAXPATH, INTERMEDIATE_ATTACH_FORMAT, i + offset);

        aggregate->dbs[i] = opendb(dbname, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE,
                                   1, 1, create_snapshot_table, NULL);
        if (!aggregate->dbs[i]) {
            fprintf(stderr, "Could not open aggregation database \"%s\"\n", dbname);
            aggregate_fin(aggregate, i);
            return 1;
        }

        addqueryfuncs(aggregate->dbs[i]);
    }

    /* always open an aggregate db */
    aggregate->agg = opendb(name, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE,
                            1, 1, create_snapshot_table, NULL);
    if (!aggregate->agg) {
        fprintf(stderr, "Could not open final aggregation database \"%s\"\n", dbname);
        aggregate_fin(aggregate, threads);
        return 1;
    }

    /* don't need addqueryfuncs */

    return 0;
}

void aggregate_intermediate(Aggregate_t *aggregate, const size_t threads, const size_t offset) {
    /* Not checking arguments */

    /*
     * attach intermediate databases to aggregate database
     * not using attachdb because attachdb modifies the input path, which is not needed here
     *
     * failure to attach is not an error - the data is simply not accessible
     */
    char dbname[MAXPATH];
    for(size_t i = 0; i < threads; i++) {
        SNPRINTF(dbname, MAXPATH, INTERMEDIATE_ATTACH_FORMAT, i + offset);

        if (attachdb_raw(dbname, aggregate->agg, INTERMEDIATE_ATTACH_NAME, 1, 1)) {
            char *err = NULL;
            if ((sqlite3_exec(aggregate->agg, "INSERT INTO " SNAPSHOT " SELECT * FROM " INTERMEDIATE_ATTACH_NAME "." SNAPSHOT, NULL, NULL, &err) != SQLITE_OK)) {
                sqlite_print_err_and_free(err, stderr, "Error: Cannot aggregate intermediate databases: %s\n", err);
            }
        }

        detachdb(dbname, aggregate->agg, INTERMEDIATE_ATTACH_NAME, 1, 1);
    }
}

void aggregate_fin(Aggregate_t *aggregate, const size_t threads) {
    /* Not checking arguments */

    if (aggregate->agg) {
        closedb(aggregate->agg);
    }

    if (aggregate->dbs) {
        for(size_t i = 0; i < threads; i++) {
            closedb(aggregate->dbs[i]);
        }

        free(aggregate->dbs);
        aggregate->dbs = NULL;
    }
}
