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
#include "gufi_query/aggregate.h"
#include "print.h"

/* must have shared cache */
static const char AGGREGATE_FILE_NAME[]      = "file:aggregatedb?mode=memory&cache=shared" GUFI_SQLITE_VFS_URI;
/* users should never use this name */
static const char INTERMEDIATE_ATTACH_NAME[] = "gufi_query_intermediate";

static sqlite3 *aggregate_setup(char *dbname, const char *init_agg) {
    sqlite3 *db = opendb(dbname, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, 1, 1, NULL, NULL
#if defined(DEBUG) && defined(PER_THREAD_STATS)
                                , NULL, NULL
                                , NULL, NULL
#endif
        );
    if (!db) {
        fprintf(stderr, "Could not open aggregation database\n");
        closedb(db);
        return NULL;
    }

    addqueryfuncs_common(db);

    /* create table */
    char *err = NULL;
    if (sqlite3_exec(db, init_agg, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "Could not create aggregation table \"%s\" on %s: %s\n", init_agg, dbname, err);
        sqlite3_free(err);
        closedb(db);
    }

    return err?NULL:db;
}

Aggregate_t *aggregate_init(Aggregate_t *aggregate, struct input *in) {
    /* Not checking arguments */

    aggregate->outfile = stdout;

    char dbname[MAXPATH];
    if ((in->output == STDOUT) || (in->output == OUTFILE)) {
        SNPRINTF(dbname, MAXPATH, AGGREGATE_FILE_NAME);

        /* only open a final file if OUTFILE */
        if (in->output == OUTFILE) {
            aggregate->outfile = fopen(in->outname.data, "w");
            if (!aggregate->outfile) {
                fprintf(stderr, "Error: Could not open output file %s\n", in->outname.data);
                aggregate_fin(aggregate, in);
                return NULL;
            }
        }

        if (!OutputBuffer_init(&aggregate->ob, in->output_buffer_size)) {
            fprintf(stderr, "Error: Could not set up output buffers\n");
            aggregate_fin(aggregate, in);
            return NULL;
        }
    }
    else if (in->output == OUTDB) {
        SNFORMAT_S(dbname, MAXPATH, 1, in->outname.data, in->outname.len);
    }

    /* always open an aggregate db */
    if (!(aggregate->db = aggregate_setup(dbname, in->sql.init_agg.data))) {
        aggregate_fin(aggregate, in);
        return NULL;
    }

    return aggregate;
}

void aggregate_intermediate(Aggregate_t *aggregate, PoolArgs_t *pa, struct input *in) {
    /* Not checking arguments */

    /*
     * attach intermediate databases to aggregate database
     * not using attachdb because attachdb modifies the input path, which is not needed here
     *
     * failure to attach is not an error - the data is simply not accessible
     */
    for(size_t i = 0; i < in->maxthreads; i++) {
        ThreadArgs_t *ta = &(pa->ta[i]);

        char attach[MAXSQL];
        SNPRINTF(attach, sizeof(attach), "ATTACH '%s' AS '%s';", ta->dbname, INTERMEDIATE_ATTACH_NAME);

        char *err = NULL;
        if (sqlite3_exec(aggregate->db, attach, NULL, NULL, &err) == SQLITE_OK) {
            if ((sqlite3_exec(aggregate->db, in->sql.intermediate.data, NULL, NULL, &err) != SQLITE_OK)) {
                fprintf(stderr, "Error: Cannot aggregate intermediate databases with \"%s\": %s\n", in->sql.intermediate.data, err);
                sqlite3_free(err);
            }
        }
        else {
            fprintf(stderr, "Could not attach aggregate database to intermediate database: %s\n", err);
            sqlite3_free(err);
        }
        detachdb(ta->dbname, aggregate->db, INTERMEDIATE_ATTACH_NAME, 1);
    }
}

int aggregate_process(Aggregate_t *aggregate, struct input *in) {
    int rc = 0;

    /* normally expect STDOUT/OUTFILE to have SQL to run, but OUTDB can have SQL to run as well */
    if ((in->output != OUTDB) || in->sql.agg.len) {
        PrintArgs_t pa;
        pa.output_buffer = &aggregate->ob;
        pa.delim = in->delim;
        pa.mutex = NULL;
        pa.outfile = aggregate->outfile;
        pa.rows = 0;

        char *err = NULL;
        if (sqlite3_exec(aggregate->db, in->sql.agg.data, print_parallel, &pa, &err) != SQLITE_OK) {
            fprintf(stderr, "Final aggregation error: \"%s\": %s\n", in->sql.agg.data, err);
            sqlite3_free(err);
            rc = -1;
        }
    }

    OutputBuffer_flush(&aggregate->ob, aggregate->outfile);

    return rc;
}

void aggregate_fin(Aggregate_t *aggregate, struct input *in) {
    /* Not checking arguments */

    if (aggregate->db) {
        closedb(aggregate->db);
    }

    if ((in->output == STDOUT) || (in->output == OUTFILE)) {
        OutputBuffer_destroy(&aggregate->ob);
        if (aggregate->outfile && (aggregate->outfile != stdout)) {
            fclose(aggregate->outfile);
        }
    }
}
