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



#include <stdio.h>
#include <stdlib.h>

#include "dbutils.h"
#include "template_db.h"

#include "gufi_query/handle_sql.h"
#include "gufi_query/query_replacement.h"

static int validate(struct input *in) {
    /*
     * - Leaves are final outputs
     * - OUTFILE/OUTDB + aggregation will create per thread and final aggregation files
     *
     *                         init                       | if writing to outdb or aggregating
     *                          |                         | -I CREATE [TEMP] TABLE <name>
     *                          |
     *                          |                         | if aggregating, create an aggregate table
     *                          |                         | -K CREATE [TEMP] TABLE <aggregate name>
     *                          |
     *   -----------------------------------------------  | start walk
     *                          |
     *                       thread
     *                          |
     *          -------------------------------
     *          |               |             |
     *          |               |       stdout/outfile.*  | -T/-S/-E SELECT FROM <index table>
     *          |               |
     *   intermediate db      outdb.*                     | -T/-S/-E INSERT into <name> SELECT FROM <index table>
     *          |
     *          |
     *   -----------------------------------------------  | after walking index
     *          |
     *          |                                         | move intermediate results into aggregate table
     *    aggregate db - outdb                            | -J INSERT INTO <aggregate name>
     *    |          |
     * outfile     stdout                                 | Get final results
     *                                                    | -G SELECT * FROM <aggregate name>
     */
     if ((in->output == OUTDB) || in->sql.init_agg.len) {
        /* -I (required) */
        if (!in->sql.init.len) {
            fprintf(stderr, "Error: Missing -I\n");
            return -1;
        }
    }

    /* not aggregating */
    if (!in->sql.init_agg.len) {
        if (in->sql.intermediate.len) {
            fprintf(stderr, "Warning: Got -J even though not aggregating. Ignoring\n");
        }

        if (in->sql.agg.len) {
            fprintf(stderr, "Warning: Got -G even though not aggregating. Ignoring\n");
        }
    }
    /* aggregating */
    else {
        /* need -J to write to aggregate db */
        if (!in->sql.intermediate.len) {
            fprintf(stderr, "Error: Missing -J\n");
            return -1;
        }

        if ((in->output == STDOUT) || (in->output == OUTFILE)) {
            /* need -G to write out results */
            if (!in->sql.agg.len) {
                fprintf(stderr, "Error: Missing -G\n");
                return -1;
            }
        }
        /* -G can be called when aggregating, but is not necessary */
    }

    /* -Q requires -I */
    if (sll_get_size(&in->external_attach)) {
        if (!in->sql.init.len) {
            fprintf(stderr, "External databases require template files attached with -I\n");
            return -1;
        }
    }

    return 0;
}

static int gen_types(struct input *in) {
    sqlite3 *db = NULL;

    /* generate types if necessary */
    if ((in->types.prefix == 1) && ((in->output == STDOUT) || (in->output == OUTFILE))) {
        /* have to create temporary db since there is no guarantee of a db yet */
        db = opendb(SQLITE_MEMORY, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                    0, 0, create_dbdb_tables, NULL);
        if (!db) {
            return -1;
        }

        int cols = 0; /* discarded */

        struct work work;
        aqfctx_t ctx = {
            .in = in,
            .work = &work,
        };
        if (addqueryfuncs_with_context(db, &ctx) != 0) {
            goto error;
        }

        if (in->sql.tsum.len) {
            if (create_table_wrapper(SQLITE_MEMORY, db, TREESUMMARY, TREESUMMARY_CREATE) != SQLITE_OK) {
                goto error;
            }
        }

        if (in->process_xattrs) {
            size_t count = 0; /* unused */
            setup_xattrs_views(in, db, &work, &count);
        }

        /* if not aggregating, get types for T, S, and E */
        if (!in->sql.init_agg.len) {
            if (in->sql.tsum.len) {
                if (get_col_types(db, &in->sql.tsum, &in->types.tsum, &cols) != 0) {
                    goto error;
                }
            }
            if (in->sql.sum.len) {
                if (get_col_types(db, &in->sql.sum,  &in->types.sum,  &cols) != 0) {
                    goto error;
                }
            }
            if (in->sql.ent.len) {
                if (get_col_types(db, &in->sql.ent,  &in->types.ent,  &cols) != 0) {
                    goto error;
                }
            }
        }
        /* types for G */
        else {
            /* run -K so -G can pull the final columns */
            char *err = NULL;
            if (sqlite3_exec(db, in->sql.init_agg.data, NULL, NULL, &err) != SQLITE_OK) {
                fprintf(stderr, "Error: -K SQL failed while getting columns types: %s\n", err);
                sqlite3_free(err);
                goto error;
            }

            if (get_col_types(db, &in->sql.agg, &in->types.agg, &cols) != 0) {
                goto error;
            }
        }

        closedb(db);
    }

    return 0;

  error:
    closedb(db);
    return -1;
}

static int sql_formatting(struct input *in) {
    save_replacements(&in->sql.tsum, &in->sql_format.tsum);
    save_replacements(&in->sql.sum,  &in->sql_format.sum);
    save_replacements(&in->sql.ent,  &in->sql_format.ent);
    return 0;
}

int handle_sql(struct input *in) {
    if (validate(in) != 0) {
        return -1;
    }

    if (gen_types(in) != 0) {
        return -1;
    }

    if (sql_formatting(in) != 0) {
        return -1;
    }

    return 0;
}
