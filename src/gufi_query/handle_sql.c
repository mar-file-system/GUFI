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
#include <string.h>

#include "dbutils.h"
#include "extensions.h"
#include "internal_copy.h"
#include "template_db.h"

#include "gufi_query/PoolArgs.h"
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
    if ((in->output == OUTDB) || str_exists(&in->sql.init_agg)) {
        /* -I (required) */
        if (!str_exists(&in->sql.init)) {
            fprintf(stderr, "Error: Missing -I\n");
            return -1;
        }
    }

    /* not aggregating */
    if (!str_exists(&in->sql.init_agg)) {
        if (str_exists(&in->sql.intermediate)) {
            fprintf(stderr, "Warning: Got -J even though not aggregating. Ignoring\n");
        }

        if (str_exists(&in->sql.agg)) {
            fprintf(stderr, "Warning: Got -G even though not aggregating. Ignoring\n");
        }
    }
    /* aggregating */
    else {
        /* need -J to write to aggregate db */
        if (!str_exists(&in->sql.intermediate)) {
            fprintf(stderr, "Error: Missing -J\n");
            return -1;
        }

        if ((in->output == STDOUT) || (in->output == OUTFILE)) {
            /* need -G to write out results */
            if (!str_exists(&in->sql.agg)) {
                fprintf(stderr, "Error: Missing -G\n");
                return -1;
            }
        }
        /* -G can be called when aggregating, but is not necessary */
    }

    /* -Q/--external-attach requires -I */
    if (sll_get_size(&in->external_attach.setup)) {
        if (!str_exists(&in->sql.init)) {
            fprintf(stderr, "Attaching external databases require template files attached with -I\n");
            return -1;
        }
    }

    /* --external-copy requires -I */
    if (sll_get_size(&in->external_copy.setup)) {
        if (!str_exists(&in->sql.init)) {
            fprintf(stderr, "Copying external databases require tables created with -I\n");
            return -1;
        }
    }

    /* --internal-copy requires -I */
    if (sll_get_size(&in->internal_copy.setup)) {
        if (!str_exists(&in->sql.init)) {
            fprintf(stderr, "Copying internal databases require tables created with -I\n");
            return -1;
        }
    }

    return 0;
}

static int set_print_tlv_types(struct input *in) {
    /*
     * The print-TLV metadata is a sequence of column descriptors:
     *
     *     <4 hex digit column count>:<type digit>:<4 hex digit length>:<column name>...
     *
     * The column names are consumed by the caller that constructs the
     * virtual-table schema. gufi_query only needs the type digits here.
     */

    const char *ptr = in->types.print_tlv.data;
    const char *end = ptr + in->types.print_tlv.len;
    unsigned int count = 0;
    char count_text[5] = {0};
    if ((end - ptr) < 5 || ptr[4] != ':') {
        return 1;
    }
    memcpy(count_text, ptr, 4);
    int count_read = 0;
    if ((sscanf(count_text, "%4x%n", &count, &count_read) != 1) ||
        (count_read != 4)) {
        return 1;
    }
    ptr += 5;

    /*
     * T/S/E can return different column counts. gufi_vt maps all of them
     * positionally into the final schema, so leave unspecified positions as
     * SQLITE_NULL while providing enough entries for every SQLite result.
     */
    static const size_t TYPE_CAPACITY = 0x10000;
    int *parsed_types = calloc(TYPE_CAPACITY, sizeof(*parsed_types));
    if (!parsed_types) {
        return 1;
    }
    for (size_t i = 0; i < TYPE_CAPACITY; i++) {
        parsed_types[i] = SQLITE_NULL;
    }

    for (unsigned int column = 0; column < count; column++) {
        /* Read one descriptor: a 7-byte header (type, separators, four
         * hexadecimal length digits), then its column name. */
        if ((end - ptr) < 7 || (ptr[0] < '1') || (ptr[0] > '5') || (ptr[1] != ':')) {
            free(parsed_types);
            return 1;
        }

        /* Four hexadecimal digits plus a terminating NUL hold the name length. */
        char length[5] = {0};
        memcpy(length, ptr + 2, 4);
        if (ptr[6] != ':') {
            free(parsed_types);
            return 1;
        }

        size_t name_len = 0;
        int length_read = 0;
        if ((sscanf(length, "%4zx%n", &name_len, &length_read) != 1) ||
            (length_read != 4) ||
            (name_len > (size_t) (end - ptr - 7))) {
            free(parsed_types);
            return 1;
        }

        parsed_types[column] = ptr[0] - '0';
        ptr += 7 + name_len; /* skip the header and fixed-length column name */
    }

    if (ptr != end) {
        free(parsed_types);
        return 1;
    }

    /* Aggregation has one final output query. */
    if (str_exists(&in->sql.init_agg)) {
        in->types.agg = parsed_types;
        return 0;
    }

    /* Non-aggregated T/S/E rows all map to the final virtual-table schema. */
    int **outputs[] = {
        &in->types.tsum,
        &in->types.sum,
        &in->types.ent,
    };
    const str_t *sql[] = {
        &in->sql.tsum,
        &in->sql.sum,
        &in->sql.ent,
    };

    int installed = 0;
    for (size_t i = 0; i < (sizeof(outputs) / sizeof(outputs[0])); i++) {
        if (!str_exists(sql[i])) {
            continue;
        }

        if (!installed) {
            *outputs[i] = parsed_types;
            installed = 1;
            continue;
        }

        *outputs[i] = malloc(TYPE_CAPACITY * sizeof(**outputs));
        if (!*outputs[i]) {
            for (size_t j = 0; j < i; j++) {
                free(*outputs[j]);
                *outputs[j] = NULL;
            }
            return 1;
        }
        memcpy(*outputs[i], parsed_types,
               TYPE_CAPACITY * sizeof(*parsed_types));
    }

    if (!installed) {
        free(parsed_types);
    }

    return 0;
}

static int gen_types(struct input *in) {
    if (!str_exists(&in->types.print_tlv) ||
        ((in->output != STDOUT) && (in->output != OUTFILE))) {
        return 0;
    }

    return set_print_tlv_types(in)?-1:0;
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
