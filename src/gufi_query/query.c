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

#include "gufi_query/query.h"
#include "gufi_query/query_formatting.h"
#include "print.h"
#include "utils.h"

static int replace_sql(const refstr_t *sql, const sll_t *fmts, const refstr_t *source_prefix,
                       struct work *work,
                       char **used) {
    *used = (char *) sql->data;
    if (sll_get_size(fmts) == 0) {
        return 0;
    }

    size_t len = 0; /* discarded */
    if (replace_formatting(sql, fmts, source_prefix, work, used, &len) != 0) {
        fprintf(stderr, "Warning: Could not replace string formatting in '%s'\n",
                sql->data);
        return -1;
    }

    return 0;
}

/* wrapper wround sqlite3_exec to pass arguments and check for errors */
void querydb(struct work *work,
             const char *dbname, const size_t dbname_len, sqlite3 *db,
             const refstr_t *query, const sll_t *fmts, const refstr_t *source_prefix, const int *types,
             PoolArgs_t *pa, int id,
             int (*callback)(void *, int, char **, char**), int *rc) {
    ThreadArgs_t *ta = &pa->ta[id];
    PrintArgs_t args = {
        .output_buffer = &ta->output_buffer,
        .delim = pa->in->delim,
        .mutex = pa->stdout_mutex,
        .outfile = ta->outfile,
        .rows = 0,
        .types = types,
    };

    /* replace original SQL string if there is formatting */
    char *sql = NULL;
    if (replace_sql(query, fmts, source_prefix, work, &sql) != 0) {
        return;
    }

    char *err = NULL;
    if (sqlite3_exec(db, sql, callback, &args, &err) != SQLITE_OK) {
        char buf[MAXPATH];
        present_user_path(dbname, dbname_len,
                          &work->root_parent, work->root_basename_len, &work->orig_root,
                          buf, sizeof(buf));
        sqlite_print_err_and_free(err, stderr, "Error: %s: %s: \"%s\"\n", err, buf, sql);
    }

    if (sql != query->data) {
        free(sql);
    }

    *rc = args.rows;
}
