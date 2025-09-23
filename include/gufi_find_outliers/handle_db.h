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



#ifndef GUFI_FIND_OUTLIERS_HANDLE_DB_H
#define GUFI_FIND_OUTLIERS_HANDLE_DB_H

#include <stddef.h>

#include "dbutils.h"
#include "str.h"

#include "gufi_find_outliers/DirData.h"
#include "gufi_find_outliers/PoolArgs.h"

#define OUTLIERS_TABLE  "outliers"
#define OUTLIERS_CREATE                                     \
    "DROP TABLE IF EXISTS " OUTLIERS_TABLE ";"              \
    "CREATE TABLE " OUTLIERS_TABLE " ("                     \
    "path TEXT, level INT64, "                              \
    "outlier_type TEXT, "                                   \
    "column TEXT, "                                         \
    "t_val DOUBLE, t_mean DOUBLE, t_stdev DOUBLE, "         \
    "s_val DOUBLE, s_mean DOUBLE, s_stdev DOUBLE "          \
    ");"
#define OUTLIERS_INSERT "INSERT INTO " OUTLIERS_TABLE " VALUES ("   \
    "@path, @level, "                                               \
    "@outlier_type, "                                               \
    "@column, "                                                     \
    "@t_val, @t_mean, @t_stdev, "                                   \
    "@s_val, @s_mean, @s_stdev "                                    \
    ");"

#define INTERMEDIATE_DBNAME_FORMAT "file:memory%zu?mode=memory&cache=shared" GUFI_SQLITE_VFS_URI

/* create a db and the outliers table */
sqlite3 *db_init(const char *dbname);

/* per-thread in-memory tables */
void intermediate_dbs_fini(struct PoolArgs *pa);
int intermediate_dbs_init(struct PoolArgs *pa);

/* insert into intermediate dbs */
int insert_outlier(sqlite3_stmt *stmt, const str_t *path, const size_t level,
                   const char *outlier_type, const refstr_t *col,
                   const Stats_t *t, const Stats_t *s);

/* output */
void write_results(struct PoolArgs *pa, size_t *rows);

#endif
