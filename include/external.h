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



#ifndef EXTERNAL_H
#define EXTERNAL_H

#include <sqlite3.h>
#include <stdlib.h>

#include "bf.h"

/*
 *
 * these tables/views only exist in db.db and list
 * external databases to attach to db.db
 *
 * these tables/views are always named the same no
 * matter the usage of the external databases
 */

#define EXTERNAL_DBS_PWD      "external_dbs_pwd"    /* *.db files found during indexing */
extern const char EXTERNAL_DBS_PWD_CREATE[];
extern const char EXTERNAL_DBS_PWD_INSERT[];

#define EXTERNAL_DBS_ROLLUP   "external_dbs_rollup" /* *.db files brought up during rollup */
extern const char EXTERNAL_DBS_ROLLUP_CREATE[];
extern const char EXTERNAL_DBS_ROLLUP_INSERT[];

#define EXTERNAL_DBS          "external_dbs"
extern const char EXTERNAL_DBS_CREATE[];

/*
 * external_loop
 *
 * Attach the databases listed in the external databases table to
 * db.db and create a temporary view of all data accessible by the
 * caller.
 *
 * @in work               the current work context
 * @in db                 handle to the main db
 * @in view_name          the name of the view to create
 * @in view_name_len      length of view_name
 * @in select             string containing "SELECT <columns> FROM "
 * @in select_len         length of select
 * @in table_name         the table to extract from each external database
 * @in table_name_len     length of table_name
 * @in modify_filename    if provided, modify the filename to attach into db.db
 *                        returns the length of the new filename
 * @return the number of external databases attached
 */
int external_loop(struct work *work, sqlite3 *db,
                  const char *view_name, const size_t view_name_len,
                  const char *select, const size_t select_len,
                  const char *table_name, const size_t table_name_len,
                  size_t (*modify_filename)(char **dst, const size_t dst_size,
                                            const char *src, const size_t src_len,
                                            struct work *work)
                  #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                  , size_t *query_count
                  #endif
    );

/*
 * drop view and detach external databases
 *
 * @in db           the current working context
 * @in drop_view    SQL statement of the format "DROP VIEW <view name>;"
 *                  This string is not validated. This is done to avoid
 *                  memcpys that would be done if the query were
 8                  generated instead of provided.
 */
void external_done(sqlite3 *db, const char *drop_view
                   #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                   , size_t *query_count
                   #endif
    );
#endif
