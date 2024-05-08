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

#include <stdlib.h>

#include "SinglyLinkedList.h"
#include "bf.h"
#include "dbutils.h"
#include "trie.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 *
 * these tables/views only exist in db.db and list
 * external databases to attach to db.db
 *
 * these tables/views are always named the same no
 * matter the usage of the external databases
 */

#define EXTERNAL_DBS_PWD           "external_dbs_pwd"    /* *.db files found during indexing */
extern const char EXTERNAL_DBS_PWD_CREATE[];
extern const char EXTERNAL_DBS_PWD_INSERT[];

#define EXTERNAL_DBS_ROLLUP        "external_dbs_rollup" /* *.db files brought up during rollup */
extern const char EXTERNAL_DBS_ROLLUP_CREATE[];
extern const char EXTERNAL_DBS_ROLLUP_INSERT[];

#define EXTERNAL_DBS               "external_dbs"
#define EXTERNAL_DBS_LEN           (sizeof(EXTERNAL_DBS) - 1)

#define EXTERNAL_TYPE_XATTR_NAME   "xattrs"
#define EXTERNAL_TYPE_XATTR_LEN    (sizeof(EXTERNAL_TYPE_XATTR_NAME) - 1)
extern const refstr_t EXTERNAL_TYPE_XATTR;               /* convenience struct */

#define EXTERNAL_TYPE_USER_DB_NAME "user_db"
#define EXTERNAL_TYPE_USER_DB_LEN  (sizeof(EXTERNAL_TYPE_USER_DB_NAME) - 1)
extern const refstr_t EXTERNAL_TYPE_USER_DB;             /* convenience struct */

#define EXTERNAL_ATTACH_PREFIX "extdb"

int create_external_tables(const char *name, sqlite3 *db, void *args);

size_t external_create_query(char *sql,         const size_t sql_size,
                             const char *cols,  const size_t cols_len,
                             const char *table, const size_t table_len,
                             const refstr_t *type,
                             const refstr_t *extra);

int external_insert(sqlite3 *db, const char *type, const long long int pinode, const char *filename);

/*
 * external_concatenate
 *
 * Attach the databases selected from the external databases table
 * to db.db and concatenate all of the data into one view.
 *
 * type and extra are associated with EXTERNAL_DBS, not with the
 * databases being attached, so the arguments come before viewname.
 *
 * @in db                 handle to the main db
 * @in type               external database type (xattrs, user_db, etc.);
 *                        if NULL, external_db table is not queried
 * @in extra              extra conditions to check to select external database rows
 * ----------------------------------------------------------------------------------
 * @in viewname           the name of the view to create
 * @in select             string containing "SELECT <columns> FROM "
 * @in table_name         the table to extract from each external database
 * @in modify_filename    if provided, modify the filename to attach into db.db
 *                        returns the length of the new filename
 * @in set_attachname     must be provided to determine the attach name
 *                        returns the length of the new filename
 * @return the number of external databases attached
 */
int external_concatenate(sqlite3 *db,
                         const refstr_t *type,
                         const refstr_t *extra,
                         const refstr_t *viewname,
                         const refstr_t *select,
                         const refstr_t *tablename,
                         const refstr_t *default_table,
                         size_t (*modify_filename)(char **dst, const size_t dst_size,
                                                   const char *src, const size_t src_len,
                                                   void *args),
                         void *filename_args,
                         size_t (*set_attachname)(char *dst, const size_t dst_size,
                                                  void *args),
                         void *attachname_args
                         #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                         , size_t *query_count
                         #endif
    );

/*
 * external_concatenate_cleanup
 * Drop combined view and detach external databases.
 *
 * @in db                 the current working context
 * @in drop_view          SQL statement of the format "DROP VIEW <view name>;"
 *                        This string is not validated. This is done to avoid
 *                        memcpys that would be done if the query were
 *                        generated instead of provided.
 * @in type               external database type (xattrs, user_db, etc.)
 * @in extra              extra conditions to check to select external database rows
 */
void external_concatenate_cleanup(sqlite3 *db, const char *drop_view,
                                  const refstr_t *type,
                                  const refstr_t *extra,
                                  size_t (*set_attachname)(char *dst, const size_t dst_size,
                                                           void *args),
                                  void *attachname_args
                                  #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                                  , size_t *query_count
                                  #endif
    );

size_t external_increment_attachname(char *dst, const size_t dst_size,
                                     void *args);
size_t external_decrement_attachname(char *dst, const size_t dst_size,
                                     void *args);

typedef struct external_user_setup {
    refstr_t basename;
    /* refstr_t attachname (pulled from table) */
    refstr_t table;
    refstr_t template_table;
    refstr_t view;
} eus_t;

/*
 * external_with_template
 *
 * For each view requested by the user:
 *     Call external_concatenate
 *         Select all database matches
 *         Attach all databases
 *         Create view for databases
 *
 * Assumes template_table_name is already available
 *
 * @in db                      handle to the main db
 * @in type                    external database type (xattrs, user, etc.)
 * @in type_len                length of type
 * @in eus                     list of external user db setup configs
 * @return the number of views created
 */
int external_with_template(sqlite3 *db,
                           const refstr_t *type,
                           sll_t *eus
                           #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                           , size_t *query_count
                           #endif
    );

/*
 * external_with_template_cleanup
 *
 * Drop each view followed by the user db if it exists.
 *
 * @in db                      handle to the main db
 * @in type                    external database type (xattrs, user, etc.)
 * @in type_len                length of type
 * @in eus                     list of external user db setup configs
 * @return 0
 */
int external_with_template_cleanup(sqlite3 *db,
                                   const refstr_t *type,
                                   sll_t *eus
                                   #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                                   , size_t *query_count
                                   #endif
    );

#ifdef __cplusplus
}
#endif

#endif
