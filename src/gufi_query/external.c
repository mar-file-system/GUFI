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



#include "external.h"

#include "gufi_query/external.h"

void attach_extdbs(struct input *in, sqlite3 *db,
                   const char *dir_inode, const size_t dir_inode_len,
                   size_t *extdb_count) {
    /*
     * for each external database basename provided by the
     * caller, create a view
     *
     * if an external db is not found, only the template is used
     *
     * assumes there won't be more than 254 attaches in total
     * if necessary, change this to attach+query+detach one at a time
     */
    sll_loop(&in->external_attach, node) {
        eus_t *user = (eus_t *) sll_node_data(node);

        char basename_comp[MAXSQL];
        const size_t basename_comp_len = SNFORMAT_S(basename_comp, sizeof(basename_comp), 7,
                                                    "(pinode == '", (size_t) 12,
                                                    dir_inode, dir_inode_len,
                                                    "')", (size_t) 2,
                                                    " AND ", (size_t) 5,
                                                    "(basename(filename) == '", (size_t) 24,
                                                    user->basename.data, user->basename.len,
                                                    "')", (size_t) 2);

        const str_t basename_comp_ref = REFSTR(basename_comp, basename_comp_len);
        static const str_t SELECT_STAR = REFSTR(" SELECT * FROM ", 15);

        /*
         * attach database for current directory (if it
         * exists) and concatenate with empty template table
         * into one view
         */
        external_concatenate(db,
                             &EXTERNAL_TYPE_USER_DB,
                             &basename_comp_ref,
                             &user->view,
                             &SELECT_STAR,
                             &user->table,
                             &user->template_table,
                             NULL, NULL,
                             external_increment_attachname, extdb_count);

    }
}

/* create views without iterating through tables */
int create_extdb_views_noiter(sqlite3 *db) {
    char *err = NULL;
    if (sqlite3_exec(db,
                     "CREATE TEMP VIEW " ESUMMARY     " AS SELECT * FROM " SUMMARY     ";"
                     "CREATE TEMP VIEW " EPENTRIES    " AS SELECT * FROM " PENTRIES    ";"
                     "CREATE TEMP VIEW " EXSUMMARY    " AS SELECT * FROM " XSUMMARY    ";"
                     "CREATE TEMP VIEW " EXPENTRIES   " AS SELECT * FROM " XPENTRIES   ";"
                     "CREATE TEMP VIEW " EVRSUMMARY   " AS SELECT * FROM " VRSUMMARY   ";"
                     "CREATE TEMP VIEW " EVRPENTRIES  " AS SELECT * FROM " VRPENTRIES  ";"
                     "CREATE TEMP VIEW " EVRXSUMMARY  " AS SELECT * FROM " VRXSUMMARY  ";"
                     "CREATE TEMP VIEW " EVRXPENTRIES " AS SELECT * FROM " VRXPENTRIES ";",
                     NULL, NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Warning: Could not create partition views for attaching with external databases: %s\n", err);
    }
    return !!err;
}

/* create views for iterating through tables */
void create_extdb_views_iter(sqlite3 *db, const char *dir_inode) {
    char extdb_views[MAXSQL];
    SNPRINTF(extdb_views, sizeof(extdb_views),
             "CREATE TEMP VIEW " ESUMMARY     " AS SELECT * FROM " SUMMARY     " WHERE  inode == '%s';"
             "CREATE TEMP VIEW " EPENTRIES    " AS SELECT * FROM " PENTRIES    " WHERE pinode == '%s';"
             "CREATE TEMP VIEW " EXSUMMARY    " AS SELECT * FROM " XSUMMARY    " WHERE  inode == '%s';"
             "CREATE TEMP VIEW " EXPENTRIES   " AS SELECT * FROM " XPENTRIES   " WHERE pinode == '%s';"
             "CREATE TEMP VIEW " EVRSUMMARY   " AS SELECT * FROM " VRSUMMARY   " WHERE  inode == '%s';"
             "CREATE TEMP VIEW " EVRPENTRIES  " AS SELECT * FROM " VRPENTRIES  " WHERE pinode == '%s';"
             "CREATE TEMP VIEW " EVRXSUMMARY  " AS SELECT * FROM " VRXSUMMARY  " WHERE  inode == '%s';"
             "CREATE TEMP VIEW " EVRXPENTRIES " AS SELECT * FROM " VRXPENTRIES " WHERE pinode == '%s';",
             dir_inode, dir_inode, dir_inode, dir_inode, dir_inode, dir_inode, dir_inode, dir_inode);

    char *err = NULL;
    if (sqlite3_exec(db, extdb_views, NULL, NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Warning: Could not create partition views for attaching with external databases: %s\n", err);
    }
}

/* drop view for attaching external dbs to */
void drop_extdb_views(sqlite3 *db) {
    char *err = NULL;
    if (sqlite3_exec(db,
                     "DROP VIEW " EVRXPENTRIES ";"
                     "DROP VIEW " EVRXSUMMARY  ";"
                     "DROP VIEW " EVRPENTRIES  ";"
                     "DROP VIEW " EVRSUMMARY   ";"
                     "DROP VIEW " EXPENTRIES   ";"
                     "DROP VIEW " EXSUMMARY    ";"
                     "DROP VIEW " EPENTRIES    ";"
                     "DROP VIEW " ESUMMARY     ";",
                     NULL, NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Warning: Could not drop views for attaching with external databases: %s\n", err);
    }
}

void detach_extdbs(struct input *in, sqlite3 *db,
                   const char *dir_inode, const size_t dir_inode_len,
                   size_t *extdb_count) {
    /* detach each external db */
    sll_loop(&in->external_attach, node) {
        eus_t *user = (eus_t *) sll_node_data(node);

        /* drop user defined view */
        char drop_extdb_view[MAXSQL];
        SNFORMAT_S(drop_extdb_view, sizeof(drop_extdb_view), 3,
                   "DROP VIEW ", (size_t) 10,
                   user->view.data, user->view.len,
                   ";", (size_t) 1);

        if (dir_inode && dir_inode_len) {
            char basename_comp[MAXSQL];
            const size_t basename_comp_len = SNFORMAT_S(basename_comp, sizeof(basename_comp), 7,
                                                        "(pinode == '", (size_t) 12,
                                                        dir_inode, dir_inode_len,
                                                        "')", (size_t) 2,
                                                        " AND ", (size_t) 5,
                                                        "(basename(filename) == '", (size_t) 24,
                                                        user->basename.data, user->basename.len,
                                                        "')", (size_t) 2);

            const str_t basename_comp_ref = REFSTR(basename_comp, basename_comp_len);

            external_concatenate_cleanup(db, drop_extdb_view,
                                         &EXTERNAL_TYPE_USER_DB,
                                         &basename_comp_ref,
                                         external_decrement_attachname,
                                         extdb_count);
        }
        else {
            char *err = NULL;
            if (sqlite3_exec(db, drop_extdb_view, NULL, NULL, &err) != SQLITE_OK) {
                sqlite_print_err_and_free(err, stderr, "Could not drop view %s: %s\n", user->view.data, err);
            }
        }
    }
}
