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



/*
 * A simple plugin for gufi_dir2index used for testing purposes.
 */

#include <stdio.h>
#include <stdlib.h>

#include <sqlite3.h>

#include "bf.h"
#include "plugin.h"

struct state {
    int n_files;
    int n_dirs;
};

static struct state *new_state(void) {
    struct state *new = calloc(1, sizeof *new);

    return new;
}

static void cleanup_state(struct state *p) {
    free(p);
}

/*
 * Set up initial state for test plugin.
 */
static void *db_init(void *ptr) {
    PCS_t *pcs = (PCS_t *) ptr;
    sqlite3 *db = pcs->db;

    struct state *state = new_state();

    const char *text =
        "CREATE TABLE plugin_test_files (id INTEGER PRIMARY KEY AUTOINCREMENT, filename TEXT);"
        "CREATE TABLE plugin_test_directories (id INTEGER PRIMARY KEY AUTOINCREMENT, dirname TEXT);"
        "CREATE TABLE plugin_test_summary (filetype TEXT PRIMARY KEY, count INTEGER, CHECK (filetype IN ('file', 'directory')));";
    char *error;

    int res = sqlite3_exec(db, text, NULL, NULL, &error);
    if (res != SQLITE_OK) {
        fprintf(stderr, "test plugin: db_init(): error executing statement: %d %s\n", res, error);
    }

    sqlite3_free(error);

    return state;
}

/*
 * Save state to the database and clean up state.
 */
static void db_exit(void *ptr, void *user_data) {
    PCS_t *pcs = (PCS_t *) ptr;
    sqlite3 *db = pcs->db;
    struct state *p = (struct state *) user_data;

    char *text = sqlite3_mprintf("INSERT INTO plugin_test_summary (filetype, count) VALUES ('file', %d);"
            "INSERT INTO plugin_test_summary (filetype, count) VALUES ('directory', %d);",
            p->n_files, p->n_dirs);
    char *error = NULL;

    int res = sqlite3_exec(db, text, NULL, NULL, &error);
    if (res != SQLITE_OK) {
        fprintf(stderr, "test plugin: db_exit(): error executing statement: %d %s\n",
                res, error);
    }

    sqlite3_free(text);
    sqlite3_free(error);

    cleanup_state(p);
}

static void process_file(void *ptr, void *user_data) {
    PCS_t *pcs = (PCS_t *) ptr;
    sqlite3 *db = pcs->db;
    struct state *p = (struct state *) user_data;

    char *text = sqlite3_mprintf("INSERT INTO plugin_test_files (filename) VALUES ('%s');", pcs->work->name);
    char *error = NULL;

    int res = sqlite3_exec(db, text, NULL, NULL, &error);
    if (res == SQLITE_OK) {
        p->n_files += 1;
    } else {
        fprintf(stderr, "test plugin: process_file(): error executing statement: %d %s\n",
                res, error);
    }

    sqlite3_free(text);
    sqlite3_free(error);
}

static void process_dir(void *ptr, void *user_data) {
    PCS_t *pcs = (PCS_t *) ptr;
    sqlite3 *db = pcs->db;
    struct state *p = (struct state *) user_data;

    char *text = sqlite3_mprintf("INSERT INTO plugin_test_directories (dirname) VALUES ('%s');", pcs->work->name);
    char *error;

    int res = sqlite3_exec(db, text, NULL, NULL, &error);
    if (res == SQLITE_OK) {
        p->n_dirs += 1;
    } else {
        fprintf(stderr, "test plugin: process_dir(): error executing statement: %d %s\n",
                res, error);
    }

    sqlite3_free(text);
    sqlite3_free(error);
}

struct plugin_operations GUFI_PLUGIN_SYMBOL = {
    .type = PLUGIN_INDEX,
    .global_init = NULL,
    .ctx_init = db_init,
    .process_dir = process_dir,
    .process_file = process_file,
    .ctx_exit = db_exit,
    .global_exit = NULL,
};
