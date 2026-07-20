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
 * A simple plugin for gufi_query used for testing purposes.
 */

#include <stdio.h>

#include <sqlite3.h>

#include "plugin.h"

static int global_init(struct input *in) {
    (void) in;
    sqlite3_initialize();
    return 0;
}

static int global_bad_init(struct input *in) {
    (void) in;
    fprintf(stderr, "Error: test_querying_plugin: Bad global initialization\n");
    /* no sqlite3_initialize() because sqlite3_shutdown() will not be called */
    return 1;
}

static int thread_init(sqlite3 *db) {
    (void) db;
    return 0;
}

static int thread_bad_init(sqlite3 *db) {
    (void) db;
    fprintf(stderr, "Error: test_querying_plugin: Bad thread initialization\n");
    return 1;
}

static void *db_init(void *ptr) {
    sqlite3 *db = (sqlite3 *) ptr;

    /* do some "setup" */
    char *err = NULL;
    if (sqlite3_exec(db, "CREATE TEMP VIEW plugin_query_view AS SELECT * FROM plugin_test_summary;",
                     NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "Error: Querying plugin context setup failed: %s\n", err);
        sqlite3_free(err);
        /* return NULL? */
    }

    return db;
}

static void db_exit(void *ptr, void *user_data) {
    (void) user_data;
    sqlite3 *db = (sqlite3 *) ptr;

    char *err = NULL;
    if (sqlite3_exec(db, "DROP VIEW IF EXISTS plugin_query_view;",
                     NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "Error: Querying plugin context teardown failed: %s\n", err);
        sqlite3_free(err);
    }
}

static void thread_exit(sqlite3 *db) {
    (void) db;
}

static void global_exit(struct input *in) {
    (void) in;
    sqlite3_shutdown();
}

struct plugin_operations test_querying_plugin_ops = {
    .type = PLUGIN_QUERY,
    .global_init = global_init,
    .thread_init = thread_init,
    .ctx_init = db_init,
    .pre_process_dir = NULL,
    .pre_process_file = NULL,
    .post_process_dir = NULL,
    .post_process_file = NULL,
    .ctx_exit = db_exit,
    .thread_exit = thread_exit,
    .global_exit = global_exit,
};

struct plugin_operations test_querying_plugin_none_type = {
    .type = PLUGIN_NONE,
    .global_init = NULL,
    .thread_init = NULL,
    .ctx_init = NULL,
    .pre_process_dir = NULL,
    .pre_process_file = NULL,
    .post_process_dir = NULL,
    .post_process_file = NULL,
    .ctx_exit = NULL,
    .thread_exit = NULL,
    .global_exit = NULL,
};

struct plugin_operations test_querying_plugin_bad_global = {
    .type = PLUGIN_QUERY,
    .global_init = global_bad_init,
    .thread_init = NULL,
    .ctx_init = NULL,
    .pre_process_dir = NULL,
    .pre_process_file = NULL,
    .post_process_dir = NULL,
    .post_process_file = NULL,
    .ctx_exit = NULL,
    .thread_exit = NULL,
    .global_exit = global_exit,
};

struct plugin_operations test_querying_plugin_bad_thread = {
    .type = PLUGIN_QUERY,
    .global_init = global_init,
    .thread_init = thread_bad_init,
    .ctx_init = NULL,
    .pre_process_dir = NULL,
    .pre_process_file = NULL,
    .post_process_dir = NULL,
    .post_process_file = NULL,
    .ctx_exit = NULL,
    .thread_exit = NULL,
    .global_exit = global_exit,
};
