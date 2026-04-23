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

#ifndef PLUGIN_H
#define PLUGIN_H

#include <stddef.h>

#include <sqlite3.h>

#ifdef __cplusplus
extern "C" {
#endif

/* forward declarations */
struct work;
struct entry_data;

typedef enum {
    PLUGIN_NONE        = 0,
    PLUGIN_INDEX       = 1,
    PLUGIN_QUERY       = 2,
    PLUGIN_INCREMENTAL = 3,
} plugin_type;

typedef enum {
    // Do not process this directory and do not descend into its subdirectories
    PLUGIN_NO_PROCESS_NO_DESCEND_DIR = 0,
    // Do not process this directory, but still allow traversal into subdirectories
    PLUGIN_NO_PROCESS_DIR = 1,
    // Process this directory and allow normal traversal
    PLUGIN_PROCESS_DIR = 2,
} plugin_dir_action;

/*
 * Operations for a user-defined plugin library, allowing the user to make custom
 * modifications to the databases as GUFI runs.
 */
struct plugin_operations {
    plugin_type type;

    /*
     * One-time initialization
     *
     * Return 0 for success, non-0 for error
     *
     * If the plugin does SQLite 3 operations, should call sqlite3_initialize()
     */
    int (*global_init)(void *global);

    /* 
     * Returns whether a directory should be processed and/or 
     * descended into
     */
    plugin_dir_action (*dir_action)(void *ptr);

    /*
     * Give the user an opportunity to initialize any state for the
     * current context. The returned pointer is passed to all
     * subsequent operations as `user_data`.
     */
    void *(*ctx_init)(void *ptr);

    /* Process a directory */
    void (*process_dir)(void *ptr, void *user_data);

    /* Process a file */
    void (*process_file)(void *ptr, void *user_data);

    /* Clean up any state for the current context */
    void (*ctx_exit)(void *ptr, void *user_data);

    /*
     * One-time cleanup
     *
     * If the plugin does SQLite 3 operations, should call sqlite3_shutdown()
     */
    void (*global_exit)(void *global);
};

/* none of these pointers can be NULL */
struct plugin {
    /* reference; for debugging */
    const char *filename;

    /* dlopen(3) handle to a plugin shared library */
    void *handle;

    /* Holds pointers to plugin functions for running custom user code to manipulate the databases. */
    const struct plugin_operations *ops;
};

struct plugin *load_plugin_library(const char *plugin_arg, const size_t len);
/* not exposing unload_plugin_library() */

struct plugins {
    struct plugin **plugins; /* list of struct plugin *; need array to go forward and backwards */
    size_t count;
    size_t initialized;      /* number of plugins to call global_exit() on */
    size_t nthreads;
    void ***user_data;       /* user_data[thread id][plugin index] */
};

/* DOES NOT call load_plugin_library() */
struct plugins *plugins_init(struct plugins *plugins, const size_t count, const size_t nthreads);

/* calls unload_plugin_library() */
void plugins_destroy(struct plugins *plugins);

/*
 * Make sure all plugins have the correct type
 *
 * This should be called before plugins_global_init()
 *
 * @return success: return plugins->count
 *         error:   return the index where the type is bad
 */
size_t plugins_check_type(struct plugins *plugins, const plugin_type accepted);

/*
 * Run same plugin op for all plugins in the correct order
 *
 * @param global      single global object passed to global init/exit
 * @param ctx         single common ctx across all plugins
 * @param tid         QPTPool thread id
 *
 * plugins_global_init()
 * @return plugins->initialized
 *         on error, stop, call plugins_global_exit() for previously
 *         successfully initialized plugins, and return error
 */
size_t            plugins_global_init (struct plugins* plugins, void* global);
plugin_dir_action plugins_dir_action  (struct plugins* plugins, void* ctx);
void              plugins_ctx_init    (struct plugins* plugins, void* ctx, const size_t tid);
void              plugins_process_dir (struct plugins* plugins, void* ctx, const size_t tid);
void              plugins_process_file(struct plugins* plugins, void* ctx, const size_t tid);
void              plugins_ctx_exit    (struct plugins* plugins, void* ctx, const size_t tid);
void              plugins_global_exit (struct plugins* plugins, void* global);

/* common plugin ptr struct (don't have to use; here to reduce duplicate struct definitions) */
typedef struct PluginCommonStruct {
    sqlite3 *db;
    struct work *work;
    struct entry_data *ed;
    void *data; /* any extra data to pass along */
} PCS_t;

#ifdef __cplusplus
}
#endif

#endif
