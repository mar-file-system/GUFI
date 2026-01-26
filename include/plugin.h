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

#include <sqlite3.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Plugins should create a `struct plugin_operations` with this name to be exported to the
 * GUFI code.
  */
#define GUFI_PLUGIN_SYMBOL gufi_plugin_operations
#define GUFI_PLUGIN_SYMBOL_STR "gufi_plugin_operations"

typedef enum {
    PLUGIN_NONE        = 0,
    PLUGIN_INDEX       = 1,
    PLUGIN_QUERY       = 2,
    PLUGIN_INCREMENTAL = 3,
} plugin_type;

/*
 * Operations for a user-defined plugin library, allowing the user to make custom
 * modifications to the databases as GUFI runs.
 */
struct plugin_operations {
    plugin_type type;

    /*
     * One-time initialization
     *
     * If the plugin does SQLite 3 operations, should call sqlite3_initialize()
     */
    void (*global_init)(void *global);

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

/* forward declarations */
struct work;
struct entry_data;

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
