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



#include <dlfcn.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "plugin.h"

/*
 * Load a plugin passed in with --plugin <entrypoint>:<path>
 *
 * Returns a struct plugin pointer on success or NULL on failure
 */
struct plugin *load_plugin_library(const char *plugin_arg, const size_t len) {
    if (!plugin_arg || !len) {
        return NULL;
    }

    char *arg = strndup(plugin_arg, len);
    const char *end = arg + len;
    char *filename = NULL;
    char *entrypoint = strtok_r(arg, ":", &filename);

    if (!entrypoint || !strlen(entrypoint)) {
        fprintf(stderr, "Error: Bad plugin entrypoint\n");
        free(arg);
        return NULL;
    }

    if (!filename || (filename == end)) {
        fprintf(stderr, "Error: Bad plugin filename\n");
        free(arg);
        return NULL;
    }

    void *lib = dlopen(filename, RTLD_NOW);
    if (!lib) {
        fprintf(stderr, "Error: Could not open plugin library %s\n",
                dlerror());
        free(arg);
        return NULL;
    }

    struct plugin_operations *user_plugin_ops = (struct plugin_operations *) dlsym(lib, entrypoint);
    if (!user_plugin_ops) {
        fprintf(stderr, "Error: Could not find exported operations \"%s\" in the plugin library: %s\n",
                entrypoint, dlerror());
        dlclose(lib);
        free(arg);
        return NULL;
    }

    struct plugin *plugin = malloc(sizeof(*plugin));
    plugin->filename = plugin_arg + (filename - arg);
    plugin->handle = lib;
    plugin->ops = user_plugin_ops;

    free(arg);

    return plugin;
}

static void unload_plugin_library(struct plugin *plugin) {
    /* Not checking argument */

    /* this null check is only useful for testing */
    if (plugin->handle) {
        dlclose(plugin->handle);
    }
    free(plugin);
}

struct plugins *plugins_init(struct plugins *plugins, const size_t count, const size_t nthreads) {
    /* nthreads cannot be 0 */
    if (!plugins || !nthreads) {
        return NULL;
    }

    memset(plugins, 0, sizeof(*plugins));

    void ***user_data = calloc(nthreads, sizeof(*plugins->user_data));
    if (!user_data) {
        return NULL;
    }

    if (count) {
        for(size_t i = 0; i < nthreads; i++) {
            user_data[i] = calloc(count, sizeof(**user_data));
            if (!user_data[i]) {
                for(size_t j = 0; j < i; j++) {
                    free(user_data[j]);
                }
                free(user_data);
                return NULL;
            }
        }
    }

    plugins->plugins = calloc(count, sizeof(*plugins->plugins));
    plugins->count = count;
    plugins->initialized = 0;
    plugins->nthreads = nthreads;
    plugins->user_data = user_data;

    return plugins;
}

void plugins_destroy(struct plugins *plugins) {
    /* Not checking arguments */

    /* stack unwind */
    for(size_t i = plugins->count; i > 0; i--) {
        unload_plugin_library(plugins->plugins[i - 1]);
    }

    for(size_t i = plugins->nthreads; i > 0; i--) {
        free(plugins->user_data[i - 1]);
    }
    free(plugins->user_data);
    plugins->user_data = NULL;
    plugins->nthreads = 0;
    plugins->initialized = 0;
    plugins->count = 0;
    free(plugins->plugins);
    plugins->plugins = NULL;
}

static int plugin_check_type(struct plugin *plugin, const plugin_type accepted) {
    /* Not checking arguments */

    if ((plugin->ops->type != PLUGIN_NONE) &&
        (plugin->ops->type != accepted)) {
        fprintf(stderr, "Error: \"%s\" has bad plugin type. Expected %d. Got: %d\n",
                plugin->filename, accepted, plugin->ops->type);
        return 0;
    }
    return 1;
}

size_t plugins_check_type(struct plugins *plugins, const plugin_type accepted) {
    /* Not checking arguments */

    for(size_t i = 0; i < plugins->count; i++) {
        if (!plugin_check_type(plugins->plugins[i], accepted)) {
            return i;
        }
    }
    return plugins->count;
}

size_t plugins_global_init(struct plugins *plugins, void *global) {
    /* Not checking arguments */

    for(size_t i = 0; i < plugins->count; i++) {
        if (plugins->plugins[i]->ops->global_init) {
            if (plugins->plugins[i]->ops->global_init(global) != 0) {
                plugins_global_exit(plugins, global);
                break;
            }
        }
        plugins->initialized++;
    }

    return plugins->initialized;
}

plugin_dir_action plugins_dir_action(struct plugins* plugins, void* ctx) {
    plugin_dir_action ret = PLUGIN_PROCESS_DIR;

    for (size_t i = 0; i < plugins->count; i++) {
        if (plugins->plugins[i]->ops->dir_action) {
            plugin_dir_action pda = plugins->plugins[i]->ops->dir_action(ctx);

            // choose the most restrictive action returned by any plugin
            if (pda < ret) {
                ret = pda;
            }
        }
    }

    return ret;
}

void plugins_ctx_init(struct plugins *plugins, void *ctx, const size_t tid) {
    /* Not checking arguments */

    for(size_t i = 0; i < plugins->count; i++) {
        if (plugins->plugins[i]->ops->ctx_init) {
            plugins->user_data[tid][i] = plugins->plugins[i]->ops->ctx_init(ctx);
        }
    }
}

void plugins_process_dir(struct plugins *plugins, void *ctx, const size_t tid) {
    /* Not checking arguments */

    for(size_t i = 0; i < plugins->count; i++) {
        if (plugins->plugins[i]->ops->process_dir) {
            plugins->plugins[i]->ops->process_dir(ctx, plugins->user_data[tid][i]);
        }
    }
}

void plugins_process_file(struct plugins *plugins, void *ctx, const size_t tid) {
    /* Not checking arguments */

    for(size_t i = 0; i < plugins->count; i++) {
        if (plugins->plugins[i]->ops->process_file) {
            plugins->plugins[i]->ops->process_file(ctx, plugins->user_data[tid][i]);
        }
    }
}

void plugins_ctx_exit(struct plugins *plugins, void *ctx, const size_t tid) {
    /* Not checking arguments */

    /* stack unwind */
    for(size_t i = plugins->count; i > 0; i--) {
        if (plugins->plugins[i - 1]->ops->ctx_exit) {
            plugins->plugins[i - 1]->ops->ctx_exit(ctx, plugins->user_data[tid][i - 1]);
        }
    }

    /* not zero-ing, but all user_data[i] should be invalid at this point */
}

void plugins_global_exit(struct plugins *plugins, void *global) {
    /* Not checking arguments */

    /* stack unwind */
    for(size_t i = plugins->initialized; i > 0; i--) {
        if (plugins->plugins[i - 1]->ops->global_exit) {
            plugins->plugins[i - 1]->ops->global_exit(global);
        }
    }
}
