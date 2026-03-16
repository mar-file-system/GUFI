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
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


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
build instruction:

cd contrib

gcc -g -c -fPIC plugins/marfs_plugin.c \
  -I../include \
  -I../build/deps/sqlite3/include \
  -I/path/to/marfs/install/include \
  -I/path/to/marfs/src/marfs/src \
  -I/usr/include/libxml2

gcc -shared -o marfs_plugin.so marfs_plugin.o \
  -L/path/to/marfs/install/lib \
  -lmarfs \
  -lxml2 \
  -Wl,-rpath,/path/to/marfs/install/lib

run example:
MARFS_CONFIG_PATH=/path/to/marfs/install/etc/marfs-config.xml \
  ./build/src/gufi_dir2index \
  --plugin ./contrib/marfs_plugin.so \
  /path/to/mdal-root/sec-root /home/$USER/gufi-index-dir
*/

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stddef.h>
#include <stdlib.h>

#include "bf.h"
#include "plugin.h"
#include "str.h"
#include "utils.h"
#include "xattrs.h"

#include "config/config.h"
#include "hash/hash.h"

static const char* MARFS_PREFIX = "MDAL_";
static const char* MARFS_SUBSPACES_NAME = "MDAL_subspaces";
static const char* MARFS_CONFIG_ENV = "MARFS_CONFIG_PATH";

static const char MARFS_XATTR_NAME[] = "user.MDAL_MARFS-FILE";

// used for sorting namespaces
static int cmp_cstr_ptr(const void* a, const void* b) {
    const char* sa = *(const char* const*)a;
    const char* sb = *(const char* const*)b;
    return strcmp(sa, sb);
}

// sort namespace paths
static void sort_namespaces(char** namespace_list, size_t namespace_count) {
    qsort(namespace_list, namespace_count, sizeof *namespace_list, cmp_cstr_ptr);
}

// returns the basename of a path
str_t get_basename(str_t path) {
    if (!path.data || path.len == 0) return (str_t)REFSTR(NULL, 0);

    size_t len = path.len;

    // trim trailing slashes except root
    while (len > 1 && path.data[len - 1] == '/') len--;

    size_t i = len;
    while (i > 0 && path.data[i - 1] != '/') i--;

    return (str_t)REFSTR(path.data + i, len - i);
}

// returns the parent of a path
str_t get_parent(str_t path) {
    if (!path.data || path.len == 0) return (str_t)REFSTR(NULL, 0);

    size_t len = path.len;

    // trim trailing slashes except root
    while (len > 1 && path.data[len - 1] == '/') len--;

    // root stays root
    if (len == 1 && path.data[0] == '/') return (str_t)REFSTR(path.data, 1);

    size_t i = len;
    while (i > 0 && path.data[i - 1] != '/') i--;

    if (i == 0) return (str_t)REFSTR(NULL, 0);

    size_t parent_len = (i - 1 == 0) ? 1 : (i - 1);
    return (str_t)REFSTR(path.data, parent_len);
}

// returns the grandparent of a path
str_t get_grandparent(str_t path) { return get_parent(get_parent(path)); }

// join two str_t file paths together
char* join_path(str_t lhs, str_t rhs) {
    size_t len = lhs.len + 1 + rhs.len + 1;
    char* out = malloc(len);
    if (!out) return NULL;

    snprintf(out, len, "%.*s/%.*s", (int)lhs.len, lhs.data, (int)rhs.len, rhs.data);
    return out;
}

// check if str_t starts with a prefix
static int str_t_startswith(str_t s, const char* prefix) {
    size_t plen = strlen(prefix);
    return s.data && s.len >= plen && memcmp(s.data, prefix, plen) == 0;
}

// check if a str_t is equal to a char*
static int str_t_eq_cstr(str_t s, const char* cstr) {
    size_t len = strlen(cstr);
    return s.data && s.len == len && memcmp(s.data, cstr, len) == 0;
}

// free a str_t
static void free_str_members(str_t* s) {
    if (!s) return;

    if (s->free) {
        s->free(s->data);
    }

    s->data = NULL;
    s->len = 0;
    s->free = NULL;
}

struct marfs_plugin {
    char** namespaces;
    char** index_namespaces;
    size_t namespaces_count;

    str_t index_parent;  // actual index is placed at <index parent>/$(basename <src>)
    str_t marfs_mountpoint;
    str_t root_namespace;
    size_t root_namespace_level;
};

static struct marfs_plugin g_state;

static void marfs_plugin_cleanup(void) {
    if (g_state.namespaces) {
        for (size_t i = 0; i < g_state.namespaces_count; i++) {
            free(g_state.namespaces[i]);
        }
        free(g_state.namespaces);
    }

    if (g_state.index_namespaces) {
        for (size_t i = 0; i < g_state.namespaces_count; i++) {
            free(g_state.index_namespaces[i]);
        }
        free(g_state.index_namespaces);
    }

    free_str_members(&g_state.index_parent);
    free_str_members(&g_state.marfs_mountpoint);
    free_str_members(&g_state.root_namespace);

    g_state.namespaces = NULL;
    g_state.index_namespaces = NULL;
    g_state.namespaces_count = 0;
}

// count the number of namespace paths in the marfs config
static size_t count_ns_paths(marfs_ns* ns) {
    if (!ns) return 0;

    size_t total = 0;

    for (size_t i = 0; i < ns->subnodecount; i++) {
        HASH_NODE* hn = &ns->subnodes[i];
        marfs_ns* child = (marfs_ns*)hn->content;

        if (!hn->name || !child) continue;

        total++;
        total += count_ns_paths(child);
    }

    return total;
}

// retrieve the namespace paths from the marfs config
static int collect_ns_paths(marfs_ns* ns, const char* parent_path, char** paths, size_t* index) {
    if (!ns || !paths || !index) return -1;

    for (size_t i = 0; i < ns->subnodecount; i++) {
        HASH_NODE* hn = &ns->subnodes[i];
        marfs_ns* child = (marfs_ns*)hn->content;

        if (!hn->name || !child) continue;

        char* path = NULL;
        int n;

        if (!parent_path || parent_path[0] == '\0') {
            n = snprintf(NULL, 0, "%s", hn->name);
            if (n < 0) return -1;

            path = malloc((size_t)n + 1);
            if (!path) return -1;

            snprintf(path, (size_t)n + 1, "%s", hn->name);
        } else {
            n = snprintf(NULL, 0, "%s/%s/%s", parent_path, MARFS_SUBSPACES_NAME, hn->name);
            if (n < 0) return -1;

            path = malloc((size_t)n + 1);
            if (!path) return -1;

            snprintf(path, (size_t)n + 1, "%s/%s/%s", parent_path, MARFS_SUBSPACES_NAME, hn->name);
        }

        paths[*index] = path;
        (*index)++;

        if (collect_ns_paths(child, path, paths, index) != 0) {
            (*index)--;
            paths[*index] = NULL;
            free(path);
            return -1;
        }
    }

    return 0;
}

// validate that we are actually pointed at the root namespace of a marfs-mdal (sec-root).
// There should be an MDAL_subspaces directly under the root namespace
// then there should be our first namespaces right under the MDAL_subspaces
// NOTE: this requires that the namespaces be sorted beforehand
static int validate_sec_root(void) {
    struct stat st;
    int ret = 0;
    char* path = NULL;
    int len;

    if (!g_state.root_namespace.data || g_state.root_namespace.len == 0) goto cleanup;
    if (!g_state.namespaces || g_state.namespaces_count == 0) goto cleanup;
    if (!g_state.namespaces[0]) goto cleanup;

    // check if MDAL_subspaces exists under the target dir
    len = snprintf(NULL, 0, "%s/%s", g_state.root_namespace.data, MARFS_SUBSPACES_NAME);
    if (len < 0) goto cleanup;

    path = malloc((size_t)len + 1);
    if (!path) goto cleanup;

    snprintf(path, (size_t)len + 1, "%s/%s", g_state.root_namespace.data, MARFS_SUBSPACES_NAME);

    if (stat(path, &st) != 0 || !S_ISDIR(st.st_mode)) goto cleanup;

    // check if one of the top namespaces is in the first MDAL_subspaces
    if (stat(g_state.namespaces[0], &st) != 0 || !S_ISDIR(st.st_mode)) goto cleanup;

    ret = 1;

cleanup:
    free(path);
    return ret;
}

// cleanup_marfs_index will go through each namesapce defined in the marfs config, move it up a directory, and delete
// the empty MDAL_subspaces that remains. It also removes any empty MDAL_subspaces that exist within the namespace as
// well as renaming the root directory to the basename of the specified moutnpoint in the marfs config
static int cleanup_marfs_index() {
    int ret = 0;

    // loop through our index namespaces and move them up a directory and delete the unecessary MDAL_subspaces
    for (size_t i = g_state.namespaces_count; i-- > 0;) {
        const str_t old = (str_t)REFSTR(g_state.index_namespaces[i], strlen(g_state.index_namespaces[i]));

        const str_t basename = get_basename(old);
        const str_t parent = get_parent(old);
        const str_t grandparent = get_grandparent(old);

        char* new_path = NULL;
        char* parent_path = NULL;
        char* child_subspaces = NULL;

        // check to see if there is an empty MDAL_subspaces within this namespace
        {
            size_t child_subspaces_len =
                old.len + 1 + strlen(MARFS_SUBSPACES_NAME) + 1;  // old + "/" + MDAL_subspaces + NUL
            child_subspaces = malloc(child_subspaces_len);
            if (!child_subspaces) {
                fprintf(stderr, "malloc failed for child_subspaces\n");
                ret = -1;
                continue;
            }

            snprintf(child_subspaces, child_subspaces_len, "%.*s/%s", (int)old.len, old.data, MARFS_SUBSPACES_NAME);
            if (rmdir(child_subspaces) != 0) {
                if (errno != ENOTEMPTY && errno != ENOENT) {
                    fprintf(stderr, "rmdir('%s') failed: %s\n", child_subspaces, strerror(errno));
                    ret = -1;
                }
            }
            free(child_subspaces);
        }

        // move this namespace up a directory (into it's grandparent)
        {
            size_t new_len = grandparent.len + 1 + basename.len + 1;  // gp + "/" + base + NUL
            new_path = malloc(new_len);
            if (!new_path) {
                fprintf(stderr, "malloc failed for new_path\n");
                ret = -1;
                continue;
            }

            if (grandparent.len == 1 && grandparent.data[0] == '/') {
                snprintf(new_path, new_len, "/%.*s", (int)basename.len, basename.data);
            } else {
                snprintf(new_path, new_len, "%.*s/%.*s", (int)grandparent.len, grandparent.data, (int)basename.len,
                         basename.data);
            }

            if (rename(old.data, new_path) != 0) {
                fprintf(stderr, "rename('%s' -> '%s') failed: %s\n", old.data, new_path, strerror(errno));
                free(new_path);
                ret = -1;
                continue;
            } else {
                // printf("moved: %s -> %s\n", old, new_path);
            }
            free(new_path);
        }

        // attempt to remove the old "parent" of the namespace
        {
            if (parent.len > 0) {
                parent_path = malloc(parent.len + 1);
                if (!parent_path) {
                    fprintf(stderr, "malloc failed for parent_path\n");
                    ret = -1;
                    continue;
                }

                memcpy(parent_path, parent.data, parent.len);
                parent_path[parent.len] = '\0';

                if (rmdir(parent_path) != 0) {
                    if (errno != ENOTEMPTY) {
                        fprintf(stderr, "rmdir('%s') failed: %s\n", parent_path, strerror(errno));
                        ret = -1;
                    }
                }
            }

            free(parent_path);
        }
    }

    // rename the root namespace to the marfs mountpoint from the marfs config
    const str_t rn_base = get_basename(g_state.root_namespace);
    const str_t mm_base = get_basename(g_state.marfs_mountpoint);

    char* rn_path = join_path(g_state.index_parent, rn_base);
    char* mm_path = join_path(g_state.index_parent, mm_base);

    if (!mm_path || !rn_path) {
        fprintf(stderr, "join_path failed\n");
        free(mm_path);
        free(rn_path);
        return -1;
    }

    if (rename(rn_path, mm_path) != 0) {
        fprintf(stderr, "rename('%s' -> '%s') failed: %s\n", rn_path, mm_path, strerror(errno));
        free(mm_path);
        free(rn_path);
        return -1;
    } else {
        // printf("moved: %s -> %s\n", rn_path, mm_path);
    }

    free(mm_path);
    free(rn_path);

    return ret;
}

// revert_marfs_index will undo everything that's in cleanup_marfs_index. This allows for indexing to be run over an
// existing index tree without error
static int revert_marfs_index() {
    mode_t mode = 0701;
    int ret = 0;

    // rename the marfs mountpoint from the marfs config to the root namespace
    const str_t mm_base = get_basename(g_state.marfs_mountpoint);
    const str_t rn_base = get_basename(g_state.root_namespace);

    char* mm_path = join_path(g_state.index_parent, mm_base);
    char* rn_path = join_path(g_state.index_parent, rn_base);

    if (!mm_path || !rn_path) {
        fprintf(stderr, "join_path failed\n");
        free(mm_path);
        free(rn_path);
        return -1;
    }

    if (rename(mm_path, rn_path) != 0) {
        // fprintf(stderr, "rename('%s' -> '%s') failed: %s\n", mm_path, rn_path, strerror(errno));
    } else {
        // printf("moved: %s -> %s\n", mm_path, rn_path);
    }

    free(mm_path);
    free(rn_path);

    // loop through our index namespaces and add a subspaces into them. then move them down into each subspace
    for (size_t i = 0; i < g_state.namespaces_count; i++) {
        const str_t old = (str_t)REFSTR(g_state.index_namespaces[i], strlen(g_state.index_namespaces[i]));

        const str_t basename = get_basename(old);
        const str_t parent = get_parent(old);
        const str_t grandparent = get_grandparent(old);

        char* indexed_path = NULL;
        char* parent_path = NULL;

        size_t new_len = grandparent.len + 1 + basename.len + 1;  // gp + "/" + base + NUL
        indexed_path = malloc(new_len);
        if (!indexed_path) {
            fprintf(stderr, "malloc failed for indexed_path\n");
            ret = -1;
            continue;
        }

        if (grandparent.len == 1 && grandparent.data[0] == '/') {
            snprintf(indexed_path, new_len, "/%.*s", (int)basename.len, basename.data);
        } else {
            snprintf(indexed_path, new_len, "%.*s/%.*s", (int)grandparent.len, grandparent.data, (int)basename.len,
                     basename.data);
        }

        // make an empty MDAL_subspaces in this namespace's parent
        {
            // parent is a slice, so make a real C string before mkdir()
            if (parent.len > 1) {
                parent_path = malloc(parent.len + 1);
                if (!parent_path) {
                    fprintf(stderr, "malloc failed for parent_path\n");
                    free(indexed_path);
                    ret = -1;
                    continue;
                }

                memcpy(parent_path, parent.data, parent.len);
                parent_path[parent.len] = '\0';

                if (mkdir(parent_path, mode) != 0) {
                    if (errno != ENOTEMPTY && errno != EEXIST) {
                        // fprintf(stderr, "mkdir('%s') failed: %s\n", parent_path, strerror(errno));
                        free(parent_path);
                        free(indexed_path);
                        // ret = -1;
                        continue;
                    }
                } else {
                    // printf("added empty %s: %s\n", MARFS_SUBSPACES_NAME, parent_path);
                }

                free(parent_path);
            }
        }

        // move this namespace into the MDAL_subspaces we just made
        {
            if (rename(indexed_path, old.data) != 0) {
                fprintf(stderr, "rename('%s' -> '%s') failed: %s\n", indexed_path, old.data, strerror(errno));
                free(indexed_path);
                // ret = -1;
                continue;
            } else {
                // printf("moved: %s -> %s\n", indexed_path, old.data);
            }

            free(indexed_path);
        }
    }

    return ret;
}

// marfs_indexing_global_init reads the marfs config and adds the namespaces to a global state.
static void marfs_indexing_global_init(void* global) {
    struct input* in = (struct input*)global;
    pthread_mutex_t marfs_erasurelock = PTHREAD_MUTEX_INITIALIZER;

    char** config_namespaces = NULL;
    size_t config_index = 0;
    size_t i = 0;
    int ret = -1;

    // We can only allow for a single dir to be indexed at a time. this is because we have no way to taking in multiple
    // marfs configs
    if (in->pos.argc != 2) {
        fprintf(stderr, "Error: Marfs plugin requires exactly 2 paths\n");
        exit(EXIT_FAILURE);
    }

    // add index parent and root namespace to global state
    INSTALL_STR(&g_state.index_parent, in->pos.argv[in->pos.argc - 1]);
    INSTALL_STR(&g_state.root_namespace, in->pos.argv[0]);

    g_state.root_namespace_level = 0;

    char* marfs_config_path = getenv(MARFS_CONFIG_ENV);

    errno = 0;
    marfs_config* cfg = config_init(marfs_config_path, &marfs_erasurelock);
    if (!cfg) {
        int e = errno;
        fprintf(stderr, "marfs config_init returned NULL (errno=%d)\n", e);
        goto cleanup;
    }

    // add marfs config mountpoint to global state
    INSTALL_STR(&g_state.marfs_mountpoint, cfg->mountpoint);

    g_state.namespaces_count = count_ns_paths(cfg->rootns);

    if (g_state.namespaces_count == 0) {
        fprintf(stderr, "Error: no namespaces found in MarFS config\n");
        goto cleanup;
    }

    config_namespaces = calloc(g_state.namespaces_count, sizeof(char*));
    if (!config_namespaces) {
        fprintf(stderr, "Error: config_namespaces calloc failed\n");
        goto cleanup;
    }

    if (collect_ns_paths(cfg->rootns, "", config_namespaces, &config_index) != 0) {
        fprintf(stderr, "Error: collect_ns_paths failed\n");
        goto cleanup;
    }

    if (config_index != g_state.namespaces_count) {
        fprintf(stderr, "Error: namespace count mismatch (expected %zu, got %zu)\n", g_state.namespaces_count,
                config_index);
        goto cleanup;
    }

    g_state.namespaces = calloc(g_state.namespaces_count, sizeof(char*));
    g_state.index_namespaces = calloc(g_state.namespaces_count, sizeof(char*));

    if (!g_state.namespaces || !g_state.index_namespaces) {
        fprintf(stderr, "Error: namespace array allocation failed\n");
        goto cleanup;
    }

    const str_t rn_base = get_basename(g_state.root_namespace);

    // prepend our prefix to each namespace
    for (i = 0; i < g_state.namespaces_count; i++) {
        int len =
            snprintf(NULL, 0, "%s/%s/%s", g_state.root_namespace.data, MARFS_SUBSPACES_NAME, config_namespaces[i]);
        if (len < 0) {
            fprintf(stderr, "Error: snprintf failed for namespace[%zu]\n", i);
            goto cleanup;
        }

        g_state.namespaces[i] = malloc((size_t)len + 1);
        if (!g_state.namespaces[i]) {
            fprintf(stderr, "Error: malloc failed for namespace[%zu]\n", i);
            goto cleanup;
        }

        snprintf(g_state.namespaces[i], (size_t)len + 1, "%s/%s/%s", g_state.root_namespace.data, MARFS_SUBSPACES_NAME,
                 config_namespaces[i]);

        // printf("namespace: %s\n", g_state.namespaces[i]);

        len = snprintf(NULL, 0, "%.*s/%.*s/%s/%s", (int)g_state.index_parent.len, g_state.index_parent.data,
                       (int)rn_base.len, rn_base.data, MARFS_SUBSPACES_NAME, config_namespaces[i]);
        if (len < 0) {
            fprintf(stderr, "Error: snprintf failed for index_namespace[%zu]\n", i);
            goto cleanup;
        }

        g_state.index_namespaces[i] = malloc((size_t)len + 1);
        if (!g_state.index_namespaces[i]) {
            fprintf(stderr, "Error: malloc failed for index_namespace[%zu]\n", i);
            goto cleanup;
        }

        snprintf(g_state.index_namespaces[i], (size_t)len + 1, "%.*s/%.*s/%s/%s", (int)g_state.index_parent.len,
                 g_state.index_parent.data, (int)rn_base.len, rn_base.data, MARFS_SUBSPACES_NAME, config_namespaces[i]);
    }

    sort_namespaces(g_state.namespaces, g_state.namespaces_count);
    sort_namespaces(g_state.index_namespaces, g_state.namespaces_count);

    if (!validate_sec_root()) {
        fprintf(stderr, "Error: provided dir is not the root of the parent marfs namespace\n");
        goto cleanup;
    }

    if (revert_marfs_index() != 0) {
        fprintf(stderr, "Error: revert_marfs_index failed\n");
        goto cleanup;
    }

    ret = 0;

cleanup:
    if (config_namespaces) {
        for (size_t j = 0; j < config_index; j++) {
            free(config_namespaces[j]);
        }
        free(config_namespaces);
    }

    if (ret != 0) {
        marfs_plugin_cleanup();
        exit(EXIT_FAILURE);
    }
}

// simple check to determine if a provided file path is a namespace in the marfs config
static int is_namespace(str_t path) {
    if (!path.data) {
        return 0;
    }

    for (size_t i = 0; i < g_state.namespaces_count; i++) {
        const char* ns = g_state.namespaces[i];
        if (!ns) {
            continue;
        }

        size_t ns_len = strlen(ns);
        if (ns_len != path.len) {
            continue;
        }

        if (memcmp(ns, path.data, path.len) == 0) {
            return 1;
        }
    }

    return 0;
}

// marfs_pre_processing_dir checks if we're about to process a marfs specific directory (MDAL_reference, MDAL_subspaces)
// or a namespace that is no longer active in the marfs config
static process_action marfs_pre_processing_dir(void* ptr, void* user_data) {
    PCS_t* pcs = (PCS_t*)ptr;
    (void)user_data;

    if (!pcs || !pcs->work || !pcs->work->name) {
        fprintf(stderr, "Error: gufi pcs is NULL\n");
        return PROCESS_DIR;
    }

    const str_t path = (str_t)REFSTR(pcs->work->name, strlen(pcs->work->name));
    const str_t basename = get_basename(path);
    const str_t parent = get_parent(path);

    if (str_t_startswith(basename, MARFS_PREFIX)) {
        // this directory starts with MDAL_

        // determine if this is actual a marfs directory or a user dir
        // cases for being a marfs dir:
        // 1. directly under sec-root
        // 2. directly under a namespace
        if (g_state.root_namespace_level == pcs->work->level - 1 || is_namespace(parent)) {
            if (str_t_eq_cstr(basename, MARFS_SUBSPACES_NAME)) {
                // this is a subspaces dir. do not process, but still descend
                return NO_PROCESS_DIR;
            } else {
                // this is any other marfs dir. do not process, do not descend
                return NO_PROCESS_NO_DESCEND_DIR;
            }
        } else {
            return PROCESS_DIR;
        }
    }

    if (str_t_startswith(get_basename(parent), MARFS_PREFIX)) {
        // parent directory starts with MDAL_
        // check to see if it's even possible for this dir to be a marfs namespace
        // cases for this being a marfs namespace:
        // 1. grandparent is sec-root
        // 2. grandparent is a namespace
        const str_t grandparent = get_grandparent(path);
        if (g_state.root_namespace_level == pcs->work->level - 2 || is_namespace(grandparent)) {
            // it's possible for this to be a marfs namespace. check the marfs config to be sure that it is currently
            // active
            if (is_namespace(path)) {
                return PROCESS_DIR;
            } else {
                // this dir is not listed in the marfs config so do not process it or descend
                return NO_PROCESS_NO_DESCEND_DIR;
            }
        }
    }

    return PROCESS_DIR;
}

// marfs_ctx is used for database init and cleanup
struct marfs_ctx {
    sqlite3* db;
    sqlite3_stmt* delete_entry;
    sqlite3_stmt* decrement_nlink;
    sqlite3_stmt* update_xattr_names;
    sqlite3_stmt* update_summary_name;
};

static void marfs_ctx_exit(void* ptr, void* plugin_user_data) {
    (void)ptr;

    struct marfs_ctx* ctx = (struct marfs_ctx*)plugin_user_data;
    if (!ctx) {
        return;
    }

    if (ctx->delete_entry) {
        sqlite3_finalize(ctx->delete_entry);
    }

    if (ctx->decrement_nlink) {
        sqlite3_finalize(ctx->decrement_nlink);
    }

    if (ctx->update_xattr_names) {
        sqlite3_finalize(ctx->update_xattr_names);
    }

    if (ctx->update_summary_name) {
        sqlite3_finalize(ctx->update_summary_name);
    }

    free(ctx);
}

static void* marfs_ctx_init(void* ptr) {
    PCS_t* pcs = (PCS_t*)ptr;
    if (!pcs || !pcs->db) {
        fprintf(stderr, "Error: marfs_ctx_init got invalid PCS/db\n");
        return NULL;
    }

    sqlite3* db = pcs->db;

    struct marfs_ctx* ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        fprintf(stderr, "Error: could not allocate marfs ctx\n");
        return NULL;
    }

    ctx->db = db;

    int rc;

    rc = sqlite3_prepare_v2(db, "DELETE FROM entries WHERE name = ?1;", -1, &ctx->delete_entry, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: prepare DELETE failed: %s\n", sqlite3_errmsg(db));
        marfs_ctx_exit(ptr, ctx);
        return NULL;
    }

    rc = sqlite3_prepare_v2(db,
                            "UPDATE entries "
                            "SET nlink = CASE WHEN nlink > 0 THEN nlink - 1 ELSE 0 END "
                            "WHERE name = ?1;",
                            -1, &ctx->decrement_nlink, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: prepare decrement nlink failed: %s\n", sqlite3_errmsg(db));
        marfs_ctx_exit(ptr, ctx);
        return NULL;
    }

    rc = sqlite3_prepare_v2(db,
                            "UPDATE entries "
                            "SET xattr_names = ?2 "
                            "WHERE name = ?1;",
                            -1, &ctx->update_xattr_names, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: prepare xattr update failed: %s\n", sqlite3_errmsg(db));
        marfs_ctx_exit(ptr, ctx);
        return NULL;
    }

    rc = sqlite3_prepare_v2(db,
                            "UPDATE summary "
                            "SET name = ?2 "
                            "WHERE name = ?1;",
                            -1, &ctx->update_summary_name, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: prepare summary update failed: %s\n", sqlite3_errmsg(db));
        marfs_ctx_exit(ptr, ctx);
        return NULL;
    }

    return ctx;
}

// filter out the marfs specific xattrs and rebuild the blob to update the gufi db
static sqlite3_int64 build_xattr_names_filtered(struct entry_data* ed, const char* remove_name, char* out,
                                                size_t out_sz) {
    size_t w = 0;

    for (size_t i = 0; i < ed->xattrs.count; i++) {
        const struct xattr* x = &ed->xattrs.pairs[i];

        if (strcmp(x->name, remove_name) == 0) {
            continue;
        }

        const size_t nmlen = x->name_len;

        // need space for name + delimiter
        if (w + nmlen + 1 > out_sz) {
            break;
        }

        memcpy(out + w, x->name, nmlen);
        w += nmlen;

        out[w++] = XATTRDELIM;
    }

    return (sqlite3_int64)w;
}

// marfs_process_file removes any marfs specific files from the gufi db, decrements every file nlink by 1, and removes
// any marfs specific xattrs
static void marfs_process_file(void* ptr, void* user_data) {
    PCS_t* pcs = (PCS_t*)ptr;
    struct entry_data* ed = pcs->ed;
    struct marfs_ctx* ctx = (struct marfs_ctx*)user_data;

    if (!pcs || !ctx || !ctx->db || !ed || !pcs->work || !pcs->work->name) {
        return;
    }

    int rc;

    const str_t path = (str_t)REFSTR(pcs->work->name, strlen(pcs->work->name));
    const str_t basename = get_basename(path);

    if (!basename.data || basename.len == 0) {
        return;
    }

    // remove mdal specific files
    if (basename.len >= strlen(MARFS_PREFIX) && str_t_startswith(basename, MARFS_PREFIX)) {
        // this files starts with MDAL_

        // determine if this is actual a marfs directory or a user dir
        // cases for being a marfs dir:
        // 1. directly under sec-root
        // 2. directly under a namespace

        const str_t parent = get_parent(path);

        if (g_state.root_namespace_level == pcs->work->level - 1 || is_namespace(parent)) {
            // this is a marfs file. remove it from the database

            sqlite3_stmt* stmt = ctx->delete_entry;

            sqlite3_reset(stmt);
            sqlite3_clear_bindings(stmt);

            rc = sqlite3_bind_text(stmt, 1, basename.data, (int)basename.len, SQLITE_TRANSIENT);
            if (rc != SQLITE_OK) {
                fprintf(stderr, "plugin: bind DELETE failed for %.*s: %s\n", (int)basename.len, basename.data,
                        sqlite3_errmsg(ctx->db));
                return;
            }

            rc = sqlite3_step(stmt);
            if (rc != SQLITE_DONE) {
                fprintf(stderr, "plugin: DELETE failed for %.*s: %s\n", (int)basename.len, basename.data,
                        sqlite3_errmsg(ctx->db));
            }

            sqlite3_reset(stmt);
            sqlite3_clear_bindings(stmt);
            return;
        }
    }

    // decrement nlink
    {
        sqlite3_stmt* stmt = ctx->decrement_nlink;

        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);

        rc = sqlite3_bind_text(stmt, 1, basename.data, (int)basename.len, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "plugin: bind nlink failed for %.*s: %s\n", (int)basename.len, basename.data,
                    sqlite3_errmsg(ctx->db));
            return;
        }

        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            fprintf(stderr, "plugin: decrement nlink failed for %.*s: %s\n", (int)basename.len, basename.data,
                    sqlite3_errmsg(ctx->db));
        }

        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
    }

    // remove marfs xattr
    {
        sqlite3_stmt* stmt = ctx->update_xattr_names;

        char xattr_names[MAXXATTR];
        sqlite3_int64 xlen = build_xattr_names_filtered(ed, MARFS_XATTR_NAME, xattr_names, sizeof(xattr_names));
        if (xlen < 0) xlen = 0;

        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);

        rc = sqlite3_bind_text(stmt, 1, basename.data, (int)basename.len, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "plugin: bind xattr name failed for %.*s: %s\n", (int)basename.len, basename.data,
                    sqlite3_errmsg(ctx->db));
            return;
        }

        rc = sqlite3_bind_blob64(stmt, 2, xattr_names, (sqlite3_uint64)xlen, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "plugin: bind xattr_names failed for %.*s: %s\n", (int)basename.len, basename.data,
                    sqlite3_errmsg(ctx->db));
            return;
        }

        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            fprintf(stderr, "plugin: update xattr_names failed for %.*s: %s\n", (int)basename.len, basename.data,
                    sqlite3_errmsg(ctx->db));
        }

        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
    }
}

// marfs_process_dir renames the root namespace in the gufi db summary table to match the mountpoint in the marfs
// config. The actual dir gets renamed in cleanup_marfs_index
static void marfs_process_dir(void* ptr, void* user_data) {
    PCS_t* pcs = (PCS_t*)ptr;
    struct marfs_ctx* ctx = (struct marfs_ctx*)user_data;

    if (!pcs || !pcs->work || !pcs->work->name || !ctx || !ctx->db) {
        return;
    }

    // determine if this is the root namespace dir
    if (g_state.root_namespace_level == pcs->work->level && str_t_eq_cstr(g_state.root_namespace, pcs->work->name)) {
        // rename this to the marfs mountpoint in the database
        const str_t path = (str_t)REFSTR(pcs->work->name, strlen(pcs->work->name));
        const str_t basename = get_basename(path);
        const str_t mountpoint = get_basename(g_state.marfs_mountpoint);

        sqlite3_stmt* stmt = ctx->update_summary_name;
        int rc;

        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);

        // old name
        rc = sqlite3_bind_text(stmt, 1, basename.data, (int)basename.len, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "plugin: bind summary name failed for %.*s: %s\n", (int)basename.len, basename.data,
                    sqlite3_errmsg(ctx->db));
            sqlite3_reset(stmt);
            sqlite3_clear_bindings(stmt);
            return;
        }

        // new name
        rc = sqlite3_bind_text(stmt, 2, mountpoint.data, (int)mountpoint.len, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "plugin: bind new summary name failed for %.*s: %s\n", (int)mountpoint.len, mountpoint.data,
                    sqlite3_errmsg(ctx->db));
            sqlite3_reset(stmt);
            sqlite3_clear_bindings(stmt);
            return;
        }

        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            fprintf(stderr, "plugin: update summary failed for %.*s: %s\n", (int)basename.len, basename.data,
                    sqlite3_errmsg(ctx->db));
        }

        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
    }
}

// marfs_indexing_global_exit cleans up the marfs index tree and cleans up the global state
static void marfs_indexing_global_exit(void* global) {
    (void)global;

    if (cleanup_marfs_index() != 0) {
        fprintf(stderr, "Warning: cleanup_marfs_index failed\n");
    }
    marfs_plugin_cleanup();
}

struct plugin_operations GUFI_PLUGIN_SYMBOL = {
    .type = PLUGIN_INDEX,
    .global_init = marfs_indexing_global_init,
    .ctx_init = marfs_ctx_init,
    .pre_process_dir = marfs_pre_processing_dir,
    .process_dir = marfs_process_dir,
    .process_file = marfs_process_file,
    .ctx_exit = marfs_ctx_exit,
    .global_exit = marfs_indexing_global_exit,
};
