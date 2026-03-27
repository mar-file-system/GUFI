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

/*
 * MarFS GUFI indexing plugin
 *
 * This plugin adapts a MarFS MDAL-sec-root tree so GUFI can index it in a
 * useful way, while hiding MarFS specific implementation details from the
 * final index.
 *
 * High level behavior:
 *
 * 1. Reads the MarFS config specified by MARFS_CONFIG_PATH and discovers all
 *    configured namespaces under the selected root namespace.
 *
 * 2. Builds a sorted list of namespace pairs:
 *      - namespace:        the real namespace path under sec-root
 *      - index_namespace:  the corresponding path GUFI will see in the
 *                          temporary index tree
 *
 * 3. Before indexing begins, rewrites the on-disk index layout to match the
 *    MarFS namespace structure expected by this plugin. In particular, it
 *    restores MDAL_subspaces directories and renames the indexed root from
 *    the MarFS mountpoint back to the configured root namespace so repeated
 *    indexing runs work correctly.
 *
 * 4. During traversal, filters out MarFS internal directories and files
 *    (such as MDAL_* entries) so GUFI does not index MarFS metadata as user
 *    content. It also skips namespaces that exist on disk but are no longer
 *    present in the current MarFS config.
 *
 * 5. While processing files, removes MarFS specific database entries,
 *    decrements link counts to account for MarFS metadata links, and strips
 *    MarFS xattrs from the stored xattr list.
 *
 * 6. While processing directories, renames the indexed root namespace in the
 *    GUFI summary table so the final index presents the MarFS mountpoint name
 *    instead of the raw sec-root namespace name.
 *
 * 7. After indexing completes, restores the index tree into a cleaner final
 *    form by moving namespace directories up out of MDAL_subspaces, removing
 *    empty MDAL_subspaces directories, and renaming the indexed root back to
 *    the MarFS mountpoint basename.
 *
 * build instruction:
 *
 * cd contrib
 *
 * gcc -g -c -fPIC plugins/marfs_plugin.c \
 *   -I../include \
 *   -I../build/deps/sqlite3/include \
 *   -I/path/to/marfs/install/include \
 *   -I/path/to/marfs/src/marfs/src \
 *   -I/usr/include/libxml2
 *
 * gcc -shared -o marfs_plugin.so marfs_plugin.o \
 *   ../build/src/libGUFI.a \
 *   -L/path/to/marfs/install/lib \
 *   -lmarfs \
 *   -lxml2 \
 *   -Wl,-rpath,/path/to/marfs/install/lib
 *
 * run example:
 * MARFS_CONFIG_PATH=/path/to/marfs/install/etc/marfs-config.xml \
 *   ./build/src/gufi_dir2index \
 *   --plugin ./contrib/marfs_plugin.so \
 *   /path/to/mdal-root/sec-root /home/$USER/gufi-index-dir
*/

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* marfs */
#include "config/config.h"
#include "hash/hash.h"

#include "bf.h"
#include "plugin.h"
#include "str.h"
#include "utils.h"
#include "xattrs.h"

static const char MARFS_PREFIX[] = "MDAL_";
static const size_t MARFS_PREFIX_LEN = sizeof(MARFS_PREFIX) - 1;
static const char MARFS_SUBSPACES_NAME[] = "MDAL_subspaces";
static const size_t MARFS_SUBSPACES_NAME_LEN = sizeof(MARFS_SUBSPACES_NAME) - 1;
static const char MARFS_XATTR_NAME[] = "user.MDAL_MARFS-FILE";
static const size_t MARFS_XATTR_NAME_LEN = sizeof(MARFS_XATTR_NAME) - 1;
static const size_t ROOT_NAMESPACE_LEVEL = 0;
static const mode_t MARFS_DIR_MODE = 0701;

static const char* MARFS_CONFIG_ENV = "MARFS_CONFIG_PATH";

typedef struct {
    str_t namespace;
    str_t index_namespace;
} namespace_pair;

// used for sorting namespaces
static int cmp_str_t_ptr(const void* a, const void* b) {
    const namespace_pair* pa = (const namespace_pair*)a;
    const namespace_pair* pb = (const namespace_pair*)b;
    return str_cmp(&pa->namespace, &pb->namespace);
}

// sort namespace paths
static void sort_namespace_pairs(namespace_pair* namespace_list, size_t namespace_count) {
    qsort(namespace_list, namespace_count, sizeof *namespace_list, cmp_str_t_ptr);
}

// returns the basename of a path
static str_t get_basename(str_t path) {
    if (!path.data || path.len == 0) return (str_t)REFSTR(NULL, 0);

    size_t len = path.len;

    // trim trailing slashes except root
    while (len > 1 && path.data[len - 1] == '/') len--;

    size_t i = len;
    while (i > 0 && path.data[i - 1] != '/') i--;

    return (str_t)REFSTR(path.data + i, len - i);
}

// returns the parent of a path
static str_t get_parent(str_t path) {
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

// join two str_t file paths together
static char* join_path(str_t lhs, str_t rhs) {
    size_t len = lhs.len + 1 + rhs.len + 1;
    char* out = malloc(len);
    if (!out) return NULL;

    snprintf(out, len, "%.*s/%.*s", (int)lhs.len, lhs.data, (int)rhs.len, rhs.data);
    return out;
}

// check if str_t starts with a prefix
static int starts_with_mdal(str_t s) {
    if (!s.data || s.len < MARFS_PREFIX_LEN) return 0;

    return strncmp(s.data, MARFS_PREFIX, MARFS_PREFIX_LEN) == 0;
}

// check if a str_t is equal to a char*
static int str_t_eq_cstr(str_t s, const char* cstr, size_t cstr_len) {
    if (!s.data || s.len != cstr_len) return 0;

    return strncmp(s.data, cstr, cstr_len) == 0;
}

struct marfs_plugin {
    namespace_pair* namespaces;
    size_t namespaces_count;

    str_t index_parent;  // actual index is placed at <index parent>/$(basename <src>)
    str_t marfs_mountpoint;
    str_t root_namespace;

    marfs_config* marfs_cfg;
};

static struct marfs_plugin g_state;

static void marfs_plugin_cleanup(void) {
    if (g_state.namespaces) {
        for (size_t i = 0; i < g_state.namespaces_count; i++) {
            str_free_existing(&g_state.namespaces[i].namespace);
            str_free_existing(&g_state.namespaces[i].index_namespace);
        }
        free(g_state.namespaces);
    }

    str_free_existing(&g_state.index_parent);
    str_free_existing(&g_state.marfs_mountpoint);
    str_free_existing(&g_state.root_namespace);

    g_state.namespaces = NULL;
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

// retrieve the namespace paths from the marfs config and build namespace pairs
static int collect_ns_paths(marfs_ns* ns, str_t parent_path, namespace_pair* paths, size_t* index, str_t root_namespace,
                            str_t index_parent, str_t rn_base) {
    if (!ns || !paths || !index || !root_namespace.data || !index_parent.data || !rn_base.data) return -1;

    for (size_t i = 0; i < ns->subnodecount; i++) {
        HASH_NODE* hn = &ns->subnodes[i];
        marfs_ns* child = (marfs_ns*)hn->content;

        if (!hn->name || !child) continue;

        str_t path = {0};
        const size_t name_len = strlen(hn->name);

        if (parent_path.len == 0) {
            const size_t path_len = name_len;

            if (!str_alloc_existing(&path, path_len)) return -1;

            snprintf(path.data, path_len + 1, "%s", hn->name);
        } else {
            // add an MDAL_subspaces dir into the path
            const size_t path_len = parent_path.len + 1 + MARFS_SUBSPACES_NAME_LEN + 1 + name_len;

            if (!str_alloc_existing(&path, path_len)) return -1;

            snprintf(path.data, path_len + 1, "%.*s/%s/%s", (int)parent_path.len, parent_path.data,
                     MARFS_SUBSPACES_NAME, hn->name);
        }

        // build namespace and index namespace paths
        {
            namespace_pair* pair = &paths[*index];
            const size_t namespace_len = root_namespace.len + 1 + MARFS_SUBSPACES_NAME_LEN + 1 + path.len;
            const size_t index_namespace_len =
                index_parent.len + 1 + rn_base.len + 1 + MARFS_SUBSPACES_NAME_LEN + 1 + path.len;

            if (!str_alloc_existing(&pair->namespace, namespace_len)) {
                str_free_existing(&path);
                return -1;
            }

            snprintf(pair->namespace.data, namespace_len + 1, "%.*s/%s/%.*s", (int)root_namespace.len,
                     root_namespace.data, MARFS_SUBSPACES_NAME, (int)path.len, path.data);

            if (!str_alloc_existing(&pair->index_namespace, index_namespace_len)) {
                str_free_existing(&pair->namespace);
                str_free_existing(&path);
                return -1;
            }

            snprintf(pair->index_namespace.data, index_namespace_len + 1, "%.*s/%.*s/%s/%.*s", (int)index_parent.len,
                     index_parent.data, (int)rn_base.len, rn_base.data, MARFS_SUBSPACES_NAME, (int)path.len, path.data);
        }

        (*index)++;

        if (collect_ns_paths(child, path, paths, index, root_namespace, index_parent, rn_base) != 0) {
            (*index)--;
            str_free_existing(&paths[*index].namespace);
            str_free_existing(&paths[*index].index_namespace);
            str_free_existing(&path);
            return -1;
        }

        str_free_existing(&path);
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

    // check if MDAL_subspaces exists under the target dir
    len = snprintf(NULL, 0, "%s/%s", g_state.root_namespace.data, MARFS_SUBSPACES_NAME);
    if (len < 0) goto cleanup;

    path = malloc((size_t)len + 1);
    if (!path) goto cleanup;

    snprintf(path, (size_t)len + 1, "%s/%s", g_state.root_namespace.data, MARFS_SUBSPACES_NAME);

    if (stat(path, &st) != 0 || !S_ISDIR(st.st_mode)) goto cleanup;

    // check if one of the top namespaces is in the first MDAL_subspaces
    if (stat(g_state.namespaces[0].namespace.data, &st) != 0 || !S_ISDIR(st.st_mode)) goto cleanup;

    ret = 1;

cleanup:
    free(path);
    return ret;
}

// cleanup_marfs_index will go through each namesapce defined in the marfs config, move it up a directory, and delete
// the empty MDAL_subspaces that remains. It also removes any empty MDAL_subspaces that exist within the namespace as
// well as renaming the root directory to the basename of the specified moutnpoint in the marfs config
static int cleanup_marfs_index(void) {
    int ret = 0;

    // loop through our index namespaces and move them up a directory and delete the unecessary MDAL_subspaces
    for (size_t i = g_state.namespaces_count; i-- > 0;) {
        const str_t old = g_state.namespaces[i].index_namespace;

        const str_t basename = get_basename(old);
        const str_t parent = get_parent(old);
        const str_t grandparent = get_parent(parent);

        char* new_path = NULL;
        char* parent_path = NULL;
        char* child_subspaces = NULL;

        // check to see if there is an empty MDAL_subspaces within this namespace
        {
            size_t child_subspaces_len =
                old.len + 1 + MARFS_SUBSPACES_NAME_LEN + 1;  // old + "/" + MDAL_subspaces + NUL
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

        // move this namespace up a directory (into its grandparent)
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
    }

    free(mm_path);
    free(rn_path);

    return ret;
}

// revert_marfs_index will undo everything that's in cleanup_marfs_index. This allows for indexing to be run over an
// existing index tree without error
static int revert_marfs_index(void) {
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

    rename(mm_path, rn_path);

    free(mm_path);
    free(rn_path);

    // loop through our index namespaces and add a subspaces into them. then move them down into each subspace
    for (size_t i = 0; i < g_state.namespaces_count; i++) {
        const str_t old = g_state.namespaces[i].index_namespace;

        const str_t basename = get_basename(old);
        const str_t parent = get_parent(old);
        const str_t grandparent = get_parent(parent);

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

                if (mkdir(parent_path, MARFS_DIR_MODE) != 0) {
                    if (errno != ENOTEMPTY && errno != EEXIST) {
                        free(parent_path);
                        free(indexed_path);
                        continue;
                    }
                }

                free(parent_path);
            }
        }

        // move this namespace into the MDAL_subspaces we just made
        {
            if (rename(indexed_path, old.data) != 0) {
                fprintf(stderr, "rename('%s' -> '%s') failed: %s\n", indexed_path, old.data, strerror(errno));
                free(indexed_path);
                continue;
            }

            free(indexed_path);
        }
    }

    return ret;
}

// marfs_indexing_global_init reads the marfs config and adds the namespaces to a global state.
static int marfs_indexing_global_init(void* global) {
    struct input* in = (struct input*)global;
    pthread_mutex_t marfs_erasurelock = PTHREAD_MUTEX_INITIALIZER;

    size_t config_index = 0;
    int ret = -1;

    // We can only allow for a single dir to be indexed at a time. this is because we have no way to taking in multiple
    // marfs configs
    if (in->pos.argc != 2) {
        fprintf(stderr, "Error: Marfs plugin requires exactly 2 paths\n");
        return ret;
    }

    // add index parent and root namespace to global state
    INSTALL_STR(&g_state.index_parent, in->pos.argv[in->pos.argc - 1]);
    INSTALL_STR(&g_state.root_namespace, in->pos.argv[0]);

    char* marfs_config_path = getenv(MARFS_CONFIG_ENV);
    if (!marfs_config_path || marfs_config_path[0] == '\0') {
        fprintf(stderr, "Error: %s is not set\n", MARFS_CONFIG_ENV);
        goto cleanup;
    }

    errno = 0;
    g_state.marfs_cfg = config_init(marfs_config_path, &marfs_erasurelock);
    if (!g_state.marfs_cfg) {
        fprintf(stderr, "marfs config_init returned NULL (errno=%d: %s) (%s=%s)\n", e, strerror(e), MARFS_CONFIG_ENV,
                marfs_config_path);
        goto cleanup;
    }

    // add marfs config mountpoint to global state
    INSTALL_STR(&g_state.marfs_mountpoint, g_state.marfs_cfg->mountpoint);

    g_state.namespaces_count = count_ns_paths(g_state.marfs_cfg->rootns);

    if (g_state.namespaces_count == 0) {
        fprintf(stderr, "Error: no namespaces found in MarFS config\n");
        goto cleanup;
    }

    g_state.namespaces = calloc(g_state.namespaces_count, sizeof(namespace_pair));
    if (!g_state.namespaces) {
        fprintf(stderr, "Error: namespace array allocation failed\n");
        goto cleanup;
    }

    {
        const str_t rn_base = get_basename(g_state.root_namespace);
        const str_t empty = REFSTR("", 0);

        if (collect_ns_paths(g_state.marfs_cfg->rootns, empty, g_state.namespaces, &config_index,
                             g_state.root_namespace, g_state.index_parent, rn_base) != 0) {
            fprintf(stderr, "Error: collect_ns_paths failed\n");
            goto cleanup;
        }
    }

    if (config_index != g_state.namespaces_count) {
        fprintf(stderr, "Error: namespace count mismatch (expected %zu, got %zu)\n", g_state.namespaces_count,
                config_index);
        goto cleanup;
    }

    sort_namespace_pairs(g_state.namespaces, g_state.namespaces_count);

    if (!validate_sec_root()) {
        fprintf(stderr, "Error: provided dir is not the root of the parent marfs namespace\n");
        goto cleanup;
    }

    if (revert_marfs_index() != 0) {
        fprintf(stderr, "Error: revert_marfs_index failed\n");
        goto cleanup;
    }

    sqlite3_initialize();

    ret = 0;

cleanup:
    if (ret != 0) {
        marfs_plugin_cleanup();
    }

    return ret;
}

// simple check to determine if a provided file path is a namespace in the marfs config
static int is_namespace(str_t path) {
    if (!path.data) {
        return 0;
    }

    for (size_t i = 0; i < g_state.namespaces_count; i++) {
        str_t ns = g_state.namespaces[i].namespace;
        if (!ns.data) {
            continue;
        }

        if (ns.len != path.len) {
            continue;
        }

        if (strncmp(ns.data, path.data, ns.len) == 0) {
            return 1;
        }
    }

    return 0;
}

// marfs_pre_processing_dir checks if we're about to process a marfs specific directory (MDAL_reference, MDAL_subspaces)
// or a namespace that is no longer active in the marfs config
static plugin_process_action marfs_pre_processing_dir(void* ptr) {
    PCS_t* pcs = (PCS_t*)ptr;

    if (!pcs || !pcs->work || !pcs->work->name) {
        fprintf(stderr, "Error: gufi pcs is NULL\n");
        return PLUGIN_PROCESS_DIR;
    }

    const str_t path = (str_t)REFSTR(pcs->work->name, pcs->work->name_len);
    const str_t basename = get_basename(path);
    const str_t parent = get_parent(path);

    if (starts_with_mdal(basename)) {
        // this directory starts with MDAL_

        // determine if this is a marfs directory or a user dir.
        // cases for being a marfs dir:
        // 1. directly under sec-root
        // 2. directly under a namespace
        if (ROOT_NAMESPACE_LEVEL == pcs->work->level - 1 || is_namespace(parent)) {
            if (str_t_eq_cstr(basename, MARFS_SUBSPACES_NAME, MARFS_SUBSPACES_NAME_LEN)) {
                // this is a subspaces dir. do not process, but still descend
                return PLUGIN_NO_PROCESS_DIR;
            } else {
                // this is any other marfs dir. do not process, do not descend
                return PLUGIN_NO_PROCESS_NO_DESCEND_DIR;
            }
        } else {
            return PLUGIN_PROCESS_DIR;
        }
    }

    if (starts_with_mdal(get_basename(parent))) {
        // parent directory starts with MDAL_
        // check to see if it's even possible for this dir to be a marfs namespace
        // cases for this being a marfs namespace:
        // 1. grandparent is sec-root
        // 2. grandparent is a namespace
        const str_t grandparent = get_parent(parent);
        if (ROOT_NAMESPACE_LEVEL == pcs->work->level - 2 || is_namespace(grandparent)) {
            // it's possible for this to be a marfs namespace. check the marfs config to be sure that it is currently
            // active
            if (is_namespace(path)) {
                return PLUGIN_PROCESS_DIR;
            } else {
                // this dir is not listed in the marfs config so do not process it or descend
                return PLUGIN_NO_PROCESS_NO_DESCEND_DIR;
            }
        }
    }

    return PLUGIN_PROCESS_DIR;
}

// marfs_ctx is used for database init and cleanup
struct marfs_ctx {
    sqlite3* db;
    sqlite3_stmt* delete_entry;
    sqlite3_stmt* decrement_nlink;
    sqlite3_stmt* update_xattr_names;
    sqlite3_stmt* remove_xattrs;
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

    if (ctx->remove_xattrs) {
        sqlite3_finalize(ctx->remove_xattrs);
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
                            "DELETE FROM xattrs_pwd "
                            "WHERE inode = ?2 AND name = ?1;",
                            -1, &ctx->remove_xattrs, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Error: remove xattrs failed: %s\n", sqlite3_errmsg(db));
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
static sqlite3_int64 build_xattr_names_filtered(struct entry_data* ed, char* out, size_t out_sz) {
    size_t w = 0;

    for (size_t i = 0; i < ed->xattrs.count; i++) {
        const struct xattr* x = &ed->xattrs.pairs[i];

        if ((x->name_len == MARFS_XATTR_NAME_LEN) && (strncmp(x->name, MARFS_XATTR_NAME, MARFS_XATTR_NAME_LEN) == 0)) {
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
    sqlite3* db = pcs->db;
    struct entry_data* ed = pcs->ed;
    struct marfs_ctx* ctx = (struct marfs_ctx*)user_data;

    if (!pcs || !ctx || !ctx->db || !ed || !pcs->work || !pcs->work->name) {
        return;
    }

    int rc;

    const str_t path = (str_t)REFSTR(pcs->work->name, pcs->work->name_len);
    const str_t basename = get_basename(path);

    if (!basename.data || basename.len == 0) {
        return;
    }

    // remove mdal specific files
    if (basename.len >= MARFS_PREFIX_LEN && starts_with_mdal(basename)) {
        // this files starts with MDAL_

        const str_t parent = get_parent(path);

        // determine if this is actual a marfs file or a user file
        // cases for being a marfs file:
        // 1. directly under sec-root
        // 2. directly under a namespace
        if (ROOT_NAMESPACE_LEVEL == pcs->work->level - 1 || is_namespace(parent)) {
            // this is a marfs file. remove it from the database

            sqlite3_stmt* stmt = ctx->delete_entry;

            sqlite3_reset(stmt);
            sqlite3_clear_bindings(stmt);

            rc = sqlite3_bind_text(stmt, 1, basename.data, (int)basename.len, SQLITE_STATIC);
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

        rc = sqlite3_bind_text(stmt, 1, basename.data, (int)basename.len, SQLITE_STATIC);
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

    // remove marfs xattr name from entries
    {
        sqlite3_stmt* stmt = ctx->update_xattr_names;

        char xattr_names[MAXXATTR];
        sqlite3_int64 xlen = build_xattr_names_filtered(ed, xattr_names, sizeof(xattr_names));
        if (xlen < 0) xlen = 0;

        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);

        rc = sqlite3_bind_text(stmt, 1, basename.data, (int)basename.len, SQLITE_STATIC);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "plugin: bind xattr name failed for %.*s: %s\n", (int)basename.len, basename.data,
                    sqlite3_errmsg(ctx->db));
            return;
        }

        rc = sqlite3_bind_blob64(stmt, 2, xattr_names, (sqlite3_uint64)xlen, SQLITE_STATIC);
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

    // remove marfs xattr from xattr_pwd
    {
        sqlite3_stmt* stmt = ctx->remove_xattrs;

        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);

        rc = sqlite3_bind_blob(stmt, 1, MARFS_XATTR_NAME, (int)MARFS_XATTR_NAME_LEN, SQLITE_STATIC);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "plugin: bind xattr remove name failed for %.*s: %s\n", (int)basename.len, basename.data,
                    sqlite3_errmsg(ctx->db));
            return;
        }

        rc = sqlite3_bind_int64(stmt, 2, (sqlite3_int64)pcs->work->statuso.st_ino);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "plugin: bind xattr remove inode failed for %lu: %s\n", pcs->work->statuso.st_ino,
                    sqlite3_errmsg(ctx->db));
            return;
        }

        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            fprintf(stderr, "plugin: remove xattrs_pwd failed for %.*s: %s\n", (int)basename.len, basename.data,
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
    if (ROOT_NAMESPACE_LEVEL == pcs->work->level &&
        str_t_eq_cstr(g_state.root_namespace, pcs->work->name, pcs->work->name_len)) {
        // rename this to the marfs mountpoint in the database
        const str_t path = (str_t)REFSTR(pcs->work->name, pcs->work->name_len);
        const str_t basename = get_basename(path);
        const str_t mountpoint = get_basename(g_state.marfs_mountpoint);

        sqlite3_stmt* stmt = ctx->update_summary_name;
        int rc;

        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);

        // old name
        rc = sqlite3_bind_text(stmt, 1, basename.data, (int)basename.len, SQLITE_STATIC);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "plugin: bind summary name failed for %.*s: %s\n", (int)basename.len, basename.data,
                    sqlite3_errmsg(ctx->db));
            sqlite3_reset(stmt);
            sqlite3_clear_bindings(stmt);
            return;
        }

        // new name
        rc = sqlite3_bind_text(stmt, 2, mountpoint.data, (int)mountpoint.len, SQLITE_STATIC);
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

    sqlite3_shutdown();

    if (cleanup_marfs_index() != 0) {
        fprintf(stderr, "Warning: cleanup_marfs_index failed\n");
    }

    // cleanup marfs config
    config_term(g_state.marfs_cfg);
    g_state.marfs_cfg = NULL;

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
