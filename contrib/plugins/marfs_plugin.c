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
 * MarFS GUFI indexing plugin
 *
 * This plugin adapts a MarFS MDAL-sec-root tree so GUFI can index it in a
 * useful way, while hiding MarFS specific implementation details from the
 * final index.
 *
 * High level behavior:
 *
 * 1. Reads the MarFS config specified by MARFS_CONFIG_PATH and the physical
 *    MDAL sec-root specified by MARFS_SEC_ROOT. The GUFI source may be the
 *    sec-root, a configured namespace, or a normal directory below one.
 *
 * 2. Builds the full physical path of every configured namespace from the
 *    configured sec-root. It separately builds temporary index paths only for
 *    configured namespaces below the selected GUFI source.
 *
 * 3. Before indexing begins, restores MDAL_subspaces directories in an
 *    existing index so GUFI can update the physical traversal layout.
 *
 * 4. During traversal, identifies MarFS metadata by checking whether its
 *    parent is the configured sec-root or a configured namespace. This avoids
 *    depending on GUFI traversal levels.
 *
 * 5. While processing files, removes MarFS xattrs, decrements link counts to
 *    account for MarFS metadata links, and hides MarFS implementation files.
 *
 * 6. After indexing completes, moves configured child namespaces out of
 *    MDAL_subspaces in the generated index. The index root is renamed to the
 *    MarFS mountpoint only when the selected source is the sec-root itself.
 *
 * build instruction:
 *
 * mkdir build && cd build
 *
 * cmake .. -DMARFS_PREFIX=/path/to/marfs/install/
 *
 * make
 *
 * (sudo) make install
 *
 * run example:
 * MARFS_CONFIG_PATH=/path/to/marfs/install/etc/marfs-config.xml \
 * MARFS_SEC_ROOT=/path/to/mdal-root/sec-root \
 *   src/gufi_dir2index \
 *   --plugin "GUFI_MARFS_PLUGIN:contrib/plugins/libmarfs_plugin.so" \
 *   /path/to/mdal-root/sec-root/MDAL_subspaces/ns1/subdir /home/$USER/gufi-index-dir
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
#include "include/config.h"

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
// marfs dir mode used for temporary additions of marfs specific directories during re-indexing
static const mode_t MARFS_DIR_MODE = S_IRWXU | S_IXOTH;

static const char* MARFS_CONFIG_ENV = "MARFS_CONFIG_PATH";
static const char* MARFS_SEC_ROOT_ENV = "MARFS_SEC_ROOT";

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

static int str_t_eq(str_t lhs, str_t rhs) {
    if (!lhs.data || !rhs.data || lhs.len != rhs.len) return 0;

    return strncmp(lhs.data, rhs.data, lhs.len) == 0;
}

// check whether path is root itself or is below root on a path-component boundary
static int path_is_at_or_below(str_t path, str_t root) {
    if (!path.data || !root.data || path.len < root.len) return 0;
    if (strncmp(path.data, root.data, root.len) != 0) return 0;
    if (path.len == root.len) return 1;

    if (root.len == 1 && root.data[0] == '/') return path.data[0] == '/';

    return path.data[root.len] == '/';
}

static str_t get_relative_path(str_t path, str_t root) {
    if (!path_is_at_or_below(path, root) || str_t_eq(path, root)) {
        return (str_t)REFSTR(NULL, 0);
    }

    if (root.len == 1 && root.data[0] == '/') {
        return (str_t)REFSTR(path.data + 1, path.len - 1);
    }

    return (str_t)REFSTR(path.data + root.len + 1, path.len - root.len - 1);
}

static str_t get_first_component(str_t path) {
    if (!path.data || path.len == 0) return (str_t)REFSTR(NULL, 0);

    size_t len = 0;
    while (len < path.len && path.data[len] != '/') len++;

    return (str_t)REFSTR(path.data, len);
}

struct marfs_plugin {
    namespace_pair* namespaces;
    size_t namespaces_count;

    str_t index_parent;  // actual index is placed at <index parent>/$(basename <src>)
    str_t marfs_mountpoint;
    str_t source;
    str_t sec_root;

    int source_is_sec_root;

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
    str_free_existing(&g_state.source);
    str_free_existing(&g_state.sec_root);

    g_state.namespaces = NULL;
    g_state.namespaces_count = 0;
    g_state.source_is_sec_root = 0;

    // cleanup marfs config
    config_term(g_state.marfs_cfg);
    g_state.marfs_cfg = NULL;
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

// retrieve the physical namespace paths from the marfs config
static int collect_ns_paths(marfs_ns* ns, str_t parent_namespace, namespace_pair* paths, size_t* index) {
    for (size_t i = 0; i < ns->subnodecount; i++) {
        HASH_NODE* hn = &ns->subnodes[i];
        marfs_ns* child = (marfs_ns*)hn->content;

        if (!hn->name || !child) continue;

        namespace_pair* pair = &paths[*index];
        const size_t name_len = strlen(hn->name);
        const size_t namespace_len = parent_namespace.len + 1 + MARFS_SUBSPACES_NAME_LEN + 1 + name_len;

        if (!str_alloc_existing(&pair->namespace, namespace_len)) return -1;

        snprintf(pair->namespace.data, namespace_len + 1, "%.*s/%s/%s", (int)parent_namespace.len,
                 parent_namespace.data, MARFS_SUBSPACES_NAME, hn->name);

        (*index)++;

        if (collect_ns_paths(child, pair->namespace, paths, index) != 0) return -1;
    }

    return 0;
}

// simple check to determine if a provided file path is a namespace in the marfs config
static int is_namespace(str_t path) {
    if (!path.data) return 0;

    for (size_t i = 0; i < g_state.namespaces_count; i++) {
        if (str_t_eq(g_state.namespaces[i].namespace, path)) return 1;
    }

    return 0;
}

// sec-root and configured namespaces are the directories that own MDAL metadata
static int is_mdal_owner(str_t path) { 
    return str_t_eq(path, g_state.sec_root) || is_namespace(path); 
}

static int is_mdal_subspaces_dir(str_t path) {
    if (!path.data) return 0;

    const str_t basename = get_basename(path);
    if (!str_t_eq_cstr(basename, MARFS_SUBSPACES_NAME, MARFS_SUBSPACES_NAME_LEN)) return 0;

    return is_mdal_owner(get_parent(path));
}

static int namespace_is_rewritten(str_t path) {
    for (size_t i = 0; i < g_state.namespaces_count; i++) {
        namespace_pair* pair = &g_state.namespaces[i];
        if (str_t_eq(pair->namespace, path)) {
            return pair->index_namespace.data && pair->index_namespace.len > 0;
        }
    }

    return 0;
}

// build only the index paths for configured namespaces that are below the selected source
static int build_index_namespace_paths(void) {
    const str_t source_base = get_basename(g_state.source);
    if (!source_base.data || source_base.len == 0) return -1;

    for (size_t i = 0; i < g_state.namespaces_count; i++) {
        namespace_pair* pair = &g_state.namespaces[i];

        // the selected source is already the index root and does not need to be moved
        if (str_t_eq(pair->namespace, g_state.source)) continue;

        const str_t relative = get_relative_path(pair->namespace, g_state.source);
        if (!relative.data || relative.len == 0) continue;

        const size_t index_namespace_len = g_state.index_parent.len + 1 + source_base.len + 1 + relative.len;

        if (!str_alloc_existing(&pair->index_namespace, index_namespace_len)) return -1;

        snprintf(pair->index_namespace.data, index_namespace_len + 1, "%.*s/%.*s/%.*s", (int)g_state.index_parent.len,
                 g_state.index_parent.data, (int)source_base.len, source_base.data, (int)relative.len, relative.data);
    }

    return 0;
}

// validate the configured sec-root and ensure the selected source is a usable path inside it
static int validate_source(void) {
    struct stat st;

    if (g_state.sec_root.data[0] != '/' || g_state.source.data[0] != '/') {
        fprintf(stderr, "Error: source and %s must both be absolute paths\n", MARFS_SEC_ROOT_ENV);
        return -1;
    }

    if (stat(g_state.sec_root.data, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Error: %s (%s) is not a directory\n", MARFS_SEC_ROOT_ENV, g_state.sec_root.data);
        return -1;
    }

    {
        const str_t subspaces_name = REFSTR(MARFS_SUBSPACES_NAME, MARFS_SUBSPACES_NAME_LEN);
        char* subspaces = join_path(g_state.sec_root, subspaces_name);

        if (!subspaces) {
            fprintf(stderr, "Error: could not build sec-root MDAL_subspaces path\n");
            return -1;
        }

        if (stat(subspaces, &st) != 0 || !S_ISDIR(st.st_mode)) {
            fprintf(stderr, "Error: %s (%s) does not contain %s\n", MARFS_SEC_ROOT_ENV, g_state.sec_root.data,
                    MARFS_SUBSPACES_NAME);
            free(subspaces);
            return -1;
        }

        free(subspaces);
    }

    if (stat(g_state.source.data, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Error: source (%s) is not a directory\n", g_state.source.data);
        return -1;
    }

    if (!path_is_at_or_below(g_state.source, g_state.sec_root)) {
        fprintf(stderr, "Error: source (%s) is not within %s (%s)\n", g_state.source.data, MARFS_SEC_ROOT_ENV,
                g_state.sec_root.data);
        return -1;
    }

    /*
     * Find the deepest configured namespace containing the source. Relative
     * to that owner, a leading MDAL_* component means the user selected an
     * internal MarFS implementation directory. Selecting the namespace itself
     * is valid because it becomes the root of the generated index.
     */
    str_t owner = g_state.sec_root;

    for (size_t i = 0; i < g_state.namespaces_count; i++) {
        const str_t ns = g_state.namespaces[i].namespace;
        if (ns.len > owner.len && path_is_at_or_below(g_state.source, ns)) owner = ns;
    }

    if (!str_t_eq(g_state.source, owner)) {
        const str_t relative = get_relative_path(g_state.source, owner);
        const str_t first = get_first_component(relative);

        if (starts_with_mdal(first)) {
            fprintf(stderr, "Error: source (%s) is inside MarFS internal directory %.*s\n", g_state.source.data,
                    (int)first.len, first.data);
            return -1;
        }
    }

    return 0;
}

// cleanup_marfs_index moves configured namespaces below the selected source out of
// MDAL_subspaces directories. When indexing sec-root, it also renames the index root
// to the basename of the MarFS mountpoint.
static int cleanup_marfs_index(void) {
    int ret = 0;

    // deepest namespaces must be moved before their parents
    for (size_t i = g_state.namespaces_count; i-- > 0;) {
        const str_t old = g_state.namespaces[i].index_namespace;
        if (!old.data || old.len == 0) continue;

        const str_t basename = get_basename(old);
        const str_t parent = get_parent(old);
        const str_t grandparent = get_parent(parent);

        char* new_path = NULL;
        char* parent_path = NULL;
        char* child_subspaces = NULL;

        // remove an empty MDAL_subspaces contained by this namespace
        {
            const size_t child_subspaces_len = old.len + 1 + MARFS_SUBSPACES_NAME_LEN + 1;
            child_subspaces = malloc(child_subspaces_len);
            if (!child_subspaces) {
                fprintf(stderr, "malloc failed for child_subspaces\n");
                ret = -1;
                continue;
            }

            snprintf(child_subspaces, child_subspaces_len, "%.*s/%s", (int)old.len, old.data, MARFS_SUBSPACES_NAME);

            if (rmdir(child_subspaces) != 0 && errno != ENOTEMPTY && errno != ENOENT) {
                fprintf(stderr, "rmdir('%s') failed: %s\n", child_subspaces, strerror(errno));
                ret = -1;
            }

            free(child_subspaces);
        }

        // move this namespace out of MDAL_subspaces
        {
            const size_t new_len = grandparent.len + 1 + basename.len + 1;
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
                if (errno != ENOENT) {
                    fprintf(stderr, "rename('%s' -> '%s') failed: %s\n", old.data, new_path, strerror(errno));
                    ret = -1;
                }

                free(new_path);
                continue;
            }

            free(new_path);
        }

        // remove the now-empty MDAL_subspaces directory
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

                if (rmdir(parent_path) != 0 && errno != ENOTEMPTY && errno != ENOENT) {
                    fprintf(stderr, "rmdir('%s') failed: %s\n", parent_path, strerror(errno));
                    ret = -1;
                }
            }

            free(parent_path);
        }
    }

    if (g_state.source_is_sec_root) {
        const str_t source_base = get_basename(g_state.source);
        const str_t mountpoint_base = get_basename(g_state.marfs_mountpoint);

        char* source_path = join_path(g_state.index_parent, source_base);
        char* mountpoint_path = join_path(g_state.index_parent, mountpoint_base);

        if (!source_path || !mountpoint_path) {
            fprintf(stderr, "join_path failed\n");
            free(source_path);
            free(mountpoint_path);
            return -1;
        }

        if (rename(source_path, mountpoint_path) != 0) {
            fprintf(stderr, "rename('%s' -> '%s') failed: %s\n", source_path, mountpoint_path, strerror(errno));
            free(source_path);
            free(mountpoint_path);
            return -1;
        }

        free(source_path);
        free(mountpoint_path);
    }

    return ret;
}

// revert_marfs_index restores the physical MDAL_subspaces layout in an existing
// index before GUFI updates it.
static int revert_marfs_index(void) {
    int ret = 0;

    if (g_state.source_is_sec_root) {
        const str_t mountpoint_base = get_basename(g_state.marfs_mountpoint);
        const str_t source_base = get_basename(g_state.source);

        char* mountpoint_path = join_path(g_state.index_parent, mountpoint_base);
        char* source_path = join_path(g_state.index_parent, source_base);

        if (!mountpoint_path || !source_path) {
            fprintf(stderr, "join_path failed\n");
            free(mountpoint_path);
            free(source_path);
            return -1;
        }

        if (rename(mountpoint_path, source_path) != 0) {
            // if we failed to rename the root ns, then don't worry about the rest of the namespaces
            if (errno != ENOENT) {
                fprintf(stderr, "rename('%s' -> '%s') failed: %s\n", mountpoint_path, source_path, strerror(errno));
                ret = -1;
            }
            free(mountpoint_path);
            free(source_path);
            return ret;
        }

        /*
         * ENOENT is expected on the first run. Continue restoring namespace
         * paths in case the source-named index root already exists from an
         * interrupted earlier run.
         */
        free(mountpoint_path);
        free(source_path);
    }

    // parent namespaces must be restored before nested namespaces
    for (size_t i = 0; i < g_state.namespaces_count; i++) {
        const str_t old = g_state.namespaces[i].index_namespace;
        if (!old.data || old.len == 0) continue;

        const str_t basename = get_basename(old);
        const str_t parent = get_parent(old);
        const str_t grandparent = get_parent(parent);

        char* indexed_path = NULL;
        char* parent_path = NULL;

        const size_t indexed_path_len = grandparent.len + 1 + basename.len + 1;
        indexed_path = malloc(indexed_path_len);
        if (!indexed_path) {
            fprintf(stderr, "malloc failed for indexed_path\n");
            ret = -1;
            continue;
        }

        if (grandparent.len == 1 && grandparent.data[0] == '/') {
            snprintf(indexed_path, indexed_path_len, "/%.*s", (int)basename.len, basename.data);
        } else {
            snprintf(indexed_path, indexed_path_len, "%.*s/%.*s", (int)grandparent.len, grandparent.data,
                     (int)basename.len, basename.data);
        }

        // create the namespace's MDAL_subspaces parent
        {
            if (parent.len > 1) {
                parent_path = malloc(parent.len + 1);
                if (!parent_path) {
                    fprintf(stderr, "malloc failed for parent_path\n");
                    free(indexed_path);
                    ret = -1;
                    continue;
                }

                snprintf(parent_path, parent.len + 1, "%.*s", (int)parent.len, parent.data);

                if (mkdir(parent_path, MARFS_DIR_MODE) != 0 && errno != EEXIST) {
                    fprintf(stderr, "mkdir('%s') failed: %s\n", parent_path, strerror(errno));
                    free(parent_path);
                    free(indexed_path);
                    ret = -1;
                    continue;
                }

                free(parent_path);
            }
        }

        if (rename(indexed_path, old.data) != 0) {
            // The namespace may not be present in an existing partial index.
            if (errno != ENOENT) {
                fprintf(stderr, "rename('%s' -> '%s') failed: %s\n", indexed_path, old.data, strerror(errno));
                ret = -1;
            }
        }

        free(indexed_path);
    }

    return ret;
}

// marfs_indexing_global_init reads the MarFS config and builds paths relative to
// the configured sec-root, independently of where GUFI starts indexing.
static int marfs_indexing_global_init(struct input* in) {
    pthread_mutex_t marfs_erasurelock = PTHREAD_MUTEX_INITIALIZER;

    size_t config_index = 0;
    int ret = -1;

    // We can only allow for a single dir to be indexed at a time. this is because we do not read multiple marfs configs
    if (in->pos.argc != 2) {
        fprintf(stderr, "Error: Marfs plugin requires exactly 2 paths\n");
        return ret;
    }

    if (INSTALL_STR(&g_state.index_parent, in->pos.argv[in->pos.argc - 1]) != 0 ||
        INSTALL_STR(&g_state.source, in->pos.argv[0]) != 0) {
        fprintf(stderr, "Error: could not store GUFI source or index path\n");
        goto cleanup;
    }

    char* sec_root = getenv(MARFS_SEC_ROOT_ENV);
    if (!sec_root || sec_root[0] == '\0') {
        fprintf(stderr, "Error: %s is not set\n", MARFS_SEC_ROOT_ENV);
        goto cleanup;
    }

    if (INSTALL_STR(&g_state.sec_root, sec_root) != 0) {
        fprintf(stderr, "Error: could not store %s\n", MARFS_SEC_ROOT_ENV);
        goto cleanup;
    }

    char* marfs_config_path = getenv(MARFS_CONFIG_ENV);
    if (!marfs_config_path || marfs_config_path[0] == '\0') {
        fprintf(stderr, "Error: %s is not set\n", MARFS_CONFIG_ENV);
        goto cleanup;
    }

    errno = 0;
    g_state.marfs_cfg = config_init(marfs_config_path, &marfs_erasurelock);
    if (!g_state.marfs_cfg) {
        fprintf(stderr, "marfs config_init returned NULL (errno=%d: %s) (%s=%s)\n", errno, strerror(errno),
                MARFS_CONFIG_ENV, marfs_config_path);
        goto cleanup;
    }

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

    if (collect_ns_paths(g_state.marfs_cfg->rootns, g_state.sec_root, g_state.namespaces, &config_index) != 0) {
        fprintf(stderr, "Error: collect_ns_paths failed\n");
        goto cleanup;
    }

    if (config_index != g_state.namespaces_count) {
        fprintf(stderr, "Error: namespace count mismatch (expected %zu, got %zu)\n", g_state.namespaces_count,
                config_index);
        goto cleanup;
    }

    sort_namespace_pairs(g_state.namespaces, g_state.namespaces_count);

    if (validate_source() != 0) goto cleanup;

    g_state.source_is_sec_root = str_t_eq(g_state.source, g_state.sec_root);

    if (build_index_namespace_paths() != 0) {
        fprintf(stderr, "Error: build_index_namespace_paths failed\n");
        goto cleanup;
    }

    // if (revert_marfs_index() != 0) {
    //     fprintf(stderr, "Error: revert_marfs_index failed\n");
    //     goto cleanup;
    // }

    sqlite3_initialize();

    ret = 0;

cleanup:
    if (ret != 0) marfs_plugin_cleanup();

    return ret;
}

// marfs_dir_action identifies MarFS implementation entries by their physical
// owner path instead of assuming sec-root is GUFI traversal level 0.
static plugin_dir_action marfs_dir_action(void* ptr) {
    PCS_t* pcs = ptr;

    const str_t path = REFSTR(pcs->work->name, pcs->work->name_len);
    const str_t basename = get_basename(path);
    const str_t parent = get_parent(path);

    if (starts_with_mdal(basename) && is_mdal_owner(parent)) {
        if (str_t_eq_cstr(basename, MARFS_SUBSPACES_NAME, MARFS_SUBSPACES_NAME_LEN)) {
            // hide MDAL_subspaces itself, but descend into configured child namespaces
            return PLUGIN_NO_PROCESS_DIR;
        }

        // hide all other MDAL implementation directories
        return PLUGIN_NO_PROCESS_NO_DESCEND_DIR;
    }

    if (is_mdal_subspaces_dir(parent)) {
        // Only namespaces present in the active config should be traversed.
        if (is_namespace(path)) return PLUGIN_PROCESS_DIR;

        return PLUGIN_NO_PROCESS_NO_DESCEND_DIR;
    }

    return PLUGIN_PROCESS_DIR;
}

// marfs_ctx is used for database init and cleanup
struct marfs_ctx {
    sqlite3* db;
    sqlite3_stmt* delete_entry;
    sqlite3_stmt* update_summary_name;
};

static void marfs_ctx_exit(void* ptr, void* plugin_user_data) {
    (void)ptr;

    struct marfs_ctx* ctx = plugin_user_data;
    if (!ctx) {
        return;
    }

    if (ctx->delete_entry) {
        sqlite3_finalize(ctx->delete_entry);
    }

    if (ctx->update_summary_name) {
        sqlite3_finalize(ctx->update_summary_name);
    }

    free(ctx);
}

static void* marfs_ctx_init(void* ptr) {
    PCS_t* pcs = ptr;
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

// marfs_pre_process_file removes MarFS xattrs, adjusts nlink, and hides MDAL files
// directly owned by sec-root or a configured namespace.
static plugin_file_action marfs_pre_process_file(void* ptr, void* user_data) {
    PCS_t* pcs = ptr;
    struct entry_data* ed = pcs->ed;
    (void)user_data;

    const str_t path = REFSTR(pcs->work->name, pcs->work->name_len);
    const str_t basename = get_basename(path);

    const size_t removed = xattr_remove(&ed->xattrs, MARFS_XATTR_NAME, MARFS_XATTR_NAME_LEN);

    if (removed > 0 && pcs->work->statuso.st_nlink > (nlink_t)0) {
        pcs->work->statuso.st_nlink--;
    }

    if (starts_with_mdal(basename) && is_mdal_owner(get_parent(path))) {
        return PLUGIN_NO_PROCESS_FILE;
    }

    return PLUGIN_PROCESS_FILE;
}

// Fix pinode only for configured namespaces whose MDAL_subspaces parent will be
// removed from this particular index.
static void marfs_pre_process_dir(void* ptr, void* user_data) {
    PCS_t* pcs = ptr;
    (void)user_data;

    const str_t path = REFSTR(pcs->work->name, pcs->work->name_len);

    if (!namespace_is_rewritten(path)) return;

    const str_t parent = get_parent(path);
    if (!is_mdal_subspaces_dir(parent)) return;

    const str_t grandparent = get_parent(parent);
    if (!grandparent.data || grandparent.len == 0) return;

    char* grandparent_path = malloc(grandparent.len + 1);
    if (!grandparent_path) {
        fprintf(stderr, "malloc failed for grandparent_path\n");
        return;
    }

    snprintf(grandparent_path, grandparent.len + 1, "%.*s", (int)grandparent.len, grandparent.data);

    struct stat st;
    if (stat(grandparent_path, &st) == 0) {
        pcs->work->pinode = (long long int)st.st_ino;
    } else {
        fprintf(stderr, "plugin: stat('%s') failed: %s\n", grandparent_path, strerror(errno));
    }

    free(grandparent_path);
}

// marfs_process_dir renames the root namespace in the gufi db summary table to match the mountpoint in the marfs
// config. The actual dir gets renamed in cleanup_marfs_index. It also fixes pinode values for directories whose
// parent is an MDAL_subspaces directory that will be removed from the final index.
static void marfs_post_process_dir(void* ptr, void* user_data) {
    PCS_t* pcs = ptr;
    struct marfs_ctx* ctx = user_data;

    const str_t path = REFSTR(pcs->work->name, pcs->work->name_len);
    const str_t basename = get_basename(path);

    // Only a sec-root index is renamed to the MarFS mountpoint.
    if (g_state.source_is_sec_root && str_t_eq(path, g_state.source)) {
        // Rename root namespace to mountpoint in database
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
static void marfs_indexing_global_exit(struct input* in) {
    (void)in;

    sqlite3_shutdown();

    if (cleanup_marfs_index() != 0) {
        fprintf(stderr, "Warning: cleanup_marfs_index failed\n");
    }

    marfs_plugin_cleanup();
}

struct plugin_operations GUFI_MARFS_PLUGIN = {
    .type = PLUGIN_INDEX,
    .global_init = marfs_indexing_global_init,
    .thread_init = NULL,
    .dir_action = marfs_dir_action,
    .ctx_init = marfs_ctx_init,
    .stat_file = NULL,
    .pre_process_dir = marfs_pre_process_dir,
    .post_process_dir = marfs_post_process_dir,
    .pre_process_file = marfs_pre_process_file,
    .post_process_file = NULL,
    .ctx_exit = marfs_ctx_exit,
    .thread_exit = NULL,
    .global_exit = marfs_indexing_global_exit,
};
