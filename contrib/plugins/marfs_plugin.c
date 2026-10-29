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
 * 1. Reads the MarFS config specified by MARFS_CONFIG_PATH and discovers all
 *    configured namespaces under the MDAL sec-root specified by
 *    MARFS_SEC_ROOT. The selected GUFI source may be the sec-root, a
 *    configured namespace, or a normal directory below one.
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
 *    instead of the raw sec-root namespace name when indexing from sec-root.
 *
 * 7. After indexing completes, restores the index tree into a cleaner final
 *    form by moving namespace directories up out of MDAL_subspaces, removing
 *    empty MDAL_subspaces directories, and renaming the indexed root back to
 *    the MarFS mountpoint basename.
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
static const size_t ROOT_NAMESPACE_LEVEL = 0;
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

// return a path with trailing slashes removed, except for "/"
static str_t trim_trailing_slashes(str_t path) {
    while (path.len > 1 && path.data[path.len - 1] == '/') {
        path.len--;
    }

    return path;
}

// returns the basename of a path
static str_t get_basename(str_t path) {
    if (!str_exists(&path)) return (str_t)REFSTR(NULL, 0);

    const size_t parent_len = dirname_len(path.data, path.len);

    return (str_t)REFSTR(path.data + parent_len, path.len - parent_len);
}

// returns the parent of a path
static str_t get_parent(str_t path) {
    if (!str_exists(&path)) return (str_t)REFSTR(NULL, 0);

    if (path.len == 1 && path.data[0] == '/') {
        return path;
    }

    size_t len = dirname_len(path.data, path.len);
    if (len == 0) return (str_t)REFSTR(NULL, 0);

    // dirname_len includes the trailing '/'
    if (len > 1) len--;

    return (str_t)REFSTR(path.data, len);
}

static void trim_trailing_slashes_in_place(str_t* path) {
    if (!str_exists(path)) return;

    const size_t len = trailing_non_match_index(path->data, path->len, "/", 1);
    path->len = len ? len : 1;
    path->data[path->len] = '\0';
}

// join two str_t file paths together
static char* join_path(str_t lhs, str_t rhs) {
    size_t len = lhs.len + 1 + rhs.len + 1;
    char* out = malloc(len);
    if (!out) return NULL;

    SNFORMAT_S(out, len, 3, lhs.data, lhs.len, "/", (size_t)1, rhs.data, rhs.len);
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

static int path_eq(str_t lhs, str_t rhs) {
    if (lhs.len != rhs.len) return 0;

    return strncmp(lhs.data, rhs.data, rhs.len) == 0;
}

// check whether path is root itself or is below root on a path-component boundary
static int path_is_at_or_below(str_t path, str_t root) {
    if (path.len < root.len) return 0;
    if (strncmp(path.data, root.data, root.len) != 0) return 0;
    if (path.len == root.len) return 1;

    if (root.len == 1 && root.data[0] == '/') return path.data[0] == '/';

    return path.data[root.len] == '/';
}

static str_t get_relative_path(str_t path, str_t root) {
    if (!path_is_at_or_below(path, root) || path_eq(path, root)) {
        return (str_t)REFSTR(NULL, 0);
    }

    if (root.len == 1 && root.data[0] == '/') {
        return (str_t)REFSTR(path.data + 1, path.len - 1);
    }

    return (str_t)REFSTR(path.data + root.len + 1, path.len - root.len - 1);
}

struct marfs_plugin {
    namespace_pair* namespaces;
    size_t namespaces_count;

    str_t index_parent;  // actual index is placed at <index parent>/$(basename <src>)
    str_t marfs_mountpoint;
    str_t root_namespace;
    str_t source;

    int source_is_root;
    str_t source_base;

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
    str_free_existing(&g_state.source);

    g_state.namespaces = NULL;
    g_state.namespaces_count = 0;

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

// retrieve the namespace paths from the marfs config and build namespace pairs
static int collect_ns_paths(marfs_ns* ns, str_t parent_path, namespace_pair* paths, size_t* index,
                            str_t root_namespace) {
    if (!ns || !paths || !index || !root_namespace.data) return -1;

    for (size_t i = 0; i < ns->subnodecount; i++) {
        HASH_NODE* hn = &ns->subnodes[i];
        marfs_ns* child = (marfs_ns*)hn->content;

        if (!hn->name || !child) continue;

        str_t path = {0};
        const size_t name_len = strlen(hn->name);

        if (parent_path.len == 0) {
            const size_t path_len = name_len;

            if (!str_alloc_existing(&path, path_len)) return -1;

            SNFORMAT_S(path.data, path_len + 1, 1, hn->name, name_len);
        } else {
            // add an MDAL_subspaces dir into the path
            const size_t path_len = parent_path.len + 1 + MARFS_SUBSPACES_NAME_LEN + 1 + name_len;

            if (!str_alloc_existing(&path, path_len)) return -1;

            SNFORMAT_S(path.data, path_len + 1, 5, parent_path.data, parent_path.len, "/", (size_t)1,
                       MARFS_SUBSPACES_NAME, MARFS_SUBSPACES_NAME_LEN, "/", (size_t)1, hn->name, name_len);
        }

        // build namespace path
        {
            namespace_pair* pair = &paths[*index];
            const size_t namespace_len = root_namespace.len + 1 + MARFS_SUBSPACES_NAME_LEN + 1 + path.len;

            if (!str_alloc_existing(&pair->namespace, namespace_len)) {
                str_free_existing(&path);
                return -1;
            }

            SNFORMAT_S(pair->namespace.data, namespace_len + 1, 5, root_namespace.data, root_namespace.len, "/",
                       (size_t)1, MARFS_SUBSPACES_NAME, MARFS_SUBSPACES_NAME_LEN, "/", (size_t)1, path.data, path.len);
        }

        (*index)++;

        if (collect_ns_paths(child, path, paths, index, root_namespace) != 0) {
            (*index)--;
            str_free_existing(&paths[*index].namespace);
            str_free_existing(&path);
            return -1;
        }

        str_free_existing(&path);
    }

    return 0;
}

// build index paths only for configured namespaces below the selected source
static int build_index_namespace_paths(void) {
    for (size_t i = 0; i < g_state.namespaces_count; i++) {
        namespace_pair* pair = &g_state.namespaces[i];

        // the selected source is already the index root and does not need to be moved
        if (path_eq(pair->namespace, g_state.source)) continue;

        const str_t relative = get_relative_path(pair->namespace, g_state.source);
        if (!str_exists(&relative)) continue;

        const size_t index_namespace_len = g_state.index_parent.len + 1 + g_state.source_base.len + 1 + relative.len;

        if (!str_alloc_existing(&pair->index_namespace, index_namespace_len)) return -1;

        SNFORMAT_S(pair->index_namespace.data, index_namespace_len + 1, 5, g_state.index_parent.data,
                   g_state.index_parent.len, "/", (size_t)1, g_state.source_base.data, g_state.source_base.len, "/",
                   (size_t)1, relative.data, relative.len);
    }

    return 0;
}

// validate that we are actually pointed at the root namespace of a marfs-mdal (sec-root).
// There should be an MDAL_subspaces directly under the root namespace
// then there should be our first namespaces right under the MDAL_subspaces
static int validate_sec_root(void) {
    struct stat st;
    int ret = 0;
    char* path = NULL;

    if (!g_state.root_namespace.data || g_state.root_namespace.len == 0) goto cleanup;
    if (!g_state.namespaces || g_state.namespaces_count == 0) goto cleanup;

    qsort(g_state.namespaces, g_state.namespaces_count, sizeof(*g_state.namespaces), cmp_str_t_ptr);

    // check if MDAL_subspaces exists under the target dir
    const size_t len = g_state.root_namespace.len + 1 + MARFS_SUBSPACES_NAME_LEN;

    path = malloc(len + 1);
    if (!path) goto cleanup;

    SNFORMAT_S(path, len + 1, 3, g_state.root_namespace.data, g_state.root_namespace.len, "/", (size_t)1,
               MARFS_SUBSPACES_NAME, MARFS_SUBSPACES_NAME_LEN);

    if (stat(path, &st) != 0 || !S_ISDIR(st.st_mode)) goto cleanup;

    // check if one of the top namespaces is in the first MDAL_subspaces
    if (stat(g_state.namespaces[0].namespace.data, &st) != 0 || !S_ISDIR(st.st_mode)) goto cleanup;

    ret = 1;

cleanup:
    free(path);
    return ret;
}

// validate that the selected source is a usable path within sec-root
static int validate_source(void) {
    struct stat st;

    if (g_state.root_namespace.data[0] != '/' || g_state.source.data[0] != '/') {
        fprintf(stderr, "Error: source and %s must both be absolute paths\n", MARFS_SEC_ROOT_ENV);
        return 0;
    }

    if (stat(g_state.source.data, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Error: source (%s) is not a directory\n", g_state.source.data);
        return 0;
    }

    if (!path_is_at_or_below(g_state.source, g_state.root_namespace)) {
        fprintf(stderr, "Error: source (%s) is not within %s (%s)\n", g_state.source.data, MARFS_SEC_ROOT_ENV,
                g_state.root_namespace.data);
        return 0;
    }

    /*
     * Find the deepest configured namespace containing the source. Relative
     * to that owner, a leading MDAL_* component means the user selected an
     * internal MarFS implementation directory.
     */
    str_t owner = g_state.root_namespace;

    for (size_t i = 0; i < g_state.namespaces_count; i++) {
        const str_t ns = g_state.namespaces[i].namespace;
        if (ns.len > owner.len && path_is_at_or_below(g_state.source, ns)) owner = ns;
    }

    if (!path_eq(g_state.source, owner)) {
        const str_t relative = get_relative_path(g_state.source, owner);
        if (starts_with_mdal(relative)) {
            fprintf(stderr, "Error: source (%s) is inside a MarFS internal directory\n", g_state.source.data);
            return 0;
        }
    }

    return 1;
}

// Returns 0 on success, 1 if missing and allowed, -1 on error.
static int rename_index_root(str_t from_base, str_t to_base, int allow_missing) {
    char* from = join_path(g_state.index_parent, from_base);
    char* to = join_path(g_state.index_parent, to_base);

    if (!from || !to) {
        fprintf(stderr, "join_path failed\n");
        free(from);
        free(to);
        return -1;
    }

    int ret = 0;

    if (rename(from, to) != 0) {
        if (allow_missing && errno == ENOENT) {
            ret = 1;
        } else {
            fprintf(stderr, "rename('%s' -> '%s') failed: %s\n", from, to, strerror(errno));
            ret = -1;
        }
    }

    free(from);
    free(to);
    return ret;
}

// cleanup_marfs_index will go through each namespace defined in the marfs config, move it up a directory, and delete
// the empty MDAL_subspaces that remains. It also removes any empty MDAL_subspaces that exist within the namespace as
// well as renaming the sec-root index to the basename of the specified mountpoint in the marfs config
static int cleanup_marfs_index(void) {
    int ret = 0;

    // loop through our index namespaces and move them up a directory and delete the unecessary MDAL_subspaces
    for (size_t i = g_state.namespaces_count; i-- > 0;) {
        const str_t old = g_state.namespaces[i].index_namespace;
        if (!str_exists(&old)) continue;

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

            SNFORMAT_S(child_subspaces, child_subspaces_len, 3, old.data, old.len, "/", (size_t)1, MARFS_SUBSPACES_NAME,
                       MARFS_SUBSPACES_NAME_LEN);
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
                SNFORMAT_S(new_path, new_len, 2, "/", (size_t)1, basename.data, basename.len);
            } else {
                SNFORMAT_S(new_path, new_len, 3, grandparent.data, grandparent.len, "/", (size_t)1, basename.data,
                           basename.len);
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
    if (g_state.source_is_root) {
        const str_t rn_base = get_basename(g_state.root_namespace);
        const str_t mm_base = get_basename(g_state.marfs_mountpoint);

        if (rename_index_root(rn_base, mm_base, 0) != 0) {
            return -1;
        }
    }

    return ret;
}

// revert_marfs_index will undo everything that's in cleanup_marfs_index. This allows for indexing to be run over an
// existing index tree without error
static int revert_marfs_index(void) {
    int ret = 0;

    // rename the marfs mountpoint from the marfs config to the root namespace
    if (g_state.source_is_root) {
        const str_t mm_base = get_basename(g_state.marfs_mountpoint);
        const str_t rn_base = get_basename(g_state.root_namespace);

        const int rc = rename_index_root(mm_base, rn_base, 1);
        if (rc != 0) {
            return rc < 0 ? -1 : 0;
        }
    } else {
        // there is nothing to restore on the first indexing run
        char* source_path = join_path(g_state.index_parent, g_state.source_base);
        struct stat st;

        if (!source_path) {
            fprintf(stderr, "join_path failed\n");
            return -1;
        }

        if (stat(source_path, &st) != 0) {
            if (errno != ENOENT) {
                fprintf(stderr, "stat('%s') failed: %s\n", source_path, strerror(errno));
                ret = -1;
            }
            free(source_path);
            return ret;
        }

        free(source_path);
    }

    // loop through our index namespaces and add a subspaces into them. then move them down into each subspace
    for (size_t i = 0; i < g_state.namespaces_count; i++) {
        const str_t old = g_state.namespaces[i].index_namespace;
        if (!str_exists(&old)) continue;

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
            SNFORMAT_S(indexed_path, new_len, 2, "/", (size_t)1, basename.data, basename.len);
        } else {
            SNFORMAT_S(indexed_path, new_len, 3, grandparent.data, grandparent.len, "/", (size_t)1, basename.data,
                       basename.len);
        }

        // make an empty MDAL_subspaces in this namespace's parent
        {
            // parent is a slice, so make a real C string before mkdir()
            if (parent.len > 1) {
                size_t parent_path_size = parent.len + 1;
                parent_path = malloc(parent_path_size);
                if (!parent_path) {
                    fprintf(stderr, "malloc failed for parent_path\n");
                    free(indexed_path);
                    ret = -1;
                    continue;
                }

                SNFORMAT_S(parent_path, parent_path_size, 1, parent.data, parent.len);

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
static int marfs_indexing_global_init(struct input* in) {
    pthread_mutex_t marfs_erasurelock = PTHREAD_MUTEX_INITIALIZER;

    size_t config_index = 0;
    int ret = -1;

    // We can only allow for a single dir to be indexed at a time. this is because we do not read multiple marfs configs
    if (in->pos.argc != 2) {
        fprintf(stderr, "Error: Marfs plugin requires exactly 2 paths\n");
        return ret;
    }

    // add index parent, selected source, and root namespace to global state
    INSTALL_STR(&g_state.index_parent, in->pos.argv[in->pos.argc - 1]);
    INSTALL_STR(&g_state.source, in->pos.argv[0]);
    trim_trailing_slashes_in_place(&g_state.index_parent);
    trim_trailing_slashes_in_place(&g_state.source);

    g_state.source_base = get_basename(g_state.source);

    char* sec_root = getenv(MARFS_SEC_ROOT_ENV);
    if (!sec_root || sec_root[0] == '\0') {
        fprintf(stderr, "Error: %s is not set\n", MARFS_SEC_ROOT_ENV);
        goto cleanup;
    }

    INSTALL_STR(&g_state.root_namespace, sec_root);
    g_state.root_namespace = trim_trailing_slashes(g_state.root_namespace);

    g_state.source_is_root = path_eq(g_state.source, g_state.root_namespace);

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

    // add marfs config mountpoint to global state
    INSTALL_STR(&g_state.marfs_mountpoint, g_state.marfs_cfg->mountpoint);
    g_state.marfs_mountpoint = trim_trailing_slashes(g_state.marfs_mountpoint);

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
        const str_t empty = REFSTR(NULL, 0);

        if (collect_ns_paths(g_state.marfs_cfg->rootns, empty, g_state.namespaces, &config_index,
                             g_state.root_namespace) != 0) {
            fprintf(stderr, "Error: collect_ns_paths failed\n");
            goto cleanup;
        }
    }

    if (config_index != g_state.namespaces_count) {
        fprintf(stderr, "Error: namespace count mismatch (expected %zu, got %zu)\n", g_state.namespaces_count,
                config_index);
        goto cleanup;
    }

    if (!validate_sec_root()) {
        fprintf(stderr, "Error: %s (%s) is not a MarFS sec-root or marfs_config is incorrect\n", MARFS_SEC_ROOT_ENV,
                g_state.root_namespace.data);
        goto cleanup;
    }

    if (!validate_source()) {
        goto cleanup;
    }

    if (build_index_namespace_paths() != 0) {
        fprintf(stderr, "Error: build_index_namespace_paths failed\n");
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
    for (size_t i = 0; i < g_state.namespaces_count; i++) {
        str_t ns = g_state.namespaces[i].namespace;
        if (!ns.data) {
            continue;
        }

        if (path_eq(ns, path)) {
            return 1;
        }
    }

    return 0;
}

// sec-root and configured namespaces are the directories that own MDAL metadata
static int is_mdal_owner(str_t path) { return path_eq(path, g_state.root_namespace) || is_namespace(path); }

// marfs_pre_processing_dir checks if we're about to process a marfs specific directory (MDAL_reference, MDAL_subspaces)
// or a namespace that is no longer active in the marfs config
static plugin_dir_action marfs_dir_action(void* ptr) {
    PCS_t* pcs = ptr;

    const str_t path = REFSTR(pcs->work->name, pcs->work->name_len);
    const str_t basename =
        REFSTR(pcs->work->name + pcs->work->name_len - pcs->work->basename_len, pcs->work->basename_len);
    const str_t parent = get_parent(path);

    if (starts_with_mdal(basename)) {
        // this directory starts with MDAL_

        // determine if this is a marfs directory or a user dir.
        // cases for being a marfs dir:
        // 1. directly under sec-root
        // 2. directly under a namespace
        if (is_mdal_owner(parent)) {
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
        if (is_mdal_owner(grandparent)) {
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

// marfs_pre_process_file removes any marfs specific files from the gufi db, decrements every file nlink by 1, and
// removes any marfs specific xattrs
static plugin_file_action marfs_pre_process_file(void* ptr, void* user_data) {
    PCS_t* pcs = ptr;
    struct entry_data* ed = pcs->ed;
    (void)user_data;

    const str_t path = REFSTR(pcs->work->name, pcs->work->name_len);
    const str_t basename =
        REFSTR(pcs->work->name + pcs->work->name_len - pcs->work->basename_len, pcs->work->basename_len);

    // remove marfs xattr from entry
    const size_t removed = xattr_remove(&ed->xattrs, MARFS_XATTR_NAME, MARFS_XATTR_NAME_LEN);

    // decrement nlink if the marfs xattr was removed
    if (removed > 0 && pcs->work->statuso.st_nlink > (nlink_t)0) {
        pcs->work->statuso.st_nlink--;
    }

    // remove mdal specific files
    if (starts_with_mdal(basename)) {
        // this files starts with MDAL_

        const str_t parent = get_parent(path);

        // determine if this is actual a marfs file or a user file
        // cases for being a marfs file:
        // 1. directly under sec-root
        // 2. directly under a namespace
        if (is_mdal_owner(parent)) {
            // this is a marfs file. do not add it to the database
            return PLUGIN_NO_PROCESS_FILE;
        }
    }

    return PLUGIN_PROCESS_FILE;
}

// marfs_process_dir renames the root namespace in the gufi db summary table to match the mountpoint in the marfs
// config. The actual dir gets renamed in cleanup_marfs_index. It also fixes pinode values for directories whose
// parent is an MDAL_subspaces directory that will be removed from the final index.
static void marfs_pre_process_dir(void* ptr, void* user_data) {
    PCS_t* pcs = ptr;
    (void)user_data;

    const str_t path = REFSTR(pcs->work->name, pcs->work->name_len);
    const str_t parent = get_parent(path);
    const str_t parent_basename = get_basename(parent);

    // Fix pinode for directories whose parent is MDAL_subspaces (which will be removed)
    // The pinode currently points to the MDAL_subspaces inode, but should point to the grandparent
    if (str_t_eq_cstr(parent_basename, MARFS_SUBSPACES_NAME, MARFS_SUBSPACES_NAME_LEN) && is_namespace(path) &&
        !path_eq(path, g_state.source)) {
        const str_t grandparent = get_parent(parent);

        if (grandparent.data && grandparent.len > 0) {
            size_t gp_path_size = grandparent.len + 1;
            char* gp_path = malloc(gp_path_size);
            if (gp_path) {
                SNFORMAT_S(gp_path, gp_path_size, 1, grandparent.data, grandparent.len);

                struct stat st;
                if (stat(gp_path, &st) == 0) {
                    pcs->work->pinode = (long long int)st.st_ino;
                } else {
                    fprintf(stderr, "plugin: stat('%s') failed: %s\n", gp_path, strerror(errno));
                }

                free(gp_path);
            } else {
                fprintf(stderr, "malloc failed for gp_path\n");
            }
        }
    }
}

// marfs_process_dir renames the root namespace in the gufi db summary table to match the mountpoint in the marfs
// config. The actual dir gets renamed in cleanup_marfs_index. It also fixes pinode values for directories whose
// parent is an MDAL_subspaces directory that will be removed from the final index.
static void marfs_post_process_dir(void* ptr, void* user_data) {
    PCS_t* pcs = ptr;
    (void)user_data;

    // determine if this is the root namespace dir
    if (!g_state.source_is_root || pcs->work->level != ROOT_NAMESPACE_LEVEL) {
        return;
    }

    const str_t basename =
        REFSTR(pcs->work->name + pcs->work->name_len - pcs->work->basename_len, pcs->work->basename_len);

    const str_t mountpoint = get_basename(g_state.marfs_mountpoint);

    char* sql = sqlite3_mprintf("UPDATE summary SET name = %.*Q WHERE name = %.*Q;", (int)mountpoint.len,
                                mountpoint.data, (int)basename.len, basename.data);

    if (!sql) {
        fprintf(stderr, "plugin: failed to allocate summary update\n");
        return;
    }

    char* err = NULL;
    if (sqlite3_exec(pcs->db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "plugin: update summary failed: %s\n", err ? err : sqlite3_errmsg(pcs->db));
    }

    sqlite3_free(err);
    sqlite3_free(sql);
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
    .ctx_init = NULL,
    .stat_file = NULL,
    .pre_process_dir = marfs_pre_process_dir,
    .post_process_dir = marfs_post_process_dir,
    .pre_process_file = marfs_pre_process_file,
    .post_process_file = NULL,
    .ctx_exit = NULL,
    .thread_exit = NULL,
    .global_exit = marfs_indexing_global_exit,
};
