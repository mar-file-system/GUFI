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



#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "config.h"
#include "debug.h"
#include "descend.h"
#include "trie.h"
#include "utils.h"

static const char WHITESPACE[] = "\x09\x0a\x0b\x0c\x0d\x20";

typedef struct {
    char *line;
    str_t path;       /* reference to line */
    size_t basename_len;
    char type;        /* [d]irectory or some other type */
    str_t inode;      /* reference to line */
    str_t pinode;     /* reference to line */
    size_t index;     /* position in global array */
} Node_t;

typedef struct {
    Node_t base;

    int isroot;

    /* references to children */
    struct {
        sll_t   list; /* Node_t * */
    } subdirs;

    struct {
        sll_t   list; /* Node_t * */
    } nondirs;
} DirNode_t;

static void node_free(Node_t *node) {
    /*
     * sll_destroy and trie_free do not delete the
     * actual nodes since they are references
     */
    if (node->type == 'd') {
        DirNode_t *dirnode = (DirNode_t *) node;
        sll_destroy(&dirnode->nondirs.list, NULL);
        sll_destroy(&dirnode->subdirs.list, NULL);
    }

    str_free_existing(&node->pinode);
    str_free_existing(&node->inode);
    str_free_existing(&node->path);
    free(node->line);
    free(node);
}

typedef struct {
    Node_t **nodes;
    size_t count;
    size_t alloc;
} Nodes_t;

static Nodes_t *node_insert(Nodes_t *nodes, Node_t *node) {
    /* index of new node is count of existing nodes */
    node->index = nodes->count;

    if (nodes->count < nodes->alloc) {
        nodes->nodes[nodes->count++] = node;
        return nodes;
    }

    /* if adding to the list would overflow, realloc */
    const size_t new_alloc = max(nodes->count + 1, nodes->alloc * 2);
    Node_t **new_nodes = realloc(nodes->nodes, new_alloc * sizeof(nodes->nodes[0]));
    if (!new_nodes) {
        return NULL;
    }

    nodes->alloc = new_alloc;
    nodes->nodes = new_nodes;
    nodes->nodes[nodes->count++] = node;

    return nodes;
}

static int get_pinode(QPTPool_ctx_t *ctx, void *data) {
    Node_t *node = (Node_t *) data;

    char *parent = NULL;
    size_t parent_len = node->path.len - node->basename_len;
    if (parent_len) {
        SNFORMAT_S_ALLOC(&parent, 1,
                         node->path.data, parent_len);
    }
    else {
        SNFORMAT_S_ALLOC(&parent, 1,
                         "..", (size_t) 2);
    }

    struct stat st;
    if (lstat(parent, &st) != 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not lstat parent of \"%s\": %s (%d)\n",
                node->path.data, strerror(err), err);
        free(parent);
        node_free(node);
        return 1;
    }

    free(parent);

    str_alloc_existing(&node->pinode, UINT64_DIGITS);
    node->pinode.len = SNPRINTF(node->pinode.data, node->pinode.len, "%" STAT_ino, st.st_ino);
    node->pinode.free = free;

    sll_t *ptnodes = QPTPool_get_args_internal(ctx);
    const size_t id = QPTPool_get_id(ctx);
    sll_push_back(&ptnodes[id], node);

    return 0;
}

/*
 * line: type inode [pinode] path
 *
 * Generate with:
 *     parallel_find -n <threads> --format "%y %i %I %p\n" <path>
 *     or
 *     find <path> -printf "%y %i %p\n"
 */
static int read_trace(FILE *file, QPTPool_ctx_t *ctx) {
    static const int NEED_PINODE = 2;
    static const int WITH_PINODE = 3;

    char *line = NULL;
    size_t n = 0;
    ssize_t len = 0;
    while ((len = getline(&line, &n, file)) != -1) {
        char  type         = '\0';
        int   inode_start  = 0;
        ino_t inode        = 0; /* only used to make sure value is an inode */
        int   pinode_start = 0;
        ino_t pinode       = 0; /* only used to make sure the value is a pinode */
        int   path_start   = 0;

        if (len && (line[len - 1] == '\n')) {
            len--;
            if (len && (line[len - 1] == '\r')) {
                len--;
            }
            line[len] = '\0';
        }

        const int rc = sscanf(line, "%c %n%" STAT_ino " %n%" STAT_ino " %n",
                              &type, &inode_start, &inode, &pinode_start, &pinode, &path_start);

        int path_offset = 0;

        if (rc == WITH_PINODE) {
            path_offset = path_start;
        }
        else if (rc == NEED_PINODE) {
            path_offset = pinode_start;
        }
        else {
            fprintf(stderr, "Warning: Ignoring bad line: \"%s\"\n", line);
            continue;
        }

        const size_t path_len = len - path_offset;
        if (len <= path_offset) {
            fprintf(stderr, "Warning: Ignoring path with length 0 on line \"%s\"\n",
                    line);
            continue;
        }

        Node_t *node       = NULL;
        if (type == 'd') {
            node = malloc(sizeof(DirNode_t));
        }
        else {
            node = malloc(sizeof(Node_t));
        }
        node->line         = line;
        node->path.data    = line + path_offset;
        node->path.len     = path_len;
        node->path.free    = NULL;
        node->basename_len = node->path.len - dirname_len(node->path.data, node->path.len);
        node->type         = type;
        node->inode.data   = line + inode_start;
        node->inode.len    = trailing_non_match_index(node->inode.data, pinode_start - inode_start, WHITESPACE, 6);
        node->inode.free   = NULL;

        if (type == 'd') {
            DirNode_t *dirnode = (DirNode_t *) node;
            dirnode->isroot = 0;
            sll_init(&dirnode->subdirs.list);
            sll_init(&dirnode->nondirs.list);
        }

        node->path.data[node->path.len]   = '\0';
        node->inode.data[node->inode.len] = '\0';

        if (rc == NEED_PINODE) {
            QPTPool_enqueue(ctx, get_pinode, node);
        }
        else {
            node->pinode.data = line + pinode_start;
            node->pinode.len  = trailing_non_match_index(node->pinode.data, path_start - pinode_start, WHITESPACE, 6);
            node->pinode.free = NULL;

            node->pinode.data[node->pinode.len] = '\0';

            sll_t *ptnodes = QPTPool_get_args_internal(ctx);
            const size_t id = QPTPool_get_id(ctx);
            sll_push_back(&ptnodes[id], node);
        }

        line = NULL;
        n = 0;
        len = 0;
    }

    free(line);

    return 0;
}

static int processdirs(const size_t threads, FILE *in,
                       Nodes_t *dirs, Nodes_t *nondirs) {
    memset(dirs, 0, sizeof(*dirs));
    memset(nondirs, 0, sizeof(*nondirs));

    int rc = 0;

    sll_t *ptnodes = malloc(threads * sizeof(*ptnodes));
    if (!ptnodes) {
        fprintf(stderr, "Failed to allocate per-thread node list\n");
        return 1;
    }

    for(size_t i = 0; i < threads; i++) {
        sll_init(&ptnodes[i]);
    }

    QPTPool_ctx_t *ctx = QPTPool_init(threads, ptnodes);
    if (QPTPool_start(ctx) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(ctx);
        rc = 1;
        goto done;
    }

    /* read any-order list of paths */
    read_trace(in, ctx);

    QPTPool_wait(ctx);

    /* get total number of nodes */
    for(size_t i = 0; i < threads; i++) {
        sll_loop(&ptnodes[i], node) {
            Node_t *n = sll_node_data(node);
            if (n->type == 'd') {
                dirs->alloc++;
            }
            else {
                nondirs->alloc++;
            }
        }
    }

    fprintf(stderr, "Found %zu directories containing %zu files\n", dirs->alloc, nondirs->alloc);

    /* merge nodes into dir/nondir arrays */
    dirs->nodes = malloc(dirs->alloc * sizeof(dirs->nodes[0]));
    nondirs->nodes = malloc(nondirs->alloc * sizeof(nondirs->nodes[0]));
    for(size_t i = 0; i < threads; i++) {
        sll_loop(&ptnodes[i], node) {
            Node_t *n = sll_node_data(node);
            if (n->type == 'd') {
                n->index = dirs->count;
                dirs->nodes[dirs->count++] = n;
            }
            else {
                n->index = nondirs->count;
                nondirs->nodes[nondirs->count++] = n;
            }
        }
    }

    QPTPool_stop(ctx);
    QPTPool_destroy(ctx);

  done:
    for(size_t i = 0; i < threads; i++) {
        sll_destroy(&ptnodes[i], NULL);
    }
    free(ptnodes);

    return rc;
}

static trie_t *create_connections(Nodes_t *dirs, Nodes_t *nondirs) {
    trie_t *search = trie_alloc();

    /*
     * store directories in trie to make them O(len(inode)) searchable
     *
     * duplicate non-dir inodes are allowed to be stored in the actual Nodes_t
     */
    for(size_t i = 0; i < dirs->count; i++) {
        Node_t *node = dirs->nodes[i];
        trie_insert(search, node->inode.data, node->inode.len, node, NULL);
    }

    /* create connections */
    for(size_t i = 0; i < dirs->count; i++) {
        DirNode_t *node = (DirNode_t *) dirs->nodes[i];
        DirNode_t *parent = NULL;
        if (trie_search(search, node->base.pinode.data, node->base.pinode.len, (void **) &parent) == 1) {
            sll_push_back(&parent->subdirs.list, node);
        }
        else {
            /* if only 1 shows up, it's root; otherwise, probably not right */
            fprintf(stderr, "Found root path \"%.*s\" (%.*s, %.*s)\n",
                    (int) node->base.path.len,   node->base.path.data,
                    (int) node->base.inode.len,  node->base.inode.data,
                    (int) node->base.pinode.len, node->base.pinode.data);
            node->isroot = 1;
        }
    }

    for(size_t i = 0; i < nondirs->count; i++) {
        Node_t *node = nondirs->nodes[i];
        DirNode_t *parent = NULL;
        if (trie_search(search, node->pinode.data, node->pinode.len, (void **) &parent) == 1) {
            sll_push_back(&parent->nondirs.list, node);
        }
        else {
            fprintf(stderr, "Warning: Found non-directory with unknown parent: \"%.*s\" (%.*s, %.*s)\n",
                    (int) node->path.len,   node->path.data,
                    (int) node->inode.len,  node->inode.data,
                    (int) node->pinode.len, node->pinode.data);
        }
    }

    return search;
}

static const char BASE62[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

static void random_buf(trie_t *search, char *buf, const size_t buf_len, const size_t mod) {
    memset(buf, 0, buf_len);
    do {
        for(size_t i = 0; i < buf_len; i++) {
            buf[i] = BASE62[rand() % mod];
        }
    } while (trie_search(search, buf, buf_len, NULL));
}

static void random_name(trie_t *search, char *name, const size_t name_len) {
    random_buf(search, name, name_len, 62);
}

static void random_inode(trie_t *search, char *inode, const size_t inode_len) {
    random_buf(search, inode, inode_len, 10);
}

/* paths under moved directories are not renamed, so use this function to get updated path */
static char *get_current_path(trie_t *search, Node_t *node) {
    sll_t nodes;
    sll_init(&nodes);

    size_t lens = 0;  /* length of path segments */
    size_t depth = 0; /* number of path segments/slashses */
    while (node) {
        lens += node->basename_len;
        depth++;

        sll_push_front(&nodes, node);

        Node_t *parent = NULL;
        if (trie_search(search, node->pinode.data, node->pinode.len, (void **) &parent) != 1) {
            break;
        }

        node = parent;
    }

    /*
     * first node needs special handling to deal with
     * path segments not in the in-memory tree
     */
    Node_t *head = sll_pop_front(&nodes);
    lens += head->path.len;
    lens -= head->basename_len;

    const size_t path_len = depth + lens;
    char *path = malloc(path_len + 1);
    char *curr = path;

    curr += SNFORMAT_S(curr, path_len + 1, 2,
                       head->path.data, head->path.len,
                       "/", (size_t) 1);

    sll_loop(&nodes, n) {
        Node_t *component = sll_node_data(n);
        curr += SNFORMAT_S(curr, path_len + 1 - (curr - path), 2,
                           component->path.data + component->path.len - component->basename_len, component->basename_len,
                           "/", (size_t) 1);
    }
    path[path_len - 1] = '\0'; /* remove trailing slash */

    sll_destroy(&nodes, NULL);

    return path;
}

static int do_create(trie_t *search, const size_t root_count, Nodes_t *dirs, Nodes_t *nondirs, const char type, const char delim) {
    (void) root_count;

    /* pick a random directory to add to */
    const size_t idx = rand() % dirs->count;
    DirNode_t *parent = (DirNode_t *) dirs->nodes[idx];

    #define TEMPLATE "XXXXXXXX"
    static const size_t NAME_LEN = 6;
    static const size_t INODE_LEN = 8;

    Node_t *child       = NULL;
    if (type == 'd') {
        child = malloc(sizeof(DirNode_t));
    }
    else {
        child = malloc(sizeof(Node_t));
    }

    child->line         = NULL;
    child->basename_len = NAME_LEN;
    child->type         = type;

    /* set random name (no need to prefix with destinaion path) */
    child->path.len     = SNFORMAT_S_ALLOC(&child->path.data, 1,
                                           TEMPLATE, NAME_LEN);
    random_name(search, child->path.data, child->path.len);
    child->path.free    = free;

    /* set random inode */
    child->inode.len    = SNFORMAT_S_ALLOC(&child->inode.data, 1,
                                           TEMPLATE, INODE_LEN);
    random_inode(search, child->inode.data, child->inode.len);
    child->inode.free   = free;

    /* set parent inode */
    child->pinode.data  = parent->base.inode.data;
    child->pinode.len   = parent->base.inode.len;
    child->pinode.free  = NULL;

    if (type == 'd') {
        DirNode_t *dirnode = (DirNode_t *) child;
        dirnode->isroot = 0;
        sll_init(&dirnode->subdirs.list);
        sll_init(&dirnode->nondirs.list);
    }

    /* store child */
    if (type == 'd') {
        sll_push_back(&parent->subdirs.list, child);
        node_insert(dirs, child);
        trie_insert(search, child->inode.data, child->inode.len, child, NULL);
    }
    else {
        sll_push_back(&parent->nondirs.list, child);
        node_insert(nondirs, child);
    }

    char *child_path = get_current_path(search, child);
    /* fprintf(stdout, "Create (%c, %.*s, %.*s) \"%s\"\n", */
    /*         type, */
    /*         (int) child->inode.len, child->inode.data, */
    /*         (int) child->pinode.len, child->pinode.data, */
    /*         child_path); */
    fprintf(stdout, "create%c%s%c%s\n", delim, (type == 'd')?"dir":"file", delim, child_path);
    free(child_path);

    return 0;
}

static int find_loop(trie_t *search, Node_t *src, Node_t *dst) {
    Node_t *node = NULL;
    Node_t *parent = NULL;

    node = dst;
    while (trie_search(search, node->pinode.data, node->pinode.len, (void **) &parent) == 1) {
        /* placing src into a child */
        if (parent == src) {
            return 1;
        }
        node = parent;
    }

    return 0;
}

static void delete_from_parent(trie_t *search, Node_t *node) {
    DirNode_t *parent = NULL;
    if (trie_search(search, node->pinode.data, node->pinode.len, (void **) &parent) != 1) {
        /* will hit here if deleting root */
        return;
    }

    sll_t  *children_list = (node->type == 'd')?&parent->subdirs.list:&parent->nondirs.list;

    /* remove this node from the parent's list of children */
    /*
     * TODO: Find better way than by moving list manually.
     *       Would store in trie, but that got crazy with inodes.
     */
    sll_t temp;
    sll_init(&temp);
    sll_loop(children_list, n) {
        Node_t *c = sll_node_data(n);
        if (strncmp(c->inode.data, node->inode.data, node->inode.len + 1) != 0) {
            sll_push_back(&temp, c);
        }
    }
    sll_destroy(children_list, NULL);
    sll_move_append(children_list, &temp);

    return;
}

static int basename_exists(Node_t *dst, const char *basename, const size_t basename_len) {
    DirNode_t *dir = (DirNode_t *) dst;

    /* not creating another trie for basenames, so have to linearly search list */
    sll_loop(&dir->subdirs.list, node) {
        Node_t *subdir = sll_node_data(node);
        const char *subdir_basename = subdir->path.data + subdir->path.len - subdir->basename_len;
        if (strncmp(subdir_basename, basename, basename_len + 1) == 0) {
            return 1;
        }
    }

    sll_loop(&dir->nondirs.list, node) {
        Node_t *nondir = sll_node_data(node);
        const char *nondir_basename = nondir->path.data + nondir->path.len - nondir->basename_len;
        if (strncmp(nondir_basename, basename, basename_len + 1) == 0) {
            return 1;
        }
    }
    return 0;
}

/* only move from one parent to another - basename is never changed */
static int do_move(trie_t *search, const size_t root_count, Nodes_t *dirs, Nodes_t *nondirs, const char type, const char delim) {
    if (type == 'd') {
        if ((dirs->count - root_count) < 2) { /* must have 2 non-root directories (for a total of 3 directories) to have a valid move */
            fprintf(stderr, "Warning: Not enough directories to move. Rerolling.\n");
            return 1;
        }
    }
    else {
        if (nondirs->count == 0) {
            fprintf(stderr, "Warning: No non-directories to move. Rerolling.\n");
            return 1;
        }

        if (dirs->count == 1) {
            fprintf(stderr, "Warning: No second directory to move non-directory to. Rerolling.\n");
            return 1;
        }
    }

    size_t src_idx = 0;
    size_t dst_idx = 0; /* index of new parent directory */

    size_t max_tries = (type == 'd')?dirs->count:nondirs->count;
    while (max_tries) {
        dst_idx = rand() % dirs->count; /* target is always the parent directory */

        if (type == 'd') {
            do {
                src_idx = rand() % dirs->count;
            } while (((DirNode_t *) (dirs->nodes[src_idx]))->isroot); /* do not move root */
        }
        else {
            src_idx = rand() % nondirs->count;
        }

        /* don't move into self */
        if (src_idx == dst_idx) {
            max_tries--;
            continue;
        }

        Node_t *s = ((type == 'd')?dirs:nondirs)->nodes[src_idx];
        Node_t *d = dirs->nodes[dst_idx];

        if (type == 'd') {
            /* don't move to child, creating loop */
            if (find_loop(search, s, d)) {
                max_tries--;
                continue;
            }
        }

        /* make sure entry with same name does not already exist in the destination directory */
        char *basename = s->path.data + s->path.len - s->basename_len;
        if (basename_exists(d, basename, s->basename_len)) {
            max_tries--;
            continue;
        }

        break;
    }

    if (max_tries == 0) {
        fprintf(stderr, "Warning: Could not find valid move. Rerolling.\n");
        return 1;
    }

    Node_t *src = ((type == 'd')?dirs:nondirs)->nodes[src_idx];
    DirNode_t *dst = (DirNode_t *) dirs->nodes[dst_idx];

    delete_from_parent(search, src);

    char *src_path = get_current_path(search, src);

    /* replace the pinode */
    src->pinode.data = dst->base.inode.data;
    src->pinode.len  = dst->base.inode.len;
    src->pinode.free = NULL;

    /* add to dst */
    sll_t *dst_list = (src->type == 'd')?&dst->subdirs.list:&dst->nondirs.list;
    sll_push_back(dst_list, src);

    /* get new path now that the pinode has been updated */
    char *dst_path = get_current_path(search, src);

    /* fprintf(stdout, "Move   (%c, %.*s, %.*s) \"%s\" to (%c, %.*s, %.*s) \"%s\"\n", */
    /*         src->type, */
    /*         (int) src->inode.len, src->inode.data, */
    /*         (int) src->pinode.len, src->pinode.data, */
    /*         src_path, */
    /*         src->type, */
    /*         (int) src->inode.len, src->inode.data, */
    /*         (int) dst->inode.len, dst->inode.data, */
    /*         dst_path); */
    fprintf(stdout, "move%c%s%c%s%c%s\n", delim, (type == 'd')?"dir":"file", delim, src_path, delim, dst_path);
    free(dst_path);
    free(src_path);

    return 0;
}

static void move_last_node(Nodes_t *nodes, const size_t idx) {
    /* move last element into deleted position */
    const size_t last = --nodes->count;
    nodes->nodes[idx] = nodes->nodes[last];
    if (last != idx) {
        nodes->nodes[idx]->index = idx;
    }
    nodes->nodes[last] = NULL;
}

static int do_delete(trie_t *search, const size_t root_count, Nodes_t *dirs, Nodes_t *nondirs, const char type, const char delim) {
    if (type == 'd') {
        if (dirs->count == root_count) {
            fprintf(stderr, "Warning: No non-root directories to delete. Rerolling.\n");
            return 1;
        }
    }
    else {
        if (nondirs->count == 0) {
            fprintf(stderr, "Warning: No non-directories to delete. Rerolling.\n");
            return 1;
        }
    }

    size_t idx = 0;
    if (type == 'd') {
        do {
            idx = rand() % dirs->count;
        } while (((DirNode_t *) dirs->nodes[idx])->isroot);
    }
    else {
        idx = rand() % nondirs->count;
    }

    Node_t *tgt = ((type == 'd')?dirs:nondirs)->nodes[idx];

    char *tgt_path = get_current_path(search, tgt);
    /* fprintf(stdout, "Delete (%c, %.*s, %.*s) \"%s\"\n", */
    /*         tgt->type, */
    /*         (int) tgt->inode.len, tgt->inode.data, */
    /*         (int) tgt->pinode.len, tgt->pinode.data, */
    /*         tgt_path); */
    fprintf(stdout, "delete%c%s%c%s\n", delim, (type == 'd')?"dir":"file", delim, tgt_path);
    free(tgt_path);

    /* clean up the parent node */
    delete_from_parent(search, tgt);

    /*
     * parent has been cleaned up; children can
     * just be deleted without handling parent
     */

    sll_t to_delete;
    sll_init(&to_delete);
    sll_push_back(&to_delete, tgt);
    while (sll_get_size(&to_delete)) {
        Node_t *node = sll_pop_front(&to_delete);

        /* directories potentially have children to delete */
        if (node->type == 'd') {
            DirNode_t *dirnode = (DirNode_t *) node;

            /* queue up nondirs for removal (moved out of node list) */
            sll_move_append(&to_delete, &dirnode->nondirs.list);

            /* queue up subdirs for removal (moved out of dirnode list) */
            sll_move_append(&to_delete, &dirnode->subdirs.list);

            /* this directory should no longer be findable in the global tree */
            trie_delete(search, node->inode.data, node->inode.len);

            /* reorder the global array and remove this node */
            move_last_node(dirs, node->index);
        }
        else {
            /* reorder the global array and remove this node */
            move_last_node(nondirs, node->index);
        }

        node_free(node);
    }

    sll_destroy(&to_delete, NULL);

    return 0;
}

static size_t make_changes(trie_t *search, Nodes_t *dirs, Nodes_t *nondirs,
                           const int seed, const char delim, const size_t op_count,
                           const size_t op_weights[], const size_t type_weights[]) {
    /* if random value is less than this value, use this op */
    const size_t create_bound = op_weights[0];
    const size_t move_bound   = op_weights[0] + op_weights[1];
    const size_t delete_bound = op_weights[0] + op_weights[1] + op_weights[2];

    if (delete_bound == 0) {
        fprintf(stderr, "Error: Op weights must add up to at least 1\n");
        return 0;
    }

    const size_t dir_bound    = type_weights[0];
    const size_t nondir_bound = type_weights[0] + type_weights[1];

    if (nondir_bound == 0) {
        fprintf(stderr, "Error: Type weights must add up to at least 1\n");
        return 0;
    }

    srand(seed);

    /* get number of root directories */
    size_t root_count = 0;
    for(size_t i = 0; i < dirs->count; i++) {
        if (((DirNode_t *) dirs->nodes[i])->isroot) {
            root_count++;
        }
    }

    size_t ops_completed = 0;
    const size_t max_rerolls = op_count; /* abort after op_count */
    size_t rerolls = 0;
    while ((ops_completed < op_count) && (rerolls < max_rerolls)) {
        const size_t type_val = rand() % nondir_bound;
        char type = '\0';
        if (type_weights[0] && (type_val < dir_bound)) {
            type = 'd';
        }
        else if (type_weights[1]) {
            type = 'f';
        }

        /*
         * Not exactly uniform, but good enough for initial op
         * selection.
         *
         * The actual op distribution is further skewed towards
         * creates and moves (especially for smaller trees) because
         * deletes cannot occur without existing entries. Deleting
         * directories also deletes the entire subtree, meaning that
         * future deletes have fewer entries to choose from and are
         * more likely to be rerolled than moves.
         */
        const size_t op = rand() % delete_bound;

        if (op_weights[0] && (op < create_bound)) {
            do_create(search, root_count, dirs, nondirs, type, delim);
            rerolls = 0;    /* unconditional reset */
        }
        else if (op_weights[1] && (op < move_bound)) {
            if (do_move(search, root_count, dirs, nondirs, type, delim) == 0) {
                rerolls = 0;
            }
            else {
                rerolls++;
                continue;
            }
        }
        else if (op_weights[2] /* no need to check delete bound */ ){
            if (do_delete(search, root_count, dirs, nondirs, type, delim) == 0) {
                rerolls = 0;
            }
            else {
                rerolls++;
                continue;
            }
        }

        ops_completed++;
    }

    fflush(stdout);

    if (rerolls) {
        fprintf(stderr, "Warning: Exiting early\n");
    }

    return ops_completed;
}

int main(int argc, char *argv[]) {
    size_t threads = 1; /* only used for stat-ing input paths */
    int seed = time(NULL);
    char delim = '|';
    size_t op_count = 0;
    size_t op_weights[] = {0, 0, 0};
    size_t type_weights[] = {0, 0};
    FILE *file = stdin;

    struct option opts[] = {
        {"threads", required_argument, NULL, 'n'},
        {"seed",    required_argument, NULL, 1000},
        {"delim",   required_argument, NULL, 1001},
        {"ops",     required_argument, NULL, 1002},
        {"create",  required_argument, NULL, 'c'},
        {"move",    required_argument, NULL, 'm'},
        {"delete",  required_argument, NULL, 'd'},
        {"dir",     required_argument, NULL, 'D'},
        {"nondir",  required_argument, NULL, 'F'},
        {"paths",   required_argument, NULL, 'p'},
        { NULL, 0, NULL, 0 }
    };

    int opt = 0;
    while ((opt = getopt_long(argc, argv, "n:c:m:d:D:F:p:", opts, NULL)) != -1) {
        switch (opt) {
            #define ARG_ERR(cond, name)                                     \
                if (cond) {                                                 \
                    fprintf(stderr, "Error: Bad %s: %s\n", name, optarg);   \
                    return EXIT_FAILURE;                                    \
                }

            case 'n':
                ARG_ERR(sscanf(optarg, "%zu", &threads) != 1, "thread input");
                ARG_ERR(threads < 1, "thread count");
                break;
            case 1000:
                ARG_ERR(sscanf(optarg, "%d", &seed) != 1, "seed");
                break;
            case 1001:
                ARG_ERR(sscanf(optarg, "%c", &delim) != 1, "delimiter");
                break;
            case 1002:
                ARG_ERR(sscanf(optarg, "%zu", &op_count) != 1, "op count");
                break;
            case 'c':
                ARG_ERR(sscanf(optarg, "%zu", &op_weights[0]) != 1, "create weight");
                break;
            case 'm':
                ARG_ERR(sscanf(optarg, "%zu", &op_weights[1]) != 1, "move weight");
                break;
            case 'd':
                ARG_ERR(sscanf(optarg, "%zu", &op_weights[2]) != 1, "delete weight");
                break;
            case 'D':
                ARG_ERR(sscanf(optarg, "%zu", &type_weights[0]) != 1, "dir weight");
                break;
            case 'F':
                ARG_ERR(sscanf(optarg, "%zu", &type_weights[1]) != 1, "nondir weight");
                break;
            case 'p':
                file = fopen(optarg, "r");
                if (!file) {
                    fprintf(stderr, "Error: Could not open file \"%s\"\n", optarg);
                    return EXIT_FAILURE;
                }
                break;
            default:
                return EXIT_FAILURE;
        }
    }

    /* block buffer stdout */
    char print_buffer[4096];
    setvbuf(stdout, print_buffer, _IOFBF, sizeof(print_buffer));

    struct start_end se = {0};
    clock_gettime(CLOCK_MONOTONIC, &se.start);

    struct start_end pd = {0}; /* processdirs time */
    clock_gettime(CLOCK_MONOTONIC, &pd.start);

    Nodes_t dirs = {0};
    Nodes_t nondirs = {0};

    int rc = processdirs(threads, file, &dirs, &nondirs);     /* read existing tree nodes into memory */
    if (file != stdin) {
        fclose(file);
    }

    if (rc != 0) {
        goto cleanup;
    }

    clock_gettime(CLOCK_MONOTONIC, &pd.end);
    fprintf(stderr, "Ingested tree in %.2Lf seconds\n", sec(nsec(&pd)));

    struct start_end gen = {0}; /* gen ops time */
    clock_gettime(CLOCK_MONOTONIC, &gen.start);

    size_t ops_completed = 0;
    if (dirs.count) {
        trie_t *trie = create_connections(&dirs, &nondirs);
        ops_completed = make_changes(trie, &dirs, &nondirs, seed, delim,
                                     op_count, op_weights, type_weights);
        trie_free(trie);
    }
    else {
        fprintf(stderr, "Error: No directories found\n");
        rc = 1;
    }

  cleanup:
    for(size_t i = 0; i < nondirs.count; i++) {
        node_free(nondirs.nodes[i]);
    }

    for(size_t i = 0; i < dirs.count; i++) {
        node_free(dirs.nodes[i]);
    }

    free(nondirs.nodes);
    free(dirs.nodes);

    if (rc == 0) {
        clock_gettime(CLOCK_MONOTONIC, &gen.end);
        fprintf(stderr, "Generated %zu filesystem operations in %.2Lf seconds\n",
                ops_completed, sec(nsec(&gen)));

        clock_gettime(CLOCK_MONOTONIC, &se.end);
        fprintf(stderr, "Overall runtime: %.2Lf seconds\n",sec(nsec(&se)));
        return EXIT_SUCCESS;
    }

    return EXIT_FAILURE;
}
