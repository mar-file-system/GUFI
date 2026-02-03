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
 * An example Lustre plugin for GUFI.
 *
 * If the lustre and lustre-devel RPMs are installed on the machine that is used to build GUFI, then this plugin
 * should be built automatically. If you want to build it manually (for example, because the lustre source is available
 * but not installed into a standard path), you can do so like this:
 *
 *     # set based on the lustre source location on your machine:
 *     $ export LUSTRE_INCLUDE_DIR=/home/$USER/lustre-release/lustre/include
 *     $ export LUSTRE_LIBRARY_DIR=/home/$USER/lustre-release/lustre/utils/.libs
 *
 *     $ cd contrib
 *
 *     $ gcc -g -c -fPIC -I../build/deps/sqlite3/include -I../include -I$LUSTRE_INCLUDE_DIR -I$LUSTRE_INCLUDE_DIR/uapi lustre_plugin.c
 *
 *     $ gcc -shared -o liblustre_plugin.so lustre_plugin.o -L$LUSTRE_LIBRARY_DIR -llustreapi
 *
 *  Note that this includes the sqlite3 header from the GUFI sources, but this should not
 *  statically link sqlite3. Instead, it should dynamically link to the sqlite3 symbols in
 *  the main GUFI binary when this code is dlopen()ed.
 *
 *  Run like this:
 *
 *      # if the lustreapi library is not installed in a standard location:
 *      $ export LD_LIBRARY_PATH=$LUSTRE_LIBRARY_DIR
 *
 *      $ cd build
 *      $ ./src/gufi_dir2index -U ../contrib/liblustre_plugin.so -n1 /mnt/lustre /tmp/gufi_tree/
 */

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <lustre/lustreapi.h>
#include <sqlite3.h>

#include "bf.h"
#include "plugin.h"

static char *my_basename(char *path) {
    char *base = path;
    char *p = path;
    int next_component = 0;

    while (*p) {
        if (next_component) {
            base = p;
            next_component = 0;
        }

        if (*p == '/') {
            next_component = 1;
        }

        p++;
    }

    return base;
}

/*
 * This struct tracks the number of components seen on each OST. For each file
 * processed, and for each component of that file, add one to the count for
 * that particular OST.
 */
struct stripe_tracker {
    /*
     * num_components[i] stores the number of components that have been seen
     * on the OST with index i.
     */
    uint64_t *stripe_count;
    /*
     * Stores the size of the num_components table. This needs to grow when we see a
     * new OST number that is higher than the maximum index in the table so far.
     */
    uint32_t array_size;
    /*
     * Tracks the highest OST index seen. This will likely be smaller than the
     * array size, so separately tracking it means we don't have to loop through the
     * useless high indexes in the stripe_count array when saving the stripe info
     * to the database.
     */
    uint32_t max_ost_idx;
};

/*
 * Allocate and Initialize a new stripe_tracker.
 */
static struct stripe_tracker *new_stripe_tracker(void) {
    uint32_t initial_size = 64;

    struct stripe_tracker *new = malloc(sizeof *new);
    if (!new) {
        return NULL;
    }

    new->stripe_count = calloc(initial_size, sizeof(*new->stripe_count));
    if (!new->stripe_count) {
        free(new);
        return NULL;
    }

    new->array_size = initial_size;

    new->max_ost_idx = 0;

    return new;
}

/*
 * Clean up a stripe_tracker, freeing its allocations.
 */
static void destroy_stripe_tracker(struct stripe_tracker *p) {
    if (p) {
        free(p->stripe_count);
    }

    free(p);
}

/*
 * If necessary, grow the stripe_count array in `s` to be large enough to
 * accomodate `ost_index`.
 *
 * Returns 0 if growing succeeded, or 1 if it failed.
 */
static int grow_stripe_tracker(struct stripe_tracker *s, uint32_t ost_index) {
    if (ost_index > s->max_ost_idx) {
        s->max_ost_idx = ost_index;
    }
    if (ost_index < s->array_size) {
        /* Nothing needs to be done: yay! */
        return 0;
    }

    uint32_t new_size = s->array_size;

    while (ost_index >= new_size) {
        if (new_size >= UINT32_MAX / 2) {
            /* In case we somehow get a filesystem with an insane number of OSTs,
             * don't let that overflow and ruin our array: */
            return 1;
        }
        new_size *= 2;
    }

    uint64_t *new_array = realloc(s->stripe_count, new_size);
    if (!new_array) {
        return 1;
    }

    /* The new space is not initialized, so do that now: */
    memset(new_array + s->array_size, 0, new_size - s->array_size);

    s->stripe_count = new_array;
    s->array_size = new_size;

    return 0;
}

/*
 * Given the `stripe_array` which contains `stripe_count` stripes, increment
 * the count for each OST that the stripe lives on.
 */
static void track_file_stripes(struct stripe_tracker *s,
                               struct lov_user_ost_data_v1 *stripe_array,
                               uint16_t stripe_count) {
    for (int i = 0; i < stripe_count; i++) {
        uint32_t ost_index = stripe_array[i].l_ost_idx;
        if (grow_stripe_tracker(s, ost_index)) {
            /* Just give up if we couldn't grow the stripe array large enough :( */
            fprintf(stderr, "lustre plugin: could not allocate memory, information may be incomplete");
            return;
        }

        s->stripe_count[ost_index] += 1;
    }
}

/*
 * Set up initial state for tracking Lustre stripe info.
 */
void *db_init(void *ptr) {
    PCS_t *pcs = (PCS_t *) ptr;
    sqlite3 *db = pcs->db;

    struct stripe_tracker *state = new_stripe_tracker();
    if (!state) {
        fprintf(stderr, "lustre plugin: could not allocate memory to track stripe info");
        return NULL;
    }

    char *text = "CREATE TABLE lustre_osts (ost_index INTEGER PRIMARY KEY, num_files INTEGER);"
                 "CREATE TABLE lustre_fids (name TEXT, fid BLOB);";
    char *error;

    int res = sqlite3_exec(db, text, NULL, NULL, &error);
    if (res != SQLITE_OK) {
        fprintf(stderr, "lustre plugin: db_init(): error executing statement: %d %s\n", res, error);
    }

    sqlite3_free(error);

    return state;
}

/*
 * This method of determining the maximum possible size of a `lov_user_md` was suggested by
 *      man 3 llapi_file_get_stripe
 */
static const size_t v1_size = sizeof(struct lov_user_md_v1) + LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);
static const size_t v3_size = sizeof(struct lov_user_md_v3) + LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);
static const size_t lum_size = v1_size > v3_size ? v1_size : v3_size;

static void *alloc_lum() {
    return calloc(1, lum_size);
}

/*
 * Insert an entry into the database "lustre_fids" table with the FID of this file or directory.
 * For the entries in the directory described by this DB, the key is the entry name.
 * For the directory itself, the key is the empty string, "".
 */
static void insert_fid(char *path, sqlite3 *db, void *user_data, int is_child_entry) {
    struct lu_fid fid;
    int res = llapi_path2fid(path, &fid);
    if (res) {
        fprintf(stderr, "lustre plugin: error getting FID for %s: %s\n",
                path, strerror(errno));
        return;
    }

    /* The key is either the entry name, or if this is for the directory itself, an empty string. */
    if (is_child_entry) {
        path = my_basename(path);
    } else {
        path = "";
    }

    char *text = sqlite3_mprintf("INSERT INTO lustre_fids (name, fid) VALUES('%s', (?));", path);
    sqlite3_stmt *statement = NULL;
    res = sqlite3_prepare_v2(db, text, -1, &statement, NULL);
    if (res != SQLITE_OK) {
        fprintf(stderr, "lustre plugin: process_dir(): error in sqlite3_prepare_v2(): %d\n", res);
        goto out;
    };

    /*
     * Interpret the FID as a string of bytes. This is a little bit fragile in that the FID is built
     * of integers, not bytes, so this would break if a DB is read on a system with a different
     * endianness than the system which wrote it. In practice, AFAIK Lustre only runs on little-endian
     * systems so this should be fine.
     */
    void *data = &fid;
    int len = sizeof fid;
    res = sqlite3_bind_blob(statement, 1, data, len, SQLITE_STATIC);
    if (res != SQLITE_OK) {
        fprintf(stderr, "lustre plugin: process_dir(): error in sqlite3_bind_blob(): %d\n", res);
        goto out_finalize;
    };

    res = sqlite3_step(statement);
    if (res != SQLITE_DONE) {
        fprintf(stderr, "lustre plugin: process_dir(): error in sqlite3_step(): %d\n", res);
    };

  out_finalize:
    sqlite3_finalize(statement);

  out:
    sqlite3_free(text);
}

static void process_dir(void *ptr, void *user_data) {
    PCS_t *pcs = (PCS_t *) ptr;
    insert_fid(pcs->work->name, pcs->db, user_data, 0);
}

static void process_file(void *ptr, void *user_data) {
    PCS_t *pcs = (PCS_t *) ptr;
    sqlite3 *db = pcs->db;
    char *path = pcs->work->name;

    insert_fid(path, db, user_data, 1);

    struct lov_user_md *layout_info = alloc_lum();

    int res = llapi_file_get_stripe(path, layout_info);

    if (res) {
        fprintf(stderr, "lustre plugin: error getting stripe info for %s: %s\n",
                path, strerror(errno));
        free(layout_info);
        return;
    }

    char *text = sqlite3_mprintf("UPDATE entries SET ossint4 = %d where name = '%q';",
            layout_info->lmm_stripe_size, my_basename(path));
    char *error = NULL;

    res = sqlite3_exec(db, text, NULL, NULL, &error);
    if (res != SQLITE_OK) {
        fprintf(stderr, "lustre plugin: process_file(): error executing statement: %d %s\n",
                res, error);
        goto out;
    }

    struct lov_user_ost_data_v1 *stripe_array;
    uint16_t stripe_count;

    if (layout_info->lmm_magic == LOV_USER_MAGIC_V1) {
        struct lov_user_md_v1 *v1= (struct lov_user_md_v1 *) layout_info;
        stripe_array = v1->lmm_objects;
        stripe_count = v1->lmm_stripe_count;
    } else if (layout_info->lmm_magic == LOV_USER_MAGIC_V3) {
        struct lov_user_md_v3 *v3= (struct lov_user_md_v3 *) layout_info;
        stripe_array = v3->lmm_objects;
        stripe_count = v3->lmm_stripe_count;
    } else {
        fprintf(stderr, "lustre plugin: unknown layout format on file %s: %d\n", path, layout_info->lmm_magic);
        goto out;
    }

    struct stripe_tracker *tracker = (struct stripe_tracker *) user_data;
    track_file_stripes(tracker, stripe_array, stripe_count);

  out:
    free(layout_info);
    sqlite3_free(error);
    sqlite3_free(text);
}

/*
 * Save stripe tracking info to the database and clean up state.
 */
static void db_exit(void *ptr, void *user_data) {
    PCS_t *pcs = (PCS_t *) ptr;
    sqlite3 *db = pcs->db;
    struct stripe_tracker *state = (struct stripe_tracker *) user_data;

    for (uint32_t i = 0; i <= state->max_ost_idx; i++) {
        char *text = sqlite3_mprintf("INSERT INTO lustre_osts VALUES(%" PRIu32 ", %" PRIu64 ");",
                i, state->stripe_count[i]);
        char *error = NULL;

        int res = sqlite3_exec(db, text, NULL, NULL, &error);
        if (res != SQLITE_OK) {
            fprintf(stderr, "lustre plugin: db_exit(): error executing statement: %d %s\n",
                    res, error);
        }

        sqlite3_free(text);
        sqlite3_free(error);
    }

    destroy_stripe_tracker(state);
};

struct plugin_operations GUFI_PLUGIN_SYMBOL = {
    .type = PLUGIN_INDEX,
    .global_init = NULL,
    .ctx_init = db_init,
    .process_dir = process_dir,
    .process_file = process_file,
    .ctx_exit = db_exit,
    .global_exit = NULL,
};
