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



#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "SinglyLinkedList.h"
#include "bf.h"
#include "histogram.h"
#include "trie.h"
#include "utils.h"

#define DEFAULT_HIST_ALLOC 512

/* generic function for serializing buckets and making sure the data was written */
/* not static to allow for testing */
int serialize_bucket(sqlite3_context *context,
                     char **buf, char **curr,
                     size_t *size,
                     ssize_t (*serialize)(char *curr, const size_t avail,
                                          void *key, void *data),
                     void *key, void *data) {
    const size_t written = *curr - *buf;

    ssize_t avail = *size - written;
    ssize_t wrote = serialize(*curr, avail, key, data);

    /* not enough space */
    if (wrote >= avail) {
        /* increase buffer size */
        *size += 2 * wrote;
        char *new_ptr = realloc(*curr, *size);

        /* could not realloc, so try allocating new buffer */
        if (!new_ptr) {
            new_ptr = malloc(*size);
            memcpy(new_ptr, *buf, written);
            free(*buf);
        }

        *buf = new_ptr;
        *curr = *buf + written;
        avail = *size - written;

        /* try again */
        wrote = serialize(*curr, avail, key, data);

        /* give up */
        if (wrote >= avail) {
            sqlite3_result_error(context, "Could not allocate more space for histogram", -1);
            return 1;
        }
    }

    *curr += wrote;
    return 0;
}

/* log2_hist(string/value, bucket_count) */
static void log2_hist_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;
    log2_hist_t *hist = (log2_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));
    if (hist->buckets == NULL) {
        hist->count = sqlite3_value_int(argv[1]);
        hist->lt = 0;
        hist->buckets = calloc(hist->count, sizeof(hist->buckets[0]));
        hist->ge = 0;
    }

    size_t val = 0;

    switch (sqlite3_value_type(argv[0])) {
        /* if the input is a string, use the length of the string as the value */
        case SQLITE_TEXT:
        case SQLITE_BLOB:
            /* never NULL */
            val = strlen((const char *) sqlite3_value_text(argv[0]));
            break;
        case SQLITE_INTEGER:
            val = sqlite3_value_int(argv[0]);
            break;
        case SQLITE_FLOAT:
            val = (size_t) sqlite3_value_double(argv[0]);
            break;
        case SQLITE_NULL:
        default:
            break;
    }

    if (val < 1) {
        hist->lt++;
        return;
    }

    const size_t bucket = (size_t) log2(val);
    if (bucket >= hist->count) {
        hist->ge++;
    }
    else {
        hist->buckets[bucket]++;
    }
}

static ssize_t serialize_log2_bucket(char *curr, const size_t avail, void *key, void *data) {
    const size_t exp = (size_t) (uintptr_t) key;
    log2_hist_t *hist = (log2_hist_t *) data;
    return snprintf(curr, avail, "%zu:%zu;", exp, hist->buckets[exp]);
}

static void log2_hist_final(sqlite3_context *context) {
    log2_hist_t *hist = (log2_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));
    if (hist->buckets == NULL) {
        sqlite3_result_text(context, "0;0;0;", -1, SQLITE_TRANSIENT);
        return;
    }

    size_t size = DEFAULT_HIST_ALLOC;
    char *serialized = malloc(size);
    char *curr = serialized;

    curr += SNPRINTF(serialized, size, "%zu;%zu;%zu;", hist->count, hist->lt, hist->ge);

    /* add [0, count) if the count in the bucket is > 0 */
    for(size_t i = 0; i < hist->count; i++) {
        if (hist->buckets[i]) {
            if (serialize_bucket(context, &serialized, &curr, &size,
                                 serialize_log2_bucket, (void *) (uintptr_t) i, hist) != 0) {
                free(serialized);
                goto cleanup;
            }
        }
    }

    sqlite3_result_text(context, serialized, curr - serialized, free);

  cleanup:
    free(hist->buckets);
}

log2_hist_t *log2_hist_parse(const char *str) {
    const size_t len = strlen(str);

    log2_hist_t *hist = calloc(1, sizeof(*hist));
    int read = 0;
    if ((sscanf(str, "%zu;%zu;%zu;%n", &hist->count, &hist->lt, &hist->ge, &read) != 3) ||
        (str[read - 1] != ';')) {
        log2_hist_free(hist);
        return NULL;
    }

    hist->buckets = calloc(hist->count, sizeof(hist->buckets[0]));

    for(size_t total_read = read; total_read < len;) {
        size_t bucket, count;
        if ((sscanf(str + total_read, "%zu:%zu;%n", &bucket, &count, &read) != 2) ||
            (str[total_read + read - 1] != ';')) {
            log2_hist_free(hist);
            return NULL;
        }

        hist->buckets[bucket] = count;
        total_read += read;
    }

    return hist;
}

void log2_hist_free(log2_hist_t *hist) {
    free(hist->buckets);
    free(hist);
}

/* mode_hist(mode) */
static void mode_hist_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;
    mode_hist_t *hist = (mode_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));
    const mode_t mode = (mode_t) sqlite3_value_int(argv[0]) & 0777;
    hist->buckets[mode]++;
}

static ssize_t serialize_mode_bucket(char *curr, const size_t avail, void *key, void *data) {
    const size_t mode = (size_t) (uintptr_t) key;
    mode_hist_t *hist = (mode_hist_t *) data;
    return snprintf(curr, avail, "%03o:%zu;", (unsigned int) mode, hist->buckets[mode]);
}

static void mode_hist_final(sqlite3_context *context) {
    mode_hist_t *hist = (mode_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));

    size_t size = DEFAULT_HIST_ALLOC;
    char *serialized = malloc(size);
    char *curr = serialized;

    /* add [000, 777] if the count in the bucket is > 0 */
    for(size_t mode = 0; mode < 01000; mode++) {
        if (hist->buckets[mode]) {
            if (serialize_bucket(context, &serialized, &curr, &size,
                                 serialize_mode_bucket, (void *) (uintptr_t) mode, hist) != 0) {
                free(serialized);
                return;
            }
        }
    }

    sqlite3_result_text(context, serialized, curr - serialized, free);
}

mode_hist_t *mode_hist_parse(const char *str) {
    const size_t len = strlen(str);

    mode_hist_t *hist = calloc(1, sizeof(*hist));
    for(size_t total_read = 0; total_read < len;) {
        unsigned int bucket;
        size_t count;
        int read = 0;
        if ((sscanf(str + total_read, "%03o:%zu;%n", &bucket, &count, &read) != 2) ||
            (str[total_read + read - 1] != ';')) {
            mode_hist_free(hist);
            return NULL;
        }

        hist->buckets[bucket] = count;
        total_read += read;
    }

    return hist;
}

void mode_hist_free(mode_hist_t *hist) {
    free(hist);
}

/* time_hist(time, ref) */
typedef struct sqlite_time_hist {
    size_t buckets[TIME_BUCKETS_COUNT];
    time_t ref;
    int init;
} sqlite_time_hist_t;

static void time_hist_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;
    sqlite_time_hist_t *hist = (sqlite_time_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));
    if (hist->init == 0) {
        hist->ref = sqlite3_value_int64(argv[1]);
        hist->init = 1;
    }

    const time_t timestamp = sqlite3_value_int64(argv[0]);
    const time_t dt = hist->ref - timestamp;
    int found = 0;
    for(size_t i = 0; i < (TIME_BUCKETS_COUNT - 1); i++) {
        if (dt < TIME_BUCKETS[i].seconds) {
            hist->buckets[i]++;
            found = 1;
            break;
        }
    }

    if (!found) {
        hist->buckets[TIME_BUCKETS_COUNT - 1]++;
    }
}

static ssize_t serialize_time_bucket(char *curr, const size_t avail, void *key, void *data) {
    const time_t bucket = (time_t) (uintptr_t) key;
    time_hist_t *hist = data;
    return snprintf(curr, avail, "%zu:%zu;", (size_t) TIME_BUCKETS[bucket].seconds, hist->buckets[bucket]);
}

static void time_hist_final(sqlite3_context *context) {
    sqlite_time_hist_t *hist = (sqlite_time_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));

    if (hist->init == 0) {
        sqlite3_result_text(context, "0;", -1, SQLITE_TRANSIENT);
        return;
    }

    size_t size = DEFAULT_HIST_ALLOC;
    char *serialized = malloc(size);
    char *curr = serialized;

    curr += SNPRINTF(serialized, size, "%zu;", hist->ref);

    /* add counts if count > 0 */
    for(size_t i = 0; i < TIME_BUCKETS_COUNT; i++) {
        if (hist->buckets[i]) {
            if (serialize_bucket(context, &serialized, &curr, &size,
                                 serialize_time_bucket, (void *) (uintptr_t) i, hist) != 0) {
                free(serialized);
                return;
            }
        }
    }

    sqlite3_result_text(context, serialized, curr - serialized, free);
}

time_hist_t *time_hist_parse(const char *str) {
    const size_t len = strlen(str);

    time_hist_t *hist = calloc(1, sizeof(*hist));

    size_t ref = 0;
    int read = 0;
    if ((sscanf(str, "%zu;%n", &ref, &read) != 1) ||
        (str[read - 1] != ';')) {
        time_hist_free(hist);
        return NULL;
    }

    hist->ref = (time_t) ref;

    for(size_t total_read = read; total_read < len;) {
        size_t bucket, count;
        if ((sscanf(str + total_read, "%zu:%zu;%n", &bucket, &count, &read) != 2) ||
            (str[total_read + read - 1] != ';')) {
            time_hist_free(hist);
            return NULL;
        }

        int found = 0;
        for(size_t i = 0; i < (TIME_BUCKETS_COUNT - 1); i++) {
            if ((time_t) bucket == TIME_BUCKETS[i].seconds) {
                hist->buckets[i] = count;
                found = 1;
                break;
            }
        }
        if (!found) {
            if ((time_t) bucket != TIME_BUCKETS[TIME_BUCKETS_COUNT - 1].seconds) {
                time_hist_free(hist);
                return NULL;
            }

            hist->buckets[TIME_BUCKETS_COUNT - 1] = count;
        }

        total_read += read;
    }

    return hist;
}

void time_hist_free(time_hist_t *hist) {
    free(hist);
}

/* category_hist(string) */
typedef struct sqlite_category_hist {
    trie_t *trie;
    sll_t categories; /* categories are collected as they appear, not preset */
} sqlite_category_hist_t;

static void category_hist_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;
    sqlite_category_hist_t *hist = (sqlite_category_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));
    if (!hist->trie) {
        hist->trie = trie_alloc();
        sll_init(&hist->categories);
    }

    const char *cat = (char *) sqlite3_value_text(argv[0]);

    if (!cat) {
        return;
    }

    const size_t len = strlen(cat);
    size_t *count = NULL;
    if (trie_search(hist->trie, cat, len, (void **) &count)) {
        (*count)++;
    }
    else {
        count = malloc(sizeof(*count));
        *count = 1;
        trie_insert(hist->trie, cat, len, count, free);

        /* use refstr_t as a real string */
        refstr_t *cat_str = malloc(sizeof(*cat_str));
        cat_str->len = len;
        cat_str->data = malloc(cat_str->len + 1);
        memcpy((char *) cat_str->data, cat, len);
        ((char *) cat_str->data)[cat_str->len] = '\0';

        sll_push(&hist->categories, cat_str);
    }
}

typedef struct category {
    refstr_t name;
    size_t count;
} category_t;

static ssize_t serialize_category_bucket(char *curr, const size_t avail, void *key, void *data) {
    (void) key;
    category_t *cat = data;
    return snprintf(curr, avail, "%zu:%s:%zu;", cat->name.len, cat->name.data, cat->count);
}

static void free_str(void *ptr) {
    refstr_t *str = ptr;
    free((char *) str->data);
    free(str);
}

static void category_hist_final(sqlite3_context *context) {
    sqlite_category_hist_t *hist = (sqlite_category_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));

    size_t size = DEFAULT_HIST_ALLOC;
    char *serialized = malloc(size);
    char *curr = serialized;

    sll_t gt1;
    sll_init(&gt1);

    /* keep categories with counts > 1 */
    sll_loop(&hist->categories, node) {
        refstr_t *cat = sll_node_data(node);

        size_t *count = NULL;

        /* not checking return value since all categories should be found */
        trie_search(hist->trie, cat->data, cat->len, (void **) &count);

        /*
         * everything that shows up must have a count of at least 1,
         * so adding categories that show up once is not useful
         */
        if (*count > 1) {
            category_t *ref = malloc(sizeof(*ref));
            ref->name = *cat;
            ref->count = *count;

            sll_push(&gt1, ref);
        }
    }

    curr += SNPRINTF(serialized, size, "%zu;", sll_get_size(&gt1));

    /* serialize categories with counts > 1 */
    sll_loop(&gt1, node) {
        category_t *ref = sll_node_data(node);

        if (serialize_bucket(context, &serialized, &curr, &size,
                             serialize_category_bucket, NULL, ref) != 0) {
            free(serialized);
            goto cleanup;
        }
    }

    sqlite3_result_text(context, serialized, curr - serialized, free);

  cleanup:
    sll_destroy(&gt1, free);
    sll_destroy(&hist->categories, free_str);
    trie_free(hist->trie);
}

static int category_bucket_cmp(const void *lhs, const void *rhs) {
    category_bucket_t *l = (category_bucket_t *) lhs;
    category_bucket_t *r = (category_bucket_t *) rhs;

    /* sort by ocunt, descending */
    if (l->count > r->count) {
        return -1;
    }
    else if (l->count < r->count) {
        return 1;
    }

    /* lexicographic name sort; use max len to hit null terminator */
    return strncmp(l->name, r->name, (l->len > r->len)?l->len:r->len);
}

category_hist_t *category_hist_parse(const char *str) {
    category_hist_t *hist = calloc(1, sizeof(*hist));

    int total_read = 0;
    if ((sscanf(str, "%zu;%n", &hist->count, &total_read) != 1) ||
        (str[total_read - 1] != ';')) {
        category_hist_free(hist);
        return NULL;
    }

    hist->buckets = calloc(hist->count, sizeof(hist->buckets[0]));

    for(size_t i = 0; i < hist->count; i++) {
        category_bucket_t *bucket = &hist->buckets[i];

        int read = 0;
        if ((sscanf(str + total_read, "%zu:%n", &bucket->len, &read) != 1) ||
            (str[total_read + read - 1] != ':')) {
            category_hist_free(hist);
            return NULL;
        }

        bucket->name = malloc(bucket->len + 1);
        total_read += read;

        if ((sscanf(str + total_read, "%[^:]:%zu;%n", bucket->name, &bucket->count, &read) != 2) ||
            (str[total_read + read - 1] != ';')) {
            category_hist_free(hist);
            return NULL;
        }

        total_read += read;
    }

    const size_t len = strlen(str);
    if ((size_t) total_read != len) {
        fprintf(stderr, "Warning: Read first %d bytes of category histogram string: '%s' (%zu bytes)\n",
                total_read, str, len);
    }

    qsort(hist->buckets, hist->count, sizeof(hist->buckets[0]), category_bucket_cmp);

    return hist;
}

void category_hist_free(category_hist_t *hist) {
    if (hist->buckets) {
        for(size_t i = 0; i < hist->count; i++) {
            free(hist->buckets[i].name);
        }
        free(hist->buckets);
    }
    free(hist);
}

static ssize_t serialize_mode(char *curr, const size_t avail, void *key, void *data) {
    refstr_t *mode = (refstr_t *) key;
    size_t *count = (size_t *) data;
    return snprintf(curr, avail, "%zu:%s:%zu;", mode->len, mode->data, *count);
}

static void mode_count_final(sqlite3_context *context) {
    sqlite_category_hist_t *hist = (sqlite_category_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));

    if (!hist->trie) {
        hist->trie = trie_alloc();
        sll_init(&hist->categories);
    }

    refstr_t mode_key;
    memset(&mode_key, 0, sizeof(mode_key));

    size_t mode_count = 0;

    sll_loop(&hist->categories, node) {
        refstr_t *cat = sll_node_data(node);

        size_t *count = NULL;

        /* not checking return value since all categories should be found */
        trie_search(hist->trie, cat->data, cat->len, (void **) &count);

        /* not stable - gets and keeps first one of a count */
        if (*count > mode_count) {
            mode_key.data = cat->data;
            mode_key.len = cat->len;
            mode_count = *count;
        }
    }

    if (mode_count == 0) {
        /* return NULL instead of erroring */
        sqlite3_result_null(context);
        goto cleanup;
    }

    size_t size = DEFAULT_HIST_ALLOC;
    char *serialized = malloc(size);
    char *curr = serialized;

    if (serialize_bucket(context, &serialized, &curr, &size,
                         serialize_mode, (void *) &mode_key, &mode_count) != 0) {
        free(serialized);
        goto cleanup;
    }

    sqlite3_result_text(context, serialized, curr - serialized, free);

  cleanup:
    sll_destroy(&hist->categories, free_str);
    trie_free(hist->trie);
}

mode_count_t *mode_count_parse(const char *str) {
    mode_count_t mc;

    int read = 0;
    if ((sscanf(str, "%zu:%n", &mc.len, &read) != 1) ||
        (str[read - 1] != ':')) {
        return NULL;
    }

    int total_read = read;

    mc.mode = malloc(mc.len + 1);
    if ((sscanf(str + total_read, "%[^:]:%zu;%n", mc.mode, &mc.count, &read) != 2) ||
        (str[total_read + read - 1] != ';')) {
        free(mc.mode);
        return NULL;
    }

    mode_count_t *ret = malloc(sizeof(*ret));
    memcpy(ret, &mc, sizeof(mc));
    return ret;
}

void mode_count_free(mode_count_t *mc) {
    free(mc->mode);
    free(mc);
}

int addhistfuncs(sqlite3 *db) {
    return (
        (sqlite3_create_function(db,   "log2_hist",           2,  SQLITE_UTF8,
                                 NULL, NULL,  log2_hist_step,     log2_hist_final)     == SQLITE_OK) &&
        (sqlite3_create_function(db,   "mode_hist",           1,  SQLITE_UTF8,
                                 NULL, NULL,  mode_hist_step,     mode_hist_final)     == SQLITE_OK) &&
        (sqlite3_create_function(db,   "time_hist",           2,  SQLITE_UTF8,
                                 NULL, NULL,  time_hist_step,     time_hist_final)     == SQLITE_OK) &&
        (sqlite3_create_function(db,   "category_hist",       1,  SQLITE_UTF8,
                                 NULL, NULL,  category_hist_step, category_hist_final) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "mode_count",          1,  SQLITE_UTF8,
                                 NULL, NULL,  category_hist_step, mode_count_final)    == SQLITE_OK)
        );
}
