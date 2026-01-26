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

        char *new_ptr = realloc(*buf, *size);
        if (!new_ptr) {
            if (context) {
                sqlite3_result_error(context, "Could not allocate more space for histogram", -1);
            }
            return 1;
        }

        *buf = new_ptr;
        *curr = *buf + written;
        avail = *size - written;
    }

    *curr += wrote;
    return 0;
}

static log_hist_t *log_hist_init(sqlite3_context *context, log_hist_t *hist,
                                 const size_t base, const size_t bucket_count) {
    if (base < 2) {
        if (context) {
            char buf[1024];
            const size_t len = SNPRINTF(buf, sizeof(buf), "Error: Bad log histogram base: %zu\n", base);
            sqlite3_result_error(context, buf, len);
        }
        return NULL;
    }

    hist->base = base;
    hist->conv = log2(hist->base);
    hist->count = bucket_count;
    hist->lt = 0;
    hist->buckets = calloc(hist->count, sizeof(hist->buckets[0]));
    hist->ge = 0;

    return hist;
}

static int log_hist_insert(log_hist_t *hist, const uint64_t value) {
    /* not checking base - bad histograms should not reach here */

    if (value < 1) {
        hist->lt++;
        return 0;
    }

    const size_t bucket = (size_t) (log2(value) / hist->conv);
    if (bucket >= hist->count) {
        hist->ge++;
    }
    else {
        hist->buckets[bucket]++;
    }

    return 0;
}

/* log_hist(string/value, bucket_count) */
static void log_hist_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    log_hist_t *hist = (log_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));
    if (hist->base == 0) {
        const size_t base = sqlite3_value_int(argv[1]);
        const size_t bucket_count = sqlite3_value_int(argv[2]);
        if (!log_hist_init(context, hist, base, bucket_count)) {
            return;
        }
    }

    uint64_t val = 0;
    int insert = 1; /* if the raw value is less than 0, don't insert (allows for link sizes to not be inserted) */

    switch (sqlite3_value_type(argv[0])) {
        /* if the input is a string, use the length of the string as the value */
        case SQLITE_TEXT:
        case SQLITE_BLOB:
            /* never NULL */
            val = (uint64_t) strlen((const char *) sqlite3_value_text(argv[0]));
            break;
        case SQLITE_INTEGER:
            {
                const int i = sqlite3_value_int(argv[0]);
                val = (uint64_t) i;
                insert = (i >= 0);
            }
            break;
        case SQLITE_FLOAT:
            {
                const double d = sqlite3_value_double(argv[0]);
                val = (uint64_t) d;
                insert = (d >= 0);
            }
            break;
        case SQLITE_NULL:
        default:
            /* insert a 0 */
            break;
    }

    if (insert) {
        log_hist_insert(hist, val);
    }
}

static ssize_t serialize_log_bucket(char *curr, const size_t avail, void *key, void *data) {
    const size_t exp = (size_t) (uintptr_t) key;
    log_hist_t *hist = (log_hist_t *) data;
    return snprintf(curr, avail, "%zu:%zu;", exp, hist->buckets[exp]);
}

static char *log_hist_serialize(log_hist_t *hist, sqlite3_context *context) {
    size_t size = DEFAULT_HIST_ALLOC;
    char *serialized = malloc(size);

    if (hist->base == 0) {
        SNFORMAT_S(serialized, size, 1,
                   "0;0;0;0;", (size_t) 8);
        return serialized;
    }

    char *curr = serialized;

    curr += SNPRINTF(serialized, size, "%zu;%zu;%zu;%zu;", hist->base, hist->count, hist->lt, hist->ge);

    /* add [0, count) if the count in the bucket is > 0 */
    for(size_t i = 0; i < hist->count; i++) {
        if (hist->buckets[i]) {
            if (serialize_bucket(context, &serialized, &curr, &size,
                                 serialize_log_bucket, (void *) (uintptr_t) i, hist) != 0) {
                free(serialized);
                return NULL;
            }
        }
    }

    return serialized;
}

static void log_hist_final(sqlite3_context *context) {
    log_hist_t *hist = (log_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));

    char *serialized = log_hist_serialize(hist, context);
    if (serialized) {
        sqlite3_result_text(context, serialized, -1, free);
    }

    free(hist->buckets);
}

log_hist_t *log_hist_parse(const char *str) {
    const size_t len = strlen(str);

    log_hist_t *hist = calloc(1, sizeof(*hist));
    int read = 0;
    if ((sscanf(str, "%zu;%zu;%zu;%zu;%n", &hist->base, &hist->count, &hist->lt, &hist->ge, &read) != 4) ||
        (str[read - 1] != ';')) {
        log_hist_free(hist);
        return NULL;
    }

    if (hist->base) {
        hist->buckets = calloc(hist->count, sizeof(hist->buckets[0]));

        for(size_t total_read = read; total_read < len;) {
            size_t bucket, count;
            if ((sscanf(str + total_read, "%zu:%zu;%n", &bucket, &count, &read) != 2) ||
                (str[total_read + read - 1] != ';')) {
                log_hist_free(hist);
                return NULL;
            }

            hist->buckets[bucket] = count;
            total_read += read;
        }
    }

    return hist;
}

static int log_hist_combine_inplace(log_hist_t *lhs, log_hist_t *rhs) {
    /* inserted 0 values */
    if (rhs->base == 0) {
        return 0;
    }

    /* handle lhs unset before lhs == rhs, which would be true */
    if (lhs->base == 0) {
        free(lhs->buckets); /* in case multple rhs' have no data */
        log_hist_init(NULL, lhs, rhs->base, rhs->count);
    }
    else if (lhs->base != rhs->base) {
        fprintf(stderr, "Error: Cannot combine log histograms with different bases: %zu vs %zu\n",
                lhs->base, rhs->base);
        return 1;

    }
    else if (lhs->count != rhs->count) {
        /*
         * possible other handling: set new histogram size to smaller
         * histogram and put buckets larger than smaller histogram in
         * overflow
         */
        fprintf(stderr, "Error: Cannot combine log histograms with different bucket counts: %zu vs %zu\n",
                lhs->count, rhs->count);
        return 1;
    }

    lhs->lt += rhs->lt;
    lhs->ge += rhs->ge;
    for(size_t i = 0; i < lhs->count; i++) {
        lhs->buckets[i] += rhs->buckets[i];
    }

    return 0;
}

/* add a histogram into an existing histogram */
static void log_hist_combine_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argv; (void) argc;

    const char *new_hist_str = (char *) sqlite3_value_text(argv[0]);
    if (!new_hist_str) {
        sqlite3_result_error(context, "Missing log histograms", -1);
        return;
    }

    log_hist_t *new_hist = log_hist_parse(new_hist_str);
    if (!new_hist) {
        sqlite3_result_error(context, "Could not parse log histogram", -1);
        return;
    }

    log_hist_t *hist = (log_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));
    if (!hist->buckets) {
        hist->base = new_hist->base;
        hist->count = new_hist->count;
        hist->buckets = calloc(hist->count, sizeof(hist->buckets[0]));
    }

    const int rc = log_hist_combine_inplace(hist, new_hist);
    log_hist_free(new_hist);

    if (rc != 0) {
        sqlite3_result_error(context, "Could not combine log histograms", -1);
    }
}

void log_hist_free(log_hist_t *hist) {
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

static void mode_hist_combine_inplace(mode_hist_t *lhs, mode_hist_t *rhs) {
    for(size_t i = 0; i < 512; i++) {
        lhs->buckets[i] += rhs->buckets[i];
    }
}

/* add a histogram into an existing histogram */
static void mode_hist_combine_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argv; (void) argc;

    const char *new_hist_str = (char *) sqlite3_value_text(argv[0]);
    if (!new_hist_str) {
        sqlite3_result_error(context, "Missing mode histogram", -1);
        return;
    }

    mode_hist_t *new_hist = mode_hist_parse(new_hist_str);
    if (!new_hist) {
        sqlite3_result_error(context, "Could not parse mode histogram", -1);
        return;
    }

    mode_hist_t *hist = (mode_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));
    mode_hist_combine_inplace(hist, new_hist);
    mode_hist_free(new_hist);
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

static int time_hist_combine_inplace(sqlite_time_hist_t *lhs, time_hist_t *rhs) {
    if (lhs->ref != rhs->ref) {
        fprintf(stderr, "Error: Cannot combine time histograms with different reference times: %ld vs %ld\n",
                lhs->ref, rhs->ref);
        return 1;
    }

    for(size_t i = 0; i < TIME_BUCKETS_COUNT; i++) {
        lhs->buckets[i] += rhs->buckets[i];
    }

    return 0;
}

/* add a histogram into an existing histogram */
static void time_hist_combine_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argv; (void) argc;

    const char *new_hist_str = (char *) sqlite3_value_text(argv[0]);
    if (!new_hist_str) {
        sqlite3_result_error(context, "Missing time histogram", -1);
        return;
    }

    time_hist_t *new_hist = time_hist_parse(new_hist_str);
    if (!new_hist) {
        sqlite3_result_error(context, "Could not parse time histogram", -1);
        return;
    }

    sqlite_time_hist_t *hist = (sqlite_time_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));
    if (!hist->init) {
        hist->ref = new_hist->ref;
        hist->init = 1;
    }

    const int rc = time_hist_combine_inplace(hist, new_hist);
    time_hist_free(new_hist);

    if (rc != 0) {
        sqlite3_result_error(context, "Could not combine time histograms", -1);
    }
}

void time_hist_free(time_hist_t *hist) {
    free(hist);
}

/* category_hist(string, keep_1) */
typedef struct sqlite_category_hist {
    trie_t *trie;
    sll_t categories; /* categories are collected as they appear, not preset */
    int keep_1;
} sqlite_category_hist_t;

static void category_hist_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;
    sqlite_category_hist_t *hist = (sqlite_category_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));
    if (!hist->trie) {
        hist->trie = trie_alloc();
        sll_init(&hist->categories);
        if (argc > 1) { /* mode_count does not pass in second arg */
            hist->keep_1 = sqlite3_value_int(argv[1]);
        }
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
        cat_str->data = malloc(len + 1);
        cat_str->len = SNFORMAT_S((char *) cat_str->data, len + 1, 1,
                                  cat, len);

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
    if (!hist->trie) {
        hist->trie = trie_alloc();
        sll_init(&hist->categories);
        hist->keep_1 = 0;
    }

    sll_t keep;
    sll_init(&keep);

    sll_loop(&hist->categories, node) {
        refstr_t *cat = sll_node_data(node);

        size_t *count = NULL;

        /* not checking return value since all categories should be found */
        trie_search(hist->trie, cat->data, cat->len, (void **) &count);

        if (hist->keep_1 ||                    /* keep everything */
            (!hist->keep_1 && (*count > 1))) { /* keep categories with counts > 1 */
            category_t *ref = malloc(sizeof(*ref));
            ref->name = *cat;
            ref->count = *count;

            sll_push(&keep, ref);
        }
    }

    /* serialize */
    size_t size = DEFAULT_HIST_ALLOC;
    char *serialized = malloc(size);
    char *curr = serialized;

    curr += SNPRINTF(serialized, size, "%zu;", sll_get_size(&keep));

    sll_loop(&keep, node) {
        category_t *ref = sll_node_data(node);

        if (serialize_bucket(context, &serialized, &curr, &size,
                             serialize_category_bucket, NULL, ref) != 0) {
            free(serialized);
            goto cleanup;
        }
    }

    sqlite3_result_text(context, serialized, curr - serialized, free);

  cleanup:
    sll_destroy(&keep, free);
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

static void category_hist_combine_inplace(sqlite_category_hist_t *accumulator,
                                          category_hist_t *hist,
                                          int check_exists) {
    for(size_t i = 0; i < hist->count; i++) {
        category_bucket_t *bucket = &hist->buckets[i];

        size_t *count = NULL;
        if (check_exists &&
            (trie_search(accumulator->trie, bucket->name, bucket->len, (void **) &count) == 1)) {
            *count += bucket->count;
        }
        else {
            count = malloc(sizeof(*count));
            *count = bucket->count;

            /* copy the category */
            /* use refstr_t as a real string */
            refstr_t *cat_str = malloc(sizeof(*cat_str));
            cat_str->data = malloc(bucket->len + 1);
            cat_str->len = SNFORMAT_S((char *) cat_str->data, bucket->len + 1, 1,
                                      bucket->name, bucket->len);

            trie_insert(accumulator->trie, bucket->name, bucket->len, count, free);
            sll_push(&accumulator->categories, cat_str);
        }
    }
}

/* combine 2 histograms into a new category_hist_t */
category_hist_t *category_hist_combine(category_hist_t *lhs, category_hist_t *rhs) {
    sqlite_category_hist_t accumulator;
    accumulator.trie = trie_alloc();
    sll_init(&accumulator.categories);

    /* combine */
    category_hist_combine_inplace(&accumulator, lhs, 0);
    category_hist_combine_inplace(&accumulator, rhs, 1);

    /* output */
    category_hist_t *hist = malloc(sizeof(*hist));
    hist->count = 0;
    hist->buckets = calloc(sll_get_size(&accumulator.categories), sizeof(hist->buckets[0]));

    sll_loop(&accumulator.categories, node) {
        category_bucket_t *ref = (category_bucket_t *) sll_node_data(node);
        category_bucket_t *bucket = &hist->buckets[hist->count++];

        size_t *count = NULL;

        /* not checking return value since all categories should be found */
        trie_search(accumulator.trie, ref->name, ref->len, (void **) &count);

        bucket->name = malloc(ref->len + 1);
        memcpy(bucket->name, ref->name, ref->len);
        bucket->name[ref->len] = '\0';
        bucket->len = ref->len;
        bucket->count = *count;
    }

    sll_destroy(&accumulator.categories, free_str);
    trie_free(accumulator.trie);
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

/* add a histogram into an existing histogram */
static void category_hist_combine_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argv; (void) argc;
    sqlite_category_hist_t *hist = (sqlite_category_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));
    if (!hist->trie) {
        hist->trie = trie_alloc();
        sll_init(&hist->categories);
        hist->keep_1 = 1;
    }

    const char *new_hist_str = (char *) sqlite3_value_text(argv[0]);
    if (!new_hist_str) {
        sqlite3_result_error(context, "Missing category histogram", -1);
        return;
    }

    category_hist_t *new_hist = category_hist_parse(new_hist_str);
    if (!new_hist) {
        sqlite3_result_error(context, "Could not parse category histogram", -1);
        return;
    }

    category_hist_combine_inplace(hist, new_hist, 1);
    category_hist_free(new_hist);
}

static ssize_t serialize_mode(char *curr, const size_t avail, void *key, void *data) {
    refstr_t *mode = (refstr_t *) key;
    size_t *count = (size_t *) data;
    return snprintf(curr, avail, "%zu:%s:%zu;", mode->len, mode->data, *count);
}

static void mode_count_final(sqlite3_context *context) {
    sqlite_category_hist_t *hist = (sqlite_category_hist_t *) sqlite3_aggregate_context(context, sizeof(*hist));

    if (!hist->trie) {
        sqlite3_result_null(context);
        return;
    }

    /* move categories into array for sorting */
    category_t *categories = malloc(sll_get_size(&hist->categories) * sizeof(*categories));
    size_t cat_count = 0;
    sll_loop(&hist->categories, node) {
        refstr_t *cat = sll_node_data(node);

        size_t *count = NULL;

        /* not checking return value since all categories should be found */
        trie_search(hist->trie, cat->data, cat->len, (void **) &count);

        /* copy reference into array */
        categories[cat_count].name = *cat;
        categories[cat_count].count = *count;
        cat_count++;
    }

    if (cat_count == 0) {
        sqlite3_result_null(context);
        goto cleanup;
    }

    /* there must be at least 1 category at this point */

    /* sort by count descending */
    qsort(categories, cat_count, sizeof(categories[0]), category_bucket_cmp);

    /* this is the mode */
    category_t *top = &categories[0];

    if (cat_count > 1) {
        category_t *next = &categories[1];

        /* multiple modes -> no mode */
        if (top->count == next->count) {
            sqlite3_result_null(context);
            goto cleanup;
        }
    }

    size_t size = DEFAULT_HIST_ALLOC;
    char *serialized = malloc(size);
    char *curr = serialized;

    if (serialize_bucket(context, &serialized, &curr, &size,
                         serialize_mode, (void *) &top->name, &top->count) != 0) {
        free(serialized);
        goto cleanup;
    }

    sqlite3_result_text(context, serialized, curr - serialized, free);

  cleanup:
    free(categories);
    sll_destroy(&hist->categories, free_str);
    trie_free(hist->trie);
}

mode_count_t *mode_count_parse(const char *str) {
    if (!str || !strlen(str)) {
        return NULL;
    }

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
        (sqlite3_create_function(db,   "log_hist",              3,  SQLITE_UTF8,
                                 NULL, NULL,  log_hist_step,             log_hist_final)       == SQLITE_OK) &&
        (sqlite3_create_function(db,   "log_hist_combine",      1,  SQLITE_UTF8,
                                 NULL, NULL,  log_hist_combine_step,     log_hist_final)       == SQLITE_OK) &&
        (sqlite3_create_function(db,   "mode_hist",             1,  SQLITE_UTF8,
                                 NULL, NULL,  mode_hist_step,             mode_hist_final)     == SQLITE_OK) &&
        (sqlite3_create_function(db,   "mode_hist_combine",     1,  SQLITE_UTF8,
                                 NULL, NULL,  mode_hist_combine_step,     mode_hist_final)     == SQLITE_OK) &&
        (sqlite3_create_function(db,   "time_hist",             2,  SQLITE_UTF8,
                                 NULL, NULL,  time_hist_step,             time_hist_final)     == SQLITE_OK) &&
        (sqlite3_create_function(db,   "time_hist_combine",     1,  SQLITE_UTF8,
                                 NULL, NULL,  time_hist_combine_step,     time_hist_final)     == SQLITE_OK) &&
        (sqlite3_create_function(db,   "category_hist",         2,  SQLITE_UTF8,
                                 NULL, NULL,  category_hist_step,         category_hist_final) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "category_hist_combine", 1,  SQLITE_UTF8,
                                 NULL, NULL,  category_hist_combine_step, category_hist_final) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "mode_count",            1,  SQLITE_UTF8,
                                 NULL, NULL,  category_hist_step,         mode_count_final)    == SQLITE_OK)
        );
}
