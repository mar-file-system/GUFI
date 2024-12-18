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



#ifndef GUFI_SQLITE3_HISTOGRAM_H
#define GUFI_SQLITE3_HISTOGRAM_H

#include <stddef.h>
#include <time.h>

#include <sqlite3.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Public API for parsing returned strings.
 *
 * These structs are intended for external use.
 *
 * Ignore the *_step and *_final functions. They are sqlite3 UDFs that
 * need to be exposed here to get linking to work for some reason.
 */

/* ********************************************* */
/*
 * log2 Histograms
 * log2_hist(input, bucket_count) ->  bucket_count;underflow;overflow;bucket1:count1;bucket2:count2;...
 *
 * Convert an input into a number and take the floor(log2()) of that
 * value. Strings and blobs are converted to lengths. Integers and
 * floats are not converted.
 *
 * The histogram contains buckets of rhe range [0, count). Each bucket
 * holds counts for values from [2^i, 2^(i+1)). There are also
 * underflow and overflow values for handling 0 values and values that
 * are larger than the expected range.
 *
 * The returned string only contains buckets with counts greater than
 * 0. The underflow and overflow counts are always returned even if
 * the counts are 0.
 */
typedef struct log2_hist {
    size_t count;
    size_t lt;       /* len == 0 */
    size_t *buckets; /* integers range [0, count) - use floor(log2(len)) to get bucket to increment */
    size_t ge;       /* len >= 2^count */
} log2_hist_t;

void log2_hist_step(sqlite3_context *context, int argc, sqlite3_value **argv);
void log2_hist_final(sqlite3_context *context);
log2_hist_t *log2_hist_parse(const char *str);
void log2_hist_free(log2_hist_t *hist);
/* ********************************************* */

/* ********************************************* */
/*
 * Mode (Permission) Histograms
 * mode_hist(mode) -> mode1:count1;mode2:count2;...
 *
 * Buckets are permission bits from 000 - 777.
 *
 * The returned string only contains permissions with counts greater
 * than 0.
 */
typedef struct mode_hist {
    size_t buckets[512];
} mode_hist_t;

void mode_hist_step(sqlite3_context *context, int argc, sqlite3_value **argv);
void mode_hist_final(sqlite3_context *context);
mode_hist_t *mode_hist_parse(const char *str);
void mode_hist_free(mode_hist_t *hist);
/* ********************************************* */

/* ********************************************* */
/*
 * Timestamp/Age Histograms
 * time_hist(timestamp, ref) -> ref;seconds1:count1;seconds2:count2;...
 *
 * The buckets where timestamps are counted are predefined.
 * The buckets represent [bucket[i - 1], bucket[i]) intervals.
 *
 * The returned string only contains timestamps with counts greater
 * than 0.
 */
/* used to define TIME_BUCKETS */
typedef struct time_bucket {
    const char name[16];
    time_t seconds;
} time_bucket_t;

static const time_bucket_t TIME_BUCKETS[] = {
    {"second",      1},
    {"minute",      60},
    {"hour",        3600},
    {"day",         86400},
    {"week",        604800},
    {"four_weeks",  2419200},
    {"year",        31536000},
    {"years",       0}, /* overflow value - keep last */
};

#define TIME_BUCKETS_COUNT (sizeof(TIME_BUCKETS) / sizeof(TIME_BUCKETS[0]))

typedef struct time_hist {
    size_t buckets[TIME_BUCKETS_COUNT];
    time_t ref;
} time_hist_t;

void time_hist_step(sqlite3_context *context, int argc, sqlite3_value **argv);
void time_hist_final(sqlite3_context *context);
time_hist_t *time_hist_parse(const char *str);
void time_hist_free(time_hist_t *hist);
/* ********************************************* */

/* ********************************************* */
/*
 * Generic Category Histograms
 * category_hist(string, keep_1) -> category_count;len1:category1:count1;len2:category2:count2;...
 *
 * Categories are not predefined. Instead, they are generated as
 * values are passed into category_hist. If none of the inputs match,
 * every single input will be a different category.
 *
 * If keep_1 is set to 0, the returned string only contains categories
 * with more than 1 count.
 */
typedef struct category_bucket {
    char *name;
    size_t len;
    size_t count;
} category_bucket_t;

typedef struct category_hist {
    category_bucket_t *buckets;
    size_t count;
} category_hist_t;

void category_hist_step(sqlite3_context *context, int argc, sqlite3_value **argv);
void category_hist_combine_step(sqlite3_context *context, int argc, sqlite3_value **argv);
void category_hist_final(sqlite3_context *context);
category_hist_t *category_hist_parse(const char *str);
category_hist_t *category_hist_combine(category_hist_t *lhs, category_hist_t *rhs);
void category_hist_free(category_hist_t *hist);
/* ********************************************* */

/* ********************************************* */
/*
 * Statistical Mode
 * mode_count(string) -> len:string:count or NULL
 *
 * This is here because mode is implemented using
 * category histograms.
 */
typedef struct mode_count {
    char *mode;
    size_t len;
    size_t count;
} mode_count_t;

void mode_count_final(sqlite3_context *context);
mode_count_t *mode_count_parse(const char *str);
void mode_count_free(mode_count_t *mc);
/* ********************************************* */

/* use this to add histogram functions to a sqlite database handle */
static inline int addhistfuncs(sqlite3 *db) {
    return (
        (sqlite3_create_function(db,   "log2_hist",             2,  SQLITE_UTF8,
                                 NULL, NULL,  log2_hist_step,             log2_hist_final)     == SQLITE_OK) &&
        (sqlite3_create_function(db,   "mode_hist",             1,  SQLITE_UTF8,
                                 NULL, NULL,  mode_hist_step,             mode_hist_final)     == SQLITE_OK) &&
        (sqlite3_create_function(db,   "time_hist",             2,  SQLITE_UTF8,
                                 NULL, NULL,  time_hist_step,             time_hist_final)     == SQLITE_OK) &&
        (sqlite3_create_function(db,   "category_hist",         2,  SQLITE_UTF8,
                                 NULL, NULL,  category_hist_step,         category_hist_final) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "category_hist_combine", 1,  SQLITE_UTF8,
                                 NULL, NULL,  category_hist_combine_step, category_hist_final) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "mode_count",            1,  SQLITE_UTF8,
                                 NULL, NULL,  category_hist_step,         mode_count_final)    == SQLITE_OK)
        );
}

#ifdef __cplusplus
}
#endif

#endif
