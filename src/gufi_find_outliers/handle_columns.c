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



#include <inttypes.h>
#include <math.h>
#include <stddef.h>
#include <string.h>

#include "bf.h"
#include "dbutils.h"
#include "utils.h"

#include "gufi_find_outliers/DirData.h"
#include "gufi_find_outliers/handle_columns.h"

static double stdev_from_parts(const double totsq, const double tot, const size_t n, const int is_sample) {
    const double variance = ((n * totsq) - (tot * tot)) / (n * (n - !!is_sample));
    return sqrt(variance);
}

/* ************************************* */
static str_t *gen_t_only_sql(str_t *dst, const char *col) {
    str_alloc_existing(dst, MAXSQL);
    dst->len = SNPRINTF(dst->data, dst->len,
                        "SELECT t.%s "
                        "FROM   %s AS t "
                        "       INNER JOIN "
                        "       %s AS s "
                        "       ON (t.inode == s.inode) AND (s.isroot == 1) "
                        ";",
                        col, TREESUMMARY, SUMMARY);
    return dst;
}

static int get_t_only_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    DirData_t *dd = (DirData_t *) args;
    return !(sscanf(data[0], "%lf", &dd->t.value) == 1);
}

/* compute partial sample stdev for single value columns */
static void compute_t_only_mean_stdev(sll_t *subdirs,
                                      double *t_mean, double *t_stdev,
                                      double *s_mean, double *s_stdev) {
    double t_tot   = 0;
    double t_totsq = 0;
    sll_loop(subdirs, node) {
        DirData_t *dd = (DirData_t *) sll_node_data(node);

        t_tot   += dd->t.value;
        t_totsq += ((double) dd->t.value) * dd->t.value;
    }

    const uint64_t n = sll_get_size(subdirs);

    *t_mean  = t_tot / n;
    *t_stdev = stdev_from_parts(t_totsq, t_tot, n, 0);

    *s_mean = 0;
    *s_stdev = 0;
}
/* ************************************* */

/* common functions for pulling one value out of treesummary and summary each */
static str_t *gen_ts_sql(str_t *dst, const char *col) {
    str_alloc_existing(dst, MAXSQL);
    dst->len = SNPRINTF(dst->data, dst->len,
                        "SELECT t.%s, "
                        "       s.%s  "
                        "FROM   %s AS t "
                        "       INNER JOIN "
                        "       %s AS s "
                        "       ON (t.inode == s.inode) AND (s.isroot == 1) "
                        ";",
                        col, col, TREESUMMARY, SUMMARY);
    return dst;
}

static int get_ts_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    DirData_t *dd = (DirData_t *) args;

    return !((sscanf(data[0], "%lf", &dd->t.value) == 1) &&
             (sscanf(data[1], "%lf", &dd->s.value) == 1));
}
/* ************************************* */

/* ************************************* */
/* compute partial sample stdev for single value columns */
static void compute_minmax_mean_stdev(sll_t *subdirs,
                                      double *t_mean, double *t_stdev,
                                      double *s_mean, double *s_stdev) {
    double t_tot   = 0;
    double t_totsq = 0;
    double s_tot   = 0;
    double s_totsq = 0;
    sll_loop(subdirs, node) {
        DirData_t *dd = (DirData_t *) sll_node_data(node);

        t_tot   += dd->t.value;
        t_totsq += ((double) dd->t.value) * dd->t.value;

        s_tot   += dd->s.value;
        s_totsq += ((double) dd->s.value) * dd->s.value;
    }

    const uint64_t n = sll_get_size(subdirs);

    *t_mean  = t_tot / n;
    *t_stdev = stdev_from_parts(t_totsq, t_tot, n, 0);

    *s_mean  = s_tot / n;
    *s_stdev = stdev_from_parts(s_totsq, s_tot, n, 0);
}
/* ************************************* */

/* ************************************* */
/* compute partial sample stdev for sum columns */
static void compute_tot_mean_stdev(sll_t *subdirs,
                                   double *t_mean, double *t_stdev,
                                   double *s_mean, double *s_stdev) {
    double t_tot   = 0;
    double t_totsq = 0;
    double s_tot   = 0;
    double s_totsq = 0;
    sll_loop(subdirs, node) {
        DirData_t *dd = (DirData_t *) sll_node_data(node);

        t_tot   += dd->t.value;
        t_totsq += ((double) dd->t.value) * dd->t.value;

        s_tot   += dd->s.value;
        s_totsq += ((double) dd->s.value) * dd->s.value;
    }

    const uint64_t n = sll_get_size(subdirs);

    *t_mean  = t_tot / n;
    *t_stdev = stdev_from_parts(t_totsq, t_tot, n, 0);

    *s_mean  = s_tot / n;
    *s_stdev = stdev_from_parts(s_totsq, s_tot, n, 0);
}
/* ************************************* */

/* ************************************* */
/* compute partial sample stdev for time columns */
static void compute_time_mean_stdev(sll_t *subdirs,
                                    double *t_mean, double *t_stdev,
                                    double *s_mean, double *s_stdev) {
    int64_t t_max = INT64_MIN;
    int64_t s_max = INT64_MIN;

    /* get max values */
    sll_loop(subdirs, node) {
        DirData_t *dd = (DirData_t *) sll_node_data(node);

        if (dd->t.value > t_max) {
            t_max = dd->t.value;
        }
        if (dd->s.value > s_max) {
            s_max = dd->s.value;
        }
    }

    double  t_tot      = 0;
    double  t_totsq    = 0;
    double  t_mean_sum = 0;
    double  s_tot      = 0;
    double  s_totsq    = 0;
    double  s_mean_sum = 0;
    sll_loop(subdirs, node) {
        DirData_t *dd = (DirData_t *) sll_node_data(node);

        const double t_val = t_max - dd->t.value;
        t_tot       += t_val;
        t_totsq     += t_val * t_val;
        t_mean_sum  += dd->t.value;

        const double s_val = s_max - dd->s.value;
        s_tot       += s_val;
        s_totsq     += s_val * s_val;
        s_mean_sum  += dd->s.value;
    }

    const uint64_t n = sll_get_size(subdirs);

    *t_mean  = t_mean_sum / n;
    *t_stdev = stdev_from_parts(t_totsq, t_tot, n, 0);

    *s_mean  = s_mean_sum / n;
    *s_stdev = stdev_from_parts(s_totsq, s_tot, n, 0);
}
/* ************************************* */

trie_t *setup_column_functions(void) {
    static const ColHandler_t T_ONLY = {
        .gen_sql            = gen_t_only_sql,
        .sqlite_callback    = get_t_only_callback,
        .compute_mean_stdev = compute_t_only_mean_stdev,
    };

    static const ColHandler_t MINMAX = {
        .gen_sql            = gen_ts_sql,
        .sqlite_callback    = get_ts_callback,
        .compute_mean_stdev = compute_minmax_mean_stdev,
    };

    static const ColHandler_t TOT = {
        .gen_sql            = gen_ts_sql,
        .sqlite_callback    = get_ts_callback,
        .compute_mean_stdev = compute_tot_mean_stdev,
    };

    static const ColHandler_t TIME = {
        .gen_sql            = gen_ts_sql,
        .sqlite_callback    = get_ts_callback,
        .compute_mean_stdev = compute_time_mean_stdev,
    };

    trie_t *col_funcs = trie_alloc();

    /* only available in treesummary */
    trie_insert(col_funcs, "totsubdirs",     10, (void *) &T_ONLY, NULL);
    trie_insert(col_funcs, "maxsubdirfiles", 14, (void *) &T_ONLY, NULL);
    trie_insert(col_funcs, "maxsubdirlinks", 14, (void *) &T_ONLY, NULL);
    trie_insert(col_funcs, "maxsubdirsize",  13, (void *) &T_ONLY, NULL);

    /* min/max columns */
    /* trie_insert(col_funcs, "minuid",          6, (void *) &MINMAX, NULL); */
    /* trie_insert(col_funcs, "maxuid",          6, (void *) &MINMAX, NULL); */
    /* trie_insert(col_funcs, "mingid",          6, (void *) &MINMAX, NULL); */
    /* trie_insert(col_funcs, "maxgid",          6, (void *) &MINMAX, NULL); */
    trie_insert(col_funcs, "minsize",         7, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "maxsize",         7, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "minblocks",       9, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "maxblocks",       9, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "minatime",        8, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "maxatime",        8, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "minmtime",        8, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "maxmtime",        8, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "minctime",        8, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "maxctime",        8, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "mincrtime",       9, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "maxcrtime",       9, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "minossint1",     10, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "maxossint1",     10, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "minossint2",     10, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "maxossint2",     10, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "minossint3",     10, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "maxossint3",     10, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "minossint4",     10, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "maxossint4",     10, (void *) &MINMAX, NULL);

    /* raw tot columns */
    trie_insert(col_funcs, "totfiles",        8, (void *) &TOT,    NULL);
    trie_insert(col_funcs, "totlinks",        8, (void *) &TOT,    NULL);
    trie_insert(col_funcs, "totsize",         7, (void *) &TOT,    NULL);
    trie_insert(col_funcs, "totzero",         7, (void *) &TOT,    NULL);
    trie_insert(col_funcs, "totltk",          6, (void *) &TOT,    NULL);
    trie_insert(col_funcs, "totmtk",          6, (void *) &TOT,    NULL);
    trie_insert(col_funcs, "totltm",          6, (void *) &TOT,    NULL);
    trie_insert(col_funcs, "totmtm",          6, (void *) &TOT,    NULL);
    trie_insert(col_funcs, "totmtg",          6, (void *) &TOT,    NULL);
    trie_insert(col_funcs, "totmtt",          6, (void *) &TOT,    NULL);
    trie_insert(col_funcs, "totblocks",       9, (void *) &TOT,    NULL);
    trie_insert(col_funcs, "totossint1",     10, (void *) &TOT,    NULL);
    trie_insert(col_funcs, "totossint2",     10, (void *) &TOT,    NULL);
    trie_insert(col_funcs, "totossint3",     10, (void *) &TOT,    NULL);
    trie_insert(col_funcs, "totossint4",     10, (void *) &TOT,    NULL);

    /* requires offset */
    trie_insert(col_funcs, "totatime",        8, (void *) &TIME,   NULL);
    trie_insert(col_funcs, "totmtime",        8, (void *) &TIME,   NULL);
    trie_insert(col_funcs, "totctime",        8, (void *) &TIME,   NULL);
    trie_insert(col_funcs, "totcrtime",       9, (void *) &TIME,   NULL);

    return col_funcs;
}

#define T_ONLY_COLS                             \
    { "totsubdirs",     10, },                  \
    { "maxsubdirfiles", 14, },                  \
    { "maxsubdirlinks", 14, },                  \
    { "maxsubdirsize",  13, },

#define MIN_COLS                                \
    /* { "minuid",          6, },            */ \
    /* { "mingid",          6, },            */ \
    { "minsize",         7, },                  \
    { "minblocks",       9, },                  \
    { "minatime",        8, },                  \
    { "minmtime",        8, },                  \
    { "minctime",        8, },                  \
    { "mincrtime",       9, },                  \
    { "minossint1",     10, },                  \
    { "minossint2",     10, },                  \
    { "minossint3",     10, },                  \
    { "minossint4",     10, },

#define MAX_COLS                                \
    /* { "maxuid",          6, },          */   \
    /* { "maxgid",          6, },          */   \
    { "maxsize",         7, },                  \
    { "maxblocks",       9, },                  \
    { "maxatime",        8, },                  \
    { "maxmtime",        8, },                  \
    { "maxctime",        8, },                  \
    { "maxcrtime",       9, },                  \
    { "maxossint1",     10, },                  \
    { "maxossint2",     10, },                  \
    { "maxossint3",     10, },                  \
    { "maxossint4",     10, },

#define TOT_COLS                                \
    { "totfiles",        8, },                  \
    { "totlinks",        8, },                  \
    { "totsize",         7, },                  \
    { "totzero",         7, },                  \
    { "totltk",          6, },                  \
    { "totmtk",          6, },                  \
    { "totltm",          6, },                  \
    { "totmtm",          6, },                  \
    { "totmtg",          6, },                  \
    { "totmtt",          6, },                  \
    { "totblocks",       9, },                  \
    { "totossint1",     10, },                  \
    { "totossint2",     10, },                  \
    { "totossint3",     10, },                  \
    { "totossint4",     10, },

#define TIME_COLS                               \
    { "totatime",        8, },                  \
    { "totmtime",        8, },                  \
    { "totctime",        8, },                  \
    { "totcrtime",       9, },

#define LAST                                    \
    { NULL,              0, },

static const refstr_t T_ONLYS[] = {
    T_ONLY_COLS
    LAST
};

static const refstr_t MINS[] = {
    MIN_COLS
    LAST
};

static const refstr_t MAXS[] = {
    MAX_COLS
    LAST
};

static const refstr_t MINMAXS[] = {
    MIN_COLS
    MAX_COLS
    LAST
};

static const refstr_t TOTS[] = {
    TOT_COLS
    LAST
};

static const refstr_t TIMES[] = {
    TIME_COLS
    LAST
};

static const refstr_t ALL[] = {
    T_ONLY_COLS
    MIN_COLS
    MAX_COLS
    TOT_COLS
    TIME_COLS
    LAST
};

typedef struct {
    const refstr_t name;
    const refstr_t *cols;
} Group_t;

static const Group_t GROUPS[] = {
    { { "T_ONLYS", 7, }, T_ONLYS, },
    { { "MINS",    5, }, MINS,    },
    { { "MAXS",    5, }, MAXS,    },
    { { "MINMAXS", 7, }, MINMAXS, },
    { { "TOTS",    4, }, TOTS,    },
    { { "TIMES",   5, }, TIMES,   },
    { { "ALL",     3, }, ALL,     },
};

static const size_t GROUP_COUNT = sizeof(GROUPS) / sizeof(GROUPS[0]);

const refstr_t *handle_group(const char *name, const size_t len) {
    for(size_t i = 0; i < GROUP_COUNT; i++) {
        const Group_t *g = &GROUPS[i];
        if (g->name.len == len) {
            if (strncmp(g->name.data, name, len) == 0) {
                return g->cols;
            }
        }
    }

    return NULL;
}
