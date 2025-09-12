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

/* GUFI headers */
#include "bf.h"
#include "dbutils.h"
#include "utils.h"

/* stats headers */
#include "DirData.h"
#include "handle_columns.h"

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
    dd->s.tot = 0;

    return !(sscanf(data[0], "%lf", &dd->t.tot) == 1);
}

/* compute partial sample stdev for single value columns */
static void compute_t_only_mean_stdev(sll_t *subdirs,
                                      double *t_mean, double *t_stdev,
                                      double *s_mean, double *s_stdev) {
    double t_tot   = 0;
    double t_totsq = 0;
    sll_loop(subdirs, node) {
        DirData_t *dd = (DirData_t *) sll_node_data(node);
        t_tot   += dd->t.tot;
        t_totsq += ((double) dd->t.tot) * dd->t.tot;
    }

    const uint64_t n = sll_get_size(subdirs);

    *t_mean  = t_tot / n;
    *t_stdev = stdev_from_parts(t_totsq, t_tot, n, 1);

    *s_mean = 0;
    *s_stdev = 0;
}
/* ************************************* */

/* ************************************* */
static str_t *gen_minmax_sql(str_t *dst, const char *col) {
    str_alloc_existing(dst, MAXSQL);
    dst->len = SNPRINTF(dst->data, dst->len,
                        "SELECT t.%s, "
                        "       s.%s "
                        "FROM   %s AS t "
                        "       INNER JOIN "
                        "       %s AS s "
                        "       ON (t.inode == s.inode) AND (s.isroot == 1) "
                        ";",
                        col, TREESUMMARY);
    return dst;
}

static int get_minmax_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    DirData_t *dd = (DirData_t *) args;

    return !((sscanf(data[0], "%lf", &dd->t.tot) == 1) &&
             (sscanf(data[1], "%lf", &dd->s.tot) == 1));
}

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
        t_tot   += dd->t.tot;
        t_totsq += ((double) dd->t.tot) * dd->t.tot;
        s_tot   += dd->s.tot;
        s_totsq += ((double) dd->s.tot) * dd->s.tot;
    }

    const uint64_t n = sll_get_size(subdirs);

    *t_mean  = t_tot / n;
    *t_stdev = stdev_from_parts(t_totsq, t_tot, n, 1);

    *s_mean  = s_tot / n;
    *s_stdev = stdev_from_parts(s_totsq, s_tot, n, 1);
}
/* ************************************* */

/* ************************************* */
str_t *gen_sum_sql(str_t *dst, const char *col) {
    str_alloc_existing(dst, MAXSQL);
    dst->len = SNPRINTF(dst->data, dst->len,
                        "SELECT t.tot%s, "
                        "       s.tot%s "
                        "FROM   %s AS t "
                        "       INNER JOIN "
                        "       %s AS s "
                        "       ON (t.inode == s.inode) AND (s.isroot == 1) "
                        ";",
                        col, col, TREESUMMARY, SUMMARY);
    return dst;
}

static int get_sum_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    DirData_t *dd = (DirData_t *) args;

    return !((sscanf(data[0], "%lf", &dd->t.tot) == 1) &&
             (sscanf(data[1], "%lf", &dd->s.tot) == 1));
}

/* compute partial sample stdev for sum columns */
static void compute_sum_mean_stdev(sll_t *subdirs,
                                   double *t_mean, double *t_stdev,
                                   double *s_mean, double *s_stdev) {
    double t_tot   = 0;
    double t_totsq = 0;
    double s_tot   = 0;
    double s_totsq = 0;
    sll_loop(subdirs, node) {
        DirData_t *dd = (DirData_t *) sll_node_data(node);
        t_tot   += dd->t.tot;
        t_totsq += ((double) dd->t.tot) * dd->t.tot;
        s_tot   += dd->s.tot;
        s_totsq += ((double) dd->s.tot) * dd->s.tot;
    }

    const uint64_t n = sll_get_size(subdirs);

    *t_mean  = t_tot / n;
    *t_stdev = stdev_from_parts(t_totsq, t_tot, n, 1);
    *s_mean  = s_tot / n;
    *s_stdev = stdev_from_parts(s_totsq, s_tot, n, 1);
}
/* ************************************* */

/* ************************************* */
str_t *gen_time_sql(str_t *dst, const char *col) {
    str_alloc_existing(dst, MAXSQL);
    dst->len = SNPRINTF(dst->data, dst->len,
                        "SELECT t.tot%s, t.epoch, t.totfiles + t.totlinks, "
                        "       s.tot%s, s.epoch, s.totfiles + s.totlinks "
                        "FROM   %s AS t "
                        "       INNER JOIN "
                        "       %s AS s "
                        "       ON (t.inode == s.inode) AND (s.isroot == 1) "
                        ";",
                        col, col, TREESUMMARY, SUMMARY);
    return dst;
}

static int get_time_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    DirData_t *dd = (DirData_t *) args;

    return !((sscanf(data[0], "%lf",      &dd->t.tot)     == 1) &&
             (sscanf(data[1], "%" PRId64, &dd->t.epoch)   == 1) &&
             (sscanf(data[2], "%" PRId64, &dd->t.nondirs) == 1) &&
             (sscanf(data[3], "%lf",      &dd->s.tot)     == 1) &&
             (sscanf(data[4], "%" PRId64, &dd->s.epoch)   == 1) &&
             (sscanf(data[5], "%" PRId64, &dd->s.nondirs) == 1));
}

/* compute partial sample stdev for time columns */
static void compute_time_mean_stdev(sll_t *subdirs,
                                    double *t_mean, double *t_stdev,
                                    double *s_mean, double *s_stdev) {
    double t_tot      = 0;
    double t_totsq    = 0;
    double t_mean_sum = 0;
    double s_tot      = 0;
    double s_totsq    = 0;
    double s_mean_sum = 0;
    sll_loop(subdirs, node) {
        DirData_t *dd = (DirData_t *) sll_node_data(node);
        t_tot      += dd->t.tot;
        t_totsq    += ((double) dd->t.tot) * dd->t.tot;
        t_mean_sum += ((double) (dd->t.epoch * dd->t.nondirs)) + dd->t.tot;
        s_tot      += dd->s.tot;
        s_totsq    += ((double) dd->s.tot) * dd->s.tot;
        s_mean_sum += ((double) (dd->s.epoch * dd->s.nondirs)) + dd->s.tot;
    }

    const uint64_t n = sll_get_size(subdirs);

    *t_mean  = t_mean_sum / n;
    *t_stdev = stdev_from_parts(t_totsq, t_tot, n, 1);
    *s_mean  = s_mean_sum / n;
    *s_stdev = stdev_from_parts(s_totsq, s_tot, n, 1);
}
/* ************************************* */

trie_t *setup_column_functions(void) {
    static const ColHandler_t T_ONLY = {
        .gen_sql            = gen_t_only_sql,
        .sqlite_callback    = get_t_only_callback,
        .compute_mean_stdev = compute_t_only_mean_stdev,
    };

    static const ColHandler_t MINMAX = {
        .gen_sql            = gen_minmax_sql,
        .sqlite_callback    = get_minmax_callback,
        .compute_mean_stdev = compute_minmax_mean_stdev,
    };

    static const ColHandler_t SUM = {
        .gen_sql            = gen_sum_sql,
        .sqlite_callback    = get_sum_callback,
        .compute_mean_stdev = compute_sum_mean_stdev,
    };

    static const ColHandler_t TIME = {
        .gen_sql            = gen_time_sql,
        .sqlite_callback    = get_time_callback,
        .compute_mean_stdev = compute_time_mean_stdev,
    };

    trie_t *col_funcs = trie_alloc();

    /* only available in treesummary */
    trie_insert(col_funcs, "totsubdirs",     10, (void *) &T_ONLY, NULL);
    trie_insert(col_funcs, "maxsubdirfiles", 14, (void *) &T_ONLY, NULL);
    trie_insert(col_funcs, "maxsubdirlinks", 14, (void *) &T_ONLY, NULL);
    trie_insert(col_funcs, "maxsubdirsize",  13, (void *) &T_ONLY, NULL);

    /* computed using min/max */
    trie_insert(col_funcs, "totfiles",        8, (void *) &MINMAX, NULL);
    trie_insert(col_funcs, "totlinks",        8, (void *) &MINMAX, NULL);
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

    /* sum of multiple values */
    trie_insert(col_funcs, "size",            4, (void *) &SUM,    NULL);
    trie_insert(col_funcs, "blocks",          6, (void *) &SUM,    NULL);
    trie_insert(col_funcs, "ossint1",         7, (void *) &SUM,    NULL);
    trie_insert(col_funcs, "ossint2",         8, (void *) &SUM,    NULL);
    trie_insert(col_funcs, "ossint3",         8, (void *) &SUM,    NULL);
    trie_insert(col_funcs, "ossint4",         8, (void *) &SUM,    NULL);

    /* requires epoch + count */
    trie_insert(col_funcs, "atime",           5, (void *) &TIME,   NULL);
    trie_insert(col_funcs, "mtime",           5, (void *) &TIME,   NULL);
    trie_insert(col_funcs, "ctime",           5, (void *) &TIME,   NULL);
    trie_insert(col_funcs, "crtime",          6, (void *) &TIME,   NULL);

    return col_funcs;
}
