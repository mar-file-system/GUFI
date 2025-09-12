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

/* ************************************* */
static str_t *gen_single_value_sql(str_t *dst, const char *col) {
    str_alloc_existing(dst, MAXSQL);
    dst->len = SNPRINTF(dst->data, dst->len, "SELECT %s FROM %s;",
                        col, TREESUMMARY);

    return dst;
}

static int get_single_value_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    DirData_t *dd = (DirData_t *) args;

    return !(sscanf(data[0], "%lf", &dd->tot) == 1);
}

/* compute partial sample stdev for single value columns */
static void compute_single_value_mean_stdev(sll_t *subdirs, double *mean, double *stdev) {
    double tot   = 0;
    double totsq = 0;
    sll_loop(subdirs, node) {
        DirData_t *dd = (DirData_t *) sll_node_data(node);
        tot   += dd->tot;
        totsq += ((double) dd->tot) * dd->tot;
    }

    const uint64_t n = sll_get_size(subdirs);

    const double variance = ((n * totsq) - (tot * tot)) / (n * (n - 1));

    *mean  = tot / n;
    *stdev = sqrt(variance);
}
/* ************************************* */

/* ************************************* */
str_t *gen_sum_sql(str_t *dst, const char *col) {
    str_alloc_existing(dst, MAXSQL);
    dst->len = SNPRINTF(dst->data, dst->len, "SELECT tot%s FROM %s;",
                        col, TREESUMMARY);
    return dst;
}

static int get_sum_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    DirData_t *dd = (DirData_t *) args;

    return !(sscanf(data[0], "%lf", &dd->tot) == 1);
}

/* compute partial sample stdev for sum columns */
static void compute_sum_mean_stdev(sll_t *subdirs, double *mean, double *stdev) {
    double tot   = 0;
    double totsq = 0;
    sll_loop(subdirs, node) {
        DirData_t *dd = (DirData_t *) sll_node_data(node);
        tot   += dd->tot;
        totsq += ((double) dd->tot) * dd->tot;
    }

    const uint64_t n = sll_get_size(subdirs);

    const double variance = ((n * totsq) - (tot * tot)) / (n * (n - 1));

    *mean  = tot / n;
    *stdev = sqrt(variance);
}
/* ************************************* */

/* ************************************* */
str_t *gen_time_sql(str_t *dst, const char *col) {
    str_alloc_existing(dst, MAXSQL);
    dst->len = SNPRINTF(dst->data, dst->len, "SELECT tot%s, epoch, totfiles + totlinks FROM %s;",
                        col, TREESUMMARY);
    return dst;
}

static int get_time_callback(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;
    DirData_t *dd = (DirData_t *) args;

    return !((sscanf(data[0], "%lf",      &dd->tot)     == 1) &&
             (sscanf(data[1], "%" PRId64, &dd->epoch)   == 1) &&
             (sscanf(data[2], "%" PRId64, &dd->nondirs) == 1));
}

/* compute partial sample stdev for time columns */
static void compute_time_mean_stdev(sll_t *subdirs, double *mean, double *stdev) {
    double tot      = 0;
    double totsq    = 0;
    double mean_sum = 0;
    sll_loop(subdirs, node) {
        DirData_t *dd = (DirData_t *) sll_node_data(node);
        tot      += dd->tot;
        totsq    += ((double) dd->tot) * dd->tot;
        mean_sum += ((double) (dd->epoch * dd->nondirs)) + dd->tot;
    }

    const uint64_t n = sll_get_size(subdirs);

    const double variance = ((n * totsq) - (tot * tot)) / (n * (n - 1));

    *mean  = mean_sum / n;
    *stdev = sqrt(variance);
}
/* ************************************* */

trie_t *setup_column_functions(void) {
    static const ColHandler_t SINGLE_VALUE = {
        .gen_sql            = gen_single_value_sql,
        .sqlite_callback    = get_single_value_callback,
        .compute_mean_stdev = compute_single_value_mean_stdev,
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

    /* single value */
    trie_insert(col_funcs, "totsubdirs",     10, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "totfiles",        8, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "totlinks",        8, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "maxsubdirfiles", 14, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "maxsubdirlinks", 14, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "maxsubdirsize",  13, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "minatime",        8, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "maxatime",        8, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "minmtime",        8, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "maxmtime",        8, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "minctime",        8, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "maxctime",        8, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "mincrtime",       9, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "maxcrtime",       9, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "minossint1",     10, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "maxossint1",     10, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "minossint2",     10, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "maxossint2",     10, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "minossint3",     10, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "maxossint3",     10, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "minossint4",     10, (void *) &SINGLE_VALUE, NULL);
    trie_insert(col_funcs, "maxossint4",     10, (void *) &SINGLE_VALUE, NULL);

    /* sum of multiple values */
    trie_insert(col_funcs, "size",            4, (void *) &SUM,          NULL);
    trie_insert(col_funcs, "blocks",          6, (void *) &SUM,          NULL);
    trie_insert(col_funcs, "ossint1",         7, (void *) &SUM,          NULL);
    trie_insert(col_funcs, "ossint2",         8, (void *) &SUM,          NULL);
    trie_insert(col_funcs, "ossint3",         8, (void *) &SUM,          NULL);
    trie_insert(col_funcs, "ossint4",         8, (void *) &SUM,          NULL);

    /* requires epoch + count */
    trie_insert(col_funcs, "atime",           5, (void *) &TIME,         NULL);
    trie_insert(col_funcs, "mtime",           5, (void *) &TIME,         NULL);
    trie_insert(col_funcs, "ctime",           5, (void *) &TIME,         NULL);
    trie_insert(col_funcs, "crtime",          6, (void *) &TIME,         NULL);

    return col_funcs;
}
