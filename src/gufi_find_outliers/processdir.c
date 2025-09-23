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



#include "SinglyLinkedList.h"

#include "gufi_find_outliers/OutlierWork.h"
#include "gufi_find_outliers/PoolArgs.h"
#include "gufi_find_outliers/get_subdirs.h"
#include "gufi_find_outliers/processdir.h"
#include "gufi_find_outliers/handle_db.h"

/*
 * Given a GUFI tree with treesummary tables, walk the parts of the
 * tree where subtree outliers are found. When outliers stop being
 * found, print the directory path.
 *
 * High level idea:
 *     At directory D
 *         1. Get subdirectories and raw value(s) for the selected column.
 *
 *         2. If the current directory does not have subdirectories
 *            and was marked as containing outliers, report the
 *            current directory.
 *
 *         3. Compute the mean and population standard deviation for
 *            the entire subtree
 *             - Population standard deviation because subtrees/directories
 *               do not represent a sampling of other subtrees/directories
 *               in this tree or any other.
 *
 *         4. Iterate through the subdirectories. If a subdirectory
 *            subtree's value is 3+ standard deviations from the mean,
 *            mark it as an outlier and push it to the outliers
 *            list.
 *
 *         5. If there are subdirectory subtree outliers, only descend
 *            into those directories. Otherwise, descend into all
 *            subdirectories.
 */

/*
 * reasons to arrive at current node
 *     root - no choice / current level had no outliers
 *     contains an outlier
 *     parent found no outliers
 *
 * subdirectories
 *     0
 *     some outliers
 *     no outliers
 */
int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    struct PoolArgs *pa = (struct PoolArgs *) args;
    OutlierWork_t *ow = (OutlierWork_t *) data;

    int rc = 0;

    sll_t subdirs;
    get_subdirs(ow, &subdirs, &pa->opendbs[id], ctx, id);

    const uint64_t n = sll_get_size(&subdirs);

    /* nothing under here */
    if (n == 0) {
        /* if this directory was an outlier, print this directory */
        if (ow->is_outlier) {
            sqlite3_stmt *stmt = insertdbprep(pa->dbs[id], OUTLIERS_INSERT);
            insert_outlier(stmt, &ow->path, ow->level,
                           "no-subdirs", &ow->col, &ow->t, &ow->s);
            insertdbfin(stmt);
        }
        sll_destroy(&subdirs, NULL);
    }
    else {
        sll_t *work = NULL;

        sll_t outliers;
        sll_init(&outliers);

        const size_t next_level = ow->level + 1;

        if (n == 1) {
            /* not enough samples to get standard deviation, so just go down a level */
            work = &subdirs;
        }
        else {
            /* compute mean and standard deviation */
            double t_mean = 0, t_stdev = 0;
            double s_mean = 0, s_stdev = 0;
            ow->handler->compute_mean_stdev(&subdirs,
                                            &t_mean, &t_stdev,
                                            &s_mean, &s_stdev);

            /* get 3 sigma values */
            const double t_limit = 3 * t_stdev;
            const double t_lo    = t_mean - t_limit;
            const double t_hi    = t_mean + t_limit;

            sqlite3_stmt *stmt   = insertdbprep(pa->dbs[id], OUTLIERS_INSERT);

            /* find outliers */
            sll_loop(&subdirs, node) {
                DirData_t *dd = (DirData_t *) sll_node_data(node);
                dd->t.mean   = t_mean;
                dd->t.stdev  = t_stdev;
                dd->s.mean   = s_mean;
                dd->s.stdev  = s_stdev;

                /* if the subtree is an outlier, queue it up for processing */
                if ((dd->t.value < t_lo) || (t_hi < dd->t.value)) {
                    sll_push(&outliers, dd);
                }
            }

            insertdbfin(stmt);

            /*
             * if there is at least one subtree outlier, process only outliers
             * otherwise, process all subdirs/subtrees
             */
            work = (sll_get_size(&outliers) == 0)?&subdirs:&outliers;
        }

        const int sub_outlier = (work == &outliers); /* all outliers or all not outliers */

        /*
         * current directory is a common root for subtree outliers
         * because the subtree contains an outlier but none of the
         * subdirectory subtrees have outliers
         */
        if (ow->is_outlier && !sub_outlier) {
            sqlite3_stmt *stmt = insertdbprep(pa->dbs[id], OUTLIERS_INSERT);
            insert_outlier(stmt, &ow->path, ow->level,
                           "subdirs-not-outliers", &ow->col, &ow->t, &ow->s);
            insertdbfin(stmt);
        }
        /*
         * current directory is not an outlier
         * or
         * current directory is an outlier, but there are no subdirectory outliers
         */
        else {
            sll_loop(work, node) {
                DirData_t *dd = (DirData_t *) sll_node_data(node);
                OutlierWork_t *new_ow = OutlierWork_create(&dd->path, next_level,
                                                           ow->col,
                                                           ow->handler, ow->query,
                                                           sub_outlier,
                                                           &dd->t, &dd->s);

                QPTPool_enqueue(ctx, id, processdir, new_ow);
            }
        }

        sll_destroy(&outliers, NULL); /* references to subdirs, so don't clean up */
        sll_destroy(&subdirs, DirData_free);
    }

    OutlierWork_free(ow);

    return rc;
}
