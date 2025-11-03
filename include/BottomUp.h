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
   This header provides an API for parallelized
   bottom-up traversal of a directory tree.
*/

#ifndef GUFI_BOTTOM_UP_H
#define GUFI_BOTTOM_UP_H

#include "QueuePerThreadPool.h"
#include "SinglyLinkedList.h"
#include "bf.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
  Structure containing the necessary information
  to traverse a tree upwards.

  This struct should be wrapped by a user struct.

  This struct will likely be directly used by
  the user, so the imeplementation is not opaque.
*/
struct BottomUp {
    char *name;
    size_t name_len;
    char *alt_name;
    size_t alt_name_len;
    struct {
        pthread_mutex_t mutex;
        size_t remaining;
    } refs;
    size_t subdir_count;
    size_t subnondir_count;
    sll_t subdirs;
    sll_t subnondirs;
    struct BottomUp *parent;

    /* when this run was started */
    long long int epoch;

    /* extra arguments available at all times */
    void *extra_args;

    size_t level;
    struct {
        size_t down;
        size_t up;
    } tid;
};

/* Signature of function for processing */
/* directories while traversing a tree */
typedef int (*BU_descend_f)(void *user_struct, int *keep_going);
typedef int (*BU_ascend_f) (void *user_struct);

/* ****************************************************************** */
/*
 * Parallel BottomUp Components
 *
 * Allows for the QPTPool context to be reused for other purposes if
 * both non-BottomUp operations and BottomUp operations need to be
 * done in the same process so that the two sets of operations do not
 * need to be done in separate pools, using more resources than a user
 * might expect. Note that non-BottomUp threads will not have the
 * QPTPool arg argument available to them because it will have been
 * assigned to an opaque value by BottomUp.
 */
QPTPool_t *parallel_bottomup_init(const size_t thread_count,
                                  const size_t user_struct_size,
                                  const size_t min_level, const size_t max_level,
                                  BU_descend_f descend, BU_ascend_f ascend,
                                  const int track_non_dirs,
                                  const int generate_alt_name);

int parallel_bottomup_enqueue(QPTPool_t *pool,
                              const char *path, const size_t len,
                              void *extra_args);

int parallel_bottomup_fini(QPTPool_t *pool);
/* ****************************************************************** */

/*
 * Function user should call to walk an existing tree in parallel.
 *
 * Returning from the bottom does not happen until all subdirectories
 * in the current subtree have been processed.
 *
 * This function wraps parallel_bottomup_{init,enqueue,fini}.
 */
int parallel_bottomup(char **root_names, const size_t root_count,
                      const size_t min_level, const size_t max_level,
                      const refstr_t *path_list,
                      const size_t thread_count,
                      const size_t user_struct_size,
                      BU_descend_f descend, BU_ascend_f ascend,
                      const int track_non_dirs,
                      const int generate_alt_name,
                      void *extra_args);

/* free a struct BottomUp */
void bottomup_destroy(void *p);

#ifdef __cplusplus
}
#endif

#endif
