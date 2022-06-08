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
   Operations are performed on directories
   during the upward portion of the traversal.
*/

#ifndef GUFI_BOTTOM_UP_H
#define GUFI_BOTTOM_UP_H

#include <pthread.h>

#include "SinglyLinkedList.h"
#include "bf.h"
#include "debug.h"

/* extra AscendFunc_t argments */
#if defined(DEBUG) && defined(PER_THREAD_STATS)
    #include "OutputBuffers.h"
    #define timestamp_sig  , struct OutputBuffers *timestamp_buffers
    #define timestamp_args , timestamp_buffers
#else
    #define timestamp_sig
    #define timestamp_args
#endif

/*
  Structure containing the necessary information
  to traverse a tree upwards.

  This struct should be wrapped by a user struct.

  This struct will likely be directly used by
  the user, so the imeplementation is not opaque.
*/
struct BottomUp {
    char name[MAXPATH];
    size_t name_len;
    struct stat st;
    struct {
        pthread_mutex_t mutex;
        size_t remaining;
    } refs;
    size_t subdir_count;
    size_t subnondir_count;
    struct sll subdirs;
    struct sll subnondirs;
    struct BottomUp *parent;

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
typedef void (*BU_f)(void *user_struct
                     timestamp_sig);

/*
 * Function user should call to walk an existing tree in parallel.
 *
 * Similar to descent, returning from the bottom upwards does
 * not happen until all subdirectories have been processed.
 */
int parallel_bottomup(char **root_names, size_t root_count,
                      const size_t thread_count,
                      const size_t user_struct_size,
                      BU_f descend, BU_f ascend,
                      const int track_non_dirs,
                      void *extra_args
                      #if defined(DEBUG) && defined(PER_THREAD_STATS)
                      , struct OutputBuffers *debug_buffers
                      #endif
);

#endif
